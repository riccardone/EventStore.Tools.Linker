using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using System.Timers;
using EventTypeFilter = KurrentDB.Client.EventTypeFilter;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Linker;

public class LinkerService : ILinkerService, IAsyncDisposable
{
    private readonly ILogger _logger;
    private readonly IPositionRepository _positionRepository;
    private readonly IStreamPositionFlusher _flusherForStreamPositions;
    private readonly IFilterService? _filterService;
    private readonly Settings _settings;
    private Position _lastPosition;
    public string Name { get; }

    private readonly ILinkerConnectionBuilder _originConnectionBuilder;
    private readonly ILinkerConnectionBuilder _destinationConnectionBuilder;
    private KurrentDBClient? _originConnection;
    private KurrentDBClient? _destinationConnection;

    private Channel<BufferedEvent> _channel;
    private Task? _processingTask;

    private readonly System.Timers.Timer _timerForStats;

    private readonly LinkerHelper _replicaHelper;
    private CancellationTokenSource _cts = new();
    private Task? _subscriptionTask;
    private bool _started;

    private long _replicatedSinceLastStats;
    private long _replicatedTotal;

    private readonly double _currentBackpressureThreshold = 0.8;
    private readonly List<long> _latencySamples = [];
    private readonly List<long> _replicationSamples = [];
    private readonly Lock _adaptiveLock = new();
    private int _bufferSize;
    private int _adaptiveIntervalCounter;
    private readonly decimal _allowedIncreaseOrDecreaseAmount = 0.15m;
    private readonly double _significantRegressionToTriggerDecrease = -0.10;
    private readonly double _significantIncreaseToTriggerIncrease = -0.10;
    public const int MaxAllowedBuffer = 1000;
    public const int MinAllowedBuffer = 1;

    private readonly ConcurrentDictionary<string, SortedDictionary<ulong, BufferedEvent>> _perStreamBuffers = new();
    private readonly ConcurrentDictionary<string, ulong> _lastWrittenPerStream = new();
    private readonly HashSet<string> _streamsToBeExcluded;
    private readonly HashSet<string> _adjustedStartStreams = new();
    private readonly IAdjustedStreamRepository _adjustedStreamRepository;

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        IPositionRepository positionRepository, IFilterService? filterService, Settings settings, IAdjustedStreamRepository adjustedStreamRepository,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger(nameof(LinkerService));
        Name = $"From-{originBuilder.ConnectionName}-To-{destinationBuilder.ConnectionName}";

        _originConnectionBuilder = originBuilder;
        _destinationConnectionBuilder = destinationBuilder;

        _positionRepository = positionRepository;
        _filterService = filterService;
        _settings = settings;
        _bufferSize = Math.Clamp(settings.BufferSize, MinAllowedBuffer, MaxAllowedBuffer);

        var streamPositionsPath = Path.Combine(settings.DataFolder, "positions", $"stream_positions_{Name}.json");
        _flusherForStreamPositions =
            new PeriodicStreamPositionFlusher(streamPositionsPath, loggerFactory.CreateLogger("Flusher"));
        _adjustedStreamRepository = adjustedStreamRepository;
        //_adjustedStreamsPath = Path.Combine(settings.DataFolder, "positions", $"adjusted_streams_{Name}.json");

        _replicaHelper = new LinkerHelper();

        _streamsToBeExcluded =
        [
            $"PositionStream-{_originConnectionBuilder.ConnectionName}",
            $"$$PositionStream-{_originConnectionBuilder.ConnectionName}"
        ];

        _timerForStats = new System.Timers.Timer(3000);
        _timerForStats.Elapsed += TimerForStats_Elapsed;
    }

    private Channel<BufferedEvent> CreateNewChannel(int size)
    {
        return Channel.CreateBounded<BufferedEvent>(new BoundedChannelOptions(size)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = false,
            SingleReader = true
        });
    }

    private async Task ResizeChannelAsync(int newSize)
    {
        _logger.LogInformation($"{Name}: Resizing buffer from {_bufferSize} to {newSize}");

        await _cts.CancelAsync();
        _channel.Writer.TryComplete();

        if (_subscriptionTask is not null)
        {
            try { await _subscriptionTask; } catch (OperationCanceledException) { }
        }

        if (_processingTask is not null)
        {
            try { await _processingTask; } catch (OperationCanceledException) { }
        }

        var buffered = new List<BufferedEvent>();
        while (_channel.Reader.TryRead(out var evt))
            buffered.Add(evt);

        _cts.Dispose();
        _cts = new CancellationTokenSource();
        _bufferSize = newSize;
        _channel = CreateNewChannel(_bufferSize);

        foreach (var evt in buffered)
            await _channel.Writer.WriteAsync(evt);

        StartWorkerTasks();
    }

    private void StartWorkerTasks()
    {
        _logger.LogDebug($"{Name}: Starting background tasks...");
        _processingTask = Task.Run(ProcessChannelEventsAsync);
        _subscriptionTask = SubscribeMeGrpc(_cts.Token);
    }

    public async Task<bool> StartAsync()
    {
        if (_started) return true;

        await StopAsync();

        if (!_positionRepository.TryGet(out _lastPosition))
            _lastPosition = Position.Start;

        _adjustedStartStreams.Clear();
        var loaded = await _adjustedStreamRepository.LoadAsync();
        foreach (var s in loaded)
            _adjustedStartStreams.Add(s);
        _logger.LogInformation($"{Name}: Loaded {_adjustedStartStreams.Count} adjusted start streams from disk");

        if (_lastPosition.Equals(Position.Start))
            _logger.LogInformation("No last position found. Starting from Position.Start");

        _destinationConnection = _destinationConnectionBuilder.Build();
        _originConnection = _originConnectionBuilder.Build();

        await _flusherForStreamPositions.StartAsync();

        await ReconcileStreamPositions();

        _started = true;

        _timerForStats.Start();

        _channel = CreateNewChannel(_bufferSize);

        StartWorkerTasks();

        _logger.LogInformation($"{Name} started.");

        return true;
    }

    private async Task ReconcileStreamPositions()
    {
        var streamPositions = await _flusherForStreamPositions.LoadAsync();
        int total = streamPositions.Count;
        int current = 0;

        var stopwatch = Stopwatch.StartNew();
        var lastLogTime = TimeSpan.Zero;

        foreach (var kvp in streamPositions)
        {
            current++;
            var streamId = kvp.Key;
            ulong recoveredPosition;

            try
            {
                var last = await GetLastEventFromAStreamAsync(streamId);
                if (last is { OriginalEvent: not null })
                {
                    recoveredPosition = last.Value.OriginalEventNumber.ToUInt64();
                }
                else
                {
                    recoveredPosition = 0;
                }
            }
            catch (Exception ex)
            {
                recoveredPosition = kvp.Value;

                var last = await GetLastEventFromAStreamAsync($"$${streamId}");
                if (last.HasValue)
                    continue; // stream has been deleted

                _logger.LogWarning(ex, $"{Name}: Could not read {streamId} from destination. Using fallback value {kvp.Value}");
            }

            _lastWrittenPerStream[streamId] = recoveredPosition;
            _flusherForStreamPositions.Update(streamId, recoveredPosition);

            if (recoveredPosition > 0)
                await AddToAdjustedStreams(streamId);

            if (stopwatch.Elapsed - lastLogTime >= TimeSpan.FromSeconds(5))
            {
                lastLogTime = stopwatch.Elapsed;
                _logger.LogInformation($"{Name}: Reconciliation progress {current}/{total} streams ({(current * 100 / total)}%)");
            }
        }

        _logger.LogInformation($"{Name}: ReconcileStreamPositions completed. Total streams reconciliations processed: {total}");
    }

    public async Task<bool> StopAsync()
    {
        if (!_started)
            return true;

        await _cts.CancelAsync();
        _timerForStats.Stop();
        _channel.Writer.TryComplete();

        if (_subscriptionTask is not null)
        {
            try { await _subscriptionTask; } catch (OperationCanceledException) { }
        }

        if (_processingTask is not null)
        {
            try { await _processingTask; } catch (OperationCanceledException) { }
            _processingTask = null;
        }

        await _flusherForStreamPositions.StopAsync();
        
        await DisposeConnectionsAsync();

        _started = false;
        _logger.LogInformation($"{Name} stopped.");
        return true;
    }

    private async Task DisposeConnectionsAsync()
    {
        if (_originConnection != null)
            await _originConnection.DisposeAsync();
        if (_destinationConnection != null)
            await _destinationConnection.DisposeAsync();
    }

    private async Task ProcessChannelEventsAsync()
    {
        _logger.LogDebug($"{Name}: Channel processing started");
        try
        {
            await foreach (var evt in _channel.Reader.ReadAllAsync(_cts.Token))
            {
                _logger.LogTrace($"{Name}: Dequeued {evt.EventNumber}@{evt.StreamId}");
                await ProcessBufferedEvent(evt);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"{Name}: Channel processing error: {ex.GetBaseException().Message}");
            await RestartServiceAsync();
        }
    }

    private async Task ProcessBufferedEvent(BufferedEvent evt)
    {
        var streamId = evt.StreamId;
        var eventNum = evt.EventNumber.ToUInt64();

        var buffer = _perStreamBuffers.GetOrAdd(streamId, _ => new SortedDictionary<ulong, BufferedEvent>());
        buffer[eventNum] = evt;

        if (!_lastWrittenPerStream.TryGetValue(streamId, out var lastWritten))
        {
            // We don't yet know what's been written — assume -1 and wait for event 0
            lastWritten = ulong.MaxValue;
        }

        var expected = lastWritten + 1;

        _logger.LogTrace($"{Name}: LastWritten={lastWritten}, Expected={expected}, BufferCount={buffer.Count}");
        _logger.LogTrace($"{Name}: Buffer keys: {string.Join(",", buffer.Keys)}");

        while (buffer.TryGetValue(expected, out var next))
        {
            await AppendEventAsync(next);

            // Only after successful append
            _lastWrittenPerStream[streamId] = expected;
            _flusherForStreamPositions.Update(streamId, expected);

            if (expected == 0)
                await _flusherForStreamPositions.FlushAsync();

            buffer.Remove(expected);
            expected++;
        }

        if (buffer.Count == 0)
            _perStreamBuffers.TryRemove(streamId, out _);
    }

    private async Task AddToAdjustedStreams(string streamId)
    {
        var updated = false;

        if (_adjustedStartStreams.Add(streamId))
            updated = true;

        if (!streamId.StartsWith("$$") && _adjustedStartStreams.Add("$$" + streamId))
            updated = true;

        if (updated)
            await _adjustedStreamRepository.SaveAsync(_adjustedStartStreams);
    }

    private async Task SubscribeMeGrpc(CancellationToken ctsToken)
    {
        if (_originConnection == null)
            throw new Exception("Origin connection is not initialized");
        await using var subscription = _originConnection.SubscribeToAll(start: FromAll.After(_lastPosition), cancellationToken: ctsToken,
            resolveLinkTos: _settings.ResolveLinkTos, filterOptions: new SubscriptionFilterOptions(EventTypeFilter.RegularExpression(@"^(\$metadata|[^\$].*)")));
        await foreach (var message in subscription.Messages.WithCancellation(ctsToken))
        {
            var currentBuffer = _channel.Reader.Count;
            var threshold = _currentBackpressureThreshold;

            if (currentBuffer >= _bufferSize * threshold)
            {
                _logger.LogDebug($"{Name}: Backpressure active. Buffer={currentBuffer}/{_bufferSize}, Threshold={threshold:P0}");
                await Task.Delay(100, ctsToken);
            }

            if (message is StreamMessage.Event(var evnt))
                await HandleEventAsync(evnt);
        }
    }

    private async Task HandleEventAsync(ResolvedEvent evt)
    {
        if (evt.Event == null || !evt.OriginalPosition.HasValue)
            return;

        var metadata = _replicaHelper.DeserializeObject(evt.Event.Metadata);

        if (!_replicaHelper.IsValidForReplica(
                evt.Event.EventType,
                evt.Event.EventStreamId,
                evt.OriginalPosition.Value,
                _positionRepository.PositionEventType,
                _filterService,
                _streamsToBeExcluded,
                metadata,
                _destinationConnectionBuilder.ConnectionName))
        {
            _logger.LogDebug(
                "{Name}: Skipping event {EventNumber}@{StreamId} - Not valid for replication",
                Name, evt.OriginalEventNumber, evt.OriginalStreamId);
            _lastPosition = evt.OriginalPosition.Value;
            _positionRepository.Set(_lastPosition);
            return;
        }

        var streamId = evt.Event.EventStreamId;
        var eventNumber = evt.Event.EventNumber.ToUInt64();

        // Always ensure the stream is initialized in tracking if not yet present
        if (!_lastWrittenPerStream.ContainsKey(streamId))
        {
            // If we've never seen this stream, and we don't know if it was already written,
            // we defer setting tracking until we actually replicate the first event.
            _logger.LogDebug("{Name}: First time seeing stream {StreamId}. Awaiting successful append before tracking.",
                Name, streamId);
        }

        if (_lastWrittenPerStream.TryGetValue(streamId, out var lastWritten) && eventNumber <= lastWritten)
        {
            _logger.LogDebug("{Name}: Skipping already replicated event {EventNumber}@{StreamId}",
                Name, evt.Event.EventNumber, streamId);
            _lastPosition = evt.OriginalPosition.Value;
            _positionRepository.Set(_lastPosition);
            return;
        }

        var bufferedEvent = new BufferedEvent(
            streamId,
            evt.Event.EventNumber,
            evt.OriginalPosition.Value,
            new EventData(
                evt.Event.EventId,
                evt.Event.EventType,
                evt.Event.Data,
                _replicaHelper.SerializeObject(metadata)),
            evt.Event.Created);

        await _channel.Writer.WriteAsync(bufferedEvent, _cts.Token);
        _logger.LogDebug("{Name} Received event {EventNumber}@{StreamId}", Name, bufferedEvent.EventNumber, streamId);
    }

    //private void TrackNotReplicatedEvent(ResolvedEvent evt)
    //{
    //    var metadata = _replicaHelper.DeserializeObject(evt.Event.Metadata);

    //    if (!_replicaHelper.IsValidForReplica(evt.Event.EventType, evt.Event.EventStreamId, evt.OriginalPosition,
    //            _positionRepository.PositionEventType, _filterService, _streamsToBeExcluded,
    //            metadata, _destinationConnectionBuilder.ConnectionName)) 
    //        return;

    //    var eventNumber = evt.Event.EventNumber.ToUInt64();
    //    var streamId = evt.Event.EventStreamId;

    //    if (!_lastWrittenPerStream.TryGetValue(streamId, out var seen) || seen < eventNumber)
    //    {
    //        _lastWrittenPerStream[streamId] = eventNumber;
    //        _flusherForStreamPositions.Update(streamId, eventNumber);
    //    }
    //}

    private async Task AppendEventAsync(BufferedEvent evt)
    {
        if (_destinationConnection == null)
            throw new InvalidOperationException("Destination connection is not initialized");

        var sw = Stopwatch.StartNew();
        ulong eventNumber = evt.EventNumber.ToUInt64();
        StreamState expectedRevision;

        try
        {
            var readResult = _destinationConnection.ReadStreamAsync(Direction.Backwards, evt.StreamId, StreamPosition.End, 1);
            ResolvedEvent? lastEvent = null;
            await foreach (var e in readResult)
            {
                lastEvent = e;
                break;
            }

            if (lastEvent is not null)
            {
                var lastEventNumber = lastEvent.Value.OriginalEventNumber.ToUInt64();
                if (eventNumber == lastEventNumber + 1)
                {
                    expectedRevision = StreamState.StreamRevision(lastEventNumber);
                }
                else if (_adjustedStartStreams.Contains(evt.StreamId))
                {
                    _logger.LogWarning($"{Name}: Skipping gap in adjusted stream {evt.StreamId}. Got {eventNumber}, expected {lastEventNumber + 1}");
                    await UpdateStreamTrackingAsync(evt);
                    return;
                }
                else if (evt.StreamId.StartsWith("$$"))
                {
                    _logger.LogInformation($"{Name}: Skipping out-of-order system event {eventNumber}@{evt.StreamId} (last was {lastEventNumber})");
                    await UpdateStreamTrackingAsync(evt);
                    return;
                }
                else
                {
                    throw new InvalidOperationException($"Out-of-order event detected in {evt.StreamId}. Expected {lastEventNumber + 1}, got {eventNumber}.");
                }
            }
            else if (_adjustedStartStreams.Contains(evt.StreamId))
            {
                _logger.LogInformation($"{Name}: Stream {evt.StreamId} is empty but adjusted start is enabled — using StreamState.NoStream for event {eventNumber}");
                expectedRevision = StreamState.NoStream;
            }
            else
            {
                if (await IsMaxAgeOrMaxCountStream(evt.StreamId))
                {
                    _logger.LogInformation($"{Name}: Using StreamState.Any for {evt.StreamId} due to $maxCount/$maxAge settings");
                    expectedRevision = StreamState.Any;
                }
                else
                {
                    var isAdjusted = _adjustedStartStreams.Contains(evt.StreamId);
                    _lastWrittenPerStream.TryGetValue(evt.StreamId, out var lastWritten);

                    _logger.LogWarning($"{Name}: Debug — about to append event {eventNumber}@{evt.StreamId}, adjusted={isAdjusted}, lastWritten={lastWritten}");
                    _logger.LogError($"{Name}: Cannot append event {eventNumber} to empty stream {evt.StreamId}. Adjusted? {isAdjusted}");
                    throw new InvalidOperationException($"Cannot append event {eventNumber} to empty stream.");
                }
            }
        }
        catch (StreamNotFoundException)
        {
            if (eventNumber == 0)
            {
                expectedRevision = StreamState.NoStream;
            }
            else if (_adjustedStartStreams.Contains(evt.StreamId))
            {
                _logger.LogDebug($"{Name}: Stream {evt.StreamId} missing but start adjusted — using StreamState.NoStream for event {eventNumber}");
                expectedRevision = StreamState.NoStream;
            }
            else
            {
                throw new InvalidOperationException($"Event {eventNumber} cannot be appended to missing stream.");
            }
        }
       
        if (expectedRevision == StreamState.Any)
        {
            _logger.LogError($"{Name}: Refusing to append with {nameof(StreamState.Any)} to stream {evt.StreamId} (event #{evt.EventNumber}) — this is unsafe for replication.");
            throw new InvalidOperationException("Unsafe replication state: StreamState.Any should not be used during replication.");
        }

        try
        {
            await _destinationConnection.AppendToStreamAsync(evt.StreamId, expectedRevision, [evt.EventData]);
        }
        catch (WrongExpectedVersionException ex)
        {
            if (!_settings.HandleConflicts || !await HandleConflictAsync(evt, ex))
            {
                _logger.LogError($"Conflict handling failed: Stream: {evt.StreamId}, EventNumber: {evt.EventNumber}, Error: {ex.Message}");
                throw new InvalidOperationException("Replication halted due to unrecoverable conflict error.");
            }

            await UpdateStreamTrackingAsync(evt);
            return;
        }

        sw.Stop();

        lock (_latencySamples)
        {
            _latencySamples.Add(sw.ElapsedMilliseconds);
            if (_latencySamples.Count > 100) _latencySamples.RemoveAt(0);
        }

        await UpdateStreamTrackingAsync(evt);
    }

    private async Task<bool> IsMaxAgeOrMaxCountStream(string streamId)
    {
        if (_originConnection == null)
            return false;

        try
        {
            var metadataResult = await _originConnection.GetStreamMetadataAsync(streamId, null, null, _cts.Token);
            var metadata = metadataResult.Metadata;
            if (metadata.MaxCount.HasValue || metadata.MaxAge.HasValue)
            {
                return true;
            }
        }
        catch (StreamNotFoundException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, $"{Name}: Failed to check stream metadata for {streamId}");
        }

        return false;
    }

    private async Task UpdateStreamTrackingAsync(BufferedEvent evt)
    {
        var streamId = evt.StreamId;
        var eventNumber = evt.EventNumber.ToUInt64();

        _lastWrittenPerStream[streamId] = eventNumber;
        _flusherForStreamPositions.Update(streamId, eventNumber);
        _lastPosition = evt.OriginalPosition;
        _positionRepository.Set(_lastPosition);

        Interlocked.Increment(ref _replicatedSinceLastStats);
        Interlocked.Increment(ref _replicatedTotal);

        if (_adjustedStartStreams.Remove(streamId))
            await _adjustedStreamRepository.SaveAsync(_adjustedStartStreams);
    }

    private async Task<ResolvedEvent?> GetLastEventFromAStreamAsync(string streamId)
    {
        if (_destinationConnection == null)
            throw new InvalidOperationException("Destination connection not initialised yet.");
        try
        {
            var result = _destinationConnection.ReadStreamAsync(
                Direction.Backwards,
                streamId,
                StreamPosition.End,
                maxCount: 1,
                resolveLinkTos: false,
                cancellationToken: _cts.Token
            );

            await foreach (var evt in result)
                return evt;

            return null;
        }
        catch (StreamNotFoundException)
        {
            _logger.LogDebug($"{Name}: Stream {streamId} not found in destination - assuming it doesn't exist yet.");
            return null;
        }
    }

    private async Task<bool> HandleConflictAsync(BufferedEvent evt, WrongExpectedVersionException ex)
    {
        if (_destinationConnection == null)
            throw new Exception("Destination connection is not initialized");

        var conflictStreamId = _settings.HandleConflicts
            ? $"$conflicts-from-{_originConnectionBuilder.ConnectionName}-to-{_destinationConnectionBuilder.ConnectionName}"
            : evt.StreamId;

        var metadata = _replicaHelper.DeserializeObject(evt.EventData.Metadata);
        metadata["$error"] = ex.GetBaseException().Message;

        try
        {
            await _destinationConnection.AppendToStreamAsync(conflictStreamId, StreamState.Any, new[]
            {
                new EventData(evt.EventData.EventId, evt.EventData.Type, evt.EventData.Data,
                    _replicaHelper.SerializeObject(metadata))
            });
            Interlocked.Increment(ref _replicatedSinceLastStats);
            Interlocked.Increment(ref _replicatedTotal);
            return true;
        }
        catch (Exception innerEx)
        {
            _logger.LogError(innerEx, $"Conflict append failed: {innerEx.Message}");
            return false;
        }
    }

    private void TimerForStats_Elapsed(object? _, ElapsedEventArgs e)
    {
        var replicatedThisInterval = Interlocked.Exchange(ref _replicatedSinceLastStats, 0);
        var totalReplicated = Interlocked.Read(ref _replicatedTotal);
        var bufferSize = _channel.Reader.Count;
        var bufferRatio = (double)bufferSize / _bufferSize;

        long avgLatency;
        lock (_latencySamples)
        {
            avgLatency = _latencySamples.Count > 0 ? (long)_latencySamples.Average() : 0;
        }

        lock (_adaptiveLock)
        {
            _replicationSamples.Add(replicatedThisInterval);
            if (_replicationSamples.Count > 5)
                _replicationSamples.RemoveAt(0);

            _adaptiveIntervalCounter++;

            if (_adaptiveIntervalCounter >= 5)
            {
                _adaptiveIntervalCounter = 0;
                var average = _replicationSamples.Average();
                var previous = _replicationSamples.Take(4).Average();

                if (average == 0 && previous == 0)
                {
                    _logger.LogInformation($"{Name} adaptive tuning skipped: no replication activity. Current BufferSize={_bufferSize}");
                    return;
                }

                var percentChange = previous == 0 ? 1 : (average - previous) / previous;
                var proposed = _bufferSize;

                if (_settings.AutomaticTuning)
                {
                    var delta = Math.Max(1, (int)Math.Round(_bufferSize * _allowedIncreaseOrDecreaseAmount));

                    if (percentChange >= _significantIncreaseToTriggerIncrease)
                    {
                        // Steady or improving: increase buffer
                        proposed = Math.Min(MaxAllowedBuffer, _bufferSize + delta);
                    }
                    else if (percentChange < _significantRegressionToTriggerDecrease)
                    {
                        // Significant regression: decrease buffer
                        proposed = Math.Max(MinAllowedBuffer, _bufferSize - delta);
                    }

                    if (proposed != _bufferSize)
                        _ = ResizeChannelAsync(proposed);

                    _logger.LogInformation($"{Name} adaptive tuning: prevAvg={previous:F1}, currentAvg={average:F1}, proposed bufferSize={proposed}, change={percentChange:P1}");
                }
                else
                    _logger.LogInformation($"{Name} prevAvg={previous:F1}, currentAvg={average:F1}, bufferSize={proposed}, change={percentChange:P1}");
            }
        }

        _logger.LogInformation($"{Name} stats: replicated {replicatedThisInterval} events, total: {totalReplicated}, buffer: {bufferSize}/{_bufferSize} ({bufferRatio:P0}), latency: {avgLatency}ms");
    }

    private async Task RestartServiceAsync()
    {
        _logger.LogWarning($"{Name} restarting after error...");
        await Task.Delay(1000);
        await StopAsync();
        await StartAsync();
    }

    public IDictionary<string, dynamic> GetStats() => new Dictionary<string, dynamic>
    {
        ["serviceType"] = "crossReplica",
        ["from"] = _originConnectionBuilder.ConnectionName,
        ["to"] = _destinationConnectionBuilder.ConnectionName,
        ["isRunning"] = _started,
        ["lastPosition"] = _lastPosition,
        ["bufferedEvents"] = _channel.Reader.Count,
        ["replicatedTotal"] = Interlocked.Read(ref _replicatedTotal)
    };

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _timerForStats.Dispose();
    }
}