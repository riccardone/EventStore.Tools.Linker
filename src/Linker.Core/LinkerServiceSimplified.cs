// LinkerSimplified.cs (Simplified with global position and adaptive buffering)
using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace Linker.Core;

public class LinkerServiceSimplified : ILinkerService, IAsyncDisposable
{
    private readonly Microsoft.Extensions.Logging.ILogger _logger;
    private readonly IPositionRepository _positionRepository;
    private readonly Settings _settings;
    private readonly LinkerHelper _replicaHelper;
    private readonly IFilterService? _filterService;

    private readonly ILinkerConnectionBuilder _originConnectionBuilder;
    private readonly ILinkerConnectionBuilder _destinationConnectionBuilder;
    private KurrentDBClient? _originConnection;
    private KurrentDBClient? _destinationConnection;

    private Channel<BufferedEvent> _channel;
    private CancellationTokenSource _cts = new();
    private Task? _processingTask;
    private Task? _subscriptionTask;
    private bool _started;

    private Position _lastPosition;
    private long _replicatedTotal;

    private int _bufferSize;
    private readonly object _adaptiveLock = new();
    private readonly List<long> _latencySamples = new();
    private readonly List<long> _replicationSamples = new();
    private int _adaptiveIntervalCounter;

    private readonly System.Timers.Timer _timerForStats;

    private readonly double _currentBackpressureThreshold = 0.8;
    private readonly decimal _allowedChange = 0.15m;
    private readonly double _increaseThreshold = -0.1;
    private readonly double _decreaseThreshold = -0.1;
    public const int MaxAllowedBuffer = 1000;
    public const int MinAllowedBuffer = 1;

    public string Name { get; }

    public LinkerServiceSimplified(
        ILinkerConnectionBuilder originBuilder,
        ILinkerConnectionBuilder destinationBuilder,
        IPositionRepository positionRepository,
        IFilterService? filterService,
        Settings settings,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger(nameof(LinkerServiceSimplified));
        Name = $"From-{originBuilder.ConnectionName}-To-{destinationBuilder.ConnectionName}";

        _originConnectionBuilder = originBuilder;
        _destinationConnectionBuilder = destinationBuilder;
        _positionRepository = positionRepository;
        _filterService = filterService;
        _settings = settings;
        _bufferSize = Math.Clamp(settings.BufferSize, MinAllowedBuffer, MaxAllowedBuffer);
        _replicaHelper = new LinkerHelper();

        _channel = CreateNewChannel(_bufferSize);

        _timerForStats = new System.Timers.Timer(3000);
        _timerForStats.Elapsed += TimerForStats_Elapsed;
    }

    private Channel<BufferedEvent> CreateNewChannel(int size) =>
        Channel.CreateBounded<BufferedEvent>(new BoundedChannelOptions(size)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = false,
            SingleReader = true
        });

    public async Task<bool> StartAsync()
    {
        if (_started) return true;

        if (!_positionRepository.TryGet(out _lastPosition))
            _lastPosition = Position.Start;

        _originConnection = _originConnectionBuilder.Build();
        _destinationConnection = _destinationConnectionBuilder.Build();

        _cts = new CancellationTokenSource();
        _channel = CreateNewChannel(_bufferSize);

        _processingTask = Task.Run(ProcessChannelEventsAsync);
        _subscriptionTask = SubscribeGrpc(_cts.Token);

        _timerForStats.Start();
        _started = true;

        _logger.LogInformation($"{Name} started from position {_lastPosition}.");
        return true;
    }

    public async Task<bool> StopAsync()
    {
        if (!_started) return true;

        await _cts.CancelAsync();
        _channel.Writer.TryComplete();
        _timerForStats.Stop();

        if (_subscriptionTask != null) await _subscriptionTask;
        if (_processingTask != null) await _processingTask;

        if (_originConnection != null) await _originConnection.DisposeAsync();
        if (_destinationConnection != null) await _destinationConnection.DisposeAsync();

        _started = false;
        _logger.LogInformation($"{Name} stopped.");
        return true;
    }

    private async Task SubscribeGrpc(CancellationToken token)
    {
        if (_originConnection == null) throw new InvalidOperationException("Origin not initialized");

        await foreach (var message in _originConnection
            .SubscribeToAll(start: FromAll.After(_lastPosition), cancellationToken: token,
                resolveLinkTos: _settings.ResolveLinkTos,
                filterOptions: new SubscriptionFilterOptions(KurrentDB.Client.EventTypeFilter.RegularExpression("^(\\$metadata|[^\\$].*)")))
            .Messages.WithCancellation(token))
        {
            if (message is StreamMessage.Event(var evnt))
                await HandleEventAsync(evnt);
        }
    }

    private async Task HandleEventAsync(ResolvedEvent evt)
    {
        if (evt.Event == null || !evt.OriginalPosition.HasValue) return;

        var metadata = _replicaHelper.DeserializeObject(evt.Event.Metadata);

        if (!_replicaHelper.IsValidForReplica(
                evt.Event.EventType,
                evt.Event.EventStreamId,
                evt.OriginalPosition.Value,
                _positionRepository.PositionEventType,
                _filterService,
                [],
                metadata,
                _destinationConnectionBuilder.ConnectionName))
            return;

        if (!_replicaHelper.TryProcessMetadata(
                evt.Event.EventStreamId,
                evt.Event.EventNumber,
                evt.Event.Created,
                _originConnectionBuilder.ConnectionName,
                metadata,
                out var enrichedMetadata))
            return;

        var bufferedEvent = new BufferedEvent(
            evt.Event.EventStreamId,
            evt.Event.EventNumber,
            evt.OriginalPosition.Value,
            new EventData(
                evt.Event.EventId,
                evt.Event.EventType,
                evt.Event.Data,
                _replicaHelper.SerializeObject(enrichedMetadata)),
            evt.Event.Created);

        var bufferCount = _channel.Reader.Count;
        if (bufferCount >= _bufferSize * _currentBackpressureThreshold)
        {
            _logger.LogDebug($"{Name}: Backpressure active at {bufferCount}/{_bufferSize}");
            await Task.Delay(100, _cts.Token);
        }

        await _channel.Writer.WriteAsync(bufferedEvent, _cts.Token);
    }

    private async Task ProcessChannelEventsAsync()
    {
        if (_destinationConnection == null) throw new InvalidOperationException("Destination not initialized");

        await foreach (var evt in _channel.Reader.ReadAllAsync(_cts.Token))
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await _destinationConnection.AppendToStreamAsync(evt.StreamId, StreamState.Any, [evt.EventData]);
                _lastPosition = evt.OriginalPosition;
                _positionRepository.Set(_lastPosition);
                Interlocked.Increment(ref _replicatedTotal);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"{Name}: Failed to replicate {evt.EventNumber}@{evt.StreamId}");
            }
            finally
            {
                sw.Stop();
                lock (_latencySamples)
                {
                    _latencySamples.Add(sw.ElapsedMilliseconds);
                    if (_latencySamples.Count > 100) _latencySamples.RemoveAt(0);
                }
            }
        }
    }

    private void TimerForStats_Elapsed(object? _, System.Timers.ElapsedEventArgs e)
    {
        long avgLatency;
        lock (_latencySamples)
        {
            avgLatency = _latencySamples.Count > 0 ? (long)_latencySamples.Average() : 0;
        }

        var bufferSize = _channel.Reader.Count;
        var bufferRatio = (double)bufferSize / _bufferSize;
        _replicationSamples.Add(bufferSize);
        if (_replicationSamples.Count > 5) _replicationSamples.RemoveAt(0);
        _adaptiveIntervalCounter++;

        if (_adaptiveIntervalCounter >= 5 && _settings.AutomaticTuning)
        {
            _adaptiveIntervalCounter = 0;
            var currentAvg = _replicationSamples.Average();
            var previousAvg = _replicationSamples.Take(4).Average();
            var change = previousAvg == 0 ? 1 : (currentAvg - previousAvg) / previousAvg;

            int newSize = _bufferSize;
            int delta = Math.Max(1, (int)Math.Round(_bufferSize * (double)_allowedChange));

            if (change >= _increaseThreshold)
                newSize = Math.Min(MaxAllowedBuffer, _bufferSize + delta);
            else if (change < _decreaseThreshold)
                newSize = Math.Max(MinAllowedBuffer, _bufferSize - delta);

            if (newSize != _bufferSize)
                _ = ResizeChannelAsync(newSize);
        }

        _logger.LogInformation($"{Name} stats: buffer: {bufferSize}/{_bufferSize} ({bufferRatio:P0}), latency: {avgLatency}ms, total: {_replicatedTotal}");
    }

    private async Task ResizeChannelAsync(int newSize)
    {
        _logger.LogInformation($"{Name}: Resizing buffer from {_bufferSize} to {newSize}");

        await _cts.CancelAsync();
        _channel.Writer.TryComplete();

        if (_subscriptionTask is not null) await _subscriptionTask;
        if (_processingTask is not null) await _processingTask;

        var buffered = new List<BufferedEvent>();
        while (_channel.Reader.TryRead(out var evt))
            buffered.Add(evt);

        _cts.Dispose();
        _cts = new CancellationTokenSource();
        _bufferSize = newSize;
        _channel = CreateNewChannel(_bufferSize);

        foreach (var evt in buffered)
            await _channel.Writer.WriteAsync(evt);

        _processingTask = Task.Run(ProcessChannelEventsAsync);
        _subscriptionTask = SubscribeGrpc(_cts.Token);
    }

    public async ValueTask DisposeAsync() => await StopAsync();

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
}
