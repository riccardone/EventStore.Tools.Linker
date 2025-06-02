using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using System.Timers;

namespace Linker;

public class LinkerService : ILinkerService, IAsyncDisposable
{
    private readonly ILinkerLogger _logger;
    private readonly IPositionRepository _positionRepository;
    private readonly IFilterService _filterService;
    private readonly bool _handleConflicts;
    private Position _lastPosition;
    public string Name { get; }

    private readonly ILinkerConnectionBuilder _originConnectionBuilder;
    private readonly ILinkerConnectionBuilder _destinationConnectionBuilder;
    private KurrentDBClient _originConnection;
    private KurrentDBClient _destinationConnection;

    private readonly ConcurrentQueue<BufferedEvent> _internalBuffer = new();
    private readonly System.Timers.Timer _timerForStats;
    private readonly System.Timers.Timer _processorTimer;

    private readonly LinkerHelper _replicaHelper;
    private PerfTuneSettings _perfTuneSettings;
    private readonly CancellationTokenSource _cts = new();
    private Task? _subscriptionTask;
    private bool _started;

    public LinkerService(
        ILinkerConnectionBuilder originBuilder,
        ILinkerConnectionBuilder destinationBuilder,
        IPositionRepository positionRepository,
        IFilterService filterService,
        Settings settings,
        ILinkerLogger logger)
    {
        Ensure.NotNull(originBuilder, nameof(originBuilder));
        Ensure.NotNull(destinationBuilder, nameof(destinationBuilder));
        Ensure.NotNull(positionRepository, nameof(positionRepository));

        _logger = logger;
        Name = $"Replica From-{originBuilder.ConnectionName}-To-{destinationBuilder.ConnectionName}";

        _originConnectionBuilder = originBuilder;
        _destinationConnectionBuilder = destinationBuilder;

        _positionRepository = positionRepository;
        _filterService = filterService;
        _handleConflicts = settings.HandleConflicts;

        _perfTuneSettings = new PerfTuneSettings(settings.MaxBufferSize, settings.MaxLiveQueue, settings.ReadBatchSize);
        _replicaHelper = new LinkerHelper();

        _timerForStats = new System.Timers.Timer(settings.StatsInterval);
        _timerForStats.Elapsed += TimerForStats_Elapsed;

        _processorTimer = new System.Timers.Timer(settings.SynchronisationInterval);
        _processorTimer.Elapsed += ProcessorTimer_Elapsed;
    }

    public async Task<bool> Start()
    {
        await Stop(); // Ensure clean startup

        _destinationConnection = _destinationConnectionBuilder.Build();
        _originConnection = _originConnectionBuilder.Build();

        _started = true;

        _subscriptionTask = SubscribeMeGrpc(_cts.Token);

        _timerForStats.Start();
        _processorTimer.Start();

        _logger.Info($"{Name} started.");

        return true;
    }

    public async Task<bool> Stop()
    {
        if (!_started) return true;

        _cts.Cancel();

        _timerForStats.Stop();
        _processorTimer.Stop();

        if (_subscriptionTask != null)
        {
            try { await _subscriptionTask; }
            catch (OperationCanceledException) { /* expected */ }
        }

        await DisposeConnectionsAsync();

        _started = false;
        _logger.Info($"{Name} stopped.");
        return true;
    }

    private async Task DisposeConnectionsAsync()
    {
        if (_originConnection != null)
            await _originConnection.DisposeAsync();
        if (_destinationConnection != null)
            await _destinationConnection.DisposeAsync();
    }

    private async Task SubscribeMeGrpc(CancellationToken ctsToken)
    {
        await using var subscription = _originConnection.SubscribeToAll(FromAll.Start, cancellationToken: ctsToken,
            resolveLinkTos: false, filterOptions: new SubscriptionFilterOptions(EventTypeFilter.ExcludeSystemEvents()));
        await foreach (var message in subscription.Messages)
        {
            switch (message)
            {
                case StreamMessage.Event(var evnt):
                    _logger.Debug($"Received event {evnt.OriginalEventNumber}@{evnt.OriginalStreamId}");
                    await HandleEventAsync(evnt);
                    break;
            }
        }
    }

    private async Task HandleEventAsync(ResolvedEvent evt)
    {
        if (evt.Event == null || !evt.OriginalPosition.HasValue)
            return;

        var bufferedEvent = new BufferedEvent(
            evt.Event.EventStreamId,
            evt.Event.EventNumber,
            evt.OriginalPosition.Value,
            new EventData(evt.Event.EventId, evt.Event.EventType, evt.Event.Data, evt.Event.Metadata),
            evt.Event.Created);

        if (!_replicaHelper.IsValidForReplica(bufferedEvent.EventData.Type, bufferedEvent.StreamId,
                bufferedEvent.OriginalPosition, _positionRepository.PositionEventType, _filterService))
        {
            _lastPosition = bufferedEvent.OriginalPosition;
            return;
        }

        if (!_replicaHelper.TryProcessMetadata(bufferedEvent.StreamId, bufferedEvent.EventNumber, bufferedEvent.Created,
            _originConnectionBuilder.ConnectionName,
            _replicaHelper.DeserializeObject(bufferedEvent.EventData.Metadata) ?? new Dictionary<string, JsonNode?>(),
            out var enrichedMetadata))
        {
            _lastPosition = bufferedEvent.OriginalPosition;
            _positionRepository.Set(_lastPosition);
            return;
        }

        // Implement proper back-pressure logic here if needed
        _internalBuffer.Enqueue(new BufferedEvent(bufferedEvent.StreamId, bufferedEvent.EventNumber, bufferedEvent.OriginalPosition,
            new EventData(bufferedEvent.EventData.EventId, bufferedEvent.EventData.Type,
                bufferedEvent.EventData.Data, _replicaHelper.SerializeObject(enrichedMetadata)), bufferedEvent.Created));
    }

    private async void ProcessorTimer_Elapsed(object sender, ElapsedEventArgs e)
    {
        _processorTimer.Stop();
        try
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();
            await ProcessBufferedEventsAsync();
            watch.Stop();

            _logger.Debug($"Processed {_internalBuffer.Count} events in {watch.ElapsedMilliseconds}ms");
        }
        catch (Exception ex)
        {
            _logger.Error($"Processing error: {ex.GetBaseException().Message}");
            await RestartServiceAsync();
        }
        finally
        {
            _processorTimer.Start();
        }
    }

    private async Task ProcessBufferedEventsAsync()
    {
        var tasks = new List<Task>();

        while (_internalBuffer.TryDequeue(out var evt))
        {
            tasks.Add(AppendEventAsync(evt));
        }

        await Task.WhenAll(tasks);
    }

    private async Task AppendEventAsync(BufferedEvent evt)
    {
        try
        {
            await _destinationConnection.AppendToStreamAsync(evt.StreamId, evt.EventNumber.ToUInt64() - 1, new[] { evt.EventData });
            _lastPosition = evt.OriginalPosition;
            _positionRepository.Set(_lastPosition);
        }
        catch (WrongExpectedVersionException ex)
        {
            if (!_handleConflicts || !await HandleConflictAsync(evt, ex))
                _logger.Warn($"Conflict handling failed: {ex.Message}");
        }
    }

    private async Task<bool> HandleConflictAsync(BufferedEvent evt, WrongExpectedVersionException ex)
    {
        var conflictStreamId = _handleConflicts
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
            return true;
        }
        catch (Exception innerEx)
        {
            _logger.Error($"Conflict append failed: {innerEx.Message}");
            return false;
        }
    }

    private void TimerForStats_Elapsed(object sender, ElapsedEventArgs e)
    {
        // Implement your stats logic here
    }

    private async Task RestartServiceAsync()
    {
        await Stop();
        await Start();
    }

    public IDictionary<string, dynamic> GetStats() => new Dictionary<string, dynamic>
    {
        ["serviceType"] = "crossReplica",
        ["from"] = _originConnectionBuilder.ConnectionName,
        ["to"] = _destinationConnectionBuilder.ConnectionName,
        ["isRunning"] = _started,
        ["lastPosition"] = _lastPosition
    };

    public async ValueTask DisposeAsync()
    {
        await Stop();
        _timerForStats.Dispose();
        _processorTimer.Dispose();
    }
}
