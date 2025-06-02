using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Nodes;
using System.Timers;

namespace Linker;

public class LinkerService : ILinkerService
{
    private readonly ILinkerLogger _logger;
    private readonly IPositionRepository _positionRepository;
    private readonly IFilterService _filterService;
    private readonly bool _handleConflicts;
    private Position _lastPosition;
    //private EventStoreAllCatchUpSubscription _allCatchUpSubscription;
    public string Name { get; }
    private readonly ILinkerConnectionBuilder _connectionBuilderForOrigin;
    private readonly ILinkerConnectionBuilder _connectionBuilderForDestination;
    private KurrentDBClient _destinationConnection;
    private KurrentDBClient _originConnection;
    private bool _started;
    private int _totalProcessedMessagesCurrent;
    private int _totalProcessedMessagesPerSecondsPrevious;
    private int _processedMessagesPerSeconds;
    private readonly System.Timers.Timer _timerForStats;
    private readonly LinkerHelper _replicaHelper;
    private PerfTuneSettings _perfTunedSettings;
    private readonly ConcurrentQueue<BufferedEvent> _internalBuffer = new ConcurrentQueue<BufferedEvent>();
    private readonly System.Timers.Timer _processor;
    private readonly bool _resolveLinkTos;
    private Task? _backgroundSubscriptionTask;
    private readonly CancellationTokenSource _cts = new();

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        IPositionRepository positionRepository, IFilterService filterService, Settings settings, ILinkerLogger logger)
    {
        Ensure.NotNull(originBuilder, nameof(originBuilder));
        Ensure.NotNull(destinationBuilder, nameof(destinationBuilder));
        Ensure.NotNull(positionRepository, nameof(positionRepository));

        _logger = logger;
        Name = $"Replica From-{originBuilder.ConnectionName}-To-{destinationBuilder.ConnectionName}";
        _connectionBuilderForOrigin = originBuilder;
        _connectionBuilderForDestination = destinationBuilder;
        _positionRepository = positionRepository;
        _filterService = filterService;
        _handleConflicts = settings.HandleConflicts;
        _resolveLinkTos = settings.ResolveLinkTos;

        _timerForStats = new System.Timers.Timer(settings.StatsInterval);
        _timerForStats.Elapsed += _timerForStats_Elapsed;

        _processor = new System.Timers.Timer(settings.SynchronisationInterval);
        _processor.Elapsed += Processor_Elapsed;
        _perfTunedSettings =
            new PerfTuneSettings(settings.MaxBufferSize, settings.MaxLiveQueue, settings.ReadBatchSize);
        _replicaHelper = new LinkerHelper();
    }

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        IFilterService filterService, Settings settings, ILinkerLogger logger) : this(
        originBuilder, destinationBuilder, new PositionRepository($"PositionStream-{destinationBuilder.ConnectionName}",
            "PositionUpdated", destinationBuilder.Build()), filterService, settings, logger)
    { }

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        IPositionRepository positionRepository, IFilterService filterService, Settings settings) : this(
        originBuilder, destinationBuilder, positionRepository, filterService, settings,
        new SimpleConsoleLogger(nameof(LinkerService))) { }

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        IFilterService filterService, Settings settings) : this(originBuilder,destinationBuilder, new PositionRepository($"PositionStream-{destinationBuilder.ConnectionName}",
        "PositionUpdated", destinationBuilder.Build()), filterService, settings, new SimpleConsoleLogger(nameof(LinkerService))) { }

    public async Task<bool> Start()
    {
        await _destinationConnection.DisposeAsync();
        _destinationConnection = _connectionBuilderForDestination.Build();
        _logger.Info($@"{_connectionBuilderForDestination.ConnectionName} gRPC connected");

        await _originConnection.DisposeAsync();
        _originConnection = _connectionBuilderForOrigin.Build();
        _backgroundSubscriptionTask = Task.Run(async () =>
        {
            try
            {
                _started = true; //DateTime.UtcNow;
                await SubscribeMeGrpc(_cts.Token);
            }
            catch (OperationCanceledException)
            {
                _logger.Info("gRPC startup cancelled");
            }
        }, _cts.Token);
        _logger.Info($@"{_connectionBuilderForOrigin.ConnectionName} gRPC background subscription started");

        _logger.Info($"{Name} started");
        return true;
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
                    await EventAppeared(evnt);
                    break;
            }
        }
    }

    public async Task<bool> Stop()
    {
        await _destinationConnection.DisposeAsync();
        await _originConnection.DisposeAsync();

        _processor.Stop();
        //_positionRepository.Stop();
        _timerForStats.Stop();
        _totalProcessedMessagesCurrent = 0;
        _started = false;
        _logger.Info($"{Name} stopped");
        return true; // Task.FromResult(true);
    }

    private void Processor_Elapsed(object sender, ElapsedEventArgs e)
    {
        if (_internalBuffer.IsEmpty)
            return;
        try
        {
            _processor.Stop();
            //_allCatchUpSubscription.Stop();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            var eventsToProcess = _internalBuffer.Count;
            var oldPerfSettings = _perfTunedSettings.Clone() as PerfTuneSettings;
            ProcessQueueAndWaitAll();
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            _logger.Debug($"{Name} Replicated '{eventsToProcess}' events in {elapsedMs}ms");
            _perfTunedSettings = _replicaHelper.OptimizeSettings(elapsedMs, _perfTunedSettings);
            if (!_perfTunedSettings.Equals(oldPerfSettings))
            {
                _logger.Debug($"{Name} Old PerfSettings: {oldPerfSettings}");
                _logger.Debug($"{Name} New PerfSettings: {_perfTunedSettings}");
            }
            SubscribeMeGrpc(new CancellationToken()); // TODO how do we await this?
            _processor.Start();
        }
        catch (Exception exception)
        {
            _logger.Error($"Error while Processor_Elapsed: {exception.GetBaseException().Message}");
            Stop();
            Start();
        }
    }

    private void _timerForStats_Elapsed(object sender, ElapsedEventArgs e)
    {
        var current = _totalProcessedMessagesCurrent;
        _processedMessagesPerSeconds = _replicaHelper.CalculateSpeed(current, _totalProcessedMessagesPerSecondsPrevious);
        _totalProcessedMessagesPerSecondsPrevious = current;
    }

    public IDictionary<string, dynamic> GetStats()
    {
        return new Dictionary<string, dynamic>
        {
            {"serviceType", "crossReplica"},
            {"from", _connectionBuilderForOrigin.ConnectionName },
            {"to", _connectionBuilderForDestination.ConnectionName },
            {"isRunning", _started},
            {"lastPosition", _lastPosition},
            {"messagesPerSeconds", _processedMessagesPerSeconds}
        };
    }

    private Task EventAppeared(ResolvedEvent resolvedEvent)
    {
        try
        {
            if (resolvedEvent.Event == null || !resolvedEvent.OriginalPosition.HasValue)
                return Task.CompletedTask;
            return EventAppeared(new BufferedEvent(resolvedEvent.Event.EventStreamId,
                resolvedEvent.Event.EventNumber, resolvedEvent.OriginalPosition.Value,
                new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType, resolvedEvent.Event.Data, resolvedEvent.Event.Metadata),
                resolvedEvent.Event.Created));
        }
        catch (Exception e)
        {
            _logger.Error($"Error during Cross Replica {e.GetBaseException().Message}");
        }

        return Task.CompletedTask;
    }

    private Task EventAppeared(BufferedEvent resolvedEvent)
    {
        if (!_replicaHelper.IsValidForReplica(resolvedEvent.EventData.Type, resolvedEvent.StreamId,
                resolvedEvent.OriginalPosition, _positionRepository.PositionEventType, _filterService))
        {
            _lastPosition = resolvedEvent.OriginalPosition;
            return Task.CompletedTask;
        }

        var origin = _connectionBuilderForOrigin.ConnectionName;
        if (!_replicaHelper.TryProcessMetadata(resolvedEvent.StreamId, resolvedEvent.EventNumber,
                resolvedEvent.Created, origin,
                _replicaHelper.DeserializeObject(resolvedEvent.EventData.Metadata) ?? new Dictionary<string, JsonNode?>(),
                out IDictionary<string, JsonNode> enrichedMetadata))
        {
            _lastPosition = resolvedEvent.OriginalPosition;
            _positionRepository.Set(_lastPosition);
            return Task.CompletedTask;
        }

        // Back-pressure
        // TODO implement back-pressure using the new kurrentdb client
        //if (_internalBuffer.Count >= _perfTunedSettings.MaxBufferSize)
        //    _allCatchUpSubscription?.Stop();

        _internalBuffer.Enqueue(new BufferedEvent(resolvedEvent.StreamId, resolvedEvent.EventNumber,
            resolvedEvent.OriginalPosition, new EventData(resolvedEvent.EventData.EventId,
                resolvedEvent.EventData.Type, resolvedEvent.EventData.Data,
                _replicaHelper.SerializeObject(enrichedMetadata)), resolvedEvent.Created));

        return Task.CompletedTask;
    }

    private void ProcessQueueAndWaitAll()
    {
        var tasks = new List<Task>();
        try
        {
            BufferedEvent ev;
            while (_internalBuffer.TryDequeue(out ev))
            {
                var ev1 = ev;
                tasks.Add(_destinationConnection
                    .AppendToStreamAsync(ev.StreamId, ev.EventNumber.ToUInt64() - 1, new[] { ev.EventData }).ContinueWith(a =>
                    {
                        if (a.Exception?.InnerException is WrongExpectedVersionException)
                        {
                            if (!TryHandleConflicts(ev1.EventData.EventId.ToGuid(), ev1.EventData.Type, ev1.StreamId,
                                    ev1.EventData.Data.ToArray(),
                                    (WrongExpectedVersionException)a.Exception.InnerException,
                                    _replicaHelper.DeserializeObject(ev1.EventData.Metadata)))
                            {
                                _logger.Warn($"Error while handling conflicts: {a.Exception.InnerException.Message}");
                            }
                        }
                    }, TaskContinuationOptions.OnlyOnFaulted).ContinueWith(a =>
                    {
                        _lastPosition = ev1.OriginalPosition;
                        _positionRepository.Set(_lastPosition);
                    }, TaskContinuationOptions.NotOnFaulted));
            }
            Task.WaitAll(tasks.ToArray());
        }
        catch (AggregateException ae)
        {
            foreach (var aeInnerException in ae.InnerExceptions)
            {
                _logger.Error(
                    $"Error while processing georeplica queue: {aeInnerException.GetBaseException().Message}");
            }
        }
    }

    private bool TryHandleConflicts(Guid eventId, string eventType, string eventStreamId, byte[] data,
        WrongExpectedVersionException exception, IDictionary<string, JsonNode> enrichedMetadata)
    {
        if (exception == null)
            return false;

        try
        {
            enrichedMetadata.Add("$error", exception.GetBaseException().Message);
            if (_handleConflicts)
            {
                var conflictStreamId = $"$conflicts-from-{_connectionBuilderForOrigin.ConnectionName}-to-{_connectionBuilderForDestination.ConnectionName}";
                _destinationConnection.AppendToStreamAsync(conflictStreamId, StreamState.Any, new[]
                {
                    new EventData(Uuid.FromGuid(eventId), eventType, data, _replicaHelper.SerializeObject(enrichedMetadata))
                }).Wait();
            }
            else
            {
                _destinationConnection.AppendToStreamAsync(eventStreamId, StreamState.Any, new[]
                {
                    new EventData(Uuid.FromGuid(eventId), eventType, data, _replicaHelper.SerializeObject(enrichedMetadata))
                }).Wait();
            }

            return true;
        }
        catch (Exception e)
        {
            _logger.Error($"Error while TryHandleConflicts {e.GetBaseException().Message}");
            return false;
        }
    }
}