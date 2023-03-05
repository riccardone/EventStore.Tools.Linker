﻿using System.Collections.Concurrent;
using System.Timers;
using EventStore.Client;
using EventStore.PositionRepository;

namespace Linker;

public class LinkerService : ILinkerService
{
    private readonly ILinkerLogger _logger;
    private readonly ILinkerSubscriber _linkerSubscriber;
    private readonly ILinkerPositionRepository _positionRepository;
    private readonly IFilterService _filterService;
    private readonly bool _handleConflicts;
    private Position _lastPosition;
    private ILinkerAllCatchUpSubscription _allCatchUpSubscription;
    public string Name { get; }
    private readonly ILinkerConnectionBuilder _connectionBuilderForOrigin;
    private readonly ILinkerConnectionBuilder _connectionBuilderForDestination;
    private ILinkerConnection _destinationConnection;
    private ILinkerConnection _originConnection;
    private bool _started;
    private int _totalProcessedMessagesCurrent;
    private int _totalProcessedMessagesPerSecondsPrevious;
    private int _processedMessagesPerSeconds;
    private readonly System.Timers.Timer _timerForStats;
    private readonly LinkerHelper _replicaHelper;
    private PerfTuneSettings _perfTunedSettings;
    private readonly ConcurrentQueue<BufferedEvent> _internalBuffer = new();
    private readonly System.Timers.Timer _processor;
    private readonly bool _resolveLinkTos;

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        ILinkerPositionRepository positionRepository, IFilterService filterService, LinkerSettings settings, ILinkerLogger logger, ILinkerSubscriber linkerSubscriber)
    {
        Ensure.NotNull(originBuilder, nameof(originBuilder));
        Ensure.NotNull(destinationBuilder, nameof(destinationBuilder));
        Ensure.NotNull(positionRepository, nameof(positionRepository));

        _logger = logger;
        _linkerSubscriber = linkerSubscriber;
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
        _replicaHelper = new LinkerHelper(null); // TODO
    }

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        ILinkerPositionRepository positionRepository, IFilterService filterService, LinkerSettings settings, ILinkerSubscriber linkerSubscriber) : this(
        originBuilder, destinationBuilder, positionRepository, filterService, settings,
        new SimpleConsoleLogger(nameof(LinkerService)), linkerSubscriber)
    { }

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        IFilterService filterService, LinkerSettings settings, ILinkerSubscriber linkerSubscriber, string connectionString) : this(originBuilder, destinationBuilder, new LinkerPositionRepositoryTcp($"PositionStream-{destinationBuilder.ConnectionName}",
        "PositionUpdated", new ConnectionBuilder(new Uri(connectionString), EventStore.ClientAPI.ConnectionSettings.Default, $"{destinationBuilder.ConnectionName}-positionrepository")), filterService, settings, new SimpleConsoleLogger(nameof(LinkerService)), linkerSubscriber)
    { }

    public LinkerService(ILinkerConnectionBuilder originBuilder, ILinkerConnectionBuilder destinationBuilder,
        IFilterService filterService, LinkerSettings settings, string connectionString) : this(originBuilder, destinationBuilder, new LinkerPositionRepositoryTcp($"PositionStream-{destinationBuilder.ConnectionName}",
        "PositionUpdated", new ConnectionBuilder(new Uri(connectionString), EventStore.ClientAPI.ConnectionSettings.Default, $"{destinationBuilder.ConnectionName}-positionrepository")), filterService, settings, new SimpleConsoleLogger(nameof(LinkerService)), new LinkerSubscriber())
    { }

    public async Task<bool> Start()
    {
        // TODO
        //_destinationConnection?.Close();
        //_destinationConnection = _connectionBuilderForDestination.Build();
        //_destinationConnection.ErrorOccurred += DestinationConnection_ErrorOccurred;
        //_destinationConnection.Disconnected += DestinationConnection_Disconnected;
        //_destinationConnection.AuthenticationFailed += DestinationConnection_AuthenticationFailed;
        //_destinationConnection.Connected += DestinationConnection_Connected;
        //_destinationConnection.Reconnecting += _destinationConnection_Reconnecting;
        _destinationConnection = _connectionBuilderForDestination.Build();
        _destinationConnection.Connected += DestinationConnection_Connected;
        await _destinationConnection.Start();

        //_originConnection?.Close();
        //_originConnection = _connectionBuilderForOrigin.Build();
        //_originConnection.ErrorOccurred += OriginConnection_ErrorOccurred;
        //_originConnection.Disconnected += OriginConnection_Disconnected;
        //_originConnection.AuthenticationFailed += OriginConnection_AuthenticationFailed;
        //_originConnection.Connected += OriginConnection_Connected;
        //_originConnection.Reconnecting += _originConnection_Reconnecting;
        _originConnection = _connectionBuilderForOrigin.Build();
        await _originConnection.Start();

        _logger.Info($"{Name} started");
        return true;
    }

    private void DestinationConnection_Connected(object sender, EventArgs e)
    {
        // _logger.Debug($"SubscriberConnection Connected to: {e.RemoteEndPoint}");
        _positionRepository.StartAsync().Wait();
        _lastPosition = _positionRepository.Get();
        Subscribe(_lastPosition);
        _processor.Enabled = true;
        _processor.Start();
        _timerForStats.Enabled = true;
        _timerForStats.Start();
        _started = true;
    }

    public Task<bool> Stop()
    {
        // TODO
        //_destinationConnection.ErrorOccurred -= DestinationConnection_ErrorOccurred;
        //_destinationConnection.Disconnected -= DestinationConnection_Disconnected;
        //_destinationConnection.AuthenticationFailed += DestinationConnection_AuthenticationFailed;
        _destinationConnection.Connected -= DestinationConnection_Connected;
        //_destinationConnection.Reconnecting -= _destinationConnection_Reconnecting;

        //_originConnection.ErrorOccurred -= OriginConnection_ErrorOccurred;
        //_originConnection.Disconnected -= OriginConnection_Disconnected;
        //_originConnection.AuthenticationFailed -= OriginConnection_AuthenticationFailed;
        //_originConnection.Connected -= OriginConnection_Connected;
        //_originConnection.Reconnecting -= _originConnection_Reconnecting;

        _processor.Stop();
        //_allCatchUpSubscription?.Stop();
        //_destinationConnection?.Close();
        //_originConnection?.Close();
        _positionRepository.Stop();
        _timerForStats.Stop();
        _totalProcessedMessagesCurrent = 0;
        _started = false;
        _logger.Info($"{Name} stopped");
        return Task.FromResult(true);
    }

    private void Processor_Elapsed(object sender, ElapsedEventArgs e)
    {
        if (_internalBuffer.IsEmpty)
            return;
        try
        {
            _processor.Stop();
            _allCatchUpSubscription.Stop();
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
            Subscribe(_lastPosition);
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

    //private void _originConnection_Reconnecting(object sender, ClientReconnectingEventArgs e)
    //{
    //    _logger.Debug($"{Name} Origin Reconnecting...");
    //}

    //private void _destinationConnection_Reconnecting(object sender, ClientReconnectingEventArgs e)
    //{
    //    _logger.Debug($"{Name} Destination Reconnecting...");
    //}

    //private void OriginConnection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
    //{
    //    _logger.Warn($"AuthenticationFailed to {_originConnection.ConnectionName}: {e.Reason}");
    //}

    //private void OriginConnection_Connected(object sender, ClientConnectionEventArgs e)
    //{
    //    _logger.Debug($"SubscriberConnection Connected to: {e.RemoteEndPoint}");
    //    _positionRepository.Start();
    //    _lastPosition = _positionRepository.Get();
    //    Subscribe(_lastPosition);
    //    _processor.Enabled = true;
    //    _processor.Start();
    //    _timerForStats.Enabled = true;
    //    _timerForStats.Start();
    //    _started = true;
    //}

    //private void OriginConnection_Disconnected(object sender, ClientConnectionEventArgs e)
    //{
    //    _logger.Warn($"{Name} disconnected from {e.RemoteEndPoint}");
    //    Stop();
    //    Start();
    //}

    //private void OriginConnection_ErrorOccurred(object sender, ClientErrorEventArgs e)
    //{
    //    _logger.Error(e.Exception.GetBaseException().Message);
    //    Stop();
    //    Start();
    //}

    //private void DestinationConnection_Connected(object sender, ClientConnectionEventArgs e)
    //{
    //    _logger.Debug($"{_destinationConnection.ConnectionName} Connected to: {e.RemoteEndPoint}");
    //}

    //private void DestinationConnection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
    //{
    //    _logger.Warn($"AuthenticationFailed with {_destinationConnection.ConnectionName}: {e.Reason}");
    //    if (!_started) return;
    //    _logger.Warn($"Restart {Name}...");
    //    Stop();
    //    Start();
    //}

    //private void DestinationConnection_Disconnected(object sender, ClientConnectionEventArgs e)
    //{
    //    _logger.Warn($"{_destinationConnection.ConnectionName} disconnected from '{e.RemoteEndPoint}'");
    //    Stop();
    //    Start();
    //}

    //private void DestinationConnection_ErrorOccurred(object sender, ClientErrorEventArgs e)
    //{
    //    _logger.Error($"Error with {_destinationConnection.ConnectionName}: {e.Exception.GetBaseException().Message}");
    //}

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

    private void Subscribe(Position position)
    {
        // TODO
        var client = new EventStoreClient();
        client.SubscribeToAllAsync()
        _allCatchUpSubscription = _originConnection.SubscribeToAllFrom(position,
            BuildSubscriptionSettings(), EventAppeared, LiveProcessingStarted, SubscriptionDropped);
        _logger.Debug($"Subscribed from position: {position}");
    }

    //private void SubscriptionDropped(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, SubscriptionDropReason subscriptionDropReason, Exception arg3)
    //{
    //    if (!_started)
    //        return;
    //    if (subscriptionDropReason == SubscriptionDropReason.UserInitiated)
    //        return;
    //    if (arg3?.GetBaseException() is ObjectDisposedException)
    //        return;
    //    _logger.Warn($"Cross Replica Resubscribing... (reason: {subscriptionDropReason})");
    //    if (arg3 != null)
    //        _logger.Error($"exception: {arg3.GetBaseException().Message}");
    //    _lastPosition = _positionRepository.Get();
    //    Subscribe(_lastPosition);
    //}

    private LinkerCatchUpSubscriptionSettings BuildSubscriptionSettings()
    {
        throw new NotImplementedException();
        // TODO
        //return new LinkerCatchUpSubscriptionSettings(_perfTunedSettings.MaxLiveQueue, _perfTunedSettings.ReadBatchSize,
        //    LinkerCatchUpSubscriptionSettings.Default.VerboseLogging, _resolveLinkTos);
    }

    private void LiveProcessingStarted(ILinkerAllCatchUpSubscription eventStoreCatchUpSubscription)
    {
        _logger.Debug($"'{Name}' Started");
    }

    public Task EventAppeared(ILinkerAllCatchUpSubscription eventStoreCatchUpSubscription, LinkerResolvedEvent resolvedEvent)
    {
        try
        {
            if (resolvedEvent.Event == null || resolvedEvent.OriginalPosition == null)
                return Task.CompletedTask;
            return EventAppeared(new BufferedEvent(resolvedEvent.Event.EventStreamId,
                resolvedEvent.Event.EventNumber, resolvedEvent.OriginalPosition,
                new LinkerEventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType,
                    resolvedEvent.Event.IsJson, resolvedEvent.Event.Data, resolvedEvent.Event.Metadata),
                resolvedEvent.Event.Created));
        }
        catch (Exception e)
        {
            _logger.Error($"Error during Cross Replica {e.GetBaseException().Message}");
        }

        return Task.CompletedTask;
    }

    internal Task EventAppeared(BufferedEvent resolvedEvent)
    {
        if (!_replicaHelper.IsValidForReplica(resolvedEvent.EventData.EventType, resolvedEvent.StreamId,
                resolvedEvent.OriginalPosition, _positionRepository.PositionEventType, _filterService))
        {
            _lastPosition = resolvedEvent.OriginalPosition;
            return Task.CompletedTask;
        }

        IDictionary<string, dynamic> enrichedMetadata;
        var origin = _connectionBuilderForOrigin.ConnectionName;
        if (!_replicaHelper.TryProcessMetadata(resolvedEvent.StreamId, resolvedEvent.EventNumber,
                resolvedEvent.Created, origin,
                _replicaHelper.DeserializeObject(resolvedEvent.EventData.Metadata) ?? new Dictionary<string, dynamic>(),
                out enrichedMetadata))
        {
            _lastPosition = resolvedEvent.OriginalPosition;
            _positionRepository.Set(_lastPosition);
            return Task.CompletedTask;
        }

        // Back-pressure
        if (_internalBuffer.Count >= _perfTunedSettings.MaxBufferSize)
            _allCatchUpSubscription?.Stop();

        _internalBuffer.Enqueue(new BufferedEvent(resolvedEvent.StreamId, resolvedEvent.EventNumber,
            resolvedEvent.OriginalPosition, new LinkerEventData(resolvedEvent.EventData.EventId,
                resolvedEvent.EventData.EventType,
                resolvedEvent.EventData.IsJson, resolvedEvent.EventData.Data,
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
                    .AppendToStreamAsync(ev.StreamId, ev.EventNumber - 1, new[] { ev.EventData }).ContinueWith(a =>
                    {
                        if (a.Exception?.InnerException is WrongExpectedVersionException)
                        {
                            if (!TryHandleConflicts(ev1.EventData.EventId, ev1.EventData.EventType, ev1.EventData.IsJson,
                                    ev1.StreamId, ev1.EventData.Data,
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

    private bool TryHandleConflicts(Guid eventId, string eventType, bool isJson, string eventStreamId, byte[] data,
        WrongExpectedVersionException exception, IDictionary<string, dynamic> enrichedMetadata)
    {
        if (exception == null)
            return false;

        try
        {
            enrichedMetadata.Add("$error", exception.GetBaseException().Message);
            if (_handleConflicts)
            {
                var conflictStreamId = $"$conflicts-from-{_connectionBuilderForOrigin.ConnectionName}-to-{_connectionBuilderForDestination.ConnectionName}";
                _destinationConnection.AppendToStreamAsync(conflictStreamId, ExpectedVersion.Any, new[]
                {
                    new LinkerEventData(eventId, eventType, isJson, data, _replicaHelper.SerializeObject(enrichedMetadata))
                }).Wait();
            }
            else
            {
                _destinationConnection.AppendToStreamAsync(eventStreamId, ExpectedVersion.Any, new[]
                {
                    new LinkerEventData(eventId, eventType, isJson, data, _replicaHelper.SerializeObject(enrichedMetadata))
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