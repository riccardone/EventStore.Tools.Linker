using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Timers;
using Est.CrossClusterReplication.Contracts;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using NLog;

namespace Est.CrossClusterReplication
{
    public class ReplicaService
    {
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        private readonly IPositionRepository _positionRepository;
        private readonly IFilterService _filterService;
        private readonly int _savePositionInterval;
        private Position _lastPosition;
        private EventStoreAllCatchUpSubscription _allCatchUpSubscription;
        public bool AutoStart { get; }
        public string Name { get; }
        private readonly IConnectionBuilder _connectionBuilderForSubscriber;
        private readonly IConnectionBuilder _connectionBuilderForAppender;
        private IEventStoreConnection _appenderConnection;
        private IEventStoreConnection _subscriberConnection;
        private bool _started;
        private int _totalProcessedMessagesCurrent;
        private int _totalProcessedMessagesPerSecondsPrevious;
        private int _processedMessagesPerSeconds;
        private readonly Timer _timerForStats;
        private readonly ReplicaHelper _replicaHelper;
        private PerfTuneSettings _perfTunedSettings = PerfTuneSettings.Default;
        private readonly ConcurrentQueue<BufferedEvent> _internalBuffer = new ConcurrentQueue<BufferedEvent>();
        private readonly Timer _processor;

        public ReplicaService(IConnectionBuilder subscriberBuilder, IConnectionBuilder appenderBuilder,
            IPositionRepository positionRepository, IFilterService filterService, int flushInterval,
            int savePositionInterval)
        {
            Ensure.NotNull(subscriberBuilder, nameof(subscriberBuilder));
            Ensure.NotNull(appenderBuilder, nameof(appenderBuilder));
            Ensure.NotNull(positionRepository, nameof(positionRepository));

            Name = $"Replica From-{subscriberBuilder.ConnectionName}-To-{appenderBuilder.ConnectionName}";
            AutoStart = true;
            _connectionBuilderForSubscriber = subscriberBuilder;
            _connectionBuilderForAppender = appenderBuilder;
            _positionRepository = positionRepository;
            _filterService = filterService;
            _savePositionInterval = savePositionInterval;

            _timerForStats = new Timer(1000);
            _timerForStats.Elapsed += _timerForStats_Elapsed;

            _processor = new Timer(flushInterval);
            _processor.Elapsed += Processor_Elapsed;

            _replicaHelper = new ReplicaHelper();
        }

        public async Task<bool> Start()
        {
            _appenderConnection?.Close();
            _appenderConnection = _connectionBuilderForAppender.Build();
            _appenderConnection.ErrorOccurred += AppenderConnection_ErrorOccurred;
            _appenderConnection.Disconnected += AppenderConnection_Disconnected;
            _appenderConnection.AuthenticationFailed += AppenderConnection_AuthenticationFailed;
            _appenderConnection.Connected += AppenderConnection_Connected;
            _appenderConnection.Reconnecting += _appenderConnection_Reconnecting;
            await _appenderConnection.ConnectAsync();

            _subscriberConnection?.Close();
            _subscriberConnection = _connectionBuilderForSubscriber.Build();
            _subscriberConnection.ErrorOccurred += SubscriberConnection_ErrorOccurred;
            _subscriberConnection.Disconnected += SubscriberConnection_Disconnected;
            _subscriberConnection.AuthenticationFailed += SubscriberConnection_AuthenticationFailed;
            _subscriberConnection.Connected += SubscriberConnection_Connected;
            _subscriberConnection.Reconnecting += _subscriberConnection_Reconnecting;
            await _subscriberConnection.ConnectAsync();

            Log.Info($"{Name} started");
            return true;
        }

        public Task<bool> Stop()
        {
            _appenderConnection.ErrorOccurred -= AppenderConnection_ErrorOccurred;
            _appenderConnection.Disconnected -= AppenderConnection_Disconnected;
            _appenderConnection.AuthenticationFailed += AppenderConnection_AuthenticationFailed;
            _appenderConnection.Connected -= AppenderConnection_Connected;
            _appenderConnection.Reconnecting -= _appenderConnection_Reconnecting;

            _subscriberConnection.ErrorOccurred -= SubscriberConnection_ErrorOccurred;
            _subscriberConnection.Disconnected -= SubscriberConnection_Disconnected;
            _subscriberConnection.AuthenticationFailed -= SubscriberConnection_AuthenticationFailed;
            _subscriberConnection.Connected -= SubscriberConnection_Connected;
            _subscriberConnection.Reconnecting -= _subscriberConnection_Reconnecting;

            _processor.Stop();
            _allCatchUpSubscription?.Stop();
            _appenderConnection?.Close();
            _subscriberConnection?.Close();
            _positionRepository.Stop();
            _timerForStats.Stop();
            _totalProcessedMessagesCurrent = 0;
            _started = false;
            Log.Info($"{Name} stopped");
            return Task.FromResult(true);
        }

        //public async Task<bool> TryHandle(IDictionary<string, dynamic> request)
        //{
        //    if (!_replicaHelper.IsExternalRequestValid(request))
        //        return false;
        //    //if (!string.Equals(request["ServiceType"], _geoReplicaModel.ToString(),
        //    //    StringComparison.InvariantCultureIgnoreCase))
        //    //    return false;
        //    if (!string.Equals(request["Name"], _to.ToString(), StringComparison.InvariantCultureIgnoreCase))
        //        return false;

        //    if (request["Action"] == "Start")
        //    {
        //        await Start();
        //        Log.Info($"{request["ServiceType"]} to '{_to}' started");
        //    }
        //    else if (request["Action"] == "Stop")
        //    {
        //        await Stop();
        //        Log.Info($"{request["ServiceType"]} to '{_to}' stopped");
        //    }
        //    return true;
        //}

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
                Log.Debug($"{Name} Replicated '{eventsToProcess}' events in {elapsedMs}ms");
                _perfTunedSettings = _replicaHelper.OptimizeSettings(elapsedMs, _perfTunedSettings);
                if (!_perfTunedSettings.Equals(oldPerfSettings))
                {
                    Log.Debug($"{Name} Old PerfSettings: {oldPerfSettings}");
                    Log.Debug($"{Name} New PerfSettings: {_perfTunedSettings}");
                }
                Subscribe(_lastPosition);
                _processor.Start();
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Error while Processor_Elapsed: {error}");
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

        private void _subscriberConnection_Reconnecting(object sender, ClientReconnectingEventArgs e)
        {
            Log.Debug($"{Name} Subscriber Reconnecting...");
        }

        private void _appenderConnection_Reconnecting(object sender, ClientReconnectingEventArgs e)
        {
            Log.Debug($"{Name} Appender Reconnecting...");
        }

        private static void SubscriberConnection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
        {
            Log.Warn($"AuthenticationFailed to local instance: {e.Reason}");
        }

        private void SubscriberConnection_Connected(object sender, ClientConnectionEventArgs e)
        {
            Log.Debug($"SubscriberConnection Connected to: {e.RemoteEndPoint}");
            _positionRepository.Start();
            _lastPosition = _positionRepository.Get();
            Subscribe(_lastPosition);
            _processor.Enabled = true;
            _processor.Start();
            _timerForStats.Enabled = true;
            _timerForStats.Start();
            _started = true;
        }

        private void SubscriberConnection_Disconnected(object sender, ClientConnectionEventArgs e)
        {
            Log.Warn($"{Name} disconnected from {e.RemoteEndPoint}");
            Stop();
            Start();
        }

        private void SubscriberConnection_ErrorOccurred(object sender, ClientErrorEventArgs e)
        {
            Log.Error(e.Exception.GetBaseException().Message);
            Stop();
            Start();
        }

        private void AppenderConnection_Connected(object sender, ClientConnectionEventArgs e)
        {
            Log.Debug($"AppenderConnection Connected to: {e.RemoteEndPoint}");
        }

        private void AppenderConnection_AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
        {
            Log.Warn("AuthenticationFailed while subscribefromall {Reason}", e.Reason);
            if (!_started) return;
            Log.Warn($"Restart {Name}...");
            Stop();
            Start();
        }

        private void AppenderConnection_Disconnected(object sender, ClientConnectionEventArgs e)
        {
            Log.Warn("Disconnected while subscribefromall from '{endpoint}'", e.RemoteEndPoint);
            Stop();
            Start();
        }

        private static void AppenderConnection_ErrorOccurred(object sender, ClientErrorEventArgs e)
        {
            Log.Error("Error while subscribefromall: {error}", e.Exception.GetBaseException().Message);
        }

        public IDictionary<string, dynamic> GetStats()
        {
            return new Dictionary<string, dynamic>
            {
                {"serviceType", "crossReplica"},
                {"from", _connectionBuilderForSubscriber.ConnectionName },
                {"to", _connectionBuilderForAppender.ConnectionName },
                {"isRunning", _started},
                {"autoStart", AutoStart},
                {"lastPosition", _lastPosition},
                {"messagesPerSeconds", _processedMessagesPerSeconds}
            };
        }

        private void Subscribe(Position position)
        {
            _allCatchUpSubscription = _subscriberConnection.SubscribeToAllFrom(position,
                BuildSubscriptionSettings(), EventAppeared, LiveProcessingStarted, SubscriptionDropped);
            Log.Debug($"Subscribed from position: {position}");
        }

        private void SubscriptionDropped(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, SubscriptionDropReason subscriptionDropReason, Exception arg3)
        {
            if (!_started)
                return;
            if (subscriptionDropReason == SubscriptionDropReason.UserInitiated)
                return;
            if (arg3?.GetBaseException() is ObjectDisposedException)
                return;
            Log.Warn($"Cross Replica Resubscribing... (reason: {subscriptionDropReason})");
            if (arg3 != null)
                Log.Error($"exception: {arg3.GetBaseException().Message}");
            // TODO review this getlastposition as maybe it is responsible to get many un-necessary idempotent writes
            _lastPosition = _positionRepository.Get();
            Subscribe(_lastPosition);
        }

        private CatchUpSubscriptionSettings BuildSubscriptionSettings()
        {
            return new CatchUpSubscriptionSettings(_perfTunedSettings.MaxLiveQueue, _perfTunedSettings.ReadBatchSize,
                CatchUpSubscriptionSettings.Default.VerboseLogging, CatchUpSubscriptionSettings.Default.ResolveLinkTos);
        }

        private void LiveProcessingStarted(EventStoreCatchUpSubscription eventStoreCatchUpSubscription)
        {
            Log.Debug($"'{Name}' Started");
        }

        protected Task EventAppeared(EventStoreCatchUpSubscription eventStoreCatchUpSubscription, ResolvedEvent resolvedEvent)
        {
            try
            {
                if (resolvedEvent.Event == null || !resolvedEvent.OriginalPosition.HasValue)
                    return Task.CompletedTask;
                return EventAppeared(new BufferedEvent(resolvedEvent.Event.EventStreamId,
                    resolvedEvent.Event.EventNumber, resolvedEvent.OriginalPosition.Value,
                    new EventData(resolvedEvent.Event.EventId, resolvedEvent.Event.EventType,
                        resolvedEvent.Event.IsJson, resolvedEvent.Event.Data, resolvedEvent.Event.Metadata),
                    resolvedEvent.Event.Created));
            }
            catch (Exception e)
            {
                Log.Error(e, $"Error during Cross Replica {e}");
            }

            return Task.CompletedTask;
        }

        public Task EventAppeared(BufferedEvent resolvedEvent)
        {
            if (!_replicaHelper.IsValidForReplica(resolvedEvent.EventData.Type, resolvedEvent.StreamId,
                resolvedEvent.OriginalPosition, _positionRepository.PositionEventType, _filterService))
            {
                _lastPosition = resolvedEvent.OriginalPosition;
                return Task.CompletedTask;
            }

            IDictionary<string, dynamic> enrichedMetadata;
            var origin = _connectionBuilderForSubscriber.ConnectionName;
            if (!_replicaHelper.TryProcessMetadata(resolvedEvent.StreamId, resolvedEvent.EventNumber,
                resolvedEvent.Created, origin,
                _replicaHelper.DeserializeObject(resolvedEvent.EventData.Metadata) ?? new Dictionary<string, dynamic>(),
                out enrichedMetadata))
            {
                _lastPosition = resolvedEvent.OriginalPosition;
                _positionRepository.Set(_lastPosition);
                return Task.CompletedTask;
            }

            // Backpressure
            if (_internalBuffer.Count >= _perfTunedSettings.MaxBufferSize)
                _allCatchUpSubscription?.Stop();

            _internalBuffer.Enqueue(new BufferedEvent(resolvedEvent.StreamId, resolvedEvent.EventNumber,
                resolvedEvent.OriginalPosition, new EventData(resolvedEvent.EventData.EventId,
                    resolvedEvent.EventData.Type,
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
                    tasks.Add(_appenderConnection
                        .AppendToStreamAsync(ev.StreamId, ev.EventNumber - 1, new[] { ev.EventData },
                            _connectionBuilderForAppender.Credentials).ContinueWith(a =>
                        {
                            if (a.Exception?.InnerException is WrongExpectedVersionException)
                            {
                                if (!TryHandleConflicts(ev1.EventData.EventId, ev1.EventData.Type, ev1.EventData.IsJson,
                                    ev1.StreamId, ev1.EventData.Data,
                                    (WrongExpectedVersionException)a.Exception.InnerException,
                                    _replicaHelper.DeserializeObject(ev1.EventData.Metadata)))
                                {
                                    Log.Warn($"Error while handling conflicts: {a.Exception.InnerException.Message}");
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
                    Log.Error(aeInnerException, "Error while processing georeplica queue: {error}");
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
                //if (_to.HandleConflicts)
                //{
                //    var conflictStreamId = $"$conflicts-from-{_connectionBuilderForSubscriber.ConnectionName}-to-{_connectionBuilderForAppender.ConnectionName}";
                //    _appenderConnection.AppendToStreamAsync(conflictStreamId, ExpectedVersion.Any, new[]
                //    {
                //        new EventData(eventId, eventType, isJson, data, _replicaHelper.SerializeObject(enrichedMetadata))
                //    }, _connectionBuilderForAppender.Credentials).Wait();
                //}
                //else
                //{
                _appenderConnection.AppendToStreamAsync(eventStreamId, ExpectedVersion.Any, new[]
                {
                        new EventData(eventId, eventType, isJson, data, _replicaHelper.SerializeObject(enrichedMetadata))
                    }, _connectionBuilderForAppender.Credentials).Wait();
                //}

                return true;
            }
            catch (Exception e)
            {
                Log.Error(e, "Error while TryHandleConflicts");
                return false;
            }
        }
    }
}