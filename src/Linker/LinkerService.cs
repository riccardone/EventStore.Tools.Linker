using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using System.Threading.Channels;
using System.Timers;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Linker;

public class LinkerService : ILinkerService, IAsyncDisposable
{
    private readonly ILogger _logger;
    private readonly IPositionRepository _positionRepository;
    private readonly IFilterService? _filterService;
    private readonly bool _handleConflicts;
    private Position _lastPosition;
    public string Name { get; }

    private readonly ILinkerConnectionBuilder _originConnectionBuilder;
    private readonly ILinkerConnectionBuilder _destinationConnectionBuilder;
    private KurrentDBClient _originConnection;
    private KurrentDBClient _destinationConnection;

    private readonly Channel<BufferedEvent> _channel;
    private Task? _processingTask;

    private readonly System.Timers.Timer _timerForStats;

    private readonly LinkerHelper _replicaHelper;
    private readonly CancellationTokenSource _cts = new();
    private Task? _subscriptionTask;
    private bool _started;

    private long _replicatedSinceLastStats;
    private long _replicatedTotal;

    public LinkerService(
        ILinkerConnectionBuilder originBuilder,
        ILinkerConnectionBuilder destinationBuilder,
        IPositionRepository positionRepository,
        IFilterService? filterService,
        Settings settings,
        ILoggerFactory loggerFactory)
    {
        Ensure.NotNull(originBuilder, nameof(originBuilder));
        Ensure.NotNull(destinationBuilder, nameof(destinationBuilder));
        Ensure.NotNull(positionRepository, nameof(positionRepository));

        _logger = loggerFactory.CreateLogger(nameof(LinkerService));
        Name = $"Replica From-{originBuilder.ConnectionName}-To-{destinationBuilder.ConnectionName}";

        _originConnectionBuilder = originBuilder;
        _destinationConnectionBuilder = destinationBuilder;

        _positionRepository = positionRepository;
        _filterService = filterService;
        _handleConflicts = settings.HandleConflicts;

        _channel = Channel.CreateBounded<BufferedEvent>(new BoundedChannelOptions(settings.MaxBufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = false,
            SingleReader = true
        });
        _replicaHelper = new LinkerHelper();

        _timerForStats = new System.Timers.Timer(3000);
        _timerForStats.Elapsed += TimerForStats_Elapsed;
    }

    public async Task<bool> StartAsync()
    {
        if (_started) return true;

        await StopAsync();

        if (!_positionRepository.TryGet(out _lastPosition))
            throw new Exception("Error retriev position");

        _destinationConnection = _destinationConnectionBuilder.Build();
        _originConnection = _originConnectionBuilder.Build();

        _started = true;

        _processingTask = Task.Run(ProcessChannelEventsAsync);
        _subscriptionTask = SubscribeMeGrpc(_cts.Token);

        _timerForStats.Start();

        _logger.LogInformation($"{Name} started.");

        return true;
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
            try
            {
                await _subscriptionTask;
            }
            catch (OperationCanceledException)
            {
                // Expected on cancellation
            }
        }

        if (_processingTask is not null)
        {
            try
            {
                await _processingTask;
            }
            catch (OperationCanceledException)
            {
                // Expected on cancellation
            }
            _processingTask = null;
        }

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
        try
        {
            await foreach (var evt in _channel.Reader.ReadAllAsync(_cts.Token))
            {
                await AppendEventAsync(evt);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError($"Channel processing error: {ex.GetBaseException().Message}");
            await RestartServiceAsync();
        }
    }


    private async Task SubscribeMeGrpc(CancellationToken ctsToken)
    {
        await using var subscription = _originConnection.SubscribeToAll(FromAll.After(_lastPosition), cancellationToken: ctsToken,
            resolveLinkTos: false, filterOptions: new SubscriptionFilterOptions(EventTypeFilter.ExcludeSystemEvents()));
        await foreach (var message in subscription.Messages)
        {
            switch (message)
            {
                case StreamMessage.Event(var evnt):
                    _logger.LogDebug($"{Name} Received event {evnt.OriginalEventNumber}@{evnt.OriginalStreamId}");
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
            _replicaHelper.DeserializeObject(bufferedEvent.EventData.Metadata),
            out var enrichedMetadata))
        {
            _lastPosition = bufferedEvent.OriginalPosition;
            _positionRepository.Set(_lastPosition);
            return;
        }

        await _channel.Writer.WriteAsync(bufferedEvent, _cts.Token);
    }

    private async Task AppendEventAsync(BufferedEvent evt)
    {
        try
        {
            await _destinationConnection.AppendToStreamAsync(evt.StreamId,
                StreamState.StreamRevision(evt.EventNumber.ToUInt64() - 1), new[] { evt.EventData });
            _lastPosition = evt.OriginalPosition;
            _positionRepository.Set(_lastPosition);
            Interlocked.Increment(ref _replicatedSinceLastStats);
            Interlocked.Increment(ref _replicatedTotal);
        }
        catch (WrongExpectedVersionException ex)
        {
            if (!_handleConflicts || !await HandleConflictAsync(evt, ex))
                _logger.LogWarning($"Conflict handling failed: {ex.Message}");
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
            _logger.LogError($"Conflict append failed: {innerEx.Message}");
            return false;
        }
    }

    private void TimerForStats_Elapsed(object sender, ElapsedEventArgs e)
    {
        var replicatedThisInterval = Interlocked.Exchange(ref _replicatedSinceLastStats, 0);
        var totalReplicated = Interlocked.Read(ref _replicatedTotal);
        var bufferSize = _channel.Reader.Count;
       
        _logger.LogInformation($"{Name} stats: replicated {replicatedThisInterval} events, total: {totalReplicated}, buffer: {bufferSize}, position: {_lastPosition}");
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