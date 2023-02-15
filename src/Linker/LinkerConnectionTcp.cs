using EventStore.ClientAPI;

namespace Linker;

public class LinkerConnectionTcp : ILinkerConnection
{
    //private readonly ILogger<LinkerConnectionTcp> _logger;
    private readonly string _name;
    private readonly IEventStoreConnection _connection;
    public delegate void ConnectedEventHandler(object sender, EventArgs e);
    public event ConnectedEventHandler Connected;

    public LinkerConnectionTcp(string connectionString, string name)
    {
        //_logger = logger;
        _name = name;
        _connection = EventStoreConnection.Create(new Uri(connectionString));
    }

    public async Task AppendToStreamAsync(string streamId, long eventNumber, LinkerEventData[] eventData)
    {
        var events = eventData.Select(linkerEventData => new EventData(eventId: Guid.NewGuid(),
            type: linkerEventData.EventType, isJson: true, data: linkerEventData.Data,
            metadata: linkerEventData.Metadata)).ToList();
        await _connection.AppendToStreamAsync(streamId, eventNumber, events);
    }

    public async Task Start()
    {
        //_connection?.Close();
        _connection.ErrorOccurred += ErrorOccurred;
        _connection.Disconnected += Disconnected;
        _connection.AuthenticationFailed += AuthenticationFailed;
        _connection.Connected += InternalConnection_Connected;
        _connection.Reconnecting += Reconnecting;
        await _connection.ConnectAsync();
    }

    private void InternalConnection_Connected(object? sender, ClientConnectionEventArgs e)
    {
        Connected?.Invoke(this, e);
    }

    private void Disconnected(object? sender, ClientConnectionEventArgs e)
    {
        // TODO
    }

    private void ErrorOccurred(object? sender, ClientErrorEventArgs e)
    {
        throw new NotImplementedException();
    }

    public Task Stop()
    {
        throw new NotImplementedException();
    }

    private void Reconnecting(object? sender, ClientReconnectingEventArgs e)
    {
        //_logger.LogDebug($"{_name} Reconnecting...");
    }

    private void AuthenticationFailed(object? sender, ClientAuthenticationFailedEventArgs e)
    {
        //_logger.LogWarning($"AuthenticationFailed to {_name}: {e.Reason}");
    }
}