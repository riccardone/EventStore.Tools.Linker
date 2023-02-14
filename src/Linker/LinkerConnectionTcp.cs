using EventStore.ClientAPI;

namespace Linker;

public class LinkerConnectionTcp : ILinkerConnection
{
    private readonly IEventStoreConnection _connection;

    public LinkerConnectionTcp(string connectionString)
    {
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
        _connection?.Close();
        _connection.ErrorOccurred += DestinationConnection_ErrorOccurred;
        _connection.Disconnected += DestinationConnection_Disconnected;
        _connection.AuthenticationFailed += AuthenticationFailed;
        _connection.Connected += DestinationConnection_Connected;
        _connection.Reconnecting += Reconnecting;
        await _connection.ConnectAsync();
    }

    public Task Stop()
    {
        throw new NotImplementedException();
    }

    private void Reconnecting(object sender, ClientReconnectingEventArgs e)
    {
        _logger.Debug($"{Name} Reconnecting...");
    }

    private void AuthenticationFailed(object sender, ClientAuthenticationFailedEventArgs e)
    {
        _logger.Warn($"AuthenticationFailed to {_originConnection.ConnectionName}: {e.Reason}");
    }
}