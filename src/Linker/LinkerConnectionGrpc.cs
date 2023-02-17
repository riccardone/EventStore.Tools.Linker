using System.Text.Json;
using EventStore.Client;

namespace Linker;

public class LinkerConnectionGrpc : ILinkerConnection
{
    private readonly EventStoreClient _eventStoreClient;

    public LinkerConnectionGrpc(string connectionString, string connectionName)
    {
        var settings = EventStoreClientSettings.Create(connectionString);
        settings.ConnectionName = connectionName;
        _eventStoreClient = new EventStoreClient(settings);
    }

    public async Task AppendToStreamAsync(string streamId, long eventNumber, LinkerEventData[] eventData)
    {
        await _eventStoreClient.AppendToStreamAsync(streamId, StreamState.Any,
            eventData.Select(data => new EventData(Uuid.Parse(data.EventId.ToString()), data.EventType,
                    JsonSerializer.SerializeToUtf8Bytes(data.Data)))
                .ToList()); // TODO check how to map eventNumber to ExpectedRevision or ExpectedState
    }

    public event LinkerConnectionTcp.ConnectedEventHandler? Connected;

    public Task Start()
    {
        return Task.CompletedTask;
    }

    public Task Stop()
    {
        return Task.CompletedTask;
    }
}