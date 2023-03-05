using EventStore.ClientAPI;
using Newtonsoft.Json;
using System.Text;

namespace Linker;

/// <summary>
/// Migration to gRPC client https://developers.eventstore.com/clients/dotnet/5.0/migration-to-gRPC.html#migration-strategies
/// </summary>

public static class TypeMapper
{
    public static string GetTypeName(Type getType)
    {
        throw new NotImplementedException();
    }
}
public class EventStoreWrapper
{
    readonly IEventStoreConnection _tcpConnection;

    public EventStoreWrapper(IEventStoreConnection tcpConnection)
        => this._tcpConnection = tcpConnection;

    public Task AppendEvents(
        string streamName,
        long version,
        params object[] events
    )
    {
        var preparedEvents = events
            .Select(ToEventData)
            .ToArray();

        return _tcpConnection.AppendToStreamAsync(
            streamName,
            version,
            preparedEvents
        );

        static EventData ToEventData(object @event) =>
            new EventData(
                Guid.NewGuid(),
                TypeMapper.GetTypeName(@event.GetType()),
                true,
                Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(@event)),
                Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(null))
            );
    }

    public Task AppendEvents(
        string streamName,
        params object[] events
    )
        => AppendEvents(streamName, ExpectedVersion.Any, events);

    public async Task<IEnumerable<object>> LoadEvents(string stream)
    {
        const int pageSize = 4096;

        var start = 0;
        var events = new List<object>();

        do
        {
            var page = await _tcpConnection.ReadStreamEventsForwardAsync(
                stream, start, pageSize, true
            );

            if (page.Status == SliceReadStatus.StreamNotFound)
                throw new ArgumentOutOfRangeException(
                    nameof(stream), $"Stream '{stream}' was not found"
                );

            events.AddRange(
                page.Events.Select(Deserialize)
            );
            if (page.IsEndOfStream) break;

            start += pageSize;
        } while (true);

        return events;

        //static object Deserialize(this ResolvedEvent resolvedEvent)
        //{
        //    var dataType = TypeMapper.GetType(resolvedEvent.Event.EventType);
        //    var jsonData = Encoding.UTF8.GetString(resolvedEvent.Event.Data);
        //    var data = JsonConvert.DeserializeObject(jsonData, dataType);
        //    return data;
        //}
    }

    private object? Deserialize(ResolvedEvent resolvedEvent)
    {
        //var dataType = TypeMapper.GetTypeName(resolvedEvent.Event.GetType());
        var jsonData = Encoding.UTF8.GetString(resolvedEvent.Event.Data);
        var data = System.Text.Json.JsonSerializer.Deserialize(jsonData, resolvedEvent.Event.GetType()); //JsonConvert.DeserializeObject(jsonData, dataType);
        return data;
    }

    public async Task<bool> StreamExists(string stream)
    {
        var result = await _tcpConnection.ReadEventAsync(stream, 1, false);
        return result.Status != EventReadStatus.NoStream;
    }
}