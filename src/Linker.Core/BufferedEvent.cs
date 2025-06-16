using KurrentDB.Client;

namespace Linker.Core;

public class BufferedEvent(
    string streamId,
    StreamPosition eventNumber,
    Position originalPosition,
    EventData eventData,
    DateTime created)
    : IComparable<BufferedEvent>
{
    public string StreamId { get; } = streamId;
    public StreamPosition EventNumber { get; } = eventNumber;
    public Position OriginalPosition { get; } = originalPosition;
    public EventData EventData { get; private set; } = eventData;
    public DateTime Created { get; } = created;

    public int CompareTo(BufferedEvent? that)
    {
        if (that == null) return 1;
        if (that.StreamId.Equals(StreamId) && that.EventNumber.Equals(EventNumber) &&
            that.EventData.EventId.Equals(EventData.EventId))
            return 0;
        return 1;
    }
}