using System;
using Linker.Model;

namespace Linker
{
    public class BufferedEvent : IComparable<BufferedEvent>
    {
        public string StreamId { get; }
        public long EventNumber { get; }
        public Position OriginalPosition { get; }
        public EventData EventData { get; }
        public DateTime Created { get; }

        public BufferedEvent(string streamId, long eventNumber, Position originalPosition, EventData eventData, DateTime created)
        {
            StreamId = streamId;
            EventNumber = eventNumber;
            OriginalPosition = originalPosition;
            EventData = eventData;
            Created = created;
        }

        public int CompareTo(BufferedEvent that)
        {
            if (that.StreamId.Equals(StreamId) && that.EventNumber.Equals(EventNumber) &&
                that.EventData.EventId.Equals(EventData.EventId))
                return 0;
            return 1;
        }
    }
}