using System;

namespace Linker.Model
{
    public class EventData
    {
        private object eventType;

        public EventData(Guid eventId, object eventType, bool isJson, byte[] data, byte[] metadata)
        {
            EventId = eventId;
            this.eventType = eventType;
            IsJson = isJson;
            Data = data;
            Metadata = metadata;
        }

        public Guid EventId { get; set; }
        public string Type { get; set; }
        public bool IsJson { get; set; }
        public byte[] Metadata { get; set; }
        public byte[] Data { get; set; }
    }
}
