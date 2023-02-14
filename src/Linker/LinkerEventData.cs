namespace Linker;

public class LinkerEventData
{
    public LinkerEventData(Guid eventId, string eventType, bool isJson, byte[] data, byte[] metadata)
    {
        EventId = eventId;
        EventType = eventType;
        IsJson = isJson;
        Data = data;
        Metadata = metadata;
    }

    public string EventType { get; }
    public Guid EventId { get; }
    public bool IsJson { get; }
    public byte[] Metadata { get; }
    public byte[] Data { get; }
}