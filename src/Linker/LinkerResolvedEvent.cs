#nullable enable
namespace Linker;

public class LinkerResolvedEvent
{
    public Event Event { get; set; }
    public Position? OriginalPosition { get; set; }
}

public class Event
{
    public long EventNumber { get; set; }
    public string EventStreamId { get; set; }
    public Guid EventId { get; set; }
    public string EventType { get; set; }
    public byte[] Metadata { get; set; }
    public byte[] Data { get; set; }
    public bool IsJson { get; set; }
    public DateTime Created { get; set; }
}