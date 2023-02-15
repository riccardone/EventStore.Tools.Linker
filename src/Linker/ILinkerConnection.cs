using static Linker.LinkerConnectionTcp;

namespace Linker;

public interface ILinkerConnection
{
    Task AppendToStreamAsync(string streamId, long eventNumber, LinkerEventData[] eventData);
    public event ConnectedEventHandler Connected;
    Task Start();
    Task Stop();
}