namespace Linker;

public interface ILinkerConnection
{
    Task AppendToStreamAsync(string streamId, long eventNumber, LinkerEventData[] eventData);

    Task Start();
    Task Stop();
}