namespace Linker;

public interface IStreamPositionFlusher
{
    void Update(string streamId, ulong position);
    Task<IDictionary<string, ulong>> LoadAsync();
    Task FlushAsync();
    Task StartAsync();
    Task StopAsync();
}