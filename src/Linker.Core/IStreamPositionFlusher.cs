namespace Linker.Core;

public interface IStreamPositionFlusher
{
    void Update(string streamId, ulong position);
    Task<IDictionary<string, ulong>> LoadAsync();
    Task StartAsync();
    Task StopAsync();
    Task FlushAsync();
    void Remove(string streamId); 
}
