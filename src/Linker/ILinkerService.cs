namespace Linker;

public interface ILinkerService
{
    Task<bool> Start();
    Task<bool> Stop();
    IDictionary<string, dynamic> GetStats();
}