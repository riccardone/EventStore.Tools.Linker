namespace Linker;

public interface ILinkerService
{
    string Name { get; }
    Task<bool> Start();
    Task<bool> Stop();
    IDictionary<string, dynamic> GetStats();
}