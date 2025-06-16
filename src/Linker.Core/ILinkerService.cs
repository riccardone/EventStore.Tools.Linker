namespace Linker.Core;

public interface ILinkerService
{
    string Name { get; }
    Task<bool> StartAsync();
    Task<bool> StopAsync();
    IDictionary<string, dynamic> GetStats();
}