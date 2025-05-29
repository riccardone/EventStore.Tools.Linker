using KurrentDB.Client;

namespace Linker;

public interface ILinkerConnectionBuilder
{
    string ConnectionName { get; }
    KurrentDBClient Build();
}