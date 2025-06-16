using KurrentDB.Client;

namespace Linker.Core;

public interface ILinkerConnectionBuilder
{
    string ConnectionName { get; }
    KurrentDBClient Build();
}