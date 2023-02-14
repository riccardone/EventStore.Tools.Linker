namespace Linker;

public interface ILinkerConnectionBuilder
{
    string ConnectionName { get; }
    ILinkerConnection Build();
}