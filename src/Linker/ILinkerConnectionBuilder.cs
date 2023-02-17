namespace Linker;

public interface ILinkerConnectionBuilder
{
    string ConnectionName { get; }
    ILinkerConnection Build();
    ILinkerConnection Build(string name);
}