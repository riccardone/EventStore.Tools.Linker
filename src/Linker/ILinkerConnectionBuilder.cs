namespace Linker;

public interface ILinkerConnectionBuilder
{
    [Obsolete]
    Uri ConnectionString { get; }
    [Obsolete]
    LinkerConnectionSettings ConnectionSettings { get; }
    string ConnectionName { get; }
    ILinkerConnection Build();
}