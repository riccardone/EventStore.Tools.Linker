namespace Linker;

public class LinkerConnectionBuilder : ILinkerConnectionBuilder
{
    public Uri ConnectionString { get; }
    public string ConnectionName { get; }
    public ILinkerConnection Build()
    {
        return Build(ConnectionName);
    }
    public ILinkerConnection Build(string name)
    {
        if (ConnectionString.Scheme.Equals("tcp"))
            return new LinkerConnectionTcp(ConnectionString.ToString(), name);
        if (ConnectionString.Scheme.Equals("esdb"))
            return new LinkerConnectionGrpc(ConnectionString.ToString(), name);
        throw new Exception("Can't understand the connectionstring");
    }
    public LinkerConnectionBuilder(Uri connectionString, string connectionName)
    {
        ConnectionName = connectionName;
        ConnectionString = connectionString;
    }
}