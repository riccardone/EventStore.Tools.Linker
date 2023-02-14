namespace Linker;

public class LinkerConnectionBuilder : ILinkerConnectionBuilder
{
    public Uri ConnectionString { get; }
    public string ConnectionName { get; }
    public ILinkerConnection Build()
    {
        if (ConnectionString.Scheme.Equals("tcp"))
            return new LinkerConnectionTcp(ConnectionString.ToString(), ConnectionName);
        if (ConnectionString.Scheme.Equals("esdb"))
            return new LinkerConnectionGrpc(ConnectionString.ToString());
        throw new Exception("Can't understand the connectionstring");
    }
    public LinkerConnectionBuilder(Uri connectionString, string connectionName)
    {
        ConnectionName = connectionName;
        ConnectionString = connectionString;
    }
}