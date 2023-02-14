namespace Linker;

public class LinkerConnectionBuilder : ILinkerConnectionBuilder
{
    public Uri ConnectionString { get; }
    public LinkerConnectionSettings ConnectionSettings { get; }
    public string ConnectionName { get; }

    public ILinkerConnection Build()
    {
        if (ConnectionString.Scheme.Equals("tcp"))
            return new LinkerConnectionTcp(ConnectionString.ToString());
        if (ConnectionString.Scheme.Equals("esdb"))
            return new LinkerConnectionGrpc(ConnectionString.ToString());
        throw new Exception("Can't understand the connectionstring");
    }

    public LinkerConnectionBuilder(Uri connectionString, LinkerConnectionSettings connectionSettings, string connectionName)
    {
        ConnectionString = connectionString;
        ConnectionSettings = connectionSettings;
        ConnectionName = connectionName;
    }
}