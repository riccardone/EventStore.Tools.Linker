using KurrentDB.Client;

namespace Linker;

public class LinkerConnectionBuilder : ILinkerConnectionBuilder
{
    public Uri ConnectionString { get; }
    public string ConnectionName { get; }
    public KurrentDBClientSettings ConnectionSettings { get; }

    public KurrentDBClient Build()
    {
        var connSettings = KurrentDBClientSettings.Create(ConnectionString.ToString());
        connSettings.ConnectionName = ConnectionName;
        connSettings.DefaultDeadline = TimeSpan.FromSeconds(30);
        connSettings.CreateHttpMessageHandler = () =>
        {
            var handler = new HttpClientHandler();
#if !DEBUG
        handler.ClientCertificates.Add(_certificate);
        handler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true;
#endif
            return handler;
        };
        return new KurrentDBClient(connSettings);
    }

    public LinkerConnectionBuilder(Uri connectionString, KurrentDBClientSettings connectionSettings, string connectionName)
    {
        ConnectionString = connectionString;
        ConnectionSettings = connectionSettings;
        ConnectionName = connectionName;
    }
}