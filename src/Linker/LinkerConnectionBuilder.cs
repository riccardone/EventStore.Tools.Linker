using KurrentDB.Client;

namespace Linker;

public class LinkerConnectionBuilder : ILinkerConnectionBuilder
{
    public string ConnectionName { get; }
    private KurrentDBClientSettings ConnectionSettings { get; }

    public KurrentDBClient Build()
    {
        ConnectionSettings.DefaultDeadline = TimeSpan.FromSeconds(30);
        ConnectionSettings.CreateHttpMessageHandler = () =>
        {
            var handler = new HttpClientHandler();
#if !DEBUG
        handler.ClientCertificates.Add(_certificate);
        handler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true;
#endif
            return handler;
        };
        return new KurrentDBClient(ConnectionSettings);
    }

    public LinkerConnectionBuilder(KurrentDBClientSettings connectionSettings, string connectionName)
    {
        ConnectionSettings = connectionSettings;
        ConnectionName = connectionName;
    }
}