using System.Security.Cryptography.X509Certificates;
using KurrentDB.Client;

namespace Linker;

public class LinkerConnectionBuilder(
    KurrentDBClientSettings connectionSettings,
    string connectionName,
    X509Certificate2? cert)
    : ILinkerConnectionBuilder
{
    public string ConnectionName { get; } = connectionName;
    private KurrentDBClientSettings ConnectionSettings { get; } = connectionSettings;

    public KurrentDBClient Build()
    {
        ConnectionSettings.DefaultDeadline = TimeSpan.FromSeconds(30);
        ConnectionSettings.CreateHttpMessageHandler = () =>
        {
            var handler = new HttpClientHandler();
            if (cert == null) return handler;
            handler.ClientCertificates.Add(cert);
            handler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true;
            return handler;
        };
        return new KurrentDBClient(ConnectionSettings);
    }
}