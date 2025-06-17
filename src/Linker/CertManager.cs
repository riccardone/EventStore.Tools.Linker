using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;

namespace Linker;

public class CertManager(ILogger<CertManager> logger)
{
    public X509Certificate2? GetCertificate(string? inlineCert, string? inlineKey, string? certFile, string? keyFile)
    {
        try
        {
            // Inline cert and key
            if (!string.IsNullOrWhiteSpace(inlineCert) && !string.IsNullOrWhiteSpace(inlineKey))
                return ConvertToX509Certificate2(LoadCertificate(inlineCert), LoadPrivateKey(inlineKey));

            if (string.IsNullOrWhiteSpace(certFile) || string.IsNullOrWhiteSpace(keyFile))
                // No certs provided = unsecure mode
                return null;

            // Cert and key from files
            if (!File.Exists(certFile) || !File.Exists(keyFile))
                throw new ArgumentException("Can't find specified security files");
            var certPem = File.ReadAllText(certFile);
            var keyPem = File.ReadAllText(keyFile);
            return ConvertToX509Certificate2(LoadCertificate(certPem), LoadPrivateKey(keyPem));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error resolving certificate from inline or file");
            throw;
        }
    }

    private static Org.BouncyCastle.X509.X509Certificate LoadCertificate(string cert)
    {
        using var reader = new StringReader(cert);
        var pemReader = new PemReader(reader);
        return (Org.BouncyCastle.X509.X509Certificate)pemReader.ReadObject();
    }

    private static AsymmetricKeyParameter LoadPrivateKey(string key)
    {
        using var reader = new StringReader(key);
        var pemReader = new PemReader(reader);
        var keyObject = pemReader.ReadObject();
        if (keyObject is AsymmetricCipherKeyPair keyPair)
            return keyPair.Private;
        throw new InvalidDataException("Invalid private key format");
    }

    private static X509Certificate2 ConvertToX509Certificate2(Org.BouncyCastle.X509.X509Certificate cert, AsymmetricKeyParameter privateKey)
    {
        using var stream = new MemoryStream();
        var store = new Pkcs12StoreBuilder().Build();
        var certificateEntry = new X509CertificateEntry(cert);
        store.SetCertificateEntry("cert", certificateEntry);
        store.SetKeyEntry("cert", new AsymmetricKeyEntry(privateKey), new[] { certificateEntry });
        store.Save(stream, null, new SecureRandom());
        return new X509Certificate2(stream.ToArray(), (string)null, X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable);
    }
}