using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;

namespace Linker.App;

public class CertManager(ILogger<CertManager> logger)
{
    public bool TryGetCertificate(string certPath, string privateKey, out X509Certificate2 cert)
    {
        cert = null;
        try
        {
            if (string.IsNullOrWhiteSpace(certPath) || string.IsNullOrWhiteSpace(privateKey))
                return false;
            cert = ConvertToX509Certificate2(LoadCertificate(certPath), LoadPrivateKey(privateKey));
            return true;
        }
        catch (Exception e)
        {
            logger.LogError(e, $"Error while {nameof(TryGetCertificate)}");
            return false;
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