namespace Linker.Core;

public class Settings
{
    public string DataFolder { get; set; } = "/data"; 
    public bool AutomaticTuning { get; set; } = false;
    public int BufferSize { get; set; } = 100;
    public bool HandleConflicts { get; set; } = true;
    public bool ResolveLinkTos { get; set; } = false;
    public bool InteractiveMode { get; set; } = false;
    public bool EnableReconciliation { get; set; } = true;
    public IEnumerable<Link> Links { get; set; } = new List<Link>();
}

public class Link
{
    public required Origin Origin { get; set; }
    public required Destination Destination { get; set; }
    public required IEnumerable<Filter> Filters { get; set; }
}

public class Origin
{
    public required string ConnectionString { get; set; }
    public required string ConnectionName { get; set; }
    public string? Certificate { get; set; }
    public string? CertificatePrivateKey { get; set; }
    public string? CertificateFile { get; set; }
    public string? PrivateKeyFile { get; set; }
}

public class Destination
{
    public required string ConnectionString { get; set; }
    public required string ConnectionName { get; set; }
    public string? Certificate { get; set; }
    public string? CertificatePrivateKey { get; set; }
    public string? CertificateFile { get; set; }
    public string? PrivateKeyFile { get; set; }
}