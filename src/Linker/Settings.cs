namespace Linker;

public class Settings
{
    public string DataFolder { get; set; } = "data";
    public bool AutomaticTuning { get; set; } = true;
    public int BufferSize { get; set; } = 100;
    public bool HandleConflicts { get; set; } = true;
    public bool ResolveLinkTos { get; set; } = false;
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
}

public class Destination
{
    public required string ConnectionString { get; set; }
    public required string ConnectionName { get; set; }
    public string? Certificate { get; set; }
    public string? CertificatePrivateKey { get; set; }
}