namespace Linker;

public class Settings
{
    public int MaxBufferSize { get; }
    public bool HandleConflicts { get; }
    public bool ResolveLinkTos { get; }

    public static Settings Default()
    {
        return new Settings(
            SettingsDefaults.HandleConflicts, SettingsDefaults.MaxBufferSize, SettingsDefaults.ResolveLinkTos);
    }

    public Settings(bool handleConflicts, int maxBufferSize, bool resolveLinkTos)
    {
        HandleConflicts = handleConflicts;
        MaxBufferSize = maxBufferSize;
        ResolveLinkTos = resolveLinkTos;
    }
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
}

public class Destination
{
    public required string ConnectionString { get; set; }
    public required string ConnectionName { get; set; }
}