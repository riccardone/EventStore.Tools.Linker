namespace Linker;

public class Settings
{
    public Link[] Links { get; set; }
    public int MaxBufferSize { get; }
    public bool HandleConflicts { get; }
    public bool ResolveLinkTos { get; }

    public static Settings Default()
    {
        return new Settings(SettingsDefaults.HandleConflicts, 
            SettingsDefaults.MaxBufferSize, SettingsDefaults.ResolveLinkTos);
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
    public Origin Origin { get; set; }
    public Destination Destination { get; set; }
    public IEnumerable<Filter> Filters { get; set; }
}

public class Origin
{
    public string ConnectionString { get; set; }
    public string ConnectionName { get; set; }
}

public class Destination
{
    public string ConnectionString { get; set; }
    public string ConnectionName { get; set; }
}