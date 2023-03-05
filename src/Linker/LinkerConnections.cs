namespace Linker;

public class LinkerConnections
{
    public Link[] Links { get; set; }
}
public class Link
{
    public Origin Origin { get; set; }
    public Destination Destination { get; set; }
    public IEnumerable<Filter>? Filters { get; set; }
}
public class Origin
{
    public string ConnectionString { get; set; }
    public string? ConnectionName { get; set; }
}
public class Destination
{
    public string ConnectionString { get; set; }
    public string? ConnectionName { get; set; }
}