namespace Linker;

public class LinkerPositionRepository : ILinkerPositionRepository
{
    private readonly string _name;
    private readonly string _streamUpdatedName;
    private readonly ILinkerConnection _connection;

    public LinkerPositionRepository(string name, string streamUpdatedName, ILinkerConnection connection)
    {
        _name = name;
        _streamUpdatedName = streamUpdatedName;
        _connection = connection;
    }

    public string PositionEventType { get; set; }
    public void Set(Position lastPosition)
    {
        throw new System.NotImplementedException();
    }

    public void Start()
    {
        _connection.Start();
    }

    public Position Get()
    {
        return new Position();
    }

    public void Stop()
    {
        _connection.Stop();
    }
}