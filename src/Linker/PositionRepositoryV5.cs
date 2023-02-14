namespace Linker;

public class PositionRepositoryV5 : ILinkerPositionRepository
{
    private readonly string _name;
    private readonly string _streamUpdatedName;
    private readonly ILinkerConnection _connection;

    public PositionRepositoryV5(string name, string streamUpdatedName, ILinkerConnection connection)
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
}