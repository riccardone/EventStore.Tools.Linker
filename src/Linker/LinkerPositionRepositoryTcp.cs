using EventStore.PositionRepository;

namespace Linker;

public class LinkerPositionRepositoryTcp : ILinkerPositionRepository
{
    private readonly PositionRepository _positionRepository;

    public LinkerPositionRepositoryTcp(string positionStreamName, string positionEventType, ConnectionBuilder connectionBuilder)
    {
        PositionEventType = positionEventType;
        _positionRepository = new PositionRepository(positionStreamName, positionEventType, connectionBuilder);
    }

    public string PositionEventType { get; }

    public void Set(Position lastPosition)
    {
        _positionRepository.Set(new EventStore.ClientAPI.Position(lastPosition.CommitPosition, lastPosition.PreparePosition));
    }

    public async Task StartAsync()
    {
        await _positionRepository.Start();
    }

    public void Stop()
    {
        _positionRepository.Stop();
    }

    public Position Get()
    {
        var pos = _positionRepository.Get();
        return new Position(pos.CommitPosition, pos.PreparePosition);
    }
}