namespace Linker;

public interface ILinkerPositionRepository
{
    string PositionEventType { get; }
    void Set(Position lastPosition);
    public Task StartAsync();
    public void Stop();
    Position Get();
}