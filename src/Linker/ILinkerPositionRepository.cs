namespace Linker;

public interface ILinkerPositionRepository
{
    string PositionEventType { get; set; }
    void Set(Position lastPosition);
    public void Start();
    public void Stop();
    Position Get();
}