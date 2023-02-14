namespace Linker;

public interface ILinkerPositionRepository
{
    string PositionEventType { get; set; }
    void Set(Position lastPosition);
}