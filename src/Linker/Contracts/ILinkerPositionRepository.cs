using Linker.Model;

namespace Linker.Contracts
{
    public interface ILinkerPositionRepository
    {
        string PositionEventType { get; set; }
        void Set(Position lastPosition);
    }
}
