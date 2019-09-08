namespace Linker
{
    public interface IFilterService
    {
        bool IsValid(string eventType, string eventStreamId);
    }
}
