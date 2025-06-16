namespace Linker.Core;

public interface IFilterService
{
    bool IsValid(string eventType, string eventStreamId);
}