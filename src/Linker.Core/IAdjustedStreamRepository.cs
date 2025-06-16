namespace Linker.Core;

public interface IAdjustedStreamRepository
{
    Task<HashSet<string>> LoadAsync();
    Task SaveAsync(HashSet<string> adjustedStreams);
}