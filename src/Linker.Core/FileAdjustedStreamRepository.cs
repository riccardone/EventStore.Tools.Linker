using Microsoft.Extensions.Logging;

namespace Linker.Core;

public class FileAdjustedStreamRepository(string path, ILogger<FileAdjustedStreamRepository> logger)
    : IAdjustedStreamRepository
{
    public async Task<HashSet<string>> LoadAsync()
    {
        try
        {
            if (!File.Exists(path))
                return new HashSet<string>();

            var lines = await File.ReadAllLinesAsync(path);
            return new HashSet<string>(lines.Select(l => l.Trim()).Where(l => !string.IsNullOrWhiteSpace(l)));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, $"Failed to load adjusted streams from {path}");
            return new HashSet<string>();
        }
    }

    public async Task SaveAsync(HashSet<string> adjustedStreams)
    {
        try
        {
            var lines = adjustedStreams.OrderBy(x => x).ToArray();
            await File.WriteAllLinesAsync(path, lines);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, $"Failed to save adjusted streams to {path}");
        }
    }
}