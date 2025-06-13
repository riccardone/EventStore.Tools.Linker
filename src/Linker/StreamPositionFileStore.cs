using System.Text.Json;

namespace Linker;

public class StreamPositionFileStore(string filePath)
{
    private readonly object _fileLock = new();

    public async Task<IDictionary<string, ulong>> LoadAsync()
    {
        if (!File.Exists(filePath))
            return new Dictionary<string, ulong>();

        using var stream = File.OpenRead(filePath);
        var positions = await JsonSerializer.DeserializeAsync<Dictionary<string, ulong>>(stream);
        return positions ?? new Dictionary<string, ulong>();
    }

    public async Task SaveAsync(string streamId, ulong position)
    {
        Dictionary<string, ulong> positions;

        lock (_fileLock)
        {
            if (File.Exists(filePath))
            {
                var json = File.ReadAllText(filePath);
                positions = JsonSerializer.Deserialize<Dictionary<string, ulong>>(json) ?? new();
            }
            else
            {
                positions = new();
            }

            positions[streamId] = position;
            File.WriteAllText(filePath, JsonSerializer.Serialize(positions));
        }
    }

    public Task SaveAllAsync(IDictionary<string, ulong> positions)
    {
        var json = JsonSerializer.Serialize(positions, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        return File.WriteAllTextAsync(filePath, json);
    }
}