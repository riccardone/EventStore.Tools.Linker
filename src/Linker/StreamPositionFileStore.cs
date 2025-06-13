using System.Text.Json;

namespace Linker;

public class StreamPositionFileStore
{
    private readonly string _filePath;
    private readonly Lock _fileLock = new();

    public StreamPositionFileStore(string folderPath, string fileName)
    {
        Directory.CreateDirectory(folderPath); // Ensure the folder exists
        _filePath = Path.Combine(folderPath, fileName);
    }

    public async Task<IDictionary<string, ulong>> LoadAsync()
    {
        var directory = Path.GetDirectoryName(_filePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory); // Creates the folder if it doesn't exist

        if (!File.Exists(_filePath))
            return new Dictionary<string, ulong>();

        using var stream = File.OpenRead(_filePath);
        var positions = await JsonSerializer.DeserializeAsync<Dictionary<string, ulong>>(stream);
        return positions ?? new Dictionary<string, ulong>();
    }

    public async Task SaveAsync(string streamId, ulong position)
    {
        Dictionary<string, ulong> positions;

        lock (_fileLock)
        {
            if (File.Exists(_filePath))
            {
                var json = File.ReadAllText(_filePath);
                positions = JsonSerializer.Deserialize<Dictionary<string, ulong>>(json) ?? new();
            }
            else
            {
                positions = new();
            }

            positions[streamId] = position;
            File.WriteAllText(_filePath, JsonSerializer.Serialize(positions));
        }
    }

    public Task SaveAllAsync(IDictionary<string, ulong> positions)
    {
        var json = JsonSerializer.Serialize(positions, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        return File.WriteAllTextAsync(_filePath, json);
    }
}