using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Timer = System.Timers.Timer;

namespace Linker;

public class PeriodicStreamPositionFlusher : IStreamPositionFlusher
{
    private readonly string _filePath;
    private readonly ILogger _logger;
    private readonly Timer _flushTimer;
    private readonly ConcurrentDictionary<string, ulong> _positions = new();
    private readonly SemaphoreSlim _flushLock = new(1, 1); // async-safe lock
    private bool _running;

    public PeriodicStreamPositionFlusher(string filePath, ILogger logger, int flushIntervalMs = 5000)
    {
        _filePath = filePath;
        _logger = logger;

        Directory.CreateDirectory(Path.GetDirectoryName(_filePath)!);

        _flushTimer = new Timer(flushIntervalMs);
        _flushTimer.Elapsed += async (_, _) =>
        {
            try
            {
                await FlushAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Flusher] Timer-based flush failed.");
            }
        };
    }

    public void Update(string streamId, ulong position)
    {
        _positions[streamId] = position;
    }

    public Task StartAsync()
    {
        if (_running) return Task.CompletedTask;
        _running = true;
        _flushTimer.Start();
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (!_running) return;
        _running = false;
        _flushTimer.Stop();
        await FlushAsync();
    }

    public async Task FlushAsync()
    {
        if (!_running && !Environment.HasShutdownStarted)
            return;

        await _flushLock.WaitAsync();
        try
        {
            var copy = new Dictionary<string, ulong>(_positions);

            var json = JsonSerializer.Serialize(copy, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(_filePath, json);
            _logger.LogDebug($"[Flusher] Flushed {copy.Count} stream positions to {_filePath}");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Flusher] Failed to flush stream positions.");
        }
        finally
        {
            _flushLock.Release();
        }
    }

    public void Remove(string streamId)
    {
        _positions.TryRemove(streamId, out _);
    }

    public async Task<IDictionary<string, ulong>> LoadAsync()
    {
        try
        {
            if (!File.Exists(_filePath))
            {
                _logger.LogDebug($"[Flusher] No stream position file found at {_filePath}");
                return new Dictionary<string, ulong>();
            }

            using var stream = File.OpenRead(_filePath);
            var positions = await JsonSerializer.DeserializeAsync<Dictionary<string, ulong>>(stream);
            return positions ?? new Dictionary<string, ulong>();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, $"[Flusher] Failed to load stream positions from {_filePath}");
            return new Dictionary<string, ulong>();
        }
    }
}