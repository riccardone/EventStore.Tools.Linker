using System.Text;
using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Linker.Core;
using Microsoft.Extensions.Logging;

namespace Linker;

public class ReplicaApp
{
    private readonly ILogger<ReplicaApp> _logger;
    private readonly CertManager _certManager;
    private readonly Settings _settings;
    private readonly ILoggerFactory _loggerFactory;

    public ReplicaApp(ILogger<ReplicaApp> logger, CertManager certManager, Settings settings,
        ILoggerFactory loggerFactory)
    {
        _logger = logger;
        _certManager = certManager;
        _settings = settings;
        _loggerFactory = loggerFactory;
    }

    public async Task RunAsync()
    {
        _logger.LogInformation("Starting Replica Services...");
        _logger.LogInformation("DataFolder       = {DataFolder}", _settings.DataFolder);
        _logger.LogInformation("AutomaticTuning  = {AutomaticTuning}", _settings.AutomaticTuning);
        _logger.LogInformation("BufferSize       = {BufferSize}", _settings.BufferSize);
        _logger.LogInformation("ResolveLinkTos   = {ResolveLinkTos}", _settings.ResolveLinkTos);
        _logger.LogInformation("HandleConflicts  = {HandleConflicts}", _settings.HandleConflicts);

        var services = new List<ILinkerService>();
        ILinkerConnectionBuilder? origin = null;
        ILinkerConnectionBuilder? destination = null;

        foreach (var link in _settings.Links)
        {
            if (link.Filters == null || !link.Filters.Any())
            {
                _logger.LogInformation("Setting 'include all' default filter");
                link.Filters = new List<Filter> { new(FilterType.Stream, "*", FilterOperation.Include) };
            }

            var filters = link.Filters
                .Select(f => new Filter(f.FilterType, f.Value, f.FilterOperation))
                .ToList();

            var originCert = _certManager.GetCertificate(link.Origin.Certificate, link.Origin.CertificatePrivateKey,
                link.Origin.CertificateFile, link.Origin.PrivateKeyFile);

            var destinationCert = _certManager.GetCertificate(link.Destination.Certificate, link.Destination.CertificatePrivateKey,
                link.Destination.CertificateFile, link.Destination.PrivateKeyFile);

            var o = new LinkerConnectionBuilder(KurrentDBClientSettings.Create(link.Origin.ConnectionString),
                link.Origin.ConnectionName, originCert);
            var d = new LinkerConnectionBuilder(KurrentDBClientSettings.Create(link.Destination.ConnectionString),
                link.Destination.ConnectionName, destinationCert);

            origin ??= o;
            destination ??= d;

            var name = $"From-{o.ConnectionName}-To-{d.ConnectionName}";

            var service = new LinkerService(o, d,
                new PositionRepository($"PositionStream-{d.ConnectionName}", "PositionUpdated", d.Build()),
                new FilterService(filters),
                _settings,
                new FileAdjustedStreamRepository(
                    Path.Combine(_settings.DataFolder, "positions", $"adjusted_streams_{name}.json"),
                    _loggerFactory.CreateLogger<FileAdjustedStreamRepository>()),
                _loggerFactory);

            services.Add(service);
        }

        _logger.LogInformation($"Found {services.Count} services to start");

        foreach (var svc in services)
        {
            _logger.LogInformation($"Starting {svc.Name}");
            await svc.StartAsync();
        }

        if (Environment.GetEnvironmentVariable("LINKER_INTERACTIVE") == "true")
        {
            // Test input loop
             if (origin != null && destination != null)
            {
                _ = Task.Run(async () =>
                {
                    while (true)
                    {
                        var key = Console.ReadKey(intercept: true);
                        switch (key.Key)
                        {
                            case ConsoleKey.O:
                                _logger.LogInformation($"Write the {origin.ConnectionName} stream name:");
                                var oStream = Console.ReadLine();
                                _logger.LogInformation($"Write the {origin.ConnectionName} event type:");
                                var oType = Console.ReadLine();
                                await AppendTestEvent(oStream!, oType!, origin);
                                break;

                            case ConsoleKey.D:
                                _logger.LogInformation($"Write the {destination.ConnectionName} stream name:");
                                var dStream = Console.ReadLine();
                                _logger.LogInformation($"Write the {destination.ConnectionName} event type:");
                                var dType = Console.ReadLine();
                                await AppendTestEvent(dStream!, dType!, destination);
                                break;
                        }
                    }
                });
            }
        }

        _logger.LogInformation("Replica services running. Press Ctrl+C to shut down.");
        await Task.Delay(Timeout.Infinite);
    }

    private static async Task AppendTestEvent(string stream, string eventType, ILinkerConnectionBuilder senderBuilder)
    {
        await AppendEventAsync("{\"name\":\"for test...\"}", stream, eventType, senderBuilder);
    }

    private static async Task<Guid> AppendEventAsync(
        string jsonBody,
        string stream,
        string eventType,
        ILinkerConnectionBuilder senderBuilder, Guid id)
    {
        await using var conn = senderBuilder.Build();

        var data = Encoding.UTF8.GetBytes(jsonBody);
        var evt = new EventData(Uuid.FromGuid(id), eventType, data);

        await conn.AppendToStreamAsync(stream, StreamState.Any, new[] { evt });
        return id;
    }

    private static async Task<Guid> AppendEventAsync(
        string jsonBody,
        string stream,
        string eventType,
        ILinkerConnectionBuilder senderBuilder)
    {
        return await AppendEventAsync(jsonBody, stream, eventType, senderBuilder, Guid.NewGuid());
    }
}