using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Microsoft.Extensions.Logging;

namespace Linker.App;

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

            var filterService = new FilterService(filters);

            _certManager.TryGetCertificate(link.Origin.Certificate, link.Origin.CertificatePrivateKey, out var originCert);
            _certManager.TryGetCertificate(link.Destination.Certificate, link.Destination.CertificatePrivateKey, out var destinationCert);

            var o = new LinkerConnectionBuilder(
                KurrentDBClientSettings.Create(link.Origin.ConnectionString),
                link.Origin.ConnectionName,
                originCert);
            var d = new LinkerConnectionBuilder(
                KurrentDBClientSettings.Create(link.Destination.ConnectionString),
                link.Destination.ConnectionName,
                destinationCert);

            origin ??= o;
            destination ??= d;

            var service = new LinkerService(o, d,
                new PositionRepository($"PositionStream-{d.ConnectionName}", "PositionUpdated", d.Build()),
                filterService, _settings, _loggerFactory);

            services.Add(service);
        }

        _logger.LogInformation($"Found {services.Count} services to start");

        foreach (var svc in services)
        {
            _logger.LogInformation($"Starting {svc.Name}");
            await svc.StartAsync();
        }

        _logger.LogInformation("Replica services running. Press Ctrl+C to shut down.");
        await Task.Delay(Timeout.Infinite); // Replace with a better lifetime manager if needed
    }
}