using Linker;
using Microsoft.Extensions.Options;

namespace LinkerApp;

public class Worker : BackgroundService
{
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private readonly LinkerConnections _linkerConnections;
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger, IHostApplicationLifetime hostApplicationLifetime, IOptions<LinkerConnections> options)
    {
        _logger = logger;
        _hostApplicationLifetime = hostApplicationLifetime;
        _linkerConnections = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var services = new List<Linker.LinkerService>();
        foreach (var link in _linkerConnections.Links)
        {
            if (link.Filters == null || !link.Filters.Any())
            {
                _logger.LogInformation("Setting 'include all' default filter");
                var defaultFilter = new Filter(FilterType.Stream, "*", FilterOperation.Include);
                link.Filters = new List<Filter> { defaultFilter };
            }
            var filters = link.Filters.Select(linkFilter => new Filter
            {
                FilterOperation = linkFilter.FilterOperation,
                FilterType = linkFilter.FilterType,
                Value = linkFilter.Value
            }).ToList();
            var filterService = new FilterService(filters);
            var service = new LinkerService(new LinkerConnectionBuilder(new Uri(link.Origin.ConnectionString),
                link.Origin.ConnectionName), new LinkerConnectionBuilder(new Uri(link.Destination.ConnectionString),
                link.Destination.ConnectionName), filterService, LinkerSettings.Default(), new LinkerSubscriber(), link.Destination.ConnectionString);
            services.Add(service);
        }
        await StartServices(services);

        _hostApplicationLifetime.StopApplication();
    }

    private async Task StartServices(IEnumerable<Linker.LinkerService> services)
    {
        foreach (var linkerService in services)
        {
            _logger.LogInformation($"Starting {linkerService.Name}");
            await linkerService.Start();
        }
    }
}