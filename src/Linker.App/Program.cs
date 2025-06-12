using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Linker.App;

static class Program
{
    private static ILogger _logger;
    private static ILoggerFactory _loggerFactory;

    static Program()
    {
        ConfigureLogging();
    }

    static async Task Main(string[] args)
    {
        _logger = _loggerFactory.CreateLogger("ReplicaLogger");
        _logger.LogInformation("Starting Replica Services...");
        var services = new List<ILinkerService>();
        try
        {
            var config = BuildConfig();
            var settings = new Settings();
            config.Bind(settings);
            settings.BufferSize = Math.Clamp(settings.BufferSize, LinkerService.MinAllowedBuffer, LinkerService.MaxAllowedBuffer);
            _logger.LogInformation(
                $"Global settings loaded: BufferSize={settings.BufferSize}, ResolveLinkTos={settings.ResolveLinkTos}, HandleConflicts={settings.HandleConflicts}");
            ILinkerConnectionBuilder origin = null;
            ILinkerConnectionBuilder destination = null;
            var certManager = new CertManager(_loggerFactory.CreateLogger(nameof(CertManager)));
            foreach (var link in settings.Links)
            {
                if (link.Filters == null || !link.Filters.Any())
                {
                    _logger.LogInformation("Setting 'include all' default filter");
                    var defaultFilter = new Filter(FilterType.Stream, "*", FilterOperation.Include);
                    link.Filters = new List<Filter> { defaultFilter };
                }

                var filters = link.Filters.Select(linkFilter => new Filter(linkFilter.FilterType, linkFilter.Value, linkFilter.FilterOperation)).ToList();
                var filterService = new FilterService(filters);
                certManager.TryGetCertificate(link.Origin.Certificate, link.Origin.CertificatePrivateKey, out var originCert);
                certManager.TryGetCertificate(link.Destination.Certificate, link.Destination.CertificatePrivateKey, out var destinationCert);
                var o = new LinkerConnectionBuilder(
                    KurrentDBClientSettings.Create(link.Origin.ConnectionString),
                    link.Origin.ConnectionName, originCert);
                var d = new LinkerConnectionBuilder(
                    KurrentDBClientSettings.Create(link.Destination.ConnectionString),
                    link.Destination.ConnectionName, destinationCert);
                origin ??= o;
                destination ??= d;
                var service = new LinkerService(o, d,
                    new PositionRepository($"PositionStream-{d.ConnectionName}", "PositionUpdated", d.Build()),
                    filterService, settings, _loggerFactory);
                services.Add(service);
            }

            _logger.LogInformation($"Found {services.Count} services to start");
            await StartServices(services);

            // For testing: press O to write a test event into the first Origin or D the first Destination found in settings
            while (true)
            {
                var key = Console.ReadKey();
                switch (key.Key)
                {
                    case ConsoleKey.O:
                        {
                            _logger.LogInformation($"Write the {origin.ConnectionName} stream name");
                            var stream = Console.ReadLine();
                            _logger.LogInformation($"Write the {origin.ConnectionName} event Type");
                            var eventType = Console.ReadLine();
                            await AppendTestEvent(stream, eventType, origin);
                            break;
                        }
                    case ConsoleKey.D:
                        {
                            _logger.LogInformation($"Write the {destination.ConnectionName} stream name");
                            var stream = Console.ReadLine();
                            _logger.LogInformation($"Write the {destination.ConnectionName} event Type");
                            var eventType = Console.ReadLine();
                            await AppendTestEvent(stream, eventType, destination);
                            break;
                        }
                }
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Fatal error");
        }
        finally
        {
            if (services.Any())
            {
                foreach (var service in services)
                {
                    try { await service.StopAsync(); }
                    catch (Exception ex) { _logger.LogError($"Failed to stop {service.Name}: {ex.Message}"); }
                }
            }
        }

        _logger.LogInformation("Press enter to exit the program");
        Console.ReadLine();
    }

    private static async Task StartServices(IEnumerable<ILinkerService> services)
    {
        foreach (var linkerService in services)
        {
            _logger.LogInformation($"Starting {linkerService.Name}");
            await linkerService.StartAsync();
        }
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

    private static IConfigurationRoot BuildConfig()
    {
        var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
            .AddJsonFile($"appsettings.{env}.json", optional: true, reloadOnChange: false)
            .AddEnvironmentVariables();
        return builder.Build();
    }

    private static void ConfigureLogging()
    {
        _loggerFactory = LoggerFactory.Create(logging =>
        {
            logging.ClearProviders();
            logging.SetMinimumLevel(LogLevel.Information);
            logging.AddFilter((category, level) => level >= LogLevel.Information);
            logging.AddSimpleConsole(options =>
            {
                options.SingleLine = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.IncludeScopes = false;
            });
            logging.AddFilter("Microsoft", LogLevel.Error);
            logging.AddFilter("System.Net.Http", LogLevel.Error);
        });
    }
}
