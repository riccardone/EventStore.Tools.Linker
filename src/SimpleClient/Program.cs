using KurrentDB.Client;
using Linker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleClient;

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

        try
        {
            var config = BuildConfig();
            var links = config.GetSection("links").Get<IEnumerable<Link>>();
            var services = new List<LinkerService>();
            ILinkerConnectionBuilder origin = default;
            foreach (var link in links)
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
                var o = new LinkerConnectionBuilder(KurrentDBClientSettings.Create(link.Origin.ConnectionString),
                    link.Origin.ConnectionName);
                var d = new LinkerConnectionBuilder(KurrentDBClientSettings.Create(link.Destination.ConnectionString),
                    link.Destination.ConnectionName);
                origin ??= o;
                var service = new LinkerService(o,d, filterService, Settings.Default(), _loggerFactory);
                services.Add(service);
            }
            _logger.LogInformation($"Found {services.Count} services to start");
            await StartServices(services);

            // For testing replicating single events press W
            while (true)
            {
                var key = Console.ReadKey();
                if (key.Key == ConsoleKey.W)
                {
                    _logger.LogInformation("Write the stream name");
                    var stream = Console.ReadLine();
                    _logger.LogInformation("Write the event Type");
                    var eventType = Console.ReadLine();
                    await AppendTestEvent(stream, eventType, origin);
                }
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e.GetBaseException().Message);
        }

        _logger.LogInformation("Press enter to exit the program");
        Console.ReadLine();
    }

    private static async Task StartServices(IEnumerable<LinkerService> services)
    {
        foreach (var linkerService in services)
        {
            _logger.LogInformation($"Starting {linkerService.Name}");
            await linkerService.Start();
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
