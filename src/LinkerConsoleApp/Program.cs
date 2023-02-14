using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Linker;
using Microsoft.Extensions.Configuration;
using NLog;

namespace LinkerConsoleApp
{
    class Program
    {
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        static async Task Main(string[] args)
        {
            Log.Info("Building services...");
            var config = BuildConfig();
            var links = config.GetSection("links").Get<IEnumerable<Link>>();
            var services = new List<LinkerService>();
            foreach (var link in links)
            {
                if (link.Filters == null || !link.Filters.Any())
                {
                    Log.Info("Setting 'include all' default filter");
                    var defaultFilter = new Filter(FilterType.Stream, "*", FilterOperation.Include);
                    link.Filters = new List<Filter> {defaultFilter};
                }
                var filters = link.Filters.Select(linkFilter => new Filter
                {
                    FilterOperation = linkFilter.FilterOperation, FilterType = linkFilter.FilterType,
                    Value = linkFilter.Value
                }).ToList();
                var filterService = new FilterService(filters);
                var service = new LinkerService(new LinkerConnectionBuilder(new Uri(link.Origin.ConnectionString),
                    LinkerConnectionSettings.Create().SetHeartbeatInterval(TimeSpan.FromSeconds(6))
                        .SetHeartbeatTimeout(TimeSpan.FromSeconds(3)).SetDefaultUserCredentials(new LinkerUserCredentials(link.Origin.User, link.Origin.Pass)),
                    link.Origin.ConnectionName), new LinkerConnectionBuilder(new Uri(link.Destination.ConnectionString),
                    LinkerConnectionSettings.Create().SetHeartbeatInterval(TimeSpan.FromSeconds(6))
                        .SetHeartbeatTimeout(TimeSpan.FromSeconds(3)).SetDefaultUserCredentials(new LinkerUserCredentials(link.Destination.User, link.Destination.Pass)),
                    link.Destination.ConnectionName), filterService, Settings.Default(), new NLogger());
                services.Add(service);
            }
            await StartServices(services);
            Log.Info("Press enter to exit the program");
            Console.ReadLine();
        }

        private static async Task StartServices(IEnumerable<LinkerService> services)
        {
            foreach (var linkerService in services)
            {
                Log.Info($"Starting {linkerService.Name}");
                await linkerService.Start();
            }
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
    }
}
