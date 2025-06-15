using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Linker.App;

internal class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                var env = context.HostingEnvironment;
                config.SetBasePath(Directory.GetCurrentDirectory());
                config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
                config.AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true, reloadOnChange: false);
                config.AddEnvironmentVariables();
            })
            .ConfigureLogging((context, logging) =>
            {
                logging.ClearProviders();
                logging.AddConfiguration(context.Configuration.GetSection("Logging"));
                logging.AddConsole();
            })
            .ConfigureServices((context, services) =>
            {
                var settings = new Settings();
                context.Configuration.Bind(settings);
                settings.BufferSize = Math.Clamp(settings.BufferSize, LinkerService.MinAllowedBuffer, LinkerService.MaxAllowedBuffer);
                services.AddSingleton(settings);

                services.AddSingleton<CertManager>();
                services.AddSingleton<ReplicaApp>(); // Your main app entry class
            })
            .Build();

        var app = host.Services.GetRequiredService<ReplicaApp>();
        await app.RunAsync();

        await host.StopAsync();
    }
}