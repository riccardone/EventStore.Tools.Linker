using Linker.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Linker;

internal class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.Sources.Clear();

                var rawEnv = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "dev";
                var configPath = Environment.GetEnvironmentVariable("LINKER_CONFIG_PATH") ?? "appsettings.json";

                config.SetBasePath(Directory.GetCurrentDirectory());
                config.AddJsonFile(configPath, optional: true, reloadOnChange: false);
                config.AddJsonFile($"appsettings.{rawEnv}.json", optional: true, reloadOnChange: false);
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