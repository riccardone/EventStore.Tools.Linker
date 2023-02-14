using Linker;

namespace LinkerApp;

public class Program
{
    public static async Task Main(string[] args)
    {
        await CreateHostBuilder(args).Build().RunAsync();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices((hostContext, services) =>
            {
                var configuration = hostContext.Configuration;
                services.Configure<LinkerSettings>(configuration.GetSection(nameof(LinkerSettings)));
                services.AddHostedService<Worker>();
            });
}