using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Est.CrossClusterReplication;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Est.Web
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var app = new CommandLineApplication
            {
                Name = "eventstore-tools",
                Description = "A tool to enrich EventStore with features like cross datacenter replicas",
            };
            app.HelpOption(inherited: true);
            app.Command("replica", replicaCmd =>
            {
                var config = replicaCmd.Option("-c|--config <FILE>", "Configuration path/file", CommandOptionType.SingleValue)
                    .Accepts(v => v.ExistingFile());

                replicaCmd.OnExecute(() =>
                {
                    var configProvider = new ConfigFromFile(config.Value());
                    var settings = configProvider.GetSettings();
                    foreach (var crossClusterReplica in settings.Replicas)
                    {
                        //var replicaService = ...
                    }

                    replicaCmd.ShowHelp();
                    return 1;
                });
            });

            app.OnExecute(() =>
            {
                Console.WriteLine("Specify a subcommand");
                app.ShowHelp();
                return 1;
            });

            var res = app.Execute(args);

            CreateWebHostBuilder(args).Build().Run();
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseStartup<Startup>();
    }
}
