using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Linker;
using Microsoft.Extensions.Logging;

namespace SimpleClient;

static class Program
{
    private static readonly Microsoft.Extensions.Logging.ILogger Logger;
    private static readonly ILinkerLogger LinkerLogger;

    static Program()
    {
        var loggerFactory = LoggerFactory.Create(logging =>
        {
            logging.ClearProviders();
            logging.SetMinimumLevel(LogLevel.Information);
            logging.AddSimpleConsole(options =>
            {
                options.SingleLine = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.IncludeScopes = false;
            });
            logging.AddFilter("Microsoft.*", Microsoft.Extensions.Logging.LogLevel.Error);
            logging.AddFilter("System.Net.Http.*", Microsoft.Extensions.Logging.LogLevel.Error);
        });

        Logger = loggerFactory.CreateLogger("ReplicaLogger");
        LinkerLogger = new SimpleLogger(Logger);
    }

    static async Task Main(string[] args)
    {
        Logger.LogInformation("Starting Replica Service...");

        try
        {
            var origin = new LinkerConnectionBuilder(
                KurrentDBClientSettings.Create("esdb://admin:changeit@localhost:2114?tls=false"),
                "origin-01");

            var destination = new LinkerConnectionBuilder(
                KurrentDBClientSettings.Create("esdb://admin:changeit@localhost:2115?tls=false"),
                "destination-01");

            var service = new LinkerService(
                origin,
                destination,
                new PositionRepository("DestinationPosition", "DestinationPosition", destination.Build()),
                GetFilterForSampleEvent(),
                Settings.Default(),
                LinkerLogger);

            await service.Start();
            Logger.LogInformation("Replica Service started");

            await TestReplicaForSampleEvent(origin, destination, "domain-test-01", "UserReplicaTested");
        }
        catch (Exception e)
        {
            Logger.LogError(e.GetBaseException().Message);
        }

        Logger.LogInformation("Press enter to exit the program");
        Console.ReadLine();
    }

    private static IFilterService GetFilterForSampleEvent()
    {
        return new FilterService(new List<Filter>
        {
            new Filter(FilterType.EventType, "User*", FilterOperation.Include),
            new Filter(FilterType.Stream, "domain-*", FilterOperation.Include),
            new Filter(FilterType.EventType, "Basket*", FilterOperation.Exclude)
        });
    }

    private static async Task TestReplicaForSampleEvent(
        ILinkerConnectionBuilder senderBuilder,
        ILinkerConnectionBuilder destinationBuilder,
        string stream,
        string eventType)
    {
        var guidOnOrigin = await AppendEventAsync("{\"name\":\"for test...\"}", stream, eventType, senderBuilder);
        Logger.LogInformation("The ID saved on the origin database is {Id}", guidOnOrigin);

        await Task.Delay(3000); // Give time for replication

        var guidOnDestination = await ReadLastEventIdAsync(stream, destinationBuilder);
        if (guidOnDestination == guidOnOrigin)
        {
            Logger.LogInformation("The last replicated ID on the destination database is {Id}", guidOnDestination);
            Logger.LogInformation("The test event has been replicated correctly!");
        }
        else
        {
            Logger.LogError("The test event has NOT been replicated correctly.");
        }
    }

    private static async Task<Guid> AppendEventAsync(
        string jsonBody,
        string stream,
        string eventType,
        ILinkerConnectionBuilder senderBuilder)
    {
        await using var conn = senderBuilder.Build();
        var guid = Guid.NewGuid();

        var data = Encoding.UTF8.GetBytes(jsonBody);
        var evt = new EventData(Uuid.FromGuid(guid), eventType, data);

        await conn.AppendToStreamAsync(stream, StreamState.Any, new[] { evt });
        return guid;
    }

    private static async Task<Guid?> ReadLastEventIdAsync(
        string stream,
        ILinkerConnectionBuilder destinationBuilder)
    {
        await using var conn = destinationBuilder.Build();
        const int batchSize = 1;

        var result = conn.ReadStreamAsync(Direction.Backwards, stream, batchSize);

        await foreach (var resolvedEvent in result)
        {
            return resolvedEvent.Event.EventId.ToGuid();
        }

        return null;
    }
}
