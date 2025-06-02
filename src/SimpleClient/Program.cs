using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Linker;
using NLog;
using NLog.Config;
using NLog.Targets;

namespace SimpleClient;

static class Program
{
    private static readonly Logger Log = LogManager.GetCurrentClassLogger();

    static async Task Main(string[] args)
    {
        ConfigureLogging();

        var origin = new LinkerConnectionBuilder(KurrentDBClientSettings.Create("esdb://admin:changeit@localhost:2113?tls=false"),  "origin-01");
        var destination = new LinkerConnectionBuilder(KurrentDBClientSettings.Create("esdb://admin:changeit@localhost:2123?tls=false"), "destination-01");

        var service = new LinkerService(
            origin,
            destination, new PositionRepository("DestinationPosition", "DestinationPosition", destination.Build()),
            new FilterService(new List<Filter>
            {
                new Filter(FilterType.EventType, "User*", FilterOperation.Include),
                new Filter(FilterType.Stream, "domain-*", FilterOperation.Include),
                new Filter(FilterType.EventType, "Basket*", FilterOperation.Exclude)
            }),
            Settings.Default(),
            new NLogLogger());

        await service.Start();
        Log.Info("Replica Service started");

        await TestReplicaForSampleEvent(origin, destination, "domain-test-01", "UserReplicaTested");

        Log.Info("Press enter to exit the program");
        Console.ReadLine();
    }

    private static async Task TestReplicaForSampleEvent(
        ILinkerConnectionBuilder senderBuilder,
        ILinkerConnectionBuilder destinationBuilder,
        string stream,
        string eventType)
    {
        var guidOnOrigin = await AppendEventAsync("{\"name\":\"for test...\"}", stream, eventType, senderBuilder);
        Log.Info($"The ID saved on the origin database is {guidOnOrigin}");

        await Task.Delay(3000); // Give time for replication

        var guidOnDestination = await ReadLastEventIdAsync(stream, destinationBuilder);
        if (guidOnDestination == guidOnOrigin)
        {
            Log.Info($"The last replicated ID on the destination database is {guidOnDestination}");
            Log.Info("The test event has been replicated correctly!");
        }
        else
        {
            Log.Error("The test event has NOT been replicated correctly.");
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

        // Read from the stream starting from the end, but since KurrentDB reads forward,
        // you'll need to fetch the last known count and subtract to read from near-end
        const int batchSize = 1;

        var result = conn.ReadStreamAsync(Direction.Backwards,stream, batchSize);

        await foreach (var resolvedEvent in result)
        {
            return resolvedEvent.Event.EventId.ToGuid();
        }

        return null;
    }

    private static void ConfigureLogging()
    {
        var config = new LoggingConfiguration();
        var consoleTarget = new ConsoleTarget("console")
        {
            Layout = @"${date:format=HH\:mm\:ss} ${level} ${message} ${exception}"
        };
        config.AddTarget(consoleTarget);
        config.AddRuleForAllLevels(consoleTarget);
        LogManager.Configuration = config;
    }
}
