using System.Text;
using EventStore.ClientAPI;
using Linker;
using NLog;
using NLog.Config;
using NLog.Targets;

namespace LinkerSimpleApp;

class Program
{
    // For testing you can run two EventStore instances on your dev machine with the following settings
    // --int-tcp-port=1111 --ext-tcp-port=1112 --int-http-port=1113 --ext-http-port=1114
    // --int-tcp-port=2111 --ext-tcp-port=2112 --int-http-port=2113 --ext-http-port=2114

    private static readonly Logger Log = LogManager.GetCurrentClassLogger();
    private static readonly Uri Origin01ConnectionString = new("tcp://admin:changeit@localhost:1112");
    private static readonly Uri Destination01ConnectionString = new("tcp://admin:changeit@localhost:2112");

    static void Main(string[] args)
    {
        ConfigureLogging();
        //var connSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials("admin", "changeit"));
        var origin = new LinkerConnectionBuilder(Origin01ConnectionString, "origin-01");
        var destination = new LinkerConnectionBuilder(Destination01ConnectionString, "destination-01");
        var service = new LinkerService(origin, destination, new FilterService(new List<Filter>
        {
            new(FilterType.EventType, "User*", FilterOperation.Include),
            new(FilterType.Stream, "domain-*", FilterOperation.Include),
            new(FilterType.EventType, "Basket*", FilterOperation.Exclude)
        }), LinkerSettings.Default(), destination.ConnectionString.ToString());
        service.Start().Wait();
        Log.Info("Replica Service started");
        TestReplicaForSampleEvent(destination, "domain-test-01", "UserReplicaTested");
        Log.Info("Press enter to exit the program");
        Console.ReadLine();
    }

    private static void TestReplicaForSampleEvent(ILinkerConnectionBuilder connBuilder, string stream, string eventType)
    {
        var senderForTestEvents = EventStoreConnection.Create(Origin01ConnectionString, "appenderToOrigin");
        var guidOnOrigin = AppendEvent("{name:'for test...'}", stream, eventType, senderForTestEvents);
        Log.Info($"the id saved on the origin database is {guidOnOrigin}");
        Thread.Sleep(3000);
        var destination = EventStoreConnection.Create(Destination01ConnectionString, "readerFromDestination");
        var guidOnDestination = ReadLastEventId(stream, destination);
        if (guidOnDestination.Equals(guidOnOrigin))
        {
            Log.Info($"the last replicated id on the destination database is {guidOnDestination}");
            Log.Info("The test event has been replicated correctly!");
        }
        else
            Log.Error("The test event has not been replicated correctly");
        senderForTestEvents.Close();
    }

    private static Guid AppendEvent(string body, string stream, string eventType, IEventStoreConnection conn)
    {
        conn.ConnectAsync().Wait();
        var guid = Guid.NewGuid();
        conn.AppendToStreamAsync(stream, EventStore.ClientAPI.ExpectedVersion.Any,
            new List<EventData> { new EventData(guid, eventType, true, Encoding.ASCII.GetBytes(body), null) }).Wait();
        conn.Close();
        return guid;
    }

    private static Guid? ReadLastEventId(string stream, IEventStoreConnection destination)
    {
        destination.ConnectAsync().Wait();
        var result = destination.ReadEventAsync(stream, StreamPosition.End, false).Result;
        destination.Close();
        return result.Status == EventReadStatus.NoStream ? null : result.Event?.Event.EventId;
    }

    private static void ConfigureLogging()
    {
        var config = new LoggingConfiguration();
        var consoleTarget = new ConsoleTarget("target1")
        {
            Layout = @"${date:format=HH\:mm\:ss} ${level} ${message} ${exception}"
        };
        config.AddTarget(consoleTarget);
        config.AddRuleForAllLevels(consoleTarget); // all to console
        LogManager.Configuration = config;
    }
}