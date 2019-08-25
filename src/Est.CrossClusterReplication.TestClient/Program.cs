using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using NLog;

namespace Est.CrossClusterReplication.TestClient
{
    class Program
    {
        // For testing you can run two EventStore instances on your dev machine with the following settings
        // --int-tcp-port=1111 --ext-tcp-port=1112 --int-http-port=1113 --ext-http-port=1114
        // --int-tcp-port=2111 --ext-tcp-port=2112 --int-http-port=2113 --ext-http-port=2114

        private static readonly Logger Log = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            var connSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials("admin", "changeit"));
            var origin = new ConnectionBuilder(new Uri("tcp://localhost:1112"), connSettings, "origin-01");
            var destination = new ConnectionBuilder(new Uri("tcp://localhost:2112"), connSettings, "destination-01");
            var positionRepo = new PositionRepository($"PositionStream-{destination.ConnectionName}", "PositionUpdated", destination);
            var service = new ReplicaService(origin, destination, positionRepo,
                new FilterService(new List<ReplicaFilter>
                {
                    new ReplicaFilter(FilterType.EventType, "User*", FilterOperation.Include),
                    new ReplicaFilter(FilterType.Stream, "domain-*", FilterOperation.Include),
                    new ReplicaFilter(FilterType.EventType, "Basket*", FilterOperation.Exclude)
                }), 1000, false);
            service.Start().Wait();
            Log.Info("Replica Service started");
            TestReplicaForSampleEvent(connSettings, destination, "test", "ReplicaTested");
            Log.Info("Press enter to exit the program");
            Console.ReadLine();
        }

        private static void TestReplicaForSampleEvent(ConnectionSettings connSettings, IConnectionBuilder connBuilder, string stream, string eventType)
        {
            var senderForTestEvents = new ConnectionBuilder(new Uri("tcp://localhost:1112"), connSettings, "sender");
            var guidOnOrigin = AppendEvent("{name:'for test...'}", stream, eventType, senderForTestEvents);
            Log.Info($"the id saved on the origin database is {guidOnOrigin}");
            Thread.Sleep(3000);
            var guidOnDestination = ReadLastEventId(stream, connBuilder);
            Log.Info($"the last replicated id on the destination database is {guidOnDestination}");
            if (guidOnDestination.Equals(guidOnOrigin))
                Log.Info("The test event has been replicated correctly!");
            else
                Log.Warn("The test event has not been replicated correctly");
        }

        private static Guid AppendEvent(string body, string stream, string eventType, IConnectionBuilder sender)
        {
            using (var conn = sender.Build())
            {
                conn.ConnectAsync().Wait();
                var guid = Guid.NewGuid();
                conn.AppendToStreamAsync(stream, ExpectedVersion.Any,
                    new List<EventData> { new EventData(guid, eventType, true, Encoding.ASCII.GetBytes(body), null) }).Wait();
                return guid;
            }
        }

        private static Guid? ReadLastEventId(string stream, IConnectionBuilder destination)
        {
            using (var conn = destination.Build())
            {
                conn.ConnectAsync().Wait();
                var boh = conn.ReadEventAsync(stream, StreamPosition.End, false).Result;
                return boh.Status == EventReadStatus.NoStream ? null : boh.Event?.Event.EventId;
            }
        }
    }
}
