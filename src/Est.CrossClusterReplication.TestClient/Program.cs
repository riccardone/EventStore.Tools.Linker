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
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            var connSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials("admin", "changeit"));
            var origin = new ConnectionBuilder(new Uri("tcp://localhost:1112"), connSettings, "origin-01");
            var destination = new ConnectionBuilder(new Uri("tcp://localhost:2112"), connSettings, "destination-01");
            var positionRepo = new PositionRepository($"PositionStream-{destination.ConnectionName}", "PositionUpdated", destination);
            var sut = new ReplicaService(origin, destination, positionRepo, null, 1000, false);
            sut.Start().Wait();
            Log.Info("Replica Service started");
            TestReplicaForSampleEvent(connSettings, destination);
            Log.Info("Press enter to exit the program");
            Console.ReadLine();
        }

        private static void TestReplicaForSampleEvent(ConnectionSettings connSettings, IConnectionBuilder connBuilder)
        {
            var senderForTestEvents = new ConnectionBuilder(new Uri("tcp://localhost:1112"), connSettings, "sender");
            var testStream = "test";
            var guidOnOrigin = AppendEvent("{name:'for test...'}", testStream, senderForTestEvents);
            Log.Info($"the id saved on the origin database is {guidOnOrigin}");
            Thread.Sleep(5000);
            var guidOnDestination = ReadLastEventId(testStream, connBuilder);
            Log.Info($"the last replicated id on the destination database is {guidOnDestination}");
            if (guidOnDestination.Equals(guidOnOrigin))
                Log.Info("The test event has been replicated correctly!");
            else
                Log.Warn("The test event has not been replicated correctly");
        }


        private static Guid AppendEvent(string body, string eventType, IConnectionBuilder sender)
        {
            using (var conn = sender.Build())
            {
                conn.ConnectAsync().Wait();
                var guid = Guid.NewGuid();
                conn.AppendToStreamAsync("test", ExpectedVersion.Any,
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
