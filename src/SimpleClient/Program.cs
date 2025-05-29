namespace SimpleClient
{
    class Program
    {
        // For testing you can run two EventStore instances on your dev machine with the following settings
        // --int-tcp-port=1111 --ext-tcp-port=1112 --int-http-port=1113 --ext-http-port=1114
        // --int-tcp-port=2111 --ext-tcp-port=2112 --int-http-port=2113 --ext-http-port=2114

        private static readonly Logger Log = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            ConfigureLogging();
            var connSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials("admin", "changeit"));
            var origin = new LinkerConnectionBuilder(new Uri("tcp://localhost:1112"), connSettings, "origin-01");
            var destination = new LinkerConnectionBuilder(new Uri("tcp://localhost:2112"), connSettings, "destination-01");
            var service = new LinkerService(origin, destination, new FilterService(new List<Filter>
                {
                    new Filter(FilterType.EventType, "User*", FilterOperation.Include),
                    new Filter(FilterType.Stream, "domain-*", FilterOperation.Include),
                    new Filter(FilterType.EventType, "Basket*", FilterOperation.Exclude)
                }), Settings.Default(), new NLogLogger());
            service.Start().Wait();
            Log.Info("Replica Service started");
            TestReplicaForSampleEvent(connSettings, destination, "domain-test-01", "UserReplicaTested");
            Log.Info("Press enter to exit the program");
            Console.ReadLine();
        }

        private static void TestReplicaForSampleEvent(ConnectionSettings connSettings, ILinkerConnectionBuilder connBuilder, string stream, string eventType)
        {
            var senderForTestEvents = new LinkerConnectionBuilder(new Uri("tcp://localhost:1112"), connSettings, "sender");
            var guidOnOrigin = AppendEvent("{name:'for test...'}", stream, eventType, senderForTestEvents);
            Log.Info($"the id saved on the origin database is {guidOnOrigin}");
            Thread.Sleep(3000);
            var guidOnDestination = ReadLastEventId(stream, connBuilder);
            if (guidOnDestination.Equals(guidOnOrigin))
            {
                Log.Info($"the last replicated id on the destination database is {guidOnDestination}");
                Log.Info("The test event has been replicated correctly!");
            }
            else
                Log.Error("The test event has not been replicated correctly");
        }

        private static Guid AppendEvent(string body, string stream, string eventType, ILinkerConnectionBuilder senderConnectionBuilder)
        {
            using (var conn = senderConnectionBuilder.Build())
            {
                conn.ConnectAsync().Wait();
                var guid = Guid.NewGuid();
                conn.AppendToStreamAsync(stream, ExpectedVersion.Any,
                    new List<EventData> { new EventData(guid, eventType, true, Encoding.ASCII.GetBytes(body), null) }).Wait();
                return guid;
            }
        }

        private static Guid? ReadLastEventId(string stream, ILinkerConnectionBuilder destination)
        {
            using (var conn = destination.Build())
            {
                conn.ConnectAsync().Wait();
                var result = conn.ReadEventAsync(stream, StreamPosition.End, false).Result;
                return result.Status == EventReadStatus.NoStream ? null : result.Event?.Event.EventId;
            }
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
}
