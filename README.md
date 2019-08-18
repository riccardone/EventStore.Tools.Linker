# eventstore-tools
Toolset to add features to EventStore OSS  

# EventStoreReplica a tool for cross cluster replication
This is a .Net Standard library for replicating user data between EventStore clusters or single instances. It is dependant on the latest EventStore.Client nuget available at the time. If you need to reference a different version of EventStore.Client fork this repo and change it instead of using as a nuget.  
  
You can reference this project using Nuget
```
PM> Install-Package EventStoreReplica  
```

# Example use
I implemented this tool as a library in order to host in any program that fit your requirements and let you configure it as you wish. 
An example of it running in a .Net Core Console application and replicating data between two single EventStore instances running on the same machine but on different tcp ports (1112, 2112)  
```c#
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
            Log.Info("Press enter to exit the program");
            Console.ReadLine();
        }
    }
```
To use the ReplicaService you pass the origin and the destination of the data replication. It is possible run multiple ReplicationService's for more complex scenarios. Eact ReplicationService is a link between Origin and Destination. The Position of the replica can be saved on both sides but it's better to save it on the destination. 

# Backpressure and performances 
One of the problem that this tool solve is related to the lack of built in backpressure management in the EventStore clients and api's. Running this program, the logic will adapt the number of events to be replicated based on the network speed and with dynamic settings.
  
# Next development  
As soon as I have some time, I will start building a UI to manage and monitor the cross cluster replica between EventStore's. It could be done as a Web application and/or as a Cli program. Get in touch if you are willing to help (open an issue on this repo).
  
# EventStore
The database being replicated is EventStore https://eventstore.org/   
A big thanks to Greg Young and the rest of the team for this great product.