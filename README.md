# eventstore-tools
Toolset to add features to EventStore OSS  

# EventStoreReplica a tool for cross cluster replication
This is a .Net Standard library for replicating user data between EventStore clusters or single instances. It requires a reference to the latest EventStore.Client nuget available at the time this tool was implemented or changed. If you need to reference a different version of EventStore.Client, you can fork the repo or open an issue to get a different nuget version. 
  
You can reference this project using Nuget
```
PM> Install-Package EventStoreReplica  
```

# Example use
I implemented this tool as a library/nuget for it to be hosted in any program that better fit your requirements and let you configure it as you wish. 
Following is an example of it running in a .Net Core Console application and replicating data between two single EventStore instances running on the same machine but on different tcp ports (1112, 2112). You can find the code in the TestClient folder (src/Est.CrossClusterReplication.TestClient).
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
            var service = new ReplicaService(origin, destination, positionRepo, null, 1000, false);
            service.Start().Wait();
            Log.Info("Replica Service started");            
            Log.Info("Press enter to exit the program");
            Console.ReadLine();
        }
    }
```
To use the ReplicaService you pass the origin and the destination of the data replication. Eact ReplicationService is a link between Origin and Destination. It is possible run multiple ReplicationService's for more complex scenarios. The Position of the replica can be saved on both sides but it's better to save it on the destination as in the example. 

# Backpressure and performances 
One of the problem that this tool solves is related to the lack of built-in backpressure management in the EventStore client's and api's. Running the replication with this program, the logic will continuosly adapt the network settings depending on the number of events being replicated.
  
# Next development  
As soon as I have time, I will start building a UI/service to manage and monitor the cross cluster replica between EventStore's. It could be done as a Web application and/or as a Cli program. Get in touch if you are willing to help (...open an issue on this repo).
  
# EventStore
The database being replicated is EventStore https://eventstore.org/   
A big thanks to Greg Young and the rest of the team for this great product.
