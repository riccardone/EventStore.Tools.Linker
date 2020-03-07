[![Build Status](https://dev.azure.com/riccardone/EventStore.Linker/_apis/build/status/riccardone.EventStore.Tools.Linker?branchName=master)](https://dev.azure.com/riccardone/EventStore.Linker/_build/latest?definitionId=7&branchName=master)

# Linker: a tool for EventStore cross cluster replication
This is a .Net Standard library for replicating user data between EventStore clusters or single instances. It requires a reference to the latest EventStore.Client nuget available at the time this tool was implemented or changed. If you need to reference a different version of EventStore.Client, you can fork the repo or open an issue to get a different nuget version.  
  
Current EventStore.Client: v5.0.6  
  
You can reference this project using Nuget
```
PM> Install-Package Linker  
```

# Simplest example usage
I implemented this tool as a library/nuget for it to be hosted in any program that better fit your requirements and let you configure it as you wish. 
Following is an example of it running in a simple .Net Core Console application and replicating data between two single EventStore instances running on the same machine but on different tcp ports (1112, 2112) and http ports (1114, 2114). If you are looking for more, the [LinkerConsoleApp](https://github.com/riccardone/EventStore.Tools.LinkerConsoleApp) is a configurable App using Linker. 
```c#
class Program
    {
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        // For testing you can run two EventStore instances on your dev machine with the following settings
        // --int-tcp-port=1111 --ext-tcp-port=1112 --int-http-port=1113 --ext-http-port=1114
        // --int-tcp-port=2111 --ext-tcp-port=2112 --int-http-port=2113 --ext-http-port=2114

        static void Main(string[] args)
        {
            var connSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials("admin", "changeit"));
            var origin = new LinkerConnectionBuilder(new Uri("tcp://localhost:1112"), connSettings, "origin-01");
            var destination = new LinkerConnectionBuilder(new Uri("tcp://localhost:2112"), connSettings, "destination-01");            
            var service = new LinkerService(origin, destination, null, Settings.Default());
            service.Start().Wait();
            Log.Info("Replica Service started");            
            Log.Info("Press enter to exit the program");
            Console.ReadLine();
        }
    }
```
To use the LinkerService you pass the origin and the destination of the data replication. Eact LinkerService is a link between Origin and Destination. It is possible run multiple LinkerService's for more complex scenarios. The Position of the replica can be saved on both sides but it's better to save it on the destination. If you want to control where to save the position you can build your PositionRepository and pass it to the LinkerService.  
If you don't want to build your own application using the Linker nuget than you can use the [Linker Console App](https://github.com/riccardone/EventStore.Tools.LinkerConsoleApp). It's a simple configurable app that can cover most of the scenario's.
  
# Use filters 
Without filters, all the user data will be replicated from the Origin to the linked Destination. When you add an exclude filter you must also add at least an include filter to include what else can be replicated.
## Include streams filters
You can set a inclusion filter to specify which stream or streams are to be replicated. This will automatically exclude anything else. You can use the wildcard * in the stream string so that you can include any stream that start with 'domain-*' for example.  
Example to create an inclusive stream filter  
```c#
var filter = new Filter(FilterType.Stream, "domain-*", FilterOperation.Include);
```
## Exclude streams filters 
You can set a filter to exclude one or more streams that are not to be replicated. This will automatically include anything else. You can use the wildcard * in the stream string so that you can exclude any stream that start with 'rawdata-*' for example.  
Example to create a filter that exclude all streams starting with the word rawdata- 
```c#
var filter = new Filter(FilterType.Stream, "rawdata-*", FilterOperation.Exclude);
```
## Include EventType filters  
You can set a inclusion filter to specify which Event Type's are to be replicated. This will automatically exclude any other event type. You can use the wildcard * in the event type string so that you can include any event type that for exampe starts with 'User*'.  
Example to create an inclusive stream filter  
```c#
var filter = new Filter(FilterType.EventType, "User*", FilterOperation.Include);
```
## Exclude EventType filters 
You can set a filter to exclude one or more EventType's that must not be replicated. This will automatically include any other event type. You can use the wildcard * in the event type string so that you can exclude any event type that for example start with the word Basket.  
Example to create a filter that exclude all streams starting with the word Basket 
```c#
var filter = new Filter(FilterType.EventType, "Basket*", FilterOperation.Exclude);
```
## Combine filters
Use of filters is optional. If you don't set any filter then all the user data will be replicated from origin to destination. If you set for example an Exclude filter then you must set one or more Include to include for example all other streams or eventtypes or only a subset.  
Following is an example of building the LinkerService with a couple of Include filters and one Exclude
```c#
            var service = new LinkerService(origin, destination, 
                new FilterService(new List<Filter>
                {
                    new Filter(FilterType.EventType, "User*", FilterOperation.Include),
                    new Filter(FilterType.Stream, "domain-*", FilterOperation.Include),
                    new Filter(FilterType.EventType, "Basket*", FilterOperation.Exclude)
                }), 1000, false);
            service.Start().Wait();
```
# Backpressure and performances 
One of the problem that this tool solves is related to the lack of built-in backpressure management in the EventStore client's and api's. Running the replication with this program, the logic will continuosly adapt the network settings depending on the number of events being replicated.
  
# Next development  
As soon as I have time, I will start building a UI/service to manage and monitor the cross cluster replica between EventStore's. It could be done as a Web application and/or as a Cli program. If you are willing to help open an issue on this repo and get in touch. 
  
# EventStore
The database being replicated is EventStore https://eventstore.org/   
A big thanks to Greg Young and the rest of the team for this great product.
