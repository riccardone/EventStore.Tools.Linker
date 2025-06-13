[![Build and Test](https://github.com/riccardone/Linker/actions/workflows/ci.yml/badge.svg)](https://github.com/riccardone/Linker/actions/workflows/ci.yml)

# Linker: a tool for KurrentDb (former EventStore) cross cluster replication
This library is for replicating user data between EventStore clusters or single instances. More info on this article [Cross Data Center Replication with Linker](http://www.dinuzzo.co.uk/2019/11/17/cross-data-center-replication-with-linker/)

![image](https://github.com/user-attachments/assets/387137d2-a65e-4a19-a96f-e3ab57c58543)
Configure the origin and the destination of the data replication. Eact Link is between Origin and Destination. It is possible run multiple links at the same time for complex scenarios. To enforce ordering in the destination db Linker save stream positions on disk in a folder called data per default. Path is configurable as a setting if needed.

# Configuration Modes
This app make use of the appsettings.json file to configure Linked KurrentDb's. 
You can have as many Links as you need and as your running machine allow you to run. Even when you are replicating at full speed, the Linker logic make use of **backpressure** tecnique in order to not take the full amount of CPU and Memory available.  

The following properties can be configured in the root of your appsettings.json file. If not set, the default values shown will be used:
```
{
  "DataFolder": "data",               // Folder used to store stream position files
  "AutomaticTuning": true,           // Enables or disables dynamic buffer size tuning
  "BufferSize": 100,                 // Initial size of the bounded buffer channel
  "HandleConflicts": true,           // Enables appending conflicts to a special stream instead of failing
  "ResolveLinkTos": false,           // Whether to resolve $> link events in EventStore
  "Links": []                        // List of replication links (see examples below)
}
```

Each Link object defines one replication pair:
```
{
  "origin": {
    "connectionString": "esdb://...",
    "connectionName": "db01",
    "certificate": null,            // Optional client certificate (PEM format)
    "certificatePrivateKey": null   // Optional private key for the certificate
  },
  "destination": {
    "connectionString": "esdb://...",
    "connectionName": "db02",
    "certificate": null,
    "certificatePrivateKey": null
  },
  "filters": []                      // Optional list of filters (see filter section)
}
```

## Active-Passive
One instance is the origin, the other instance is the destination. To configure a simple link with a filter excluding one specific stream:
```
{
  "links": [
    {
      "origin": {
        "connectionString": "esdb://admin:changeit@localhost:2114?tls=false",
        "connectionName": "db01"
      },
      "destination": {
        "connectionString": "esdb://admin:changeit@localhost:2115?tls=false",
        "connectionName": "db02"
      },
      "filters": [
        {
          "filterType": "stream",
          "value": "diary-input",
          "filterOperation": "exclude"
        },
        {
          "filterType": "stream",
          "value": "*",
          "filterOperation": "include"
        }
      ]
    }
  ]
}
```

## MultiMaster  
Configure two links swapping the same Origin and Destination for a **multi master** ACTIVE-ACTIVE replication  
```
{
  "links": [
    {
      "origin": {
        "connectionString": "esdb://admin:changeit@localhost:2114?tls=false",
        "connectionName": "db01"
      },
      "destination": {
        "connectionString": "esdb://admin:changeit@localhost:2115?tls=false",
        "connectionName": "db02"
      },
      "filters": []
    },
    {
      "origin": {
        "connectionString": "esdb://admin:changeit@localhost:2115?tls=false",
        "connectionName": "db02"
      },
      "destination": {
        "connectionString": "esdb://admin:changeit@localhost:2114?tls=false",
        "connectionName": "db01"
      },
      "filters": []
    }
  ]
}
```
## Fan-Out
Configure the same Origin in separate Links replicating data to different destination for a **Fan-Out** solution.  

## Fan-In
You can have separate Links with different Origins linked with the same Destination for a **Fan-In** solution. 
 
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

# KurrentDb
The database being replicated is KurrentDb https://github.com/kurrent-io/KurrentDB 
