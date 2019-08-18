using System;
using EventStore.ClientAPI;

namespace Est.CrossClusterReplication
{
    public interface IConnectionBuilder
    {
        Uri ConnectionString { get; }
        ConnectionSettings ConnectionSettings { get; }
        string ConnectionName { get; }
        IEventStoreConnection Build();
    }
}
