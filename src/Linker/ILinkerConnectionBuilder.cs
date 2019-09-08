using System;
using EventStore.ClientAPI;

namespace Linker
{
    public interface ILinkerConnectionBuilder
    {
        Uri ConnectionString { get; }
        ConnectionSettings ConnectionSettings { get; }
        string ConnectionName { get; }
        IEventStoreConnection Build();
    }
}
