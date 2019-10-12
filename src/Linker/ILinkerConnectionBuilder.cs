using System;
using EventStore.ClientAPI;

namespace Linker
{
    public interface ILinkerConnectionBuilder
    {
        [Obsolete]
        Uri ConnectionString { get; }
        [Obsolete]
        ConnectionSettings ConnectionSettings { get; }
        string ConnectionName { get; }
        IEventStoreConnection Build();
    }
}
