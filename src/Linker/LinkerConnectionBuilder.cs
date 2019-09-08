using System;
using EventStore.ClientAPI;

namespace Linker
{
    public class LinkerConnectionBuilder : ILinkerConnectionBuilder
    {
        public Uri ConnectionString { get; }
        public ConnectionSettings ConnectionSettings { get; }
        public string ConnectionName { get; }

        public IEventStoreConnection Build()
        {
            return EventStoreConnection.Create(ConnectionSettings, ConnectionString, ConnectionName);
        }

        public LinkerConnectionBuilder(Uri connectionString, ConnectionSettings connectionSettings, string connectionName)
        {
            ConnectionString = connectionString;
            ConnectionSettings = connectionSettings;
            ConnectionName = connectionName;
        }
    }
}
