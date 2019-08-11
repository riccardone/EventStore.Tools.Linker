using System;
using Est.CrossClusterReplication.Contracts;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;

namespace Est.CrossClusterReplication
{
    public class ConnectionBuilder : IConnectionBuilder
    {
        public Uri ConnectionString { get; }
        public ConnectionSettings ConnectionSettings { get; }
        public string ConnectionName { get; }
        public UserCredentials Credentials { get; }

        public IEventStoreConnection Build()
        {
            return EventStoreConnection.Create(ConnectionSettings, ConnectionString, ConnectionName);
        }

        public ConnectionBuilder(Uri connectionString, ConnectionSettings connectionSettings, string connectionName, UserCredentials credentials)
        {
            ConnectionString = connectionString;
            ConnectionSettings = connectionSettings;
            ConnectionName = connectionName;
            Credentials = credentials;
        }
    }
}
