using System;
using Linker.Contracts;

namespace Linker
{
    public class LinkerConnectionBuilder : ILinkerConnectionBuilder
    {
        public Uri ConnectionString { get; }
        public LinkerConnectionSettings ConnectionSettings { get; }
        public string ConnectionName { get; }

        public ILinkerConnection Build()
        {
            throw new NotImplementedException();
            //return EventStoreConnection.Create(ConnectionSettings, ConnectionString, ConnectionName);
        }

        public LinkerConnectionBuilder(Uri connectionString, LinkerConnectionSettings connectionSettings, string connectionName)
        {
            ConnectionString = connectionString;
            ConnectionSettings = connectionSettings;
            ConnectionName = connectionName;
        }
    }
}
