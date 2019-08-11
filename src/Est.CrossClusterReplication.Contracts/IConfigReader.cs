using System.Collections.Generic;
using Est.CrossClusterReplication.Model;

namespace Est.CrossClusterReplication.Contracts
{
    public interface IConfigReader
    {
        IList<CrossClusterReplica> Get();
        CrossClusterReplica Get(string id);
    }
}
