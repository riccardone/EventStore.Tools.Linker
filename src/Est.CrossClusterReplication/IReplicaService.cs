using System.Collections.Generic;
using System.Threading.Tasks;

namespace Est.CrossClusterReplication
{
    public interface IReplicaService
    {
        Task<bool> Start();
        Task<bool> Stop();
        IDictionary<string, dynamic> GetStats();
    }
}
