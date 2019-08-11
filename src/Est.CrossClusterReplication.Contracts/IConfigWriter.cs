using System.Threading.Tasks;
using Est.CrossClusterReplication.Model;

namespace Est.CrossClusterReplication.Contracts
{
    public interface IConfigWriter
    {
        void Set(CrossClusterReplica replica);
    }
}
