using Est.CrossClusterReplication.Model;

namespace Est.CrossClusterReplication.Contracts
{
    public interface IConfigProvider
    {
        Root GetSettings();
    }
}
