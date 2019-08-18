using System.Collections.Generic;

namespace Est.CrossClusterReplication
{
    public interface IReader<T>
    {
        IEnumerable<T> Get();
    }
}
