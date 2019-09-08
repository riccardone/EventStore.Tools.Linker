using System.Collections.Generic;
using System.Threading.Tasks;

namespace Linker
{
    public interface ILinkerService
    {
        Task<bool> Start();
        Task<bool> Stop();
        IDictionary<string, dynamic> GetStats();
    }
}
