using Linker.Contracts;
using Linker.Model;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Linker
{
    public interface ILinkerService
    {
        Task<bool> Start();
        Task<bool> Stop();
        IDictionary<string, dynamic> GetStats();
        Task EventAppeared(ILinkerAllCatchUpSubscription eventStoreCatchUpSubscription,
            LinkerResolvedEvent resolvedEvent);
    }
}
