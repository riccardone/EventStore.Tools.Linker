using System.Threading.Tasks;
using Linker.Model;

namespace Linker.Contracts
{
    public interface ILinkerConnection
    {
        Task AppendToStreamAsync(string evStreamId, long evEventNumber, EventData[] eventDatas);
    }
}
