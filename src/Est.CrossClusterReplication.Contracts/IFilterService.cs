namespace Est.CrossClusterReplication.Contracts
{
    public interface IFilterService
    {
        bool IsValid(string eventType, string eventStreamId);
    }
}
