namespace Est.CrossClusterReplication
{
    public interface IFilterService
    {
        bool IsValid(string eventType, string eventStreamId);
    }
}
