namespace Est.CrossClusterReplication.Model
{
    public enum FilterOperation
    {
        Include,
        Exclude
    }

    public enum FilterType
    {
        Stream,
        EventType
    }

    public class ReplicaFilter
    {
        public FilterType FilterType { get; set; }
        public string Value { get; set; }
        public FilterOperation FilterOperation { get; set; }

        public ReplicaFilter(FilterType filterType, string value, FilterOperation filterOperation)
        {
            FilterType = filterType;
            Value = value;
            FilterOperation = filterOperation;
        }

        public ReplicaFilter()
        {
            
        }
    }
}
