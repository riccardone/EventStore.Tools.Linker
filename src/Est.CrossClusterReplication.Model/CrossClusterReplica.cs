using System.Collections.Generic;

namespace Est.CrossClusterReplication.Model
{
    public class CrossClusterReplica
    {
        public string Name { get; set; }
        public int SavePositionInterval { get; set; }
        public bool AutoStart { get; set; }
        public bool HandleConflicts { get; set; }
        public List<ReplicaFilter> Filters { get; set; }
        public From From { get; set; }
        public To To { get; set; }
    }
}
