namespace Est.CrossClusterReplication
{
    public class ReplicaSettings
    {
        public int SynchronisationInterval { get; }
        public int StatsInterval { get; }
        public int MaxBufferSize { get; }
        public int MaxLiveQueue { get; }
        public int ReadBatchSize { get; }
        public bool HandleConflicts { get; }
        public static ReplicaSettings Default()
        {
            return new ReplicaSettings(ReplicaSettingsDefaults.SynchronisationInterval,
                ReplicaSettingsDefaults.HandleConflicts, ReplicaSettingsDefaults.StatsInterval,
                ReplicaSettingsDefaults.MaxBufferSize, ReplicaSettingsDefaults.MaxLiveQueue,
                ReplicaSettingsDefaults.ReadBatchSize);
        }
        public ReplicaSettings(int synchronisationInterval, bool handleConflicts, int statsInterval, int maxBufferSize,
            int maxLiveQueue, int readBatchSize)
        {
            SynchronisationInterval = synchronisationInterval;
            HandleConflicts = handleConflicts;
            StatsInterval = statsInterval;
            MaxBufferSize = maxBufferSize;
            MaxLiveQueue = maxLiveQueue;
            ReadBatchSize = readBatchSize;
        }
    }
}
