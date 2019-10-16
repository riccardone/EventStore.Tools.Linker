namespace Linker
{
    public class Settings
    {
        public int SynchronisationInterval { get; }
        public int StatsInterval { get; }
        public int MaxBufferSize { get; }
        public int MaxLiveQueue { get; }
        public int ReadBatchSize { get; }
        public bool HandleConflicts { get; }
        public bool ResolveLinkTos { get; }
        public static Settings Default()
        {
            return new Settings(SettingsDefaults.SynchronisationInterval,
                SettingsDefaults.HandleConflicts, SettingsDefaults.StatsInterval,
                SettingsDefaults.MaxBufferSize, SettingsDefaults.MaxLiveQueue,
                SettingsDefaults.ReadBatchSize, SettingsDefaults.ResolveLinkTos);
        }
        public Settings(int synchronisationInterval, bool handleConflicts, int statsInterval, int maxBufferSize,
            int maxLiveQueue, int readBatchSize, bool resolveLinkTos)
        {
            SynchronisationInterval = synchronisationInterval;
            HandleConflicts = handleConflicts;
            StatsInterval = statsInterval;
            MaxBufferSize = maxBufferSize;
            MaxLiveQueue = maxLiveQueue;
            ReadBatchSize = readBatchSize;
            ResolveLinkTos = resolveLinkTos;
        }
    }
}
