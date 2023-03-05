namespace Linker;

public class LinkerSettings
{
    public int SynchronisationInterval { get; }
    public int StatsInterval { get; }
    public int MaxBufferSize { get; }
    public int MaxLiveQueue { get; }
    public int ReadBatchSize { get; }
    public bool HandleConflicts { get; }
    public bool ResolveLinkTos { get; }
    public static LinkerSettings Default()
    {
        return new LinkerSettings(LinkerSettingsDefaults.SynchronisationInterval,
            LinkerSettingsDefaults.HandleConflicts, LinkerSettingsDefaults.StatsInterval,
            LinkerSettingsDefaults.MaxBufferSize, LinkerSettingsDefaults.MaxLiveQueue,
            LinkerSettingsDefaults.ReadBatchSize, LinkerSettingsDefaults.ResolveLinkTos);
    }
    public LinkerSettings(int synchronisationInterval, bool handleConflicts, int statsInterval, int maxBufferSize,
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