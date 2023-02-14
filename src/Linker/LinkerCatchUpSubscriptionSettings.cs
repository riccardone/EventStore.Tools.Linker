namespace Linker;

public class LinkerCatchUpSubscriptionSettings
{
    public static LinkerCatchUpSubscriptionSettings Default { get; }
    public int MaxLiveQueueSize { get; set; }
    public int ReadBatchSize { get; set; }
}