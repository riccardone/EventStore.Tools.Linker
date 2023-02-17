namespace Linker;

public class LinkerSubscriber : ILinkerSubscriber
{
    public Task SubscribeToAllFrom(Position position, LinkerCatchUpSubscriptionSettings settings,
        LinkerResolvedEvent eventAppeared, LinkerLiveProcessingStarted linkerLiveProcessingStarted,
        LinkerSubscriptionDropped linkerSubscriptionDropped)
    {
        throw new NotImplementedException();
    }
}