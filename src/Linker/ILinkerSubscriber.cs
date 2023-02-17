namespace Linker;

public interface ILinkerSubscriber
{
    Task SubscribeToAllFrom(Position position, LinkerCatchUpSubscriptionSettings settings,
        LinkerResolvedEvent eventAppeared, LinkerLiveProcessingStarted linkerLiveProcessingStarted,
        LinkerSubscriptionDropped linkerSubscriptionDropped);
}