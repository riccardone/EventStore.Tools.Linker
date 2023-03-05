using EventStore.Client;
using static Linker.LinkerConnectionTcp;

namespace Linker;

public interface ILinkerConnection
{
    Task AppendToStreamAsync(string streamId, long eventNumber, LinkerEventData[] eventData);
    public event ConnectedEventHandler Connected;

    Task<StreamSubscription> SubscribeToAllAsync(FromAll start, Func<StreamSubscription, ResolvedEvent, CancellationToken, Task> eventAppeared, bool resolveLinkTos = false, Action<StreamSubscription, SubscriptionDroppedReason, Exception?>? subscriptionDropped = null, SubscriptionFilterOptions? filterOptions = null, UserCredentials? userCredentials = null, CancellationToken cancellationToken = default);

    Task Start();
    Task Stop();
}