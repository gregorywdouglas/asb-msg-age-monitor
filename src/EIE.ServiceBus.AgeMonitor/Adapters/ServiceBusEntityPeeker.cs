using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Adapters;

/// <summary>
/// The single Service Bus data-plane adapter. Spec FR-050: this type calls
/// <c>PeekMessagesAsync</c> and nothing else — no receive, complete, abandon, defer,
/// dead-letter or lock-renewal operation appears anywhere in the assembly, which
/// TST-070 enforces at build time.
/// </summary>
public sealed class ServiceBusEntityPeeker : IEntityPeeker, IAsyncDisposable
{
    private const string SubscriptionSeparator = "/Subscriptions/";

    private readonly ServiceBusClient _client;
    private readonly IMessageIdHasher _hasher;
    private readonly ConcurrentDictionary<string, ServiceBusReceiver> _receivers =
        new(StringComparer.OrdinalIgnoreCase);

    public ServiceBusEntityPeeker(ServiceBusClient client, IMessageIdHasher hasher)
    {
        _client = client;
        _hasher = hasher;
    }

    public async Task<IReadOnlyList<PeekedMessage>> PeekAsync(
        string entityPath,
        long? fromSequenceNumber,
        int maxMessages,
        CancellationToken ct)
    {
        var receiver = _receivers.GetOrAdd(entityPath, CreateReceiver);

        var messages = await receiver
            .PeekMessagesAsync(maxMessages, fromSequenceNumber, ct)
            .ConfigureAwait(false);

        var mapped = new List<PeekedMessage>(messages.Count);
        foreach (var message in messages)
        {
            mapped.Add(Map(message));
        }

        return mapped;
    }

    private PeekedMessage Map(ServiceBusReceivedMessage message) => new(
        message.SequenceNumber,
        message.EnqueuedTime,
        message.DeliveryCount,
        MapState(message.State),
        message.TimeToLive,
        message.ScheduledEnqueueTime == default ? null : message.ScheduledEnqueueTime,
        _hasher.Hash(message.MessageId),
        _hasher.Hash(message.CorrelationId));

    private static MessageState MapState(ServiceBusMessageState state) => state switch
    {
        ServiceBusMessageState.Deferred => MessageState.Deferred,
        ServiceBusMessageState.Scheduled => MessageState.Scheduled,
        _ => MessageState.Active
    };

    /// <summary>
    /// Receivers are cached because creating one opens an AMQP link; doing that per
    /// entity per tick would add exactly the namespace load the scan budget exists to
    /// contain.
    /// </summary>
    private ServiceBusReceiver CreateReceiver(string entityPath)
    {
        var separatorIndex = entityPath.IndexOf(SubscriptionSeparator, StringComparison.OrdinalIgnoreCase);

        return separatorIndex < 0
            ? _client.CreateReceiver(entityPath)
            : _client.CreateReceiver(
                entityPath[..separatorIndex],
                entityPath[(separatorIndex + SubscriptionSeparator.Length)..]);
    }

    public void Forget(string entityPath)
    {
        if (_receivers.TryRemove(entityPath, out var receiver))
        {
            _ = receiver.DisposeAsync().AsTask();
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var receiver in _receivers.Values)
        {
            await receiver.DisposeAsync().ConfigureAwait(false);
        }

        _receivers.Clear();
        await _client.DisposeAsync().ConfigureAwait(false);
    }
}
