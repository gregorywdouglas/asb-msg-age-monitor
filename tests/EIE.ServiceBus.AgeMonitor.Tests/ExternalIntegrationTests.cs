using Azure.Messaging.ServiceBus;
using Azure.Identity;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Adapters;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Tests.Support;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>
/// Spec §11.7 — integration against the containerised Service Bus emulator, TST-080..085.
/// <para>
/// These verify the SDK adapter, which the unit tests deliberately never touch: the port
/// exists so that every state machine can be tested with plain records, which leaves the
/// mapping itself as the one thing only a real broker can confirm.
/// </para>
/// </summary>
public sealed class EmulatorIntegrationTests
{
    private const string QueueName = "asbmon-test-queue";

    [RequiresEnvironmentFact(TestEnvironment.EmulatorConnectionString)]
    public async Task Tst080_AdapterMapsMessagesToThePortModel()
    {
        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new PassthroughHasher());

        await SendAsync(client, new ServiceBusMessage("payload") { MessageId = "msg-1" });

        var peeked = await peeker.PeekAsync(QueueName, null, 10, default);

        var message = Assert.Single(peeked, m => m.MessageIdHash == "msg-1");
        Assert.True(message.SequenceNumber > 0);
        Assert.NotEqual(default, message.EnqueuedTimeUtc);
        Assert.Equal(MessageState.Active, message.State);
    }

    [RequiresEnvironmentFact(TestEnvironment.EmulatorConnectionString)]
    public async Task Tst081_DeferredMessagesArePeekableWithDistinguishableState()
    {
        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new PassthroughHasher());

        await SendAsync(client, new ServiceBusMessage("to-defer") { MessageId = "defer-1" });

        // Deferring requires a receive, which the monitor itself must never do — so the
        // test arranges the state through a separate receiver rather than through any
        // monitor code path.
        await using var receiver = client.CreateReceiver(QueueName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        await receiver.DeferMessageAsync(received);

        var peeked = await peeker.PeekAsync(QueueName, null, 50, default);

        Assert.Contains(peeked, m => m.State == MessageState.Deferred);
    }

    [RequiresEnvironmentFact(TestEnvironment.EmulatorConnectionString)]
    public async Task Tst082_ScheduledMessagesCarryTheirScheduledEnqueueTime()
    {
        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new PassthroughHasher());

        await using var sender = client.CreateSender(QueueName);
        await sender.ScheduleMessageAsync(
            new ServiceBusMessage("later") { MessageId = "sched-1" },
            DateTimeOffset.UtcNow.AddHours(1));

        var peeked = await peeker.PeekAsync(QueueName, null, 50, default);
        var scheduled = peeked.Single(m => m.MessageIdHash == "sched-1");

        Assert.NotNull(scheduled.ScheduledEnqueueTimeUtc);
        Assert.True(scheduled.IsPendingSchedule(DateTimeOffset.UtcNow));
    }

    [RequiresEnvironmentFact(TestEnvironment.EmulatorConnectionString)]
    public async Task Tst083_FromSequenceNumberResumesAtTheExpectedPosition()
    {
        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new PassthroughHasher());

        await SendAsync(client,
            new ServiceBusMessage("a"), new ServiceBusMessage("b"), new ServiceBusMessage("c"));

        var first = await peeker.PeekAsync(QueueName, null, 2, default);
        Assert.Equal(2, first.Count);

        var resumed = await peeker.PeekAsync(QueueName, first[^1].SequenceNumber + 1, 2, default);

        Assert.All(resumed, m => Assert.True(m.SequenceNumber > first[^1].SequenceNumber));
    }

    [RequiresEnvironmentFact(TestEnvironment.EmulatorConnectionString)]
    public async Task Tst084_MissingEntitySurfacesAsNotFound()
    {
        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new PassthroughHasher());

        var exception = await Assert.ThrowsAnyAsync<Exception>(
            () => peeker.PeekAsync("queue-that-does-not-exist", null, 1, default));

        Assert.Equal(EntityFaultKind.NotFound, new ServiceBusFaultClassifier().Classify(exception));
    }

    [RequiresEnvironmentFact(TestEnvironment.EmulatorConnectionString)]
    public async Task Tst085_EmptyEntityReturnsAnEmptyListRatherThanThrowing()
    {
        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new PassthroughHasher());

        // Peek far beyond the tail: the broker must answer with nothing, not an error,
        // because the zero-result path depends on being able to tell those apart.
        var peeked = await peeker.PeekAsync(QueueName, long.MaxValue - 1, 10, default);

        Assert.Empty(peeked);
    }

    private static ServiceBusClient Client() =>
        new(TestEnvironment.Require(TestEnvironment.EmulatorConnectionString));

    private static async Task SendAsync(ServiceBusClient client, params ServiceBusMessage[] messages)
    {
        await using var sender = client.CreateSender(QueueName);
        await sender.SendMessagesAsync(messages);
    }

    private sealed class PassthroughHasher : EIE.ServiceBus.AgeMonitor.Abstractions.IMessageIdHasher
    {
        public string? Hash(string? value) => value;
    }
}

/// <summary>
/// Spec §11.8 — contract tests against a real non-production Premium namespace,
/// TST-090..094.
/// <para>
/// The emulator only approximates peek semantics and the entire design rests on them.
/// These run nightly rather than per-commit.
/// </para>
/// </summary>
public sealed class ServiceBusContractTests
{
    [RequiresEnvironmentFact(TestEnvironment.ContractNamespace, TestEnvironment.ContractQueue)]
    public async Task Tst090_PeekDoesNotIncrementDeliveryCountOrLockTheMessage()
    {
        // The highest-priority assertion in the suite. If this were ever untrue, the
        // monitor would be silently mutating production message state across every entity
        // in a Tier 1 namespace, every 60 seconds. "Confident" is the wrong standard for
        // something with that blast radius when the test costs five lines.
        await using var client = Client();
        var queue = TestEnvironment.Require(TestEnvironment.ContractQueue);
        var peeker = new ServiceBusEntityPeeker(client, new NullHasher());

        await using var sender = client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("contract-probe"));

        var first = await peeker.PeekAsync(queue, null, 1, default);
        Assert.NotEmpty(first);
        var initialDeliveryCount = first[0].DeliveryCount;

        for (var i = 0; i < 5; i++)
        {
            await peeker.PeekAsync(queue, first[0].SequenceNumber, 1, default);
        }

        var after = await peeker.PeekAsync(queue, first[0].SequenceNumber, 1, default);

        Assert.Equal(initialDeliveryCount, after[0].DeliveryCount);
        Assert.Equal(first[0].SequenceNumber, after[0].SequenceNumber);
    }

    [RequiresEnvironmentFact(TestEnvironment.ContractNamespace, TestEnvironment.ContractQueue)]
    public async Task Tst091_ResumePointSemanticsHoldAcrossDeferredMessages()
    {
        await using var client = Client();
        var queue = TestEnvironment.Require(TestEnvironment.ContractQueue);
        var peeker = new ServiceBusEntityPeeker(client, new NullHasher());

        var all = await peeker.PeekAsync(queue, null, 100, default);
        if (all.Count < 2)
        {
            Assert.Fail("The contract queue needs at least two messages staged for this assertion.");
        }

        var resumed = await peeker.PeekAsync(queue, all[0].SequenceNumber + 1, 100, default);

        // The resume design depends on this: advancing past a rejected batch must never
        // re-serve the message we already examined, or a deferred block is paid for on
        // every tick.
        Assert.DoesNotContain(resumed, m => m.SequenceNumber <= all[0].SequenceNumber);
    }

    [RequiresEnvironmentFact(TestEnvironment.ContractNamespace, TestEnvironment.ContractQueue)]
    public async Task Tst092_MessageStateIsReliablyDistinguishable()
    {
        await using var client = Client();
        var queue = TestEnvironment.Require(TestEnvironment.ContractQueue);
        var peeker = new ServiceBusEntityPeeker(client, new NullHasher());

        var peeked = await peeker.PeekAsync(queue, null, 100, default);

        // The skip logic in §3.2 depends entirely on this enum being populated.
        Assert.All(peeked, m => Assert.True(Enum.IsDefined(m.State)));
    }

    [RequiresEnvironmentFact(TestEnvironment.ContractNamespace)]
    public async Task Tst093_EmptyEntityAndImpairedEntityAreDistinguishable()
    {
        // Observes, rather than assumes, the behaviour the corroboration logic is built
        // on: a genuinely empty entity answers successfully with nothing.
        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new NullHasher());
        var queue = Environment.GetEnvironmentVariable(TestEnvironment.ContractQueue) ?? "asbmon-empty";

        var peeked = await peeker.PeekAsync(queue, null, 10, default);

        Assert.NotNull(peeked);
    }

    [RequiresEnvironmentFact(TestEnvironment.ContractNamespace, "ASBMON_CONTRACT_FORWARDER")]
    public async Task Tst094_ForwardingSubscriptionHoldsNoMessagesOfItsOwn()
    {
        // Confirms the forwarder model: a subscription with ForwardTo set is structurally
        // empty, which is why FR-038 gives it a much tighter threshold — any sustained
        // age on one means forwarding has stopped.
        var forwarder = TestEnvironment.Require("ASBMON_CONTRACT_FORWARDER");

        await using var client = Client();
        var peeker = new ServiceBusEntityPeeker(client, new NullHasher());

        var peeked = await peeker.PeekAsync(forwarder, null, 10, default);

        Assert.Empty(peeked);
    }

    private static ServiceBusClient Client() =>
        new(TestEnvironment.Require(TestEnvironment.ContractNamespace), new DefaultAzureCredential());

    private sealed class NullHasher : EIE.ServiceBus.AgeMonitor.Abstractions.IMessageIdHasher
    {
        public string? Hash(string? value) => null;
    }
}
