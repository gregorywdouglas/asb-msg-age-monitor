using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

// Spec §11.9 — the staging synthetic scenario generator.
//
// Without this, none of the interesting logic in the monitor is ever executed before
// production: dev and staging namespaces are near-idle, so the monitor emits Empty for
// every entity every tick and runs green for months. The first time the deferred-head,
// degradation or corroboration paths execute for real would otherwise be in production,
// during an incident.
//
// SAFETY: every entity this creates carries the reserved prefix below, and the tool
// refuses to run against a namespace whose name contains "prod". It must never be
// deployed to production.

const string ReservedPrefix = "asbmon-synthetic-";

if (args.Length < 2)
{
    PrintUsage();
    return 1;
}

var scenario = args[0];
var fullyQualifiedNamespace = args[1];

if (fullyQualifiedNamespace.Contains("prod", StringComparison.OrdinalIgnoreCase))
{
    Console.Error.WriteLine("Refusing to run against a namespace whose name contains 'prod'.");
    return 2;
}

var credential = new DefaultAzureCredential();
var admin = new ServiceBusAdministrationClient(fullyQualifiedNamespace, credential);
await using var client = new ServiceBusClient(fullyQualifiedNamespace, credential);

try
{
    switch (scenario)
    {
        case "aged-message":
            await AgedMessageAsync();
            break;
        case "deferred-head":
            await DeferredHeadAsync();
            break;
        case "entity-lifecycle":
            await EntityLifecycleAsync();
            break;
        case "burst":
            await BurstAsync();
            break;
        case "consumer-stopped":
            await ConsumerStoppedAsync();
            break;
        case "forwarding-broken":
            await ForwardingBrokenAsync();
            break;
        case "cleanup":
            await CleanupAsync();
            break;
        default:
            PrintUsage();
            return 1;
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Scenario '{scenario}' failed: {ex.Message}");
    return 3;
}

return 0;

// TST-100 — a message aged past Sev2, then Sev1. Expect ALR-001 then ALR-002 with a
// payload carrying absolute seconds and the threshold in force.
async Task AgedMessageAsync()
{
    var queue = await EnsureQueueAsync("aged");
    await using var sender = client.CreateSender(queue);

    await sender.SendMessageAsync(new ServiceBusMessage("aged-probe")
    {
        MessageId = $"aged-{Guid.NewGuid():N}"
    });

    Console.WriteLine($"Sent one message to {queue} and left it unconsumed.");
    Console.WriteLine("Expect: OldestMessageAgeSeconds climbing ~1s per second;");
    Console.WriteLine("        ALR-001 within ~90s of the Sev2 threshold in force,");
    Console.WriteLine("        ALR-002 within ~90s of the Sev1 threshold.");
}

// TST-101 — a deferred head deeper than the scan budget with Active messages behind it.
// Expect HeadBlockedByDeferred, then Measured once the resume point advances past the
// block on a subsequent tick.
async Task DeferredHeadAsync()
{
    var queue = await EnsureQueueAsync("deferred-head");
    const int deferredCount = 600;

    await using var sender = client.CreateSender(queue);
    var batch = Enumerable.Range(0, deferredCount)
        .Select(i => new ServiceBusMessage($"defer-{i}") { MessageId = $"defer-{i}" })
        .ToList();

    await sender.SendMessagesAsync(batch);

    // Deferring requires receiving, which the monitor itself must never do — that is the
    // whole point of the port boundary. The generator does it deliberately, as a
    // stand-in for the application that would defer in production.
    await using var receiver = client.CreateReceiver(queue);
    var deferred = 0;

    while (deferred < deferredCount)
    {
        var received = await receiver.ReceiveMessagesAsync(100, TimeSpan.FromSeconds(5));
        if (received.Count == 0)
        {
            break;
        }

        foreach (var message in received)
        {
            await receiver.DeferMessageAsync(message);
            deferred++;
        }
    }

    await sender.SendMessageAsync(new ServiceBusMessage("active-behind-the-block")
    {
        MessageId = "active-behind-block"
    });

    Console.WriteLine($"Deferred {deferred} messages at the head of {queue}, with one Active message behind them.");
    Console.WriteLine("Expect: MeasurementStatus=HeadBlockedByDeferred while the budget is exhausted,");
    Console.WriteLine("        then Measured once the resume point advances past the block,");
    Console.WriteLine("        and ScanBatchesUsed dropping sharply on that later tick.");
    Console.WriteLine("Expect NOT: ConsumptionStalled — a deferred head is expected to be unchanging.");
}

// TST-102/103 — an entity created, measured, then deleted mid-run.
async Task EntityLifecycleAsync()
{
    var queue = await EnsureQueueAsync("lifecycle");
    Console.WriteLine($"Created {queue}.");
    Console.WriteLine("Expect: measured within DiscoveryCacheTtlSeconds (default 300s).");
    Console.WriteLine();
    Console.WriteLine("Now delete it while the monitor is running:");
    Console.WriteLine($"  az servicebus queue delete --name {queue} ...");
    Console.WriteLine("Expect: EntityDisappeared (Sev4), then a tombstone after the next discovery");
    Console.WriteLine("        refresh confirms the absence, and emission stops silently.");
}

// TST-104 — a burst sized to induce ServerBusy, exercising the degradation ladder and
// its hysteretic recovery.
async Task BurstAsync()
{
    var queue = await EnsureQueueAsync("burst");
    await using var sender = client.CreateSender(queue);

    var sends = Enumerable.Range(0, 40).Select(async batchIndex =>
    {
        var batch = Enumerable.Range(0, 500)
            .Select(i => new ServiceBusMessage(new byte[8 * 1024]) { MessageId = $"burst-{batchIndex}-{i}" });

        await sender.SendMessagesAsync(batch);
    });

    await Task.WhenAll(sends);

    Console.WriteLine($"Pushed 20,000 messages into {queue}.");
    Console.WriteLine("Expect: MonitorDegraded escalating one level per ThrottleTicksToEscalate ticks,");
    Console.WriteLine("        DegradationLevel on every record, and recovery advancing ONE level per");
    Console.WriteLine("        RecoveryCleanTicks clean ticks — never instantly.");
    Console.WriteLine("Expect: critical-class entities keeping a single peek batch even at L3.");
}

// TST-109 — consumption stopped while messages continue arriving.
async Task ConsumerStoppedAsync()
{
    var queue = await EnsureQueueAsync("stalled");
    await using var sender = client.CreateSender(queue);

    for (var i = 0; i < 10; i++)
    {
        await sender.SendMessageAsync(new ServiceBusMessage($"stall-{i}") { MessageId = $"stall-{i}" });
        await Task.Delay(TimeSpan.FromSeconds(5));
    }

    Console.WriteLine($"Sent 10 messages to {queue} with no consumer attached.");
    Console.WriteLine("Expect: the head SequenceNumber unchanged across ticks, StalledTickCount climbing,");
    Console.WriteLine($"        and ConsumptionStalled once it reaches StalledTicksForAlert (default 5) — ALR-010.");
}

// TST-110 — auto-forwarding whose destination has been removed. Messages land in the
// source's transfer dead-letter queue, which is a distinct entity from the normal DLQ.
async Task ForwardingBrokenAsync()
{
    var destination = $"{ReservedPrefix}forward-destination";
    var source = $"{ReservedPrefix}forward-source";

    if (!await admin.QueueExistsAsync(destination))
    {
        await admin.CreateQueueAsync(destination);
    }

    if (!await admin.QueueExistsAsync(source))
    {
        await admin.CreateQueueAsync(new CreateQueueOptions(source) { ForwardTo = destination });
    }

    await using var sender = client.CreateSender(source);
    await sender.SendMessageAsync(new ServiceBusMessage("forward-probe"));

    Console.WriteLine($"Created {source} forwarding to {destination}.");
    Console.WriteLine("Expect while healthy: EntityRole=Forwarder on the source, and it staying at");
    Console.WriteLine("        age ~0 under the tighter forwarder threshold (default 60s).");
    Console.WriteLine();
    Console.WriteLine($"Now delete {destination} and send again:");
    Console.WriteLine("Expect: TransferDeadLetterMessageCount > 0 and ALR-022 within ~10 minutes.");
}

async Task CleanupAsync()
{
    var removed = 0;

    await foreach (var queue in admin.GetQueuesAsync())
    {
        if (!queue.Name.StartsWith(ReservedPrefix, StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        await admin.DeleteQueueAsync(queue.Name);
        removed++;
        Console.WriteLine($"Deleted {queue.Name}");
    }

    Console.WriteLine($"Removed {removed} synthetic entities.");
}

async Task<string> EnsureQueueAsync(string suffix)
{
    var name = ReservedPrefix + suffix;

    if (!await admin.QueueExistsAsync(name))
    {
        await admin.CreateQueueAsync(new CreateQueueOptions(name)
        {
            // Spec FR-070: partitioning is prohibited, and the generator must not create
            // an entity the monitor would have to flag as BestEffort.
            EnablePartitioning = false,
            MaxDeliveryCount = 10,
            DefaultMessageTimeToLive = TimeSpan.FromDays(1)
        });
    }

    return name;
}

static void PrintUsage()
{
    Console.WriteLine("""
        asbmon-scenario <scenario> <fully-qualified-namespace>

        Staging-only synthetic scenario generator (spec §11.9). Never deploy to production.
        All entities are created under the reserved prefix 'asbmon-synthetic-'.

          aged-message        TST-100  a message left to age past Sev2 and then Sev1
          deferred-head       TST-101  a deferred block deeper than the scan budget
          entity-lifecycle    TST-102/103  entity created, then deleted mid-run
          burst               TST-104  load sized to trigger the degradation ladder
          consumer-stopped    TST-109  messages arriving with nothing consuming
          forwarding-broken   TST-110  auto-forward failure into the transfer DLQ
          cleanup                      delete every synthetic entity

        Scenarios that cannot be driven from outside the monitor, and are operational
        steps in the runbook instead:

          TST-105  clock skew          — requires host clock manipulation
          TST-106  DCR misconfigured   — requires an infrastructure change
          TST-107  region stopped      — stop the active region's function app
          TST-108  forced split-brain  — deny both regions access to the lease store
          TST-050  monitor stopped     — stop the function app; ALR-030 and ALR-032
                                         must both fire within 5 minutes
        """);
}
