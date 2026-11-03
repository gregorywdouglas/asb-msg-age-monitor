using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using EIE.ServiceBus.AgeMonitor.Adapters;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Measurement;
using EIE.ServiceBus.AgeMonitor.Telemetry;
using EIE.ServiceBus.AgeMonitor.Tests.Support;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>
/// Spec §11.10 — the published entity catalog and unknown runtime counts,
/// TST-120..131 (SPEC-DEVIATION-008).
/// </summary>
public sealed class CatalogTests
{
    private readonly MonitorOptions _options = new();
    private readonly TestClock _clock = new(Build.T0);

    private static readonly EntityRuntimeInfo UnknownCounts = EntityRuntimeInfo.Unknown(Build.T0);

    // ---- AppConfigEntityCatalog (TST-120..125) ----------------------------------

    [Fact(DisplayName = "TST-120 the catalog yields queues and subscriptions with their recorded properties")]
    public async Task Tst120_CatalogParsesEntities()
    {
        var catalog = Catalog(new Dictionary<string, string?>
        {
            ["asbmon:catalog:publishedAtUtc"] = "2026-10-08T12:00:00Z",
            ["asbmon:catalog:entities:orders-in:kind"] = "Queue",
            ["asbmon:catalog:entities:orders-in:forwardTo"] = "orders-work",
            ["asbmon:catalog:entities:orders-in:enablePartitioning"] = "false",
            ["asbmon:catalog:entities:orders-in:maxDeliveryCount"] = "5",
            ["asbmon:catalog:entities:orders-in:defaultMessageTimeToLive"] = "14.00:00:00",
            ["asbmon:catalog:entities:events/Subscriptions/billing:kind"] = "Subscription",
            ["asbmon:catalog:entities:events/Subscriptions/billing:enablePartitioning"] = "false",
            ["asbmon:catalog:entities:events/Subscriptions/billing:maxDeliveryCount"] = "10",
            ["asbmon:catalog:entities:events/Subscriptions/billing:defaultMessageTimeToLive"] = "P1D"
        });

        var entities = (await catalog.ListEntitiesAsync(default)).ToDictionary(e => e.EntityPath);

        var queue = entities["orders-in"];
        Assert.Equal(EntityKind.Queue, queue.Kind);
        Assert.Equal(EntityRole.Forwarder, queue.Role);
        Assert.Equal("orders-work", queue.ForwardTarget);
        Assert.False(queue.IsPartitioned);
        Assert.Equal(5, queue.MaxDeliveryCount);
        Assert.Equal(TimeSpan.FromDays(14), queue.DefaultTimeToLive);

        var subscription = entities["events/Subscriptions/billing"];
        Assert.Equal(EntityKind.Subscription, subscription.Kind);
        Assert.Equal(EntityRole.Terminal, subscription.Role);
        Assert.Null(subscription.ForwardTarget);
        Assert.Equal(TimeSpan.FromDays(1), subscription.DefaultTimeToLive);
    }

    [Fact(DisplayName = "TST-121 an empty or unpublished catalog throws so discovery keeps its cache")]
    public async Task Tst121_EmptyCatalogThrows()
    {
        var catalog = Catalog(new Dictionary<string, string?>
        {
            ["asbmon:threshold:default:sev2"] = "300"
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => catalog.ListEntitiesAsync(default));
    }

    [Fact(DisplayName = "TST-122 a malformed partitioning flag is read as partitioned, never as exact")]
    public async Task Tst122_MalformedPartitioningIsConservative()
    {
        var catalog = Catalog(new Dictionary<string, string?>
        {
            ["asbmon:catalog:entities:q-a:kind"] = "Queue",
            ["asbmon:catalog:entities:q-a:enablePartitioning"] = "maybe"
        });

        var entity = Assert.Single(await catalog.ListEntitiesAsync(default));

        Assert.True(entity.IsPartitioned);
        Assert.Equal(AgeAccuracy.BestEffort, entity.Accuracy);
    }

    [Fact(DisplayName = "TST-123 malformed optional fields keep the entity with defaults rather than dropping it")]
    public async Task Tst123_MalformedFieldsKeepEntity()
    {
        var catalog = Catalog(new Dictionary<string, string?>
        {
            ["asbmon:catalog:entities:events/Subscriptions/audit:kind"] = "bogus",
            ["asbmon:catalog:entities:events/Subscriptions/audit:enablePartitioning"] = "false",
            ["asbmon:catalog:entities:events/Subscriptions/audit:maxDeliveryCount"] = "-1",
            ["asbmon:catalog:entities:events/Subscriptions/audit:defaultMessageTimeToLive"] = "forever"
        });

        var entity = Assert.Single(await catalog.ListEntitiesAsync(default));

        Assert.Equal(EntityKind.Subscription, entity.Kind);
        Assert.Equal(10, entity.MaxDeliveryCount);
        Assert.Equal(TimeSpan.Zero, entity.DefaultTimeToLive);
    }

    // ---- Unknown runtime counts (TST-126..131) ----------------------------------

    [Fact(DisplayName = "TST-126 with counts unknown no resume point is trusted")]
    public async Task Tst126_UnknownCountsInvalidateResumePoint()
    {
        var messages = Enumerable.Range(1, 400)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-900), MessageState.Deferred))
            .Append(Build.Message(401, Build.T0.AddSeconds(-500)))
            .ToArray();

        var peeker = new FakePeeker().With("q-test", messages);
        var state = Build.StateStore(_options);
        var resolver = Build.Resolver(peeker, state, Build.Degradation(_options), _options, _clock);

        state.BeginTick();
        await resolver.ResolveAsync(Build.Queue(), UnknownCounts, 250, default);
        Assert.NotNull(state.GetResumePoint("q-test"));

        state.BeginTick();
        peeker.Calls.Clear();
        await resolver.ResolveAsync(Build.Queue(), UnknownCounts, 250, default);

        Assert.Null(peeker.Calls[0].From);
    }

    [Fact(DisplayName = "TST-127 with counts unknown, a zero straight after a Measured tick is Indeterminate")]
    public async Task Tst127_UnknownCountsDropAfterMeasuredIsIndeterminate()
    {
        // ZeroCorroborationTicks = 1 would otherwise promote this zero to Empty at once.
        var options = new MonitorOptions { ZeroCorroborationTicks = 1 };
        var state = Build.StateStore(options);
        var resolver = Build.Resolver(new FakePeeker(), state, Build.Degradation(options), options, _clock);

        state.RecordObservation("q-test", MeasurementStatus.Measured, 42, 120d, null);

        var result = await resolver.ResolveAsync(Build.Queue(), UnknownCounts, 5000, default);

        Assert.Equal(MeasurementStatus.Indeterminate, result.Status);
    }

    [Fact(DisplayName = "TST-128 with counts unknown, sustained clean zeros still promote to Empty")]
    public async Task Tst128_UnknownCountsZeroStreakPromotes()
    {
        var state = Build.StateStore(_options);
        var resolver = Build.Resolver(new FakePeeker(), state, Build.Degradation(_options), _options, _clock);
        var entity = Build.Queue();
        AgeResolution? result = null;

        for (var tick = 0; tick < _options.ZeroCorroborationTicks; tick++)
        {
            result = await resolver.ResolveAsync(entity, UnknownCounts, 5000, default);
            state.RecordObservation(entity.EntityPath, result.Status, null, null, null);
        }

        Assert.Equal(MeasurementStatus.Empty, result!.Status);
    }

    [Fact(DisplayName = "TST-129 with counts unknown, an unchanging measured head is still ConsumptionStalled")]
    public void Tst129_UnknownCountsStillDetectStall()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);
        MeasurementRecord? record = null;

        for (var tick = 0; tick < _options.StalledTicksForAlert; tick++)
        {
            record = factory.Create(new MeasurementInputs(
                Build.Queue(),
                UnknownCounts,
                new AgeResolution(MeasurementStatus.Measured, Build.Message(77, Build.T0.AddSeconds(-120)), 1, 1),
                new ThresholdSet(300, 900, ThresholdSource.AppConfig),
                DegradationLevel.L0,
                "tick",
                Build.T0,
                null));
        }

        Assert.True(record!.ConsumptionStalled);
    }

    [Fact(DisplayName = "TST-130 an unknown count is emitted as null in the row, never as zero")]
    public void Tst130_UnknownCountsAreNullInRow()
    {
        var factory = Build.Factory(Build.StateStore(_options), _options, _clock);

        var record = factory.Create(new MeasurementInputs(
            Build.Queue(),
            UnknownCounts,
            new AgeResolution(MeasurementStatus.Indeterminate, null, 1, 0),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null));

        var row = LogsIngestionSink.MeasurementRow.From(record);

        Assert.Null(row.ActiveMessageCount);
        Assert.Null(row.DeadLetterMessageCount);
        Assert.Null(row.ScheduledMessageCount);
        Assert.Null(row.TransferDeadLetterMessageCount);
    }

    [Fact(DisplayName = "TST-131 an unknown count emits no metric point rather than a zero")]
    public async Task Tst131_UnknownCountsEmitNoMetric()
    {
        var channel = new CapturingChannel();
        using var configuration = new TelemetryConfiguration
        {
            TelemetryChannel = channel,
            ConnectionString = "InstrumentationKey=00000000-0000-0000-0000-000000000000"
        };

        var sink = new ApplicationInsightsMetricSink(new TelemetryClient(configuration), Build.Settings());
        var factory = Build.Factory(Build.StateStore(_options), _options, _clock);

        var record = factory.Create(new MeasurementInputs(
            Build.Queue(),
            UnknownCounts,
            new AgeResolution(MeasurementStatus.Measured, Build.Message(1, Build.T0.AddSeconds(-60)), 1, 1),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null));

        await sink.EmitAsync([record], Heartbeat(), default);

        var metrics = channel.Sent.OfType<MetricTelemetry>().ToList();

        Assert.Contains(metrics, m => m.Name == "OldestMessageAgeSeconds");
        Assert.DoesNotContain(metrics, m => m.Name.EndsWith("MessageCount", StringComparison.Ordinal));
    }

    private static AppConfigEntityCatalog Catalog(Dictionary<string, string?> values) =>
        new(new ConfigurationBuilder().AddInMemoryCollection(values).Build(), Build.Log<AppConfigEntityCatalog>());

    private static MonitorHeartbeat Heartbeat() =>
        new("tick", Build.T0, "eastus2", "test", true, 1, 1, 0, 0, 0, 12,
            DegradationLevel.L0, ThresholdSource.AppConfig, true, true);

    private sealed class CapturingChannel : ITelemetryChannel
    {
        public List<ITelemetry> Sent { get; } = [];

        public bool? DeveloperMode { get; set; }

        public string? EndpointAddress { get; set; }

        public void Send(ITelemetry item) => Sent.Add(item);

        public void Flush()
        {
        }

        public void Dispose()
        {
        }
    }
}
