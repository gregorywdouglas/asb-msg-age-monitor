using Microsoft.Extensions.Logging.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Measurement;
using EIE.ServiceBus.AgeMonitor.State;
using EIE.ServiceBus.AgeMonitor.Tests.Support;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>Spec §11.2 — state machines, TST-020..039.</summary>
public sealed class StateMachineTests
{
    private readonly MonitorOptions _options = new();
    private readonly TestClock _clock = new(Build.T0);

    private static EntityRuntimeInfo Runtime(long active) => new(active, 0, 0, 0, Build.T0);

    // ---- Resume points (TST-020..024) --------------------------------------

    [Fact(DisplayName = "TST-020 a resume point makes the next tick cheaper than the first")]
    public async Task Tst020_ResumePointReducesBatches()
    {
        var deferred = Enumerable.Range(1, 1000)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-900), MessageState.Deferred))
            .Append(Build.Message(1001, Build.T0.AddSeconds(-400)))
            .ToArray();

        var peeker = new FakePeeker().With("q-test", deferred);
        var state = Build.StateStore(_options);
        var resolver = Build.Resolver(peeker, state, Build.Degradation(_options), _options, _clock);
        var entity = Build.Queue();

        state.BeginTick();
        var first = await resolver.ResolveAsync(entity, Runtime(1001), 750, default);
        Assert.Equal(MeasurementStatus.HeadBlockedByDeferred, first.Status);

        var batchesAfterBlock = peeker.PeekCallCount;
        state.RecordObservation(entity.EntityPath, first.Status, null, null, 1001);

        state.BeginTick();
        var second = await resolver.ResolveAsync(entity, Runtime(1001), 750, default);

        Assert.Equal(MeasurementStatus.Measured, second.Status);
        Assert.Equal(1001, second.OldestActive!.SequenceNumber);
        Assert.True(
            second.BatchesUsed < batchesAfterBlock,
            $"resumed scan used {second.BatchesUsed} batches, first scan used {batchesAfterBlock}");
    }

    [Fact(DisplayName = "TST-021 a decrease in ActiveMessageCount invalidates the resume point")]
    public async Task Tst021_DepthDecreaseInvalidates()
    {
        var messages = Enumerable.Range(1, 400)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-900), MessageState.Deferred))
            .Append(Build.Message(401, Build.T0.AddSeconds(-500)))
            .ToArray();

        var peeker = new FakePeeker().With("q-test", messages);
        var state = Build.StateStore(_options);
        var resolver = Build.Resolver(peeker, state, Build.Degradation(_options), _options, _clock);
        var entity = Build.Queue();

        state.BeginTick();
        await resolver.ResolveAsync(entity, Runtime(401), 250, default);
        state.RecordObservation(entity.EntityPath, MeasurementStatus.HeadBlockedByDeferred, null, null, 401);
        Assert.NotNull(state.GetResumePoint(entity.EntityPath));

        state.BeginTick();
        await resolver.ResolveAsync(entity, Runtime(100), 250, default);

        Assert.Equal(1L, peeker.Calls[^1].From ?? 1L);
    }

    [Fact(DisplayName = "TST-022 observing the entity empty invalidates the resume point")]
    public void Tst022_EmptyInvalidates()
    {
        var state = Build.StateStore(_options);
        state.BeginTick();
        state.SetResumePoint("q-test", 500);

        state.Invalidate("q-test", "entity observed empty");

        Assert.Null(state.GetResumePoint("q-test"));
    }

    [Fact(DisplayName = "TST-023 the resume point expires after ResumePointMaxTicks regardless of other conditions")]
    public void Tst023_ResumePointHardTtl()
    {
        var state = Build.StateStore(_options);
        state.BeginTick();
        state.SetResumePoint("q-test", 900);

        for (var i = 0; i < _options.ResumePointMaxTicks - 1; i++)
        {
            state.BeginTick();
            Assert.Equal(900L, state.GetResumePoint("q-test"));
        }

        state.BeginTick();
        Assert.Null(state.GetResumePoint("q-test"));
    }

    [Fact(DisplayName = "TST-024 a restart loses every resume point and costs one expensive tick")]
    public async Task Tst024_RestartLosesResumePoints()
    {
        var messages = Enumerable.Range(1, 600)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-900), MessageState.Deferred))
            .Append(Build.Message(601, Build.T0.AddSeconds(-450)))
            .ToArray();

        var peeker = new FakePeeker().With("q-test", messages);
        var warm = Build.StateStore(_options);
        var resolver = Build.Resolver(peeker, warm, Build.Degradation(_options), _options, _clock);
        var entity = Build.Queue();

        warm.BeginTick();
        await resolver.ResolveAsync(entity, Runtime(601), 5000, default);
        var warmBatches = peeker.PeekCallCount;

        // A new store models process restart: nothing durable survives (NFR-020).
        var cold = Build.StateStore(_options);
        var coldPeeker = new FakePeeker().With("q-test", messages);
        var coldResolver = Build.Resolver(coldPeeker, cold, Build.Degradation(_options), _options, _clock);

        cold.BeginTick();
        Assert.Null(cold.GetResumePoint(entity.EntityPath));

        var result = await coldResolver.ResolveAsync(entity, Runtime(601), 5000, default);

        // The rebuilt scan starts from the head and costs exactly what the first warm
        // scan cost — one expensive tick, which is the whole price of losing the cache.
        Assert.Equal(MeasurementStatus.Measured, result.Status);
        Assert.Equal(warmBatches, coldPeeker.PeekCallCount);
        Assert.Null(coldPeeker.Calls[0].From);
    }

    // ---- Stall detection (TST-025..028) -------------------------------------

    [Fact(DisplayName = "TST-025 an unchanging head over five measured ticks with depth is ConsumptionStalled")]
    public void Tst025_StallDetected()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);
        MeasurementRecord? record = null;

        for (var tick = 0; tick < _options.StalledTicksForAlert; tick++)
        {
            record = factory.Create(Inputs(MeasurementStatus.Measured, head: 77, active: 12));
        }

        Assert.NotNull(record);
        Assert.True(record!.ConsumptionStalled);
        Assert.Equal(_options.StalledTicksForAlert, record.StalledTickCount);
    }

    [Fact(DisplayName = "TST-026 an unchanging head on an empty queue is not a stall")]
    public void Tst026_EmptyQueueIsNotStalled()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);
        MeasurementRecord? record = null;

        for (var tick = 0; tick < 8; tick++)
        {
            record = factory.Create(Inputs(MeasurementStatus.Empty, head: null, active: 0));
        }

        Assert.False(record!.ConsumptionStalled);
        Assert.Equal(0, record.StalledTickCount);
    }

    [Fact(DisplayName = "TST-027 a head blocked by deferred messages is not a stall")]
    public void Tst027_BlockedHeadIsNotStalled()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);
        MeasurementRecord? record = null;

        for (var tick = 0; tick < 8; tick++)
        {
            record = factory.Create(Inputs(MeasurementStatus.HeadBlockedByDeferred, head: null, active: 4000));
        }

        Assert.False(record!.ConsumptionStalled);
    }

    [Fact(DisplayName = "TST-028 an Indeterminate tick inside the window resets the stall counter")]
    public void Tst028_IndeterminateResetsStall()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);

        factory.Create(Inputs(MeasurementStatus.Measured, head: 77, active: 12));
        factory.Create(Inputs(MeasurementStatus.Measured, head: 77, active: 12));
        factory.Create(Inputs(MeasurementStatus.Indeterminate, head: null, active: 12));

        var record = factory.Create(Inputs(MeasurementStatus.Measured, head: 77, active: 12));

        Assert.False(record.ConsumptionStalled);
        Assert.Equal(1, record.StalledTickCount);
    }

    // ---- Degradation (TST-029..032) -----------------------------------------

    [Fact(DisplayName = "TST-029 sustained throttling escalates the degradation level")]
    public void Tst029_ThrottleEscalates()
    {
        var degradation = Build.Degradation(_options);

        for (var tick = 0; tick < _options.ThrottleTicksToEscalate; tick++)
        {
            degradation.BeginTick();
            degradation.RecordThrottle("q-test");
            degradation.RecordCleanTick();
        }

        Assert.Equal(DegradationLevel.L1, degradation.CurrentLevel);
    }

    [Fact(DisplayName = "TST-030 recovery advances one level per N clean ticks, never instantly")]
    public void Tst030_RecoveryIsHysteretic()
    {
        var degradation = Build.Degradation(_options);

        for (var tick = 0; tick < _options.ThrottleTicksToEscalate * 2; tick++)
        {
            degradation.BeginTick();
            degradation.RecordThrottle("q-test");
            degradation.RecordCleanTick();
        }

        Assert.Equal(DegradationLevel.L2, degradation.CurrentLevel);

        // A single clean tick must not restore full budget: instant recovery oscillates.
        degradation.BeginTick();
        degradation.RecordCleanTick();
        Assert.Equal(DegradationLevel.L2, degradation.CurrentLevel);

        for (var tick = 1; tick < _options.RecoveryCleanTicks; tick++)
        {
            degradation.BeginTick();
            degradation.RecordCleanTick();
        }

        Assert.Equal(DegradationLevel.L1, degradation.CurrentLevel);
    }

    [Fact(DisplayName = "TST-031 a critical-class entity retains a peek batch at the deepest degradation level")]
    public void Tst031_CriticalKeepsPeekAtL3()
    {
        var degradation = Build.Degradation(_options);
        DriveToLevel(degradation, DegradationLevel.L3);

        Assert.Equal(DegradationLevel.L3, degradation.CurrentLevel);
        Assert.True(degradation.EffectiveScanDepth(CriticalityClass.Critical) > 0);
        Assert.Equal(_options.PeekBatchSize, degradation.EffectiveScanDepth(CriticalityClass.Critical));
    }

    [Fact(DisplayName = "TST-032 a bulk-class entity drops to depth-only at the deepest degradation level")]
    public void Tst032_BulkGoesDepthOnlyAtL3()
    {
        var degradation = Build.Degradation(_options);
        DriveToLevel(degradation, DegradationLevel.L3);

        Assert.Equal(0, degradation.EffectiveScanDepth(CriticalityClass.Bulk));
        Assert.True(degradation.EffectiveScanDepth(CriticalityClass.Standard) > 0);
    }

    // ---- Thresholds (TST-033..036) ------------------------------------------

    [Fact(DisplayName = "TST-033 an out-of-clamp threshold is rejected and last-known-good is retained")]
    public void Tst033_InvalidThresholdRejected()
    {
        var options = new MonitorOptions();
        options.EntityThresholds["q-outage"] = new ThresholdPair(300_000, 400_000);

        var provider = new ThresholdProvider(
            new StaticOptionsMonitor<MonitorOptions>(options),
            new FakeConfigurationHealth(),
            NullLogger<ThresholdProvider>.Instance);

        var resolved = provider.Resolve(Build.Queue("q-outage"));

        Assert.Equal(300, resolved.Sev2Seconds);
        Assert.Equal(900, resolved.Sev1Seconds);
        Assert.Contains(provider.DrainRejections(), r => r.Contains("q-outage", StringComparison.Ordinal));
    }

    [Fact(DisplayName = "TST-033b a sev2 at or above sev1 is rejected")]
    public void Tst033b_InvertedThresholdsRejected()
    {
        Assert.False(ConfigurationClamps.IsValidThresholdPair(900, 900, out _));
        Assert.False(ConfigurationClamps.IsValidThresholdPair(1200, 900, out _));
        Assert.False(ConfigurationClamps.IsValidThresholdPair(10, 900, out _));
        Assert.True(ConfigurationClamps.IsValidThresholdPair(300, 900, out _));
    }

    [Fact(DisplayName = "TST-034 an unreachable App Configuration falls back to compiled defaults and still starts")]
    public void Tst034_CompiledDefaultsWhenConfigNeverLoaded()
    {
        var health = new FakeConfigurationHealth { IsAvailable = false, HasEverLoaded = false };

        var provider = new ThresholdProvider(
            new StaticOptionsMonitor<MonitorOptions>(new MonitorOptions()),
            health,
            NullLogger<ThresholdProvider>.Instance);

        var resolved = provider.Resolve(Build.Queue());

        Assert.Equal(ThresholdSource.CompiledDefault, provider.CurrentSource);
        Assert.Equal(MonitorOptions.CompiledDefaultSev2Seconds, resolved.Sev2Seconds);
        Assert.Equal(MonitorOptions.CompiledDefaultSev1Seconds, resolved.Sev1Seconds);
    }

    [Fact(DisplayName = "TST-034b a config outage after a successful load reports LastKnownGood")]
    public void Tst034b_LastKnownGoodAfterOutage()
    {
        var health = new FakeConfigurationHealth { IsAvailable = false, HasEverLoaded = true };

        var provider = new ThresholdProvider(
            new StaticOptionsMonitor<MonitorOptions>(new MonitorOptions()),
            health,
            NullLogger<ThresholdProvider>.Instance);

        Assert.Equal(ThresholdSource.LastKnownGood, provider.CurrentSource);
    }

    [Fact(DisplayName = "TST-035 an entity override wins over the class default")]
    public void Tst035_EntityOverrideWins()
    {
        var options = new MonitorOptions();
        options.EntityThresholds["q-metering-in"] = new ThresholdPair(1800, 3600);

        var provider = new ThresholdProvider(
            new StaticOptionsMonitor<MonitorOptions>(options),
            new FakeConfigurationHealth(),
            NullLogger<ThresholdProvider>.Instance);

        var overridden = provider.Resolve(Build.Queue("q-metering-in", CriticalityClass.Critical));
        var classDefault = provider.Resolve(Build.Queue("q-other", CriticalityClass.Critical));

        Assert.Equal(1800, overridden.Sev2Seconds);
        Assert.Equal(3600, overridden.Sev1Seconds);
        Assert.Equal(60, classDefault.Sev2Seconds);
    }

    [Fact(DisplayName = "TST-036 a forwarder with no explicit override uses the tighter forwarder threshold")]
    public void Tst036_ForwarderThreshold()
    {
        var provider = new ThresholdProvider(
            new StaticOptionsMonitor<MonitorOptions>(new MonitorOptions()),
            new FakeConfigurationHealth(),
            NullLogger<ThresholdProvider>.Instance);

        var forwarder = provider.Resolve(Build.Queue("t-events/Subscriptions/forward", role: EntityRole.Forwarder));
        var terminal = provider.Resolve(Build.Queue("q-terminal"));

        Assert.Equal(60, forwarder.Sev2Seconds);
        Assert.Equal(180, forwarder.Sev1Seconds);
        Assert.Equal(300, terminal.Sev2Seconds);
    }

    [Fact(DisplayName = "TST-036b an explicit entity override still beats the forwarder default")]
    public void Tst036b_EntityOverrideBeatsForwarderDefault()
    {
        var options = new MonitorOptions();
        options.EntityThresholds["t-events/Subscriptions/forward"] = new ThresholdPair(600, 1200);

        var provider = new ThresholdProvider(
            new StaticOptionsMonitor<MonitorOptions>(options),
            new FakeConfigurationHealth(),
            NullLogger<ThresholdProvider>.Instance);

        var resolved = provider.Resolve(Build.Queue("t-events/Subscriptions/forward", role: EntityRole.Forwarder));

        Assert.Equal(600, resolved.Sev2Seconds);
    }

    // ---- Discovery lifecycle (TST-037..039) ---------------------------------

    [Fact(DisplayName = "TST-037 an entity present at the next refresh is treated as a transient not-found")]
    public async Task Tst037_TransientNotFoundRecovers()
    {
        var (discovery, catalog) = Discovery(Build.Queue("q-a"), Build.Queue("q-b"));

        await discovery.GetEntitiesAsync(default);
        discovery.EvictOnNotFound("q-b");

        _clock.Advance(TimeSpan.FromSeconds(_options.DiscoveryCacheTtlSeconds * 2));
        var entities = await discovery.GetEntitiesAsync(default);

        Assert.Contains(entities, e => e.EntityPath == "q-b");
        Assert.False(discovery.IsTombstoned("q-b"));
        Assert.Contains(discovery.DrainEvents(), e => e.EventType == MonitorEventType.EntityRecovered);
        Assert.True(catalog.ListCallCount >= 2);
    }

    [Fact(DisplayName = "TST-038 an entity absent at the next refresh is tombstoned and stops emitting")]
    public async Task Tst038_ConfirmedDeletionTombstones()
    {
        var (discovery, catalog) = Discovery(Build.Queue("q-a"), Build.Queue("q-gone"));

        await discovery.GetEntitiesAsync(default);
        discovery.EvictOnNotFound("q-gone");

        catalog.Entities = [Build.Queue("q-a")];
        _clock.Advance(TimeSpan.FromSeconds(_options.DiscoveryCacheTtlSeconds * 2));
        var entities = await discovery.GetEntitiesAsync(default);

        Assert.DoesNotContain(entities, e => e.EntityPath == "q-gone");
        Assert.True(discovery.IsTombstoned("q-gone"));
    }

    [Fact(DisplayName = "TST-039 a tombstone older than its TTL allows a recreated entity to be monitored again")]
    public async Task Tst039_TombstoneExpires()
    {
        var (discovery, catalog) = Discovery(Build.Queue("q-a"), Build.Queue("q-recreated"));

        await discovery.GetEntitiesAsync(default);
        discovery.EvictOnNotFound("q-recreated");

        catalog.Entities = [Build.Queue("q-a")];
        _clock.Advance(TimeSpan.FromSeconds(_options.DiscoveryCacheTtlSeconds * 2));
        await discovery.GetEntitiesAsync(default);
        Assert.True(discovery.IsTombstoned("q-recreated"));

        catalog.Entities = [Build.Queue("q-a"), Build.Queue("q-recreated")];
        _clock.Advance(TimeSpan.FromHours(_options.TombstoneTtlHours + 1));
        var entities = await discovery.GetEntitiesAsync(default);

        Assert.False(discovery.IsTombstoned("q-recreated"));
        Assert.Contains(entities, e => e.EntityPath == "q-recreated");
    }

    [Fact(DisplayName = "TST-039b a discovery outage retains the previous cache rather than losing coverage")]
    public async Task Tst039b_DiscoveryFailureRetainsCache()
    {
        var (discovery, catalog) = Discovery(Build.Queue("q-a"), Build.Queue("q-b"));
        await discovery.GetEntitiesAsync(default);

        catalog.ThrowOnList = new InvalidOperationException("ARM unavailable");
        _clock.Advance(TimeSpan.FromSeconds(_options.DiscoveryCacheTtlSeconds * 2));

        var entities = await discovery.GetEntitiesAsync(default);

        Assert.Equal(2, entities.Count);
    }

    [Fact(DisplayName = "TST-039c a partitioned entity raises UnsupportedEntityConfiguration and is flagged BestEffort")]
    public async Task Tst039c_PartitionedEntityFlagged()
    {
        var (discovery, _) = Discovery(Build.Queue("q-partitioned", partitioned: true));

        var entities = await discovery.GetEntitiesAsync(default);

        Assert.Equal(AgeAccuracy.BestEffort, entities[0].Accuracy);
        Assert.Contains(
            discovery.DrainEvents(),
            e => e.EventType == MonitorEventType.UnsupportedEntityConfiguration);
    }

    [Fact(DisplayName = "TST-039d a forwarding cycle is detected and reported")]
    public async Task Tst039d_ForwardingCycleDetected()
    {
        var (discovery, _) = Discovery(
            Build.Queue("q-a", role: EntityRole.Forwarder, forwardTarget: "q-b"),
            Build.Queue("q-b", role: EntityRole.Forwarder, forwardTarget: "q-a"));

        await discovery.GetEntitiesAsync(default);

        Assert.Contains(discovery.DrainEvents(), e => e.EventType == MonitorEventType.ForwardingCycleDetected);
    }

    [Fact(DisplayName = "TST-039e an unclassified entity defaults to standard and raises a signal")]
    public async Task Tst039e_UnclassifiedEntity()
    {
        var catalog = new FakeCatalog
        {
            Entities = [new EntityDescriptor("q-new", EntityKind.Queue)]
        };

        var discovery = new EntityDiscoveryService(
            catalog,
            new StaticOptionsMonitor<MonitorOptions>(_options),
            Build.StateStore(_options),
            _clock,
            NullLogger<EntityDiscoveryService>.Instance,
            new Random(1));

        var entities = await discovery.GetEntitiesAsync(default);

        Assert.Equal(CriticalityClass.Standard, entities[0].Class);
        Assert.False(entities[0].IsClassExplicit);
        Assert.Contains(discovery.DrainEvents(), e => e.EventType == MonitorEventType.EntityUnclassified);
    }

    // ---- helpers ------------------------------------------------------------

    private (EntityDiscoveryService Discovery, FakeCatalog Catalog) Discovery(params EntityDescriptor[] entities)
    {
        var catalog = new FakeCatalog { Entities = entities.ToList() };

        foreach (var entity in entities)
        {
            _options.EntityClasses[entity.EntityPath] = entity.Class;
        }

        var discovery = new EntityDiscoveryService(
            catalog,
            new StaticOptionsMonitor<MonitorOptions>(_options),
            Build.StateStore(_options),
            _clock,
            NullLogger<EntityDiscoveryService>.Instance,
            new Random(1));

        return (discovery, catalog);
    }

    private void DriveToLevel(DegradationController degradation, DegradationLevel target)
    {
        while (degradation.CurrentLevel < target)
        {
            degradation.BeginTick();
            degradation.RecordThrottle("q-test");
            degradation.RecordCleanTick();
        }
    }

    private MeasurementInputs Inputs(MeasurementStatus status, long? head, long active) =>
        new(
            Build.Queue(),
            Runtime(active),
            new AgeResolution(
                status,
                head is { } sequence ? Build.Message(sequence, Build.T0.AddSeconds(-120)) : null,
                1,
                1),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null);
}
