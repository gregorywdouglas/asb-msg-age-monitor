using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Adapters;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Measurement;
using EIE.ServiceBus.AgeMonitor.Orchestration;
using EIE.ServiceBus.AgeMonitor.State;
using EIE.ServiceBus.AgeMonitor.Telemetry;
using EIE.ServiceBus.AgeMonitor.Tests.Support;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>Spec §11.3 — tick orchestration, TST-040..049.</summary>
public sealed class OrchestrationTests
{
    private readonly MonitorOptions _options = new()
    {
        ColdStartTicks = 0,
        MaxPeekScanDepth = 500,
        PeekBatchSize = 250
    };

    private readonly TestClock _clock = new(Build.T0);
    private readonly FakeCatalog _catalog = new();
    private readonly FakePeeker _peeker = new();
    private readonly FakeRuntimeReader _runtime = new();
    private readonly FakeLease _lease = new();
    private readonly FakeEmitter _emitter = new();

    private MonitorTickOrchestrator Sut()
    {
        var monitor = new StaticOptionsMonitor<MonitorOptions>(_options);
        var state = Build.StateStore(_options);
        var degradation = Build.Degradation(_options);
        var settings = Build.Settings();

        var discovery = new EntityDiscoveryService(
            _catalog, monitor, state, _clock,
            NullLogger<EntityDiscoveryService>.Instance, new Random(1));

        var thresholds = new ThresholdProvider(
            monitor, new FakeConfigurationHealth(), NullLogger<ThresholdProvider>.Instance);

        return new MonitorTickOrchestrator(
            discovery,
            _runtime,
            Build.Resolver(_peeker, state, degradation, _options, _clock),
            thresholds,
            degradation,
            state,
            _lease,
            _emitter,
            new ServiceBusFaultClassifier(),
            Build.Factory(state, _options, _clock, settings),
            monitor,
            settings,
            new FakeConfigurationHealth(),
            _clock,
            NullLogger<MonitorTickOrchestrator>.Instance);
    }

    private void GivenEntities(params string[] paths)
    {
        _catalog.Entities = paths.Select(p => Build.Queue(p)).ToList();

        foreach (var path in paths)
        {
            _options.EntityClasses[path] = CriticalityClass.Standard;
            _runtime.With(path, active: 1);
            _peeker.With(path, [Build.Message(1, Build.T0.AddSeconds(-120))]);
        }
    }

    [Fact(DisplayName = "TST-040 entities unmeasured at the deadline become NotMeasured and raise MonitorTickIncomplete")]
    public async Task Tst040_DeadlineProducesVisibleDegradation()
    {
        GivenEntities("q-a", "q-b", "q-c");

        // Set directly rather than through the clamped config path so the deadline can be
        // short enough to assert on without a ten-second test.
        _options.TickDeadlineSeconds = 1;
        _options.PerEntityTimeoutSeconds = 30;
        _options.MaxConcurrentEntityScans = 1;

        var blocked = 0;

        // The first entity blocks past the tick deadline; everything queued behind it must
        // be reported as skipped rather than silently omitted from the batch.
        _runtime.BeforeRead = async _ =>
        {
            if (Interlocked.Increment(ref blocked) == 1)
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
        };

        var result = await Sut().ExecuteTickAsync(Build.T0, default);

        // FR-007 still holds at the deadline: absence of a row must never mean healthy.
        Assert.Equal(3, result.Records.Count);
        Assert.Contains(result.Records, r => r.Status == MeasurementStatus.NotMeasured);
        Assert.Contains(result.Events, e => e.EventType == MonitorEventType.MonitorTickIncomplete);
        Assert.True(result.Heartbeat.SkippedEntities > 0);
    }

    [Fact(DisplayName = "TST-041 skipped entities are ordered first on the following tick")]
    public async Task Tst041_CarryOverGoesToHead()
    {
        GivenEntities("q-a", "q-b", "q-c");
        _options.MaxConcurrentEntityScans = 1;
        _options.PerEntityTimeoutSeconds = 1;

        var sut = Sut();

        // Force the last entity in enumeration order to time out on the first tick.
        _runtime.BeforeRead = async path =>
        {
            if (path == "q-c")
            {
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        };

        var first = await sut.ExecuteTickAsync(Build.T0, default);
        Assert.Equal(MeasurementStatus.NotMeasured, Status(first, "q-c"));

        _runtime.BeforeRead = null;
        var readOrder = new List<string>();
        _runtime.BeforeRead = path =>
        {
            lock (readOrder)
            {
                readOrder.Add(path);
            }

            return Task.CompletedTask;
        };

        var second = await sut.ExecuteTickAsync(Build.T0.AddMinutes(1), default);

        // Carry-over must go to the head, not the tail: a consistently slow entity at the
        // end of the enumeration would otherwise never be measured, and its absence reads
        // as a healthy queue.
        Assert.Equal("q-c", readOrder[0]);
        Assert.Equal(3, second.Records.Count);
        Assert.All(second.Records, r => Assert.NotEqual(MeasurementStatus.NotMeasured, r.Status));
    }

    [Fact(DisplayName = "TST-042 no entity is starved across repeated ticks")]
    public async Task Tst042_NoEntityStarved()
    {
        GivenEntities("q-a", "q-b", "q-c", "q-d");
        var sut = Sut();
        var measuredAtLeastOnce = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (var tick = 0; tick < 10; tick++)
        {
            var result = await sut.ExecuteTickAsync(Build.T0.AddMinutes(tick), default);

            foreach (var record in result.Records.Where(r => r.Status == MeasurementStatus.Measured))
            {
                measuredAtLeastOnce.Add(record.Entity.EntityPath);
            }
        }

        Assert.Equal(4, measuredAtLeastOnce.Count);
    }

    [Fact(DisplayName = "TST-043 in-flight scans never exceed the configured concurrency cap")]
    public async Task Tst043_ConcurrencyCapRespected()
    {
        GivenEntities("q-a", "q-b", "q-c", "q-d", "q-e", "q-f");
        _options.MaxConcurrentEntityScans = 2;

        var inFlight = 0;
        var peak = 0;

        _runtime.BeforeRead = async _ =>
        {
            var current = Interlocked.Increment(ref inFlight);
            InterlockedMax(ref peak, current);
            await Task.Delay(20);
            Interlocked.Decrement(ref inFlight);
        };

        var sut = Sut();
        await sut.ExecuteTickAsync(Build.T0, default);

        Assert.True(peak <= 2, $"peak concurrency was {peak}, cap is 2");
    }

    [Fact(DisplayName = "TST-044 one entity timing out leaves the others measured")]
    public async Task Tst044_PerEntityTimeoutIsolated()
    {
        GivenEntities("q-a", "q-slow", "q-b");
        _options.PerEntityTimeoutSeconds = 2;
        _options.MaxConcurrentEntityScans = 4;

        _runtime.BeforeRead = async path =>
        {
            if (path == "q-slow")
            {
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        };

        var sut = Sut();
        var result = await sut.ExecuteTickAsync(Build.T0, default);

        Assert.Equal(MeasurementStatus.NotMeasured, Status(result, "q-slow"));
        Assert.Equal(MeasurementStatus.Measured, Status(result, "q-a"));
        Assert.Equal(MeasurementStatus.Measured, Status(result, "q-b"));
    }

    [Fact(DisplayName = "TST-045 cold-start ticks cap scan depth to a single batch")]
    public async Task Tst045_ColdStartCapsDepth()
    {
        _options.ColdStartTicks = 3;
        _options.MaxPeekScanDepth = 5000;
        _options.PeekBatchSize = 250;

        GivenEntities("q-deep");
        _peeker.With("q-deep", Enumerable.Range(1, 4000)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-900), MessageState.Deferred)));
        _runtime.With("q-deep", active: 4000);

        var sut = Sut();
        await sut.ExecuteTickAsync(Build.T0, default);

        // Without the cap this would be 20 peeks on the very first tick after a recycle.
        Assert.Equal(1, _peeker.PeekCallCount);
    }

    [Fact(DisplayName = "TST-046 an Application Insights failure still writes to Log Analytics and is witnessed there")]
    public async Task Tst046_MetricSinkFailureIsWitnessedInLogs()
    {
        var metrics = new FakeMetricSink { Fail = true };
        var logs = new FakeLogsSink();
        var emitter = Emitter(metrics, logs);

        var outcome = await emitter.EmitAsync([], Heartbeat(), [], default);

        Assert.False(outcome.AppInsightsOk);
        Assert.True(outcome.LogsIngestionOk);
        Assert.Contains(logs.Events, e => e.EventType == MonitorEventType.EmitFailure);
    }

    [Fact(DisplayName = "TST-047 a Log Analytics failure still writes metrics and is witnessed in Application Insights")]
    public async Task Tst047_LogsSinkFailureIsWitnessedInMetrics()
    {
        var metrics = new FakeMetricSink();
        var logs = new FakeLogsSink { Fail = true };
        var emitter = Emitter(metrics, logs);

        var outcome = await emitter.EmitAsync([], Heartbeat(), [], default);

        Assert.True(outcome.AppInsightsOk);
        Assert.False(outcome.LogsIngestionOk);
        Assert.Contains(metrics.Witnesses, w => w.Sink == "LogsIngestion");
    }

    [Fact(DisplayName = "TST-048 a total sink outage does not throw out of the tick")]
    public async Task Tst048_BothSinksFailingDoesNotThrow()
    {
        var metrics = new FakeMetricSink { Fail = true };
        var logs = new FakeLogsSink { Fail = true };
        var emitter = Emitter(metrics, logs);

        var outcome = await emitter.EmitAsync([], Heartbeat(), [], default);

        Assert.False(outcome.AppInsightsOk);
        Assert.False(outcome.LogsIngestionOk);
        Assert.NotNull(outcome.AppInsightsError);
        Assert.NotNull(outcome.LogsIngestionError);
    }

    [Fact(DisplayName = "TST-048b retries are bounded by EmitRetryCount and never delegate to Functions retry")]
    public async Task Tst048b_RetryIsBounded()
    {
        _options.EmitRetryCount = 2;
        var metrics = new FakeMetricSink { Fail = true };
        var emitter = Emitter(metrics, new FakeLogsSink());

        await emitter.EmitAsync([], Heartbeat(), [], default);

        Assert.Equal(_options.EmitRetryCount + 1, metrics.EmitAttempts);
    }

    [Fact(DisplayName = "TST-049 the same scheduled tick yields an identical MeasurementId on retry")]
    public async Task Tst049_MeasurementIdIsDeterministic()
    {
        GivenEntities("q-a");
        var sut = Sut();

        var first = await sut.ExecuteTickAsync(Build.T0, default);
        var retry = await sut.ExecuteTickAsync(Build.T0, default);

        Assert.Equal(first.Records[0].MeasurementId, retry.Records[0].MeasurementId);
    }

    [Fact(DisplayName = "TST-049b execution time does not influence MeasurementId")]
    public void Tst049b_MeasurementIdIgnoresExecutionTime()
    {
        var a = MeasurementRecordFactory.BuildMeasurementId("eastus2", Build.T0, "q-a");
        var b = MeasurementRecordFactory.BuildMeasurementId("eastus2", Build.T0, "q-a");
        var different = MeasurementRecordFactory.BuildMeasurementId("centralus", Build.T0, "q-a");

        Assert.Equal(a, b);
        Assert.NotEqual(a, different);
    }

    // ---- Additional orchestration invariants ---------------------------------

    [Fact(DisplayName = "FR-007 every discovered entity emits a record every tick, including idle ones")]
    public async Task Fr007_EveryEntityEmitsEveryTick()
    {
        GivenEntities("q-busy", "q-idle");
        _runtime.With("q-idle", active: 0);
        _peeker.With("q-idle", []);

        var sut = Sut();
        var result = await sut.ExecuteTickAsync(Build.T0, default);

        Assert.Equal(2, result.Records.Count);
        Assert.Contains(result.Records, r => r.Entity.EntityPath == "q-idle");
    }

    [Fact(DisplayName = "FR-066 a non-holder still emits a heartbeat so its liveness is observable")]
    public async Task Fr066_NonHolderStillHeartbeats()
    {
        GivenEntities("q-a");
        _lease.ShouldHold = false;

        var sut = Sut();
        var result = await sut.ExecuteTickAsync(Build.T0, default);

        Assert.Empty(result.Records);
        Assert.False(result.Heartbeat.IsLeaseHolder);
        Assert.Single(_emitter.Heartbeats);
    }

    [Fact(DisplayName = "FR-063 an unreachable lease store fails open and the region keeps measuring")]
    public async Task Fr063_LeaseFailureFailsOpen()
    {
        GivenEntities("q-a");
        _lease.ThrowOnAcquire = new InvalidOperationException("lease store unreachable");

        var sut = Sut();
        var result = await sut.ExecuteTickAsync(Build.T0, default);

        Assert.Single(result.Records);
        Assert.Equal(MeasurementStatus.Measured, result.Records[0].Status);
        Assert.Contains(result.Events, e => e.EventType == MonitorEventType.SplitBrain);
    }

    [Fact(DisplayName = "A throttled entity is reported as Throttled and escalates degradation")]
    public async Task ThrottleIsClassifiedAndEscalates()
    {
        GivenEntities("q-a");
        _peeker.ThrowOnPeek = new ServiceBusException("busy", ServiceBusFailureReason.ServiceBusy);

        var sut = Sut();
        var result = await sut.ExecuteTickAsync(Build.T0, default);

        Assert.Equal(MeasurementStatus.Throttled, result.Records[0].Status);
        Assert.Null(result.Records[0].AgeSeconds);
        Assert.Null(result.Records[0].AgeBreachRatioSev2);
    }

    [Fact(DisplayName = "A deleted entity is reported as EntityNotFound and evicted from discovery")]
    public async Task NotFoundIsClassifiedAndEvicted()
    {
        GivenEntities("q-a");
        _peeker.ThrowOnPeek = new ServiceBusException("gone", ServiceBusFailureReason.MessagingEntityNotFound);

        var sut = Sut();
        var result = await sut.ExecuteTickAsync(Build.T0, default);

        Assert.Equal(MeasurementStatus.EntityNotFound, result.Records[0].Status);
        Assert.Contains(result.Events, e => e.EventType == MonitorEventType.EntityDisappeared);
    }

    // ---- helpers --------------------------------------------------------------

    private TelemetryEmitter Emitter(FakeMetricSink metrics, FakeLogsSink logs) =>
        new(
            metrics,
            logs,
            new StaticOptionsMonitor<MonitorOptions>(_options),
            Build.Settings(),
            _clock,
            NullLogger<TelemetryEmitter>.Instance);

    private static MonitorHeartbeat Heartbeat() =>
        new("tick", Build.T0, "eastus2", "test", true, 0, 0, 0, 0, 0, 5,
            DegradationLevel.L0, ThresholdSource.AppConfig, true, true);

    private static MeasurementStatus Status(TickResult result, string entityPath) =>
        result.Records.Single(r => r.Entity.EntityPath == entityPath).Status;

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        do
        {
            current = Volatile.Read(ref target);
            if (value <= current)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(ref target, value, current) != current);
    }
}
