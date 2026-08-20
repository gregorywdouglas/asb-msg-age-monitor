using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Measurement;
using EIE.ServiceBus.AgeMonitor.State;

namespace EIE.ServiceBus.AgeMonitor.Tests.Support;

internal sealed class StaticOptionsMonitor<T>(T value) : IOptionsMonitor<T>
{
    public T CurrentValue { get; } = value;

    public T Get(string? name) => CurrentValue;

    public IDisposable? OnChange(Action<T, string?> listener) => null;
}

/// <summary>
/// Only <see cref="GetUtcNow"/> is overridden; timers keep using the system clock so
/// that retry backoff in the emitter still completes rather than hanging.
/// </summary>
internal sealed class TestClock(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now = _now.Add(by);

    public void Set(DateTimeOffset to) => _now = to;
}

/// <summary>
/// Serves peek batches from an in-memory message list, honouring
/// <c>fromSequenceNumber</c> the way the broker does.
/// </summary>
internal sealed class FakePeeker : IEntityPeeker
{
    private readonly Dictionary<string, List<PeekedMessage>> _messages = new(StringComparer.OrdinalIgnoreCase);

    public int PeekCallCount { get; private set; }

    public List<(string EntityPath, long? From, int Max)> Calls { get; } = [];

    public Exception? ThrowOnPeek { get; set; }

    public FakePeeker With(string entityPath, IEnumerable<PeekedMessage> messages)
    {
        _messages[entityPath] = messages.OrderBy(m => m.SequenceNumber).ToList();
        return this;
    }

    public Task<IReadOnlyList<PeekedMessage>> PeekAsync(
        string entityPath,
        long? fromSequenceNumber,
        int maxMessages,
        CancellationToken ct)
    {
        PeekCallCount++;
        Calls.Add((entityPath, fromSequenceNumber, maxMessages));

        if (ThrowOnPeek is not null)
        {
            throw ThrowOnPeek;
        }

        if (!_messages.TryGetValue(entityPath, out var all))
        {
            return Task.FromResult<IReadOnlyList<PeekedMessage>>([]);
        }

        var from = fromSequenceNumber ?? long.MinValue;
        IReadOnlyList<PeekedMessage> batch = all
            .Where(m => m.SequenceNumber >= from)
            .Take(maxMessages)
            .ToList();

        return Task.FromResult(batch);
    }
}

internal sealed class FakeRuntimeReader : IEntityRuntimeReader
{
    private readonly Dictionary<string, EntityRuntimeInfo> _runtime = new(StringComparer.OrdinalIgnoreCase);

    public Exception? ThrowOnRead { get; set; }

    public Func<string, Task>? BeforeRead { get; set; }

    public FakeRuntimeReader With(string entityPath, long active, long deadLetter = 0, long scheduled = 0, long transferDeadLetter = 0)
    {
        _runtime[entityPath] = new EntityRuntimeInfo(active, deadLetter, scheduled, transferDeadLetter, DateTimeOffset.UnixEpoch);
        return this;
    }

    public async Task<EntityRuntimeInfo> GetRuntimeInfoAsync(string entityPath, CancellationToken ct)
    {
        if (BeforeRead is not null)
        {
            await BeforeRead(entityPath).ConfigureAwait(false);
        }

        ct.ThrowIfCancellationRequested();

        if (ThrowOnRead is not null)
        {
            throw ThrowOnRead;
        }

        return _runtime.TryGetValue(entityPath, out var info)
            ? info
            : new EntityRuntimeInfo(0, 0, 0, 0, DateTimeOffset.UnixEpoch);
    }
}

internal sealed class FakeCatalog : IEntityCatalog
{
    public List<EntityDescriptor> Entities { get; set; } = [];

    public int ListCallCount { get; private set; }

    public Exception? ThrowOnList { get; set; }

    public Task<IReadOnlyList<EntityDescriptor>> ListEntitiesAsync(CancellationToken ct)
    {
        ListCallCount++;

        if (ThrowOnList is not null)
        {
            throw ThrowOnList;
        }

        return Task.FromResult<IReadOnlyList<EntityDescriptor>>(Entities.ToList());
    }
}

internal sealed class FakeLease : ILeaseCoordinator
{
    public bool ShouldHold { get; set; } = true;

    public Exception? ThrowOnAcquire { get; set; }

    public int ReleaseCallCount { get; private set; }

    public bool IsHolder { get; private set; }

    public DateTimeOffset? AcquiredAtUtc { get; private set; }

    public Task<bool> TryAcquireOrRenewAsync(CancellationToken ct)
    {
        if (ThrowOnAcquire is not null)
        {
            throw ThrowOnAcquire;
        }

        IsHolder = ShouldHold;
        AcquiredAtUtc = ShouldHold ? DateTimeOffset.UnixEpoch : null;
        return Task.FromResult(ShouldHold);
    }

    public Task ReleaseAsync(CancellationToken ct)
    {
        ReleaseCallCount++;
        IsHolder = false;
        return Task.CompletedTask;
    }
}

internal sealed class FakeEmitter : ITelemetryEmitter
{
    public List<IReadOnlyList<MeasurementRecord>> EmittedBatches { get; } = [];

    public List<MonitorHeartbeat> Heartbeats { get; } = [];

    public List<MonitorHealthEvent> Events { get; } = [];

    public bool FailAppInsights { get; set; }

    public bool FailLogsIngestion { get; set; }

    public IReadOnlyList<MeasurementRecord> LastBatch => EmittedBatches.Count > 0 ? EmittedBatches[^1] : [];

    public MonitorHeartbeat LastHeartbeat => Heartbeats[^1];

    public Task<EmitOutcome> EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        IReadOnlyList<MonitorHealthEvent> events,
        CancellationToken ct)
    {
        EmittedBatches.Add(records);
        Heartbeats.Add(heartbeat);
        Events.AddRange(events);

        return Task.FromResult(new EmitOutcome(
            !FailAppInsights,
            !FailLogsIngestion,
            records.Count,
            FailAppInsights ? "forced app insights failure" : null,
            FailLogsIngestion ? "forced logs ingestion failure" : null));
    }
}

internal sealed class FakeMetricSink : IMetricSink
{
    public List<(string Name, double Value, string EntityPath)> Points { get; } = [];

    public List<(string Sink, string Error)> Witnesses { get; } = [];

    public bool Fail { get; set; }

    public int EmitAttempts { get; private set; }

    public Task EmitAsync(IReadOnlyList<MeasurementRecord> records, MonitorHeartbeat heartbeat, CancellationToken ct)
    {
        EmitAttempts++;

        if (Fail)
        {
            throw new InvalidOperationException("forced metric sink failure");
        }

        foreach (var record in records)
        {
            if (MeasurementRecord.StatusCarriesAge(record.Status) && record.AgeSeconds is { } age)
            {
                Points.Add(("OldestMessageAgeSeconds", age, record.Entity.EntityPath));
            }

            if (MeasurementRecord.StatusCarriesRatio(record.Status))
            {
                if (record.AgeBreachRatioSev2 is { } sev2)
                {
                    Points.Add(("AgeBreachRatioSev2", sev2, record.Entity.EntityPath));
                }

                if (record.AgeBreachRatioSev1 is { } sev1)
                {
                    Points.Add(("AgeBreachRatioSev1", sev1, record.Entity.EntityPath));
                }
            }

            Points.Add(("ActiveMessageCount", record.Runtime.ActiveMessageCount, record.Entity.EntityPath));
        }

        Points.Add(("MonitorHeartbeat", 1d, string.Empty));
        return Task.CompletedTask;
    }

    public Task WitnessSinkFailureAsync(string sinkName, string error, CancellationToken ct)
    {
        Witnesses.Add((sinkName, error));
        return Task.CompletedTask;
    }
}

internal sealed class FakeLogsSink : ILogsSink
{
    public List<IReadOnlyList<MeasurementRecord>> Batches { get; } = [];

    public List<MonitorHealthEvent> Events { get; } = [];

    public List<MonitorHeartbeat> Heartbeats { get; } = [];

    public bool Fail { get; set; }

    public int EmitAttempts { get; private set; }

    public Task EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        IReadOnlyList<MonitorHealthEvent> events,
        CancellationToken ct)
    {
        EmitAttempts++;

        if (Fail)
        {
            throw new InvalidOperationException("forced logs sink failure");
        }

        Batches.Add(records);
        Heartbeats.Add(heartbeat);
        Events.AddRange(events);
        return Task.CompletedTask;
    }
}

internal sealed class FakeConfigurationHealth : IConfigurationHealth
{
    public bool IsAvailable { get; set; } = true;

    public bool HasEverLoaded { get; set; } = true;

    public DateTimeOffset? LastSuccessfulRefreshUtc { get; set; } = DateTimeOffset.UnixEpoch;

    public string? LastFailureReason { get; set; }

    public void ReportSuccess()
    {
        IsAvailable = true;
        HasEverLoaded = true;
    }

    public void ReportFailure(string reason)
    {
        IsAvailable = false;
        LastFailureReason = reason;
    }
}

internal static class Build
{
    public static readonly DateTimeOffset T0 = new(2026, 8, 7, 3, 0, 0, TimeSpan.Zero);

    public static MonitorSettings Settings() => new()
    {
        SourceRegion = "eastus2",
        Environment = "test",
        NamespaceName = "sb-eie-test",
        FullyQualifiedNamespace = "sb-eie-test.servicebus.windows.net",
        HashSalt = "test-salt"
    };

    public static EntityDescriptor Queue(
        string path = "q-test",
        CriticalityClass criticality = CriticalityClass.Standard,
        EntityRole role = EntityRole.Terminal,
        bool partitioned = false,
        string? forwardTarget = null) =>
        new(path, EntityKind.Queue, role, forwardTarget, partitioned, 10, TimeSpan.Zero, criticality, true);

    public static PeekedMessage Message(
        long sequence,
        DateTimeOffset enqueued,
        MessageState state = MessageState.Active,
        int deliveryCount = 1,
        TimeSpan? ttl = null,
        DateTimeOffset? scheduledEnqueue = null) =>
        new(sequence, enqueued, deliveryCount, state, ttl ?? TimeSpan.Zero, scheduledEnqueue, $"hash-{sequence}", null);

    public static ILogger<T> Log<T>() => NullLogger<T>.Instance;

    public static ScanStateStore StateStore(MonitorOptions options) =>
        new(new StaticOptionsMonitor<MonitorOptions>(options), Log<ScanStateStore>());

    public static DegradationController Degradation(MonitorOptions options) =>
        new(new StaticOptionsMonitor<MonitorOptions>(options), Log<DegradationController>());

    public static ClockSkewDetector ClockSkew(MonitorOptions options) =>
        new(new StaticOptionsMonitor<MonitorOptions>(options));

    public static ZeroResultCorroborator Corroborator(IScanStateStore state, MonitorOptions options) =>
        new(state, new StaticOptionsMonitor<MonitorOptions>(options));

    public static OldestActiveMessageResolver Resolver(
        IEntityPeeker peeker,
        IScanStateStore state,
        IDegradationController degradation,
        MonitorOptions options,
        TimeProvider clock) =>
        new(
            peeker,
            state,
            Corroborator(state, options),
            degradation,
            new StaticOptionsMonitor<MonitorOptions>(options),
            clock,
            Log<OldestActiveMessageResolver>());

    public static MeasurementRecordFactory Factory(
        IScanStateStore state,
        MonitorOptions options,
        TimeProvider clock,
        MonitorSettings? settings = null) =>
        new(
            ClockSkew(options),
            state,
            new StaticOptionsMonitor<MonitorOptions>(options),
            settings ?? Settings(),
            clock);
}
