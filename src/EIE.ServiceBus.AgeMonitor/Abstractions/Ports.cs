using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Abstractions;

/// <summary>
/// The ONLY path to Service Bus message data.
/// <para>
/// Spec FR-050 / §10.3: this interface deliberately exposes no receive, complete,
/// abandon, defer, dead-letter or renew-lock operation. The managed identity holds
/// <c>Azure Service Bus Data Receiver</c> because no smaller built-in role permits
/// peek, so this type is the architectural guard that stops that privilege from
/// being reachable. TST-070 fails the build if the assembly references any
/// receive-family symbol.
/// </para>
/// </summary>
public interface IEntityPeeker
{
    Task<IReadOnlyList<PeekedMessage>> PeekAsync(
        string entityPath,
        long? fromSequenceNumber,
        int maxMessages,
        CancellationToken ct);
}

public interface IEntityRuntimeReader
{
    Task<EntityRuntimeInfo> GetRuntimeInfoAsync(string entityPath, CancellationToken ct);
}

public enum EntityFaultKind
{
    Transient,
    Throttled,
    NotFound,
    Unauthorized
}

/// <summary>
/// Keeps Azure SDK exception shapes out of the orchestration logic so the tick's
/// failure handling is unit-testable with plain exceptions.
/// </summary>
public interface IEntityFaultClassifier
{
    EntityFaultKind Classify(Exception exception);
}

/// <summary>Raw entity enumeration from the ARM management plane. Spec §10.3.</summary>
public interface IEntityCatalog
{
    Task<IReadOnlyList<EntityDescriptor>> ListEntitiesAsync(CancellationToken ct);
}

public interface IEntityDiscoveryService
{
    Task<IReadOnlyList<EntityDescriptor>> GetEntitiesAsync(CancellationToken ct);

    /// <summary>Spec FR-004: evict on <c>MessagingEntityNotFoundException</c>.</summary>
    void EvictOnNotFound(string entityPath);

    /// <summary>Spec FR-005/FR-006.</summary>
    bool IsTombstoned(string entityPath);

    /// <summary>Health events raised by discovery since the previous drain.</summary>
    IReadOnlyList<(string EventType, string Severity, string EntityPath, string Detail)> DrainEvents();
}

public interface IThresholdProvider
{
    ThresholdSet Resolve(EntityDescriptor entity);

    ThresholdSource CurrentSource { get; }

    /// <summary>Spec FR-033: rejected values, drained for emission as ConfigRejected.</summary>
    IReadOnlyList<string> DrainRejections();
}

public interface IOldestActiveMessageResolver
{
    Task<AgeResolution> ResolveAsync(
        EntityDescriptor entity,
        EntityRuntimeInfo runtime,
        int effectiveScanDepth,
        CancellationToken ct);
}

/// <summary>
/// Spec NFR-020: in-process, non-durable, advisory state. Lost on restart; nothing
/// here participates in correctness.
/// </summary>
public interface IScanStateStore
{
    /// <summary>Advances the tick counter that backs the resume-point hard TTL (FR-013).</summary>
    void BeginTick();

    long CurrentTick { get; }

    long? GetResumePoint(string entityPath);

    void SetResumePoint(string entityPath, long sequenceNumber);

    void Invalidate(string entityPath, string reason);

    long? GetPreviousHeadSequence(string entityPath);

    double? GetPreviousAgeSeconds(string entityPath);

    long? GetPreviousActiveCount(string entityPath);

    /// <summary>Spec §3.5: a previously corroborated Empty stays trusted.</summary>
    MeasurementStatus? GetPreviousStatus(string entityPath);

    int GetStalledTickCount(string entityPath);

    int GetZeroStreak(string entityPath);

    int GetIndeterminateStreak(string entityPath);

    /// <summary>
    /// Records a completed observation and advances all per-entity counters.
    /// Spec §3.2 / §3.5 / §3.7.
    /// </summary>
    void RecordObservation(
        string entityPath,
        MeasurementStatus status,
        long? headSequence,
        double? ageSeconds,
        long activeCount);

    void Forget(string entityPath);
}

public interface IDegradationController
{
    DegradationLevel CurrentLevel { get; }

    int EffectiveConcurrency { get; }

    /// <summary>Spec FR-042: a critical-class entity never loses all peek measurement.</summary>
    int EffectiveScanDepth(CriticalityClass entityClass);

    void RecordThrottle(string entityPath);

    /// <summary>Spec FR-043: recovery is hysteretic — one level per N clean ticks.</summary>
    void RecordCleanTick();

    bool ThrottleObservedThisTick { get; }

    void BeginTick();
}

public interface ILeaseCoordinator
{
    Task<bool> TryAcquireOrRenewAsync(CancellationToken ct);

    Task ReleaseAsync(CancellationToken ct);

    bool IsHolder { get; }

    DateTimeOffset? AcquiredAtUtc { get; }
}

public interface ITelemetryEmitter
{
    Task<EmitOutcome> EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        IReadOnlyList<MonitorHealthEvent> events,
        CancellationToken ct);
}

/// <summary>Spec §8.2 / NFR-018: salted SHA-256, never raw business keys by default.</summary>
public interface IMessageIdHasher
{
    string? Hash(string? value);
}

public interface IClockSkewDetector
{
    ClockAssessment Assess(DateTimeOffset enqueuedTimeUtc, DateTimeOffset nowUtc);
}

public interface IMonitorTickOrchestrator
{
    Task<TickResult> ExecuteTickAsync(DateTimeOffset scheduledTickUtc, CancellationToken ct);
}

/// <summary>Sink ports. Spec FR-045: the two sinks fail independently.</summary>
public interface IMetricSink
{
    Task EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        CancellationToken ct);

    /// <summary>
    /// Spec FR-047 cross-witnessing: records the <em>other</em> sink's failure here, so
    /// no failure mode leaves a sink outage without a trace anywhere.
    /// </summary>
    Task WitnessSinkFailureAsync(string sinkName, string error, CancellationToken ct);
}

public interface ILogsSink
{
    Task EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        IReadOnlyList<MonitorHealthEvent> events,
        CancellationToken ct);
}
