namespace EIE.ServiceBus.AgeMonitor.Domain;

/// <summary>
/// Broker metadata only. Spec §12/§8.2: never carries application properties or
/// body, and identifiers are hashed unless <c>EmitRawMessageIdentifiers</c> is set.
/// </summary>
public sealed record PeekedMessage(
    long SequenceNumber,
    DateTimeOffset EnqueuedTimeUtc,
    int DeliveryCount,
    MessageState State,
    TimeSpan TimeToLive,
    DateTimeOffset? ScheduledEnqueueTimeUtc = null,
    string? MessageIdHash = null,
    string? CorrelationIdHash = null)
{
    /// <summary>
    /// Spec FR-011: expired-but-not-collected messages are not age candidates.
    /// A zero or negative TTL is treated as "no expiry" rather than "already
    /// expired" — the SDK reports <see cref="TimeSpan.MaxValue"/> for entities
    /// with no TTL, and arithmetic on it overflows.
    /// </summary>
    public bool IsExpiredAt(DateTimeOffset nowUtc)
    {
        if (TimeToLive <= TimeSpan.Zero || TimeToLive == TimeSpan.MaxValue)
        {
            return false;
        }

        var remainingTicks = DateTimeOffset.MaxValue.Ticks - EnqueuedTimeUtc.Ticks;
        if (TimeToLive.Ticks >= remainingTicks)
        {
            return false;
        }

        return EnqueuedTimeUtc + TimeToLive < nowUtc;
    }

    /// <summary>
    /// Spec FR-011: a scheduled message whose enqueue time has not yet arrived is
    /// not an age candidate. State alone is not sufficient — a message can carry a
    /// past scheduled time and be genuinely Active.
    /// </summary>
    public bool IsPendingSchedule(DateTimeOffset nowUtc) =>
        State == MessageState.Scheduled ||
        (ScheduledEnqueueTimeUtc is { } scheduled && scheduled > nowUtc);

    /// <summary>Spec §3.2: candidate for the oldest-Active determination.</summary>
    public bool IsActiveCandidate(DateTimeOffset nowUtc) =>
        State == MessageState.Active &&
        !IsPendingSchedule(nowUtc) &&
        !IsExpiredAt(nowUtc);
}

public sealed record EntityDescriptor(
    string EntityPath,
    EntityKind Kind,
    EntityRole Role = EntityRole.Terminal,
    string? ForwardTarget = null,
    bool IsPartitioned = false,
    int MaxDeliveryCount = 10,
    TimeSpan DefaultTimeToLive = default,
    CriticalityClass Class = CriticalityClass.Standard,
    bool IsClassExplicit = false)
{
    /// <summary>Spec FR-072.</summary>
    public AgeAccuracy Accuracy => IsPartitioned ? AgeAccuracy.BestEffort : AgeAccuracy.Exact;
}

public sealed record EntityRuntimeInfo(
    long ActiveMessageCount,
    long DeadLetterMessageCount,
    long ScheduledMessageCount,
    long TransferDeadLetterMessageCount,
    DateTimeOffset ReadAtUtc)
{
    public static EntityRuntimeInfo Unknown(DateTimeOffset at) => new(0, 0, 0, 0, at);
}

public sealed record ThresholdSet(int Sev2Seconds, int Sev1Seconds, ThresholdSource Source);

/// <summary>Spec §6.2.</summary>
public sealed record AgeResolution(
    MeasurementStatus Status,
    PeekedMessage? OldestActive,
    int BatchesUsed,
    int MessagesExamined);

/// <summary>Spec §3.3.</summary>
public sealed record ClockAssessment(bool IsAnomalous, double AgeSeconds, string? Reason);

/// <summary>Spec §6.2.</summary>
public sealed record EmitOutcome(
    bool AppInsightsOk,
    bool LogsIngestionOk,
    int RecordsSent,
    string? AppInsightsError = null,
    string? LogsIngestionError = null);

public sealed record MeasurementRecord(
    string MeasurementId,
    int SchemaVersion,
    DateTimeOffset TimeGenerated,
    string TickId,
    string SourceRegion,
    string Environment,
    string NamespaceName,
    EntityDescriptor Entity,
    MeasurementStatus Status,
    AgeAccuracy Accuracy,
    double? AgeSeconds,
    double? AgeDeltaSeconds,
    double? AgeBreachRatioSev1,
    double? AgeBreachRatioSev2,
    ThresholdSet Thresholds,
    EntityRuntimeInfo Runtime,
    long? OldestSequenceNumber,
    int? OldestDeliveryCount,
    string? OldestMessageIdHash,
    string? OldestCorrelationIdHash,
    int StalledTickCount,
    bool ConsumptionStalled,
    DegradationLevel Degradation,
    string? MeasurementContext,
    int ScanBatchesUsed,
    int ScanMessagesExamined)
{
    public const int CurrentSchemaVersion = 1;

    /// <summary>
    /// Spec §8.1: statuses for which an age value is meaningful and a metric point
    /// may be emitted.
    /// </summary>
    public static bool StatusCarriesAge(MeasurementStatus status) =>
        status is MeasurementStatus.Measured or MeasurementStatus.Empty or MeasurementStatus.Degraded;

    /// <summary>
    /// Spec §8.1 ratio emission rule: ratios are emitted only for Measured and
    /// Degraded. Never zero for a non-measurement — a zero ratio reads as
    /// perfectly healthy and would mask a blind monitor.
    /// </summary>
    public static bool StatusCarriesRatio(MeasurementStatus status) =>
        status is MeasurementStatus.Measured or MeasurementStatus.Degraded;
}

/// <summary>Spec §6.1 / FR-045: the heartbeat carries counts, not just presence.</summary>
public sealed record MonitorHeartbeat(
    string TickId,
    DateTimeOffset TimeGenerated,
    string SourceRegion,
    string Environment,
    bool IsLeaseHolder,
    int DiscoveredEntities,
    int MeasuredEntities,
    int IndeterminateEntities,
    int SkippedEntities,
    int BlockedEntities,
    long TickDurationMs,
    DegradationLevel Degradation,
    ThresholdSource ConfigSource,
    bool AppInsightsEmitOk,
    bool LogsIngestionEmitOk);

/// <summary>Spec §8.3.</summary>
public sealed record MonitorHealthEvent(
    DateTimeOffset TimeGenerated,
    string TickId,
    string SourceRegion,
    string Environment,
    string EventType,
    string Severity,
    string? EntityPath = null,
    string? Detail = null);

/// <summary>Spec §6.3.</summary>
public sealed record TickResult(
    MonitorHeartbeat Heartbeat,
    IReadOnlyList<MeasurementRecord> Records,
    IReadOnlyList<MonitorHealthEvent> Events,
    EmitOutcome Emit);
