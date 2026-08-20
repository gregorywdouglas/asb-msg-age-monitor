namespace EIE.ServiceBus.AgeMonitor.Domain;

/// <summary>Broker-assigned message state. Spec §3.2.</summary>
public enum MessageState
{
    Active,
    Deferred,
    Scheduled
}

/// <summary>
/// Spec §3.6. Absence of age is an explicit value, never a missing field or a
/// dropped record: a missing row and a healthy row are indistinguishable.
/// </summary>
public enum MeasurementStatus
{
    Measured,
    Empty,
    Indeterminate,
    HeadBlockedByDeferred,
    Degraded,
    NotMeasured,
    Throttled,
    ClockAnomaly,
    EntityNotFound
}

public enum EntityKind
{
    Queue,
    Subscription
}

public enum EntityRole
{
    Terminal,
    Forwarder
}

public enum CriticalityClass
{
    Critical,
    Standard,
    Bulk
}

public enum ThresholdSource
{
    AppConfig,
    LastKnownGood,
    CompiledDefault
}

/// <summary>
/// Spec FR-072. On a partitioned entity an age is a lower bound only; absence
/// of breach is not evidence of health.
/// </summary>
public enum AgeAccuracy
{
    Exact,
    BestEffort
}

/// <summary>Spec FR-041.</summary>
public enum DegradationLevel
{
    L0 = 0,
    L1 = 1,
    L2 = 2,
    L3 = 3
}

/// <summary>Spec §8.3 <c>EventType</c> values.</summary>
public static class MonitorEventType
{
    public const string Heartbeat = "Heartbeat";
    public const string MonitorTickIncomplete = "MonitorTickIncomplete";
    public const string MonitorDegraded = "MonitorDegraded";
    public const string ConfigRejected = "ConfigRejected";
    public const string ConfigStale = "ConfigStale";
    public const string EmitFailure = "EmitFailure";
    public const string LeaseAcquired = "LeaseAcquired";
    public const string LeaseReleased = "LeaseReleased";
    public const string LeaseTakeover = "LeaseTakeover";
    public const string SplitBrain = "SplitBrain";
    public const string ClockAnomaly = "ClockAnomaly";
    public const string EntityDisappeared = "EntityDisappeared";
    public const string EntityRecovered = "EntityRecovered";
    public const string EntityUnclassified = "EntityUnclassified";
    public const string UnsupportedEntityConfiguration = "UnsupportedEntityConfiguration";
    public const string ForwardingCycleDetected = "ForwardingCycleDetected";
    public const string DepthReconciliationDivergence = "DepthReconciliationDivergence";
}

/// <summary>Spec §8.2 <c>MeasurementContext</c> values.</summary>
public static class MeasurementContexts
{
    public const string PostFailover = "PostFailover";
    public const string ColdStart = "ColdStart";
}
