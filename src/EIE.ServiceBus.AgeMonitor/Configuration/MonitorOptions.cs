using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Configuration;

/// <summary>
/// Deployment-time settings (spec §7.1, CFG-001..019). These come from application
/// settings and require a redeploy to change.
/// </summary>
public sealed class MonitorSettings
{
    public string FullyQualifiedNamespace { get; set; } = string.Empty;
    public string SubscriptionId { get; set; } = string.Empty;
    public string ResourceGroup { get; set; } = string.Empty;
    public string NamespaceName { get; set; } = string.Empty;
    public string SourceRegion { get; set; } = string.Empty;
    public string Environment { get; set; } = string.Empty;

    public string AppConfigEndpoint { get; set; } = string.Empty;
    public string AppConfigSentinelKey { get; set; } = "asbmon:sentinel";
    public int AppConfigRefreshIntervalSeconds { get; set; } = 30;

    public string LeaseStorageAccountUri { get; set; } = string.Empty;
    public string LeaseContainerName { get; set; } = "asbmon-lease";
    public string LeaseBlobName { get; set; } = "primary";

    public string DceEndpoint { get; set; } = string.Empty;
    public string DcrImmutableId { get; set; } = string.Empty;
    public string MeasurementStream { get; set; } = "Custom-ServiceBusMessageAge_CL";
    public string HealthStream { get; set; } = "Custom-ServiceBusMonitorHealth_CL";
    public string MetricNamespace { get; set; } = "EIE.ServiceBus";

    public string HashSalt { get; set; } = string.Empty;
}

/// <summary>
/// Runtime configuration (spec §7.2, CFG-100..190) sourced from Azure App
/// Configuration. Every value is clamped on read; invalid values are rejected and
/// last-known-good is retained (FR-033).
/// </summary>
public sealed class MonitorOptions
{
    // --- Thresholds (CFG-100..107) -----------------------------------------
    /// <summary>Spec FR-031/FR-034: the compiled last-resort fallback.</summary>
    public const int CompiledDefaultSev2Seconds = 300;

    public const int CompiledDefaultSev1Seconds = 900;

    public int DefaultSev2Seconds { get; set; } = CompiledDefaultSev2Seconds;
    public int DefaultSev1Seconds { get; set; } = CompiledDefaultSev1Seconds;

    public Dictionary<CriticalityClass, ThresholdPair> ClassThresholds { get; set; } = new()
    {
        [CriticalityClass.Critical] = new ThresholdPair(60, 180),
        [CriticalityClass.Standard] = new ThresholdPair(300, 900),
        [CriticalityClass.Bulk] = new ThresholdPair(1800, 3600)
    };

    public Dictionary<string, ThresholdPair> EntityThresholds { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Spec CFG-106/FR-038: a healthy forwarder holds messages for milliseconds.</summary>
    public ThresholdPair ForwarderThresholds { get; set; } = new(60, 180);

    public Dictionary<string, CriticalityClass> EntityClasses { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);

    // --- Scan (CFG-110..113) ------------------------------------------------
    public int MaxPeekScanDepth { get; set; } = 5000;
    public int PeekBatchSize { get; set; } = 250;
    public int ResumePointMaxTicks { get; set; } = 15;
    public int ColdStartTicks { get; set; } = 3;

    // --- Tick (CFG-120..122) ------------------------------------------------
    public int MaxConcurrentEntityScans { get; set; } = 8;
    public int TickDeadlineSeconds { get; set; } = 45;
    public int PerEntityTimeoutSeconds { get; set; } = 10;

    // --- Discovery (CFG-130..131) -------------------------------------------
    public int DiscoveryCacheTtlSeconds { get; set; } = 300;
    public int TombstoneTtlHours { get; set; } = 24;

    // --- Degradation (CFG-140..142) -----------------------------------------
    public int RecoveryCleanTicks { get; set; } = 3;
    public int ThrottleTicksToEscalate { get; set; } = 2;
    public double MaxNamespaceMuSharePercent { get; set; } = 2.0;

    // --- Corroboration and stall (CFG-150..152) -----------------------------
    public int ZeroCorroborationTicks { get; set; } = 3;
    public int IndeterminateTicksForAlert { get; set; } = 3;
    public int StalledTicksForAlert { get; set; } = 5;

    // --- Clock (CFG-160..162) -----------------------------------------------
    public int ClockSkewToleranceSeconds { get; set; } = 2;
    public int ImplausibleAgeSeconds { get; set; } = 2_592_000;
    public int ClockAnomalyTicksForAlert { get; set; } = 5;

    // --- Lease (CFG-170..173) -----------------------------------------------
    public int LeaseDurationSeconds { get; set; } = 60;
    public int LeaseRenewSeconds { get; set; } = 20;
    public int LeaseReleaseTimeoutSeconds { get; set; } = 5;
    public int SplitBrainAlertMinutes { get; set; } = 5;

    // --- Emission (CFG-180..182) --------------------------------------------
    public int EmitRetryCount { get; set; } = 3;

    /// <summary>Spec NFR-018: raw business keys are never written by default.</summary>
    public bool EmitRawMessageIdentifiers { get; set; }

    public int StaleConfigMinutes { get; set; } = 15;

    // --- Maintenance (CFG-190) ----------------------------------------------
    public int MaxSuppressionHours { get; set; } = 4;
}

public readonly record struct ThresholdPair(int Sev2Seconds, int Sev1Seconds);
