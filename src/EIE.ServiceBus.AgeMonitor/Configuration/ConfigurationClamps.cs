namespace EIE.ServiceBus.AgeMonitor.Configuration;

/// <summary>
/// Spec FR-033 and §7.2. Out-of-range values are <em>rejected</em>, not silently
/// coerced: a fat-fingered threshold must fail loudly and keep last-known-good,
/// because the whole point of no-redeploy configuration is that nothing else gates it.
/// </summary>
public static class ConfigurationClamps
{
    public const int ThresholdMinSeconds = 30;
    public const int ThresholdMaxSeconds = 86_400;

    public static readonly IReadOnlyDictionary<string, (int Min, int Max)> IntegerRanges =
        new Dictionary<string, (int, int)>(StringComparer.Ordinal)
        {
            ["scan:maxPeekScanDepth"] = (250, 50_000),
            ["scan:peekBatchSize"] = (50, 250),
            ["scan:resumePointMaxTicks"] = (1, 100),
            ["scan:coldStartTicks"] = (0, 20),
            ["tick:maxConcurrentEntityScans"] = (1, 64),
            ["tick:deadlineSeconds"] = (10, 55),
            ["tick:perEntityTimeoutSeconds"] = (2, 30),
            ["discovery:cacheTtlSeconds"] = (60, 3600),
            ["discovery:tombstoneTtlHours"] = (1, 168),
            ["degradation:recoveryCleanTicks"] = (1, 20),
            ["degradation:throttleTicksToEscalate"] = (1, 10),
            ["corroboration:zeroCorroborationTicks"] = (1, 10),
            ["corroboration:indeterminateTicksForAlert"] = (1, 20),
            ["stall:stalledTicksForAlert"] = (2, 30),
            ["clock:skewToleranceSeconds"] = (1, 60),
            ["clock:anomalyTicksForAlert"] = (1, 50),
            ["lease:durationSeconds"] = (15, 60),
            ["lease:renewSeconds"] = (5, 30),
            ["lease:releaseTimeoutSeconds"] = (1, 15),
            ["lease:splitBrainAlertMinutes"] = (1, 60),
            ["emit:retryCount"] = (0, 5),
            ["emit:staleConfigMinutes"] = (1, 120),
            ["maintenance:maxSuppressionHours"] = (1, 72)
        };

    /// <summary>
    /// Spec FR-033: <c>30 &lt;= sev2 &lt; sev1 &lt;= 86400</c>. Strict inequality between
    /// the two is required — equal thresholds would make Sev1 and Sev2 fire together,
    /// which reads as an escalation that never escalates.
    /// </summary>
    public static bool IsValidThresholdPair(int sev2Seconds, int sev1Seconds, out string? reason)
    {
        if (sev2Seconds < ThresholdMinSeconds || sev2Seconds > ThresholdMaxSeconds)
        {
            reason = $"sev2={sev2Seconds} outside [{ThresholdMinSeconds},{ThresholdMaxSeconds}]";
            return false;
        }

        if (sev1Seconds < ThresholdMinSeconds || sev1Seconds > ThresholdMaxSeconds)
        {
            reason = $"sev1={sev1Seconds} outside [{ThresholdMinSeconds},{ThresholdMaxSeconds}]";
            return false;
        }

        if (sev2Seconds >= sev1Seconds)
        {
            reason = $"sev2={sev2Seconds} must be strictly less than sev1={sev1Seconds}";
            return false;
        }

        reason = null;
        return true;
    }

    public static bool IsValidInteger(string key, int value, out string? reason)
    {
        if (!IntegerRanges.TryGetValue(key, out var range))
        {
            reason = null;
            return true;
        }

        if (value < range.Min || value > range.Max)
        {
            reason = $"{key}={value} outside [{range.Min},{range.Max}]";
            return false;
        }

        reason = null;
        return true;
    }

    /// <summary>
    /// Cross-field invariants that a per-key range cannot express. Spec CFG-121
    /// (deadline must fit inside the tick interval) and CFG-171 (renew must leave
    /// room for at least two attempts before expiry).
    /// </summary>
    public static IReadOnlyList<string> ValidateInvariants(MonitorOptions options, int tickIntervalSeconds = 60)
    {
        var problems = new List<string>();

        if (options.TickDeadlineSeconds >= tickIntervalSeconds)
        {
            problems.Add(
                $"tick:deadlineSeconds={options.TickDeadlineSeconds} must be less than the tick interval {tickIntervalSeconds}");
        }

        if (options.PerEntityTimeoutSeconds > options.TickDeadlineSeconds)
        {
            problems.Add(
                $"tick:perEntityTimeoutSeconds={options.PerEntityTimeoutSeconds} exceeds tick:deadlineSeconds={options.TickDeadlineSeconds}");
        }

        if (options.LeaseRenewSeconds * 2 > options.LeaseDurationSeconds)
        {
            problems.Add(
                $"lease:renewSeconds={options.LeaseRenewSeconds} must be at most half of lease:durationSeconds={options.LeaseDurationSeconds}");
        }

        if (options.PeekBatchSize > options.MaxPeekScanDepth)
        {
            problems.Add(
                $"scan:peekBatchSize={options.PeekBatchSize} exceeds scan:maxPeekScanDepth={options.MaxPeekScanDepth}");
        }

        return problems;
    }
}
