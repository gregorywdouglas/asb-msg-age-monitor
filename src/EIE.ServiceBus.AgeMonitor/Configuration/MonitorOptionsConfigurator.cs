using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Configuration;

/// <summary>
/// Binds the <c>asbmon:*</c> keys from App Configuration onto <see cref="MonitorOptions"/>.
/// <para>
/// Spec FR-033/FR-034: every value is clamp-checked on bind. An out-of-range value is
/// rejected and the compiled or previously bound value is retained, rather than being
/// silently coerced — a fat-fingered threshold must fail loudly, because no redeploy
/// gate stands between the edit and production.
/// </para>
/// </summary>
public sealed class MonitorOptionsConfigurator : IConfigureOptions<MonitorOptions>
{
    private const string Prefix = "asbmon:";

    private readonly IConfiguration _configuration;
    private readonly IConfigurationHealth _health;
    private readonly ILogger<MonitorOptionsConfigurator> _log;

    public MonitorOptionsConfigurator(
        IConfiguration configuration,
        IConfigurationHealth health,
        ILogger<MonitorOptionsConfigurator> log)
    {
        _configuration = configuration;
        _health = health;
        _log = log;
    }

    public void Configure(MonitorOptions options)
    {
        var sawAnyKey = false;

        Bind("scan:maxPeekScanDepth", v => options.MaxPeekScanDepth = v, ref sawAnyKey);
        Bind("scan:peekBatchSize", v => options.PeekBatchSize = v, ref sawAnyKey);
        Bind("scan:resumePointMaxTicks", v => options.ResumePointMaxTicks = v, ref sawAnyKey);
        Bind("scan:coldStartTicks", v => options.ColdStartTicks = v, ref sawAnyKey);

        Bind("tick:maxConcurrentEntityScans", v => options.MaxConcurrentEntityScans = v, ref sawAnyKey);
        Bind("tick:deadlineSeconds", v => options.TickDeadlineSeconds = v, ref sawAnyKey);
        Bind("tick:perEntityTimeoutSeconds", v => options.PerEntityTimeoutSeconds = v, ref sawAnyKey);

        Bind("discovery:cacheTtlSeconds", v => options.DiscoveryCacheTtlSeconds = v, ref sawAnyKey);
        Bind("discovery:tombstoneTtlHours", v => options.TombstoneTtlHours = v, ref sawAnyKey);

        Bind("degradation:recoveryCleanTicks", v => options.RecoveryCleanTicks = v, ref sawAnyKey);
        Bind("degradation:throttleTicksToEscalate", v => options.ThrottleTicksToEscalate = v, ref sawAnyKey);

        Bind("corroboration:zeroCorroborationTicks", v => options.ZeroCorroborationTicks = v, ref sawAnyKey);
        Bind("corroboration:indeterminateTicksForAlert", v => options.IndeterminateTicksForAlert = v, ref sawAnyKey);
        Bind("stall:stalledTicksForAlert", v => options.StalledTicksForAlert = v, ref sawAnyKey);

        Bind("clock:skewToleranceSeconds", v => options.ClockSkewToleranceSeconds = v, ref sawAnyKey);
        Bind("clock:anomalyTicksForAlert", v => options.ClockAnomalyTicksForAlert = v, ref sawAnyKey);

        Bind("lease:durationSeconds", v => options.LeaseDurationSeconds = v, ref sawAnyKey);
        Bind("lease:renewSeconds", v => options.LeaseRenewSeconds = v, ref sawAnyKey);
        Bind("lease:releaseTimeoutSeconds", v => options.LeaseReleaseTimeoutSeconds = v, ref sawAnyKey);
        Bind("lease:splitBrainAlertMinutes", v => options.SplitBrainAlertMinutes = v, ref sawAnyKey);

        Bind("emit:retryCount", v => options.EmitRetryCount = v, ref sawAnyKey);
        Bind("emit:staleConfigMinutes", v => options.StaleConfigMinutes = v, ref sawAnyKey);
        Bind("maintenance:maxSuppressionHours", v => options.MaxSuppressionHours = v, ref sawAnyKey);

        BindUnclamped("clock:implausibleAgeSeconds", v => options.ImplausibleAgeSeconds = v, ref sawAnyKey);

        if (bool.TryParse(_configuration[$"{Prefix}emit:rawMessageIdentifiers"], out var raw))
        {
            options.EmitRawMessageIdentifiers = raw;
            sawAnyKey = true;
        }

        BindThresholds(options, ref sawAnyKey);
        BindClasses(options, ref sawAnyKey);

        foreach (var problem in ConfigurationClamps.ValidateInvariants(options))
        {
            _log.LogError("Configuration invariant violated, retaining previous value: {Problem}", problem);
        }

        if (sawAnyKey)
        {
            _health.ReportSuccess();
        }
        else
        {
            // Nothing under asbmon:* was visible. Either App Configuration is unreachable
            // or the store is empty; both mean the values in force are not from config.
            _health.ReportFailure("No asbmon:* keys were present in configuration");
        }
    }

    private void BindThresholds(MonitorOptions options, ref bool sawAnyKey)
    {
        if (TryReadPair("threshold:default", out var defaults))
        {
            options.DefaultSev2Seconds = defaults.Sev2Seconds;
            options.DefaultSev1Seconds = defaults.Sev1Seconds;
            sawAnyKey = true;
        }

        if (TryReadPair("threshold:forwarder", out var forwarder))
        {
            options.ForwarderThresholds = forwarder;
            sawAnyKey = true;
        }

        foreach (var criticality in Enum.GetValues<CriticalityClass>())
        {
            var key = $"threshold:class:{criticality.ToString().ToLowerInvariant()}";
            if (TryReadPair(key, out var pair))
            {
                options.ClassThresholds[criticality] = pair;
                sawAnyKey = true;
            }
        }

        var entitySection = _configuration.GetSection($"{Prefix}threshold:entity");
        foreach (var child in entitySection.GetChildren())
        {
            if (TryReadPairFromSection(child, $"threshold:entity:{child.Key}", out var pair))
            {
                options.EntityThresholds[child.Key] = pair;
                sawAnyKey = true;
            }
        }
    }

    private void BindClasses(MonitorOptions options, ref bool sawAnyKey)
    {
        var section = _configuration.GetSection($"{Prefix}class:entity");
        foreach (var child in section.GetChildren())
        {
            if (Enum.TryParse<CriticalityClass>(child.Value, ignoreCase: true, out var parsed))
            {
                options.EntityClasses[child.Key] = parsed;
                sawAnyKey = true;
            }
            else if (!string.IsNullOrWhiteSpace(child.Value))
            {
                _log.LogError("Rejected criticality class '{Value}' for {Entity}", child.Value, child.Key);
            }
        }
    }

    private bool TryReadPair(string key, out ThresholdPair pair) =>
        TryReadPairFromSection(_configuration.GetSection($"{Prefix}{key}"), key, out pair);

    private bool TryReadPairFromSection(IConfigurationSection section, string key, out ThresholdPair pair)
    {
        pair = default;

        if (!int.TryParse(section["sev2"], NumberStyles.Integer, CultureInfo.InvariantCulture, out var sev2) ||
            !int.TryParse(section["sev1"], NumberStyles.Integer, CultureInfo.InvariantCulture, out var sev1))
        {
            return false;
        }

        if (!ConfigurationClamps.IsValidThresholdPair(sev2, sev1, out var reason))
        {
            _log.LogError("Rejected {Key}: {Reason}", key, reason);
            return false;
        }

        pair = new ThresholdPair(sev2, sev1);
        return true;
    }

    private void Bind(string key, Action<int> apply, ref bool sawAnyKey)
    {
        var raw = _configuration[$"{Prefix}{key}"];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return;
        }

        sawAnyKey = true;

        if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            _log.LogError("Rejected {Key}: '{Raw}' is not an integer", key, raw);
            return;
        }

        if (!ConfigurationClamps.IsValidInteger(key, value, out var reason))
        {
            _log.LogError("Rejected {Key}: {Reason}", key, reason);
            return;
        }

        apply(value);
    }

    private void BindUnclamped(string key, Action<int> apply, ref bool sawAnyKey)
    {
        var raw = _configuration[$"{Prefix}{key}"];
        if (string.IsNullOrWhiteSpace(raw))
        {
            return;
        }

        sawAnyKey = true;

        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value > 0)
        {
            apply(value);
        }
        else
        {
            _log.LogError("Rejected {Key}: '{Raw}' is not a positive integer", key, raw);
        }
    }
}
