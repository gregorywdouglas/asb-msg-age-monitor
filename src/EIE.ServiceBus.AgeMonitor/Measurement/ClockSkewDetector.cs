using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Measurement;

/// <summary>
/// Spec §3.3. Skew is <em>detected, not corrected</em>: applying a derived offset can
/// introduce error rather than remove it, and the resulting error is undetectable.
/// </summary>
public sealed class ClockSkewDetector : IClockSkewDetector
{
    private readonly IOptionsMonitor<MonitorOptions> _options;

    public ClockSkewDetector(IOptionsMonitor<MonitorOptions> options) => _options = options;

    public ClockAssessment Assess(DateTimeOffset enqueuedTimeUtc, DateTimeOffset nowUtc)
    {
        var options = _options.CurrentValue;
        var rawAge = (nowUtc - enqueuedTimeUtc).TotalSeconds;

        if (rawAge < 0)
        {
            // Spec §3.3 lists both "AgeSeconds < 0 -> clamp and emit ClockAnomaly" and
            // "EnqueuedTimeUtc > now + tolerance -> ClockAnomaly". Taken literally the
            // first makes the tolerance dead config and turns sub-second NTP jitter into
            // a permanent ClockAnomaly on every entity, which would keep ALR-019 firing
            // continuously. The clamp is therefore unconditional but the anomaly flag is
            // gated on the tolerance. See SPEC-DEVIATION-001.
            var skewSeconds = -rawAge;
            var beyondTolerance = skewSeconds > options.ClockSkewToleranceSeconds;

            return new ClockAssessment(
                beyondTolerance,
                0d,
                beyondTolerance
                    ? $"EnqueuedTimeUtc is {skewSeconds:F3}s in the future, beyond the {options.ClockSkewToleranceSeconds}s tolerance"
                    : null);
        }

        if (rawAge > options.ImplausibleAgeSeconds)
        {
            return new ClockAssessment(
                true,
                rawAge,
                $"Age {rawAge:F0}s exceeds the implausible-age bound of {options.ImplausibleAgeSeconds}s");
        }

        return new ClockAssessment(false, rawAge, null);
    }
}
