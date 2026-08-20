using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Measurement;

/// <summary>
/// Spec §3.5 and NFR-001. A throttled namespace returns zero counts and empty peek
/// batches, which is indistinguishable from a drained queue — so the monitor's
/// happiest-looking output is also its blindest state.
/// <para>
/// The governing rule: never promote an uncorroborated zero to <c>Empty</c>.
/// Silence and <c>Indeterminate</c> are acceptable; a fabricated zero is not.
/// </para>
/// </summary>
public sealed class ZeroResultCorroborator
{
    private readonly IScanStateStore _state;
    private readonly IOptionsMonitor<MonitorOptions> _options;

    public ZeroResultCorroborator(IScanStateStore state, IOptionsMonitor<MonitorOptions> options)
    {
        _state = state;
        _options = options;
    }

    public MeasurementStatus Corroborate(
        string entityPath,
        long currentActiveCount,
        bool throttleObservedThisTick)
    {
        // A throttle anywhere in this tick invalidates every zero it produced.
        if (throttleObservedThisTick)
        {
            return MeasurementStatus.Indeterminate;
        }

        // The peek found nothing but runtime properties claim messages exist. The two
        // views contradict each other; neither can be trusted.
        if (currentActiveCount > 0)
        {
            return MeasurementStatus.Indeterminate;
        }

        // Already corroborated on a previous tick — a trusted Empty stays trusted.
        if (_state.GetPreviousStatus(entityPath) == MeasurementStatus.Empty)
        {
            return MeasurementStatus.Empty;
        }

        // An abrupt drop from a non-zero depth in a single tick is not evidence of a
        // drain; it is equally consistent with the broker having stopped answering.
        var previousActive = _state.GetPreviousActiveCount(entityPath);
        if (previousActive is > 0)
        {
            return MeasurementStatus.Indeterminate;
        }

        // Otherwise require a sustained run of clean zeros before claiming health.
        var streakIncludingThisTick = _state.GetZeroStreak(entityPath) + 1;
        return streakIncludingThisTick >= _options.CurrentValue.ZeroCorroborationTicks
            ? MeasurementStatus.Empty
            : MeasurementStatus.Indeterminate;
    }
}
