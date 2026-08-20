using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.State;

/// <summary>
/// Spec §4.5 (FR-040..044). The monitor peeks the same namespace it observes, so
/// under load its own cost rises exactly when the namespace is most stressed. This
/// controller sheds load in a defined, visible order.
/// </summary>
public sealed class DegradationController : IDegradationController
{
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly ILogger<DegradationController> _log;
    private readonly Lock _gate = new();

    private DegradationLevel _level = DegradationLevel.L0;
    private int _consecutiveThrottleTicks;
    private int _consecutiveCleanTicks;
    private bool _throttleThisTick;

    public DegradationController(IOptionsMonitor<MonitorOptions> options, ILogger<DegradationController> log)
    {
        _options = options;
        _log = log;
    }

    public DegradationLevel CurrentLevel
    {
        get
        {
            lock (_gate)
            {
                return _level;
            }
        }
    }

    public bool ThrottleObservedThisTick
    {
        get
        {
            lock (_gate)
            {
                return _throttleThisTick;
            }
        }
    }

    public void BeginTick()
    {
        lock (_gate)
        {
            _throttleThisTick = false;
        }
    }

    public int EffectiveConcurrency
    {
        get
        {
            var configured = _options.CurrentValue.MaxConcurrentEntityScans;
            return CurrentLevel switch
            {
                DegradationLevel.L0 => configured,
                DegradationLevel.L1 => Math.Max(1, configured / 2),
                _ => Math.Max(1, configured / 4)
            };
        }
    }

    /// <summary>
    /// Spec FR-041/FR-042. At the deepest level a critical-class entity retains a
    /// single peek batch: one round-trip is cheap enough that inability to perform it
    /// means the namespace is unavailable, which is a different alert. Going fully
    /// depth-only for critical entities would blind the monitor during exactly the
    /// incident it exists to detect.
    /// </summary>
    public int EffectiveScanDepth(CriticalityClass entityClass)
    {
        var options = _options.CurrentValue;
        var full = options.MaxPeekScanDepth;
        var oneBatch = Math.Min(options.PeekBatchSize, full);

        return CurrentLevel switch
        {
            DegradationLevel.L0 => full,
            DegradationLevel.L1 => full,
            DegradationLevel.L2 => Math.Max(oneBatch, full / 10),
            DegradationLevel.L3 => entityClass switch
            {
                CriticalityClass.Bulk => 0,
                _ => oneBatch
            },
            _ => full
        };
    }

    public void RecordThrottle(string entityPath)
    {
        lock (_gate)
        {
            _throttleThisTick = true;
            _consecutiveCleanTicks = 0;
        }

        _log.LogWarning("Throttling observed while scanning {EntityPath}", entityPath);
    }

    /// <summary>
    /// Spec FR-043. Recovery is hysteretic — one level per N consecutive clean ticks.
    /// Instant recovery oscillates: shed load, namespace recovers, full budget returns,
    /// namespace throttles again. That oscillation is worse than staying degraded.
    /// </summary>
    public void RecordCleanTick()
    {
        lock (_gate)
        {
            if (_throttleThisTick)
            {
                _consecutiveCleanTicks = 0;
                _consecutiveThrottleTicks++;

                if (_consecutiveThrottleTicks >= _options.CurrentValue.ThrottleTicksToEscalate &&
                    _level < DegradationLevel.L3)
                {
                    _level++;
                    _consecutiveThrottleTicks = 0;
                    _log.LogWarning("Monitor degraded to {Level}", _level);
                }

                return;
            }

            _consecutiveThrottleTicks = 0;

            if (_level == DegradationLevel.L0)
            {
                _consecutiveCleanTicks = 0;
                return;
            }

            _consecutiveCleanTicks++;

            if (_consecutiveCleanTicks >= _options.CurrentValue.RecoveryCleanTicks)
            {
                _level--;
                _consecutiveCleanTicks = 0;
                _log.LogInformation("Monitor recovered to {Level}", _level);
            }
        }
    }
}
