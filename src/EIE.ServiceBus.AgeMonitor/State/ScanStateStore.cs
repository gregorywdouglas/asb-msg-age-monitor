using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.State;

/// <summary>
/// Spec NFR-020. In-process, non-durable, advisory state. Every entry is lost on
/// restart, scale-out or host recycle, and nothing here participates in correctness —
/// losing all of it costs exactly one expensive tick to rebuild.
/// </summary>
public sealed class ScanStateStore : IScanStateStore
{
    private readonly ConcurrentDictionary<string, EntityScanState> _state =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly ILogger<ScanStateStore> _log;
    private long _tickNumber;

    public ScanStateStore(IOptionsMonitor<MonitorOptions> options, ILogger<ScanStateStore> log)
    {
        _options = options;
        _log = log;
    }

    public long CurrentTick => Interlocked.Read(ref _tickNumber);

    public void BeginTick() => Interlocked.Increment(ref _tickNumber);

    public long? GetResumePoint(string entityPath)
    {
        if (!_state.TryGetValue(entityPath, out var state) || state.ResumePoint is null)
        {
            return null;
        }

        // Spec FR-013: hard TTL regardless of any other condition.
        var age = CurrentTick - state.ResumePointSetAtTick;
        if (age >= _options.CurrentValue.ResumePointMaxTicks)
        {
            Invalidate(entityPath, $"resume point exceeded {_options.CurrentValue.ResumePointMaxTicks} ticks");
            return null;
        }

        return state.ResumePoint;
    }

    public void SetResumePoint(string entityPath, long sequenceNumber)
    {
        var state = _state.GetOrAdd(entityPath, static _ => new EntityScanState());
        state.ResumePoint = sequenceNumber;
        state.ResumePointSetAtTick = CurrentTick;
    }

    public void Invalidate(string entityPath, string reason)
    {
        if (!_state.TryGetValue(entityPath, out var state) || state.ResumePoint is null)
        {
            return;
        }

        state.ResumePoint = null;
        state.ResumePointSetAtTick = 0;

        _log.LogDebug("Resume point for {EntityPath} invalidated: {Reason}", entityPath, reason);
    }

    public long? GetPreviousHeadSequence(string entityPath) =>
        _state.TryGetValue(entityPath, out var state) ? state.PreviousHeadSequence : null;

    public double? GetPreviousAgeSeconds(string entityPath) =>
        _state.TryGetValue(entityPath, out var state) ? state.PreviousAgeSeconds : null;

    public long? GetPreviousActiveCount(string entityPath) =>
        _state.TryGetValue(entityPath, out var state) ? state.PreviousActiveCount : null;

    public MeasurementStatus? GetPreviousStatus(string entityPath) =>
        _state.TryGetValue(entityPath, out var state) ? state.PreviousStatus : null;

    public int GetStalledTickCount(string entityPath) =>
        _state.TryGetValue(entityPath, out var state) ? state.StalledTickCount : 0;

    public int GetZeroStreak(string entityPath) =>
        _state.TryGetValue(entityPath, out var state) ? state.ZeroStreak : 0;

    public int GetIndeterminateStreak(string entityPath) =>
        _state.TryGetValue(entityPath, out var state) ? state.IndeterminateStreak : 0;

    public void RecordObservation(
        string entityPath,
        MeasurementStatus status,
        long? headSequence,
        double? ageSeconds,
        long activeCount)
    {
        var state = _state.GetOrAdd(entityPath, static _ => new EntityScanState());

        // Spec §3.7. The stall counter advances only across ticks that were genuinely
        // measured with messages present: an empty queue trivially has an unchanging
        // head, and a stall inferred across an Indeterminate gap is guessed, not observed.
        if (status == MeasurementStatus.Measured && activeCount > 0 && headSequence is { } head)
        {
            state.StalledTickCount = state.PreviousHeadSequence == head
                ? state.StalledTickCount + 1
                : 1;
        }
        else
        {
            state.StalledTickCount = 0;
        }

        state.ZeroStreak = activeCount == 0 && status is MeasurementStatus.Empty or MeasurementStatus.Indeterminate
            ? state.ZeroStreak + 1
            : 0;

        state.IndeterminateStreak = status == MeasurementStatus.Indeterminate
            ? state.IndeterminateStreak + 1
            : 0;

        state.PreviousHeadSequence = headSequence;
        state.PreviousAgeSeconds = ageSeconds;
        state.PreviousActiveCount = activeCount;
        state.PreviousStatus = status;
    }

    public void Forget(string entityPath) => _state.TryRemove(entityPath, out _);

    private sealed class EntityScanState
    {
        public long? ResumePoint;
        public long ResumePointSetAtTick;
        public long? PreviousHeadSequence;
        public double? PreviousAgeSeconds;
        public long? PreviousActiveCount;
        public MeasurementStatus? PreviousStatus;
        public int StalledTickCount;
        public int ZeroStreak;
        public int IndeterminateStreak;
    }
}
