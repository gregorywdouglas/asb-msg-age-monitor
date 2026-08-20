using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using EIE.ServiceBus.AgeMonitor.Abstractions;

namespace EIE.ServiceBus.AgeMonitor.Functions;

/// <summary>
/// Spec FR-020/FR-021. Sixty-second cadence, <c>runOnStartup: false</c> so a scale
/// event cannot trigger a thundering herd of cold-cache full-depth scans, and
/// singleton execution within the app.
/// </summary>
public sealed class MessageAgeMonitorFunction
{
    private readonly IMonitorTickOrchestrator _orchestrator;
    private readonly TimeProvider _clock;
    private readonly ILogger<MessageAgeMonitorFunction> _log;

    public MessageAgeMonitorFunction(
        IMonitorTickOrchestrator orchestrator,
        TimeProvider clock,
        ILogger<MessageAgeMonitorFunction> log)
    {
        _orchestrator = orchestrator;
        _clock = clock;
        _log = log;
    }

    [Function("MessageAgeMonitor")]
    public async Task RunAsync(
        [TimerTrigger("%MonitorSchedule%", RunOnStartup = false)] TimerInfo timer,
        CancellationToken hostCancellation)
    {
        // Spec FR-051: the identifier must derive from the scheduled occurrence, not
        // execution time, or a retried invocation produces a different MeasurementId and
        // the duplicate it exists to detect becomes undetectable.
        var scheduledTickUtc = ResolveScheduledTick(timer);

        try
        {
            var result = await _orchestrator.ExecuteTickAsync(scheduledTickUtc, hostCancellation).ConfigureAwait(false);

            _log.LogInformation(
                "Tick {TickId} complete: discovered={Discovered} measured={Measured} indeterminate={Indeterminate} skipped={Skipped} blocked={Blocked} degradation={Degradation} durationMs={DurationMs}",
                result.Heartbeat.TickId,
                result.Heartbeat.DiscoveredEntities,
                result.Heartbeat.MeasuredEntities,
                result.Heartbeat.IndeterminateEntities,
                result.Heartbeat.SkippedEntities,
                result.Heartbeat.BlockedEntities,
                result.Heartbeat.Degradation,
                result.Heartbeat.TickDurationMs);
        }
        catch (OperationCanceledException) when (hostCancellation.IsCancellationRequested)
        {
            _log.LogInformation("Tick cancelled by host shutdown");
        }
        catch (Exception ex)
        {
            // Spec FR-046: the invocation must not be failed back to the host. Functions
            // retry would re-peek the namespace, adding load during what is most likely
            // already a platform-wide degradation. The L4 no-data rules (ALR-030/032)
            // detect a monitor that has stopped producing telemetry.
            _log.LogError(ex, "Tick failed; suppressing to avoid a re-peek retry");
        }
    }

    private DateTimeOffset ResolveScheduledTick(TimerInfo timer)
    {
        var scheduled = timer.ScheduleStatus?.Next ?? timer.ScheduleStatus?.Last;

        if (scheduled is { } value && value != default)
        {
            return new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc));
        }

        // First invocation after a cold start has no schedule status yet. Truncating to
        // the minute keeps the identifier stable across a retry of this same invocation.
        var now = _clock.GetUtcNow();
        return new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, TimeSpan.Zero);
    }
}
