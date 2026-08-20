using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Telemetry;

/// <summary>
/// Spec FR-045..047. The two sinks fail independently and each records the other's
/// failure, so there is no failure mode in which a sink outage leaves no trace.
/// <para>
/// Emission is deliberately sequential — Application Insights first, then Log
/// Analytics carrying the Application Insights outcome, then a second Application
/// Insights witness if Log Analytics failed. Parallel emission would make
/// cross-witnessing impossible because neither result would be known in time.
/// </para>
/// </summary>
public sealed class TelemetryEmitter : ITelemetryEmitter
{
    private readonly IMetricSink _metrics;
    private readonly ILogsSink _logs;
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly MonitorSettings _settings;
    private readonly TimeProvider _clock;
    private readonly ILogger<TelemetryEmitter> _log;

    public TelemetryEmitter(
        IMetricSink metrics,
        ILogsSink logs,
        IOptionsMonitor<MonitorOptions> options,
        MonitorSettings settings,
        TimeProvider clock,
        ILogger<TelemetryEmitter> log)
    {
        _metrics = metrics;
        _logs = logs;
        _options = options;
        _settings = settings;
        _clock = clock;
        _log = log;
    }

    public async Task<EmitOutcome> EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        IReadOnlyList<MonitorHealthEvent> events,
        CancellationToken ct)
    {
        var retries = _options.CurrentValue.EmitRetryCount;

        var metricError = await TryAsync(
            () => _metrics.EmitAsync(records, heartbeat, ct),
            retries,
            "ApplicationInsights",
            ct).ConfigureAwait(false);

        var allEvents = events;
        if (metricError is not null)
        {
            allEvents = [.. events, WitnessEvent(heartbeat, "ApplicationInsights", metricError)];
        }

        var logsError = await TryAsync(
            () => _logs.EmitAsync(records, heartbeat with { AppInsightsEmitOk = metricError is null }, allEvents, ct),
            retries,
            "LogsIngestion",
            ct).ConfigureAwait(false);

        if (logsError is not null)
        {
            // The Log Analytics write failed, so the record of that failure has to live in
            // the sink that survived.
            try
            {
                await _metrics.WitnessSinkFailureAsync("LogsIngestion", logsError, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to witness the Logs Ingestion failure in Application Insights");
            }
        }

        // Spec FR-047: both outcomes always reach ILogger, which is the last resort when
        // both sinks are down. The L4 no-data rules catch that case.
        if (metricError is not null || logsError is not null)
        {
            _log.LogError(
                "Telemetry emit partial failure. tickId={TickId} appInsightsOk={AppInsightsOk} logsIngestionOk={LogsOk} aiError={AiError} lawError={LawError}",
                heartbeat.TickId,
                metricError is null,
                logsError is null,
                metricError,
                logsError);
        }
        else
        {
            _log.LogInformation(
                "Tick {TickId} emitted {Count} measurements to both sinks in {DurationMs}ms",
                heartbeat.TickId,
                records.Count,
                heartbeat.TickDurationMs);
        }

        return new EmitOutcome(
            metricError is null,
            logsError is null,
            records.Count,
            metricError,
            logsError);
    }

    /// <summary>
    /// Spec FR-046. Retry is bounded inside the tick rather than delegated to the
    /// Functions retry policy: a retried invocation would re-peek the namespace, adding
    /// load during what is most likely already a platform-wide degradation.
    /// </summary>
    private async Task<string?> TryAsync(Func<Task> action, int retries, string sinkName, CancellationToken ct)
    {
        Exception? last = null;

        for (var attempt = 0; attempt <= retries; attempt++)
        {
            if (attempt > 0)
            {
                var delay = TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt - 1));
                try
                {
                    await Task.Delay(delay, _clock, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            try
            {
                await action().ConfigureAwait(false);
                return null;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return $"{sinkName} emit cancelled by host shutdown";
            }
            catch (Exception ex)
            {
                last = ex;
                _log.LogWarning(ex, "{Sink} emit attempt {Attempt} failed", sinkName, attempt + 1);
            }
        }

        return last?.Message ?? $"{sinkName} emit failed";
    }

    private MonitorHealthEvent WitnessEvent(MonitorHeartbeat heartbeat, string sinkName, string error) =>
        new(
            _clock.GetUtcNow(),
            heartbeat.TickId,
            _settings.SourceRegion,
            _settings.Environment,
            MonitorEventType.EmitFailure,
            "Sev2",
            null,
            $"{sinkName} emit failed: {error}");
}
