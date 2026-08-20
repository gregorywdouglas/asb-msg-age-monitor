using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Measurement;

namespace EIE.ServiceBus.AgeMonitor.Orchestration;

/// <summary>
/// Spec §4.3 (FR-020..027). Bounded parallelism with a hard per-tick deadline, so
/// that when the budget is genuinely exceeded the degradation is <em>visible</em>
/// rather than silently reducing the sampling interval (NFR-004).
/// </summary>
public sealed class MonitorTickOrchestrator : IMonitorTickOrchestrator
{
    private readonly IEntityDiscoveryService _discovery;
    private readonly IEntityRuntimeReader _runtimeReader;
    private readonly IOldestActiveMessageResolver _resolver;
    private readonly IThresholdProvider _thresholds;
    private readonly IDegradationController _degradation;
    private readonly IScanStateStore _scanState;
    private readonly ILeaseCoordinator _lease;
    private readonly ITelemetryEmitter _emitter;
    private readonly IEntityFaultClassifier _faults;
    private readonly MeasurementRecordFactory _factory;
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly MonitorSettings _settings;
    private readonly IConfigurationHealth _configHealth;
    private readonly TimeProvider _clock;
    private readonly ILogger<MonitorTickOrchestrator> _log;

    /// <summary>Spec FR-025: fair carry-over, placed at the head of the next tick.</summary>
    private readonly ConcurrentDictionary<string, byte> _carryOver = new(StringComparer.OrdinalIgnoreCase);

    private int _ticksSinceStart;
    private bool _wasHolderLastTick;

    public MonitorTickOrchestrator(
        IEntityDiscoveryService discovery,
        IEntityRuntimeReader runtimeReader,
        IOldestActiveMessageResolver resolver,
        IThresholdProvider thresholds,
        IDegradationController degradation,
        IScanStateStore scanState,
        ILeaseCoordinator lease,
        ITelemetryEmitter emitter,
        IEntityFaultClassifier faults,
        MeasurementRecordFactory factory,
        IOptionsMonitor<MonitorOptions> options,
        MonitorSettings settings,
        IConfigurationHealth configHealth,
        TimeProvider clock,
        ILogger<MonitorTickOrchestrator> log)
    {
        _discovery = discovery;
        _runtimeReader = runtimeReader;
        _resolver = resolver;
        _thresholds = thresholds;
        _degradation = degradation;
        _scanState = scanState;
        _lease = lease;
        _emitter = emitter;
        _faults = faults;
        _factory = factory;
        _options = options;
        _settings = settings;
        _configHealth = configHealth;
        _clock = clock;
        _log = log;
    }

    public async Task<TickResult> ExecuteTickAsync(DateTimeOffset scheduledTickUtc, CancellationToken ct)
    {
        var options = _options.CurrentValue;
        var stopwatch = Stopwatch.StartNew();
        var tickId = scheduledTickUtc.UtcDateTime.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var tickIndex = Interlocked.Increment(ref _ticksSinceStart);
        var events = new List<MonitorHealthEvent>();

        _scanState.BeginTick();
        _degradation.BeginTick();

        var isHolder = await TryAcquireLeaseAsync(tickId, scheduledTickUtc, events, ct).ConfigureAwait(false);

        // Spec FR-066: the non-holder still emits a heartbeat, so its liveness remains
        // observable and a standby that has silently died is not mistaken for a standby
        // that is correctly idle.
        if (!isHolder)
        {
            var idleHeartbeat = BuildHeartbeat(tickId, scheduledTickUtc, false, 0, 0, 0, 0, 0, stopwatch, true, true);
            var idleOutcome = await _emitter
                .EmitAsync([], idleHeartbeat, events, ct)
                .ConfigureAwait(false);

            return new TickResult(idleHeartbeat with
            {
                AppInsightsEmitOk = idleOutcome.AppInsightsOk,
                LogsIngestionEmitOk = idleOutcome.LogsIngestionOk
            }, [], events, idleOutcome);
        }

        var entities = await _discovery.GetEntitiesAsync(ct).ConfigureAwait(false);
        var ordered = OrderWithCarryOverFirst(entities);

        using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadlineCts.CancelAfter(TimeSpan.FromSeconds(options.TickDeadlineSeconds));

        var isColdStart = tickIndex <= options.ColdStartTicks;
        var records = new ConcurrentBag<MeasurementRecord>();
        var measuredCarryOver = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

        using var throttle = new SemaphoreSlim(Math.Max(1, _degradation.EffectiveConcurrency));

        var tasks = ordered.Select((entity, index) => MeasureOneAsync(
            entity,
            index,
            ordered.Count,
            tickId,
            scheduledTickUtc,
            isColdStart,
            options,
            throttle,
            records,
            measuredCarryOver,
            deadlineCts.Token,
            ct));

        await Task.WhenAll(tasks).ConfigureAwait(false);

        var deadlineHit = deadlineCts.IsCancellationRequested && !ct.IsCancellationRequested;

        _carryOver.Clear();
        foreach (var path in measuredCarryOver.Keys)
        {
            _carryOver.TryAdd(path, 0);
        }

        _degradation.RecordCleanTick();

        var snapshot = records.ToList();
        AppendTickEvents(events, tickId, scheduledTickUtc, snapshot, ordered.Count, deadlineHit);

        var heartbeat = BuildHeartbeat(
            tickId,
            scheduledTickUtc,
            true,
            ordered.Count,
            snapshot.Count(r => r.Status is MeasurementStatus.Measured or MeasurementStatus.Degraded or MeasurementStatus.Empty),
            snapshot.Count(r => r.Status == MeasurementStatus.Indeterminate),
            snapshot.Count(r => r.Status == MeasurementStatus.NotMeasured),
            snapshot.Count(r => r.Status == MeasurementStatus.HeadBlockedByDeferred),
            stopwatch,
            true,
            true);

        var outcome = await _emitter.EmitAsync(snapshot, heartbeat, events, ct).ConfigureAwait(false);

        if (!outcome.AppInsightsOk || !outcome.LogsIngestionOk)
        {
            events.Add(new MonitorHealthEvent(
                scheduledTickUtc,
                tickId,
                _settings.SourceRegion,
                _settings.Environment,
                MonitorEventType.EmitFailure,
                "Sev2",
                Detail: $"appInsightsOk={outcome.AppInsightsOk} logsIngestionOk={outcome.LogsIngestionOk} " +
                        $"aiError={outcome.AppInsightsError} lawError={outcome.LogsIngestionError}"));
        }

        return new TickResult(
            heartbeat with
            {
                AppInsightsEmitOk = outcome.AppInsightsOk,
                LogsIngestionEmitOk = outcome.LogsIngestionOk
            },
            snapshot,
            events,
            outcome);
    }

    private async Task MeasureOneAsync(
        EntityDescriptor entity,
        int index,
        int totalEntities,
        string tickId,
        DateTimeOffset scheduledTickUtc,
        bool isColdStart,
        MonitorOptions options,
        SemaphoreSlim throttle,
        ConcurrentBag<MeasurementRecord> records,
        ConcurrentDictionary<string, byte> carryOver,
        CancellationToken deadlineToken,
        CancellationToken hostToken)
    {
        var thresholds = _thresholds.Resolve(entity);
        var degradation = _degradation.CurrentLevel;

        // Spec FR-027. A platform-initiated recycle would otherwise produce a full-depth
        // scan across every entity at once — a self-inflicted burst arriving at an
        // unpredictable time. Staggered entities still emit a record so the liveness
        // contract in FR-007 holds.
        if (isColdStart && options.ColdStartTicks > 1 && index % options.ColdStartTicks != (_ticksSinceStart - 1) % options.ColdStartTicks)
        {
            records.Add(BuildNonMeasurement(
                entity, thresholds, degradation, tickId, scheduledTickUtc,
                MeasurementStatus.NotMeasured, MeasurementContexts.ColdStart));
            return;
        }

        try
        {
            await throttle.WaitAsync(deadlineToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            RecordNotMeasured(entity, thresholds, degradation, tickId, scheduledTickUtc, records, carryOver);
            return;
        }

        try
        {
            using var entityCts = CancellationTokenSource.CreateLinkedTokenSource(deadlineToken);
            entityCts.CancelAfter(TimeSpan.FromSeconds(options.PerEntityTimeoutSeconds));

            var runtime = await _runtimeReader
                .GetRuntimeInfoAsync(entity.EntityPath, entityCts.Token)
                .ConfigureAwait(false);

            var depth = isColdStart
                ? Math.Min(options.PeekBatchSize, options.MaxPeekScanDepth)
                : _degradation.EffectiveScanDepth(entity.Class);

            AgeResolution resolution;
            if (depth <= 0)
            {
                // Spec FR-041 L3 for bulk-class entities: depth is known, age is not. The
                // record is still emitted, with a null age rather than a fabricated one.
                resolution = new AgeResolution(MeasurementStatus.Degraded, null, 0, 0);
            }
            else
            {
                resolution = await _resolver
                    .ResolveAsync(entity, runtime, depth, entityCts.Token)
                    .ConfigureAwait(false);
            }

            var record = _factory.Create(new MeasurementInputs(
                entity,
                runtime,
                resolution,
                thresholds,
                degradation,
                tickId,
                scheduledTickUtc,
                isColdStart ? MeasurementContexts.ColdStart : null));

            records.Add(record);
        }
        catch (OperationCanceledException) when (!hostToken.IsCancellationRequested)
        {
            RecordNotMeasured(entity, thresholds, degradation, tickId, scheduledTickUtc, records, carryOver);
        }
        catch (Exception ex)
        {
            var kind = _faults.Classify(ex);
            var status = kind switch
            {
                EntityFaultKind.Throttled => MeasurementStatus.Throttled,
                EntityFaultKind.NotFound => MeasurementStatus.EntityNotFound,
                _ => MeasurementStatus.Indeterminate
            };

            switch (kind)
            {
                case EntityFaultKind.Throttled:
                    _degradation.RecordThrottle(entity.EntityPath);
                    break;
                case EntityFaultKind.NotFound:
                    _discovery.EvictOnNotFound(entity.EntityPath);
                    break;
                default:
                    _log.LogError(ex, "Measurement of {EntityPath} failed", entity.EntityPath);
                    break;
            }

            var record = BuildNonMeasurement(
                entity, thresholds, degradation, tickId, scheduledTickUtc, status, null);

            records.Add(record);
        }
        finally
        {
            throttle.Release();
        }
    }

    private void RecordNotMeasured(
        EntityDescriptor entity,
        ThresholdSet thresholds,
        DegradationLevel degradation,
        string tickId,
        DateTimeOffset scheduledTickUtc,
        ConcurrentBag<MeasurementRecord> records,
        ConcurrentDictionary<string, byte> carryOver)
    {
        carryOver.TryAdd(entity.EntityPath, 0);

        records.Add(BuildNonMeasurement(
            entity, thresholds, degradation, tickId, scheduledTickUtc,
            MeasurementStatus.NotMeasured, null));

    }

    private MeasurementRecord BuildNonMeasurement(
        EntityDescriptor entity,
        ThresholdSet thresholds,
        DegradationLevel degradation,
        string tickId,
        DateTimeOffset scheduledTickUtc,
        MeasurementStatus status,
        string? context) =>
        _factory.Create(new MeasurementInputs(
            entity,
            EntityRuntimeInfo.Unknown(scheduledTickUtc),
            new AgeResolution(status, null, 0, 0),
            thresholds,
            degradation,
            tickId,
            scheduledTickUtc,
            context));

    /// <summary>
    /// Spec FR-025. Carry-over goes to the head, never the tail: a consistently slow
    /// entity at the end of the enumeration would otherwise never be measured at all,
    /// and its absence reads as a healthy queue.
    /// </summary>
    private IReadOnlyList<EntityDescriptor> OrderWithCarryOverFirst(IReadOnlyList<EntityDescriptor> entities)
    {
        if (_carryOver.IsEmpty)
        {
            return entities;
        }

        return entities
            .OrderByDescending(e => _carryOver.ContainsKey(e.EntityPath))
            .ThenBy(e => e.EntityPath, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private async Task<bool> TryAcquireLeaseAsync(
        string tickId,
        DateTimeOffset scheduledTickUtc,
        List<MonitorHealthEvent> events,
        CancellationToken ct)
    {
        bool isHolder;
        try
        {
            isHolder = await _lease.TryAcquireOrRenewAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Spec FR-063: fail open in both directions. Every failure of the coordination
            // store must result in more monitoring, never less — duplicate emission is the
            // safe outcome and is detectable via MeasurementId.
            _log.LogError(ex, "Lease coordination failed; failing open and measuring");
            events.Add(Event(tickId, scheduledTickUtc, MonitorEventType.SplitBrain, "Sev3",
                $"Lease store unreachable, failing open: {ex.Message}"));
            return true;
        }

        if (isHolder && !_wasHolderLastTick)
        {
            events.Add(Event(tickId, scheduledTickUtc, MonitorEventType.LeaseAcquired, "Sev4",
                $"{_settings.SourceRegion} acquired the measurement lease"));
        }
        else if (!isHolder && _wasHolderLastTick)
        {
            events.Add(Event(tickId, scheduledTickUtc, MonitorEventType.LeaseTakeover, "Sev3",
                $"{_settings.SourceRegion} lost the measurement lease"));
        }

        _wasHolderLastTick = isHolder;
        return isHolder;
    }

    private void AppendTickEvents(
        List<MonitorHealthEvent> events,
        string tickId,
        DateTimeOffset scheduledTickUtc,
        IReadOnlyList<MeasurementRecord> records,
        int expectedEntities,
        bool deadlineHit)
    {
        var skipped = records.Count(r => r.Status == MeasurementStatus.NotMeasured);

        if (deadlineHit || skipped > 0)
        {
            events.Add(Event(tickId, scheduledTickUtc, MonitorEventType.MonitorTickIncomplete, "Sev3",
                $"measured={records.Count - skipped} skipped={skipped} expected={expectedEntities} deadlineHit={deadlineHit}"));
        }

        if (_degradation.CurrentLevel > DegradationLevel.L0)
        {
            events.Add(Event(tickId, scheduledTickUtc, MonitorEventType.MonitorDegraded, "Sev3",
                $"level={_degradation.CurrentLevel}"));
        }

        foreach (var rejection in _thresholds.DrainRejections())
        {
            events.Add(Event(tickId, scheduledTickUtc, MonitorEventType.ConfigRejected, "Sev2", rejection));
        }

        if (_thresholds.CurrentSource != ThresholdSource.AppConfig)
        {
            events.Add(Event(tickId, scheduledTickUtc, MonitorEventType.ConfigStale, "Sev3",
                $"source={_thresholds.CurrentSource} lastSuccess={_configHealth.LastSuccessfulRefreshUtc:o} reason={_configHealth.LastFailureReason}"));
        }

        foreach (var record in records.Where(r => r.MeasurementContext?.Contains("tolerance", StringComparison.OrdinalIgnoreCase) == true
                                                  || r.Status == MeasurementStatus.ClockAnomaly))
        {
            events.Add(new MonitorHealthEvent(
                scheduledTickUtc, tickId, _settings.SourceRegion, _settings.Environment,
                MonitorEventType.ClockAnomaly, "Sev3", record.Entity.EntityPath, record.MeasurementContext));
        }

        foreach (var (eventType, severity, entityPath, detail) in _discovery.DrainEvents())
        {
            events.Add(new MonitorHealthEvent(
                scheduledTickUtc, tickId, _settings.SourceRegion, _settings.Environment,
                eventType, severity, entityPath, detail));
        }
    }

    private MonitorHealthEvent Event(
        string tickId, DateTimeOffset at, string eventType, string severity, string detail) =>
        new(at, tickId, _settings.SourceRegion, _settings.Environment, eventType, severity, null, detail);

    private MonitorHeartbeat BuildHeartbeat(
        string tickId,
        DateTimeOffset scheduledTickUtc,
        bool isHolder,
        int discovered,
        int measured,
        int indeterminate,
        int skipped,
        int blocked,
        Stopwatch stopwatch,
        bool appInsightsOk,
        bool logsOk) =>
        new(
            tickId,
            scheduledTickUtc,
            _settings.SourceRegion,
            _settings.Environment,
            isHolder,
            discovered,
            measured,
            indeterminate,
            skipped,
            blocked,
            stopwatch.ElapsedMilliseconds,
            _degradation.CurrentLevel,
            _thresholds.CurrentSource,
            appInsightsOk,
            logsOk);
}
