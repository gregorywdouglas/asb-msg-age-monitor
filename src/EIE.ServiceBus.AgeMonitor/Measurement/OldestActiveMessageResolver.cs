using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Measurement;

/// <summary>Spec §3.2 — the bounded forward scan.</summary>
public sealed class OldestActiveMessageResolver : IOldestActiveMessageResolver
{
    private readonly IEntityPeeker _peeker;
    private readonly IScanStateStore _state;
    private readonly ZeroResultCorroborator _corroborator;
    private readonly IDegradationController _degradation;
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly TimeProvider _clock;
    private readonly ILogger<OldestActiveMessageResolver> _log;

    public OldestActiveMessageResolver(
        IEntityPeeker peeker,
        IScanStateStore state,
        ZeroResultCorroborator corroborator,
        IDegradationController degradation,
        IOptionsMonitor<MonitorOptions> options,
        TimeProvider clock,
        ILogger<OldestActiveMessageResolver> log)
    {
        _peeker = peeker;
        _state = state;
        _corroborator = corroborator;
        _degradation = degradation;
        _options = options;
        _clock = clock;
        _log = log;
    }

    public async Task<AgeResolution> ResolveAsync(
        EntityDescriptor entity,
        EntityRuntimeInfo runtime,
        int effectiveScanDepth,
        CancellationToken ct)
    {
        var options = _options.CurrentValue;
        var now = _clock.GetUtcNow();

        InvalidateResumePointIfStale(entity.EntityPath, runtime);

        var fromSequence = _state.GetResumePoint(entity.EntityPath);
        var batchSize = Math.Min(options.PeekBatchSize, effectiveScanDepth);
        var examined = 0;
        var batches = 0;

        while (examined < effectiveScanDepth)
        {
            ct.ThrowIfCancellationRequested();

            var remaining = effectiveScanDepth - examined;
            var thisBatchSize = Math.Min(batchSize, remaining);

            var batch = await _peeker
                .PeekAsync(entity.EntityPath, fromSequence, thisBatchSize, ct)
                .ConfigureAwait(false);

            batches++;

            if (batch.Count == 0)
            {
                // Spec §3.5. An empty batch part-way through a scan is not the same as an
                // empty entity: we already walked past non-Active messages, so the head is
                // blocked rather than the entity drained.
                if (examined > 0)
                {
                    return new AgeResolution(MeasurementStatus.HeadBlockedByDeferred, null, batches, examined);
                }

                var status = _corroborator.Corroborate(
                    entity.EntityPath,
                    runtime.ActiveMessageCount,
                    _degradation.ThrottleObservedThisTick);

                return new AgeResolution(status, null, batches, 0);
            }

            foreach (var message in batch)
            {
                examined++;

                if (message.IsActiveCandidate(now))
                {
                    return new AgeResolution(MeasurementStatus.Measured, message, batches, examined);
                }
            }

            // Spec §3.2: advance past the batch we just rejected so a static deferred
            // block is paid for once rather than on every tick.
            fromSequence = batch[^1].SequenceNumber + 1;
            _state.SetResumePoint(entity.EntityPath, fromSequence.Value);
        }

        _log.LogInformation(
            "Scan budget of {Depth} exhausted for {EntityPath} without finding an Active message",
            effectiveScanDepth,
            entity.EntityPath);

        return new AgeResolution(MeasurementStatus.HeadBlockedByDeferred, null, batches, examined);
    }

    /// <summary>
    /// Spec FR-013. A stale resume point can skip past a genuinely old Active message,
    /// producing a silent under-report — the worst failure this system can produce — so
    /// invalidation is deliberately aggressive.
    /// </summary>
    private void InvalidateResumePointIfStale(string entityPath, EntityRuntimeInfo runtime)
    {
        var previousActive = _state.GetPreviousActiveCount(entityPath);

        if (previousActive is { } previous && runtime.ActiveMessageCount < previous)
        {
            _state.Invalidate(entityPath, "ActiveMessageCount decreased");
            return;
        }

        if (runtime.ActiveMessageCount == 0)
        {
            _state.Invalidate(entityPath, "entity observed empty");
        }
    }
}
