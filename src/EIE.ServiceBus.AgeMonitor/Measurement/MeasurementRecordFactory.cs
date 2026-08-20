using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Measurement;

public sealed record MeasurementInputs(
    EntityDescriptor Entity,
    EntityRuntimeInfo Runtime,
    AgeResolution Resolution,
    ThresholdSet Thresholds,
    DegradationLevel Degradation,
    string TickId,
    DateTimeOffset ScheduledTickUtc,
    string? MeasurementContext);

/// <summary>
/// Builds the emitted record. Spec §8.2 plus the ratio and status rules that keep the
/// monitor from ever asserting health it cannot substantiate (NFR-001).
/// </summary>
public sealed class MeasurementRecordFactory
{
    private readonly IClockSkewDetector _clockSkew;
    private readonly IScanStateStore _state;
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly MonitorSettings _settings;
    private readonly TimeProvider _clock;

    public MeasurementRecordFactory(
        IClockSkewDetector clockSkew,
        IScanStateStore state,
        IOptionsMonitor<MonitorOptions> options,
        MonitorSettings settings,
        TimeProvider clock)
    {
        _clockSkew = clockSkew;
        _state = state;
        _options = options;
        _settings = settings;
        _clock = clock;
    }

    /// <summary>
    /// Builds the record and advances the per-entity counters as one step.
    /// <para>
    /// The state update deliberately lives here rather than in the caller: the stall
    /// counter must reflect <em>this</em> tick's observation while the age delta must be
    /// computed against the <em>previous</em> one, and splitting those across two call
    /// sites makes the record silently lag by a tick if the ordering is ever changed.
    /// Callers must not invoke <see cref="IScanStateStore.RecordObservation"/> separately.
    /// </para>
    /// </summary>
    public MeasurementRecord Create(MeasurementInputs inputs)
    {
        var options = _options.CurrentValue;
        var entity = inputs.Entity;
        var status = inputs.Resolution.Status;
        var oldest = inputs.Resolution.OldestActive;

        // Captured before the observation is recorded, because recording overwrites it.
        var previousAge = _state.GetPreviousAgeSeconds(entity.EntityPath);

        double? ageSeconds = null;
        string? clockAnomalyReason = null;

        if (status == MeasurementStatus.Measured && oldest is not null)
        {
            var assessment = _clockSkew.Assess(oldest.EnqueuedTimeUtc, _clock.GetUtcNow());
            ageSeconds = assessment.AgeSeconds;

            if (assessment.IsAnomalous)
            {
                // Spec §3.3: an implausible age is recorded but never alerted on, and the
                // entity's alert is suppressed for the tick by the status change.
                status = MeasurementStatus.ClockAnomaly;
                clockAnomalyReason = assessment.Reason;
            }
        }
        else if (status == MeasurementStatus.Empty)
        {
            ageSeconds = 0d;
        }

        // Spec §3.6: a measurement taken under a reduced budget is flagged, because its
        // age is a lower bound rather than a fact.
        if (status == MeasurementStatus.Measured && inputs.Degradation > DegradationLevel.L0)
        {
            status = MeasurementStatus.Degraded;
        }

        _state.RecordObservation(
            entity.EntityPath,
            status,
            oldest?.SequenceNumber,
            ageSeconds,
            inputs.Runtime.ActiveMessageCount);

        var stalledTicks = _state.GetStalledTickCount(entity.EntityPath);
        var consumptionStalled = IsConsumptionStalled(inputs, status, stalledTicks, options);

        double? ageDelta = ageSeconds is { } current && previousAge is { } previous
            ? current - previous
            : null;

        double? ratioSev2 = null;
        double? ratioSev1 = null;

        // Spec §8.1: ratios exist only where an age was genuinely established. A zero
        // ratio reads as perfectly healthy and would mask a blind monitor, so absence is
        // the only correct representation.
        if (MeasurementRecord.StatusCarriesRatio(status) && ageSeconds is { } age)
        {
            ratioSev2 = inputs.Thresholds.Sev2Seconds > 0 ? age / inputs.Thresholds.Sev2Seconds : null;
            ratioSev1 = inputs.Thresholds.Sev1Seconds > 0 ? age / inputs.Thresholds.Sev1Seconds : null;
        }

        var context = clockAnomalyReason is not null
            ? Combine(inputs.MeasurementContext, clockAnomalyReason)
            : inputs.MeasurementContext;

        return new MeasurementRecord(
            MeasurementId: BuildMeasurementId(_settings.SourceRegion, inputs.ScheduledTickUtc, entity.EntityPath),
            SchemaVersion: MeasurementRecord.CurrentSchemaVersion,
            TimeGenerated: inputs.ScheduledTickUtc,
            TickId: inputs.TickId,
            SourceRegion: _settings.SourceRegion,
            Environment: _settings.Environment,
            NamespaceName: _settings.NamespaceName,
            Entity: entity,
            Status: status,
            Accuracy: entity.Accuracy,
            AgeSeconds: ageSeconds,
            AgeDeltaSeconds: ageDelta,
            AgeBreachRatioSev1: ratioSev1,
            AgeBreachRatioSev2: ratioSev2,
            Thresholds: inputs.Thresholds,
            Runtime: inputs.Runtime,
            OldestSequenceNumber: oldest?.SequenceNumber,
            OldestDeliveryCount: oldest?.DeliveryCount,
            OldestMessageIdHash: oldest?.MessageIdHash,
            OldestCorrelationIdHash: oldest?.CorrelationIdHash,
            StalledTickCount: stalledTicks,
            ConsumptionStalled: consumptionStalled,
            Degradation: inputs.Degradation,
            MeasurementContext: context,
            ScanBatchesUsed: inputs.Resolution.BatchesUsed,
            ScanMessagesExamined: inputs.Resolution.MessagesExamined);
    }

    /// <summary>
    /// Spec §3.7. An observation, not a prediction — which is why it can carry Sev2.
    /// All three guards are mandatory: an empty queue trivially has an unchanging head,
    /// a deferred head is expected to be unchanging, and a stall inferred across an
    /// Indeterminate gap is guessed rather than observed.
    /// </summary>
    private static bool IsConsumptionStalled(
        MeasurementInputs inputs,
        MeasurementStatus status,
        int stalledTicks,
        MonitorOptions options)
    {
        if (status is not (MeasurementStatus.Measured or MeasurementStatus.Degraded))
        {
            return false;
        }

        if (inputs.Runtime.ActiveMessageCount <= 0)
        {
            return false;
        }

        if (inputs.Resolution.Status == MeasurementStatus.HeadBlockedByDeferred)
        {
            return false;
        }

        return stalledTicks >= options.StalledTicksForAlert;
    }

    /// <summary>
    /// Spec FR-051. Derived from the <em>scheduled</em> occurrence, never execution time:
    /// a retried invocation must produce the same identifier or the duplicate it exists
    /// to detect becomes undetectable.
    /// </summary>
    public static string BuildMeasurementId(string sourceRegion, DateTimeOffset scheduledTickUtc, string entityPath)
    {
        var canonical = string.Create(
            CultureInfo.InvariantCulture,
            $"{sourceRegion}|{scheduledTickUtc.UtcDateTime:yyyy-MM-ddTHH:mm:ssZ}|{entityPath}");

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }

    private static string? Combine(string? left, string? right) =>
        (left, right) switch
        {
            (null, null) => null,
            (null, _) => right,
            (_, null) => left,
            _ => $"{left}; {right}"
        };
}
