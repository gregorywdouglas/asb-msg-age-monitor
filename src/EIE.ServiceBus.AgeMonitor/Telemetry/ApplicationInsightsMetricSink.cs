using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Telemetry;

/// <summary>
/// Spec §8.1 (TEL-001..008).
/// <para>
/// Two host settings decide whether this design functions at all and both default to
/// the wrong value: adaptive sampling must be disabled for metric telemetry (FR-048),
/// because sampled-away points produce missing alert evaluations indistinguishable
/// from healthy silence; and custom metric dimensions must be enabled (FR-049), or
/// the entity dimension is stripped and the metric collapses to a namespace-wide
/// average that would essentially never breach.
/// </para>
/// </summary>
public sealed class ApplicationInsightsMetricSink : IMetricSink
{
    private readonly TelemetryClient _client;
    private readonly MonitorSettings _settings;

    public ApplicationInsightsMetricSink(TelemetryClient client, MonitorSettings settings)
    {
        _client = client;
        _settings = settings;
    }

    public Task EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        CancellationToken ct)
    {
        foreach (var record in records)
        {
            var dimensions = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["EntityPath"] = record.Entity.EntityPath,
                ["SourceRegion"] = record.SourceRegion
            };

            // TEL-001. Only where an age was genuinely established.
            if (MeasurementRecord.StatusCarriesAge(record.Status) && record.AgeSeconds is { } age)
            {
                Track("OldestMessageAgeSeconds", age, dimensions);
            }

            // TEL-002/003. Spec §8.1 ratio rule: no point at all rather than a zero.
            if (MeasurementRecord.StatusCarriesRatio(record.Status))
            {
                if (record.AgeBreachRatioSev2 is { } sev2)
                {
                    Track("AgeBreachRatioSev2", sev2, dimensions);
                }

                if (record.AgeBreachRatioSev1 is { } sev1)
                {
                    Track("AgeBreachRatioSev1", sev1, dimensions);
                }
            }

            // TEL-004..006. Always emitted, including for idle entities, because absence
            // of a data point must never be readable as health.
            Track("ActiveMessageCount", record.Runtime.ActiveMessageCount, dimensions);
            Track("DeadLetterMessageCount", record.Runtime.DeadLetterMessageCount, dimensions);
            Track("TransferDeadLetterMessageCount", record.Runtime.TransferDeadLetterMessageCount, dimensions);

            // TEL-007.
            if (record.AgeDeltaSeconds is { } delta)
            {
                Track("AgeDeltaSeconds", delta, dimensions);
            }
        }

        // TEL-008. Unconditional: this is what ALR-030 keys on.
        Track("MonitorHeartbeat", 1d, new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["SourceRegion"] = heartbeat.SourceRegion,
            ["IsLeaseHolder"] = heartbeat.IsLeaseHolder ? "true" : "false"
        });

        _client.Flush();
        return Task.CompletedTask;
    }

    public Task WitnessSinkFailureAsync(string sinkName, string error, CancellationToken ct)
    {
        var telemetry = new TraceTelemetry(
            $"Telemetry sink {sinkName} failed: {error}",
            SeverityLevel.Error);

        telemetry.Properties["Sink"] = sinkName;
        telemetry.Properties["SourceRegion"] = _settings.SourceRegion;
        telemetry.Properties["Environment"] = _settings.Environment;
        telemetry.Properties["EventType"] = MonitorEventType.EmitFailure;

        _client.TrackTrace(telemetry);
        _client.Flush();

        return Task.CompletedTask;
    }

    private void Track(string name, double value, IDictionary<string, string> dimensions)
    {
        var telemetry = new MetricTelemetry(name, value)
        {
            MetricNamespace = _settings.MetricNamespace
        };

        foreach (var (key, dimensionValue) in dimensions)
        {
            telemetry.Properties[key] = dimensionValue;
        }

        _client.TrackMetric(telemetry);
    }
}
