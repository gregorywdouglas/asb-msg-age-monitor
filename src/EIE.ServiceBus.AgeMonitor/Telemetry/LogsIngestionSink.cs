using Azure.Monitor.Ingestion;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Telemetry;

/// <summary>
/// Spec §8.2/§8.3. Column names must match the DCR stream declaration exactly — a
/// field the function sends that the DCR does not declare is dropped silently, with
/// no error at either end. TST-060's canary round-trip exists to catch that at
/// deployment time rather than at query time weeks later.
/// </summary>
public sealed class LogsIngestionSink : ILogsSink
{
    private readonly LogsIngestionClient _client;
    private readonly MonitorSettings _settings;

    public LogsIngestionSink(LogsIngestionClient client, MonitorSettings settings)
    {
        _client = client;
        _settings = settings;
    }

    public async Task EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        IReadOnlyList<MonitorHealthEvent> events,
        CancellationToken ct)
    {
        if (records.Count > 0)
        {
            var rows = records.Select(MeasurementRow.From).ToList();
            await _client
                .UploadAsync(_settings.DcrImmutableId, _settings.MeasurementStream, rows, cancellationToken: ct)
                .ConfigureAwait(false);
        }

        var healthRows = new List<HealthRow> { HealthRow.FromHeartbeat(heartbeat) };
        healthRows.AddRange(events.Select(e => HealthRow.FromEvent(e, heartbeat)));

        await _client
            .UploadAsync(_settings.DcrImmutableId, _settings.HealthStream, healthRows, cancellationToken: ct)
            .ConfigureAwait(false);
    }

    /// <summary>Spec §8.2. One property per declared DCR column.</summary>
    internal sealed record MeasurementRow
    {
        public required DateTimeOffset TimeGenerated { get; init; }
        public required string MeasurementId { get; init; }
        public required int SchemaVersion { get; init; }
        public required string TickId { get; init; }
        public required string SourceRegion { get; init; }
        public required string Environment { get; init; }
        public required string NamespaceName { get; init; }
        public required string EntityPath { get; init; }
        public required string EntityKind { get; init; }
        public required string EntityRole { get; init; }
        public string? ForwardTarget { get; init; }
        public required string CriticalityClass { get; init; }
        public required string MeasurementStatus { get; init; }
        public required string AgeAccuracy { get; init; }
        public double? AgeSeconds { get; init; }
        public double? AgeDeltaSeconds { get; init; }
        public double? AgeBreachRatioSev1 { get; init; }
        public double? AgeBreachRatioSev2 { get; init; }
        public required int Sev1ThresholdSeconds { get; init; }
        public required int Sev2ThresholdSeconds { get; init; }
        public required string ThresholdSource { get; init; }
        public required long ActiveMessageCount { get; init; }
        public required long DeadLetterMessageCount { get; init; }
        public required long ScheduledMessageCount { get; init; }
        public required long TransferDeadLetterMessageCount { get; init; }
        public long? OldestSequenceNumber { get; init; }
        public int? OldestDeliveryCount { get; init; }
        public string? OldestMessageIdHash { get; init; }
        public string? OldestCorrelationIdHash { get; init; }
        public required int StalledTickCount { get; init; }
        public required bool ConsumptionStalled { get; init; }
        public required int DegradationLevel { get; init; }
        public string? MeasurementContext { get; init; }
        public required bool IsPartitioned { get; init; }
        public required int ScanBatchesUsed { get; init; }
        public required int ScanMessagesExamined { get; init; }

        public static MeasurementRow From(MeasurementRecord r) => new()
        {
            TimeGenerated = r.TimeGenerated,
            MeasurementId = r.MeasurementId,
            SchemaVersion = r.SchemaVersion,
            TickId = r.TickId,
            SourceRegion = r.SourceRegion,
            Environment = r.Environment,
            NamespaceName = r.NamespaceName,
            EntityPath = r.Entity.EntityPath,
            EntityKind = r.Entity.Kind.ToString(),
            EntityRole = r.Entity.Role.ToString(),
            ForwardTarget = r.Entity.ForwardTarget,
            CriticalityClass = r.Entity.Class.ToString(),
            MeasurementStatus = r.Status.ToString(),
            AgeAccuracy = r.Accuracy.ToString(),
            AgeSeconds = r.AgeSeconds,
            AgeDeltaSeconds = r.AgeDeltaSeconds,
            AgeBreachRatioSev1 = r.AgeBreachRatioSev1,
            AgeBreachRatioSev2 = r.AgeBreachRatioSev2,
            Sev1ThresholdSeconds = r.Thresholds.Sev1Seconds,
            Sev2ThresholdSeconds = r.Thresholds.Sev2Seconds,
            ThresholdSource = r.Thresholds.Source.ToString(),
            ActiveMessageCount = r.Runtime.ActiveMessageCount,
            DeadLetterMessageCount = r.Runtime.DeadLetterMessageCount,
            ScheduledMessageCount = r.Runtime.ScheduledMessageCount,
            TransferDeadLetterMessageCount = r.Runtime.TransferDeadLetterMessageCount,
            OldestSequenceNumber = r.OldestSequenceNumber,
            OldestDeliveryCount = r.OldestDeliveryCount,
            OldestMessageIdHash = r.OldestMessageIdHash,
            OldestCorrelationIdHash = r.OldestCorrelationIdHash,
            StalledTickCount = r.StalledTickCount,
            ConsumptionStalled = r.ConsumptionStalled,
            DegradationLevel = (int)r.Degradation,
            MeasurementContext = r.MeasurementContext,
            IsPartitioned = r.Entity.IsPartitioned,
            ScanBatchesUsed = r.ScanBatchesUsed,
            ScanMessagesExamined = r.ScanMessagesExamined
        };
    }

    /// <summary>Spec §8.3.</summary>
    internal sealed record HealthRow
    {
        public required DateTimeOffset TimeGenerated { get; init; }
        public required string TickId { get; init; }
        public required string SourceRegion { get; init; }
        public required string Environment { get; init; }
        public required string EventType { get; init; }
        public required string Severity { get; init; }
        public required bool IsLeaseHolder { get; init; }
        public string? LeaseHolder { get; init; }
        public required int DiscoveredEntities { get; init; }
        public required int MeasuredEntities { get; init; }
        public required int IndeterminateEntities { get; init; }
        public required int SkippedEntities { get; init; }
        public required int BlockedEntities { get; init; }
        public required long TickDurationMs { get; init; }
        public required int DegradationLevel { get; init; }
        public required string ConfigSource { get; init; }
        public required bool AppInsightsEmitOk { get; init; }
        public required bool LogsIngestionEmitOk { get; init; }
        public string? EntityPath { get; init; }
        public string? Detail { get; init; }

        public static HealthRow FromHeartbeat(MonitorHeartbeat h) => new()
        {
            TimeGenerated = h.TimeGenerated,
            TickId = h.TickId,
            SourceRegion = h.SourceRegion,
            Environment = h.Environment,
            EventType = MonitorEventType.Heartbeat,
            Severity = "Sev4",
            IsLeaseHolder = h.IsLeaseHolder,
            LeaseHolder = h.IsLeaseHolder ? h.SourceRegion : null,
            DiscoveredEntities = h.DiscoveredEntities,
            MeasuredEntities = h.MeasuredEntities,
            IndeterminateEntities = h.IndeterminateEntities,
            SkippedEntities = h.SkippedEntities,
            BlockedEntities = h.BlockedEntities,
            TickDurationMs = h.TickDurationMs,
            DegradationLevel = (int)h.Degradation,
            ConfigSource = h.ConfigSource.ToString(),
            AppInsightsEmitOk = h.AppInsightsEmitOk,
            LogsIngestionEmitOk = h.LogsIngestionEmitOk
        };

        public static HealthRow FromEvent(MonitorHealthEvent e, MonitorHeartbeat h) =>
            FromHeartbeat(h) with
            {
                TimeGenerated = e.TimeGenerated,
                EventType = e.EventType,
                Severity = e.Severity,
                EntityPath = e.EntityPath,
                Detail = e.Detail
            };
    }
}
