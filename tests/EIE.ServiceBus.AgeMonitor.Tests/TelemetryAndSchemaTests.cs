using System.Reflection;
using System.Text.Json;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using EIE.ServiceBus.AgeMonitor.Adapters;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Telemetry;
using EIE.ServiceBus.AgeMonitor.Tests.Support;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>Spec §11.5 — telemetry and schema, TST-060..065.</summary>
public sealed class TelemetryAndSchemaTests
{
    /// <summary>Spec §8.2. The DCR stream declaration must match this set exactly.</summary>
    private static readonly string[] DeclaredMeasurementColumns =
    [
        "TimeGenerated", "MeasurementId", "SchemaVersion", "TickId", "SourceRegion", "Environment",
        "NamespaceName", "EntityPath", "EntityKind", "EntityRole", "ForwardTarget", "CriticalityClass",
        "MeasurementStatus", "AgeAccuracy", "AgeSeconds", "AgeDeltaSeconds", "AgeBreachRatioSev1",
        "AgeBreachRatioSev2", "Sev1ThresholdSeconds", "Sev2ThresholdSeconds", "ThresholdSource",
        "ActiveMessageCount", "DeadLetterMessageCount", "ScheduledMessageCount",
        "TransferDeadLetterMessageCount", "OldestSequenceNumber", "OldestDeliveryCount",
        "OldestMessageIdHash", "OldestCorrelationIdHash", "StalledTickCount", "ConsumptionStalled",
        "DegradationLevel", "MeasurementContext", "IsPartitioned", "ScanBatchesUsed", "ScanMessagesExamined"
    ];

    /// <summary>Spec §8.3.</summary>
    private static readonly string[] DeclaredHealthColumns =
    [
        "TimeGenerated", "TickId", "SourceRegion", "Environment", "EventType", "Severity",
        "IsLeaseHolder", "LeaseHolder", "DiscoveredEntities", "MeasuredEntities",
        "IndeterminateEntities", "SkippedEntities", "BlockedEntities", "TickDurationMs",
        "DegradationLevel", "ConfigSource", "AppInsightsEmitOk", "LogsIngestionEmitOk",
        "EntityPath", "Detail"
    ];

    [Fact(DisplayName = "TST-060a the emitted measurement row matches the declared DCR schema exactly")]
    public void Tst060a_MeasurementRowMatchesDeclaredSchema()
    {
        // A field the function sends that the DCR does not declare is dropped silently,
        // with no error at either end. This guards the drift locally; TST-060 proves it
        // end to end against a real DCE at deployment time.
        AssertColumnsMatch("LogsIngestionSink+MeasurementRow", DeclaredMeasurementColumns);
    }

    [Fact(DisplayName = "TST-060b the emitted health row matches the declared DCR schema exactly")]
    public void Tst060b_HealthRowMatchesDeclaredSchema() =>
        AssertColumnsMatch("LogsIngestionSink+HealthRow", DeclaredHealthColumns);

    [Fact(DisplayName = "TST-062 identifiers are hashed by default and the raw value never appears")]
    public void Tst062_IdentifiersHashedByDefault()
    {
        var hasher = new Sha256MessageIdHasher(Build.Settings(), emitRawIdentifiers: false);

        var hashed = hasher.Hash("ACCT-4471902");

        Assert.NotNull(hashed);
        Assert.DoesNotContain("ACCT", hashed, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("4471902", hashed, StringComparison.Ordinal);
        Assert.Equal(64, hashed!.Length);
    }

    [Fact(DisplayName = "TST-062b the raw-identifier flag is honoured for non-production use")]
    public void Tst062b_RawIdentifiersWhenEnabled()
    {
        var hasher = new Sha256MessageIdHasher(Build.Settings(), emitRawIdentifiers: true);

        Assert.Equal("ACCT-4471902", hasher.Hash("ACCT-4471902"));
    }

    [Fact(DisplayName = "TST-063 the same identifier hashes identically across ticks so correlation still works")]
    public void Tst063_HashIsStableAcrossTicks()
    {
        var hasher = new Sha256MessageIdHasher(Build.Settings(), emitRawIdentifiers: false);

        Assert.Equal(hasher.Hash("msg-1"), hasher.Hash("msg-1"));
        Assert.NotEqual(hasher.Hash("msg-1"), hasher.Hash("msg-2"));
    }

    [Fact(DisplayName = "TST-063b a different salt produces a different hash for the same identifier")]
    public void Tst063b_SaltSeparatesEnvironments()
    {
        var prod = new Sha256MessageIdHasher(new EIE.ServiceBus.AgeMonitor.Configuration.MonitorSettings { HashSalt = "prod" }, false);
        var dev = new Sha256MessageIdHasher(new EIE.ServiceBus.AgeMonitor.Configuration.MonitorSettings { HashSalt = "dev" }, false);

        Assert.NotEqual(prod.Hash("msg-1"), dev.Hash("msg-1"));
    }

    [Fact(DisplayName = "TST-064 every metric point carries the EntityPath dimension")]
    public async Task Tst064_MetricDimensionsArePresent()
    {
        // Without CustomMetricsOptedInType the dimension is stripped and the metric
        // collapses to a namespace-wide average that would essentially never breach. This
        // asserts the sink's half of the contract; the host setting is asserted by the
        // Bicep test.
        var channel = new CapturingChannel();
        using var configuration = new TelemetryConfiguration
        {
            TelemetryChannel = channel,
            ConnectionString = "InstrumentationKey=00000000-0000-0000-0000-000000000000"
        };

        var sink = new ApplicationInsightsMetricSink(new TelemetryClient(configuration), Build.Settings());
        var record = MeasuredRecord();

        await sink.EmitAsync([record], Heartbeat(), default);

        var metrics = channel.Sent.OfType<MetricTelemetry>().ToList();
        var ageMetric = metrics.Single(m => m.Name == "OldestMessageAgeSeconds");

        Assert.Equal("q-test", ageMetric.Properties["EntityPath"]);
        Assert.Equal("eastus2", ageMetric.Properties["SourceRegion"]);
        Assert.Equal("EIE.ServiceBus", ageMetric.MetricNamespace);
        Assert.Contains(metrics, m => m.Name == "MonitorHeartbeat");
    }

    [Fact(DisplayName = "TST-064b a non-measurement emits no ratio point rather than a zero")]
    public async Task Tst064b_NoRatioPointForNonMeasurement()
    {
        var channel = new CapturingChannel();
        using var configuration = new TelemetryConfiguration
        {
            TelemetryChannel = channel,
            ConnectionString = "InstrumentationKey=00000000-0000-0000-0000-000000000000"
        };

        var sink = new ApplicationInsightsMetricSink(new TelemetryClient(configuration), Build.Settings());
        var record = MeasuredRecord() with
        {
            Status = MeasurementStatus.Indeterminate,
            AgeSeconds = null,
            AgeBreachRatioSev1 = null,
            AgeBreachRatioSev2 = null
        };

        await sink.EmitAsync([record], Heartbeat(), default);

        var metrics = channel.Sent.OfType<MetricTelemetry>().ToList();

        Assert.DoesNotContain(metrics, m => m.Name.StartsWith("AgeBreachRatio", StringComparison.Ordinal));
        Assert.DoesNotContain(metrics, m => m.Name == "OldestMessageAgeSeconds");

        // Counts are still emitted: absence of a data point must never be readable as health.
        Assert.Contains(metrics, m => m.Name == "ActiveMessageCount");
    }

    [Fact(DisplayName = "TST-065 host.json disables Application Insights sampling")]
    public void Tst065_SamplingIsDisabled()
    {
        // A sampled-away metric point produces a missing alert evaluation that is
        // indistinguishable from healthy silence.
        var hostJson = JsonDocument.Parse(File.ReadAllText(RepoFile("src/EIE.ServiceBus.AgeMonitor/host.json")));

        var enabled = hostJson.RootElement
            .GetProperty("logging")
            .GetProperty("applicationInsights")
            .GetProperty("samplingSettings")
            .GetProperty("isEnabled")
            .GetBoolean();

        Assert.False(enabled);
    }

    // ---- helpers --------------------------------------------------------------

    private static void AssertColumnsMatch(string typeName, string[] declared)
    {
        var type = typeof(LogsIngestionSink).Assembly
            .GetTypes()
            .Single(t => t.FullName!.EndsWith(typeName, StringComparison.Ordinal));

        var actual = type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .Where(n => n != "EqualityContract")
            .ToHashSet(StringComparer.Ordinal);

        var expected = declared.ToHashSet(StringComparer.Ordinal);

        Assert.Empty(actual.Except(expected));
        Assert.Empty(expected.Except(actual));
    }

    private static MeasurementRecord MeasuredRecord() => new(
        "measurement-id", MeasurementRecord.CurrentSchemaVersion, Build.T0, "tick",
        "eastus2", "test", "sb-eie-test", Build.Queue(),
        MeasurementStatus.Measured, AgeAccuracy.Exact,
        450d, 30d, 0.5d, 1.5d,
        new ThresholdSet(300, 900, ThresholdSource.AppConfig),
        new EntityRuntimeInfo(5, 1, 0, 0, Build.T0),
        99L, 3, "hash", null, 0, false, DegradationLevel.L0, null, 1, 1);

    private static MonitorHeartbeat Heartbeat() =>
        new("tick", Build.T0, "eastus2", "test", true, 1, 1, 0, 0, 0, 12,
            DegradationLevel.L0, ThresholdSource.AppConfig, true, true);

    internal static string RepoFile(string relativePath)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "asb-msg-age-monitor.slnx")) &&
               !Directory.Exists(Path.Combine(directory.FullName, ".git")))
        {
            directory = directory.Parent;
        }

        Assert.NotNull(directory);
        return Path.Combine(directory!.FullName, relativePath);
    }

    private sealed class CapturingChannel : ITelemetryChannel
    {
        public List<ITelemetry> Sent { get; } = [];

        public bool? DeveloperMode { get; set; }

        public string? EndpointAddress { get; set; }

        public void Send(ITelemetry item) => Sent.Add(item);

        public void Flush()
        {
        }

        public void Dispose()
        {
        }
    }
}
