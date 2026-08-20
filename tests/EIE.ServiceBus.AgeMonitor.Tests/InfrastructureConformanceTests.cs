using System.Reflection;
using System.Text.Json;
using EIE.ServiceBus.AgeMonitor.Telemetry;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>
/// Spec §11.6 (TST-072) plus the local half of TST-060.
/// <para>
/// The Logs Ingestion path drops any field the DCR does not declare, silently, with no
/// error at either end — so code, table schema and DCR stream can drift apart and the
/// first symptom is a column that has been null for weeks. These tests bind the three
/// together at build time; TST-060's canary round-trip proves it end to end at deploy
/// time against a real endpoint.
/// </para>
/// </summary>
public sealed class InfrastructureConformanceTests
{
    [Fact(DisplayName = "TST-060c the shared column schema matches the emitted measurement row exactly")]
    public void Tst060c_MeasurementSchemaMatchesCode()
    {
        var declared = LoadColumnNames("infra/schema/measurement-columns.json");
        var emitted = RowProperties("LogsIngestionSink+MeasurementRow");

        AssertSetsEqual(declared, emitted, "measurement");
    }

    [Fact(DisplayName = "TST-060d the shared column schema matches the emitted health row exactly")]
    public void Tst060d_HealthSchemaMatchesCode()
    {
        var declared = LoadColumnNames("infra/schema/health-columns.json");
        var emitted = RowProperties("LogsIngestionSink+HealthRow");

        AssertSetsEqual(declared, emitted, "health");
    }

    [Fact(DisplayName = "TST-060e the DCR and the custom tables are driven from the same schema file")]
    public void Tst060e_DcrAndTablesShareOneSource()
    {
        // If either the table schema or the stream declaration is ever hand-written
        // rather than loaded, they can drift independently and nothing will report it.
        var bicep = File.ReadAllText(RepoFile("infra/modules/telemetry.bicep"));

        Assert.Contains("loadJsonContent('../schema/measurement-columns.json')", bicep, StringComparison.Ordinal);
        Assert.Contains("loadJsonContent('../schema/health-columns.json')", bicep, StringComparison.Ordinal);

        // Both consumers must reference the loaded variables, not literal column lists.
        Assert.Contains("columns: measurementColumns", bicep, StringComparison.Ordinal);
        Assert.Contains("columns: healthColumns", bicep, StringComparison.Ordinal);
    }

    [Fact(DisplayName = "TST-072 the identity is granted exactly the roles the spec allows, and never Data Owner")]
    public void Tst072_RoleAssignmentsAreLeastPrivilege()
    {
        var main = File.ReadAllText(RepoFile("infra/main.bicep"));

        // Spec §10.3. Azure Service Bus Data Owner would let the monitor manage entities
        // as well as consume them, and is what ServiceBusAdministrationClient would have
        // required — avoiding it is the reason discovery goes through the ARM plane.
        const string dataOwnerRoleId = "090c5cfd-751d-490a-894a-3ce6f1109419";
        const string dataSenderRoleId = "69a216fc-b8fb-44d8-bc22-1f3c2cd27a39";

        Assert.DoesNotContain(dataOwnerRoleId, main, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(dataSenderRoleId, main, StringComparison.OrdinalIgnoreCase);

        // The exact set the spec permits.
        var expected = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["reader"] = "acdd72a7-3385-48ef-bd42-f606fba81ae7",
            ["serviceBusDataReceiver"] = "4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0",
            ["storageBlobDataContributor"] = "ba92f5b4-2d11-453d-a403-e96b0029c9fe",
            ["monitoringMetricsPublisher"] = "3913510d-42f4-4e42-8a64-420c390055eb",
            ["appConfigurationDataReader"] = "516239f1-63e1-4d78-a4de-a74fb236a071",
            ["keyVaultSecretsUser"] = "4633458b-17de-408a-b874-0445c86b69e6"
        };

        foreach (var (name, id) in expected)
        {
            Assert.Contains($"{name}: '{id}'", main, StringComparison.Ordinal);
        }
    }

    [Fact(DisplayName = "TST-072b the identity is system-assigned, so no secret exists to leak")]
    public void Tst072b_SystemAssignedIdentity()
    {
        var main = File.ReadAllText(RepoFile("infra/main.bicep"));

        Assert.Contains("type: 'SystemAssigned'", main, StringComparison.Ordinal);
        Assert.DoesNotContain("UserAssigned", main, StringComparison.Ordinal);
    }

    [Fact(DisplayName = "FR-049 the host enables custom metric dimensions")]
    public void Fr049_CustomMetricDimensionsEnabled()
    {
        // A one-line setting that decides whether the entire alerting design functions.
        // Without it the entity dimension is stripped and the metric collapses to a
        // namespace-wide average that would essentially never breach — while the
        // telemetry still arrives and still looks plausible.
        //
        // This is a regression guard, not a proof of correctness: it pins the template
        // against the documented Azure property name so the line cannot be dropped or
        // renamed unnoticed. It cannot tell us the platform still honours that name.
        var main = File.ReadAllText(RepoFile("infra/main.bicep"));

        Assert.Contains("CustomMetricsOptedInType: 'WithDimensions'", main, StringComparison.Ordinal);

        // The setting lives on the Application Insights component. A function app setting
        // of a similar name is not recognised by anything and would fail silently.
        Assert.DoesNotContain("APPLICATIONINSIGHTS_ENABLE_CUSTOM_METRIC", main, StringComparison.Ordinal);
    }

    [Fact(DisplayName = "ALR-032 the no-measurement-rows rule is configured to fire on no data")]
    public void Alr032_FiresOnNoData()
    {
        // Azure log alert rules do not fire on zero rows by default: the query returning
        // nothing is treated as "condition not met", silencing the alert precisely when
        // the data is gone.
        var alerts = File.ReadAllText(RepoFile("infra/modules/alerts.bicep"));

        var index = alerts.IndexOf("ALR-032", StringComparison.Ordinal);
        Assert.True(index > 0, "ALR-032 must be declared");

        var block = alerts[index..Math.Min(alerts.Length, index + 900)];
        Assert.Contains("failOnNoData: true", block, StringComparison.Ordinal);
    }

    [Fact(DisplayName = "FR-070 an Azure Policy denies partitioned entities at create time")]
    public void Fr070_PartitioningDeniedByPolicy()
    {
        var policy = File.ReadAllText(RepoFile("infra/policy/deny-partitioned-entities.bicep"));

        Assert.Contains("effect: 'deny'", policy, StringComparison.Ordinal);
        Assert.Contains("enablePartitioning", policy, StringComparison.Ordinal);
    }

    [Fact(DisplayName = "NFR-015 retention is tiered so SLA questions stay answerable retroactively")]
    public void Nfr015_RetentionIsTiered()
    {
        var telemetry = File.ReadAllText(RepoFile("infra/modules/telemetry.bicep"));

        Assert.Contains("interactiveRetentionDays int = 90", telemetry, StringComparison.Ordinal);
        Assert.Contains("totalRetentionDays int = 730", telemetry, StringComparison.Ordinal);
    }

    // ---- helpers --------------------------------------------------------------

    private static List<string> LoadColumnNames(string relativePath)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(RepoFile(relativePath)));

        return document.RootElement
            .EnumerateArray()
            .Select(e => e.GetProperty("name").GetString()!)
            .ToList();
    }

    private static HashSet<string> RowProperties(string typeName)
    {
        var type = typeof(LogsIngestionSink).Assembly
            .GetTypes()
            .Single(t => t.FullName!.EndsWith(typeName, StringComparison.Ordinal));

        return type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .Where(n => n != "EqualityContract")
            .ToHashSet(StringComparer.Ordinal);
    }

    private static void AssertSetsEqual(List<string> declared, HashSet<string> emitted, string label)
    {
        var declaredSet = declared.ToHashSet(StringComparer.Ordinal);

        var missingFromSchema = emitted.Except(declaredSet).ToList();
        var missingFromCode = declaredSet.Except(emitted).ToList();

        Assert.True(
            missingFromSchema.Count == 0,
            $"{label}: the function emits fields the DCR does not declare, which are dropped silently: {string.Join(", ", missingFromSchema)}");

        Assert.True(
            missingFromCode.Count == 0,
            $"{label}: the DCR declares columns the function never populates: {string.Join(", ", missingFromCode)}");

        Assert.Equal(declared.Count, declaredSet.Count);
    }

    private static string RepoFile(string relativePath) =>
        TelemetryAndSchemaTests.RepoFile(relativePath);
}
