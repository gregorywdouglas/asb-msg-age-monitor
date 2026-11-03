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
        // required. Reader is absent because discovery reads the published catalog, not
        // ARM (SPEC-DEVIATION-008).
        const string dataOwnerRoleId = "090c5cfd-751d-490a-894a-3ce6f1109419";
        const string dataSenderRoleId = "69a216fc-b8fb-44d8-bc22-1f3c2cd27a39";
        const string readerRoleId = "acdd72a7-3385-48ef-bd42-f606fba81ae7";

        Assert.DoesNotContain(dataOwnerRoleId, main, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(dataSenderRoleId, main, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(readerRoleId, main, StringComparison.OrdinalIgnoreCase);

        // The exact set the spec permits.
        var expected = new Dictionary<string, string>(StringComparer.Ordinal)
        {
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

    [Fact(DisplayName = "SPEC-DEVIATION-008 the catalog is written and read under the namespace label, so a shared store cannot mix namespaces")]
    public void Dev008_CatalogIsLabelledByNamespace()
    {
        // Entity paths are namespace-relative. Unlabelled, two namespaces in one store
        // overwrite each other, and one's strict import deletes the other's catalog.
        var script = File.ReadAllText(RepoFile("infra/pipeline/Publish-EntityCatalog.ps1"));
        var program = File.ReadAllText(RepoFile("src/EIE.ServiceBus.AgeMonitor/Program.cs"));

        Assert.Contains("--label $ServiceBusNamespaceName", script, StringComparison.Ordinal);
        Assert.Contains("--strict", script, StringComparison.Ordinal);
        Assert.Contains(".Select(AppConfigEntityCatalog.CatalogKeyFilter, settings.NamespaceName)", program, StringComparison.Ordinal);
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

    [Theory(DisplayName = "TST-060f the portal-wizard sample and transform match the shared column schema")]
    [InlineData("measurement")]
    [InlineData("health")]
    public void Tst060f_ManualPortalFilesMatchSchema(string kind)
    {
        // infra/manual is the fallback when neither Bicep nor the script can run. It is a
        // hand-kept copy of the schema, so without this it drifts on the next new column.
        using var schema = JsonDocument.Parse(File.ReadAllText(RepoFile($"infra/schema/{kind}-columns.json")));
        var columns = schema.RootElement.EnumerateArray()
            .Select(e => (Name: e.GetProperty("name").GetString()!, Type: e.GetProperty("type").GetString()!))
            .ToList();

        var kql = File.ReadAllText(RepoFile($"infra/manual/{kind}-transform.kql"));
        var projected = System.Text.RegularExpressions.Regex.Matches(kql, @"(\w+) = to(\w+)\(")
            .Select(m => (Name: m.Groups[1].Value, Type: KqlCastToSchemaType(m.Groups[2].Value)))
            .ToList();

        Assert.Equal(columns, projected);

        using var sample = JsonDocument.Parse(File.ReadAllText(RepoFile($"infra/manual/{kind}-sample.json")));
        var row = sample.RootElement.EnumerateArray().Single().EnumerateObject().ToList();

        Assert.Equal(columns.Select(c => c.Name), row.Select(p => p.Name));

        // The wizard infers each column's type from the sample value.
        foreach (var (column, value) in columns.Zip(row.Select(p => p.Value)))
        {
            var inferred = value.ValueKind switch
            {
                JsonValueKind.True or JsonValueKind.False => "boolean",
                JsonValueKind.String => DateTimeOffset.TryParse(value.GetString(), out _) ? "datetime" : "string",
                JsonValueKind.Number when !value.TryGetInt64(out _) => "real",
                JsonValueKind.Number when !value.TryGetInt32(out _) => "long",
                JsonValueKind.Number => "int",
                _ => value.ValueKind.ToString()
            };

            Assert.True(inferred == column.Type, $"{kind}-sample.json: {column.Name} would be inferred as {inferred}, schema says {column.Type}");
        }
    }

    [Fact(DisplayName = "CFG-001 app setting names use '__', because Flex Consumption runs on Linux where ':' is not allowed")]
    public void Cfg001_AppSettingNamesHaveNoColon()
    {
        var main = File.ReadAllText(RepoFile("infra/main.bicep"));

        // 'function:<name>' under alwaysReady is Flex syntax, not an app setting.
        var colonNames = System.Text.RegularExpressions.Regex.Matches(main, @"name: '(?!function:)([A-Za-z]+:[A-Za-z]+)'")
            .Select(m => m.Groups[1].Value)
            .ToList();

        Assert.True(colonNames.Count == 0, $"App settings with ':' are not delivered on Linux: {string.Join(", ", colonNames)}");
    }

    // ---- helpers --------------------------------------------------------------

    private static string KqlCastToSchemaType(string cast) => cast == "bool" ? "boolean" : cast;

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
