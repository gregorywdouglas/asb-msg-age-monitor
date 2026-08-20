namespace EIE.ServiceBus.AgeMonitor.Tests.Support;

/// <summary>
/// A test that requires external infrastructure. It is reported as <em>skipped</em>
/// (never as passed) when the environment is absent, so a green local run never implies
/// that emulator or live-namespace coverage actually executed.
/// </summary>
public sealed class RequiresEnvironmentFactAttribute : FactAttribute
{
    public RequiresEnvironmentFactAttribute(params string[] variables)
    {
        var missing = variables
            .Where(v => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(v)))
            .ToList();

        if (missing.Count > 0)
        {
            Skip = $"Requires external infrastructure. Set {string.Join(", ", missing)} to run.";
        }
    }
}

public static class TestEnvironment
{
    /// <summary>Connection string for the containerised Service Bus emulator.</summary>
    public const string EmulatorConnectionString = "ASBMON_EMULATOR_CONNECTION_STRING";

    /// <summary>Fully qualified namespace of a real non-production Premium namespace.</summary>
    public const string ContractNamespace = "ASBMON_CONTRACT_NAMESPACE";

    /// <summary>A queue in the contract namespace reserved for these tests.</summary>
    public const string ContractQueue = "ASBMON_CONTRACT_QUEUE";

    public static string Require(string name) =>
        Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"{name} is not set");
}
