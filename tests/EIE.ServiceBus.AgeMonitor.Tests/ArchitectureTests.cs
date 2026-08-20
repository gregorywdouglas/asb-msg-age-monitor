using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using EIE.ServiceBus.AgeMonitor.Abstractions;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>
/// Spec §11.6 — security and architecture, TST-070..072.
/// <para>
/// The managed identity holds <c>Azure Service Bus Data Receiver</c> because no smaller
/// built-in role permits peek (OPN-003), which means the monitor has standing
/// permission to consume and destroy production messages across a Tier 1 namespace.
/// Until a peek-only custom role is proven expressible, these tests <em>are</em> the
/// control: they convert "we intend to only peek" into something CI enforces, and they
/// survive a careless refactor in a way a code review does not.
/// </para>
/// </summary>
public sealed class ArchitectureTests
{
    /// <summary>Spec FR-050 / TST-070.</summary>
    private static readonly string[] ForbiddenMembers =
    [
        "ReceiveMessagesAsync",
        "ReceiveMessageAsync",
        "ReceiveDeferredMessagesAsync",
        "ReceiveDeferredMessageAsync",
        "CompleteMessageAsync",
        "AbandonMessageAsync",
        "DeadLetterMessageAsync",
        "DeferMessageAsync",
        "RenewMessageLockAsync"
    ];

    [Fact(DisplayName = "TST-070 the compiled assembly references no receive-family Service Bus member")]
    public void Tst070_NoReceiveFamilyReferences()
    {
        var assemblyPath = typeof(IEntityPeeker).Assembly.Location;

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);

        var metadata = peReader.GetMetadataReader();
        var referenced = new HashSet<string>(StringComparer.Ordinal);

        foreach (var handle in metadata.MemberReferences)
        {
            referenced.Add(metadata.GetString(metadata.GetMemberReference(handle).Name));
        }

        var violations = ForbiddenMembers.Where(referenced.Contains).ToList();

        Assert.True(
            violations.Count == 0,
            $"The monitor must never reach a message-consuming operation. Found: {string.Join(", ", violations)}");
    }

    [Fact(DisplayName = "TST-070b the peek member the monitor does depend on is present")]
    public void Tst070b_PeekReferenceIsPresent()
    {
        // Guards the guard: if the scan were ever rewritten to reach Service Bus by some
        // other route, TST-070 would still pass while asserting nothing.
        var assemblyPath = typeof(IEntityPeeker).Assembly.Location;

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);

        var metadata = peReader.GetMetadataReader();
        var referenced = metadata.MemberReferences
            .Select(h => metadata.GetString(metadata.GetMemberReference(h).Name))
            .ToHashSet(StringComparer.Ordinal);

        Assert.Contains("PeekMessagesAsync", referenced);
    }

    [Fact(DisplayName = "TST-071 IEntityPeeker exposes no receive-family method")]
    public void Tst071_PortSurfaceIsPeekOnly()
    {
        var methods = typeof(IEntityPeeker)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Select(m => m.Name)
            .ToList();

        Assert.Equal(["PeekAsync"], methods);
    }

    [Fact(DisplayName = "TST-071b no public port exposes a message-mutating operation")]
    public void Tst071b_NoPortMutatesMessages()
    {
        var portTypes = typeof(IEntityPeeker).Assembly
            .GetTypes()
            .Where(t => t.IsInterface && t.Namespace == typeof(IEntityPeeker).Namespace);

        var offenders = portTypes
            .SelectMany(t => t.GetMethods().Select(m => new { Type = t.Name, m.Name }))
            .Where(m => ForbiddenMembers.Contains(m.Name, StringComparer.Ordinal))
            .ToList();

        Assert.Empty(offenders);
    }
}
