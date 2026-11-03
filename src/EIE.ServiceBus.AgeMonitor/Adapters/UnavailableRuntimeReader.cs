using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Adapters;

/// <summary>
/// The runtime reader when entities come from the App Configuration catalog.
/// <para>
/// SPEC-DEVIATION-008: counts have no source here. ARM is unreachable, and
/// <c>ServiceBusAdministrationClient</c> needs Data Owner, which §10.3 rules out. Every
/// count is reported as unknown rather than zero, so no consumer can mistake it for
/// an empty queue (NFR-001).
/// </para>
/// </summary>
public sealed class UnavailableRuntimeReader : IEntityRuntimeReader
{
    private readonly TimeProvider _clock;

    public UnavailableRuntimeReader(TimeProvider clock) => _clock = clock;

    public Task<EntityRuntimeInfo> GetRuntimeInfoAsync(string entityPath, CancellationToken ct) =>
        Task.FromResult(EntityRuntimeInfo.Unknown(_clock.GetUtcNow()));
}
