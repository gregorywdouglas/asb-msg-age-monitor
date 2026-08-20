using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using EIE.ServiceBus.AgeMonitor.Abstractions;

namespace EIE.ServiceBus.AgeMonitor.Hosting;

/// <summary>
/// Spec FR-064 / §10.5. Releasing the lease on graceful shutdown lets the standby
/// promote within roughly one tick instead of waiting out the full lease duration,
/// so a routine deployment produces a shorter blind window than an unplanned failure
/// — which is the opposite of the default behaviour.
/// </summary>
public sealed class LeaseReleaseOnShutdown : IHostedService
{
    private readonly ILeaseCoordinator _lease;
    private readonly ILogger<LeaseReleaseOnShutdown> _log;

    public LeaseReleaseOnShutdown(ILeaseCoordinator lease, ILogger<LeaseReleaseOnShutdown> log)
    {
        _lease = lease;
        _log = log;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_lease.IsHolder)
        {
            return;
        }

        _log.LogInformation("Shutting down while holding the measurement lease; releasing for fast standby promotion");
        await _lease.ReleaseAsync(cancellationToken).ConfigureAwait(false);
    }
}
