using Microsoft.Extensions.Configuration.AzureAppConfiguration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using EIE.ServiceBus.AgeMonitor.Configuration;

namespace EIE.ServiceBus.AgeMonitor.Hosting;

/// <summary>
/// Spec FR-032. Drives the App Configuration sentinel refresh on a background cadence,
/// never from the tick path.
/// <para>
/// <c>ConfigureRefresh</c> only declares what to watch; without a caller of
/// <c>TryRefreshAsync</c> the provider never reloads, and a catalog the pipeline
/// publishes would not reach a running instance until it restarted.
/// </para>
/// </summary>
public sealed class AppConfigurationRefreshService : BackgroundService
{
    private readonly IConfigurationRefresherProvider _refreshers;
    private readonly MonitorSettings _settings;
    private readonly TimeProvider _clock;
    private readonly ILogger<AppConfigurationRefreshService> _log;

    public AppConfigurationRefreshService(
        IConfigurationRefresherProvider refreshers,
        MonitorSettings settings,
        TimeProvider clock,
        ILogger<AppConfigurationRefreshService> log)
    {
        _refreshers = refreshers;
        _settings = settings;
        _clock = clock;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_settings.AppConfigRefreshIntervalSeconds), _clock);

        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            foreach (var refresher in _refreshers.Refreshers)
            {
                // TryRefreshAsync logs and swallows its own failures; a config-plane outage
                // leaves the last loaded values in force (FR-034).
                if (!await refresher.TryRefreshAsync(stoppingToken).ConfigureAwait(false))
                {
                    _log.LogWarning("App Configuration refresh did not complete; previous values remain in force");
                }
            }
        }
    }
}
