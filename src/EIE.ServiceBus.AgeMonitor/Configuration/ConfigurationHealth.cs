namespace EIE.ServiceBus.AgeMonitor.Configuration;

/// <summary>
/// Tracks whether the App Configuration snapshot in memory is fresh. Spec FR-034 /
/// FR-036: a config-plane blip must degrade the monitor to stale thresholds, never
/// stop it — thresholds change monthly, message ages change every 60 seconds.
/// </summary>
public interface IConfigurationHealth
{
    bool IsAvailable { get; }

    bool HasEverLoaded { get; }

    DateTimeOffset? LastSuccessfulRefreshUtc { get; }

    void ReportSuccess();

    void ReportFailure(string reason);

    string? LastFailureReason { get; }
}

public sealed class ConfigurationHealth : IConfigurationHealth
{
    private readonly TimeProvider _clock;
    private readonly Lock _gate = new();

    private bool _isAvailable;
    private bool _hasEverLoaded;
    private DateTimeOffset? _lastSuccess;
    private string? _lastFailureReason;

    public ConfigurationHealth(TimeProvider clock) => _clock = clock;

    public bool IsAvailable
    {
        get { lock (_gate) { return _isAvailable; } }
    }

    public bool HasEverLoaded
    {
        get { lock (_gate) { return _hasEverLoaded; } }
    }

    public DateTimeOffset? LastSuccessfulRefreshUtc
    {
        get { lock (_gate) { return _lastSuccess; } }
    }

    public string? LastFailureReason
    {
        get { lock (_gate) { return _lastFailureReason; } }
    }

    public void ReportSuccess()
    {
        lock (_gate)
        {
            _isAvailable = true;
            _hasEverLoaded = true;
            _lastSuccess = _clock.GetUtcNow();
            _lastFailureReason = null;
        }
    }

    public void ReportFailure(string reason)
    {
        lock (_gate)
        {
            _isAvailable = false;
            _lastFailureReason = reason;
        }
    }
}
