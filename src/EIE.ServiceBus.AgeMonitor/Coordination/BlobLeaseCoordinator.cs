using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;

namespace EIE.ServiceBus.AgeMonitor.Coordination;

/// <summary>
/// Spec §4.7 (FR-060..066). Active/standby coordination via a blob lease.
/// <para>
/// A blob lease is used rather than a timestamp comparison because its expiry is
/// evaluated server-side: an ETag-plus-timestamp scheme would require both regions to
/// agree on wall-clock time, and clock skew is already a risk this system detects
/// rather than trusts (§3.3).
/// </para>
/// <para>
/// This type never swallows store failures into a "not holder" result. Failures
/// propagate so the caller can fail open (FR-063) — every failure of the coordination
/// store must result in more monitoring, never less.
/// </para>
/// </summary>
public sealed class BlobLeaseCoordinator : ILeaseCoordinator
{
    private readonly BlobContainerClient _container;
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly MonitorSettings _settings;
    private readonly TimeProvider _clock;
    private readonly ILogger<BlobLeaseCoordinator> _log;

    private string? _leaseId;
    private bool _initialised;

    public BlobLeaseCoordinator(
        BlobContainerClient container,
        IOptionsMonitor<MonitorOptions> options,
        MonitorSettings settings,
        TimeProvider clock,
        ILogger<BlobLeaseCoordinator> log)
    {
        _container = container;
        _options = options;
        _settings = settings;
        _clock = clock;
        _log = log;
    }

    public bool IsHolder => _leaseId is not null;

    public DateTimeOffset? AcquiredAtUtc { get; private set; }

    public async Task<bool> TryAcquireOrRenewAsync(CancellationToken ct)
    {
        var blob = await EnsureBlobAsync(ct).ConfigureAwait(false);
        var lease = blob.GetBlobLeaseClient(_leaseId);

        if (_leaseId is not null)
        {
            try
            {
                await lease.RenewAsync(cancellationToken: ct).ConfigureAwait(false);
                return true;
            }
            catch (RequestFailedException ex) when (IsLeaseLost(ex))
            {
                _log.LogWarning("Lease lost during renew ({Status}/{ErrorCode}); attempting reacquisition", ex.Status, ex.ErrorCode);
                _leaseId = null;
                AcquiredAtUtc = null;
                lease = blob.GetBlobLeaseClient();
            }
        }

        try
        {
            var duration = TimeSpan.FromSeconds(_options.CurrentValue.LeaseDurationSeconds);
            var response = await lease.AcquireAsync(duration, cancellationToken: ct).ConfigureAwait(false);

            _leaseId = response.Value.LeaseId;
            AcquiredAtUtc = _clock.GetUtcNow();

            _log.LogInformation("{Region} acquired the measurement lease", _settings.SourceRegion);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            // Held by the other region. This is the one legitimate "not holder" outcome.
            return false;
        }
    }

    /// <summary>
    /// Spec FR-064. Releasing on shutdown lets the standby promote within about one tick
    /// instead of waiting out the full lease duration, so a planned deploy costs far less
    /// blind time than an unplanned failure.
    /// </summary>
    public async Task ReleaseAsync(CancellationToken ct)
    {
        if (_leaseId is null)
        {
            return;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(_options.CurrentValue.LeaseReleaseTimeoutSeconds));

        try
        {
            var blob = _container.GetBlobClient(_settings.LeaseBlobName);
            await blob.GetBlobLeaseClient(_leaseId)
                .ReleaseAsync(cancellationToken: timeout.Token)
                .ConfigureAwait(false);

            _log.LogInformation("{Region} released the measurement lease", _settings.SourceRegion);
        }
        catch (Exception ex)
        {
            // A hung release must never block shutdown; the lease will expire on its own.
            _log.LogWarning(ex, "Lease release failed; the lease will expire naturally");
        }
        finally
        {
            _leaseId = null;
            AcquiredAtUtc = null;
        }
    }

    private async Task<BlobClient> EnsureBlobAsync(CancellationToken ct)
    {
        var blob = _container.GetBlobClient(_settings.LeaseBlobName);

        if (_initialised)
        {
            return blob;
        }

        await _container.CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);

        try
        {
            using var empty = new MemoryStream([]);
            await blob.UploadAsync(empty, overwrite: false, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            // Already created, possibly by the other region, possibly leased right now.
        }

        _initialised = true;
        return blob;
    }

    private static bool IsLeaseLost(RequestFailedException ex) =>
        ex.Status is 409 or 412 ||
        string.Equals(ex.ErrorCode, BlobErrorCode.LeaseIdMismatchWithLeaseOperation.ToString(), StringComparison.Ordinal) ||
        string.Equals(ex.ErrorCode, BlobErrorCode.LeaseNotPresentWithLeaseOperation.ToString(), StringComparison.Ordinal);
}
