using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.State;

/// <summary>
/// Spec §4.1 (FR-001..008). Dynamic discovery with a short TTL, because in an
/// integration platform a brand-new queue is statistically the most likely thing to
/// break — it was just deployed.
/// </summary>
public sealed class EntityDiscoveryService : IEntityDiscoveryService
{
    private readonly IEntityCatalog _catalog;
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly IScanStateStore _scanState;
    private readonly TimeProvider _clock;
    private readonly Random _jitter;
    private readonly ILogger<EntityDiscoveryService> _log;

    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly ConcurrentDictionary<string, DateTimeOffset> _tombstones =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, byte> _pendingNotFound =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<(string EventType, string Severity, string EntityPath, string Detail)> _events = new();
    private readonly ConcurrentDictionary<string, byte> _reportedOnce = new(StringComparer.Ordinal);

    private IReadOnlyList<EntityDescriptor> _cache = [];
    private DateTimeOffset _cacheExpiresAt = DateTimeOffset.MinValue;

    public EntityDiscoveryService(
        IEntityCatalog catalog,
        IOptionsMonitor<MonitorOptions> options,
        IScanStateStore scanState,
        TimeProvider clock,
        ILogger<EntityDiscoveryService> log,
        Random? jitter = null)
    {
        _catalog = catalog;
        _options = options;
        _scanState = scanState;
        _clock = clock;
        _log = log;
        _jitter = jitter ?? Random.Shared;
    }

    public async Task<IReadOnlyList<EntityDescriptor>> GetEntitiesAsync(CancellationToken ct)
    {
        if (_clock.GetUtcNow() < _cacheExpiresAt)
        {
            return _cache;
        }

        await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_clock.GetUtcNow() < _cacheExpiresAt)
            {
                return _cache;
            }

            return await RefreshAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    /// <summary>Spec FR-004.</summary>
    public void EvictOnNotFound(string entityPath)
    {
        if (!_pendingNotFound.TryAdd(entityPath, 0))
        {
            return;
        }

        _cache = _cache.Where(e => !string.Equals(e.EntityPath, entityPath, StringComparison.OrdinalIgnoreCase)).ToList();

        _events.Enqueue((
            MonitorEventType.EntityDisappeared,
            "Sev4",
            entityPath,
            "Entity not found during peek; alerts suppressed pending discovery confirmation"));

        _log.LogWarning("Entity {EntityPath} not found; awaiting discovery confirmation", entityPath);
    }

    public bool IsTombstoned(string entityPath) => _tombstones.ContainsKey(entityPath);

    public IReadOnlyList<(string EventType, string Severity, string EntityPath, string Detail)> DrainEvents()
    {
        var drained = new List<(string, string, string, string)>();
        while (_events.TryDequeue(out var item))
        {
            drained.Add(item);
        }

        return drained;
    }

    private async Task<IReadOnlyList<EntityDescriptor>> RefreshAsync(CancellationToken ct)
    {
        var options = _options.CurrentValue;
        var now = _clock.GetUtcNow();

        IReadOnlyList<EntityDescriptor> discovered;
        try
        {
            discovered = await _catalog.ListEntitiesAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Spec principle 5: a dependency failure must never reduce monitoring. Serve
            // the stale cache and try again on the next tick rather than losing coverage.
            _log.LogError(ex, "Entity discovery failed; retaining cache of {Count} entities", _cache.Count);
            _cacheExpiresAt = now.AddSeconds(Math.Min(60, options.DiscoveryCacheTtlSeconds));
            return _cache;
        }

        var present = new HashSet<string>(discovered.Select(e => e.EntityPath), StringComparer.OrdinalIgnoreCase);

        ConfirmDeletions(present, now);
        ExpireTombstones(present, now, options);

        var live = discovered
            .Where(e => !_tombstones.ContainsKey(e.EntityPath))
            .Select(e => Classify(e, options))
            .ToList();

        AssertEntityConfiguration(live);
        DetectForwardingCycles(live);

        _cache = live;

        // Spec FR-002: jitter so the two regional deployments do not synchronise their
        // management-plane calls.
        var jitterFactor = 0.9 + (_jitter.NextDouble() * 0.2);
        _cacheExpiresAt = now.AddSeconds(options.DiscoveryCacheTtlSeconds * jitterFactor);

        return _cache;
    }

    /// <summary>
    /// Spec FR-005. Deletion cannot be distinguished from a transient fault on a single
    /// observation, so it is confirmed in two phases.
    /// </summary>
    private void ConfirmDeletions(HashSet<string> present, DateTimeOffset now)
    {
        foreach (var path in _pendingNotFound.Keys.ToList())
        {
            _pendingNotFound.TryRemove(path, out _);

            if (present.Contains(path))
            {
                _events.Enqueue((
                    MonitorEventType.EntityRecovered,
                    "Sev4",
                    path,
                    "Entity present at discovery refresh; the earlier not-found was transient"));
                continue;
            }

            _tombstones[path] = now;
            _scanState.Forget(path);

            _log.LogInformation("Entity {EntityPath} tombstoned after discovery confirmed its absence", path);
        }
    }

    /// <summary>
    /// Spec FR-006. Tombstones expire so a queue recreated under the same name — a real
    /// scenario when entities are redeployed by Bicep — is not ignored forever.
    /// </summary>
    private void ExpireTombstones(HashSet<string> present, DateTimeOffset now, MonitorOptions options)
    {
        foreach (var (path, tombstonedAt) in _tombstones.ToArray())
        {
            var expired = now - tombstonedAt >= TimeSpan.FromHours(options.TombstoneTtlHours);

            if (expired && present.Contains(path))
            {
                _tombstones.TryRemove(path, out _);
                _events.Enqueue((
                    MonitorEventType.EntityRecovered,
                    "Sev4",
                    path,
                    "Tombstone expired and the entity exists again; resuming measurement"));
            }
            else if (expired && !present.Contains(path))
            {
                _tombstones.TryRemove(path, out _);
            }
        }
    }

    /// <summary>Spec FR-037: an unclassified entity is itself a signal.</summary>
    private EntityDescriptor Classify(EntityDescriptor entity, MonitorOptions options)
    {
        if (options.EntityClasses.TryGetValue(entity.EntityPath, out var explicitClass))
        {
            return entity with { Class = explicitClass, IsClassExplicit = true };
        }

        ReportOnce(
            MonitorEventType.EntityUnclassified,
            "Sev4",
            entity.EntityPath,
            "No criticality class configured; defaulting to standard");

        return entity with { Class = CriticalityClass.Standard, IsClassExplicit = false };
    }

    /// <summary>
    /// Spec FR-071. Asserted independently of Azure Policy so a policy exemption cannot
    /// silently produce best-effort measurements presented as exact.
    /// </summary>
    private void AssertEntityConfiguration(IReadOnlyList<EntityDescriptor> entities)
    {
        foreach (var entity in entities.Where(e => e.IsPartitioned))
        {
            ReportOnce(
                MonitorEventType.UnsupportedEntityConfiguration,
                "Sev3",
                entity.EntityPath,
                "Entity is partitioned; SequenceNumber ordering is per-partition so age is a lower bound only (AgeAccuracy=BestEffort)");
        }
    }

    /// <summary>Spec FR-074.</summary>
    private void DetectForwardingCycles(IReadOnlyList<EntityDescriptor> entities)
    {
        var targets = entities
            .Where(e => !string.IsNullOrWhiteSpace(e.ForwardTarget))
            .ToDictionary(e => e.EntityPath, e => e.ForwardTarget!, StringComparer.OrdinalIgnoreCase);

        foreach (var start in targets.Keys)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { start };
            var current = start;

            while (targets.TryGetValue(current, out var next))
            {
                if (!seen.Add(next))
                {
                    ReportOnce(
                        MonitorEventType.ForwardingCycleDetected,
                        "Sev3",
                        start,
                        $"Auto-forward chain from {start} forms a cycle at {next}");
                    break;
                }

                current = next;
            }
        }
    }

    private void ReportOnce(string eventType, string severity, string entityPath, string detail)
    {
        if (_reportedOnce.TryAdd($"{eventType}|{entityPath}", 0))
        {
            _events.Enqueue((eventType, severity, entityPath, detail));
        }
    }
}
