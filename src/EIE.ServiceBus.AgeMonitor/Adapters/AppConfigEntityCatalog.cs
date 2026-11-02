using System.Globalization;
using System.Xml;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Adapters;

/// <summary>
/// Entity enumeration from the catalog the deployment pipeline publishes to App
/// Configuration (<c>infra/pipeline/Publish-EntityCatalog.ps1</c>).
/// <para>
/// SPEC-DEVIATION-008: the ARM management plane is not reachable from the hosting
/// network. The pipeline that creates entities is the one principal that can reach it,
/// so it publishes what it created and this adapter reads it back. The function needs
/// no ARM access and no Reader role.
/// </para>
/// <para>
/// Reads <see cref="IConfiguration"/>, which the App Configuration provider keeps
/// current on its background refresh, so a discovery refresh costs no network call.
/// </para>
/// </summary>
public sealed class AppConfigEntityCatalog : IEntityCatalog
{
    /// <summary>
    /// Loaded under a label equal to the namespace name, because a shared store may hold
    /// catalogs for several namespaces. The publish script writes the same label.
    /// </summary>
    public const string CatalogKeyFilter = "asbmon:catalog:*";

    public const string EntitiesSection = "asbmon:catalog:entities";
    public const string PublishedAtKey = "asbmon:catalog:publishedAtUtc";

    private const int DefaultMaxDeliveryCount = 10;

    private readonly IConfiguration _configuration;
    private readonly ILogger<AppConfigEntityCatalog> _log;

    public AppConfigEntityCatalog(IConfiguration configuration, ILogger<AppConfigEntityCatalog> log)
    {
        _configuration = configuration;
        _log = log;
    }

    public Task<IReadOnlyList<EntityDescriptor>> ListEntitiesAsync(CancellationToken ct)
    {
        var entries = _configuration.GetSection(EntitiesSection).GetChildren().ToList();

        // Spec principle 5. An empty catalog is far more likely to be an unpublished or
        // unreadable one than a namespace with no entities. Throwing makes discovery keep
        // its previous cache instead of dropping every entity from measurement.
        if (entries.Count == 0)
        {
            throw new InvalidOperationException(
                $"No entities under '{EntitiesSection}'. The deployment pipeline has not published the catalog, " +
                "or App Configuration is unreachable.");
        }

        var descriptors = entries.Select(Parse).ToList();

        _log.LogDebug(
            "Catalog read: {Count} entities, published at {PublishedAt}",
            descriptors.Count,
            _configuration[PublishedAtKey] ?? "(unknown)");

        return Task.FromResult<IReadOnlyList<EntityDescriptor>>(descriptors);
    }

    /// <summary>
    /// A malformed field never drops the entity, because an entity missing from the
    /// catalog is an entity nobody measures. Each field falls back to the value that
    /// makes the measurement claim less, not more.
    /// </summary>
    private EntityDescriptor Parse(IConfigurationSection entry)
    {
        var path = entry.Key;
        var forwardTo = entry["forwardTo"];

        return new EntityDescriptor(
            path,
            ParseKind(path, entry["kind"]),
            string.IsNullOrWhiteSpace(forwardTo) ? EntityRole.Terminal : EntityRole.Forwarder,
            string.IsNullOrWhiteSpace(forwardTo) ? null : forwardTo,
            ParsePartitioned(path, entry["enablePartitioning"]),
            ParseMaxDeliveryCount(path, entry["maxDeliveryCount"]),
            ParseTimeToLive(path, entry["defaultMessageTimeToLive"]));
    }

    private EntityKind ParseKind(string path, string? raw)
    {
        if (Enum.TryParse<EntityKind>(raw, ignoreCase: true, out var kind))
        {
            return kind;
        }

        var inferred = path.Contains("/Subscriptions/", StringComparison.OrdinalIgnoreCase)
            ? EntityKind.Subscription
            : EntityKind.Queue;

        _log.LogError("Catalog entry {EntityPath}: kind '{Raw}' is invalid; inferred {Kind} from the path", path, raw, inferred);
        return inferred;
    }

    /// <summary>
    /// Falls back to <c>true</c>: a partitioned entity reports its age as a lower bound
    /// (FR-072), whereas wrongly assuming <c>false</c> presents a lower bound as exact.
    /// </summary>
    private bool ParsePartitioned(string path, string? raw)
    {
        if (bool.TryParse(raw, out var partitioned))
        {
            return partitioned;
        }

        _log.LogError("Catalog entry {EntityPath}: enablePartitioning '{Raw}' is invalid; assuming partitioned (BestEffort)", path, raw);
        return true;
    }

    private int ParseMaxDeliveryCount(string path, string? raw)
    {
        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var count) && count > 0)
        {
            return count;
        }

        _log.LogError("Catalog entry {EntityPath}: maxDeliveryCount '{Raw}' is invalid; using {Default}", path, raw, DefaultMaxDeliveryCount);
        return DefaultMaxDeliveryCount;
    }

    /// <summary>Accepts the invariant <c>TimeSpan</c> form or ISO 8601, which is what <c>az</c> emits.</summary>
    private TimeSpan ParseTimeToLive(string path, string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return TimeSpan.Zero;
        }

        if (TimeSpan.TryParse(raw, CultureInfo.InvariantCulture, out var ttl))
        {
            return ttl;
        }

        try
        {
            return XmlConvert.ToTimeSpan(raw);
        }
        catch (FormatException)
        {
            _log.LogError("Catalog entry {EntityPath}: defaultMessageTimeToLive '{Raw}' is invalid; treating as no TTL", path, raw);
            return TimeSpan.Zero;
        }
    }
}
