using System.Collections.Concurrent;
using Azure.ResourceManager;
using Azure.ResourceManager.ServiceBus;
using Microsoft.Extensions.Logging;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Adapters;

/// <summary>
/// Entity enumeration and runtime counts from the ARM management plane.
/// <para>
/// Spec §10.3: this deliberately avoids <c>ServiceBusAdministrationClient</c>, which
/// authenticates against the Service Bus data plane and effectively requires Data
/// Owner. ARM entity reads are covered by plain <b>Reader</b> at namespace scope, a
/// material privilege reduction for the half of the workload that never touches
/// message content.
/// </para>
/// <para>
/// The list call returns <c>countDetails</c> alongside entity properties, so one
/// enumeration serves both discovery and per-tick runtime counts. Per-entity calls
/// would be roughly 80 ARM reads per tick and would approach subscription read limits.
/// </para>
/// </summary>
public sealed class ArmServiceBusCatalog : IEntityCatalog, IEntityRuntimeReader
{
    private readonly ArmClient _arm;
    private readonly MonitorSettings _settings;
    private readonly TimeProvider _clock;
    private readonly ILogger<ArmServiceBusCatalog> _log;

    private readonly SemaphoreSlim _snapshotGate = new(1, 1);
    private ConcurrentDictionary<string, EntityRuntimeInfo> _runtime = new(StringComparer.OrdinalIgnoreCase);
    private IReadOnlyList<EntityDescriptor> _descriptors = [];
    private DateTimeOffset _snapshotTakenAt = DateTimeOffset.MinValue;

    /// <summary>
    /// Runtime counts are refreshed at most once per tick. A shorter window would mean
    /// several ARM enumerations for a single tick's worth of entities.
    /// </summary>
    private static readonly TimeSpan SnapshotFreshness = TimeSpan.FromSeconds(30);

    public ArmServiceBusCatalog(
        ArmClient arm,
        MonitorSettings settings,
        TimeProvider clock,
        ILogger<ArmServiceBusCatalog> log)
    {
        _arm = arm;
        _settings = settings;
        _clock = clock;
        _log = log;
    }

    public async Task<IReadOnlyList<EntityDescriptor>> ListEntitiesAsync(CancellationToken ct)
    {
        await RefreshSnapshotAsync(force: true, ct).ConfigureAwait(false);
        return _descriptors;
    }

    public async Task<EntityRuntimeInfo> GetRuntimeInfoAsync(string entityPath, CancellationToken ct)
    {
        await RefreshSnapshotAsync(force: false, ct).ConfigureAwait(false);

        if (_runtime.TryGetValue(entityPath, out var info))
        {
            return info;
        }

        // Spec FR-004: absence from the ARM snapshot is how a deleted entity surfaces on
        // this path. Surfacing it as an exception lets the orchestrator run the same
        // eviction flow as a data-plane not-found.
        throw new EntityNotFoundInSnapshotException(entityPath);
    }

    private async Task RefreshSnapshotAsync(bool force, CancellationToken ct)
    {
        if (!force && _clock.GetUtcNow() - _snapshotTakenAt < SnapshotFreshness)
        {
            return;
        }

        await _snapshotGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!force && _clock.GetUtcNow() - _snapshotTakenAt < SnapshotFreshness)
            {
                return;
            }

            var descriptors = new List<EntityDescriptor>();
            var runtime = new ConcurrentDictionary<string, EntityRuntimeInfo>(StringComparer.OrdinalIgnoreCase);
            var now = _clock.GetUtcNow();

            var namespaceId = ServiceBusNamespaceResource.CreateResourceIdentifier(
                _settings.SubscriptionId,
                _settings.ResourceGroup,
                _settings.NamespaceName);

            var serviceBusNamespace = _arm.GetServiceBusNamespaceResource(namespaceId);

            await foreach (var queue in serviceBusNamespace.GetServiceBusQueues().GetAllAsync(cancellationToken: ct).ConfigureAwait(false))
            {
                var data = queue.Data;
                var path = data.Name;

                descriptors.Add(new EntityDescriptor(
                    path,
                    EntityKind.Queue,
                    string.IsNullOrWhiteSpace(data.ForwardTo) ? EntityRole.Terminal : EntityRole.Forwarder,
                    string.IsNullOrWhiteSpace(data.ForwardTo) ? null : data.ForwardTo,
                    data.EnablePartitioning ?? false,
                    data.MaxDeliveryCount ?? 10,
                    data.DefaultMessageTimeToLive ?? TimeSpan.Zero));

                runtime[path] = new EntityRuntimeInfo(
                    data.CountDetails?.ActiveMessageCount ?? 0,
                    data.CountDetails?.DeadLetterMessageCount ?? 0,
                    data.CountDetails?.ScheduledMessageCount ?? 0,
                    data.CountDetails?.TransferDeadLetterMessageCount ?? 0,
                    now);
            }

            await foreach (var topic in serviceBusNamespace.GetServiceBusTopics().GetAllAsync(cancellationToken: ct).ConfigureAwait(false))
            {
                var topicPartitioned = topic.Data.EnablePartitioning ?? false;

                await foreach (var subscription in topic.GetServiceBusSubscriptions().GetAllAsync(cancellationToken: ct).ConfigureAwait(false))
                {
                    var data = subscription.Data;
                    var path = $"{topic.Data.Name}/Subscriptions/{data.Name}";

                    descriptors.Add(new EntityDescriptor(
                        path,
                        EntityKind.Subscription,
                        string.IsNullOrWhiteSpace(data.ForwardTo) ? EntityRole.Terminal : EntityRole.Forwarder,
                        string.IsNullOrWhiteSpace(data.ForwardTo) ? null : data.ForwardTo,
                        topicPartitioned,
                        data.MaxDeliveryCount ?? 10,
                        data.DefaultMessageTimeToLive ?? TimeSpan.Zero));

                    runtime[path] = new EntityRuntimeInfo(
                        data.CountDetails?.ActiveMessageCount ?? 0,
                        data.CountDetails?.DeadLetterMessageCount ?? 0,
                        data.CountDetails?.ScheduledMessageCount ?? 0,
                        data.CountDetails?.TransferDeadLetterMessageCount ?? 0,
                        now);
                }
            }

            _descriptors = descriptors;
            _runtime = runtime;
            _snapshotTakenAt = now;

            _log.LogDebug("ARM snapshot refreshed: {Count} entities", descriptors.Count);
        }
        finally
        {
            _snapshotGate.Release();
        }
    }
}

public sealed class EntityNotFoundInSnapshotException : Exception
{
    public EntityNotFoundInSnapshotException(string entityPath)
        : base($"Entity '{entityPath}' was not present in the most recent ARM snapshot") =>
        EntityPath = entityPath;

    public string EntityPath { get; }
}
