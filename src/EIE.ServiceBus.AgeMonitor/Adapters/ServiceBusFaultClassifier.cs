using System.Security.Cryptography;
using System.Text;
using Azure;
using Azure.Messaging.ServiceBus;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Configuration;

namespace EIE.ServiceBus.AgeMonitor.Adapters;

/// <summary>
/// Maps Azure SDK exception shapes onto the small set of outcomes the orchestrator
/// reasons about, so the tick's failure handling stays testable with plain exceptions.
/// </summary>
public sealed class ServiceBusFaultClassifier : IEntityFaultClassifier
{
    public EntityFaultKind Classify(Exception exception) => exception switch
    {
        EntityNotFoundInSnapshotException => EntityFaultKind.NotFound,

        ServiceBusException { Reason: ServiceBusFailureReason.MessagingEntityNotFound } =>
            EntityFaultKind.NotFound,

        // Spec §3.5: a throttle invalidates every zero this tick produced, so it must be
        // distinguishable from an ordinary transient fault.
        ServiceBusException { Reason: ServiceBusFailureReason.ServiceBusy } =>
            EntityFaultKind.Throttled,

        ServiceBusException { Reason: ServiceBusFailureReason.QuotaExceeded } =>
            EntityFaultKind.Throttled,

        UnauthorizedAccessException => EntityFaultKind.Unauthorized,

        RequestFailedException { Status: 429 } => EntityFaultKind.Throttled,
        RequestFailedException { Status: 404 } => EntityFaultKind.NotFound,
        RequestFailedException { Status: 401 or 403 } => EntityFaultKind.Unauthorized,

        AggregateException aggregate when aggregate.InnerException is not null =>
            Classify(aggregate.InnerException),

        _ => EntityFaultKind.Transient
    };
}

/// <summary>
/// Spec §8.2 / NFR-018. At a utility, <c>MessageId</c> and <c>CorrelationId</c> are
/// frequently populated with business keys (account number, service point, work
/// order), which must not land in Log Analytics under a retention policy chosen for
/// operational telemetry. The hash still correlates the same message across ticks,
/// which is all the runbook needs; <c>SequenceNumber</c> is what locates it.
/// </summary>
public sealed class Sha256MessageIdHasher : IMessageIdHasher
{
    private readonly MonitorSettings _settings;
    private readonly bool _emitRaw;

    public Sha256MessageIdHasher(MonitorSettings settings, bool emitRawIdentifiers)
    {
        _settings = settings;
        _emitRaw = emitRawIdentifiers;
    }

    public string? Hash(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        if (_emitRaw)
        {
            return value;
        }

        var salted = Encoding.UTF8.GetBytes(_settings.HashSalt + value);
        return Convert.ToHexStringLower(SHA256.HashData(salted));
    }
}
