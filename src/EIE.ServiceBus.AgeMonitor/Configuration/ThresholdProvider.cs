using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Domain;

namespace EIE.ServiceBus.AgeMonitor.Configuration;

/// <summary>
/// Spec FR-030..038. Resolution order is entity override, then role or class default,
/// then the compiled fallback. Every resolved pair is validated; an invalid value is
/// rejected and the next level down is used, so a fat-fingered threshold cannot
/// silence an entity indefinitely (FR-033).
/// </summary>
public sealed class ThresholdProvider : IThresholdProvider
{
    private readonly IOptionsMonitor<MonitorOptions> _options;
    private readonly IConfigurationHealth _health;
    private readonly ILogger<ThresholdProvider> _log;
    private readonly ConcurrentQueue<string> _rejections = new();
    private readonly ConcurrentDictionary<string, byte> _reportedRejections = new(StringComparer.Ordinal);

    public ThresholdProvider(
        IOptionsMonitor<MonitorOptions> options,
        IConfigurationHealth health,
        ILogger<ThresholdProvider> log)
    {
        _options = options;
        _health = health;
        _log = log;
    }

    /// <summary>
    /// Spec FR-035. Provenance of the snapshot in force, which is what ALR-015 keys on:
    /// a monitor that has been running on compiled defaults for six hours because the
    /// sentinel poll is silently failing is otherwise invisible.
    /// </summary>
    public ThresholdSource CurrentSource
    {
        get
        {
            if (_health.IsAvailable)
            {
                return ThresholdSource.AppConfig;
            }

            return _health.HasEverLoaded ? ThresholdSource.LastKnownGood : ThresholdSource.CompiledDefault;
        }
    }

    public ThresholdSet Resolve(EntityDescriptor entity)
    {
        var options = _options.CurrentValue;
        var source = CurrentSource;

        if (options.EntityThresholds.TryGetValue(entity.EntityPath, out var entityOverride) &&
            Accept(entityOverride, $"threshold:entity:{entity.EntityPath}"))
        {
            return new ThresholdSet(entityOverride.Sev2Seconds, entityOverride.Sev1Seconds, source);
        }

        // Spec FR-038: a healthy forwarder holds messages for milliseconds, so any
        // sustained age on one means forwarding has stopped. An explicit entity
        // override still wins over the role default.
        if (entity.Role == EntityRole.Forwarder &&
            Accept(options.ForwarderThresholds, "threshold:forwarder"))
        {
            return new ThresholdSet(
                options.ForwarderThresholds.Sev2Seconds,
                options.ForwarderThresholds.Sev1Seconds,
                source);
        }

        if (options.ClassThresholds.TryGetValue(entity.Class, out var classPair) &&
            Accept(classPair, $"threshold:class:{entity.Class}"))
        {
            return new ThresholdSet(classPair.Sev2Seconds, classPair.Sev1Seconds, source);
        }

        var defaults = new ThresholdPair(options.DefaultSev2Seconds, options.DefaultSev1Seconds);
        if (Accept(defaults, "threshold:default"))
        {
            return new ThresholdSet(defaults.Sev2Seconds, defaults.Sev1Seconds, source);
        }

        return new ThresholdSet(
            MonitorOptions.CompiledDefaultSev2Seconds,
            MonitorOptions.CompiledDefaultSev1Seconds,
            ThresholdSource.CompiledDefault);
    }

    public IReadOnlyList<string> DrainRejections()
    {
        var drained = new List<string>();
        while (_rejections.TryDequeue(out var rejection))
        {
            drained.Add(rejection);
        }

        return drained;
    }

    private bool Accept(ThresholdPair pair, string key)
    {
        if (ConfigurationClamps.IsValidThresholdPair(pair.Sev2Seconds, pair.Sev1Seconds, out var reason))
        {
            return true;
        }

        var message = $"{key} rejected: {reason}";

        // Report each distinct bad value once rather than on every tick for every entity,
        // otherwise a single bad key produces N rejections per minute forever.
        if (_reportedRejections.TryAdd(message, 0))
        {
            _rejections.Enqueue(message);
            _log.LogError("Configuration rejected, retaining last-known-good: {Message}", message);
        }

        return false;
    }
}
