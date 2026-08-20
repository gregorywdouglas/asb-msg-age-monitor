using System.Globalization;
using Microsoft.Extensions.Configuration;

namespace EIE.ServiceBus.AgeMonitor.Configuration;

/// <summary>Spec §7.1 (CFG-001..019). Deployment-time settings; a change requires a redeploy.</summary>
public static class MonitorSettingsBinder
{
    public static void Bind(IConfiguration configuration, MonitorSettings settings)
    {
        settings.FullyQualifiedNamespace = Read("ServiceBus:FullyQualifiedNamespace", settings.FullyQualifiedNamespace);
        settings.SubscriptionId = Read("ServiceBus:SubscriptionId", settings.SubscriptionId);
        settings.ResourceGroup = Read("ServiceBus:ResourceGroup", settings.ResourceGroup);
        settings.NamespaceName = Read("ServiceBus:NamespaceName", settings.NamespaceName);

        settings.SourceRegion = Read("Monitor:SourceRegion", settings.SourceRegion);
        settings.Environment = Read("Monitor:Environment", settings.Environment);

        settings.AppConfigEndpoint = Read("AppConfig:Endpoint", settings.AppConfigEndpoint);
        settings.AppConfigSentinelKey = Read("AppConfig:SentinelKey", settings.AppConfigSentinelKey);

        settings.LeaseStorageAccountUri = Read("Lease:StorageAccountUri", settings.LeaseStorageAccountUri);
        settings.LeaseContainerName = Read("Lease:ContainerName", settings.LeaseContainerName);
        settings.LeaseBlobName = Read("Lease:BlobName", settings.LeaseBlobName);

        settings.DceEndpoint = Read("Telemetry:DceEndpoint", settings.DceEndpoint);
        settings.DcrImmutableId = Read("Telemetry:DcrImmutableId", settings.DcrImmutableId);
        settings.MeasurementStream = Read("Telemetry:MeasurementStream", settings.MeasurementStream);
        settings.HealthStream = Read("Telemetry:HealthStream", settings.HealthStream);
        settings.MetricNamespace = Read("Telemetry:MetricNamespace", settings.MetricNamespace);

        settings.HashSalt = Read("Hashing:Salt", settings.HashSalt);

        if (int.TryParse(
                configuration["AppConfig:RefreshIntervalSeconds"],
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var refreshInterval) &&
            refreshInterval > 0)
        {
            settings.AppConfigRefreshIntervalSeconds = refreshInterval;
        }

        string Read(string key, string fallback)
        {
            var value = configuration[key];
            return string.IsNullOrWhiteSpace(value) ? fallback : value;
        }
    }
}
