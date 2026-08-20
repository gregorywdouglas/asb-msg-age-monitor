using Azure.Core;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Monitor.Ingestion;
using Azure.ResourceManager;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EIE.ServiceBus.AgeMonitor.Abstractions;
using EIE.ServiceBus.AgeMonitor.Adapters;
using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Coordination;
using EIE.ServiceBus.AgeMonitor.Hosting;
using EIE.ServiceBus.AgeMonitor.Measurement;
using EIE.ServiceBus.AgeMonitor.Orchestration;
using EIE.ServiceBus.AgeMonitor.State;
using EIE.ServiceBus.AgeMonitor.Telemetry;

TokenCredential credential = new DefaultAzureCredential();
MonitorSettings settings = new();

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureAppConfiguration(configuration =>
    {
        var built = configuration.Build();
        MonitorSettingsBinder.Bind(built, settings);

        // Spec FR-032: refresh runs on a background cadence and is never awaited inside
        // the tick path. Coupling detection latency to a control-plane call would make
        // the monitor's availability depend on App Configuration's.
        if (!string.IsNullOrWhiteSpace(settings.AppConfigEndpoint))
        {
            configuration.AddAzureAppConfiguration(options =>
            {
                options.Connect(new Uri(settings.AppConfigEndpoint), credential)
                    .Select("asbmon:*")
                    .ConfigureRefresh(refresh => refresh
                        .Register(settings.AppConfigSentinelKey, refreshAll: true)
                        .SetRefreshInterval(TimeSpan.FromSeconds(settings.AppConfigRefreshIntervalSeconds)));
            });
        }
    })
    .ConfigureServices(services =>
    {
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton(settings);
        services.AddSingleton(credential);

        // --- Runtime configuration (spec §7.2) -------------------------------------
        services.AddSingleton<IConfigurationHealth, ConfigurationHealth>();
        services.AddSingleton<IConfigureOptions<MonitorOptions>, MonitorOptionsConfigurator>();
        services.AddOptions<MonitorOptions>();

        // --- Azure clients ----------------------------------------------------------
        services.AddSingleton(_ => new ServiceBusClient(
            settings.FullyQualifiedNamespace,
            credential,
            new ServiceBusClientOptions { TransportType = ServiceBusTransportType.AmqpTcp }));

        services.AddSingleton(_ => new ArmClient(credential));

        services.AddSingleton(_ => new LogsIngestionClient(new Uri(settings.DceEndpoint), credential));

        services.AddSingleton(_ => new BlobContainerClient(
            new Uri($"{settings.LeaseStorageAccountUri.TrimEnd('/')}/{settings.LeaseContainerName}"),
            credential));

        // --- Ports and adapters ------------------------------------------------------
        services.AddSingleton<IMessageIdHasher>(sp => new Sha256MessageIdHasher(
            settings,
            sp.GetRequiredService<IOptionsMonitor<MonitorOptions>>().CurrentValue.EmitRawMessageIdentifiers));

        services.AddSingleton<ServiceBusEntityPeeker>();
        services.AddSingleton<IEntityPeeker>(sp => sp.GetRequiredService<ServiceBusEntityPeeker>());

        services.AddSingleton<ArmServiceBusCatalog>();
        services.AddSingleton<IEntityCatalog>(sp => sp.GetRequiredService<ArmServiceBusCatalog>());
        services.AddSingleton<IEntityRuntimeReader>(sp => sp.GetRequiredService<ArmServiceBusCatalog>());

        services.AddSingleton<IEntityFaultClassifier, ServiceBusFaultClassifier>();
        services.AddSingleton<IScanStateStore, ScanStateStore>();
        services.AddSingleton<IDegradationController, DegradationController>();
        services.AddSingleton<IEntityDiscoveryService, EntityDiscoveryService>();
        services.AddSingleton<IThresholdProvider, ThresholdProvider>();
        services.AddSingleton<IClockSkewDetector, ClockSkewDetector>();
        services.AddSingleton<ZeroResultCorroborator>();
        services.AddSingleton<MeasurementRecordFactory>();
        services.AddSingleton<IOldestActiveMessageResolver, OldestActiveMessageResolver>();
        services.AddSingleton<ILeaseCoordinator, BlobLeaseCoordinator>();

        services.AddSingleton<IMetricSink, ApplicationInsightsMetricSink>();
        services.AddSingleton<ILogsSink, LogsIngestionSink>();
        services.AddSingleton<ITelemetryEmitter, TelemetryEmitter>();
        services.AddSingleton<IMonitorTickOrchestrator, MonitorTickOrchestrator>();

        // Spec FR-064: release the lease on graceful shutdown so the standby promotes
        // within about one tick rather than waiting out the full lease duration.
        services.AddHostedService<LeaseReleaseOnShutdown>();

        // Spec FR-048/FR-049. Sampling is disabled in host.json for metric telemetry —
        // a sampled-away metric point produces a missing alert evaluation that is
        // indistinguishable from healthy silence.
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
    })
    .ConfigureLogging(logging =>
    {
        logging.AddFilter("Azure.Core", LogLevel.Warning);
        logging.AddFilter("Azure.Identity", LogLevel.Warning);
    })
    .Build();

await host.RunAsync();

/// <summary>Exposed so the architecture tests (TST-070..072) can load the assembly.</summary>
public partial class Program;
