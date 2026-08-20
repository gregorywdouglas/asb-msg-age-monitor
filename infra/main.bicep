// Spec §10 — infrastructure for asb-msg-age-monitor.
//
// Deployed once per region. The lease storage account is the one resource that is NOT
// per-region: it lives in the secondary region only (spec §2.2), so that the region
// making the promotion decision shares a failure domain with the store it must read to
// make it.

targetScope = 'resourceGroup'

@description('Deployment region for this instance, e.g. eastus2 | centralus.')
param location string

@description('Region discriminator emitted on every record as SourceRegion.')
@allowed(['eastus2', 'centralus'])
param sourceRegion string

@description('Environment discriminator.')
@allowed(['dev', 'staging', 'prod'])
param environmentName string

@description('Fully qualified Service Bus namespace, geo-replicated so it follows the current primary.')
param serviceBusFullyQualifiedNamespace string

@description('Service Bus namespace name (ARM).')
param serviceBusNamespaceName string

@description('Resource group holding the Service Bus namespace.')
param serviceBusResourceGroup string

@description('Existing Log Analytics workspace name (spec OPN-005: referenced, not created).')
param logAnalyticsWorkspaceName string

@description('Resource group holding the Log Analytics workspace.')
param logAnalyticsResourceGroup string

@description('Existing App Configuration store name.')
param appConfigName string

@description('Resource group holding the App Configuration store.')
param appConfigResourceGroup string

@description('Existing Key Vault holding the identifier hash salt.')
param keyVaultName string

@description('Resource group holding the Key Vault.')
param keyVaultResourceGroup string

@description('Blob endpoint of the ZRS lease storage account, which lives in the standby region.')
param leaseStorageAccountUri string

@description('Resource id of the paging action group.')
param pagingActionGroupId string

@description('Resource id of the ticket/chat action group.')
param ticketActionGroupId string

@description('Spec §9.6 — production alone pages; non-production is capped at Sev3.')
param enablePaging bool = environmentName == 'prod'

@description('Spec ASM-010 — dev runs a relaxed cadence to reduce cost; staging matches production.')
param monitorSchedule string = environmentName == 'dev' ? '0 */5 * * * *' : '0 */1 * * * *'

@description('Alert rules are deployed once, alongside the primary region only.')
param deployAlerts bool = sourceRegion == 'eastus2'

var namePrefix = 'asbmon-${environmentName}-${sourceRegion}'

// Spec §10.3. Built-in role definition ids.
var roles = {
  // Reader at namespace scope covers ARM entity enumeration. Deliberately NOT Data
  // Owner: ServiceBusAdministrationClient would require it, which is why discovery goes
  // through the ARM plane instead.
  reader: 'acdd72a7-3385-48ef-bd42-f606fba81ae7'
  // The smallest built-in role that permits peek. It also grants receive and complete,
  // which is why TST-070 fails the build on any receive-family symbol (spec OPN-003).
  serviceBusDataReceiver: '4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0'
  storageBlobDataContributor: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
  monitoringMetricsPublisher: '3913510d-42f4-4e42-8a64-420c390055eb'
  appConfigurationDataReader: '516239f1-63e1-4d78-a4de-a74fb236a071'
  keyVaultSecretsUser: '4633458b-17de-408a-b874-0445c86b69e6'
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: logAnalyticsWorkspaceName
  scope: resourceGroup(logAnalyticsResourceGroup)
}

resource appConfig 'Microsoft.AppConfiguration/configurationStores@2023-03-01' existing = {
  name: appConfigName
  scope: resourceGroup(appConfigResourceGroup)
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
  scope: resourceGroup(keyVaultResourceGroup)
}

// ---------------------------------------------------------------------------
// Telemetry: custom tables, DCE, DCR, rollup summary rule.
// ---------------------------------------------------------------------------

module telemetry 'modules/telemetry.bicep' = {
  name: 'telemetry-${sourceRegion}'
  scope: resourceGroup(logAnalyticsResourceGroup)
  params: {
    workspaceName: logAnalyticsWorkspaceName
    environmentName: environmentName
    location: location
  }
}

// ---------------------------------------------------------------------------
// Application Insights.
// ---------------------------------------------------------------------------

resource applicationInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: 'appi-${namePrefix}'
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: workspace.id
    // Spec FR-048. Sampling would drop metric points, and a sampled-away point produces
    // a missing alert evaluation indistinguishable from healthy silence.
    SamplingPercentage: 100
    DisableIpMasking: false
    // Spec FR-049. Ingest custom-metric dimensions into the Azure metrics store. Without
    // it the EntityPath dimension is stripped and the metric collapses to a namespace-wide
    // average that would essentially never breach — while the telemetry still arrives and
    // still looks plausible. This is a component property, not a function app setting.
    // Absent from the 2020-02-02 type definition (Azure/bicep#6595) though the API honours
    // it, hence the suppression.
    #disable-next-line BCP037
    CustomMetricsOptedInType: 'WithDimensions'
  }
}

// ---------------------------------------------------------------------------
// Function app: Flex Consumption with an always-ready instance.
//
// Spec OPN-016: .NET 10 isolated support on Flex and the timer singleton's behaviour
// under Flex scaling are verification items. If either does not hold, the documented
// fallback is EP1 pinned to one instance — Bicep changes, application code does not.
// ---------------------------------------------------------------------------

resource functionStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: replace('st${namePrefix}', '-', '')
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource deploymentContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: '${functionStorage.name}/default/deployments'
}

resource hostingPlan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: 'plan-${namePrefix}'
  location: location
  kind: 'functionapp'
  sku: {
    tier: 'FlexConsumption'
    name: 'FC1'
  }
  properties: {
    reserved: true
  }
}

resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: 'func-${namePrefix}'
  location: location
  kind: 'functionapp,linux'
  identity: {
    // Spec S-09: system-assigned managed identity. No secrets anywhere in the pipeline.
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: hostingPlan.id
    httpsOnly: true
    functionAppConfig: {
      deployment: {
        storage: {
          type: 'blobContainer'
          value: '${functionStorage.properties.primaryEndpoints.blob}deployments'
          authentication: {
            type: 'SystemAssignedIdentity'
          }
        }
      }
      scaleAndConcurrency: {
        // Nothing about this workload benefits from scale-out: it is bounded by the tick
        // budget, not by demand. Pinning the ceiling low keeps the in-memory caches warm
        // and keeps the timer singleton uncontended.
        alwaysReady: [
          {
            name: 'function:MessageAgeMonitor'
            instanceCount: 1
          }
        ]
        maximumInstanceCount: 40
        instanceMemoryMB: 2048
      }
      runtime: {
        name: 'dotnet-isolated'
        version: '10.0'
      }
    }
    siteConfig: {
      appSettings: [
        { name: 'AzureWebJobsStorage__accountName', value: functionStorage.name }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'dotnet-isolated' }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: applicationInsights.properties.ConnectionString }

        // FR-049 is configured on the Application Insights component above
        // (CustomMetricsOptedInType), not here — no app setting controls it.

        { name: 'MonitorSchedule', value: monitorSchedule }

        { name: 'ServiceBus:FullyQualifiedNamespace', value: serviceBusFullyQualifiedNamespace }
        { name: 'ServiceBus:SubscriptionId', value: subscription().subscriptionId }
        { name: 'ServiceBus:ResourceGroup', value: serviceBusResourceGroup }
        { name: 'ServiceBus:NamespaceName', value: serviceBusNamespaceName }

        { name: 'Monitor:SourceRegion', value: sourceRegion }
        { name: 'Monitor:Environment', value: environmentName }

        { name: 'AppConfig:Endpoint', value: appConfig.properties.endpoint }
        { name: 'AppConfig:SentinelKey', value: 'asbmon:sentinel' }
        { name: 'AppConfig:RefreshIntervalSeconds', value: '30' }

        { name: 'Lease:StorageAccountUri', value: leaseStorageAccountUri }
        { name: 'Lease:ContainerName', value: 'asbmon-lease' }
        { name: 'Lease:BlobName', value: 'primary' }

        { name: 'Telemetry:DceEndpoint', value: telemetry.outputs.dceEndpoint }
        { name: 'Telemetry:DcrImmutableId', value: telemetry.outputs.dcrImmutableId }
        { name: 'Telemetry:MeasurementStream', value: 'Custom-ServiceBusMessageAge_CL' }
        { name: 'Telemetry:HealthStream', value: 'Custom-ServiceBusMonitorHealth_CL' }
        { name: 'Telemetry:MetricNamespace', value: 'EIE.ServiceBus' }

        {
          name: 'Hashing:Salt'
          value: '@Microsoft.KeyVault(VaultName=${keyVaultName};SecretName=asbmon-hash-salt)'
        }
      ]
    }
  }
  dependsOn: [
    deploymentContainer
  ]
}

// ---------------------------------------------------------------------------
// Spec §10.3 — role assignments. Least privilege, one grant per purpose.
// ---------------------------------------------------------------------------

module serviceBusRoles 'modules/servicebus-roles.bicep' = {
  name: 'sb-roles-${sourceRegion}'
  scope: resourceGroup(serviceBusResourceGroup)
  params: {
    serviceBusNamespaceName: serviceBusNamespaceName
    principalId: functionApp.identity.principalId
    readerRoleId: roles.reader
    dataReceiverRoleId: roles.serviceBusDataReceiver
  }
}

resource functionStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(functionStorage.id, functionApp.id, roles.storageBlobDataContributor)
  scope: functionStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.storageBlobDataContributor)
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

module dcrRole 'modules/dcr-role.bicep' = {
  name: 'dcr-role-${sourceRegion}'
  scope: resourceGroup(logAnalyticsResourceGroup)
  params: {
    dcrId: telemetry.outputs.dcrId
    dcrName: 'dcr-asbmon-${environmentName}'
    principalId: functionApp.identity.principalId
    roleId: roles.monitoringMetricsPublisher
  }
}

module appConfigRole 'modules/scoped-role.bicep' = {
  name: 'appconfig-role-${sourceRegion}'
  scope: resourceGroup(appConfigResourceGroup)
  params: {
    resourceId: appConfig.id
    principalId: functionApp.identity.principalId
    roleId: roles.appConfigurationDataReader
  }
}

module keyVaultRole 'modules/scoped-role.bicep' = {
  name: 'kv-role-${sourceRegion}'
  scope: resourceGroup(keyVaultResourceGroup)
  params: {
    resourceId: keyVault.id
    principalId: functionApp.identity.principalId
    roleId: roles.keyVaultSecretsUser
  }
}

// ---------------------------------------------------------------------------
// Alert rules. Deployed once, with the primary region.
// ---------------------------------------------------------------------------

module alerts 'modules/alerts.bicep' = if (deployAlerts) {
  name: 'alerts-${environmentName}'
  params: {
    applicationInsightsId: applicationInsights.id
    workspaceId: workspace.id
    pagingActionGroupId: pagingActionGroupId
    ticketActionGroupId: ticketActionGroupId
    environmentName: environmentName
    location: location
    enablePaging: enablePaging
  }
}

output functionAppName string = functionApp.name
output functionPrincipalId string = functionApp.identity.principalId
output applicationInsightsId string = applicationInsights.id
output dceEndpoint string = telemetry.outputs.dceEndpoint
output dcrImmutableId string = telemetry.outputs.dcrImmutableId
