// Spec §8 / §10.1. Custom tables, Data Collection Endpoint and Data Collection Rule.
//
// The column definitions are loaded from infra/schema/*.json, which is the single
// source of truth shared by the table schema, the DCR stream declaration and the
// emitted row DTO. A field the function sends that the DCR does not declare is
// dropped silently, with no error at either end (spec §8.2), so the three must not be
// allowed to drift independently — TST-060c asserts they still agree.

@description('Log Analytics workspace name (existing).')
param workspaceName string

@description('Environment discriminator, e.g. dev | staging | prod.')
param environmentName string

@description('Deployment location for the DCE and DCR.')
param location string

@description('Interactive retention in days for the raw measurement table (spec NFR-015).')
param interactiveRetentionDays int = 90

@description('Total retention in days, including the cheap archive tier (spec NFR-015).')
param totalRetentionDays int = 730

var measurementColumns = loadJsonContent('../schema/measurement-columns.json')
var healthColumns = loadJsonContent('../schema/health-columns.json')

var measurementTableName = 'ServiceBusMessageAge_CL'
var healthTableName = 'ServiceBusMonitorHealth_CL'
var rollupTableName = 'ServiceBusMessageAgeHourly_CL'

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: workspaceName
}

// Spec §8.5. Raw per-tick rows: 90 days interactive for operational investigation,
// then the archive tier so SLA questions remain answerable retroactively. Retention is
// effectively irreversible after the fact, which is why it is set now rather than
// deferred (spec §8.5 / ASM-008).
resource measurementTable 'Microsoft.OperationalInsights/workspaces/tables@2023-09-01' = {
  parent: workspace
  name: measurementTableName
  properties: {
    plan: 'Analytics'
    retentionInDays: interactiveRetentionDays
    totalRetentionInDays: totalRetentionDays
    schema: {
      name: measurementTableName
      columns: measurementColumns
    }
  }
}

resource healthTable 'Microsoft.OperationalInsights/workspaces/tables@2023-09-01' = {
  parent: workspace
  name: healthTableName
  properties: {
    plan: 'Analytics'
    retentionInDays: interactiveRetentionDays
    totalRetentionInDays: totalRetentionDays
    schema: {
      name: healthTableName
      columns: healthColumns
    }
  }
}

// Spec §8.4. Compact hourly rollup retained for two years: SLA reporting queries this
// rather than the raw ticks. SuppressedMinutes exists so a breach during an approved
// maintenance window is distinguishable from a real one without reconstructing it from
// activity logs.
resource rollupTable 'Microsoft.OperationalInsights/workspaces/tables@2023-09-01' = {
  parent: workspace
  name: rollupTableName
  properties: {
    plan: 'Analytics'
    retentionInDays: totalRetentionDays
    totalRetentionInDays: totalRetentionDays
    schema: {
      name: rollupTableName
      columns: [
        { name: 'TimeGenerated', type: 'datetime' }
        { name: 'EntityPath', type: 'string' }
        { name: 'EntityKind', type: 'string' }
        { name: 'CriticalityClass', type: 'string' }
        { name: 'Environment', type: 'string' }
        { name: 'NamespaceName', type: 'string' }
        { name: 'AgeP50Seconds', type: 'real' }
        { name: 'AgeP95Seconds', type: 'real' }
        { name: 'AgeMaxSeconds', type: 'real' }
        { name: 'BreachMinutesSev1', type: 'int' }
        { name: 'BreachMinutesSev2', type: 'int' }
        { name: 'MeasuredTicks', type: 'int' }
        { name: 'IndeterminateTicks', type: 'int' }
        { name: 'BlockedTicks', type: 'int' }
        { name: 'NotMeasuredTicks', type: 'int' }
        { name: 'SuppressedMinutes', type: 'int' }
        { name: 'MaxActiveMessageCount', type: 'long' }
        { name: 'MaxDeadLetterMessageCount', type: 'long' }
      ]
    }
  }
}

resource dataCollectionEndpoint 'Microsoft.Insights/dataCollectionEndpoints@2023-03-11' = {
  name: 'dce-asbmon-${environmentName}'
  location: location
  properties: {
    networkAcls: {
      // Spec §10.2 / OPN-004: public with firewall restriction is the decided posture.
      publicNetworkAccess: 'Enabled'
    }
  }
}

resource dataCollectionRule 'Microsoft.Insights/dataCollectionRules@2023-03-11' = {
  name: 'dcr-asbmon-${environmentName}'
  location: location
  properties: {
    dataCollectionEndpointId: dataCollectionEndpoint.id
    streamDeclarations: {
      'Custom-ServiceBusMessageAge_CL': {
        columns: measurementColumns
      }
      'Custom-ServiceBusMonitorHealth_CL': {
        columns: healthColumns
      }
    }
    destinations: {
      logAnalytics: [
        {
          workspaceResourceId: workspace.id
          name: 'law'
        }
      ]
    }
    dataFlows: [
      {
        streams: ['Custom-ServiceBusMessageAge_CL']
        destinations: ['law']
        transformKql: 'source'
        outputStream: 'Custom-${measurementTableName}'
      }
      {
        streams: ['Custom-ServiceBusMonitorHealth_CL']
        destinations: ['law']
        transformKql: 'source'
        outputStream: 'Custom-${healthTableName}'
      }
    ]
  }
  dependsOn: [
    measurementTable
    healthTable
  ]
}

// Spec §8.4. Hourly rollup driven by a summary rule so SLA reporting reads a compact
// table rather than scanning raw ticks over a quarter.
resource rollupSummaryRule 'Microsoft.OperationalInsights/workspaces/summaryLogs@2023-01-01-preview' = {
  parent: workspace
  name: 'asbmon-hourly-rollup'
  properties: {
    ruleType: 'User'
    description: 'Hourly per-entity message-age rollup for SLA reporting (spec §8.4).'
    destinationTable: rollupTableName
    binSize: 60
    query: '''
ServiceBusMessageAge_CL
| summarize arg_max(TimeGenerated, *) by MeasurementId
| summarize
    AgeP50Seconds       = percentile(AgeSeconds, 50),
    AgeP95Seconds       = percentile(AgeSeconds, 95),
    AgeMaxSeconds       = max(AgeSeconds),
    BreachMinutesSev1   = countif(AgeBreachRatioSev1 >= 1.0),
    BreachMinutesSev2   = countif(AgeBreachRatioSev2 >= 1.0),
    MeasuredTicks       = countif(MeasurementStatus in ("Measured", "Degraded", "Empty")),
    IndeterminateTicks  = countif(MeasurementStatus == "Indeterminate"),
    BlockedTicks        = countif(MeasurementStatus == "HeadBlockedByDeferred"),
    NotMeasuredTicks    = countif(MeasurementStatus == "NotMeasured"),
    SuppressedMinutes   = 0,
    MaxActiveMessageCount     = max(ActiveMessageCount),
    MaxDeadLetterMessageCount = max(DeadLetterMessageCount),
    EntityKind          = any(EntityKind),
    CriticalityClass    = any(CriticalityClass),
    Environment         = any(Environment),
    NamespaceName       = any(NamespaceName)
  by EntityPath
'''
  }
  dependsOn: [
    rollupTable
  ]
}

output dceEndpoint string = dataCollectionEndpoint.properties.logsIngestion.endpoint
output dcrImmutableId string = dataCollectionRule.properties.immutableId
output dcrId string = dataCollectionRule.id
output measurementTableName string = measurementTableName
output healthTableName string = healthTableName
