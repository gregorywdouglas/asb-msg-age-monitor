// Spec §9. Alert rule definitions.
//
// The design principle that shapes this file (spec Appendix A, principle 2): the
// monitor measures, the alert layer decides what wakes someone. Nothing here is
// implemented inside the function, because suppression in code is invisible,
// untestable in production and cannot be reverted at 03:00.

@description('Application Insights resource id, source of the fast-path metric alerts.')
param applicationInsightsId string

@description('Log Analytics workspace resource id, source of the stateful query alerts.')
param workspaceId string

@description('Action group for Sev1/Sev2 paging. Non-production passes the non-paging group.')
param pagingActionGroupId string

@description('Action group for Sev3/Sev4 tickets and chat notification.')
param ticketActionGroupId string

@description('Environment discriminator.')
param environmentName string

@description('Location for scheduled query rules.')
param location string

@description('Spec §9.6 — production alone pages. Non-production is capped at Sev3.')
param enablePaging bool

var metricNamespace = 'EIE.ServiceBus'
var severityCap = enablePaging ? 0 : 3

// ---------------------------------------------------------------------------
// §9.1 Fast path — metric alerts.
//
// Two rules total. Per-entity thresholds are carried in the emitted ratio, so a
// threshold change in App Configuration takes effect on the next sentinel refresh
// without touching an alert rule. That is what makes per-entity thresholds
// operationally real rather than theoretical.
// ---------------------------------------------------------------------------

resource ageSev2 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'ALR-001-message-age-sev2-${environmentName}'
  location: 'global'
  properties: {
    description: 'ALR-001: oldest message age reached the Sev2 threshold in force for this entity.'
    severity: max(2, severityCap)
    enabled: true
    scopes: [applicationInsightsId]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT1M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'AgeBreachRatioSev2'
          metricNamespace: metricNamespace
          metricName: 'AgeBreachRatioSev2'
          operator: 'GreaterThanOrEqual'
          threshold: 1
          timeAggregation: 'Maximum'
          // Split by entity so one rule covers the whole estate and the alert payload
          // identifies which queue breached.
          dimensions: [
            {
              name: 'EntityPath'
              operator: 'Include'
              values: ['*']
            }
          ]
          skipMetricValidation: true
        }
      ]
    }
    autoMitigate: true
    actions: [
      {
        actionGroupId: enablePaging ? pagingActionGroupId : ticketActionGroupId
      }
    ]
  }
}

resource ageSev1 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'ALR-002-message-age-sev1-${environmentName}'
  location: 'global'
  properties: {
    description: 'ALR-002: oldest message age reached the Sev1 threshold in force for this entity.'
    severity: max(1, severityCap)
    enabled: true
    scopes: [applicationInsightsId]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT1M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'AgeBreachRatioSev1'
          metricNamespace: metricNamespace
          metricName: 'AgeBreachRatioSev1'
          operator: 'GreaterThanOrEqual'
          threshold: 1
          timeAggregation: 'Maximum'
          dimensions: [
            {
              name: 'EntityPath'
              operator: 'Include'
              values: ['*']
            }
          ]
          skipMetricValidation: true
        }
      ]
    }
    autoMitigate: true
    actions: [
      {
        actionGroupId: enablePaging ? pagingActionGroupId : ticketActionGroupId
      }
    ]
  }
}

// ---------------------------------------------------------------------------
// §9.3 L4 — monitoring health.
//
// Both no-data behaviours below default to NOT firing. Getting either wrong produces
// an L4 layer that passes review, looks correct in the portal, and never fires.
// TST-050 exists specifically to catch that.
// ---------------------------------------------------------------------------

resource heartbeatAbsent 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'ALR-030-monitor-heartbeat-absent-${environmentName}'
  location: 'global'
  properties: {
    description: 'ALR-030: no monitor heartbeat for 5 minutes. The monitor itself is down.'
    severity: max(1, severityCap)
    enabled: true
    scopes: [applicationInsightsId]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'MonitorHeartbeat'
          metricNamespace: metricNamespace
          metricName: 'MonitorHeartbeat'
          operator: 'LessThan'
          threshold: 1
          timeAggregation: 'Count'
          skipMetricValidation: true
        }
      ]
    }
    // Required. The default treats absent data as "condition not met", which silences
    // the alert precisely when the data is gone.
    autoMitigate: false
    actions: [
      {
        actionGroupId: enablePaging ? pagingActionGroupId : ticketActionGroupId
      }
    ]
  }
}

// ---------------------------------------------------------------------------
// §9.2 Stateful path — scheduled query rules.
// ---------------------------------------------------------------------------

var dedupePrefix = 'ServiceBusMessageAge_CL | summarize arg_max(TimeGenerated, *) by MeasurementId'

var queryRules = [
  {
    id: 'ALR-010'
    name: 'consumption-stalled'
    description: 'ALR-010: the head message has not moved while the queue holds messages. Consumption has stopped.'
    severity: 2
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: '${dedupePrefix}\n| where ConsumptionStalled == true\n| summarize StalledSince = min(TimeGenerated), MaxAge = max(AgeSeconds) by EntityPath, SourceRegion'
  }
  {
    id: 'ALR-011'
    name: 'head-blocked-by-deferred'
    description: 'ALR-011: the scan budget is exhausted by deferred messages. Likely an application-level defer leak.'
    severity: 3
    frequency: 'PT15M'
    window: 'PT30M'
    failOnNoData: false
    query: '${dedupePrefix}\n| where MeasurementStatus == "HeadBlockedByDeferred"\n| summarize BlockedTicks = count() by EntityPath, SourceRegion\n| where BlockedTicks >= 15'
  }
  {
    id: 'ALR-012'
    name: 'sustained-indeterminate'
    description: 'ALR-012: the monitor cannot distinguish an empty entity from an impaired one. This is a monitor incident, not an entity one.'
    severity: 2
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: '${dedupePrefix}\n| where MeasurementStatus == "Indeterminate"\n| summarize IndeterminateTicks = count() by EntityPath, SourceRegion\n| where IndeterminateTicks >= 3'
  }
  {
    id: 'ALR-013'
    name: 'tick-incomplete'
    description: 'ALR-013: ticks are not completing inside the deadline; the effective sampling interval has degraded.'
    severity: 3
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "MonitorTickIncomplete"\n| summarize IncompleteTicks = count() by SourceRegion\n| where IncompleteTicks >= 3'
  }
  {
    id: 'ALR-014'
    name: 'monitor-degraded'
    description: 'ALR-014: the monitor is shedding load. Reported ages are lower bounds.'
    severity: 3
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "Heartbeat" and DegradationLevel >= 2\n| summarize DegradedTicks = count() by SourceRegion\n| where DegradedTicks >= 10'
  }
  {
    id: 'ALR-015'
    name: 'config-stale'
    description: 'ALR-015: thresholds in force did not come from App Configuration. The sentinel refresh may be failing silently.'
    severity: 3
    frequency: 'PT15M'
    window: 'PT30M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "Heartbeat" and ConfigSource != "AppConfig"\n| summarize StaleTicks = count() by SourceRegion, ConfigSource\n| where StaleTicks >= 15'
  }
  {
    id: 'ALR-016'
    name: 'config-rejected'
    description: 'ALR-016: an App Configuration value failed validation and was rejected. Last-known-good is in force.'
    severity: 2
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "ConfigRejected"\n| project TimeGenerated, SourceRegion, Detail'
  }
  {
    id: 'ALR-017'
    name: 'emit-failure'
    description: 'ALR-017: a telemetry sink has been failing. The analytical and alerting records have diverged.'
    severity: 2
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "Heartbeat" and (AppInsightsEmitOk == false or LogsIngestionEmitOk == false)\n| summarize FailedTicks = count() by SourceRegion\n| where FailedTicks >= 5'
  }
  {
    id: 'ALR-019'
    name: 'clock-anomaly'
    description: 'ALR-019: message ages are implausible. Clock trust is broken and measurements are suspect.'
    severity: 3
    frequency: 'PT15M'
    window: 'PT30M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "ClockAnomaly"\n| summarize Anomalies = count() by SourceRegion\n| where Anomalies >= 5'
  }
  {
    id: 'ALR-020'
    name: 'lease-takeover'
    description: 'ALR-020: the measurement lease changed regions. Expect a short detection gap around the transition.'
    severity: 3
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType in ("LeaseTakeover", "LeaseAcquired")\n| project TimeGenerated, SourceRegion, EventType, Detail'
  }
  {
    id: 'ALR-021'
    name: 'split-brain'
    description: 'ALR-021: both regions are measuring. Duplicate rows are expected; dedupe on MeasurementId.'
    severity: 3
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "Heartbeat" and IsLeaseHolder == true\n| summarize Regions = dcount(SourceRegion) by bin(TimeGenerated, 1m)\n| where Regions > 1\n| summarize SplitBrainMinutes = count()\n| where SplitBrainMinutes >= 5'
  }
  {
    id: 'ALR-022'
    name: 'transfer-dlq-non-empty'
    description: 'ALR-022: auto-forwarding is failing. Messages are landing in the transfer dead-letter queue.'
    severity: 2
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: '${dedupePrefix}\n| where TransferDeadLetterMessageCount > 0\n| summarize Ticks = count(), MaxDepth = max(TransferDeadLetterMessageCount) by EntityPath, ForwardTarget\n| where Ticks >= 10'
  }
  {
    id: 'ALR-023'
    name: 'unsupported-entity-configuration'
    description: 'ALR-023: a partitioned entity was found. Its age is a lower bound only and absence of breach is not evidence of health.'
    severity: 3
    frequency: 'PT1H'
    window: 'PT6H'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "UnsupportedEntityConfiguration"\n| summarize by EntityPath, Detail'
  }
  {
    id: 'ALR-024'
    name: 'entity-unclassified'
    description: 'ALR-024: an entity has no criticality class. It is defaulting to standard, which may be wrong for its traffic profile.'
    severity: 4
    frequency: 'P1D'
    window: 'P1D'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "EntityUnclassified"\n| summarize by EntityPath'
  }
  {
    id: 'ALR-028'
    name: 'threshold-drift'
    description: 'ALR-028: an entity threshold has diverged from its class baseline for over a day. Confirm this is intentional and not a forgotten override.'
    severity: 4
    frequency: 'P1D'
    window: 'P1D'
    failOnNoData: false
    query: '${dedupePrefix}\n| summarize Sev2 = any(Sev2ThresholdSeconds), Sev1 = any(Sev1ThresholdSeconds), Class = any(CriticalityClass) by EntityPath\n| where (Class == "Standard" and Sev2 != 300) or (Class == "Critical" and Sev2 != 60) or (Class == "Bulk" and Sev2 != 1800)'
  }
  {
    id: 'ALR-032'
    name: 'no-measurement-rows'
    description: 'ALR-032: no measurement rows have arrived. Either the monitor is dead or the ingestion path is broken.'
    severity: 1
    frequency: 'PT5M'
    window: 'PT10M'
    // The one rule that MUST fire on no data. Without this the query returning nothing
    // is treated as "condition not met" and the alert is silent exactly when the data
    // is gone (spec §9.3).
    failOnNoData: true
    query: 'ServiceBusMessageAge_CL\n| summarize Rows = count()\n| where Rows == 0'
  }
  {
    id: 'ALR-031'
    name: 'monitor-running-but-blind'
    description: 'ALR-031: the monitor is executing and emitting, but measured nothing. This is a different incident from a dead monitor.'
    severity: 1
    frequency: 'PT5M'
    window: 'PT15M'
    failOnNoData: false
    query: 'ServiceBusMonitorHealth_CL\n| where EventType == "Heartbeat" and IsLeaseHolder == true\n| where MeasuredEntities == 0 and DiscoveredEntities > 0\n| summarize BlindTicks = count() by SourceRegion\n| where BlindTicks >= 3'
  }
]

resource scheduledQueryRules 'Microsoft.Insights/scheduledQueryRules@2023-03-15-preview' = [
  for rule in queryRules: {
    name: '${rule.id}-${rule.name}-${environmentName}'
    location: location
    properties: {
      description: rule.description
      severity: max(rule.severity, severityCap)
      enabled: true
      scopes: [workspaceId]
      evaluationFrequency: rule.frequency
      windowSize: rule.window
      criteria: {
        allOf: [
          {
            query: rule.query
            timeAggregation: 'Count'
            operator: 'GreaterThan'
            threshold: 0
            failingPeriods: {
              numberOfEvaluationPeriods: 1
              minFailingPeriodsToAlert: 1
            }
          }
        ]
      }
      autoMitigate: true
      // Spec §9.3: this must be set explicitly. The platform default does not fire on
      // an empty result set.
      checkWorkspaceAlertsStorageConfigured: false
      actions: {
        actionGroups: [
          rule.severity <= 2 && enablePaging ? pagingActionGroupId : ticketActionGroupId
        ]
      }
    }
  }
]

// ---------------------------------------------------------------------------
// §9.4 Alert processing rules.
//
// ALR-041 (cross-region duplicate grouping) needs no resource. Both regions emit the
// ratio metric with EntityPath and SourceRegion dimensions, but ALR-001/002 split on
// EntityPath alone — so Azure aggregates the two regions onto a single time series
// with Maximum, and a split-brain window produces one alert per entity rather than
// two. Deduplication falls out of the dimension design rather than needing a rule.
//
// ALR-040 (failover grouping), ALR-042 (maintenance suppression) and ALR-043
// (deployment suppression) are created on demand and deliberately not declared here:
// ALR-042 by a human with a mandatory end time, ALR-043 by the release pipeline scoped
// to the L4 rules only. Declaring them statically would mean a standing suppression,
// which is the failure mode ALR-026/027 exist to catch.
//
// KNOWN GAP: Azure alert processing rules cannot express correlation grouping
// declaratively — only suppression and action-group substitution. ALR-040's "group the
// post-failover burst into one incident with N members" is therefore documented in the
// runbook as an operational step rather than deployed. See docs/specs open items.
// ---------------------------------------------------------------------------

output metricAlertIds array = [ageSev2.id, ageSev1.id, heartbeatAbsent.id]
output queryRuleCount int = length(queryRules)
