// Monitoring Metrics Publisher on the Data Collection Rule, which is what the Logs
// Ingestion API authorises against.

param dcrId string
param dcrName string
param principalId string
param roleId string

resource dataCollectionRule 'Microsoft.Insights/dataCollectionRules@2023-03-11' existing = {
  name: dcrName
}

resource assignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(dcrId, principalId, roleId)
  scope: dataCollectionRule
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleId)
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}
