// Spec §10.3. The two grants the monitor needs against the Service Bus namespace,
// deliberately split so the privilege reduction is visible in review.

param serviceBusNamespaceName string
param principalId string

@description('Reader — ARM entity enumeration only. No data-plane access.')
param readerRoleId string

@description('Azure Service Bus Data Receiver — the smallest built-in role permitting peek.')
param dataReceiverRoleId string

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' existing = {
  name: serviceBusNamespaceName
}

// Discovery goes through the ARM management plane precisely so this can be Reader
// rather than Data Owner, which ServiceBusAdministrationClient would have required.
resource readerAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusNamespace.id, principalId, readerRoleId)
  scope: serviceBusNamespace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', readerRoleId)
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

// This grant also permits receive and complete, because Azure exposes no peek-only
// built-in role (spec OPN-003). Until a custom role is proven expressible, TST-070 is
// the control: it fails the build if the assembly references any receive-family symbol.
resource dataReceiverAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusNamespace.id, principalId, dataReceiverRoleId)
  scope: serviceBusNamespace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', dataReceiverRoleId)
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}
