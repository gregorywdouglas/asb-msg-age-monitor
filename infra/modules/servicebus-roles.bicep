// Spec §10.3. The one grant the monitor needs against the Service Bus namespace. Entity
// discovery reads the pipeline-published catalog (SPEC-DEVIATION-008), so no ARM role
// is held here.

param serviceBusNamespaceName string
param principalId string

@description('Azure Service Bus Data Receiver — the smallest built-in role permitting peek.')
param dataReceiverRoleId string

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' existing = {
  name: serviceBusNamespaceName
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
