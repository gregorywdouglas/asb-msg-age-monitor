// Spec §2.2 / FR-060. The cross-region measurement lease.
//
// Deployed ONCE, in Central US — the standby region — not per region. The promotion
// decision is Central's to make, so the store it must read to make that decision should
// share Central's availability. Placing it in the primary region would couple the
// coordination mechanism to the compute region whose loss it exists to survive.
//
// ZRS rather than LRS because the whole point is that this outlives a zone failure in
// the region that holds it.
//
// Note that no monitoring is declared for this account. Every failure of it is designed
// to result in MORE monitoring, not less: an unreachable lease store makes the holder
// keep measuring and the standby start measuring (FR-063). That fail-open property is
// what makes it acceptable to put a coordination resource on a Tier 1 path at all.

@description('Location for the lease account. Must be the STANDBY compute region.')
param location string = 'centralus'

@description('Environment discriminator.')
param environmentName string

@description('Principal ids of both regional function apps.')
param principalIds array

var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var accountName = replace('stasbmonlease${environmentName}', '-', '')

resource leaseStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: accountName
  location: location
  sku: {
    name: 'Standard_ZRS'
  }
  kind: 'StorageV2'
  properties: {
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: leaseStorage
  name: 'default'
}

resource leaseContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'asbmon-lease'
  properties: {
    publicAccess: 'None'
  }
}

resource leaseRoleAssignments 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for principalId in principalIds: {
    name: guid(leaseStorage.id, principalId, storageBlobDataContributorRoleId)
    scope: leaseStorage
    properties: {
      roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
      principalId: principalId
      principalType: 'ServicePrincipal'
    }
  }
]

output blobEndpoint string = leaseStorage.properties.primaryEndpoints.blob
output accountName string = leaseStorage.name
