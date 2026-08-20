// Spec §10.4 / FR-070. Partitioned entities are prohibited by platform standard.
//
// On a partitioned entity, SequenceNumber encodes the partition in its high bits and
// ordering holds only within a partition. A forward scan from the lowest sequence
// number therefore walks one partition preferentially, so "the oldest message I found"
// may not be the oldest message in the entity — the monitor could report 40s while a
// 900s message sits at the head of another partition. That is a silent under-report,
// the worst failure this design can produce.
//
// Partitioning is immutable after entity creation, so create-time denial is the only
// enforcement point. The monitor asserts the same invariant independently at discovery
// (FR-071) so that a policy exemption cannot quietly produce best-effort measurements
// presented as exact.

targetScope = 'subscription'

@description('Management group or subscription scope at which to assign the policy.')
param assignmentScopeName string = subscription().displayName

resource denyPartitionedEntities 'Microsoft.Authorization/policyDefinitions@2023-04-01' = {
  name: 'eie-deny-partitioned-servicebus-entities'
  properties: {
    displayName: 'EIE: deny partitioned Service Bus queues and topics'
    description: 'Message-age monitoring depends on a monotonic SequenceNumber space. Partitioned entities break that guarantee and cause silent under-reporting of message age.'
    policyType: 'Custom'
    mode: 'All'
    metadata: {
      category: 'Service Bus'
      version: '1.0.0'
    }
    policyRule: {
      if: {
        allOf: [
          {
            field: 'type'
            in: [
              'Microsoft.ServiceBus/namespaces/queues'
              'Microsoft.ServiceBus/namespaces/topics'
            ]
          }
          {
            anyOf: [
              {
                field: 'Microsoft.ServiceBus/namespaces/queues/enablePartitioning'
                equals: 'true'
              }
              {
                field: 'Microsoft.ServiceBus/namespaces/topics/enablePartitioning'
                equals: 'true'
              }
            ]
          }
        ]
      }
      then: {
        effect: 'deny'
      }
    }
  }
}

resource assignment 'Microsoft.Authorization/policyAssignments@2023-04-01' = {
  name: 'eie-deny-partitioned-servicebus'
  properties: {
    displayName: 'EIE: deny partitioned Service Bus entities (${assignmentScopeName})'
    policyDefinitionId: denyPartitionedEntities.id
    enforcementMode: 'Default'
  }
}

output policyDefinitionId string = denyPartitionedEntities.id
