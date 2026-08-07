# Service Bus Message Age Monitor (`asb-msg-age-monitor`)

**Status:** Draft for build
**Version:** 1.0
**Platform:** WMX — Azure Integration Services middleware (BizTalk Server 2020 replacement, IBM Maximo integration)
**Service tier:** Tier 1 — RTO < 1 hour, RPO near-zero
**Regions:** East US 2 (primary), Central US (secondary), hot standby
**Runtime:** .NET 10, Azure Functions isolated worker

---

## Table of contents

1. [Purpose and scope](#1-purpose-and-scope)
2. [Architecture](#2-architecture)
3. [Measurement semantics](#3-measurement-semantics)
4. [Functional requirements](#4-functional-requirements)
5. [Non-functional requirements](#5-non-functional-requirements)
6. [Component contracts](#6-component-contracts)
7. [Configuration surface](#7-configuration-surface)
8. [Telemetry schema](#8-telemetry-schema)
9. [Alert rule definitions](#9-alert-rule-definitions)
10. [Infrastructure, identity and deployment](#10-infrastructure-identity-and-deployment)
11. [Test specification](#11-test-specification)
12. [Runbook requirements](#12-runbook-requirements)
13. [Assumptions](#13-assumptions)
14. [Open items register](#14-open-items-register)
15. [Deferred and out of scope](#15-deferred-and-out-of-scope)

---

## 1. Purpose and scope

### 1.1 Problem

Azure Monitor exposes no native message-age metric for Service Bus. No per-message timestamp appears in any Service Bus resource log, and no settlement timestamp exists anywhere in the platform. Queue depth is a poor proxy: a queue holding a steady twelve messages looks identical whether those messages arrived seconds ago or have been stuck for a week.

For a Tier 1 integration layer replacing BizTalk, the operationally meaningful question is not *how many* messages are waiting but *how long the oldest one has been waiting*. Broker-side peek is the only source of truth for that number.

### 1.2 Scope

This component is **L1** of a four-layer detection topology:

| Layer | Mechanism | Detects | Owner |
|---|---|---|---|
| **L1** | **Timer-triggered peek (this component)** | **Broker-side message age** | **WMX platform** |
| L2 | Consumer-side dequeue latency instrumentation | Consumer processing lag | Consumer app teams |
| L3 | TTL + dead-letter backstop | Terminal message failure | WMX platform |
| L4 | Monitoring-health no-data alerting | Failure of L1–L3 themselves | WMX platform |

L1 detects when a message in any Service Bus queue or topic subscription has aged past a configured threshold, and emits that as alertable telemetry to two independent sinks.

### 1.3 Non-goals

- **Not** a consumer-side latency measurement (that is L2).
- **Not** a dead-letter triage system (that is L3; see [OUT-002](#15-deferred-and-out-of-scope)).
- **Not** an end-to-end business-transaction latency measurement. This component measures per-hop residence time only. See [§3.4](#34-known-limitation-auto-forwarding).
- **Not** a remediation system. It never receives, completes, abandons, defers or dead-letters a message. See [FR-050](#fr-050).

### 1.4 Settled architectural decisions (context, not open for revision)

| # | Decision |
|---|---|
| S-01 | Service Bus namespace is **Premium** tier. No session-enabled entities. |
| S-02 | Timer-triggered peek at 60 s is the **sole detection mechanism**. |
| S-03 | Event Grid is deliberately excluded from the critical path because it fails silent. |
| S-04 | There is no native message-age metric, no per-message timestamp in resource logs, and no settlement timestamp. Peek is the only broker-side truth. Age is **not** derived from Log Analytics or correlated Application Insights spans. |
| S-05 | **Dual emit**: `OldestMessageAgeSeconds` to Application Insights as a custom metric, and `ServiceBusMessageAge_CL` to Log Analytics via the Logs Ingestion API (DCR/DCE). Both writes fail independently. |
| S-06 | Baseline thresholds: **300 s Severity 2**, **900 s Severity 1**. |
| S-07 | Age is computed from the oldest message in **Active** state via a bounded forward scan, skipping Deferred, Scheduled, and expired-but-not-collected messages. Budget exhaustion without an Active message emits `HeadBlockedByDeferred` and no age. |
| S-08 | Entity discovery is **dynamic and cached**. |
| S-09 | System-assigned managed identity, Bicep, OIDC-authenticated pipelines. |
| S-10 | Execution is stateless in the sense that no durable or shared state store participates in correctness. See [NFR-020](#nfr-020) for the precise reconciliation with in-memory caching. |

---

## 2. Architecture

### 2.1 Execution model

```
                    ┌────────────────────────────────────────────┐
                    │  Azure App Configuration (thresholds,      │
                    │  classes, sentinel)  — background refresh  │
                    └────────────────┬───────────────────────────┘
                                     │ (never blocks a tick)
  ┌──────────────────────────────────▼──────────────────────────────────┐
  │  MessageAgeMonitorFunction   [TimerTrigger 0 */1 * * * *]           │
  │  runOnStartup: false, singleton                                      │
  │                                                                      │
  │   1. LeaseCoordinator.TryAcquireOrRenew()  ──► not holder? exit      │
  │   2. EntityDiscoveryService.GetEntitiesAsync()   (cached, 300 s TTL) │
  │   3. DegradationController.CurrentLevel                              │
  │   4. bounded-parallel fan-out (MaxConcurrentEntityScans)             │
  │        └─ per entity, under PerEntityTimeout:                        │
  │             a. EntityRuntimeReader.GetRuntimePropertiesAsync()       │
  │             b. OldestActiveMessageResolver.ResolveAsync()            │
  │                  └─ IEntityPeeker.PeekAsync (bounded forward scan)   │
  │             c. ThresholdProvider.Resolve(entity)                     │
  │             d. build MeasurementRecord                               │
  │   5. hard TickDeadline ──► unmeasured entities carried to next tick  │
  │   6. TelemetryEmitter.EmitAsync(records + heartbeat)                 │
  └───────────────┬──────────────────────────────────┬──────────────────┘
                  │                                  │
      ┌───────────▼────────────┐        ┌────────────▼─────────────────┐
      │ Application Insights   │        │ Logs Ingestion API (DCE/DCR) │
      │ custom metrics         │        │ ServiceBusMessageAge_CL      │
      │ (metric alert path)    │        │ ServiceBusMonitorHealth_CL   │
      └────────────────────────┘        └──────────────────────────────┘
                  │                                  │
      ┌───────────▼──────────────────────────────────▼─────────────────┐
      │ Azure Monitor: 2 metric alert rules (ratio ≥ 1.0)              │
      │              + N scheduled query rules (stateful conditions)   │
      │              + alert processing rules (grouping, suppression)  │
      └────────────────────────────────────────────────────────────────┘
```

### 2.2 Regional topology

Both regional deployments resolve the same geo-replicated namespace FQDN and therefore observe the same primary namespace. The secondary deployment exists to survive loss of the **East US 2 compute region**; it provides no additional coverage for a Service Bus namespace failover, because namespace promotion and Function App regional availability are independent failure domains.

Coordination is **active/standby via a blob lease** ([FR-060](#fr-060)). The lease store is deliberately placed in **Central US** — the standby region — so that the region making the promotion decision shares a failure domain with the store it must read to make it.

> **Recorded objection.** The design author's recommendation was both regions running unconditionally with deduplication in the alert layer, on the grounds that (a) safe lease semantics degenerate to "both run" whenever the lease store is unreachable, (b) the takeover gap is a detection blind window opening precisely during a regional incident, and (c) cross-region divergence is the only independent correctness check available on the monitor, since there is no second source of broker truth. The active/standby design was reaffirmed by the requirement owner and is specified here as decided. The blind window is documented as an accepted limit in [NFR-011](#nfr-011).

### 2.3 Dependency inventory

| Dependency | Criticality | Failure behaviour |
|---|---|---|
| Service Bus Premium namespace | **Critical** | No measurement possible. `Indeterminate` + monitor alert. |
| Application Insights | **Critical** (alerting path) | Retry; failure recorded in LAW + `ILogger`. |
| Log Analytics / DCE / DCR | High (analysis, SLA) | Retry; failure recorded in App Insights + `ILogger`. |
| Azure App Configuration | Medium | Fall back to last-known-good, then compiled defaults. Never blocks. |
| Lease storage account (ZRS, Central US) | **Low by design** | Fail open in both directions — every failure results in *more* monitoring, never less. |

---

## 3. Measurement semantics

### 3.1 Definition of age

```
AgeSeconds = (host UTC now) − (oldest Active message EnqueuedTimeUtc)
```

`AgeSeconds` measures **residence time in a single entity**. `EnqueuedTimeUtc` is broker-assigned, which is what makes it trustworthy: it cannot be influenced by a producer's clock or by a producer forgetting to stamp a field.

**Alerting must never depend on producer compliance.** No application property participates in the alert path.

### 3.2 Bounded forward scan

For each entity, the resolver walks messages in `SequenceNumber` order looking for the first message in `Active` state.

```
resolve(entity):
  fromSeq  ← resumePoint[entity]  (or null → head)
  scanned  ← 0
  while scanned < effectiveScanDepth:
      batch ← peek(entity, fromSeq, PeekBatchSize)
      if batch is empty:
          → zero-result path (§3.5)
      for msg in batch:
          if msg.State == Active and not expired:
              → MEASURED (age, sequence number, delivery count)
      scanned += batch.Count
      fromSeq  = batch.Last.SequenceNumber + 1
      resumePoint[entity] = fromSeq
  → HeadBlockedByDeferred (no age emitted)
```

**Skipped states.** `Deferred`, `Scheduled` (`ScheduledEnqueueTimeUtc` in the future), and expired-but-not-collected messages (`EnqueuedTimeUtc + TimeToLive < now`) are not candidates.

**Resume points.** `resumePoint[entity]` is an **in-memory, non-durable, advisory** `ConcurrentDictionary<string, long>`. It converts a static deferred block from a per-tick cost into a one-time cost. It is never persisted or shared, and correctness never depends on it — loss costs exactly one expensive tick to rebuild ([NFR-020](#nfr-020)).

Because a stale resume point could cause the scan to skip past a genuinely old Active message — a **silent under-report**, the worst failure this system can produce — invalidation is aggressive ([FR-013](#fr-013)):

- `ActiveMessageCount` decreased since the previous tick
- entity observed empty
- hard TTL of `ResumePointMaxTicks` (default 15) regardless of other conditions
- process start

> Resume points are sound only on a monotonic sequence space. This is why partitioned entities are prohibited ([FR-070](#fr-070)) — the two decisions are coupled.

### 3.3 Clock trust

Skew is **detected, not corrected**. Applying a derived offset can introduce error rather than remove it, and the resulting error is undetectable. Azure host NTP skew is sub-second in practice; the value is entirely in knowing when the assumption breaks.

| Observation | Action |
|---|---|
| `AgeSeconds < 0` | Clamp to 0, emit `ClockAnomaly`, suppress that entity's alert for the tick |
| `EnqueuedTimeUtc > now + ClockSkewToleranceSeconds` (default 2) | Emit `ClockAnomaly` with measured delta |
| `AgeSeconds > ImplausibleAgeSeconds` (default 2 592 000 / 30 d) | Emit `ClockAnomaly`, record but do not alert |
| `ClockAnomaly` sustained `ClockAnomalyTicksForAlert` (default 5) ticks | Sev3 against the monitor |

### 3.4 Known limitation: auto-forwarding

`EnqueuedTimeUtc` is assigned per entity, so **auto-forwarding resets it**. A message that waited 10 minutes in a topic and then auto-forwarded to a queue reports `AgeSeconds ≈ 0` at the destination.

**Compensating control:** every hop is itself a monitored entity, so the delay is detected *at the hop where it occurs*. This is arguably better for diagnosis, but it is emphatically **not** an end-to-end latency measurement, and must not be presented as one. See [OPN-011](#14-open-items-register) and [OUT-001](#15-deferred-and-out-of-scope).

### 3.5 Zero-result ambiguity

A throttled or degraded namespace can return zero counts and empty peek batches — indistinguishable from a genuinely drained queue. The monitor's happiest-looking output is therefore also its blindest state.

**Governing principle ([NFR-001](#nfr-001)): the monitor must never emit a healthy measurement it cannot substantiate.** Silence and `Indeterminate` are acceptable; a fabricated zero is not.

```
zero or empty result:
    throttle exception observed this tick?      → Indeterminate
    prior tick depth > 0 with no drain evidence → Indeterminate
    zero sustained ZeroCorroborationTicks (3)
        with no exceptions                      → Empty (trusted)
    otherwise                                   → Indeterminate
```

`Indeterminate` sustained for `IndeterminateTicksForAlert` (default 3) raises **Sev2 against the monitor**, not against the entity.

### 3.6 Measurement status

`MeasurementStatus` is a required field on every record. **Absence of age is an explicit value, never a missing field or a dropped record** — a missing row and a healthy row are indistinguishable in a dashboard, and that is how monitoring systems lie.

| Status | Meaning | `AgeSeconds` | Age alerting |
|---|---|---|---|
| `Measured` | Active message found, age computed | value | active |
| `Empty` | Entity corroborated empty | `0` | active (never breaches) |
| `Indeterminate` | Cannot distinguish empty from impaired | `null` | suppressed; monitor alert |
| `HeadBlockedByDeferred` | Scan budget exhausted without an Active message | `null` | suppressed; separate signal |
| `Degraded` | Reduced-budget measurement, may under-report | value | active (lower bound) |
| `NotMeasured` | Tick deadline reached before this entity | `null` | suppressed |
| `Throttled` | Explicit throttling response from broker | `null` | suppressed; monitor alert |
| `ClockAnomaly` | Age implausible; clock trust broken | clamped | suppressed |
| `EntityNotFound` | Entity disappeared mid-scan | `null` | suppressed; tombstone flow |

### 3.7 Stall detection

Distinct from age, and observed rather than predicted:

```
ConsumptionStalled ⟺
      head SequenceNumber unchanged for StalledTicksForAlert (5) consecutive ticks
  AND ActiveMessageCount > 0
  AND MeasurementStatus == Measured for all K ticks
  AND NOT HeadBlockedByDeferred
```

This is a statement about the present, not a projection, which is why it can carry Sev2 without the trust problem that undermines predictive alerting. The three guards are mandatory: an empty queue trivially has an unchanging head; a deferred head is *expected* to be unchanging; and a stall inferred across `Indeterminate` gaps is guessed, not observed.

`ConsumptionStalled` is the **primary consumer-health signal**, replacing Event Grid's `ActiveMessagesAvailableWithNoListeners` in phase 1 ([OPN-013](#14-open-items-register)).

---

## 4. Functional requirements

### 4.1 Discovery

| ID | Requirement |
|---|---|
| **FR-001** | The monitor SHALL discover all queues and topic subscriptions in the configured namespace dynamically, via the ARM management plane. |
| **FR-002** | Discovery results SHALL be cached for `DiscoveryCacheTtlSeconds` (default 300), with jitter of ±10 % so regional deployments do not synchronise management-plane calls. |
| **FR-003** | A newly created entity SHALL be measured within `DiscoveryCacheTtlSeconds` of creation. |
| **FR-004** | On `MessagingEntityNotFoundException` during peek, the monitor SHALL evict the entity from cache, suppress its alerts, and emit `EntityDisappeared` (Sev4). |
| **FR-005** | Deletion SHALL be confirmed in two phases: if the next discovery refresh omits the entity, it is tombstoned and emission stops silently; if present, emit `EntityRecovered` and resume. |
| **FR-006** | Tombstones SHALL expire after `TombstoneTtlHours` (default 24), so a recreated entity of the same name is not permanently ignored. |
| **FR-007** | Every discovered, non-tombstoned entity SHALL emit exactly one record every tick, regardless of state or traffic. **Absence of a record never means healthy.** |
| **FR-008** | Discovery SHALL read and record `EnablePartitioning`, `ForwardTo`, `ForwardDeadLetteredMessagesTo`, `MaxDeliveryCount` and `DefaultMessageTimeToLive` per entity. |

### 4.2 Measurement

| ID | Requirement |
|---|---|
| **FR-010** | The monitor SHALL compute `AgeSeconds` per [§3.1](#31-definition-of-age). |
| **FR-011** | The monitor SHALL locate the oldest `Active` message via a bounded forward scan per [§3.2](#32-bounded-forward-scan), skipping `Deferred`, `Scheduled` and expired-but-not-collected messages. |
| **FR-012** | Scan depth SHALL be bounded by the effective budget; exhaustion without an Active message SHALL emit `HeadBlockedByDeferred` and no age. |
| **FR-013** | Resume points SHALL be invalidated per [§3.2](#32-bounded-forward-scan). |
| **FR-014** | The monitor SHALL apply clock-skew detection per [§3.3](#33-clock-trust). |
| **FR-015** | The monitor SHALL apply zero-result corroboration per [§3.5](#35-zero-result-ambiguity). |
| **FR-016** | The monitor SHALL compute `ConsumptionStalled` per [§3.7](#37-stall-detection). |
| **FR-017** | The monitor SHALL read entity runtime properties (`ActiveMessageCount`, `DeadLetterMessageCount`, `ScheduledMessageCount`, `TransferDeadLetterMessageCount`) each tick. |

### 4.3 Tick orchestration

| ID | Requirement |
|---|---|
| **FR-020** | The timer SHALL fire every 60 s (`0 */1 * * * *`) with `runOnStartup: false`. |
| **FR-021** | The function SHALL execute as a singleton; concurrent invocations within a region are prohibited. |
| **FR-022** | Entity scans SHALL fan out with concurrency bounded by `MaxConcurrentEntityScans` (default 8). |
| **FR-023** | Each tick SHALL enforce a hard wall-clock deadline of `TickDeadlineSeconds` (default 45) via `CancellationToken`. |
| **FR-024** | Each entity scan SHALL enforce `PerEntityTimeoutSeconds` (default 10). |
| **FR-025** | Entities unmeasured at deadline SHALL emit `NotMeasured` and be placed at the **head** of the next tick's ordering (fair carry-over — never LIFO, which would starve a consistently slow entity whose absence then reads as health). |
| **FR-026** | Deadline exhaustion SHALL emit `MonitorTickIncomplete { measured, skipped, deadlineHit }`. |
| **FR-027** | For the first `ColdStartTicks` (default 3) after process start, scan depth SHALL be capped at one batch per entity and entities staggered across those ticks, to prevent a platform-initiated recycle producing a namespace-wide burst. |

### 4.4 Thresholds and configuration

| ID | Requirement |
|---|---|
| **FR-030** | Per-entity thresholds SHALL be resolved from Azure App Configuration, with a sentinel key driving refresh. |
| **FR-031** | Resolution order SHALL be: entity override → class default → compiled default (300/900). |
| **FR-032** | Configuration refresh SHALL occur on a background cadence and SHALL NEVER block or be awaited inside the tick path. |
| **FR-033** | Threshold values SHALL be validated: `ThresholdMinSeconds` (30) ≤ sev2 < sev1 ≤ `ThresholdMaxSeconds` (86 400). Invalid values SHALL be rejected, logged as `ConfigRejected` (alertable), and the last-known-good retained. |
| **FR-034** | On App Configuration unavailability the monitor SHALL use last-known-good, then compiled defaults. It SHALL NOT fail to start. |
| **FR-035** | Every measurement record SHALL carry `Sev1ThresholdSeconds`, `Sev2ThresholdSeconds` and `ThresholdSource` ∈ { `AppConfig`, `LastKnownGood`, `CompiledDefault` }. |
| **FR-036** | `ThresholdSource != AppConfig` sustained beyond `StaleConfigMinutes` (default 15) SHALL raise Sev3. |
| **FR-037** | Entities SHALL carry a criticality class ∈ { `critical`, `standard`, `bulk` }, defaulting to `standard`. An unclassified entity SHALL emit `EntityUnclassified` (Sev4). |
| **FR-038** | Entities with `EntityRole = Forwarder` SHALL use `ForwarderAgeThresholdSeconds` (default 60) unless explicitly overridden. |

### 4.5 Degradation and self-interference

| ID | Requirement |
|---|---|
| **FR-040** | The monitor SHALL implement a graded degradation ladder responding to `ServiceBusException` with `Reason = ServiceBusy`, or sustained elevated latency. |
| **FR-041** | Levels SHALL be: `L0` normal; `L1` concurrency ÷2; `L2` concurrency ÷4 and depth ÷10; `L3` depth-only for `bulk`, single-batch peek retained for `standard` and `critical`. |
| **FR-042** | At no level SHALL a `critical`-class entity lose all peek measurement. A single peek batch is cheap enough that inability to perform one indicates namespace unavailability, which is a different alert. |
| **FR-043** | Recovery SHALL be hysteretic: one level per `RecoveryCleanTicks` (default 3) consecutive clean ticks. Instant recovery oscillates and is prohibited. |
| **FR-044** | Degradation level SHALL be emitted on every record and on the heartbeat. |

### 4.6 Emission

| ID | Requirement |
|---|---|
| **FR-045** | The monitor SHALL emit to Application Insights and to Log Analytics independently; neither failure SHALL prevent the other. |
| **FR-046** | Each sink SHALL retry with exponential backoff up to `EmitRetryCount` (default 3), **inside** the tick deadline. Functions-level retry SHALL NOT be used, because a retried invocation re-peeks the namespace. |
| **FR-047** | **Cross-witnessing:** a sink failure SHALL be recorded in the surviving sink and in `ILogger`. There SHALL be no failure mode in which a sink outage leaves no trace. |
| **FR-048** | Adaptive sampling SHALL be disabled for metric telemetry. Sampled-away metric points produce missing alert evaluations indistinguishable from healthy silence. |
| **FR-049** | `EnableCustomMetricsDimensions` SHALL be enabled. Without it, dimensions are stripped and the metric collapses to a namespace-wide average that would essentially never breach. |
| **FR-050** | The monitor SHALL NOT call any receive, complete, abandon, defer, dead-letter or renew-lock operation. This SHALL be enforced by an architecture test ([TST-070](#116-security-and-architecture)). |
| **FR-051** | Every record SHALL carry `SchemaVersion` and a deterministic `MeasurementId = SHA256(SourceRegion | ScheduledTickUtc | EntityPath)`, derived from the **scheduled** occurrence time, not execution time. |

### 4.7 Regional coordination

| ID | Requirement |
|---|---|
| **FR-060** | Exactly one region SHALL be the active measurer, coordinated by a blob lease on a ZRS storage account in Central US. |
| **FR-061** | Lease duration SHALL be `LeaseDurationSeconds` (default 60), renewed every `LeaseRenewSeconds` (default 20). |
| **FR-062** | The standby SHALL attempt acquisition once the lease is observed expired, and SHALL begin measuring immediately on acquisition. |
| **FR-063** | **Fail open, both directions.** If the holder cannot renew but is otherwise healthy, it SHALL continue measuring. If the standby cannot read the lease, it SHALL begin measuring. Duplicate emission is the safe outcome. |
| **FR-064** | On graceful shutdown (`IHostApplicationLifetime.ApplicationStopping`) the holder SHALL release the lease explicitly, bounded by `LeaseReleaseTimeoutSeconds` (default 5) so a hung release cannot block shutdown. |
| **FR-065** | `LeaseState { holder, acquiredAt, isHolder }` SHALL be emitted on every heartbeat. Takeover events and split-brain exceeding `SplitBrainAlertMinutes` (default 5) SHALL be alertable (Sev3). |
| **FR-066** | The non-holder SHALL still emit a heartbeat (with `isHolder = false`) so its liveness is observable. |

### 4.8 Entity configuration constraints

| ID | Requirement |
|---|---|
| **FR-070** | Partitioned entities are **prohibited** by platform standard. On a partitioned entity, `SequenceNumber` ordering is per-partition only, so a forward scan can report a low age while an older message sits at the head of another partition — a silent under-report. |
| **FR-071** | The monitor SHALL assert `EnablePartitioning == false` at discovery and emit `UnsupportedEntityConfiguration` (Sev3) if violated, independently of policy enforcement. |
| **FR-072** | A partitioned entity, if encountered, SHALL still be measured but flagged `AgeAccuracy = BestEffort`; its age is a **lower bound** and absence of breach is not evidence of health. |
| **FR-073** | Entities with `ForwardTo` set SHALL be flagged `EntityRole = Forwarder`, remain in age alerting under the tighter forwarder threshold, and additionally have `TransferDeadLetterMessageCount` monitored. |
| **FR-074** | Forwarding chains SHALL be recorded via `ForwardTarget`; cycles SHALL emit `ForwardingCycleDetected` (Sev3). |

---

## 5. Non-functional requirements

| ID | Requirement |
|---|---|
| <a id="nfr-001"></a>**NFR-001** | **The monitor SHALL NOT emit a healthy measurement it cannot substantiate.** Silence and `Indeterminate` are acceptable outcomes; a fabricated zero is not. |
| **NFR-002** | **Measurement and notification are separate concerns.** The function measures and emits truth; the alert layer decides what wakes someone. Suppression logic SHALL NOT be implemented inside the function — it is invisible, untestable in production, and cannot be reverted at 03:00. |
| **NFR-003** | Detection latency for an age breach SHALL be ≤ 90 s from breach (60 s sampling + metric alert evaluation), at `L0`. |
| **NFR-004** | A tick SHALL complete within `TickDeadlineSeconds`; sampling degradation SHALL always be visible via `MonitorTickIncomplete`, never silent. |
| **NFR-005** | Monitoring load SHALL remain below `MaxNamespaceMuSharePercent` (target 2 %) of namespace Messaging Units at P99. **Currently unquantified — see [OPN-002](#14-open-items-register).** |
| **NFR-006** | The monitor SHALL hold no standing permission beyond what peek and ARM enumeration require. See [§10.3](#103-identity-and-rbac). |
| **NFR-007** | All timestamps SHALL be UTC. No local time or DST handling appears anywhere. |
| **NFR-008** | The monitor SHALL be deployable to dev, staging and production from identical code, differing only by configuration. |
| **NFR-009** | Staging SHALL run the same 60 s cadence as production, so timing-sensitive logic (resume points, hysteresis counters, deadline handling) is exercised at production rates. |
| **NFR-010** | The monitor SHALL tolerate loss of all in-memory state at any instant, at a cost of at most one expensive tick. |
| <a id="nfr-011"></a>**NFR-011** | **Accepted limit:** during regional takeover, a detection blind window of up to `LeaseDurationSeconds` (60 s), plus one tick, is accepted. Graceful shutdown reduces this to approximately one tick for planned events. See [§2.2](#22-regional-topology). |
| **NFR-012** | **Accepted limit:** age is per-hop residence time, not end-to-end latency ([§3.4](#34-known-limitation-auto-forwarding)). |
| **NFR-013** | **Accepted limit:** DLQ message age is not measured; only depth ([OUT-002](#15-deferred-and-out-of-scope)). |
| **NFR-014** | Telemetry ingestion cost SHALL be bounded and predictable; it scales with entity count, not traffic volume. |
| **NFR-015** | `ServiceBusMessageAge_CL` SHALL retain 90 days interactive and 730 days total. |
| **NFR-016** | The monitor's own failure SHALL be detectable within `MonitorDeadDetectionMinutes` (default 5) by a mechanism that does not depend solely on the monitor's own emit path. |
| **NFR-017** | Schema changes SHALL be detected at deployment time, not at query time ([TST-060](#115-telemetry-and-schema)). |
| **NFR-018** | No business-identifying data SHALL be written to Log Analytics by default ([FR-045](#46-emission), `EmitRawMessageIdentifiers = false`). |
| **NFR-019** | The monitor SHALL log structured events with `TickId`, `EntityPath`, `SourceRegion` and `CorrelationId` on every scope. |
| <a id="nfr-020"></a>**NFR-020** | **Statelessness reconciliation.** "Stateless" (S-10) means: no durable state store, no Durable Functions, no shared state, and any instance can serve any tick. It does **not** mean the process holds no memory. Resume points, degradation level, stall counters, discovery cache and threshold cache are in-process, non-durable and advisory; all are lost on restart, and none participates in correctness. A developer must assume every cache may be cold on any tick. |

---

## 6. Component contracts

Namespace root: `Wmx.ServiceBus.AgeMonitor`.

### 6.1 Domain model

```csharp
public enum MessageState { Active, Deferred, Scheduled }

public enum MeasurementStatus
{
    Measured, Empty, Indeterminate, HeadBlockedByDeferred,
    Degraded, NotMeasured, Throttled, ClockAnomaly, EntityNotFound
}

public enum EntityKind { Queue, Subscription }
public enum EntityRole { Terminal, Forwarder }
public enum CriticalityClass { Critical, Standard, Bulk }
public enum ThresholdSource { AppConfig, LastKnownGood, CompiledDefault }
public enum AgeAccuracy { Exact, BestEffort }
public enum DegradationLevel { L0 = 0, L1 = 1, L2 = 2, L3 = 3 }

/// <summary>Broker metadata only. Never carries application properties or body.</summary>
public sealed record PeekedMessage(
    long SequenceNumber,
    DateTimeOffset EnqueuedTimeUtc,
    int DeliveryCount,
    MessageState State,
    TimeSpan TimeToLive,
    DateTimeOffset? ScheduledEnqueueTimeUtc,
    string? MessageIdHash,
    string? CorrelationIdHash);

public sealed record EntityDescriptor(
    string EntityPath,
    EntityKind Kind,
    EntityRole Role,
    string? ForwardTarget,
    bool IsPartitioned,
    int MaxDeliveryCount,
    TimeSpan DefaultTimeToLive,
    CriticalityClass Class);

public sealed record EntityRuntimeInfo(
    long ActiveMessageCount,
    long DeadLetterMessageCount,
    long ScheduledMessageCount,
    long TransferDeadLetterMessageCount,
    DateTimeOffset ReadAtUtc);

public sealed record ThresholdSet(
    int Sev2Seconds,
    int Sev1Seconds,
    ThresholdSource Source);

public sealed record MeasurementRecord(
    string MeasurementId,
    int SchemaVersion,
    DateTimeOffset TimeGenerated,
    string TickId,
    string SourceRegion,
    string Environment,
    string NamespaceName,
    EntityDescriptor Entity,
    MeasurementStatus Status,
    AgeAccuracy Accuracy,
    double? AgeSeconds,
    double? AgeDeltaSeconds,
    double? AgeBreachRatioSev1,
    double? AgeBreachRatioSev2,
    ThresholdSet Thresholds,
    EntityRuntimeInfo Runtime,
    long? OldestSequenceNumber,
    int? OldestDeliveryCount,
    string? OldestMessageIdHash,
    int StalledTickCount,
    bool ConsumptionStalled,
    DegradationLevel Degradation,
    string? MeasurementContext,   // e.g. "PostFailover"
    int ScanBatchesUsed,
    int ScanMessagesExamined);

public sealed record MonitorHeartbeat(
    string TickId,
    DateTimeOffset TimeGenerated,
    string SourceRegion,
    bool IsLeaseHolder,
    int DiscoveredEntities,
    int MeasuredEntities,
    int IndeterminateEntities,
    int SkippedEntities,
    int BlockedEntities,
    long TickDurationMs,
    DegradationLevel Degradation,
    ThresholdSource ConfigSource,
    bool AppInsightsEmitOk,
    bool LogsIngestionEmitOk);
```

### 6.2 Ports

```csharp
/// <summary>
/// The ONLY path to Service Bus message data. Deliberately exposes no receive
/// operation: this interface is the architectural guard behind FR-050.
/// </summary>
public interface IEntityPeeker
{
    Task<IReadOnlyList<PeekedMessage>> PeekAsync(
        string entityPath,
        long? fromSequenceNumber,
        int maxMessages,
        CancellationToken ct);
}

public interface IEntityRuntimeReader
{
    Task<EntityRuntimeInfo> GetRuntimeInfoAsync(string entityPath, CancellationToken ct);
}

public interface IEntityDiscoveryService
{
    Task<IReadOnlyList<EntityDescriptor>> GetEntitiesAsync(CancellationToken ct);
    void EvictOnNotFound(string entityPath);
    bool IsTombstoned(string entityPath);
}

public interface IThresholdProvider
{
    ThresholdSet Resolve(EntityDescriptor entity);
    ThresholdSource CurrentSource { get; }
}

public interface IOldestActiveMessageResolver
{
    Task<AgeResolution> ResolveAsync(
        EntityDescriptor entity,
        EntityRuntimeInfo runtime,
        int effectiveScanDepth,
        CancellationToken ct);
}

public sealed record AgeResolution(
    MeasurementStatus Status,
    PeekedMessage? OldestActive,
    int BatchesUsed,
    int MessagesExamined);

public interface IScanStateStore
{
    long? GetResumePoint(string entityPath);
    void SetResumePoint(string entityPath, long sequenceNumber);
    void Invalidate(string entityPath, string reason);

    long? GetPreviousHeadSequence(string entityPath);
    double? GetPreviousAgeSeconds(string entityPath);
    int GetStalledTickCount(string entityPath);
    void RecordObservation(string entityPath, long? headSequence, double? ageSeconds);
}

public interface IDegradationController
{
    DegradationLevel CurrentLevel { get; }
    int EffectiveConcurrency { get; }
    int EffectiveScanDepth(CriticalityClass entityClass);
    void RecordThrottle(string entityPath);
    void RecordCleanTick();
}

public interface ILeaseCoordinator
{
    Task<bool> TryAcquireOrRenewAsync(CancellationToken ct);
    Task ReleaseAsync(CancellationToken ct);
    bool IsHolder { get; }
    DateTimeOffset? AcquiredAtUtc { get; }
}

public interface ITelemetryEmitter
{
    Task<EmitOutcome> EmitAsync(
        IReadOnlyList<MeasurementRecord> records,
        MonitorHeartbeat heartbeat,
        CancellationToken ct);
}

public sealed record EmitOutcome(
    bool AppInsightsOk, bool LogsIngestionOk,
    int RecordsSent, string? AppInsightsError, string? LogsIngestionError);

public interface IMessageIdHasher
{
    string? Hash(string? value);   // SHA256 with per-environment salt; null-safe
}

public interface IClockSkewDetector
{
    ClockAssessment Assess(DateTimeOffset enqueuedTimeUtc, DateTimeOffset nowUtc);
}

public sealed record ClockAssessment(bool IsAnomalous, double AgeSeconds, string? Reason);
```

### 6.3 Orchestration

```csharp
public sealed class MessageAgeMonitorFunction
{
    [Function("MessageAgeMonitor")]
    public Task RunAsync(
        [TimerTrigger("%MonitorSchedule%", RunOnStartup = false)] TimerInfo timer,
        FunctionContext context,
        CancellationToken hostCancellation);
}

public interface IMonitorTickOrchestrator
{
    Task<TickResult> ExecuteTickAsync(DateTimeOffset scheduledTickUtc, CancellationToken ct);
}

public sealed record TickResult(
    MonitorHeartbeat Heartbeat,
    IReadOnlyList<MeasurementRecord> Records,
    EmitOutcome Emit);
```

**`TickId` derivation:** `ScheduledTickUtc.ToString("yyyyMMddTHHmmssZ")`. It must come from the timer's scheduled occurrence, not `UtcNow`, otherwise a retried invocation yields a different `MeasurementId` and the duplicate becomes undetectable — which is the case the key exists for.

---

## 7. Configuration surface

### 7.1 Application settings (deployment-time, Bicep)

| Key | Type | Default | Notes |
|---|---|---|---|
| `CFG-001` `ServiceBus__FullyQualifiedNamespace` | string | — | Required. Geo-replicated FQDN. |
| `CFG-002` `ServiceBus__SubscriptionId` | string | — | For ARM discovery. |
| `CFG-003` `ServiceBus__ResourceGroup` | string | — | For ARM discovery. |
| `CFG-004` `ServiceBus__NamespaceName` | string | — | For ARM discovery. |
| `CFG-005` `MonitorSchedule` | cron | `0 */1 * * * *` | 60 s cadence. |
| `CFG-006` `Monitor__SourceRegion` | string | — | `eastus2` \| `centralus`. |
| `CFG-007` `Monitor__Environment` | string | — | `dev` \| `staging` \| `prod`. |
| `CFG-008` `AppConfig__Endpoint` | uri | — | Azure App Configuration endpoint. |
| `CFG-009` `AppConfig__SentinelKey` | string | `asbmon:sentinel` | |
| `CFG-010` `AppConfig__RefreshIntervalSeconds` | int | `30` | Background only; never awaited in tick. |
| `CFG-011` `Lease__StorageAccountUri` | uri | — | ZRS account, Central US. |
| `CFG-012` `Lease__ContainerName` | string | `asbmon-lease` | |
| `CFG-013` `Lease__BlobName` | string | `primary` | |
| `CFG-014` `Telemetry__DceEndpoint` | uri | — | Data Collection Endpoint. |
| `CFG-015` `Telemetry__DcrImmutableId` | string | — | |
| `CFG-016` `Telemetry__MeasurementStream` | string | `Custom-ServiceBusMessageAge_CL` | |
| `CFG-017` `Telemetry__HealthStream` | string | `Custom-ServiceBusMonitorHealth_CL` | |
| `CFG-018` `Telemetry__MetricNamespace` | string | `WMX.ServiceBus` | |
| `CFG-019` `Hashing__SaltSecretName` | string | `asbmon-hash-salt` | Key Vault reference. |

### 7.2 Runtime configuration (Azure App Configuration, no redeploy)

Prefix: `asbmon:`. All values validated per [FR-033](#44-thresholds-and-configuration); invalid values are rejected with `ConfigRejected` and last-known-good retained.

| Key | Type | Default | Clamp | Notes |
|---|---|---|---|---|
| `CFG-100` `threshold:default:sev2` | int s | `300` | 30–86400 | Compiled fallback: 300. |
| `CFG-101` `threshold:default:sev1` | int s | `900` | > sev2, ≤ 86400 | Compiled fallback: 900. |
| `CFG-102` `threshold:class:critical:sev2` \| `:sev1` | int s | `60` / `180` | | |
| `CFG-103` `threshold:class:standard:sev2` \| `:sev1` | int s | `300` / `900` | | |
| `CFG-104` `threshold:class:bulk:sev2` \| `:sev1` | int s | `1800` / `3600` | | |
| `CFG-105` `threshold:entity:<entityPath>:sev2` \| `:sev1` | int s | — | | Highest precedence. |
| `CFG-106` `threshold:forwarder:sev2` \| `:sev1` | int s | `60` / `180` | | A healthy forwarder holds messages for milliseconds. |
| `CFG-107` `class:entity:<entityPath>` | enum | `standard` | | `critical` \| `standard` \| `bulk`. |
| `CFG-110` `scan:maxPeekScanDepth` | int | `5000` | 250–50000 | Messages per entity per tick at L0. |
| `CFG-111` `scan:peekBatchSize` | int | `250` | 50–250 | SDK maximum applies. |
| `CFG-112` `scan:resumePointMaxTicks` | int | `15` | 1–100 | Hard resume-point TTL. |
| `CFG-113` `scan:coldStartTicks` | int | `3` | 0–20 | Burst protection after restart. |
| `CFG-120` `tick:maxConcurrentEntityScans` | int | `8` | 1–64 | |
| `CFG-121` `tick:deadlineSeconds` | int | `45` | 10–55 | Must be < tick interval. |
| `CFG-122` `tick:perEntityTimeoutSeconds` | int | `10` | 2–30 | |
| `CFG-130` `discovery:cacheTtlSeconds` | int | `300` | 60–3600 | Bounds unmonitored window for new entities. |
| `CFG-131` `discovery:tombstoneTtlHours` | int | `24` | 1–168 | |
| `CFG-140` `degradation:recoveryCleanTicks` | int | `3` | 1–20 | Hysteresis. |
| `CFG-141` `degradation:throttleTicksToEscalate` | int | `2` | 1–10 | |
| `CFG-142` `degradation:maxNamespaceMuSharePercent` | double | `2.0` | | Target; see OPN-002. |
| `CFG-150` `corroboration:zeroCorroborationTicks` | int | `3` | 1–10 | Before trusting a zero. |
| `CFG-151` `corroboration:indeterminateTicksForAlert` | int | `3` | 1–20 | |
| `CFG-152` `stall:stalledTicksForAlert` | int | `5` | 2–30 | |
| `CFG-160` `clock:skewToleranceSeconds` | int | `2` | 1–60 | |
| `CFG-161` `clock:implausibleAgeSeconds` | int | `2592000` | | 30 days. |
| `CFG-162` `clock:anomalyTicksForAlert` | int | `5` | 1–50 | |
| `CFG-170` `lease:durationSeconds` | int | `60` | 15–60 | Azure blob lease maximum is 60. |
| `CFG-171` `lease:renewSeconds` | int | `20` | 5–30 | Must be < duration ÷ 2. |
| `CFG-172` `lease:releaseTimeoutSeconds` | int | `5` | 1–15 | |
| `CFG-173` `lease:splitBrainAlertMinutes` | int | `5` | 1–60 | |
| `CFG-180` `emit:retryCount` | int | `3` | 0–5 | Inside tick deadline. |
| `CFG-181` `emit:rawMessageIdentifiers` | bool | `false` | | `true` permitted in dev only. |
| `CFG-182` `emit:staleConfigMinutes` | int | `15` | 1–120 | |
| `CFG-190` `maintenance:maxSuppressionHours` | int | `4` | 1–72 | Guard, not a mechanism. |

### 7.3 Configuration governance

- App Configuration RBAC is scoped tightly; the set of principals able to write `asbmon:*` is the real control over alert silencing ([OPN-009](#14-open-items-register)).
- Every threshold in force is emitted with every measurement ([FR-035](#44-thresholds-and-configuration)), so `ServiceBusMessageAge_CL | where Sev2ThresholdSeconds > 3600` is a one-line audit of silenced entities — no App Configuration revision archaeology at 03:00.
- Changes are visible in App Configuration revision history and the Azure activity log.

---

## 8. Telemetry schema

### 8.1 Application Insights custom metrics

Namespace `WMX.ServiceBus`. Dimensions are deliberately minimal to bound time-series cardinality and custom-metric dimension billing.

| Metric | Dimensions | Emitted when |
|---|---|---|
| `TEL-001` `OldestMessageAgeSeconds` | `EntityPath`, `SourceRegion` | `Status ∈ {Measured, Empty, Degraded}` |
| `TEL-002` `AgeBreachRatioSev2` | `EntityPath`, `SourceRegion` | `Status ∈ {Measured, Degraded}` |
| `TEL-003` `AgeBreachRatioSev1` | `EntityPath`, `SourceRegion` | `Status ∈ {Measured, Degraded}` |
| `TEL-004` `ActiveMessageCount` | `EntityPath`, `SourceRegion` | always |
| `TEL-005` `DeadLetterMessageCount` | `EntityPath`, `SourceRegion` | always |
| `TEL-006` `TransferDeadLetterMessageCount` | `EntityPath`, `SourceRegion` | always |
| `TEL-007` `AgeDeltaSeconds` | `EntityPath`, `SourceRegion` | when prior observation exists |
| `TEL-008` `MonitorHeartbeat` | `SourceRegion`, `IsLeaseHolder` | every tick, unconditionally |

**Ratio emission rule.** When `MeasurementStatus ∉ {Measured, Degraded}`, **no ratio point is emitted at all** — never zero. A zero ratio reads as perfectly healthy and would actively mask a blind monitor. Absence is handled by the no-data rules; a fabricated zero is handled by nothing.

**Required host configuration:**
- Adaptive sampling disabled for metric telemetry ([FR-048](#46-emission)).
- `EnableCustomMetricsDimensions = true` ([FR-049](#46-emission)). Without it the entire alerting design silently fails while still producing plausible-looking telemetry.

### 8.2 `ServiceBusMessageAge_CL`

`SchemaVersion = 1`. Declared explicitly in the DCR; any field the function sends that the DCR does not declare is **silently dropped** — hence [TST-060](#115-telemetry-and-schema).

| Column | Type | Notes |
|---|---|---|
| `TimeGenerated` | datetime | Tick scheduled time, UTC |
| `MeasurementId` | string | `SHA256(SourceRegion \| ScheduledTickUtc \| EntityPath)` |
| `SchemaVersion` | int | |
| `TickId` | string | |
| `SourceRegion` | string | `eastus2` \| `centralus` |
| `Environment` | string | |
| `NamespaceName` | string | |
| `EntityPath` | string | `queue` or `topic/Subscriptions/sub` |
| `EntityKind` | string | `Queue` \| `Subscription` |
| `EntityRole` | string | `Terminal` \| `Forwarder` |
| `ForwardTarget` | string | nullable |
| `CriticalityClass` | string | |
| `MeasurementStatus` | string | [§3.6](#36-measurement-status) |
| `AgeAccuracy` | string | `Exact` \| `BestEffort` |
| `AgeSeconds` | real | **nullable** |
| `AgeDeltaSeconds` | real | nullable |
| `AgeBreachRatioSev1` | real | nullable |
| `AgeBreachRatioSev2` | real | nullable |
| `Sev1ThresholdSeconds` | int | effective threshold in force |
| `Sev2ThresholdSeconds` | int | effective threshold in force |
| `ThresholdSource` | string | `AppConfig` \| `LastKnownGood` \| `CompiledDefault` |
| `ActiveMessageCount` | long | |
| `DeadLetterMessageCount` | long | |
| `ScheduledMessageCount` | long | |
| `TransferDeadLetterMessageCount` | long | |
| `OldestSequenceNumber` | long | nullable |
| `OldestDeliveryCount` | int | nullable — poison-message signal |
| `OldestMessageIdHash` | string | nullable, salted SHA256 |
| `OldestCorrelationIdHash` | string | nullable, salted SHA256 |
| `StalledTickCount` | int | |
| `ConsumptionStalled` | bool | |
| `DegradationLevel` | int | 0–3 |
| `MeasurementContext` | string | nullable, e.g. `PostFailover` |
| `IsPartitioned` | bool | |
| `ScanBatchesUsed` | int | |
| `ScanMessagesExamined` | int | |

### 8.3 `ServiceBusMonitorHealth_CL`

Monitor-level events, separate table because the schema is unrelated to per-entity measurement.

| Column | Type | Notes |
|---|---|---|
| `TimeGenerated` | datetime | |
| `TickId` | string | |
| `SourceRegion` | string | |
| `Environment` | string | |
| `EventType` | string | see below |
| `Severity` | string | |
| `IsLeaseHolder` | bool | |
| `LeaseHolder` | string | nullable |
| `DiscoveredEntities` | int | |
| `MeasuredEntities` | int | |
| `IndeterminateEntities` | int | |
| `SkippedEntities` | int | |
| `BlockedEntities` | int | |
| `TickDurationMs` | long | |
| `DegradationLevel` | int | |
| `ConfigSource` | string | |
| `AppInsightsEmitOk` | bool | |
| `LogsIngestionEmitOk` | bool | |
| `EntityPath` | string | nullable, for entity-scoped events |
| `Detail` | string | nullable, free text |

**`EventType` values:** `Heartbeat`, `MonitorTickIncomplete`, `MonitorDegraded`, `ConfigRejected`, `ConfigStale`, `EmitFailure`, `LeaseAcquired`, `LeaseReleased`, `LeaseTakeover`, `SplitBrain`, `ClockAnomaly`, `EntityDisappeared`, `EntityRecovered`, `EntityUnclassified`, `UnsupportedEntityConfiguration`, `ForwardingCycleDetected`, `DepthReconciliationDivergence`.

### 8.4 `ServiceBusMessageAgeHourly_CL` (rollup)

Produced by an Azure Monitor **summary rule** over `ServiceBusMessageAge_CL`, retained 730 days. Small enough to keep indefinitely (≈ 80 entities × 24 × 365 ≈ 700 k rows/year) and the table SLA reporting queries.

| Column | Type |
|---|---|
| `TimeGenerated` | datetime (hour bucket) |
| `EntityPath`, `EntityKind`, `CriticalityClass`, `Environment`, `NamespaceName` | string |
| `AgeP50Seconds`, `AgeP95Seconds`, `AgeMaxSeconds` | real |
| `BreachMinutesSev1`, `BreachMinutesSev2` | int |
| `MeasuredTicks`, `IndeterminateTicks`, `BlockedTicks`, `NotMeasuredTicks` | int |
| `SuppressedMinutes` | int |
| `MaxActiveMessageCount`, `MaxDeadLetterMessageCount` | long |

`SuppressedMinutes` exists so SLA reporting can distinguish "we breached" from "we were inside an approved maintenance window" without reconstructing it from activity logs.

### 8.5 Retention and cost

| Table | Interactive | Total | Est. volume |
|---|---|---|---|
| `ServiceBusMessageAge_CL` | 90 d | 730 d (archive tier) | ≈ 115 k rows/day at 80 entities, single active region ≈ 2 GB/month |
| `ServiceBusMonitorHealth_CL` | 90 d | 730 d | ≈ 1.5 k rows/day |
| `ServiceBusMessageAgeHourly_CL` | 730 d | 730 d | ≈ 2 k rows/day |

Ingestion scales with **entity count, not traffic** ([FR-007](#41-discovery)) — a namespace of 80 mostly-idle entities costs the same as one under full load. This is the deliberate price of the liveness contract.

### 8.6 Deduplication

Duplicate rows can arise during split-brain or emit retry. Canonical query pattern:

```kusto
ServiceBusMessageAge_CL
| summarize arg_max(TimeGenerated, *) by MeasurementId
```

A saved function `ServiceBusMessageAgeDeduped()` SHALL be deployed wrapping this, and all alert and rollup queries SHALL use it.

---

## 9. Alert rule definitions

### 9.1 Fast path — metric alerts

Two rules total. Per-entity thresholds are handled by the emitted ratio, so a threshold change in App Configuration takes effect on the next sentinel refresh **without touching an alert rule**.

| ID | Rule | Condition | Severity | Eval / window |
|---|---|---|---|---|
| `ALR-001` | Message age Sev2 | `AgeBreachRatioSev2` max ≥ 1.0, split by `EntityPath` | Sev2 | 1 min / 1 min |
| `ALR-002` | Message age Sev1 | `AgeBreachRatioSev1` max ≥ 1.0, split by `EntityPath` | Sev1 | 1 min / 1 min |

**Payload requirement.** Both rules SHALL resolve absolute values into the notification. A responder woken by "ratio 1.4" learns nothing. Required fields: `EntityPath`, `AgeSeconds`, threshold in force, `ActiveMessageCount`, `DeadLetterMessageCount`, `ConsumptionStalled`, `OldestDeliveryCount`, `StalledTickCount`, `AgeDeltaSeconds`, projected time to Sev1, `SourceRegion`, deep links to the entity, the dashboard, and the runbook.

**Time-series cap.** Metric alert rules cap at approximately 5 000 time series. At 80 entities × 1 active region this is not near the limit, but capacity must be re-evaluated if entity count exceeds `MetricSeriesReviewThreshold` (1 000). See [OPN-014](#14-open-items-register).

### 9.2 Stateful path — scheduled query rules

All over `ServiceBusMessageAgeDeduped()` / `ServiceBusMonitorHealth_CL`.

| ID | Rule | Condition | Severity | Freq |
|---|---|---|---|---|
| `ALR-010` | Consumption stalled | `ConsumptionStalled == true` for any entity | Sev2 | 5 min |
| `ALR-011` | Head blocked by deferred | `MeasurementStatus == "HeadBlockedByDeferred"` sustained ≥ 15 min | Sev3 | 15 min |
| `ALR-012` | Sustained indeterminate | `MeasurementStatus == "Indeterminate"` ≥ `IndeterminateTicksForAlert` consecutive | Sev2 | 5 min |
| `ALR-013` | Tick incomplete | `EventType == "MonitorTickIncomplete"` in 3 of last 5 ticks | Sev3 | 5 min |
| `ALR-014` | Monitor degraded | `DegradationLevel >= 2` sustained ≥ 10 min | Sev3 | 5 min |
| `ALR-015` | Config stale | `ThresholdSource != "AppConfig"` sustained ≥ `StaleConfigMinutes` | Sev3 | 15 min |
| `ALR-016` | Config rejected | `EventType == "ConfigRejected"` | Sev2 | 5 min |
| `ALR-017` | Emit failure | `AppInsightsEmitOk == false` or `LogsIngestionEmitOk == false` sustained ≥ 5 min | Sev2 | 5 min |
| `ALR-018` | AI/LAW divergence | hourly row count per entity differs > 5 % between sinks | Sev3 | 60 min |
| `ALR-019` | Clock anomaly | `EventType == "ClockAnomaly"` ≥ `ClockAnomalyTicksForAlert` | Sev3 | 15 min |
| `ALR-020` | Lease takeover | `EventType == "LeaseTakeover"` | Sev3 | 5 min |
| `ALR-021` | Split brain | `EventType == "SplitBrain"` sustained ≥ `SplitBrainAlertMinutes` | Sev3 | 5 min |
| `ALR-022` | Transfer DLQ non-empty | `TransferDeadLetterMessageCount > 0` sustained ≥ 10 min | Sev2 | 5 min |
| `ALR-023` | Unsupported entity config | `EventType == "UnsupportedEntityConfiguration"` | Sev3 | 60 min |
| `ALR-024` | Entity unclassified | `EventType == "EntityUnclassified"` | Sev4 | 24 h |
| `ALR-025` | Depth reconciliation divergence | platform-metric depth vs peek-observed depth disagree ≥ 3 consecutive checks | Sev3 | 5 min |
| `ALR-026` | Suppression without end time | alert processing rule active with no end time | Sev3 | 60 min |
| `ALR-027` | Suppression over-long | any suppression active > `MaxSuppressionHours` | Sev3 | 60 min |
| `ALR-028` | Threshold drift | entity threshold differs from class baseline > 24 h | Sev4 | 24 h |

### 9.3 L4 — monitoring health

The watchdog must not depend solely on the path it watches.

| ID | Rule | Mechanism | Severity |
|---|---|---|---|
| `ALR-030` | Heartbeat absent | Metric alert on `MonitorHeartbeat`, **no-data behaviour explicitly configured to fire**, 5 min | Sev1 |
| `ALR-031` | Monitor running but blind | `MeasuredEntities == 0 && DiscoveredEntities > 0` for 3 consecutive heartbeats | Sev1 |
| `ALR-032` | No measurement rows | Scheduled query on `ServiceBusMessageAge_CL`, **fire-on-no-data explicitly enabled**, 10 min | Sev1 |
| `ALR-033` | Function not executing | Platform metric `FunctionExecutionCount` below expected for 5 min | Sev2 |
| `ALR-034` | Host exception rate | Platform exception metric above baseline | Sev3 |

> **Two configuration traps, both defaulting to the dangerous setting.** Azure Monitor **log alert rules do not fire on zero rows by default** — a query returning nothing is treated as "condition not met", so the alert is silent precisely when the data is gone. **Metric alerts have a separate no-data behaviour** that must be set to fire. Getting either wrong produces an L4 layer that passes review, looks correct in the portal, and never fires. [TST-050](#114-alerting) exists specifically to catch this.

### 9.4 Alert processing rules

| ID | Rule | Purpose |
|---|---|---|
| `ALR-040` | Group by namespace during failover | Correlates the post-promotion burst into one incident with N members rather than N pages. Age alerts still **fire** — they are true statements — but arrive grouped. |
| `ALR-041` | Cross-region duplicate grouping | Groups by `EntityPath` so a split-brain window produces one incident. |
| `ALR-042` | Maintenance suppression | Scheduled, **explicit end time mandatory**. Scoped to entity sets. Guarded by `ALR-026` / `ALR-027`. |
| `ALR-043` | Deployment suppression | Created and expired by the pipeline, **never by a human**. Scoped to `ALR-030`–`ALR-034` only — **never** to age alerts, because a deployment window is exactly when an unobserved backlog can develop. |

### 9.5 Failover behaviour

Age alerts are **never suppressed** during or after a namespace promotion. A post-failover backlog is the single most likely moment in this system's life for messages to genuinely accumulate; near-zero RPO means someone must confirm the backlog drained. The recovery burst consists of true statements and is handled by grouping ([ALR-040](#94-alert-processing-rules)), not silence.

Connection-level failures during a promotion window emit `Indeterminate` and do not page. Records inside the recovery window carry `MeasurementContext = PostFailover` — **enrichment, not suppression** — so the notification states that the condition coincided with a promotion.

Promotion detection cannot be inferred from connection failures alone; that is also what a network partition looks like. Geo-DR replication state is read out-of-band at low frequency as *corroborating* evidence, with the honest caveat that this metadata may be unavailable during the window it is needed.

### 9.6 Action groups and notification (assumed — [ASM-004](#13-assumptions))

| Severity | Destination | Environments |
|---|---|---|
| Sev1 | On-call paging + incident channel | prod only |
| Sev2 | On-call notification + ticket | prod only |
| Sev3 | Ticket queue + Teams channel | prod, staging |
| Sev4 | Teams channel / log only | all |

Non-production severities are **capped at Sev3** and never page. Dev is capped at Sev4. Identical rules deploy to all environments so the configuration path is exercised; only the action group differs.

**Until the runbook exists ([OPN-006](#14-open-items-register)), Sev1 SHOULD route to a ticket queue rather than paging.** A Sev1 with no defined action is a Sev2 that ruins someone's sleep.

---

## 10. Infrastructure, identity and deployment

### 10.1 Resources (Bicep, per region unless noted)

| Resource | Notes |
|---|---|
| Function App | Flex Consumption, .NET 10 isolated, `alwaysReady = 1`, `maximumInstanceCount` pinned low |
| Application Insights | Workspace-based; adaptive sampling disabled for metrics |
| Storage account (function) | Standard, per region |
| Storage account (lease) | **ZRS, Central US only, single instance** |
| Log Analytics workspace | Shared (assumed existing — [OPN-005](#14-open-items-register)) |
| Data Collection Endpoint | Shared |
| Data Collection Rule | **Deployed with the function**, so schema and code cannot drift |
| App Configuration store | Shared, with replica in the secondary region |
| Key Vault | Hash salt |
| Metric + query alert rules | Deployed from Bicep |
| Alert processing rules | Deployed from Bicep |
| Summary rule (rollup) | Deployed from Bicep |

### 10.2 Networking

Public endpoints with firewall restriction, parameterised so the posture can change without code changes.

> **Risk to resolve ([OPN-004](#14-open-items-register)).** Azure Functions is **not** covered by Service Bus's trusted-Microsoft-services exception on the data plane, and a Flex Consumption app's outbound IP set can change on plan or scale operations. An IP allow-list will therefore eventually break silently and present exactly as a Service Bus outage. Making it stable requires VNet integration plus a NAT Gateway with a static IP — most of the networking work the public posture was chosen to avoid.
>
> The alternative, and the design author's recommendation, is a permissive namespace firewall with **`disableLocalAuth: true`** and Entra ID + managed identity as the access control. With no SAS key to leak, network-layer restriction adds little. This requires a security decision.

`disableLocalAuth: true` is required on the Service Bus namespace regardless of posture.

### 10.3 Identity and RBAC

System-assigned managed identity per Function App.

| Purpose | Role | Scope | Notes |
|---|---|---|---|
| Entity discovery | **Reader** | Namespace | ARM plane only: `Microsoft.ServiceBus/namespaces/queues/read`, `.../topics/subscriptions/read`. Materially less privilege than `ServiceBusAdministrationClient`, which wants Data Owner. |
| Peek | **Azure Service Bus Data Receiver** | Namespace | Smallest built-in role permitting peek. |
| Lease | Storage Blob Data Contributor | Lease container | |
| Logs Ingestion | Monitoring Metrics Publisher | DCR | |
| App Configuration | App Configuration Data Reader | Store | |
| Key Vault | Key Vault Secrets User | Salt secret | |

**Over-privilege and its mitigation.** `Data Receiver` also grants receive and complete, so the monitoring identity holds standing permission to consume production messages across a Tier 1 namespace. Three controls:

1. `IEntityPeeker` is the **only** path to message data and exposes no receive operation ([§6.2](#62-ports)).
2. An architecture test fails the build if the assembly references any receive-family symbol ([TST-070](#116-security-and-architecture)). This converts intent into something CI enforces and survives careless refactoring.
3. A spike determines whether a peek-only custom role is expressible ([OPN-003](#14-open-items-register)). The author's assessment is that it is **not** — peek travels over the same AMQP receive link and no distinct data action is known — but that is not certain enough to assert in a buildable spec. If it is not expressible, control (2) *is* the control and requires security sign-off as an accepted risk.

### 10.4 Azure Policy

| Policy | Effect |
|---|---|
| `EnablePartitioning == false` on `Microsoft.ServiceBus/namespaces/queues` and `.../topics` | **Deny** |

Partitioning is immutable after entity creation, so create-time denial is the only enforcement point. The monitor asserts independently ([FR-071](#48-entity-configuration-constraints)) so a policy exemption cannot silently produce best-effort measurements presented as exact.

### 10.5 Deployment

OIDC-authenticated pipelines, Bicep, no secrets in the pipeline.

```
1. Build, unit test, architecture test
2. Deploy Bicep to STANDBY region (Central US)
3. Slot swap with warm-up (populates discovery cache pre-swap)
4. GATE: poll until heartbeat observed with
         MeasuredEntities > 0 AND DegradationLevel == 0
         → fail the release if not observed within GateTimeoutMinutes (5)
5. Force lease to the newly deployed region (optional, for verification)
6. Deploy Bicep to ACTIVE region (East US 2)
7. Repeat gate
8. Schema canary round-trip test (TST-060)
9. Expire deployment suppression (auto)
10. Write App Insights release annotation
```

**Gating on recovery rather than on deployment success is the point.** It is what stops "no-data alerts during deploys are normal" from becoming the habit that lets a genuinely failed deploy pass unnoticed — a failed deploy then *looks different* from a successful one.

Graceful shutdown releases the lease explicitly ([FR-064](#47-regional-coordination)), so a planned deploy costs roughly one tick rather than the full staleness timeout.

---

## 11. Test specification

### 11.1 Unit tests — measurement logic

All exercised through the port with plain records; no SDK types, no mocking framework gymnastics.

| ID | Case | Expected |
|---|---|---|
| `TST-001` | Single Active message, 400 s old | `Measured`, `AgeSeconds ≈ 400` |
| `TST-002` | Head is Deferred, Active at position 300 | `Measured`, age from the Active message |
| `TST-003` | All messages Deferred within budget | `HeadBlockedByDeferred`, `AgeSeconds == null` |
| `TST-004` | Scan budget exhausted, all Deferred | `HeadBlockedByDeferred`, `ScanBatchesUsed == depth/batch` |
| `TST-005` | Head is Scheduled with future `ScheduledEnqueueTimeUtc` | skipped |
| `TST-006` | Head expired (`EnqueuedTimeUtc + TTL < now`) | skipped |
| `TST-007` | Mixed Deferred/Scheduled/expired, one Active last | `Measured` |
| `TST-008` | Empty entity, prior tick also empty, no exceptions | `Empty`, `AgeSeconds == 0` |
| `TST-009` | Empty entity, prior tick depth 500, no drain evidence | `Indeterminate` |
| `TST-010` | Empty result with throttle exception this tick | `Indeterminate` |
| `TST-011` | Zero sustained 3 ticks, no exceptions | promotes to `Empty` |
| `TST-012` | `EnqueuedTimeUtc` in the future by 10 s | `ClockAnomaly`, age clamped to 0, alert suppressed |
| `TST-013` | Age exceeds `ImplausibleAgeSeconds` | `ClockAnomaly`, recorded, not alerted |
| `TST-014` | `MeasurementStatus != Measured` | no ratio points emitted (**not** zero) |

### 11.2 Unit tests — state machines

| ID | Case | Expected |
|---|---|---|
| `TST-020` | Resume point set, next tick starts from it | fewer batches used |
| `TST-021` | `ActiveMessageCount` decreases | resume point invalidated |
| `TST-022` | Entity observed empty | resume point invalidated |
| `TST-023` | `ResumePointMaxTicks` elapsed | resume point invalidated regardless |
| `TST-024` | Process restart | all resume points cold; one expensive tick, then normal |
| `TST-025` | Head `SequenceNumber` unchanged 5 ticks, depth > 0 | `ConsumptionStalled == true` |
| `TST-026` | Head unchanged 5 ticks, `ActiveMessageCount == 0` | `ConsumptionStalled == false` |
| `TST-027` | Head unchanged 5 ticks but `HeadBlockedByDeferred` | `ConsumptionStalled == false` |
| `TST-028` | Head unchanged with an `Indeterminate` tick in the window | counter reset, no stall |
| `TST-029` | Throttle observed | degradation escalates one level |
| `TST-030` | Clean ticks after degradation | recovers **one** level per 3 clean ticks, never instantly |
| `TST-031` | `L3` degradation, `critical`-class entity | retains a single peek batch |
| `TST-032` | `L3` degradation, `bulk`-class entity | depth-only, no peek |
| `TST-033` | Threshold out of clamp range | rejected, `ConfigRejected`, last-known-good retained |
| `TST-034` | App Configuration unreachable at cold start | compiled defaults, `ThresholdSource == CompiledDefault`, monitor starts |
| `TST-035` | Entity override present | overrides class default |
| `TST-036` | Forwarder with no explicit override | uses `ForwarderAgeThresholdSeconds` |
| `TST-037` | `MessagingEntityNotFound`, present at next discovery | `EntityRecovered`, resumes |
| `TST-038` | `MessagingEntityNotFound`, absent at next discovery | tombstoned, emission stops |
| `TST-039` | Tombstone older than `TombstoneTtlHours`, entity present | re-monitored |

### 11.3 Unit tests — orchestration

| ID | Case | Expected |
|---|---|---|
| `TST-040` | Entities exceed tick deadline | unmeasured → `NotMeasured`, `MonitorTickIncomplete` emitted |
| `TST-041` | Skipped entities on next tick | appear at the **head** of the ordering |
| `TST-042` | Repeated deadline overruns | no entity starved across 10 ticks |
| `TST-043` | Concurrency cap | never exceeds `MaxConcurrentEntityScans` in flight |
| `TST-044` | Single entity times out | other entities unaffected |
| `TST-045` | First 3 ticks after start | depth capped at one batch, entities staggered |
| `TST-046` | App Insights write fails | LAW still written; failure recorded in LAW and `ILogger` |
| `TST-047` | LAW write fails | App Insights still written; failure recorded in App Insights and `ILogger` |
| `TST-048` | Both sinks fail | `ILogger` records it; no exception escapes to fail the tick |
| `TST-049` | Retried invocation, same scheduled tick | identical `MeasurementId` |

### 11.4 Alerting

| ID | Case | Expected |
|---|---|---|
| `TST-050` | **Stop the function in staging** | `ALR-030` and `ALR-032` both fire within 5 min |
| `TST-051` | Function running, all entities `Indeterminate` | `ALR-031` fires (running but blind) |
| `TST-052` | Entity breaches its per-entity threshold | correct rule fires; payload carries absolute seconds and threshold |
| `TST-053` | Threshold changed in App Configuration | takes effect within one refresh interval, **no alert rule modified** |
| `TST-054` | Split-brain window | duplicate alerts group into one incident |
| `TST-055` | Maintenance suppression active | notification suppressed, measurement rows still written, dashboard shows amber |
| `TST-056` | Suppression rule created with no end time | `ALR-026` fires |

> `TST-050` is the single most important test in the suite and the easiest to skip. Both no-data behaviours default to *not firing*; a review will not catch it.

### 11.5 Telemetry and schema

| ID | Case | Expected |
|---|---|---|
| `TST-060` | **Schema canary round-trip** — write a row populated in every declared field, read back after ingestion | any null field fails the deployment |
| `TST-061` | Field sent but not declared in DCR | detected by `TST-060` (it would otherwise be silently dropped) |
| `TST-062` | `EmitRawMessageIdentifiers == false` | `MessageIdHash` populated, raw `MessageId` absent from all output |
| `TST-063` | Same `MessageId`, two ticks | identical hash (correlation works without disclosure) |
| `TST-064` | Custom metric dimensions | `EntityPath` present in App Insights (guards the `EnableCustomMetricsDimensions` trap) |
| `TST-065` | Sampling configuration | no metric telemetry sampled away under sustained emission |

### 11.6 Security and architecture

| ID | Case | Expected |
|---|---|---|
| `TST-070` | **Assembly scan** | no reference to `ReceiveMessagesAsync`, `CompleteMessageAsync`, `AbandonMessageAsync`, `DeadLetterMessageAsync`, `DeferMessageAsync`, `RenewMessageLockAsync`. Build fails on violation. |
| `TST-071` | `IEntityPeeker` surface | exposes no receive-family method |
| `TST-072` | Managed identity roles | assigned roles match [§10.3](#103-identity-and-rbac) exactly; no Data Owner |

### 11.7 Integration — Service Bus emulator

| ID | Case |
|---|---|
| `TST-080` | Adapter maps SDK message to `PeekedMessage` correctly |
| `TST-081` | Deferred messages appear in peek with distinguishable `State` |
| `TST-082` | Scheduled messages appear with `ScheduledEnqueueTimeUtc` |
| `TST-083` | `fromSequenceNumber` resumes at the expected position |
| `TST-084` | `MessagingEntityNotFoundException` surfaces as `EntityNotFound` |
| `TST-085` | Empty entity peek returns an empty list, not an exception |

### 11.8 Contract tests — real non-production namespace (nightly)

The emulator only approximates peek semantics, and the entire design rests on them.

| ID | Assertion | Why |
|---|---|---|
| `TST-090` | **Peek does not increment `DeliveryCount` and does not lock the message** | If untrue, the monitor silently mutates production message state across every entity in a Tier 1 namespace. Five-line test; "confident" is the wrong standard for that blast radius. |
| `TST-091` | `fromSequenceNumber` semantics hold in the presence of deferred and scheduled messages | The resume-point design depends on it |
| `TST-092` | Deferred/scheduled `State` is reliably distinguishable | The skip logic depends on it |
| `TST-093` | Runtime properties on a genuinely empty entity vs a throttled one | The corroboration logic is built on the difference — observe it, don't assume it |
| `TST-094` | Peek on a subscription with `ForwardTo` returns empty | Confirms the forwarder model |

### 11.9 Staging synthetic scenario generator (deliverable)

A separate utility — **explicitly not deployed to production** — that provisions dedicated test entities under a reserved name prefix in the staging namespace, so it can never touch real staging traffic. Without it, none of the interesting logic is executed before production.

| ID | Scenario | Asserted telemetry |
|---|---|---|
| `TST-100` | Message aged past Sev2, then Sev1 | `ALR-001` then `ALR-002` fire with correct payload |
| `TST-101` | Deferred head deeper than the scan budget, Active behind it | `HeadBlockedByDeferred`, then `Measured` once the resume point advances |
| `TST-102` | Entity deleted mid-scan | `EntityDisappeared`, tombstone after next discovery |
| `TST-103` | Entity created | measured within `DiscoveryCacheTtlSeconds` |
| `TST-104` | Burst sized to induce `ServerBusy` | degradation ladder escalates, then recovers hysteretically |
| `TST-105` | Forced clock skew | `ClockAnomaly` |
| `TST-106` | DCR misconfigured deliberately | `ALR-017` fires; App Insights unaffected |
| `TST-107` | Active region stopped | standby takes over within `LeaseDurationSeconds` + 1 tick |
| `TST-108` | Both regions running (forced split-brain) | duplicates detectable by `MeasurementId`; `ALR-021` fires |
| `TST-109` | Consumer stopped, messages arriving | `ConsumptionStalled` within 5 ticks |
| `TST-110` | Forwarding destination deleted | transfer DLQ populates; `ALR-022` fires |

---

## 12. Runbook requirements

The runbook does not exist today and is a **required deliverable of this work** ([OPN-006](#14-open-items-register)). Its decision branches determine the alert payload, so it is specified here rather than left to the responder.

### 12.1 Required document

`docs/runbooks/service-bus-message-age.md`, containing for each branch: the distinguishing evidence, the immediate action, the escalation target, and the escalation threshold.

### 12.2 Decision branches

| Evidence in the payload | Conclusion | First action |
|---|---|---|
| `ConsumptionStalled == true`, age rising ≈ 1 s/s | Consumer is not processing at all | Check consumer function health; restart or escalate to the consumer app team |
| Age rising, `ConsumptionStalled == false`, `ActiveMessageCount` rising | Consumer alive but outpaced, or downstream slow | Check L2 dequeue latency and Maximo responsiveness |
| Age high and flat, `OldestDeliveryCount` near `MaxDeliveryCount` | Poison message at the head | Identify by `OldestSequenceNumber`; escalate for disposition |
| `MeasurementStatus == HeadBlockedByDeferred` sustained | Application-level defer leak — messages deferred and never resumed | Escalate to the consumer app team; this is a code defect, not an infrastructure issue |
| `MeasurementStatus == Indeterminate` sustained | Monitoring or platform impairment, **not** an entity problem | Treat as a monitor incident; check Service Bus health and throttling |
| `MeasurementContext == PostFailover` | Backlog coincides with a namespace promotion | Verify drain within the recovery window before escalating |
| `TransferDeadLetterMessageCount > 0` | Auto-forwarding is failing | Check destination entity existence, quota and status |
| `DegradationLevel >= 2` | Monitor is shedding load; measurements may under-report | Treat ages as lower bounds |

### 12.3 Payload contract

Every Sev1/Sev2 notification must let a responder reach a branch **without opening the portal**: entity, region, namespace; age now and the trend; thresholds in force; active/dead-letter/transfer-DLQ counts; `ConsumptionStalled` and `StalledTickCount`; `OldestSequenceNumber` and `OldestDeliveryCount`; `MeasurementStatus`, `DegradationLevel`, `MeasurementContext`; projected time to Sev1; deep links to entity, dashboard and runbook.

### 12.4 Business-impact mapping

Each entity requires a plain-language impact statement for the notification body ("outage callback notifications to customers may be delayed"). This mapping must come from the integration owner and cannot be inferred from the platform ([OPN-007](#14-open-items-register)). It lives alongside criticality class in App Configuration under `asbmon:impact:entity:<entityPath>`.

---

## 13. Assumptions

Assumptions are working defaults chosen where a decision was needed to make the spec buildable. Each is falsifiable and should be confirmed.

| ID | Assumption |
|---|---|
| `ASM-001` | Entity cardinality is 40–80 queues and subscriptions per environment, growing toward ~200 as BizTalk migration completes. All budget figures derive from this ([OPN-001](#14-open-items-register)). |
| `ASM-002` | BizTalk and WMX running in parallel during migration does **not** increase Service Bus entity count, because BizTalk does not use Service Bus. Migration increases entity count monotonically as integrations move. |
| `ASM-003` | Structured logging conventions: `ILogger` with scopes carrying `TickId`, `EntityPath`, `SourceRegion`, `CorrelationId`; `Information` for tick summary, `Warning` for degradation and per-entity failure, `Error` for emit failure and unhandled exceptions. No per-message `Debug` logging in production. |
| `ASM-004` | Action group composition per [§9.6](#96-action-groups-and-notification-assumed--asm-004). |
| `ASM-005` | RACI: WMX platform team is Responsible and Accountable for the monitor and its alert rules; consumer app teams are Consulted on per-entity thresholds and criticality class; the monitoring/NOC function is Informed. |
| `ASM-006` | The Log Analytics workspace, DCE and action groups already exist as shared WMX resources and are referenced, not created ([OPN-005](#14-open-items-register)). |
| `ASM-007` | Metric alert time-series limits are not approached at current cardinality; re-evaluate above 1 000 entities ([OPN-014](#14-open-items-register)). |
| `ASM-008` | Interactive retention of 90 days is sufficient for operational investigation; SLA questions are served by the rollup table. |
| `ASM-009` | Hourly rollup granularity is sufficient for SLA reporting. If sub-hourly breach accounting is required, the summary rule cadence changes but the schema does not. |
| `ASM-010` | Dev environment runs at a relaxed cadence (300 s) to reduce cost; staging runs the production 60 s cadence ([NFR-009](#5-non-functional-requirements)). |

---

## 14. Open items register

Items the requirement owner was uncertain about, or that require verification. **These are not resolved in this spec.**

| ID | Item | Impact if wrong | Owner | Blocking? |
|---|---|---|---|---|
| `OPN-001` | Actual entity cardinality per environment | Every budget number: concurrency, deadline, ingestion cost, metric series | WMX platform | No — defaults hold to ~200 entities |
| `OPN-002` | **MU cost of a full-budget tick is unquantified.** Requires a load test against realistic entity counts and depths, with acceptance criterion "monitoring ≤ 2 % of namespace MU at P99" | Degradation thresholds in [§7.2](#72-runtime-configuration-azure-app-configuration-no-redeploy) are placeholders; the self-interference feedback loop is unbounded in theory | WMX platform | **Yes — pre-production gate** |
| `OPN-003` | Whether a peek-only custom RBAC role is expressible. Author's assessment: **not** expressible | If not, the code guard ([TST-070](#116-security-and-architecture)) is the sole control and needs security sign-off as an accepted risk | Security + WMX | No — fallback defined |
| `OPN-004` | **Namespace firewall posture.** IP allow-listing requires deterministic egress (VNet + NAT Gateway); the alternative is a permissive firewall with `disableLocalAuth` and Entra-only access. See [§10.2](#102-networking) | An IP allow-list that drifts fails silently and presents as a Service Bus outage | Security | **Yes — changes Bicep** |
| `OPN-005` | Which shared resources are referenced vs created: App Configuration store, Log Analytics workspace, DCE, DCR, action groups | Changes Bicep from `create` to `existing` throughout | WMX platform | **Yes** |
| `OPN-006` | **Runbook ownership and authorship.** Confirmed as a deliverable of this work; author and reviewer not assigned. Also unresolved: whether Sev1 pages before the runbook exists (recommendation: no) | Sev1 with no defined action | WMX platform | No — spec defines required content |
| `OPN-007` | Business-impact mapping per entity | Alert text cannot be written; NOC cannot triage | Integration owner | No |
| `OPN-008` | Criticality class assignment process, and the approver for new entities | Degradation ladder sheds the wrong entities; default is `standard` | WMX platform | No |
| `OPN-009` | RBAC approver set for `asbmon:*` in App Configuration | The no-redeploy design rests entirely on this being tightly scoped | Security | No |
| `OPN-010` | **Whether the L2 consumer-side dequeue-latency layer exists yet.** Two runbook branches depend on it | Those branches degrade to "escalate and investigate" | WMX platform | No |
| `OPN-011` | The SLA target itself. Rollup schema is generic (breach-minutes) and can serve most definitions, but the definition is unknown | Rollup may need recomputation from raw within the 90-day window | Business + WMX | No |
| `OPN-012` | Whether the L3 backstop observes the **transfer** DLQ (`$Transfer/$DeadLetterQueue`) as distinct from the normal DLQ. A rule written against the normal DLQ will not see auto-forward failures | Auto-forward failures may be unmonitored by both L1 and L3 | WMX platform | No — `ALR-022` covers it independently |
| `OPN-013` | **Whether Event Grid enrichment is phase 1 or phase 2.** This spec defers it, using `ConsumptionStalled` as the primary consumer-health signal. This **deviates from settled decision S-03's implication** that Event Grid appears as enrichment. Rationale: a fail-silent source in an alert payload is worse than an absent field, because "NoListeners: false" cannot be distinguished from "Event Grid stopped delivering three weeks ago" | If phase 1 is required, adds a table, a DCR stream, an Event Grid subscription, an Event-Grid-health check, and a staleness field in the payload | Requirement owner | **Yes — schema impact** |
| `OPN-014` | Metric alert time-series capacity as entity count grows past ~1 000 | Metric alerting silently stops covering some entities | WMX platform | No |
| `OPN-015` | Whether receiver/listener count is readable via a supported SDK or ARM surface. If it is, it is strictly better than `ConsumptionStalled` inference | Would simplify the consumer-health signal | WMX platform | No |
| `OPN-016` | **.NET 10 isolated worker support on Flex Consumption**, and whether the timer trigger's singleton lock holds under Flex's scaling model | Fallback is EP1 pinned to one instance — Bicep changes, code does not | WMX platform | **Yes — verify at build start** |

---

## 15. Deferred and out of scope

| ID | Item | Rationale | Residual risk |
|---|---|---|---|
| `OUT-001` | **End-to-end age** from a producer-stamped origin timestamp (`EndToEndAgeSeconds`) | Requires a producer contract that does not exist, and would put alerting at the mercy of producer compliance | Per-hop measurement does not answer end-to-end SLA questions. Prerequisite for phase 2: a WMX producer contract mandating an origin timestamp |
| `OUT-002` | **DLQ message age** | Scoped out in favour of DLQ depth via L3. Would roughly double entity count, scan budget and telemetry for a signal whose thresholds are measured in days | **A dead-letter queue whose contents are never triaged is invisible to this system.** A DLQ holding a steady 12 messages reads identically whether they arrived this morning or last quarter. Candidate enhancement: a stagnant-depth KQL rule (`min(depth) == max(depth) over 7d and depth > 0`) at zero scan cost |
| `OUT-003` | Event Grid enrichment | See [OPN-013](#14-open-items-register) | Cannot distinguish "zero receivers attached" from "receivers attached but not completing". Assessed as a distinction without a difference at 03:00 — both mean nothing is being consumed and the responder's next step is identical |
| `OUT-004` | External synthetic canary outside Azure Monitor | Deliberate exclusion, not an oversight. At RTO < 1 hour, a third independent monitoring system costs more than the residual risk | A whole-subscription or Azure-Monitor-wide failure would blind L4 |
| `OUT-005` | Automatic remediation (resubmit, drain, dead-letter) | The monitor holds no remediation mandate and its identity guard ([FR-050](#46-emission)) forbids it | Remediation remains manual, per runbook |
| `OUT-006` | Session-enabled entity support | No session-enabled entities exist (S-01) | Would require per-session-state scanning; the forward-scan model does not generalise |
| `OUT-007` | Partitioned entity support | Prohibited by platform standard ([FR-070](#48-entity-configuration-constraints)) | If a partitioned entity appears, its age is a lower bound flagged `BestEffort`; absence of breach is not evidence of health |

---

## Appendix A — Design principles

Five principles decided multiple questions during specification. Where a future change conflicts with one of these, the principle should win or be explicitly overturned.

1. **The monitor must never emit a healthy measurement it cannot substantiate.** Silence and `Indeterminate` are acceptable; a fabricated zero is not. *(Decides: zero-result corroboration, ratio suppression, absence-of-age handling.)*
2. **The monitor measures; the alert layer decides what wakes someone.** Suppression inside the function is invisible, untestable in production, and cannot be reverted at 03:00. Suppression in an alert processing rule is a portal toggle with an audit trail. *(Decides: failover behaviour, maintenance windows, cross-region duplication.)*
3. **Degradation must always be visible.** Any design where reduced coverage is silent fails L4's purpose, regardless of how much cost it saves. *(Decides: tick deadline over sharding, `MonitorTickIncomplete`, `MeasurementStatus`, `ThresholdSource`.)*
4. **Alerting must never depend on producer compliance.** Anything a producer can forget to do, a producer will eventually forget to do. *(Decides: broker `EnqueuedTimeUtc` over stamped origin timestamps, end-to-end age deferral.)*
5. **The monitor should have fewer dependencies than the thing it monitors,** and every dependency it does have should fail toward more monitoring rather than less. *(Decides: App Configuration fallback chain, lease fail-open semantics, Event Grid exclusion.)*
