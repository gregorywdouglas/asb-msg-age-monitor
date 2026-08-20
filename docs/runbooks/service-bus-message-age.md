# Runbook — Service Bus message age (EIE L1)

**Component:** `asb-msg-age-monitor`
**Spec:** [docs/specs/asb-msg-age-monitor.md](../specs/asb-msg-age-monitor.md) §12
**Tier:** 1 — RTO < 1 hour, RPO near-zero
**Owner:** EIE platform team

> **Status: this runbook is a required deliverable that is not yet signed off (OPN-006).**
> Until it is reviewed and an on-call rotation is confirmed, **Sev1 should route to a
> ticket queue rather than paging.** A Sev1 with no agreed action is a Sev2 that ruins
> someone's sleep.

---

## 1. What this alert means

A message has been sitting in a Service Bus queue or topic subscription longer than the
threshold configured for that entity. The monitor peeks the broker every 60 seconds and
reports the age of the oldest **Active** message.

**It does not mean** the total end-to-end latency is that value. Age is measured per
hop: auto-forwarding resets the broker's enqueue timestamp, so a delay upstream of this
entity is reported at the hop where it occurred, not here (spec §3.4, NFR-012).

---

## 2. Read the payload first

Every Sev1/Sev2 notification is built to let you reach a conclusion **without opening the
portal**. Before anything else, read these fields:

| Field | What it tells you |
|---|---|
| `EntityPath` | Which queue or subscription |
| `AgeSeconds` | How long the oldest message has waited, in seconds |
| `Sev1ThresholdSeconds` / `Sev2ThresholdSeconds` | The threshold actually in force for this entity |
| `AgeDeltaSeconds` | Rising, flat, or draining since the last tick |
| `ConsumptionStalled` / `StalledTickCount` | Whether the head message has moved at all |
| `ActiveMessageCount` | Depth — is this one stuck message or a backlog |
| `OldestDeliveryCount` | Near `MaxDeliveryCount` means a poison message |
| `MeasurementStatus` | Whether this is a real measurement or a monitor problem |
| `DegradationLevel` | Above 0, the age is a **lower bound**, not a fact |
| `MeasurementContext` | `PostFailover` means a namespace promotion happened recently |
| `TransferDeadLetterMessageCount` | Auto-forwarding is failing |

---

## 3. Decision branches

Work down this table. The first row that matches is your branch.

### 3.1 `MeasurementStatus` is not `Measured` or `Degraded`

**This is a monitor incident, not an entity incident.** Do not escalate to the
application team. Go to [§4](#4-monitor-incidents).

### 3.2 `ConsumptionStalled == true`, age rising ≈ 1s per second

**Conclusion:** nothing is consuming this entity.

The head message has not moved for at least `StalledTicksForAlert` consecutive measured
ticks while the queue holds messages. This is an observation, not an inference.

**Actions:**
1. Check the consumer function/app health for this entity.
2. If the consumer is down, restart it, then confirm `AgeDeltaSeconds` goes negative.
3. If the consumer is up but not processing, escalate to the consumer application team
   with the `EntityPath` and `OldestSequenceNumber`.

**Escalate if:** age is still rising 10 minutes after the consumer is confirmed healthy.

### 3.3 Age rising, `ConsumptionStalled == false`, `ActiveMessageCount` rising

**Conclusion:** the consumer is alive but outpaced, or something downstream is slow.

Messages *are* moving — the head advances between ticks — but arrivals exceed
departures.

**Actions:**
1. Check the L2 consumer-side dequeue latency for this entity.
   *(If L2 is not yet deployed — OPN-010 — this branch degrades to step 2.)*
2. Check Maximo responsiveness for the integration this entity feeds.
3. If Maximo is slow, this is a downstream incident; escalate to the Maximo team.
4. If Maximo is healthy, escalate to the consumer application team as a throughput issue.

**Escalate if:** the backlog is still growing after 15 minutes.

### 3.4 Age high and flat, `OldestDeliveryCount` near `MaxDeliveryCount`

**Conclusion:** a poison message is stuck at the head.

**Actions:**
1. Record `OldestSequenceNumber` — that is how the message is located. `MessageId` is
   hashed in telemetry by default and is not reversible.
2. Escalate to the consumer application team for disposition.
3. Do **not** manually receive or drain the message. Manual data-plane operations break
   ordering and at-least-once guarantees and are not authorised by this runbook.

### 3.5 `MeasurementStatus == HeadBlockedByDeferred`, sustained

**Conclusion:** an application-level defer leak — messages are being deferred and never
resumed. This is a code defect, not an infrastructure problem.

The monitor exhausted its scan budget without finding an Active message, so no age could
be established for this entity. That is a **coverage gap**, not health.

**Actions:**
1. Escalate to the consumer application team.
2. Note that `ConsumptionStalled` will be `false` here by design — a deferred head is
   *expected* to be unchanging, so treating it as a stall would be a false positive.

### 3.6 `MeasurementContext` contains `PostFailover`

**Conclusion:** the backlog coincides with a namespace promotion.

Age alerts are deliberately **not** suppressed during or after a failover: a post-failover
backlog is the most likely moment in this system's life for messages to genuinely
accumulate, and near-zero RPO means someone must confirm the backlog drained.

**Actions:**
1. Confirm the promotion in the Service Bus geo-DR status.
2. Watch `AgeDeltaSeconds` — it should go negative within a few minutes as consumers
   reconnect.
3. **Escalate normally** if the backlog is still growing after 10 minutes. Do not dismiss
   it as failover noise.

### 3.7 `TransferDeadLetterMessageCount > 0` (ALR-022)

**Conclusion:** auto-forwarding is failing. This is the transfer dead-letter queue, which
is a **different entity** from the normal DLQ and is not covered by the L3 backstop
(OPN-012).

**Actions:**
1. Check that `ForwardTarget` still exists and is not disabled or over quota.
2. If the destination was deleted, restore it or clear `ForwardTo` on the source.

### 3.8 `DegradationLevel >= 2`

**Caveat, not a branch.** The monitor is shedding load to protect the namespace it
observes. Reported ages are **lower bounds** — the real oldest message may be older.
Treat any age you see as a floor, and expect `MeasurementStatus == Degraded`.

---

## 4. Monitor incidents

These mean the monitoring is impaired. The entity may be perfectly healthy — or you may
simply have no idea, which is the point.

| Signal | Meaning | Action |
|---|---|---|
| `Indeterminate` sustained (ALR-012) | The monitor cannot tell an empty entity from a throttled one. It refuses to report health it cannot substantiate. | Check Service Bus namespace health and throttling. Treat affected entities as **unknown**, not healthy. |
| `MonitorTickIncomplete` (ALR-013) | Ticks are not finishing inside the deadline; the effective sampling interval has degraded. | Check entity count growth and `TickDurationMs`. Consider raising `MaxConcurrentEntityScans`. |
| `ConfigStale` (ALR-015) | Thresholds in force did not come from App Configuration. | Check App Configuration availability and the sentinel key. Thresholds are stale but valid; detection still works. |
| `ConfigRejected` (ALR-016) | Someone set a threshold outside the clamp range. Last-known-good is in force. | Read `Detail` for the offending key. Fix in App Configuration. |
| `EmitFailure` (ALR-017) | A telemetry sink is failing. The analytical and alerting records have diverged. | The surviving sink records the failure. If Application Insights is the failed sink, **alerting is impaired** — treat as urgent. |
| `ClockAnomaly` (ALR-019) | Message ages are implausible. Clock trust is broken. | All ages from the affected region are suspect. Check host time sync. |
| `LeaseTakeover` (ALR-020) | Measurement moved between regions. | Expect a detection gap of up to `LeaseDurationSeconds` plus one tick (NFR-011, accepted). |
| `SplitBrain` (ALR-021) | Both regions are measuring. | Harmless — duplicate rows, deduped by `MeasurementId`. Check lease-store reachability. |
| `UnsupportedEntityConfiguration` (ALR-023) | A partitioned entity exists. | Its age is a **lower bound**; absence of breach is not evidence of health. Escalate to have it recreated non-partitioned. |
| ALR-030 / ALR-032 | **The monitor is dead.** No heartbeat, no rows. | Highest priority. Message-age detection is entirely dark. Check the function app in both regions. |
| ALR-031 | The monitor is running and emitting, but measured nothing. | Different incident from a dead monitor. Usually credentials or namespace reachability. |

---

## 5. Things this runbook does not authorise

- Manually receiving, completing, abandoning or dead-lettering messages. The monitoring
  identity cannot do this by design (FR-050, TST-070), and neither should a responder
  acting on this alert alone.
- Disabling an alert rule to stop the noise. Use a **time-boxed** alert processing rule
  with a mandatory end time; ALR-026 and ALR-027 exist to catch suppressions that were
  left switched on.
- Raising a threshold in App Configuration during an incident to silence a page, without
  recording why. ALR-028 will flag the drift, and every record carries the threshold that
  was in force, so the change is visible in the data.

---

## 6. Open items affecting this runbook

| Item | Effect |
|---|---|
| OPN-006 | This runbook is unreviewed and no on-call rotation is confirmed. Sev1 should not page until it is. |
| OPN-007 | Per-entity business-impact statements are missing, so alert text cannot say what actually breaks. Branches still work; the escalation target may not be obvious. |
| OPN-010 | If the L2 dequeue-latency layer is not deployed, §3.3 cannot distinguish "consumer slow" from "downstream slow" and degrades to escalate-and-investigate. |
| OPN-012 | Whether L3 observes the transfer DLQ is unconfirmed. §3.7 relies on ALR-022 covering it independently. |
| ALR-040 | Azure alert processing rules cannot express correlation grouping declaratively. During a namespace failover, expect **N separate alerts** rather than one grouped incident. Treat a burst that shares a timestamp and `MeasurementContext=PostFailover` as one event. |
