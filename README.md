# asb-msg-age-monitor

Timer-triggered .NET 10 isolated-worker Azure Function that detects when a message in
any Azure Service Bus queue or topic subscription has aged past a threshold, and emits
that as alertable telemetry.

Built for **EIE**, the Azure Integration Services middleware layer replacing BizTalk
Server 2020 for IBM Maximo integration. Tier 1: RTO < 1 hour, RPO near-zero.

## Why this exists

Azure Monitor has no native message-age metric for Service Bus, no per-message timestamp
in any resource log, and no settlement timestamp anywhere in the platform. Queue depth is
a poor proxy — a queue holding a steady twelve messages looks identical whether those
arrived seconds ago or have been stuck for a week. Broker-side peek is the only source of
truth for how long the oldest message has actually waited.

## Documents

| Document | Purpose |
|---|---|
| [docs/specs/asb-msg-age-monitor.md](docs/specs/asb-msg-age-monitor.md) | The buildable specification: requirements, contracts, configuration, telemetry schema, alert rules, test specification, assumptions and open items |
| [docs/runbooks/service-bus-message-age.md](docs/runbooks/service-bus-message-age.md) | On-call decision branches for a Sev1/Sev2 at 03:00 |

## Layout

```
src/EIE.ServiceBus.AgeMonitor/        the function app
  Abstractions/                       ports — IEntityPeeker is the only path to message data
  Domain/                             records and enums, no framework dependencies
  Measurement/                        bounded forward scan, clock skew, corroboration
  State/                              scan state, degradation ladder, discovery cache
  Orchestration/                      tick orchestrator: parallelism, deadline, carry-over
  Telemetry/                          dual emit with cross-witnessing
  Coordination/                       cross-region blob lease
  Adapters/                           Azure SDK bindings
tests/EIE.ServiceBus.AgeMonitor.Tests/
tools/EIE.ServiceBus.AgeMonitor.ScenarioGenerator/   staging synthetic scenarios
infra/                                Bicep: tables, DCR, alert rules, RBAC, policy
infra/schema/                         column definitions shared by code, tables and DCR
```

## Build and test

```bash
dotnet build
dotnet test
```

Tests requiring external infrastructure are reported as **skipped**, never as passed, so
a green local run never implies they ran:

```bash
# Emulator integration (TST-080..085)
ASBMON_EMULATOR_CONNECTION_STRING="..." dotnet test

# Contract tests against a real non-production namespace (TST-090..094)
ASBMON_CONTRACT_NAMESPACE="sb-eie-dev.servicebus.windows.net" \
ASBMON_CONTRACT_QUEUE="asbmon-contract" dotnet test
```

## Two settings that silently break the design

Both default to the wrong value and both produce telemetry that still looks plausible:

- **Adaptive sampling must be off for metric telemetry** (`host.json`). A sampled-away
  metric point is a missing alert evaluation, indistinguishable from healthy silence.
- **`CustomMetricsOptedInType` must be `WithDimensions`** on the Application Insights
  component — not a function app setting, and off by default. Without it the entity
  dimension is stripped and the metric collapses to a namespace-wide average that would
  essentially never breach.

Both are asserted by tests.

## Design principles

Five principles decided most of the hard calls. Where a change conflicts with one, the
principle should win or be explicitly overturned — see spec Appendix A.

1. **Never emit a healthy measurement that cannot be substantiated.** Silence and
   `Indeterminate` are acceptable; a fabricated zero is not.
2. **The monitor measures; the alert layer decides what wakes someone.**
3. **Degradation must always be visible.** Silent reduced coverage defeats the purpose.
4. **Alerting must never depend on producer compliance.**
5. **Fewer dependencies than the thing it monitors,** and each fails toward *more*
   monitoring, not less.

## Status

The monitor, its tests and its infrastructure templates are implemented. Not yet done:

- **OPN-002** — the Messaging Unit cost of a full-budget tick is unquantified. The
  degradation thresholds are placeholders until a load test runs. This is a
  pre-production gate.
- **OPN-004** — the namespace firewall posture needs a security decision.
- **OPN-006** — the runbook is unreviewed; Sev1 should not page until it is.
- **OPN-016** — .NET 10 isolated on Flex Consumption and timer-singleton behaviour under
  Flex scaling both need verifying. Fallback is EP1 pinned to one instance.

Full register in the spec's open items section, and verification status in Appendix B.
