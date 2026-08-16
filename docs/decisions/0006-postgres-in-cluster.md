# 0006. Move Postgres in-cluster on k3s, superseding managed RDS/Aurora

Date: 2026-08-16
Status: Accepted

## Context

[ADR 0002](0002-single-custom-operator.md) decided Postgres would stay a
managed AWS service (RDS/Aurora), with an in-cluster Postgres operator
considered only if running Postgres in-cluster became a specific
requirement. That requirement has now materialized: the sandbox
deployment runs everything — Kafka (Strimzi), Postgres, and the
`TradingTenant` operator itself — as pods on the same self-hosted k3s
cluster on EC2, with no managed AWS data services in the loop. Aurora was
the only remaining piece of infrastructure not running in-cluster.

## Decision

Run Postgres in-cluster on k3s via an existing, maintained operator/chart
(e.g. CloudNativePG or Zalando's `postgres-operator`), the same pattern
already used for Kafka (Strimzi). No managed RDS/Aurora instance is
provisioned. `docs/decisions/0002-single-custom-operator.md` is superseded
by this decision for its Postgres-hosting claim; its reasoning for why
`TradingTenant` remains the only *custom-built* operator is unaffected —
Postgres still consumes an existing operator, it doesn't get a bespoke one.

## Consequences

- Kafka, Postgres, and the metrics stack are now uniformly in-cluster,
  which simplifies the mental model (one environment, one set of
  Kubernetes primitives for networking/secrets/storage) and makes the
  local Kind environment a closer mirror of the k3s sandbox: the same
  operator/chart manifests apply to both, differing mainly in
  `StorageClass` and resource sizing rather than in kind.
- Trades away Aurora's zero-operational-surface promise (automated
  backups, patching, failover handled by AWS) for operational
  responsibility this project now owns: PVC-backed storage sized and
  monitored ourselves, backup/restore procedure defined ourselves, and
  failover behavior bounded by whatever the chosen operator provides
  rather than a managed service's SLA.
- [ADR 0001](0001-postgres-over-cassandra.md)'s core decision (Postgres
  over Cassandra) is unaffected — that ADR's reasoning was about the
  ACID/consistency model, not about who hosts the instance — but its
  "single Aurora writer" framing is now stale and reworded to a generic
  self-hosted single-primary Postgres.
- `docs/decisions/0003-postgres-connection-pool-sizing.md`'s
  `SandboxPoolConfig()` rationale ("a modestly-sized managed Postgres
  instance") needs the same generic self-hosted reframing; the pool-sizing
  numbers themselves are unaffected, since the constraint (a shared
  instance's connection ceiling vs. several service replicas) is the same
  regardless of who operates the instance.
- The `TradingTenant` operator's own scope is unaffected: it still only
  reconciles ingestion/processor scaling and isolation via Prometheus
  signals, never touching Postgres.

## Alternatives considered

- **Keep Postgres on managed RDS/Aurora** (the original ADR 0002
  decision). Rejected now that the project's actual infrastructure choice
  is to self-host everything on EC2/k3s rather than mixing managed and
  self-hosted data services — keeping Aurora would mean carrying a
  separate Terraform module, a separate network path (VPC peering/security
  groups to the managed instance), and a separate operational model for
  exactly one component, with no corresponding benefit once the team is
  already taking on k3s's own operational overhead per
  [ADR 0002](0002-single-custom-operator.md)'s "self-hosted k3s on EC2"
  trade-off.
- **A custom Postgres operator**, mirroring `TradingTenant`. Rejected for
  the same reason ADR 0002 rejected custom Kafka/TLS/metrics operators:
  Postgres HA/backup/failover is well-trodden ground with mature existing
  operators (CloudNativePG, Zalando); building one here would be pure
  duplication with no differentiation to show for it.
