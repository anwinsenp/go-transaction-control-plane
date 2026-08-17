# 0007. Automate tenant isolation instead of flagging it for manual review

Date: 2026-08-17
Status: Accepted

## Context

`TradingTenant`'s reconciler already detects the noisy-tenant case (high
Kafka lag, normal latency, `currentReplicas >= observedPartitionCount`) and
sets `spec.isolation.dedicatedNodePool = true` on the CR (see
[DESIGN-operator.md](../DESIGN-operator.md)'s decision table). Nothing
consumes that flag. There is no dedicated Deployment/Pod for an isolated
tenant, Kafka partitioning is a shared-topic hash of `tenantID:instrument`
(`internal/ingestion/kafka/publisher.go`'s `partitionKey`), Postgres writes
go through one shared `pgxpool.Pool` for every tenant
(`internal/ledger/storage/pool.go`), and there is no ingress/gRPC-level
tenant routing anywhere in the repo. Isolation today is a status flag a
human has to notice and act on manually, which doesn't demonstrate the
operator actually doing anything with its own decision.

## Decision

Automate the full isolation path in four parts:

1. **Deterministic per-tenant Kafka partitioning.** Replace the
   `tenantID:instrument` hash key in `internal/ingestion/kafka/publisher.go`
   with an explicit `tenantID -> []partition` reservation: each tenant is
   assigned a fixed subset of a topic's partitions, and `instrument` is
   hashed only within that subset to pick a partition. This preserves
   per-instrument publish ordering (still required for idempotent P&L
   reconciliation) and per-instrument parallelism, while guaranteeing no two
   tenants ever land on the same partition.
2. **Operator gains a Deployment/Service/ConfigMap builder.** When
   `isolation.dedicatedNodePool` is `true`, the reconciler get-or-creates a
   dedicated ingestion + processor Deployment for that tenant, with
   `nodeSelector`/`tolerations` targeting an isolated node pool, plus a
   dedicated Service scoped to that Deployment's pods via label selector.
   The dedicated processor consumes via manual Kafka partition assignment
   (not consumer-group subscription), scoped to only that tenant's reserved
   partitions, so isolating or de-isolating a tenant never triggers a
   consumer-group rebalance that affects other tenants.
3. **Tenant-to-partition mapping propagation.** The operator writes the
   `tenantID -> []partition` mapping to a ConfigMap (or CR status field)
   that the shared ingestion publisher and the dedicated processor both
   watch and hot-reload at runtime. No restart is required to pick up a new
   isolation assignment.
4. **Ingress-level tenant routing.** The operator also creates a routing
   rule (an Ingress path/host rule, or a Gateway API HTTPRoute/GRPCRoute if
   that's adopted instead) that routes the isolated tenant's traffic,
   identified via the tenant's existing API-key-derived tenant ID, to the
   dedicated Service instead of the shared ingestion Service. **This
   depends on an ingress/gateway component existing in the cluster, which
   is not yet built** — this ADR introduces the dependency, it doesn't
   resolve it.

Reconcile state transitions (`Isolated` entry/exit, `DedicatedPoolCreated`,
etc.) emit `record.EventRecorder` Kubernetes Events (`Normal`/`Warning`),
visible via `kubectl describe` and exportable to Alertmanager via
kube-state-metrics or an event exporter. A low-cardinality Prometheus
counter, `tradingtenant_isolation_transitions_total{state=...}` (no raw
tenant ID label, per this repo's no-high-cardinality rule), is the primary
Grafana-graphable signal; the Events are a secondary audit trail, not the
primary metric source.

All new reconcile side effects (Deployment, Service, ConfigMap creation)
use get-or-create patterns, so a reconcile pass against an already-isolated
tenant is a no-op, consistent with this repo's operator idempotency rule.

## Consequences

- The operator's RBAC surface grows substantially: from CR-only read/write
  to creating and owning Deployments, Services, ConfigMaps, and an
  Ingress/Route resource. Its blast radius if the reconciler has a bug is
  correspondingly larger than the current status-only implementation.
- The Kafka publisher and processor both gain hot-reload complexity for the
  tenant-partition mapping (a config source that can change under a running
  process is new surface neither component has today).
- The ingress/gateway routing piece is a net-new architecture component.
  Nothing in this repo currently terminates ingress traffic with routing
  rules, so part 4 has a hard prerequisite that isn't built yet.
- `TradingTenant` remains the only custom-built operator per
  [ADR 0002](0002-single-custom-operator.md); this ADR expands what that
  one operator does, it doesn't add a second one.
- Once a tenant is isolated, its dedicated processor's manual partition
  assignment must stay in sync with the ConfigMap mapping; a mismatch
  (partitions reserved but not yet assigned, or vice versa) is a new
  failure mode this design has to handle in the processor's reload logic,
  not just in the operator.

## Alternatives considered

- **Leave isolation as detect-only, status quo.** Rejected: setting a spec
  flag that nothing reads doesn't demonstrate the operator doing anything
  beyond classification, and leaves the "noisy tenant" problem unsolved in
  practice, just labeled.
- **Route by Kafka partition key alone, without dedicated compute or
  ingress.** Rejected: a noisy tenant sharing ingestion/processor pods with
  other tenants still causes CPU/memory contention regardless of which
  Kafka partitions its events land on. Partition isolation alone solves the
  ordering/collision problem but not the resource-contention problem that
  triggered isolation in the first place.
