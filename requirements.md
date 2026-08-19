# go-transaction-control-plane — Requirements

A distributed, real-time transaction processing engine in Go: zero-allocation
ingestion hot path, Kafka streaming, Postgres reconciliation, a custom
Kubernetes operator for tenant scaling, and Prometheus/Grafana telemetry,
deployed end-to-end on a local Kind cluster for demonstration.

## Ingestion service (`/cmd/ingestion`, `/internal/api`)
- gRPC/REST endpoint that accepts mock high-throughput trade/transaction
  events.
- Zero-allocation hot path: use `sync.Pool` for reused buffers/objects,
  preallocate slices with known capacity, avoid unnecessary interface boxing.
- Request validation (malformed/invalid transaction payloads rejected with
  appropriate 4xx, not silently dropped).
- Publish validated events to Kafka.
- Benchmarks (`go test -bench`, `-benchmem`) proving the zero/low-allocation
  claim on the hot path.

## Transaction processor (`/internal/processor`)
- Kafka consumer that reads transaction events and applies P&L-style
  reconciliation logic.
- Idempotent processing (safe against Kafka's at-least-once delivery —
  duplicate events must not double-count).
- Write reconciled state to Postgres via the storage layer.
- Dead-letter queue: events that fail processing after a bounded number of
  retries are routed to a DLQ topic instead of blocking the consumer or
  being silently dropped.
- Table-driven unit tests covering happy path, malformed event,
  duplicate-delivery, and DLQ-routing cases.

## Resiliency & hardening
- **Auth on the ingestion endpoint**: at minimum an API key / bearer-token
  check — an endpoint with no auth is not acceptable to ship, even for a
  demo/portfolio project.
- **Rate limiting** on the ingestion endpoint, so it can't be trivially
  overwhelmed by traffic outside your own load tests.
- **Backpressure**: the ingestion service must apply backpressure (reject
  or shed load with a clear response) when downstream (Kafka) can't keep
  up, rather than buffering unboundedly or blocking the hot path.
- **Circuit breaker** around the ingestion → Kafka publish call and the
  processor → Postgres write path: trip after repeated failures, fail fast
  while open, and recover (half-open probe) once the dependency is healthy
  again. This is what keeps a degraded Kafka/Postgres from cascading into
  the whole system stalling.
- **Schema/API versioning**: the transaction event schema (Kafka payload)
  and the API contract must be versioned so they can evolve without
  silently breaking consumers.

## Storage layer (`/internal/<domain>/storage`)
- Postgres schema for transactions and reconciled state, with
  indexing appropriate for high-concurrency writes/reads.
- Repository interfaces defined in the domain package; Postgres
  implementation lives only in `storage/`.
- Optimized connection pooling (sized/configured deliberately, not left at
  driver defaults) with a brief rationale documented.
- Migration tooling/scripts for schema setup.

## Kubernetes operator (`/operator`)
- Custom CRD: `TradingTenant`, representing one institutional trading client
  on the platform. Spec fields: `tenantID`, `minReplicas`/`maxReplicas`,
  per-pod `resources` (CPU/memory request), `scaling` thresholds
  (`kafkaLagThreshold`, `p99LatencyThresholdMs`), `isolation.dedicatedNodePool`
  (bool, set by the operator, not the user). Status fields:
  `currentReplicas`, `state` (`Stable`/`Scaling`/`Isolated`/`Degraded`),
  `observedKafkaLag`, `observedP99Ms`, `observedPartitionCount`,
  `lastReconcileTime`.
- Reconciler loop (via `controller-runtime`) implementing joint-signal
  decision logic, not a single-metric threshold (that's just HPA
  reimplemented). For each `TradingTenant`, evaluate consumer lag and P99
  latency together:
  - **Lag high AND latency high**: genuine under-capacity. Scale up
    (bounded by `maxReplicas`).
  - **Lag high AND latency normal**: backlog is building but each item
    still processes fast once picked up, so this is a queue-depth problem,
    not a per-item slowness problem. Two distinct sub-cases, checked in
    order:
    1. If `currentReplicas < observedPartitionCount` (there's still
       Kafka-side parallelism headroom): scale up consumer replicas first,
       same as the high/high branch, bounded by `maxReplicas` and by
       `observedPartitionCount` (adding a replica beyond partition count
       adds an idle consumer, not more throughput).
    2. If `currentReplicas >= observedPartitionCount` (already at
       partition parity, no more Kafka-side parallelism available): this
       is the noisy-neighbor / partition-skew case. More replicas can't
       help since Kafka can't hand out more than one consumer per
       partition. Isolate onto a dedicated node pool
       (`isolation.dedicatedNodePool: true`) instead, and flag for manual
       review of the tenant's partitioning key (skewed key hashing is a
       common root cause and needs a human to fix, the operator can only
       contain the symptom).
  - **Latency high AND lag normal**: likely a downstream (Postgres)
    bottleneck that adding replicas won't fix, since the bottleneck sits
    outside the operator's authority (compute/replicas), not inside it.
    Set `state: Degraded`, don't scale, surface for investigation rather
    than masking it.
  - **Both normal**: `state: Stable`, no action.
  - Default behavior for most tenants is the shared pool with resource
    quotas. Escalation to `dedicatedNodePool: true` is the exception path
    for tenants whose signals indicate a genuine isolation need after
    Kafka-side scaling headroom is exhausted, not the default for every
    tenant (mirrors how real multi-tenant trading platforms tier clients
    rather than isolating everyone by default). The operator does not
    auto-revert a tenant out of isolation once set; reverting
    `dedicatedNodePool` to `false` is a manual step once the root cause
    (repartitioning, quota adjustment) has actually been addressed, to
    avoid thrashing a tenant back and forth if its metrics sit right at
    the threshold.
- Reconcile must be idempotent and safe to call repeatedly with the same
  object state; bounded context on any external call.
- Status subresource updates kept separate from spec mutation.
- Tests using `controller-runtime`'s fake client, with one test case per
  branch of the decision table above, including both lag-high/latency-normal
  sub-cases (five cases minimum total), plus an explicit idempotency test
  (reconcile twice, assert same end state).

## Telemetry (`/internal/metrics`, or wherever instrumentation lives)
- Prometheus metrics exported via `client_golang` on `/metrics` per service:
  transaction throughput (counter), P50/P99 latency (histogram), Kafka
  consumer lag (gauge), Kafka partition count and active consumer count per
  tenant topic (gauges, feeding the operator's partition-headroom check),
  operator reconcile loop duration (histogram), circuit breaker state
  (gauge, per breaker).
- No high-cardinality labels (no raw transaction/tenant IDs on metrics).
- Grafana dashboard definition (as code/JSON, checked into the repo) showing
  the above metrics.
- Alert rules (Prometheus Alertmanager or Grafana alerting), including at
  minimum:
  - `TenantScalingEvent` (info, no page): fires when a `TradingTenant`
    transitions to `Scaling`. Used for trend visibility, not response.
  - `TenantAtMaxReplicasWithGrowingLag` (warning): fires when a tenant is at
    `maxReplicas` and `observedKafkaLag` is still increasing over the
    reconcile window. Signals automatic scaling has hit its ceiling.
  - `TenantIsolatedNoisyNeighborSuspected` (warning, escalate to critical if
    lag keeps growing post-isolation): fires when a `TradingTenant`
    transitions to `Isolated` (lag high, latency normal, already at
    partition parity). This is the alert that pages an SRE, since it means
    the operator has reached the edge of its own authority (it can contain
    the symptom via isolation but cannot repartition Kafka or diagnose root
    cause) and a human decision is required.
  - `TenantDegradedDownstreamBottleneck` (warning): fires when a
    `TradingTenant` transitions to `Degraded` (latency high, lag normal).
    Same handoff logic: the operator deliberately takes no scaling action
    here since the bottleneck is outside its authority (likely Postgres).
  - Standard: P99 latency breach, Kafka consumer lag growing unbounded,
    circuit breaker open longer than a threshold.
  - Every alert carries a `runbook_url` annotation pointing at the
    relevant section of `docs/RUNBOOK-operator-alerts.md`, so a paged SRE
    lands directly on diagnosis steps rather than reconstructing them at
    2am. Dashboards alone only show state; alerting with a linked runbook
    is what demonstrates actual operational maturity.

## Local development environment
- `docker-compose.yml` bringing up Kafka and Postgres locally, so
  development and tests don't require a cluster.
- A documented `make` target or script (`make dev-up` / `./scripts/dev.sh`)
  to start the local stack and run the ingestion + processor services
  against it.

## Infrastructure (`/terraform`) — dropped
A self-hosted k3s-on-EC2 deployment with a public endpoint (modular
Terraform: networking/VPC, k3s cluster, ingress, TLS via cert-manager) was
originally scoped here. It's been dropped in favor of a local Kind cluster
as the deployment target — see
[the README's status](README.md#go-transaction-control-plane) and
[ARCHITECTURE.md's trade-offs](docs/ARCHITECTURE.md#major-trade-offs) for
why. No `/terraform` directory exists in the repo.

The in-cluster component choices below still apply to the Kind deployment:
- **Use existing operators for undifferentiated infrastructure — do not
  build custom operators for these.** Kafka: **Strimzi**, in-cluster. TLS:
  **cert-manager** (not currently deployed in Kind — no public endpoint
  needs a certificate). Metrics stack: **kube-prometheus-stack**. Postgres:
  in-cluster via a maintained operator/chart (e.g. CloudNativePG or
  Zalando's `postgres-operator`) — see
  [ADR 0006](docs/decisions/0006-postgres-in-cluster.md), which supersedes
  the original managed-RDS/Aurora decision. The only custom-built operator
  in this project is `TradingTenant` — everything else should consume the
  existing ecosystem, which itself demonstrates knowing when to build vs.
  when to adopt.

## Load testing & demo artifacts
- A load-test script (curl burst or small Go load generator) that fires a
  rapid burst of requests at the local Kind ingestion service.
- Document observed results (P99 latency, throughput) from a real run
  against the Kind cluster — not fabricated numbers. See the README's
  [Load test results](README.md#load-test-results).

## Documentation
- README: architecture diagram, the "why" behind key trade-offs (e.g. Kind
  over a public deployment, and long-running service vs. Lambda for
  cold-start avoidance), Grafana dashboard link, how to run the load-test
  script.
- `docs/RUNBOOK-operator-alerts.md`: diagnosis and action steps for each
  `TradingTenant` alert (isolation, degraded, at-max-replicas), linked from
  every relevant alert's `runbook_url` annotation.
- Embed or link the architecture walkthrough video (if/when recorded) at the
  top of the README.

## CI / repo hygiene
- CI pipeline (GitHub Actions) running `gofmt -l .`, `go vet ./...`,
  `go build ./...`, `go test ./... -race` on every PR.
- Branch protection / PR template referencing the standard PR summary format.
- Repo meta files: `LICENSE`, `CONTRIBUTING.md`, issue templates, PR
  template — these are what GitHub's own "Community Standards" checklist
  evaluates, and a portfolio repo missing them looks unfinished even if the
  code is solid.
