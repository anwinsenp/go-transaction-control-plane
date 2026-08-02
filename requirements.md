# go-transaction-control-plane — Requirements

A distributed, real-time transaction processing engine in Go: zero-allocation
ingestion hot path, Kafka streaming, Postgres reconciliation, a custom
Kubernetes operator for tenant scaling, and Prometheus/Grafana telemetry,
deployed to a public sandbox for demonstration.

## Ingestion service (`/cmd/ingestion`, `/internal/api`)
- Public gRPC/REST endpoint that accepts mock high-frequency trade/transaction
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
- **Auth on the public ingestion endpoint**: at minimum an API key /
  bearer-token check — a public endpoint with no auth is not acceptable to
  ship, even for a demo/portfolio project.
- **Rate limiting** on the ingestion endpoint, so the public sandbox can't
  be trivially overwhelmed by traffic outside your own load tests.
- **Backpressure**: the ingestion service must apply backpressure (reject
  or shed load with a clear response) when downstream (Kafka) can't keep
  up, rather than buffering unboundedly or blocking the hot path.
- **Circuit breaker** around the ingestion → Kafka publish call and the
  processor → Postgres write path: trip after repeated failures, fail fast
  while open, and recover (half-open probe) once the dependency is healthy
  again. This is what keeps a degraded Kafka/Postgres from cascading into
  the whole system stalling.
- **Schema/API versioning**: the transaction event schema (Kafka payload)
  and the public API contract must be versioned so they can evolve without
  silently breaking consumers.

## Storage layer (`/internal/<domain>/storage`)
- Postgres/Aurora schema for transactions and reconciled state, with
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
  `observedKafkaLag`, `observedP99Ms`, `lastReconcileTime`.
- Reconciler loop (via `controller-runtime`) implementing joint-signal
  decision logic — not a single-metric threshold (that's just HPA
  reimplemented). For each `TradingTenant`, evaluate consumer lag and P99
  latency together:
  - **Lag high AND latency high** → genuine under-capacity → scale up
    (bounded by `maxReplicas`).
  - **Lag high AND latency normal** → noisy-neighbor / partition-skew
    pattern, not a capacity problem → isolate onto a dedicated node pool
    (set `isolation.dedicatedNodePool: true`) instead of blindly adding
    replicas.
  - **Latency high AND lag normal** → likely a downstream (Postgres)
    bottleneck that adding replicas won't fix → set `state: Degraded`,
    don't scale, surface for investigation rather than masking it.
  - **Both normal** → `state: Stable`, no action.
  - Default behavior for most tenants is the shared pool with resource
    quotas; escalation to `dedicatedNodePool: true` is the exception path
    for tenants whose signals indicate a genuine isolation need, not the
    default for every tenant (mirrors how real multi-tenant trading
    platforms tier clients rather than isolating everyone by default).
- Reconcile must be idempotent and safe to call repeatedly with the same
  object state; bounded context on any external call.
- Status subresource updates kept separate from spec mutation.
- Tests using `controller-runtime`'s fake client, with one test case per
  branch of the decision table above (four cases minimum), plus an explicit
  idempotency test (reconcile twice, assert same end state).

## Telemetry (`/internal/metrics`, or wherever instrumentation lives)
- Prometheus metrics exported via `client_golang` on `/metrics` per service:
  transaction throughput (counter), P50/P99 latency (histogram), Kafka
  consumer lag (gauge), operator reconcile loop duration (histogram),
  circuit breaker state (gauge, per breaker).
- No high-cardinality labels (no raw transaction/tenant IDs on metrics).
- Grafana dashboard definition (as code/JSON, checked into the repo) showing
  the above metrics.
- Alert rules (Prometheus Alertmanager or Grafana alerting) for: P99 latency
  breach, Kafka consumer lag growing unbounded, circuit breaker open longer
  than a threshold. Dashboards alone only show state; alerting demonstrates
  actual operational maturity.

## Local development environment
- `docker-compose.yml` bringing up Kafka and Postgres locally, so
  development and tests don't require the AWS sandbox.
- A documented `make` target or script (`make dev-up` / `./scripts/dev.sh`)
  to start the local stack and run the ingestion + processor services
  against it.

## Infrastructure (`/terraform`)
- Modular Terraform: one concern per module (networking, ECS/EKS cluster,
  Kafka, Postgres/Aurora, ingress).
- Deploy the ingestion layer to AWS (ECS or EKS) with a public endpoint.
- Deploy Prometheus + Grafana (or point Grafana at a managed Prometheus) with
  a public, read-only dashboard link.
- **Use existing operators for undifferentiated infrastructure — do not
  build custom operators for these.** Kafka: **Strimzi**, if running Kafka
  in-cluster (otherwise a managed service). TLS: **cert-manager**.
  Metrics stack: **kube-prometheus-stack**. Postgres: managed (RDS/Aurora)
  per the original architecture, or a maintained operator (Zalando/PGO) only
  if running Postgres in-cluster is specifically required. The only
  custom-built operator in this project is `TradingTenant` — everything
  else should consume the existing ecosystem, which itself demonstrates
  knowing when to build vs. when to adopt.

## Load testing & demo artifacts
- A load-test script (curl burst or small Go load generator) that fires a
  rapid burst of requests at the public sandbox endpoint.
- Document expected/observed results (P99 latency, throughput) from a real
  run against the sandbox — not fabricated numbers.

## Documentation
- README: architecture diagram, the "why" behind key trade-offs (e.g.
  ECS/Kinesis vs. Lambda for cold-start avoidance), public sandbox URL,
  Grafana dashboard link, how to run the load-test script.
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
