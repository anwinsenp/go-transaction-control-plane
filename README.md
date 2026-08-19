# go-transaction-control-plane

[![CI](https://github.com/anwinsenp/go-transaction-control-plane/actions/workflows/ci.yml/badge.svg)](https://github.com/anwinsenp/go-transaction-control-plane/actions/workflows/ci.yml)

A distributed, real-time transaction processing engine in Go: a
zero-allocation ingestion hot path, Kafka-based event streaming, a
Postgres-backed reconciliation layer, a custom Kubernetes operator for
tenant-aware scaling, and Prometheus/Grafana telemetry, deployed to a
public sandbox for demonstration.

**Status:** Design complete, implementation in progress. See
[Architecture](docs/ARCHITECTURE.md) for the full design. Code is being
built incrementally against the tracked issues in this repo. The full stack
(Strimzi Kafka, CloudNativePG Postgres, kube-prometheus-stack, ingestion,
processor, and the TradingTenant operator) now deploys and runs end-to-end
on a local Kind cluster — see [Local development](#local-development). The
k3s/EC2 sandbox deployment (issues #32-#35) is still open.

**Stack:** Go · Kafka (Strimzi) · PostgreSQL (self-hosted, in-cluster) ·
Kubernetes (`controller-runtime`) · Prometheus/Grafana · Terraform · AWS
(self-hosted k3s on EC2, Kind for local dev)

## Documentation

| Doc | What's in it |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | System design, goals/non-goals, major trade-offs |
| [Ingestion design](docs/DESIGN-ingestion.md) | Zero-allocation hot-path strategy, gRPC/REST transport lifecycle, request validation, backpressure, circuit breaker around Kafka publish |
| [Ledger design](docs/DESIGN-ledger.md) | `internal/ledger` domain types, fixed-point `int64` amount representation and parsing, Postgres `BIGINT` schema |
| [Processor design](docs/DESIGN-processor.md) | Idempotent weighted-average-cost P&L reconciliation and the Kafka consumer's bounded-retry dead-letter-queue routing |
| [Operator design](docs/DESIGN-operator.md) | `TradingTenant` CRD spec and reconcile decision logic |
| [Operator alerts runbook](docs/RUNBOOK-operator-alerts.md) | Per-alert diagnosis and action steps for scaling, noisy-neighbor isolation, and downstream bottleneck alerts |
| [Architecture decisions](docs/decisions/) | Log of significant design decisions with rationale |

## Local development

Prerequisites: Docker, `kind`, `helm`, `kubectl` (on macOS: `brew install
kind helm`).

Bring up the full stack (Kafka, Postgres, Prometheus, ingestion, processor,
operator) on a local Kind cluster:

```
make kind-up      # creates the cluster and installs the TradingTenant CRDs
make kind-load    # builds ingestion/processor/operator images and loads
                   # them into the cluster (re-run after any code change)
make kind-deploy  # installs Strimzi/CloudNativePG/kube-prometheus-stack via
                   # Helm and applies the ingestion/processor/operator
                   # manifests, in dependency order
make kind-verify  # sends a sample transaction through the live stack and
                   # asserts (non-zero exit on failure, not just prints) that
                   # the TradingTenant reached a known status.state AND that
                   # tradingtenant_reconcile_duration_seconds{result="success"}
                   # actually increased off the back of it
```

`kind-verify` (`deploy/kind/verify.sh`) is a real assertion, not a smoke
test for a human to eyeball: it snapshots the operator's
`tradingtenant_reconcile_duration_seconds{result="success"}` count, POSTs a
transaction to ingestion, waits for `TradingTenant/tenant-a` to reach a
known `status.state`, then polls the metric until the success count
increases. It exits non-zero with a `FAIL: ...` message if either
condition isn't met.

Tear the cluster down with `make kind-down`.

This Kind environment is for local development only. The production-style
deploy target is the k3s cluster on EC2 (issues #32-#35), which is still
open.

### Visualizing metrics in Grafana

`make kind-deploy` already installs Grafana as part of the
`kube-prometheus-stack` Helm release, there's no separate install step.
Reach it via port-forward (there's no ingress in the Kind values):

```
kubectl -n monitoring port-forward svc/prometheus-grafana 3000:80
```

Then open `http://localhost:3000` and log in with the chart's default
admin user (`admin`) and the generated password:

```
kubectl -n monitoring get secret prometheus-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d
```

`deploy/grafana/dashboard.json` is a pre-built "Transaction Control Plane"
dashboard covering throughput, P50/P99 latency, Kafka consumer lag,
operator reconcile duration, and circuit breaker state. It imports cleanly
via **Dashboards → Import** with no manual edits, since it uses a
templated `${DS_PROMETHEUS}` datasource variable rather than a hardcoded
UID. See `deploy/grafana/README.md` for the full import steps and a
panel-to-metric table.

Note that `kind-verify` only sends a single transaction, to see meaningful
data in the throughput/latency panels you'll want to generate sustained
traffic against ingestion first.

## License

[MIT](LICENSE)
