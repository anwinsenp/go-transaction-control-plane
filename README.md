# go-transaction-control-plane

[![CI](https://github.com/anwinsenp/go-transaction-control-plane/actions/workflows/ci.yml/badge.svg)](https://github.com/anwinsenp/go-transaction-control-plane/actions/workflows/ci.yml)

A distributed, real-time transaction processing engine in Go: a
zero-allocation ingestion hot path, Kafka-based event streaming, a
Postgres-backed reconciliation layer, a custom Kubernetes operator for
tenant-aware scaling, and Prometheus/Grafana telemetry, deployed to a
public sandbox for demonstration.

**Status:** Design complete, implementation in progress. See
[Architecture](docs/ARCHITECTURE.md) for the full design. Code is being
built incrementally against the tracked issues in this repo.

**Stack:** Go · Kafka (Strimzi) · PostgreSQL/Aurora · Kubernetes
(`controller-runtime`) · Prometheus/Grafana · Terraform · AWS (self-hosted
k3s on EC2, Kind for local dev)

## Documentation

| Doc | What's in it |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | System design, goals/non-goals, major trade-offs |
| [Ingestion design](docs/DESIGN-ingestion.md) | Zero-allocation hot-path strategy, gRPC/REST transport lifecycle, request validation, backpressure, circuit breaker around Kafka publish |
| [Ledger design](docs/DESIGN-ledger.md) | `internal/ledger` domain types, fixed-point `int64` amount representation and parsing, Postgres `BIGINT` schema |
| [Operator design](docs/DESIGN-operator.md) | `TradingTenant` CRD spec and reconcile decision logic |
| [Operator alerts runbook](docs/RUNBOOK-operator-alerts.md) | Per-alert diagnosis and action steps for scaling, noisy-neighbor isolation, and downstream bottleneck alerts |
| [Architecture decisions](docs/decisions/) | Log of significant design decisions with rationale |

## License

[MIT](LICENSE)
