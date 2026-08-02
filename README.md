# go-transaction-control-plane

A distributed, real-time transaction processing engine in Go: a
zero-allocation ingestion hot path, Kafka-based event streaming, a
Postgres-backed reconciliation layer, a custom Kubernetes operator for
tenant-aware scaling, and Prometheus/Grafana telemetry, deployed to a
public sandbox for demonstration.

## Documentation

| Doc | What's in it |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | System design, goals/non-goals, major trade-offs |
| [Ingestion design](docs/DESIGN-ingestion.md) | Zero-allocation hot-path strategy, request validation, backpressure, circuit breaker around Kafka publish |
| [Operator design](docs/DESIGN-operator.md) | `TradingTenant` CRD spec and reconcile decision logic |
| [Operator alerts runbook](docs/RUNBOOK-operator-alerts.md) | Per-alert diagnosis and action steps for scaling, noisy-neighbor isolation, and downstream bottleneck alerts |
| [Architecture decisions](docs/decisions/) | Log of significant design decisions with rationale |
