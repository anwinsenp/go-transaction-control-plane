# 0002. Build one custom operator, not several

Date: 2026-08-02
Status: Accepted

## Context

The platform needs a Kubernetes operator for tenant-aware scaling and
isolation logic (`TradingTenant`). This is the one piece of genuinely
domain-specific control-plane logic in the system. The platform also needs
Kafka, TLS certificate management, and a Prometheus metrics stack running
in-cluster, all of which have mature, widely-adopted operators already.

## Decision

Build exactly one custom operator: `TradingTenant`, for tenant scaling and
isolation decisions specific to this platform's domain. For everything
else, use the existing ecosystem instead of building custom operators:
Strimzi for Kafka, cert-manager for TLS, kube-prometheus-stack for
Prometheus. Postgres stays a managed service (RDS/Aurora) per
[ADR 0001](0001-postgres-over-cassandra.md); an in-cluster Postgres
operator (Zalando/PGO) is only considered if running Postgres in-cluster
becomes a specific requirement.

## Consequences

- Avoids reimplementing mature, battle-tested tooling (leader election,
  rolling upgrades, cert rotation, alerting rule CRDs) that Strimzi,
  cert-manager, and kube-prometheus-stack already solve well: that
  effort would be pure duplication with no differentiation to show for it.
- Keeps the one custom operator focused: `TradingTenant`'s reconciler can
  stay concentrated on the joint lag/latency decision logic that's
  actually specific to this platform (see
  [DESIGN-operator.md](../DESIGN-operator.md)), rather than that logic
  being diluted across several shallow, half-finished operators.
- Rules out having full control over Kafka/TLS/metrics operator behavior
  beyond what Strimzi/cert-manager/kube-prometheus-stack expose via their
  own CRDs and config: acceptable here since none of those areas are
  where this project's differentiation is meant to come from.
- Adds a dependency on three external operators' release cadence and CRD
  stability instead of one project fully owning its own control plane;
  mitigated by these being widely-adopted, actively maintained projects
  rather than fringe tooling.

## Alternatives considered

- **Custom operators for Kafka, TLS, and Prometheus as well as
  `TradingTenant`.** Rejected: this is scope explosion for a project whose
  differentiated value is the tenant scaling/isolation logic, not
  reimplementing infrastructure operators. It would also read, to an
  infra-literate reviewer, as not knowing when to build versus when to
  adopt an existing tool: the opposite of the impression a "build vs.
  adopt" decision like this is meant to demonstrate.
