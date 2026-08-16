# 0001. Postgres over Cassandra for the transaction ledger

Date: 2026-08-02
Status: Accepted

## Context

The reconciliation/storage layer needs a datastore for the transaction
ledger and reconciled state, written by a single Kafka consumer
(the processor) and read for reconciliation queries and reporting. The
system needs correctness guarantees for financial data (no double-counted
or partially-applied transactions) under Kafka's at-least-once delivery.

## Decision

Use PostgreSQL for the transaction ledger and reconciled state, not
Cassandra. (Hosting location — originally managed Aurora, later moved
in-cluster — is a separate decision; see
[ADR 0006](0006-postgres-in-cluster.md). Nothing below depends on which.)

## Consequences

- Get ACID transactions and referential integrity natively. Idempotent
  reconciliation (rejecting/no-op'ing duplicate deliveries) and multi-row
  consistency (e.g. updating a transaction and its reconciled-balance
  record together) are expressed as ordinary transactions, without
  building an external consistency layer.
- Give up Cassandra's easier horizontal write scaling and its pattern of
  per-service keyspace isolation, where each service owns its own
  keyspace with no cross-service joins or shared schema. At this project's
  scale (a single processor service, single ledger schema), that trade-off
  costs nothing; it would matter more at a scale with many independent
  services each needing their own write-scaled keyspace.
- Ties correctness to a single primary writer rather than a leaderless
  multi-node write path: acceptable here since the goal is demonstrating
  financial correctness under realistic load, not proving Cassandra-scale
  write throughput.

## Alternatives considered

- **Cassandra**, following the pattern used by some real trading platforms
  (e.g. Monzo) of one keyspace per service. Rejected because Cassandra has
  no native multi-row ACID transactions: getting the equivalent guarantee
  requires building consistency externally (e.g. distributed locking via
  etcd/ZooKeeper around the ledger writes), which is unnecessary complexity
  for this project's scale and would mean spending engineering effort
  reimplementing what Postgres gives natively.
- **CockroachDB**, which several fintechs have migrated to from Postgres
  specifically for horizontal write scale while keeping Postgres wire
  compatibility and ACID semantics. This is a credible alternative and
  noted here as a future consideration if/when this system needed to scale
  writes past what a single Postgres primary can sustain, but it's not
  adopted now: current scale doesn't need it, and introducing it today
  would add operational surface (a different distributed-consensus
  storage engine) without a corresponding requirement to justify it.
