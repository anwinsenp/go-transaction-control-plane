# Design: Ingestion Service (`/cmd/ingestion`, `/internal/api`)

This is the low-level design for the ingestion hot path: the public
gRPC/REST endpoint that accepts mock transaction events and publishes them
to Kafka. For where this fits in the overall system, see
[ARCHITECTURE.md](ARCHITECTURE.md).

## Zero-allocation strategy

The ingestion path (request parse -> validate -> publish to Kafka) is the
one place in this codebase held to the hot-path performance discipline
described in `CLAUDE.md`. The strategy:

- **`sync.Pool` for reused buffers/objects.** The request-decoding buffer
  and the intermediate transaction-event struct used to build the Kafka
  payload are pooled (`sync.Pool`), not allocated per request. Pool `Get`
  happens at the start of the handler, `Put` happens in a `defer` after the
  object is reset, so a panic mid-request doesn't leak a poisoned object
  back into the pool with stale data.
- **Preallocated slices with known capacity.** Anywhere a slice is built
  from a request (e.g. batched transaction events, header/metadata
  collection), it's preallocated with `make([]T, 0, knownCap)` using a
  capacity derived from the request (e.g. a declared batch size field),
  not grown via repeated `append` from a nil/zero-cap slice.
- **Avoiding unnecessary interface boxing.** Hot-path functions take and
  return concrete types (the pooled event struct, not `interface{}`/`any`);
  interfaces are used at the service boundary (e.g. the `Publisher`
  interface the domain package defines for Kafka) but not inside the
  per-request decode/validate loop, where boxing a concrete value into an
  interface would force a heap allocation.
- Every allocation-sensitive change on this path must be backed by a
  benchmark, not a comment claiming zero-alloc. Add or update a
  `_test.go` benchmark using `go test -bench=. -benchmem` alongside the
  change, and treat a benchmark showing new `allocs/op` on this path as a
  regression to explain or fix, not to note and move past.

## Request validation and rejection

- All inbound payloads are validated before any Kafka publish attempt:
  required fields present, field types/ranges within the versioned schema
  (see schema versioning below), no unbounded-size fields that could be
  used to force large allocations.
- Malformed or invalid payloads are rejected with an appropriate 4xx
  (`400 Bad Request` for malformed encoding/missing required fields,
  `422 Unprocessable Entity` for payloads that decode but fail semantic
  validation) and a response body identifying what failed. Invalid
  requests are never silently dropped. The caller always gets a
  synchronous answer.
- Auth (API key / bearer token) and rate-limit checks happen before
  payload validation, so unauthenticated or rate-limited traffic is
  rejected as cheaply as possible, without spending validation work on it.

## Kafka publish and producer configuration

Once a request passes validation, the handler converts it into a
transport-agnostic `ingestion.Event` and hands it to the `ingestion.Publisher`
port (`internal/ingestion`). `internal/ingestion/kafka` is the concrete
implementation, built on
[`github.com/twmb/franz-go`](https://github.com/twmb/franz-go):

- **Durability:** the producer is configured with `RequiredAcks(AllISRAcks())`
  — it waits for every in-sync replica to acknowledge a write, not just the
  partition leader. A transaction event is part of the ledger's source of
  truth, so losing one to a leader crash before ISR replication is not an
  acceptable trade for lower publish latency.
- **Partitioning:** the Kafka message key is `<tenant_id>:<instrument>`, so
  every event for a given tenant's instrument lands on the same partition
  and is observed by the processor in publish order — idempotent P&L
  reconciliation depends on seeing an instrument's events in order. The two
  components are joined without escaping (`tenantID + ":" + instrument`),
  which is safe only because `internal/api`'s validation charsets forbid
  `:` in both fields (tenant_id: lowercase alphanumeric/hyphen; instrument:
  uppercase alphanumeric/dot). If either charset is ever relaxed to allow
  `:`, this key could collide across tenant/instrument boundaries.
- **Batching/latency:** `ProducerLinger` batches concurrently in-flight
  produces into fewer requests. The publish call itself is synchronous
  (`ProduceSync`), which cancels any pending linger and drains immediately,
  so linger only helps when multiple requests are publishing concurrently —
  it never adds latency to a single in-flight request.
- **Timeouts:** `ProduceRequestTimeout` bounds how long a single produce
  request can block on the broker, so a stalled broker can't hang a request
  goroutine indefinitely.
- **Config source:** `kafka.Config` is resolved via `kafka.ConfigFromEnv()`
  (`KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_REQUEST_TIMEOUT`,
  `KAFKA_LINGER`), defaulting to `kafka.LocalConfig()` sized for the
  docker-compose local stack: `localhost:9092` (single broker),
  `transaction-events` topic, a 10s `RequestTimeout`, and a 5ms `Linger`.
- **Error handling:** a publish failure is wrapped (`fmt.Errorf(...: %w...)`),
  logged, and surfaced to the caller as `503 Service Unavailable` — the
  request is never acknowledged with `202` unless the event durably reached
  Kafka. There is no circuit breaker or backpressure shedding around this
  call yet; that's tracked separately (see Backpressure and Circuit breaker
  sections below).

## Schema/API versioning

The transaction event schema (the Kafka payload contract) and the public
API contract are versioned independently:

- The public API exposes a version in its path/header (e.g. `/v1/...`),
  so the wire contract with external callers can evolve without breaking
  existing integrations.
- The Kafka payload carries a schema version field, so the processor (a
  separate deployable) can be rolled out independently from ingestion and
  still correctly interpret events produced by an older or newer
  ingestion version during a rolling deploy.

## Backpressure

The Kafka publish call sits between the ingestion handler and Kafka. When
Kafka can't keep up (publish latency rising, or the circuit breaker below
is open), the ingestion service applies backpressure rather than buffering
requests unboundedly in memory or blocking the request goroutine
indefinitely:

- A bounded in-process queue/semaphore caps the number of in-flight
  publish attempts. Once that bound is reached, new requests are rejected
  immediately with `503 Service Unavailable` (and a `Retry-After` hint)
  instead of queuing behind the ones already in flight.
- This is a deliberate shed-load choice: an unbounded queue would let
  ingestion memory grow without bound under sustained Kafka slowness,
  which turns a Kafka problem into an ingestion-service OOM. Bounded
  rejection keeps the failure mode visible and cheap to recover from.

## Circuit breaker around Kafka publish

The Kafka publish call is wrapped in a circuit breaker with three states:

| State | Behavior |
|---|---|
| Closed | Publish attempts proceed normally; failures are counted in a rolling window. |
| Open | Publish attempts fail fast (no network call to Kafka) and the handler immediately returns `503`; entered after failures in the rolling window exceed a configured threshold. |
| Half-open | After a cooldown period, a limited number of probe requests are allowed through; if they succeed, the breaker closes, if any fails, it reopens and the cooldown restarts. |

This mirrors the same breaker pattern used on the processor's Postgres
write path (see `requirements.md`), so a degraded Kafka broker or a
degraded Postgres instance each fail fast and stay contained to their own
service instead of cascading into the whole pipeline stalling. Breaker
state is exported as a Prometheus gauge per breaker (per the telemetry
requirements), so an open breaker is visible on the dashboard, not just
inferred from error logs.

## Why not X

- **Why not buffer-and-retry indefinitely instead of shedding load?**
  Because the ingestion service's job is to accept traffic at the rate the
  rest of the pipeline can sustain, not to become an unbounded queue in
  front of Kafka: Kafka itself is already the durable buffer in this
  architecture; adding a second, in-memory one duplicates that role with
  worse durability and an OOM risk.
- **Why interfaces at the service boundary but not inside the hot loop?**
  The `Publisher` interface (defined in the domain package per this repo's
  ports-and-adapters layout) is what lets the Kafka implementation live in
  `storage/`/`transport` and be swapped in tests; that's a
  once-per-request call, not a per-field operation, so the single
  interface-dispatch cost there is negligible compared to boxing every
  decoded field into `any` inside the parse loop.
