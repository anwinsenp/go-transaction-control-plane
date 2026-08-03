# Design: Ingestion Service (`/cmd/ingestion`, `/internal/api`)

This is the low-level design for the ingestion hot path: the public
gRPC/REST endpoint that accepts mock transaction events and publishes them
to Kafka. For where this fits in the overall system, see
[ARCHITECTURE.md](ARCHITECTURE.md). For the fixed-point representation of
`quantity`/`price` referenced throughout this doc, see
[DESIGN-ledger.md](DESIGN-ledger.md).

## Transports: REST and gRPC

`cmd/ingestion/main.go` starts two transports against the same
`ingestion.Publisher`, one HTTP (`api.NewServer`, REST) and one gRPC
(`api.NewGRPCServer`), listening on independently configurable addresses
(`HTTP_ADDR`, default `:8080`; `GRPC_ADDR`, default `:9090`). Both run for
the lifetime of the process; there's no primary/fallback relationship
between them, a caller picks whichever transport fits its stack.

- **Lifecycle coupling.** The two servers are started in separate
  goroutines against a shared `runCtx`. If either server fails to start
  (anything other than the graceful-shutdown sentinel errors,
  `http.ErrServerClosed` / `grpc.ErrServerStopped`), a `sync.Once`-guarded
  `recordStartErr` cancels `runCtx`, which unblocks the main goroutine and
  triggers shutdown of both servers, not just the one that failed. A
  startup failure on one transport never leaves the other's listener and
  goroutine running unsupervised.
- **Shutdown budget.** Both `Shutdown` calls run concurrently against the
  same `shutdownTimeout`-bound context, in separate goroutines joined with
  a `sync.WaitGroup`, rather than sequentially. Running them sequentially
  would let a slow HTTP drain eat into the gRPC server's share of the
  shutdown budget (or vice versa); running them concurrently means each
  gets the full `shutdownTimeout` to drain in-flight requests/RPCs.
- **gRPC health checking.** `GRPCServer` registers the standard gRPC
  health checking protocol (`grpc_health_v1`) alongside the ingestion
  service, reporting `SERVING` for both the empty service name (overall
  server health) and `ingestion.v1.TransactionIngestionService`
  specifically. `Shutdown` calls `health.Server.Shutdown()` first, which
  flips every registered service to `NOT_SERVING` so a load balancer polling
  the health check stops routing new traffic to this instance before
  `GracefulStop` starts draining in-flight RPCs. The REST server has no
  equivalent liveness/readiness endpoint yet.
- **Validation is duplicated per transport, not shared.** REST validates a
  `TransactionEventRequest` (JSON, amount fields as decimal strings);
  gRPC's `transactionIngestionServer.IngestTransaction` validates a
  generated `*ingestionv1.TransactionEvent` (already-typed proto fields,
  amount fields as pre-scaled `int64`). The two validation functions
  (`TransactionEventRequest.validate` and `validateTransactionEvent`)
  enforce the same rules but can't share an implementation because they
  operate on different wire types; keeping them in sync when a validation
  rule changes is a manual responsibility, not one enforced by the
  compiler.
- **Error mapping.** REST maps validation failures to `400`/`422` and a
  publish failure to `503` (see below). gRPC maps validation failures to
  `codes.InvalidArgument` and a publish failure to `codes.Unavailable`,
  the closest gRPC status equivalents.

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

### Benchmark results

`BenchmarkTransactionHandler` in `internal/api/transaction_handler_test.go`
covers the full decode -> validate -> publish hot path (publish goes to an
in-process `ingestion.Publisher` test double, not a live Kafka broker) end
to end through the real HTTP handler, with sub-benchmarks for the range of
payload sizes validation allows on the two variable-length fields
(`tenant_id`: 1-64 chars, `instrument`: 1-16 chars). Reported figures
include `httptest.NewRequest`/`httptest.NewRecorder` scaffolding allocated
fresh per iteration, so they aren't a pure figure for the handler in
isolation, but they're consistent across the before/after runs below.

"Before" is commit `b189903` (publish path landed, request/response buffer
pooling already in place, decoder/reader/event-struct pooling not yet
added). "After" is the current `sync.Pool`-backed decoder path on top of
the fixed-point `int64` amount representation (commit `eae5934`, which
replaced `shopspring/decimal`'s `*big.Int`-backed `ParseAmount` with
allocation-free fixed-point parsing — see
[DESIGN-ledger.md](DESIGN-ledger.md)). Run with `go test ./internal/api/...
-bench=BenchmarkTransactionHandler -benchmem -run='^$' -benchtime=2s` on
Apple M5 Pro / darwin/arm64, go1.26.5:

| Payload            | Before: ns/op | Before: B/op | Before: allocs/op | After: ns/op | After: B/op | After: allocs/op |
|--------------------|--------------:|-------------:|-------------------:|-------------:|------------:|-------------------:|
| typical             | 2637 | 8806 | 53 | 2489 | 8062 | 35 |
| min-length fields   | 2670 | 8745 | 51 | 2481 | 8120 | 33 |
| max-length fields   | 2942 | 8980 | 53 | 2818 | 8226 | 35 |

Decoder-buffer pooling (`9f69405`) plus the fixed-point `ParseAmount`
refactor (`eae5934`) together cut 18 allocations per request relative to
"Before"; the fixed-point change alone accounts for most of that drop
(50/48/50 allocs/op at `9f69405` down to 35/33/35 now), since
`decimal.Decimal` parsing allocated a `*big.Int` per `quantity`/`price`
field on every request, and `ParseAmount` on `int64` does not. Latency
(`ns/op`) tracks the allocation-count drop loosely but is noisier
run-to-run than `allocs/op`, which is exact. allocs/op scaling with the
size of `tenant_id`/`instrument` (33 at min length vs. 35 at max, both
before and after the fixed-point change) is expected:
`isValidTenantID`/`isValidInstrument` iterate the field's runes but don't
allocate, so the delta comes from `json.Decoder` parsing, not from
validation itself.

`BenchmarkTransactionIngestionServer_IngestTransaction` in
`internal/api/grpc_transaction_handler_test.go` covers the gRPC handler's
own validate -> convert -> publish path, calling `IngestTransaction`
directly rather than over a network connection, so it isolates the
handler's allocation behavior from gRPC transport/codec overhead (unlike
`BenchmarkTransactionHandler`, which includes `httptest` scaffolding). Run
under the same conditions as above, at `eae5934`:

| Payload            | ns/op | B/op | allocs/op |
|--------------------|------:|-----:|----------:|
| typical             | 143.0 | 807 | 1 |
| min-length fields   | 97.6  | 711 | 1 |
| max-length fields   | 125.3 | 861 | 1 |

The gRPC path allocates far less than the REST path (1 `allocs/op` vs. ~35)
for two reasons that are structural, not incidental: gRPC's request is
already a decoded, typed Go struct handed in by `grpc-go` before
`IngestTransaction` runs, so there's no JSON decode step to benchmark; and
`quantity`/`price` arrive as already-scaled `int64` wire values (see
[DESIGN-ledger.md](DESIGN-ledger.md)), so there's no `ParseAmount` string
parsing on this path either — REST's remaining ~35 allocs/op are now
dominated by `json.Decoder`/`encoding/json` reflection overhead rather
than amount parsing. The one gRPC-path allocation is the returned
`*ingestionv1.IngestTransactionResponse` itself, which escapes to the heap
because it's returned as a pointer across the gRPC handler boundary.

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
