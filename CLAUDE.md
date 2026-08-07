# Project Context — go-transaction-control-plane

## What this project is
A distributed, real-time transaction processing engine in Go: a
zero-allocation ingestion hot path, Kafka-based event streaming, a
reconciliation/storage layer on Postgres, a custom Kubernetes operator for
tenant scaling, and Prometheus telemetry. The goal is production-quality,
portfolio-grade code — assume it will be read closely by engineers
evaluating it on GitHub, not just run once and discarded.

## Language & tooling
- Go version: target whatever is pinned in `go.mod`; otherwise assume a
  recent stable Go (1.22+).
- Dependencies are expected here (Kafka client, Postgres driver,
  `controller-runtime`, `client_golang`, etc.) — this is not a
  standard-library-only project. Still keep the dependency list deliberate:
  before adding a new one, check whether the standard library or an
  existing dependency already covers it.
- Every change must pass before it's considered done:
  `gofmt -l .`, `go vet ./...`, `go build ./...`, `go test ./...`.
  Run them, don't just assume they'd pass.

## Coding standards
- Idiomatic Go: small interfaces, accept interfaces / return structs, no
  needless abstraction.
- Errors: wrap with `fmt.Errorf("...: %w", err)`, never swallow, never use
  panic for expected control flow. Sentinel errors via `errors.New` /
  `errors.Is` / `errors.As` where it aids the caller.
- Concurrency: every goroutine has a clear owner and a clear exit path. Use
  `context.Context` as the first parameter for anything that can block or is
  cancellable. Guard shared state explicitly (mutex or channel) — call out
  the chosen strategy in a comment if it's non-obvious.
- No global mutable state unless the task is trivial enough that it's
  clearly fine.
- Table-driven design for anything with >2 branches worth testing.
- Prefer clarity over cleverness. If a one-liner needs a comment to explain
  itself, it's probably better as three plain lines.
- Minimal comments. Comment only the "why" for non-obvious decisions (a
  tricky concurrency choice, a workaround for a specific gotcha, a
  deliberate trade-off) — never the "what" a reader can already see from the
  code itself. Don't add a comment above every block or line; that reads as
  AI-generated and undermines idiomatic Go, where naming and structure
  should carry the meaning. Exported identifiers still get a proper doc
  comment (`// FuncName does X`) per Go convention — that's required, not
  "extra."
- Variable names should be at least 3 characters and descriptive of what
  they hold — avoid single/double-letter names (e.g. `err`, `req`, `usr`
  rather than `e`, `r`, `u`). Standard short idioms are still fine: `err`
  for errors, `ctx` for context, `i`/`j` for simple loop indices, `ok` for
  comma-ok checks.

## Performance discipline (hot path)
This applies specifically to the ingestion service and anything on the
transaction hot path — not the whole codebase.
- Zero/low allocation is a design goal, not an afterthought: prefer
  `sync.Pool` for reused buffers/objects, avoid unnecessary interface
  boxing, be deliberate about slice growth (preallocate with known
  capacity where possible).
- Before claiming a performance property, back it up: use `go test -bench`
  and `pprof` (`-benchmem`, alloc profiles) rather than asserting it in a
  comment. If a benchmark doesn't exist yet for a hot-path change, add one.
- Don't apply this discipline outside the hot path (e.g. the operator's
  reconcile loop, the Terraform tooling, one-off scripts) — over-optimizing
  cold paths just adds noise.

## Repo layout
This repo has three distinct layers with different rules — don't blend
them.

```
/cmd/<service>/main.go     — entrypoints (ingestion, processor). Wiring and
                              config load only, no business logic.
/internal/<domain>/        — core business logic, layered so domain code
                              has no import dependency on transport or
                              storage (domain defines the interfaces it
                              needs; storage/transport implement/consume
                              them — lightweight ports-and-adapters, not
                              full ceremony).
/internal/<domain>/storage/ — concrete Postgres/Kafka implementations of
                              the interfaces defined in the domain package.
/internal/api/             — gRPC/REST handlers, DTOs, request validation.
                              Talks to /internal/<domain> only through its
                              exported interface, never reaches into
                              storage directly.
/operator/                 — the custom Kubernetes controller: CRD types,
                              reconciler loop, built with controller-runtime.
                              Treat this as its own Go module/boundary —
                              don't let ingestion/processor code depend on
                              anything here or vice versa.
/terraform/                — infrastructure definitions (EC2 + k3s, VPC,
                              etc). Modular, one concern per module. Not Go
                              — apply Terraform conventions, not the Go
                              rules above, when touching these files.
```
- Package names: short, lowercase, no underscores, named after what they
  provide.
- One `_test.go` file alongside each source file it tests.

## Kubernetes operator specifics (`/operator`)
- Reconcile functions must be idempotent and safe to call repeatedly with
  the same object state — no assumptions about call frequency or ordering.
- Every reconcile loop iteration should be bounded (context with timeout)
  and must not block indefinitely on external calls.
- Status subresource updates and spec reads are separate concerns — don't
  mutate spec from within a reconcile loop that's meant to just observe.
- Errors returned from `Reconcile` should be requeued via the controller's
  standard retry, not swallowed or retried manually with sleep loops.

## Telemetry (Prometheus)
- Metrics are exposed via `client_golang` on a dedicated `/metrics`
  endpoint per service.
- Naming follows Prometheus convention: `snake_case`, unit-suffixed
  (`_seconds`, `_bytes`, `_total` for counters).
- Minimum expected metrics: transaction throughput (counter), P50/P99
  latency (histogram, sensible bucket boundaries for microsecond-to-low-ms
  range), Kafka consumer lag (gauge), reconciler loop duration (histogram,
  in `/operator`).
- Don't add high-cardinality labels (e.g. raw transaction IDs) — this is a
  common Prometheus footgun and will be flagged in review.

## How to work through a task
1. Restate the problem and constraints briefly before writing code.
2. If requirements are ambiguous, state the assumption and move on rather
   than stalling on it.
3. Sketch the approach (types/signatures) before filling in logic,
   especially for anything non-trivial or touching the hot path.
4. Write the code.
5. Self-review: while iterating, scope gofmt/vet/build/test (and
   benchmarks, if hot-path) to the package(s) you're touching — a full
   `./...` run on every iteration is wasted work. Run the full-repo gate
   (`gofmt -l .`, `go vet ./...`, `go build ./...`, `go test ./...`) exactly
   once, as the last step before calling the task done, then re-read the
   diff once for naming, error handling, and edge cases.
6. Summarize what changed and why in plain language.

## Guardrails (things not to do without asking)
- Don't add a dependency, run `go get`, or touch `go.mod`/`go.sum` without
  flagging it first.
- Don't delete or rewrite existing tests to make them pass — fix the code,
  or flag if the test itself looks wrong.
- Don't silently change public function signatures in existing code without
  calling it out — that's an API break.
- Don't restructure/rename things outside the scope of the current task
  "while I'm in here" — mention it as a suggestion instead.
- Don't blend the layout rules above across layers (e.g. don't let
  `/operator` import from `/internal`, don't put SQL outside `storage/`).

