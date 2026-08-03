# 0003. Postgres connection pool sizing per environment

Date: 2026-08-03
Status: Accepted

## Context

`internal/ledger/storage/pool.go` builds every `pgxpool.Pool` used against
the ledger's Postgres instance. Left at pgxpool's own defaults (100 max
conns, no `MaxConnLifetime`/`MaxConnIdleTime` cap), a pool doesn't account
for how many replicas of the ingestion and processor services run
concurrently against the same Postgres instance, or for a managed
database's own connection ceiling. A handful of replicas each opening up
to 100 connections can exhaust a modestly-sized instance well before any
individual service is under real load. Issue #3 asked for pool settings
that are configurable, documented, and differ sensibly between local dev
and the sandbox deployment; a code review on that issue's PR flagged that
the rationale for the chosen defaults lived only in doc comments on
`LocalPoolConfig()`/`SandboxPoolConfig()`, with no standalone doc — this
ADR gives that rationale a proper home, alongside those comments rather
than replacing them.

## Decision

Every pool goes through `storage.PoolConfig` instead of `pgxpool.New`'s
defaults. Two named profiles cover the deployments this project targets,
selected by the `APP_ENV` environment variable and overridable per field:

- **`LocalPoolConfig()`** — `MaxConns: 5`, `MinConns: 1`,
  `MaxConnLifetime: 30 * time.Minute`, `MaxConnIdleTime: 5 * time.Minute`.
  The docker-compose stack runs a single Postgres instance with no other
  tenants, so the pool can stay small without risking exhaustion and
  doesn't need aggressive idle reclamation.
- **`SandboxPoolConfig()`** — `MaxConns: 20`, `MinConns: 5`,
  `MaxConnLifetime: 30 * time.Minute`, `MaxConnIdleTime: 2 * time.Minute`.
  The sandbox runs a modestly-sized managed Postgres instance shared by
  every replica of both the ingestion and processor services. `MaxConns`
  is kept conservative per-service so a handful of replicas can't
  collectively exhaust the database's own connection ceiling.
  `MaxConnIdleTime` is shorter than local's so idle connections are
  returned quickly, leaving headroom for bursts from the public endpoint's
  traffic. `MaxConnLifetime` rotates connections periodically so
  long-lived ones don't pin a stale route behind a load balancer or
  pooler.

`PoolConfigFromEnv()` resolves `APP_ENV` ("local" or "sandbox", defaulting
to "local" if unset) to one of the two profiles above, then applies
per-field overrides from `PGPOOL_MAX_CONNS`, `PGPOOL_MIN_CONNS`,
`PGPOOL_MAX_CONN_LIFETIME`, and `PGPOOL_MAX_CONN_IDLE_TIME` if set (the
duration variables accept anything `time.ParseDuration` understands, e.g.
`"30m"`). `PoolConfig.validate()` then enforces: `MaxConns >= 1` (a
zero-sized pool can never hand out a connection), `MinConns >= 0`,
`MinConns <= MaxConns` (pgxpool otherwise silently fails to pre-warm idle
connections beyond `MaxConns`), and both duration fields strictly `> 0`
(unlike pgxpool's own zero value, which means "no cap" — that's not a
setting this project wants reachable). `validate()` runs both inside
`PoolConfigFromEnv()` and again inside `NewPool()`, so a `PoolConfig{}`
constructed by hand and passed straight to `NewPool()` can't bypass the
same checks the env path enforces.

## Consequences

- Pool sizing is data-driven per environment and testable in isolation
  (`LocalPoolConfig()`/`SandboxPoolConfig()`/`PoolConfigFromEnv()` are
  plain functions with no I/O), and can be tuned via env vars without a
  code change.
- Settings are still fixed at process startup: `NewPool()` reads `cfg`
  once when the pool is built. There's no live reconfiguration — changing
  `MaxConns` for a running deployment means changing the env var and
  redeploying, not an in-place update.
- The sandbox numbers (`MaxConns: 20`, `MinConns: 5`,
  `MaxConnIdleTime: 2m`) are provisional. They're sized from the
  deployment topology (replica count × service count vs. instance
  connection ceiling), not from observed load. `requirements.md`'s
  load-testing section calls for documenting real P99 latency/throughput
  from an actual sandbox run; once that data exists, these defaults should
  be revisited and this ADR updated if they change materially.
- Nothing in `cmd/` currently calls `NewPool()` — there's no production
  wiring yet that constructs a pool from `PoolConfigFromEnv()` and holds
  it for the ingestion or processor service. This ADR documents the
  sizing decision now so it's in place before that wiring lands, but the
  defaults haven't been exercised against a running service yet.

## Alternatives considered

- **Single fixed `PoolConfig` for all environments.** Rejected: local dev
  and the sandbox have different connection-ceiling constraints (a
  single-tenant docker-compose Postgres vs. a shared managed instance),
  and one set of numbers would either be too small for local iteration or
  too large for the sandbox's shared instance.
- **Leaving pool sizing at pgxpool's built-in defaults.** Rejected for the
  reason in Context: 100 max conns per pool times several service
  replicas has no relationship to what a shared Postgres instance can
  actually support, and there's no idle/lifetime cap to bound connection
  churn or stale routing behind a load balancer.
- **A single `PGPOOL_*`-only configuration with no named profiles.**
  Considered, since env var overrides alone can express any pool shape.
  Rejected because it would push every deployment (including local dev)
  into having to set four env vars just to get a reasonable value,
  instead of a sane default that only needs overriding when it's actually
  wrong for a given deployment.
