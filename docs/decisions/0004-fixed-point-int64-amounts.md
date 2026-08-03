# 0004. Fixed-point int64 amounts instead of decimal.Decimal

Date: 2026-08-03
Status: Accepted

## Context

Quantity, price, position, average cost, and realized P&L originally used
`github.com/shopspring/decimal`. `decimal.Decimal` is backed by `*big.Int`,
so every parse or arithmetic operation on it allocates, which conflicts
directly with the ingestion hot path's zero-allocation discipline
(`CLAUDE.md`). The domain also already commits to a fixed 8-decimal-place
precision for every one of these fields, and both the proto wire format and
the Postgres schema need to agree with the Go representation at each
boundary regardless of which in-memory type is chosen. Carrying an
arbitrary-precision type through the whole stack just to convert it at every
boundary anyway wasn't buying anything.

## Decision

Represent Quantity, Price, Position, AverageCost, and RealizedPnL as `int64`
scaled by `ledger.AmountScale` (`1e8`) everywhere: the Go domain types
(`ledger.Transaction`, `ledger.ReconciledState`, `ingestion.Event`), the
gRPC wire format, and the Postgres columns (`BIGINT`). `internal/ledger/amount.go`
provides `ParseAmount`/`FormatAmount` to convert the REST API's string wire
format to and from the scaled `int64`; the gRPC wire format carries the
scaled `int64` directly, so no string parsing happens on that path.
`ledger.MaxAmount` (`10^10` whole units) bounds every field so scaled
arithmetic can never overflow `int64`, including later multiplication or
summation during reconciliation. Migration `0003_amounts_to_bigint`
converts the existing `NUMERIC` columns to `BIGINT`, with a preflight guard
that fails the migration outright if any existing row is too large to
convert rather than aborting mid-`ALTER` with a cryptic Postgres error.
`realized_pnl`'s scale is unified from its prior `1e4` to the shared `1e8`
in the same migration; no reconciler arithmetic exists yet, so this carried
no behavioral risk.

## Consequences

- Removes `decimal.Decimal`'s `*big.Int` heap allocation from the ingestion
  hot path; validating a request's quantity/price no longer allocates for
  the numeric value itself.
- `shopspring/decimal` is removed from `go.mod` entirely.
- Precision is fixed at exactly 8 decimal digits everywhere. A value with
  more fractional digits than that is rejected by `ParseAmount`, not
  silently rounded.
- `MaxAmount` is a hard ceiling baked into `int64`'s range at this scale.
  Raising it later means widening the integer type or lowering the scale,
  either of which is a breaking change to the wire format and the Postgres
  schema, not a config change.
- Amount parsing is now hand-rolled code that has to defend itself against
  adversarial input (e.g. an attacker-controlled exponent), a
  responsibility a well-audited third-party library previously carried.
  `amount.go` bounds the parsed exponent to `[-1000, 1000]` and caps
  `mulPow10`'s loop at 19 iterations before it will even attempt the
  multiplication, specifically to avoid an unbounded loop on a string like
  `"0e9223372036854775807"`. That mitigation needs to stay correct under
  future changes to `amount.go`, since there's no library maintainer doing
  that auditing anymore.
- `BIGINT` at `1e8` scale tops out at `92233720368.54775807`, narrower than
  the previous `NUMERIC(20,8)` columns could hold. The migration's
  preflight guard surfaces any row that doesn't fit instead of letting the
  `ALTER` fail partway through.

## Alternatives considered

- **Keep `decimal.Decimal`.** Rejected: its `*big.Int`-backed arithmetic
  allocates on every operation, which is directly at odds with the
  ingestion hot path's zero-allocation goal.
- **`float64`.** Rejected: IEEE 754 doubles can't exactly represent every
  8-decimal-digit fixed-point value (the classic `0.1 + 0.2` rounding
  problem), which is not acceptable for a P&L figure where rounding drift
  compounds across reconciled positions over time.
- **Raw `math/big.Int` without a fixed scale.** Rejected: still allocates
  on the hot path, and buys nothing over `decimal.Decimal` here since every
  field's precision is already fixed at 8 decimals by the domain, not
  variable per value.
