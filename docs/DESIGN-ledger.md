# Design: Ledger Domain Model (`/internal/ledger`)

This is the low-level design for the transaction ledger's domain types, the
fixed-point amount representation shared across the whole stack, and the
Postgres schema those types map to. For where this fits in the overall
system, see [ARCHITECTURE.md](ARCHITECTURE.md). For the trade-off of
representing amounts as fixed-point `int64` instead of `decimal.Decimal`,
see [ADR 0004](decisions/0004-fixed-point-int64-amounts.md).

`internal/ledger` defines the domain types and repository interfaces; it
has no import dependency on `storage/` or `api/` per this repo's
ports-and-adapters layout. `internal/ledger/storage` implements the
repository interfaces against Postgres; `internal/api` and
`internal/ingestion` convert their own wire types to and from
`ledger`/`ingestion.Event` at the boundary.

## Domain types

### `Transaction`

One row of the append-only trade ledger.

| Field | Type | Notes |
|---|---|---|
| `ID` | `int64` | Postgres-assigned primary key. |
| `EventID` | `uuid.UUID` | Client-supplied idempotency key; unique, enforced at the storage layer via `ErrDuplicateEvent`. |
| `TenantID` | `string` | 1-64 lowercase alphanumeric/hyphen, validated at the API layer. |
| `SchemaVersion` | `int16` | Kafka payload schema version this row was ingested under. |
| `Instrument` | `string` | 1-16 uppercase alphanumeric/dot, validated at the API layer. |
| `Side` | `Side` (`"BUY"` \| `"SELL"`) | |
| `Quantity` | `int64` | Fixed-point, scaled by `AmountScale`. |
| `Price` | `int64` | Fixed-point, scaled by `AmountScale`. |
| `Currency` | `string` | 3-letter uppercase ISO 4217 code. |
| `OccurredAt` | `time.Time` | Client-supplied event time. |
| `ReceivedAt` | `time.Time` | Server-assigned ingestion time. |

### `ReconciledState`

A tenant's running position and P&L for one instrument, updated
idempotently as transactions are reconciled.

| Field | Type | Notes |
|---|---|---|
| `TenantID` | `string` | |
| `Instrument` | `string` | |
| `Position` | `int64` | Fixed-point, scaled by `AmountScale`. Signed: positive for a net long position, negative for net short. |
| `AverageCost` | `int64` | Fixed-point, scaled by `AmountScale`. |
| `RealizedPnL` | `int64` | Fixed-point, scaled by `AmountScale`. Signed. |
| `LastTransactionID` | `*int64` | The last `Transaction.ID` folded into this state; nil before any transaction has been reconciled. |
| `UpdatedAt` | `time.Time` | |

### Repository interfaces

```go
type TransactionRepository interface {
    Insert(ctx context.Context, txn Transaction) (Transaction, error)
    GetByEventID(ctx context.Context, eventID uuid.UUID) (Transaction, error)
    ListByTenant(ctx context.Context, tenantID string, since time.Time) ([]Transaction, error)
}

type ReconciledStateRepository interface {
    Upsert(ctx context.Context, state ReconciledState) error
    Get(ctx context.Context, tenantID, instrument string) (ReconciledState, error)
}
```

`Insert` returns `ErrDuplicateEvent` (not a generic storage error) when a
transaction with the same `EventID` already exists, so a caller processing
Kafka's at-least-once delivery can treat redelivery as a no-op rather than
double-counting it. `GetByEventID`/`Get` return `ErrNotFound` for a missing
row, so callers can `errors.Is` against a single sentinel instead of
string-matching a driver error.

## Fixed-point amount representation

Every quantity, price, position, average cost, and P&L figure in the
domain is an `int64` holding the real decimal value multiplied by
`ledger.AmountScale` (`10^8`, i.e. 8 decimal places of precision). This
matches the proto wire format's `int64` fields and the Postgres `BIGINT`
columns exactly, so no conversion happens at either boundary; only the
REST API's string wire format needs `ParseAmount`/`FormatAmount` to convert
at all. See [ADR 0004](decisions/0004-fixed-point-int64-amounts.md) for why
this replaced `decimal.Decimal`.

`ledger.MaxAmount` (`10^10 * AmountScale` = `10,000,000,000` whole units)
bounds every field so that scaled arithmetic, including later
multiplication or summation during reconciliation, can never overflow
`int64`. Both the REST and gRPC handlers reject `quantity`/`price` outside
`(0, MaxAmount]` before publishing to Kafka.

### `ParseAmount`

`ParseAmount(value string) (int64, error)` parses a decimal string,
optionally in scientific notation (e.g. `"123.45000000"`, `"1.5e3"`), into
its `AmountScale`-scaled `int64`. It accepts an optional leading sign and
does not itself enforce `MaxAmount` or positivity; callers check the
returned value against those separately.

| Step | Behavior |
|---|---|
| Sign | Optional leading `+`/`-`; absence means positive. |
| Exponent | Optional `e`/`E` suffix; the exponent is parsed and clamped to `[-1000, 1000]` before use. An exponent outside that range is rejected, not clamped silently. |
| Mantissa | Split on `.` into whole/fractional parts; both must be all-ASCII-digit (fractional part may be empty). |
| Magnitude | Whole and fractional digits are concatenated and parsed as a `uint64`. |
| Scaling | The net power of ten (`exponent - len(fracPart) + amountFractionDigits`) is applied via repeated multiplication (if `>= 0`) or division with an exact-divisibility check (if `< 0`, rejecting values with more than 8 fractional digits after the exponent is applied). |
| Overflow | The scaled magnitude is checked against `math.MaxInt64` before conversion; oversized values return an error rather than wrapping. |

Edge cases the implementation specifically guards against:

- **Exponent-driven denial of service.** A power-of-ten loop bounded only
  by the parsed exponent would hang on adversarial input like
  `"0e9223372036854775807"`. The exponent is clamped to `[-1000, 1000]`
  before any loop runs, and the internal `mulPow10` helper additionally
  caps its own loop at 19 iterations (`uint64`'s decimal digit ceiling) and
  short-circuits a zero mantissa to `0` before even entering the loop.
- **More than 8 fractional digits.** Rejected outright (`"has more than 8
  fractional digits"`), not rounded, since silently rounding a client's
  submitted quantity or price is a correctness risk this domain doesn't
  accept.
- **`math.MinInt64` in `FormatAmount`.** Negating `math.MinInt64` directly
  overflows back to itself in two's complement. `FormatAmount` shifts the
  value by one before negating, then folds the `+1` back in as a `uint64`,
  to keep every intermediate value representable.

### `FormatAmount`

`FormatAmount(value int64) string` renders a fixed-point `int64` back into
a decimal string with exactly 8 fraction digits (e.g.
`FormatAmount(1234500000) == "12.34500000"`). It's a pure formatting
inverse of `ParseAmount`'s scaling step and is used to render `MaxAmount`
into user-facing validation error messages, not just in tests.

## Postgres schema

`transactions.quantity`, `transactions.price`, `reconciled_state.position`,
`reconciled_state.average_cost`, and `reconciled_state.realized_pnl` are
all `BIGINT`, storing the `AmountScale`-scaled value directly, no
conversion at read/write time. Migration `0003_amounts_to_bigint`
(`internal/ledger/storage/migrations/`) converted these from
`NUMERIC(20,8)` (`realized_pnl` was `NUMERIC(20,4)`, i.e. a different scale
than the other fields, unified to `1e8` in the same migration).

### Migration overflow guard

`BIGINT` at `1e8` scale tops out at `92233720368.54775807`, narrower than
`NUMERIC(20,8)` could hold. The `up` migration runs a preflight check
before the `ALTER TABLE` statements:

1. Scan `transactions` for any row where `quantity` or `price` exceeds
   `92233720368.54775807`.
2. Scan `reconciled_state` for any row where `abs(position)`,
   `abs(average_cost)`, or `abs(realized_pnl)` exceeds the same ceiling.
3. If either scan finds a match, `RAISE EXCEPTION` with a message naming
   the table and telling the operator to audit/backfill the offending rows,
   failing the migration cleanly.
4. Only if both scans pass does the migration proceed to the `ALTER TABLE
   ... USING (value * 100000000)::BIGINT` conversions.

This guard was verified against a live Postgres instance, including
confirming it actually fires (rather than the `ALTER` itself failing with
a generic numeric-overflow error) when a row is deliberately seeded above
the ceiling. The `down` migration reverses the conversion back to
`NUMERIC(20,8)` (`realized_pnl` back to `NUMERIC(20,4)`), dividing by
`100000000.0`.

## Why not X

- **Why not keep amounts as decimal strings all the way to Postgres and
  parse only at reconciliation time?** Rejected: it would push
  `ParseAmount`'s validation and overflow risk into the processor's
  reconciliation path instead of catching malformed/oversized values at
  ingestion, and it would mean storing `NUMERIC` (or `TEXT`) in Postgres
  instead of `BIGINT`, giving up the direct match to the Go and proto
  representations that motivated this design. See
  [ADR 0004](decisions/0004-fixed-point-int64-amounts.md) for the full
  comparison against `decimal.Decimal` and `float64`.
- **Why clamp the exponent to `[-1000, 1000]` instead of just bounding the
  final magnitude?** The magnitude bound alone doesn't stop the loop from
  running an unbounded number of iterations before it ever reaches that
  check; the exponent has to be bounded before any power-of-ten
  computation starts, not after.
