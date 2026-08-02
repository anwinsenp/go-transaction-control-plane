-- transactions is the append-only trade ledger written by the processor
-- (see requirements.md "Transaction processor" and ADR 0001). One row per
-- Kafka event.
CREATE TABLE transactions (
    -- BIGSERIAL (sequential) rather than the natural UUID event_id, so
    -- inserts stay index-append-friendly under high concurrency instead of
    -- causing random-UUID page splits on the primary key's btree.
    id              BIGSERIAL PRIMARY KEY,
    -- Idempotency key carried on the Kafka payload; the processor's
    -- at-least-once redelivery relies on this UNIQUE constraint to reject
    -- duplicate inserts, which TransactionStore.Insert catches (Postgres
    -- error 23505) and maps to ledger.ErrDuplicateEvent rather than
    -- double-counting.
    event_id        UUID NOT NULL UNIQUE,
    tenant_id       TEXT NOT NULL,
    schema_version  SMALLINT NOT NULL,
    instrument      TEXT NOT NULL,
    side            TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity        NUMERIC(20, 8) NOT NULL CHECK (quantity > 0),
    price           NUMERIC(20, 8) NOT NULL CHECK (price > 0),
    currency        CHAR(3) NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serves per-tenant reconciliation/reporting queries ordered by trade time
-- (e.g. "this tenant's trades since X"), the main read pattern outside the
-- event_id dedup lookup already covered by the UNIQUE constraint above.
CREATE INDEX idx_transactions_tenant_occurred_at
    ON transactions (tenant_id, occurred_at);

-- Serves the processor's per-instrument position/cost-basis lookups when
-- reconciling a tenant's P&L for a given instrument.
CREATE INDEX idx_transactions_tenant_instrument
    ON transactions (tenant_id, instrument);
