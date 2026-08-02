-- reconciled_state holds the processor's running per-tenant, per-instrument
-- position and P&L, updated idempotently (UPSERT keyed on the primary key
-- below) as each transaction is reconciled.
CREATE TABLE reconciled_state (
    tenant_id            TEXT NOT NULL,
    instrument           TEXT NOT NULL,
    -- Net quantity held (buys minus sells); can go negative for a short
    -- position.
    position             NUMERIC(20, 8) NOT NULL DEFAULT 0,
    -- Weighted average cost basis per unit of the current open position.
    average_cost         NUMERIC(20, 8) NOT NULL DEFAULT 0,
    -- Cumulative realized P&L from closing/reducing positions.
    realized_pnl         NUMERIC(20, 4) NOT NULL DEFAULT 0,
    last_transaction_id  BIGINT REFERENCES transactions (id),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, instrument)
);
