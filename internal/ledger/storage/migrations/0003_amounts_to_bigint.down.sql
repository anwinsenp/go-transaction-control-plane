ALTER TABLE transactions
    ALTER COLUMN quantity TYPE NUMERIC(20, 8) USING (quantity / 100000000.0)::NUMERIC(20, 8),
    ALTER COLUMN price    TYPE NUMERIC(20, 8) USING (price    / 100000000.0)::NUMERIC(20, 8);

ALTER TABLE reconciled_state
    ALTER COLUMN position      TYPE NUMERIC(20, 8) USING (position      / 100000000.0)::NUMERIC(20, 8),
    ALTER COLUMN average_cost  TYPE NUMERIC(20, 8) USING (average_cost  / 100000000.0)::NUMERIC(20, 8),
    ALTER COLUMN realized_pnl  TYPE NUMERIC(20, 4) USING (realized_pnl  / 100000000.0)::NUMERIC(20, 4);
