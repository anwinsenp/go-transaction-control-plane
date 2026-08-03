-- Switches quantity/price/position/cost/P&L columns from NUMERIC to BIGINT,
-- storing the fixed-point value scaled by ledger.AmountScale (10^8)
-- directly, matching the Go domain's int64 representation. realized_pnl is
-- rescaled from its prior 10^4 precision to the shared 10^8 scale.
--
-- BIGINT tops out at 9223372036854775807, i.e. 92233720368.54775807 once
-- scaled by 1e8, a narrower ceiling than the NUMERIC(20,8) columns could
-- previously hold. The guard below fails the migration with a clear message
-- instead of letting a silent numeric-overflow error abort mid-ALTER on any
-- pre-existing row too large to convert.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM transactions
        WHERE quantity > 92233720368.54775807 OR price > 92233720368.54775807
    ) THEN
        RAISE EXCEPTION 'transactions contains a quantity or price too large to convert to BIGINT at 1e8 scale; audit/backfill offending rows before running this migration';
    END IF;

    IF EXISTS (
        SELECT 1 FROM reconciled_state
        WHERE abs(position) > 92233720368.54775807
           OR abs(average_cost) > 92233720368.54775807
           OR abs(realized_pnl) > 92233720368.54775807
    ) THEN
        RAISE EXCEPTION 'reconciled_state contains a position, average_cost, or realized_pnl too large to convert to BIGINT at 1e8 scale; audit/backfill offending rows before running this migration';
    END IF;
END $$;

ALTER TABLE transactions
    ALTER COLUMN quantity TYPE BIGINT USING (quantity * 100000000)::BIGINT,
    ALTER COLUMN price    TYPE BIGINT USING (price    * 100000000)::BIGINT;

ALTER TABLE reconciled_state
    ALTER COLUMN position      TYPE BIGINT USING (position      * 100000000)::BIGINT,
    ALTER COLUMN average_cost  TYPE BIGINT USING (average_cost  * 100000000)::BIGINT,
    ALTER COLUMN realized_pnl  TYPE BIGINT USING (realized_pnl  * 100000000)::BIGINT;
