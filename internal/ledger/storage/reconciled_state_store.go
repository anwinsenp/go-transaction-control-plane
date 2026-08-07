package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// ReconciledStateStore is the Postgres-backed ledger.ReconciledStateRepository.
type ReconciledStateStore struct {
	pool *pgxpool.Pool
}

// NewReconciledStateStore returns a ReconciledStateStore backed by pool.
func NewReconciledStateStore(pool *pgxpool.Pool) *ReconciledStateStore {
	return &ReconciledStateStore{pool: pool}
}

var _ ledger.ReconciledStateRepository = (*ReconciledStateStore)(nil)

// Upsert writes state, replacing any existing row for the same
// (tenant_id, instrument) pair, but only if state.LastTransactionID
// advances the row's existing watermark (or no row exists yet) — enforced
// by the conflict action's WHERE clause, so the compare-and-swap is atomic
// with the write rather than a separate read-then-write race. If an
// existing row's watermark has already reached or passed
// state.LastTransactionID, no write happens and ErrStaleReconciledState is
// returned, matching the processor's idempotent-reconciliation
// requirement: replaying an already-applied (or superseded) update is a
// no-op rather than clobbering newer state.
func (store *ReconciledStateStore) Upsert(ctx context.Context, state ledger.ReconciledState) error {
	const query = `
		INSERT INTO reconciled_state
			(tenant_id, instrument, position, average_cost, realized_pnl, last_transaction_id, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (tenant_id, instrument) DO UPDATE SET
			position = EXCLUDED.position,
			average_cost = EXCLUDED.average_cost,
			realized_pnl = EXCLUDED.realized_pnl,
			last_transaction_id = EXCLUDED.last_transaction_id,
			updated_at = EXCLUDED.updated_at
		WHERE reconciled_state.last_transaction_id IS NULL
			OR reconciled_state.last_transaction_id < EXCLUDED.last_transaction_id`

	tag, err := store.pool.Exec(ctx, query,
		state.TenantID, state.Instrument, state.Position, state.AverageCost,
		state.RealizedPnL, state.LastTransactionID)
	if err != nil {
		return fmt.Errorf("upsert reconciled state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ledger.ErrStaleReconciledState
	}
	return nil
}

// Get returns the reconciled state for a tenant's instrument.
func (store *ReconciledStateStore) Get(ctx context.Context, tenantID, instrument string) (ledger.ReconciledState, error) {
	const query = `
		SELECT tenant_id, instrument, position, average_cost, realized_pnl, last_transaction_id, updated_at
		FROM reconciled_state
		WHERE tenant_id = $1 AND instrument = $2`

	var state ledger.ReconciledState
	err := store.pool.QueryRow(ctx, query, tenantID, instrument).Scan(
		&state.TenantID, &state.Instrument, &state.Position, &state.AverageCost,
		&state.RealizedPnL, &state.LastTransactionID, &state.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.ReconciledState{}, ledger.ErrNotFound
		}
		return ledger.ReconciledState{}, fmt.Errorf("get reconciled state: %w", err)
	}
	return state, nil
}
