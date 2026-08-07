// Package ledger defines the domain model for the transaction ledger and
// reconciled P&L state, plus the repository interfaces storage
// implementations must satisfy. Per this repo's ports-and-adapters layout,
// this package has no import dependency on storage/.
package ledger

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Side identifies the direction of a trade.
type Side string

// The two directions a Side may take.
const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// Transaction is one row of the append-only trade ledger. Quantity and
// Price are fixed-point, scaled by AmountScale.
type Transaction struct {
	ID            int64
	EventID       uuid.UUID
	TenantID      string
	SchemaVersion int16
	Instrument    string
	Side          Side
	Quantity      int64
	Price         int64
	Currency      string
	OccurredAt    time.Time
	ReceivedAt    time.Time
}

// ReconciledState is a tenant's running position and P&L for one
// instrument, updated idempotently as transactions are reconciled.
// Position, AverageCost, and RealizedPnL are fixed-point, scaled by
// AmountScale.
type ReconciledState struct {
	TenantID          string
	Instrument        string
	Position          int64
	AverageCost       int64
	RealizedPnL       int64
	LastTransactionID *int64
	UpdatedAt         time.Time
}

// ErrNotFound is returned when a lookup finds no matching row.
var ErrNotFound = errors.New("ledger: not found")

// ErrDuplicateEvent is returned by Insert when a transaction with the same
// EventID already exists, so callers can treat redelivery as a no-op
// rather than double-counting it.
var ErrDuplicateEvent = errors.New("ledger: duplicate event id")

// ErrStaleReconciledState is returned by ReconciledStateRepository.Upsert
// when state.LastTransactionID does not advance past the row's current
// LastTransactionID, so a caller computing an update from a since-outdated
// read can't clobber a newer one applied concurrently.
var ErrStaleReconciledState = errors.New("ledger: reconciled state upsert is stale")

// TransactionRepository persists and retrieves trade ledger entries.
type TransactionRepository interface {
	Insert(ctx context.Context, txn Transaction) (Transaction, error)
	GetByEventID(ctx context.Context, eventID uuid.UUID) (Transaction, error)
	ListByTenant(ctx context.Context, tenantID string, since time.Time) ([]Transaction, error)
}

// ReconciledStateRepository persists and retrieves per-tenant,
// per-instrument reconciled P&L state.
type ReconciledStateRepository interface {
	// Upsert writes state, applying it only if state.LastTransactionID
	// advances past the row's existing LastTransactionID (or the row
	// doesn't exist yet). It returns ErrStaleReconciledState, without
	// writing anything, if that isn't the case — implementations must
	// enforce this as a single atomic operation so two racing callers
	// can't both observe success while one clobbers the other's update.
	Upsert(ctx context.Context, state ReconciledState) error
	Get(ctx context.Context, tenantID, instrument string) (ReconciledState, error)
}
