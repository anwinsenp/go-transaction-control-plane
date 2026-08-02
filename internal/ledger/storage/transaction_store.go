// Package storage provides the Postgres implementation of the repository
// interfaces defined in internal/ledger.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

const pgUniqueViolation = "23505"

// TransactionStore is the Postgres-backed ledger.TransactionRepository.
type TransactionStore struct {
	pool *pgxpool.Pool
}

// NewTransactionStore returns a TransactionStore backed by pool.
func NewTransactionStore(pool *pgxpool.Pool) *TransactionStore {
	return &TransactionStore{pool: pool}
}

var _ ledger.TransactionRepository = (*TransactionStore)(nil)

// Insert writes a new transaction. If a transaction with the same EventID
// already exists, it returns ledger.ErrDuplicateEvent so callers can treat
// Kafka's at-least-once redelivery as a no-op instead of double-counting.
func (store *TransactionStore) Insert(ctx context.Context, txn ledger.Transaction) (ledger.Transaction, error) {
	const query = `
		INSERT INTO transactions
			(event_id, tenant_id, schema_version, instrument, side, quantity, price, currency, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, event_id, tenant_id, schema_version, instrument, side, quantity, price, currency, occurred_at, received_at`

	row := store.pool.QueryRow(ctx, query,
		txn.EventID, txn.TenantID, txn.SchemaVersion, txn.Instrument, txn.Side,
		txn.Quantity, txn.Price, txn.Currency, txn.OccurredAt)

	inserted, err := scanTransaction(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return ledger.Transaction{}, ledger.ErrDuplicateEvent
		}
		return ledger.Transaction{}, fmt.Errorf("insert transaction: %w", err)
	}
	return inserted, nil
}

// GetByEventID looks up a transaction by its Kafka-payload idempotency key.
func (store *TransactionStore) GetByEventID(ctx context.Context, eventID uuid.UUID) (ledger.Transaction, error) {
	const query = `
		SELECT id, event_id, tenant_id, schema_version, instrument, side, quantity, price, currency, occurred_at, received_at
		FROM transactions
		WHERE event_id = $1`

	txn, err := scanTransaction(store.pool.QueryRow(ctx, query, eventID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledger.Transaction{}, ledger.ErrNotFound
		}
		return ledger.Transaction{}, fmt.Errorf("get transaction by event id: %w", err)
	}
	return txn, nil
}

// ListByTenant returns a tenant's transactions occurring at or after since,
// ordered by occurred_at.
func (store *TransactionStore) ListByTenant(ctx context.Context, tenantID string, since time.Time) ([]ledger.Transaction, error) {
	const query = `
		SELECT id, event_id, tenant_id, schema_version, instrument, side, quantity, price, currency, occurred_at, received_at
		FROM transactions
		WHERE tenant_id = $1 AND occurred_at >= $2
		ORDER BY occurred_at`

	rows, err := store.pool.Query(ctx, query, tenantID, since)
	if err != nil {
		return nil, fmt.Errorf("list transactions by tenant: %w", err)
	}
	defer rows.Close()

	transactions := make([]ledger.Transaction, 0)
	for rows.Next() {
		txn, err := scanTransaction(rows)
		if err != nil {
			return nil, fmt.Errorf("list transactions by tenant: %w", err)
		}
		transactions = append(transactions, txn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list transactions by tenant: %w", err)
	}
	return transactions, nil
}

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query),
// letting scanTransaction serve both single-row and multi-row callers.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanTransaction(row rowScanner) (ledger.Transaction, error) {
	var txn ledger.Transaction
	err := row.Scan(
		&txn.ID, &txn.EventID, &txn.TenantID, &txn.SchemaVersion, &txn.Instrument, &txn.Side,
		&txn.Quantity, &txn.Price, &txn.Currency, &txn.OccurredAt, &txn.ReceivedAt)
	if err != nil {
		return ledger.Transaction{}, err
	}
	return txn, nil
}
