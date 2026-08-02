package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

func TestReconciledStateStore_UpsertInsertsAndUpdates(t *testing.T) {
	pool := testPool(t)
	txnStore := NewTransactionStore(pool)
	store := NewReconciledStateStore(pool)
	ctx := context.Background()

	txn, err := txnStore.Insert(ctx, sampleTransaction())
	if err != nil {
		t.Fatalf("Insert transaction: %v", err)
	}

	initial := ledger.ReconciledState{
		TenantID:          txn.TenantID,
		Instrument:        txn.Instrument,
		Position:          decimal.NewFromInt(10),
		AverageCost:       decimal.NewFromFloat(189.50),
		RealizedPnL:       decimal.Zero,
		LastTransactionID: &txn.ID,
	}
	if err := store.Upsert(ctx, initial); err != nil {
		t.Fatalf("Upsert (insert): %v", err)
	}

	got, err := store.Get(ctx, initial.TenantID, initial.Instrument)
	if err != nil {
		t.Fatalf("Get after insert: %v", err)
	}
	if !got.Position.Equal(initial.Position) {
		t.Fatalf("Get after insert: position = %s, want %s", got.Position, initial.Position)
	}

	updated := initial
	updated.Position = decimal.NewFromInt(20)
	updated.RealizedPnL = decimal.NewFromFloat(15.25)
	if err := store.Upsert(ctx, updated); err != nil {
		t.Fatalf("Upsert (update): %v", err)
	}

	got, err = store.Get(ctx, initial.TenantID, initial.Instrument)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if !got.Position.Equal(updated.Position) {
		t.Fatalf("Get after update: position = %s, want %s", got.Position, updated.Position)
	}
	if !got.RealizedPnL.Equal(updated.RealizedPnL) {
		t.Fatalf("Get after update: realized P&L = %s, want %s", got.RealizedPnL, updated.RealizedPnL)
	}
}

func TestReconciledStateStore_GetNotFound(t *testing.T) {
	pool := testPool(t)
	store := NewReconciledStateStore(pool)

	_, err := store.Get(context.Background(), "no-such-tenant", "AAPL")
	if !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("got err %v, want ledger.ErrNotFound", err)
	}
}

func TestReconciledStateStore_PrecisionAndSign(t *testing.T) {
	tests := []struct {
		name        string
		position    string
		averageCost string
		realizedPnL string
	}{
		{
			name:        "short position",
			position:    "-25.5",
			averageCost: "100.12345678",
			realizedPnL: "0",
		},
		{
			name:        "realized loss",
			position:    "0",
			averageCost: "0",
			realizedPnL: "-500.1234",
		},
		{
			name:        "realized pnl at full 4-decimal precision",
			position:    "10",
			averageCost: "189.50000000",
			realizedPnL: "-1234.5678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := testPool(t)
			txnStore := NewTransactionStore(pool)
			store := NewReconciledStateStore(pool)
			ctx := context.Background()

			txn, err := txnStore.Insert(ctx, sampleTransaction())
			if err != nil {
				t.Fatalf("Insert transaction: %v", err)
			}

			state := ledger.ReconciledState{
				TenantID:          txn.TenantID,
				Instrument:        txn.Instrument,
				Position:          decimal.RequireFromString(tt.position),
				AverageCost:       decimal.RequireFromString(tt.averageCost),
				RealizedPnL:       decimal.RequireFromString(tt.realizedPnL),
				LastTransactionID: &txn.ID,
			}
			if err := store.Upsert(ctx, state); err != nil {
				t.Fatalf("Upsert: %v", err)
			}

			got, err := store.Get(ctx, state.TenantID, state.Instrument)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !got.Position.Equal(state.Position) {
				t.Fatalf("Get: position = %s, want %s", got.Position, state.Position)
			}
			if !got.AverageCost.Equal(state.AverageCost) {
				t.Fatalf("Get: average cost = %s, want %s", got.AverageCost, state.AverageCost)
			}
			if !got.RealizedPnL.Equal(state.RealizedPnL) {
				t.Fatalf("Get: realized P&L = %s, want %s", got.RealizedPnL, state.RealizedPnL)
			}
		})
	}
}
