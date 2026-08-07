package storage

import (
	"context"
	"errors"
	"testing"

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
		Position:          10 * ledger.AmountScale,
		AverageCost:       18950000000, // 189.50
		RealizedPnL:       0,
		LastTransactionID: &txn.ID,
	}
	if err := store.Upsert(ctx, initial); err != nil {
		t.Fatalf("Upsert (insert): %v", err)
	}

	got, err := store.Get(ctx, initial.TenantID, initial.Instrument)
	if err != nil {
		t.Fatalf("Get after insert: %v", err)
	}
	if got.Position != initial.Position {
		t.Fatalf("Get after insert: position = %d, want %d", got.Position, initial.Position)
	}

	secondTxn, err := txnStore.Insert(ctx, sampleTransaction())
	if err != nil {
		t.Fatalf("Insert second transaction: %v", err)
	}

	updated := initial
	updated.Position = 20 * ledger.AmountScale
	updated.RealizedPnL = 1525000000 // 15.25
	updated.LastTransactionID = &secondTxn.ID
	if err := store.Upsert(ctx, updated); err != nil {
		t.Fatalf("Upsert (update): %v", err)
	}

	got, err = store.Get(ctx, initial.TenantID, initial.Instrument)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if got.Position != updated.Position {
		t.Fatalf("Get after update: position = %d, want %d", got.Position, updated.Position)
	}
	if got.RealizedPnL != updated.RealizedPnL {
		t.Fatalf("Get after update: realized P&L = %d, want %d", got.RealizedPnL, updated.RealizedPnL)
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
			name:        "realized pnl at full precision",
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

			position, err := ledger.ParseAmount(tt.position)
			if err != nil {
				t.Fatalf("ParseAmount(position): %v", err)
			}
			averageCost, err := ledger.ParseAmount(tt.averageCost)
			if err != nil {
				t.Fatalf("ParseAmount(averageCost): %v", err)
			}
			realizedPnL, err := ledger.ParseAmount(tt.realizedPnL)
			if err != nil {
				t.Fatalf("ParseAmount(realizedPnL): %v", err)
			}

			state := ledger.ReconciledState{
				TenantID:          txn.TenantID,
				Instrument:        txn.Instrument,
				Position:          position,
				AverageCost:       averageCost,
				RealizedPnL:       realizedPnL,
				LastTransactionID: &txn.ID,
			}
			if err := store.Upsert(ctx, state); err != nil {
				t.Fatalf("Upsert: %v", err)
			}

			got, err := store.Get(ctx, state.TenantID, state.Instrument)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Position != state.Position {
				t.Fatalf("Get: position = %d, want %d", got.Position, state.Position)
			}
			if got.AverageCost != state.AverageCost {
				t.Fatalf("Get: average cost = %d, want %d", got.AverageCost, state.AverageCost)
			}
			if got.RealizedPnL != state.RealizedPnL {
				t.Fatalf("Get: realized P&L = %d, want %d", got.RealizedPnL, state.RealizedPnL)
			}
		})
	}
}
