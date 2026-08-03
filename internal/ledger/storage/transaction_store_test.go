package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

func sampleTransaction() ledger.Transaction {
	return ledger.Transaction{
		EventID:       uuid.New(),
		TenantID:      "tenant-a",
		SchemaVersion: 1,
		Instrument:    "AAPL",
		Side:          ledger.SideBuy,
		Quantity:      10 * ledger.AmountScale,
		Price:         18950000000, // 189.50
		Currency:      "USD",
		OccurredAt:    time.Now().UTC().Truncate(time.Microsecond),
	}
}

func TestTransactionStore_InsertAndGetByEventID(t *testing.T) {
	pool := testPool(t)
	store := NewTransactionStore(pool)
	ctx := context.Background()

	want := sampleTransaction()
	inserted, err := store.Insert(ctx, want)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if inserted.ID == 0 {
		t.Fatal("Insert: expected a non-zero generated ID")
	}

	got, err := store.GetByEventID(ctx, want.EventID)
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if got.EventID != want.EventID || got.TenantID != want.TenantID || got.Instrument != want.Instrument {
		t.Fatalf("GetByEventID: got %+v, want fields matching %+v", got, want)
	}
	if got.Quantity != want.Quantity {
		t.Fatalf("GetByEventID: quantity = %d, want %d", got.Quantity, want.Quantity)
	}
	if got.Price != want.Price {
		t.Fatalf("GetByEventID: price = %d, want %d", got.Price, want.Price)
	}
}

func TestTransactionStore_InsertDuplicateEventID(t *testing.T) {
	pool := testPool(t)
	store := NewTransactionStore(pool)
	ctx := context.Background()

	txn := sampleTransaction()
	if _, err := store.Insert(ctx, txn); err != nil {
		t.Fatalf("first Insert: %v", err)
	}

	_, err := store.Insert(ctx, txn)
	if !errors.Is(err, ledger.ErrDuplicateEvent) {
		t.Fatalf("second Insert: got err %v, want ledger.ErrDuplicateEvent", err)
	}
}

func TestTransactionStore_GetByEventIDNotFound(t *testing.T) {
	pool := testPool(t)
	store := NewTransactionStore(pool)

	_, err := store.GetByEventID(context.Background(), uuid.New())
	if !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("got err %v, want ledger.ErrNotFound", err)
	}
}

func TestTransactionStore_ListByTenant(t *testing.T) {
	pool := testPool(t)
	store := NewTransactionStore(pool)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Microsecond)

	older := sampleTransaction()
	older.TenantID = "tenant-b"
	older.OccurredAt = base.Add(-2 * time.Hour)
	if _, err := store.Insert(ctx, older); err != nil {
		t.Fatalf("Insert older: %v", err)
	}

	inRange := sampleTransaction()
	inRange.TenantID = "tenant-b"
	inRange.OccurredAt = base
	if _, err := store.Insert(ctx, inRange); err != nil {
		t.Fatalf("Insert inRange: %v", err)
	}

	otherTenant := sampleTransaction()
	otherTenant.TenantID = "tenant-c"
	otherTenant.OccurredAt = base
	if _, err := store.Insert(ctx, otherTenant); err != nil {
		t.Fatalf("Insert otherTenant: %v", err)
	}

	got, err := store.ListByTenant(ctx, "tenant-b", base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("ListByTenant: got %d transactions, want 1", len(got))
	}
	if got[0].EventID != inRange.EventID {
		t.Fatalf("ListByTenant: got event %s, want %s", got[0].EventID, inRange.EventID)
	}
}

func TestTransactionStore_ListByTenant_NoMatches(t *testing.T) {
	pool := testPool(t)
	store := NewTransactionStore(pool)
	ctx := context.Background()

	txn := sampleTransaction()
	txn.TenantID = "tenant-with-data"
	if _, err := store.Insert(ctx, txn); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := store.ListByTenant(ctx, "tenant-with-no-data", time.Time{})
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if got == nil {
		t.Fatal("ListByTenant: got nil slice, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("ListByTenant: got %d transactions, want 0", len(got))
	}
}

func TestTransactionStore_InsertConstraintViolations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(txn *ledger.Transaction)
	}{
		{
			name: "side not BUY or SELL",
			mutate: func(txn *ledger.Transaction) {
				txn.Side = "HOLD"
			},
		},
		{
			name: "zero quantity",
			mutate: func(txn *ledger.Transaction) {
				txn.Quantity = 0
			},
		},
		{
			name: "negative quantity",
			mutate: func(txn *ledger.Transaction) {
				txn.Quantity = -5 * ledger.AmountScale
			},
		},
		{
			name: "zero price",
			mutate: func(txn *ledger.Transaction) {
				txn.Price = 0
			},
		},
		{
			name: "negative price",
			mutate: func(txn *ledger.Transaction) {
				txn.Price = -150000000 // -1.5
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := testPool(t)
			store := NewTransactionStore(pool)
			ctx := context.Background()

			txn := sampleTransaction()
			tt.mutate(&txn)

			_, err := store.Insert(ctx, txn)
			if err == nil {
				t.Fatal("Insert: got nil error, want a constraint violation error")
			}
			if errors.Is(err, ledger.ErrDuplicateEvent) {
				t.Fatal("Insert: got ErrDuplicateEvent, want a constraint violation error")
			}
			if errors.Is(err, ledger.ErrNotFound) {
				t.Fatal("Insert: got ErrNotFound, want a constraint violation error")
			}
		})
	}
}

func TestTransactionStore_QuantityPricePrecision(t *testing.T) {
	tests := []struct {
		name     string
		quantity string
		price    string
	}{
		{
			name:     "smallest representable fraction",
			quantity: "0.00000001",
			price:    "0.00000001",
		},
		{
			name:     "large value with full 8 decimal places",
			quantity: "50000000000.12345678",
			price:    "98765.87654321",
		},
		{
			name:     "whole numbers",
			quantity: "100",
			price:    "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := testPool(t)
			store := NewTransactionStore(pool)
			ctx := context.Background()

			quantity, err := ledger.ParseAmount(tt.quantity)
			if err != nil {
				t.Fatalf("ParseAmount(quantity): %v", err)
			}
			price, err := ledger.ParseAmount(tt.price)
			if err != nil {
				t.Fatalf("ParseAmount(price): %v", err)
			}

			txn := sampleTransaction()
			txn.Quantity = quantity
			txn.Price = price

			inserted, err := store.Insert(ctx, txn)
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if inserted.Quantity != txn.Quantity {
				t.Fatalf("Insert: quantity = %d, want %d", inserted.Quantity, txn.Quantity)
			}
			if inserted.Price != txn.Price {
				t.Fatalf("Insert: price = %d, want %d", inserted.Price, txn.Price)
			}

			got, err := store.GetByEventID(ctx, txn.EventID)
			if err != nil {
				t.Fatalf("GetByEventID: %v", err)
			}
			if got.Quantity != txn.Quantity {
				t.Fatalf("GetByEventID: quantity = %d, want %d", got.Quantity, txn.Quantity)
			}
			if got.Price != txn.Price {
				t.Fatalf("GetByEventID: price = %d, want %d", got.Price, txn.Price)
			}
		})
	}
}
