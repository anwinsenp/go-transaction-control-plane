package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

func sampleTransaction() ledger.Transaction {
	return ledger.Transaction{
		EventID:       uuid.New(),
		TenantID:      "tenant-a",
		SchemaVersion: 1,
		Instrument:    "AAPL",
		Side:          ledger.SideBuy,
		Quantity:      decimal.NewFromInt(10),
		Price:         decimal.NewFromFloat(189.50),
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
	if !got.Quantity.Equal(want.Quantity) {
		t.Fatalf("GetByEventID: quantity = %s, want %s", got.Quantity, want.Quantity)
	}
	if !got.Price.Equal(want.Price) {
		t.Fatalf("GetByEventID: price = %s, want %s", got.Price, want.Price)
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
				txn.Quantity = decimal.Zero
			},
		},
		{
			name: "negative quantity",
			mutate: func(txn *ledger.Transaction) {
				txn.Quantity = decimal.NewFromInt(-5)
			},
		},
		{
			name: "zero price",
			mutate: func(txn *ledger.Transaction) {
				txn.Price = decimal.Zero
			},
		},
		{
			name: "negative price",
			mutate: func(txn *ledger.Transaction) {
				txn.Price = decimal.NewFromFloat(-1.5)
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
			quantity: "123456789012.12345678",
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

			txn := sampleTransaction()
			txn.Quantity = decimal.RequireFromString(tt.quantity)
			txn.Price = decimal.RequireFromString(tt.price)

			inserted, err := store.Insert(ctx, txn)
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if !inserted.Quantity.Equal(txn.Quantity) {
				t.Fatalf("Insert: quantity = %s, want %s", inserted.Quantity, txn.Quantity)
			}
			if !inserted.Price.Equal(txn.Price) {
				t.Fatalf("Insert: price = %s, want %s", inserted.Price, txn.Price)
			}

			got, err := store.GetByEventID(ctx, txn.EventID)
			if err != nil {
				t.Fatalf("GetByEventID: %v", err)
			}
			if !got.Quantity.Equal(txn.Quantity) {
				t.Fatalf("GetByEventID: quantity = %s, want %s", got.Quantity, txn.Quantity)
			}
			if !got.Price.Equal(txn.Price) {
				t.Fatalf("GetByEventID: price = %s, want %s", got.Price, txn.Price)
			}
		})
	}
}
