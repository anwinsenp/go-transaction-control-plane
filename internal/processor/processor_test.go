package processor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// fakeTransactionRepository is an in-memory ledger.TransactionRepository
// double keyed by EventID, so repeated Insert calls with the same EventID
// naturally reproduce Kafka's at-least-once redelivery behavior.
type fakeTransactionRepository struct {
	transactions map[uuid.UUID]ledger.Transaction
	nextID       int64
	insertErr    error
}

func newFakeTransactionRepository() *fakeTransactionRepository {
	return &fakeTransactionRepository{transactions: make(map[uuid.UUID]ledger.Transaction)}
}

func (fake *fakeTransactionRepository) Insert(ctx context.Context, txn ledger.Transaction) (ledger.Transaction, error) {
	if _, exists := fake.transactions[txn.EventID]; exists {
		return ledger.Transaction{}, ledger.ErrDuplicateEvent
	}
	if fake.insertErr != nil {
		return ledger.Transaction{}, fake.insertErr
	}
	fake.nextID++
	txn.ID = fake.nextID
	fake.transactions[txn.EventID] = txn
	return txn, nil
}

func (fake *fakeTransactionRepository) GetByEventID(ctx context.Context, eventID uuid.UUID) (ledger.Transaction, error) {
	txn, ok := fake.transactions[eventID]
	if !ok {
		return ledger.Transaction{}, ledger.ErrNotFound
	}
	return txn, nil
}

func (fake *fakeTransactionRepository) ListByTenant(ctx context.Context, tenantID string, since time.Time) ([]ledger.Transaction, error) {
	return nil, nil
}

// stateKey identifies a ReconciledState by tenant and instrument.
type stateKey struct {
	tenantID   string
	instrument string
}

// fakeReconciledStateRepository is an in-memory ledger.ReconciledStateRepository
// double that also records every Upsert call, so tests can assert on how many
// times (and with what values) state was written.
type fakeReconciledStateRepository struct {
	states      map[stateKey]ledger.ReconciledState
	getErr      error
	upsertErr   error
	upsertCalls []ledger.ReconciledState
}

func newFakeReconciledStateRepository() *fakeReconciledStateRepository {
	return &fakeReconciledStateRepository{states: make(map[stateKey]ledger.ReconciledState)}
}

func (fake *fakeReconciledStateRepository) Get(ctx context.Context, tenantID, instrument string) (ledger.ReconciledState, error) {
	if fake.getErr != nil {
		return ledger.ReconciledState{}, fake.getErr
	}
	state, ok := fake.states[stateKey{tenantID: tenantID, instrument: instrument}]
	if !ok {
		return ledger.ReconciledState{}, ledger.ErrNotFound
	}
	return state, nil
}

func (fake *fakeReconciledStateRepository) Upsert(ctx context.Context, state ledger.ReconciledState) error {
	fake.upsertCalls = append(fake.upsertCalls, state)
	if fake.upsertErr != nil {
		return fake.upsertErr
	}
	fake.states[stateKey{tenantID: state.TenantID, instrument: state.Instrument}] = state
	return nil
}

// mustAmount parses a decimal literal known to be valid at compile time,
// panicking on failure so test tables can stay terse.
func mustAmount(value string) int64 {
	amount, err := ledger.ParseAmount(value)
	if err != nil {
		panic(err)
	}
	return amount
}

func newTestTransaction(eventID uuid.UUID, side ledger.Side, quantity, price string) ledger.Transaction {
	return ledger.Transaction{
		EventID:    eventID,
		TenantID:   "tenant-1",
		Instrument: "AAPL",
		Side:       side,
		Quantity:   mustAmount(quantity),
		Price:      mustAmount(price),
		Currency:   "USD",
		OccurredAt: time.Now(),
	}
}

func TestReconcileWeightedAverageCostAccounting(t *testing.T) {
	tests := []struct {
		name            string
		transactions    []ledger.Transaction
		wantPosition    string
		wantAverageCost string
		wantRealizedPnL string
	}{
		{
			name: "fresh buy establishes position and average cost",
			transactions: []ledger.Transaction{
				newTestTransaction(uuid.New(), ledger.SideBuy, "10", "100"),
			},
			wantPosition:    "10",
			wantAverageCost: "100",
			wantRealizedPnL: "0",
		},
		{
			name: "fresh sell against zero position opens a short at the trade price, no pnl realized yet",
			transactions: []ledger.Transaction{
				newTestTransaction(uuid.New(), ledger.SideSell, "5", "50"),
			},
			wantPosition:    "-5",
			wantAverageCost: "50",
			wantRealizedPnL: "0",
		},
		{
			name: "buy that covers a short and flips long realizes pnl on the covered quantity and opens the long at trade price",
			transactions: []ledger.Transaction{
				newTestTransaction(uuid.New(), ledger.SideSell, "5", "100"),
				newTestTransaction(uuid.New(), ledger.SideBuy, "10", "120"),
			},
			wantPosition:    "5",
			wantAverageCost: "120",
			wantRealizedPnL: "-100",
		},
		{
			name: "sell that closes a long and flips short realizes pnl on the closed quantity and opens the short at trade price",
			transactions: []ledger.Transaction{
				newTestTransaction(uuid.New(), ledger.SideBuy, "5", "100"),
				newTestTransaction(uuid.New(), ledger.SideSell, "10", "120"),
			},
			wantPosition:    "-5",
			wantAverageCost: "120",
			wantRealizedPnL: "100",
		},
		{
			name: "multiple buys compute weighted average cost",
			transactions: []ledger.Transaction{
				newTestTransaction(uuid.New(), ledger.SideBuy, "10", "100"),
				newTestTransaction(uuid.New(), ledger.SideBuy, "10", "200"),
			},
			wantPosition:    "20",
			wantAverageCost: "150",
			wantRealizedPnL: "0",
		},
		{
			name: "sell after buys realizes correct pnl and leaves average cost unchanged",
			transactions: []ledger.Transaction{
				newTestTransaction(uuid.New(), ledger.SideBuy, "10", "100"),
				newTestTransaction(uuid.New(), ledger.SideBuy, "10", "200"),
				newTestTransaction(uuid.New(), ledger.SideSell, "5", "180"),
			},
			wantPosition:    "15",
			wantAverageCost: "150",
			wantRealizedPnL: "150",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			transactionRepo := newFakeTransactionRepository()
			stateRepo := newFakeReconciledStateRepository()
			reconciler := NewReconciler(transactionRepo, stateRepo)

			for _, txn := range testCase.transactions {
				if err := reconciler.Reconcile(context.Background(), txn); err != nil {
					t.Fatalf("Reconcile() error = %v, want nil", err)
				}
			}

			got, err := stateRepo.Get(context.Background(), "tenant-1", "AAPL")
			if err != nil {
				t.Fatalf("Get() error = %v, want nil", err)
			}
			if want := mustAmount(testCase.wantPosition); got.Position != want {
				t.Errorf("Position = %s, want %s", ledger.FormatAmount(got.Position), testCase.wantPosition)
			}
			if want := mustAmount(testCase.wantAverageCost); got.AverageCost != want {
				t.Errorf("AverageCost = %s, want %s", ledger.FormatAmount(got.AverageCost), testCase.wantAverageCost)
			}
			if want := mustAmount(testCase.wantRealizedPnL); got.RealizedPnL != want {
				t.Errorf("RealizedPnL = %s, want %s", ledger.FormatAmount(got.RealizedPnL), testCase.wantRealizedPnL)
			}
		})
	}
}

func TestReconcileRedeliveryOfCompletedTransactionIsNoOp(t *testing.T) {
	transactionRepo := newFakeTransactionRepository()
	stateRepo := newFakeReconciledStateRepository()
	reconciler := NewReconciler(transactionRepo, stateRepo)

	txn := newTestTransaction(uuid.New(), ledger.SideBuy, "10", "100")

	if err := reconciler.Reconcile(context.Background(), txn); err != nil {
		t.Fatalf("first Reconcile() error = %v, want nil", err)
	}
	firstState, err := stateRepo.Get(context.Background(), "tenant-1", "AAPL")
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}
	if len(stateRepo.upsertCalls) != 1 {
		t.Fatalf("Upsert called %d times after first delivery, want 1", len(stateRepo.upsertCalls))
	}

	if err := reconciler.Reconcile(context.Background(), txn); err != nil {
		t.Fatalf("redelivered Reconcile() error = %v, want nil", err)
	}
	secondState, err := stateRepo.Get(context.Background(), "tenant-1", "AAPL")
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}

	if secondState != firstState {
		t.Errorf("state after redelivery = %+v, want unchanged %+v", secondState, firstState)
	}
	if len(stateRepo.upsertCalls) != 1 {
		t.Errorf("Upsert called %d times after redelivery, want 1 (redelivery must be a true no-op)", len(stateRepo.upsertCalls))
	}
}

// TestReconcileCrashRecoveryCompletesDeferredReconciliation simulates a
// processor crash between Transactions.Insert succeeding and
// States.Upsert running: the transaction is already durably persisted (so a
// redelivered Insert returns ErrDuplicateEvent), but the reconciled state's
// watermark was never advanced past it. Reconcile must complete the deferred
// state update rather than treating the duplicate-insert error as a signal
// to skip.
func TestReconcileCrashRecoveryCompletesDeferredReconciliation(t *testing.T) {
	transactionRepo := newFakeTransactionRepository()
	stateRepo := newFakeReconciledStateRepository()
	reconciler := NewReconciler(transactionRepo, stateRepo)

	eventID := uuid.New()
	txn := newTestTransaction(eventID, ledger.SideBuy, "10", "100")
	persisted := txn
	persisted.ID = 1
	transactionRepo.transactions[eventID] = persisted
	transactionRepo.nextID = 1

	if err := reconciler.Reconcile(context.Background(), txn); err != nil {
		t.Fatalf("Reconcile() error = %v, want nil", err)
	}

	if len(stateRepo.upsertCalls) != 1 {
		t.Fatalf("Upsert called %d times, want 1 (deferred reconciliation must complete)", len(stateRepo.upsertCalls))
	}
	got, err := stateRepo.Get(context.Background(), "tenant-1", "AAPL")
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}
	if got.Position != mustAmount("10") {
		t.Errorf("Position = %s, want 10", ledger.FormatAmount(got.Position))
	}
	if got.LastTransactionID == nil || *got.LastTransactionID != 1 {
		t.Errorf("LastTransactionID = %v, want pointer to 1", got.LastTransactionID)
	}
}

func TestReconcileErrorPropagation(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*fakeTransactionRepository, *fakeReconciledStateRepository)
		txn         ledger.Transaction
		wantErrText string
	}{
		{
			name: "non-duplicate insert error is wrapped",
			setup: func() (*fakeTransactionRepository, *fakeReconciledStateRepository) {
				transactionRepo := newFakeTransactionRepository()
				transactionRepo.insertErr = errors.New("connection reset")
				return transactionRepo, newFakeReconciledStateRepository()
			},
			txn:         newTestTransaction(uuid.New(), ledger.SideBuy, "10", "100"),
			wantErrText: "reconcile: insert transaction",
		},
		{
			name: "unexpected state get error is wrapped",
			setup: func() (*fakeTransactionRepository, *fakeReconciledStateRepository) {
				stateRepo := newFakeReconciledStateRepository()
				stateRepo.getErr = errors.New("connection reset")
				return newFakeTransactionRepository(), stateRepo
			},
			txn:         newTestTransaction(uuid.New(), ledger.SideBuy, "10", "100"),
			wantErrText: "reconcile: get reconciled state",
		},
		{
			name: "upsert error is wrapped",
			setup: func() (*fakeTransactionRepository, *fakeReconciledStateRepository) {
				stateRepo := newFakeReconciledStateRepository()
				stateRepo.upsertErr = errors.New("connection reset")
				return newFakeTransactionRepository(), stateRepo
			},
			txn:         newTestTransaction(uuid.New(), ledger.SideBuy, "10", "100"),
			wantErrText: "reconcile: upsert reconciled state",
		},
		{
			name: "unknown side errors instead of panicking or silently no-oping",
			setup: func() (*fakeTransactionRepository, *fakeReconciledStateRepository) {
				return newFakeTransactionRepository(), newFakeReconciledStateRepository()
			},
			txn:         newTestTransaction(uuid.New(), ledger.Side("HOLD"), "10", "100"),
			wantErrText: "unknown transaction side",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			transactionRepo, stateRepo := testCase.setup()
			reconciler := NewReconciler(transactionRepo, stateRepo)

			err := reconciler.Reconcile(context.Background(), testCase.txn)
			if err == nil {
				t.Fatal("Reconcile() error = nil, want non-nil")
			}
			if !strings.Contains(err.Error(), testCase.wantErrText) {
				t.Errorf("Reconcile() error = %q, want it to contain %q", err.Error(), testCase.wantErrText)
			}
		})
	}
}
