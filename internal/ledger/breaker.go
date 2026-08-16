package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/anwinsenp/go-transaction-control-plane/internal/corebreaker"
)

// ErrCircuitOpen is returned by a repository breaker when the breaker is
// open (or a half-open probe is already in flight) and the call is failed
// fast without reaching the wrapped repository.
var ErrCircuitOpen = corebreaker.ErrOpen

// ErrInvalidBreakerConfig indicates a BreakerConfig failed validation.
var ErrInvalidBreakerConfig = corebreaker.ErrInvalidConfig

// BreakerState is one of the three states a repository breaker can be in.
type BreakerState = corebreaker.State

// The three states a repository breaker can be in.
const (
	BreakerClosed   = corebreaker.Closed
	BreakerOpen     = corebreaker.Open
	BreakerHalfOpen = corebreaker.HalfOpen
)

// BreakerConfig controls when a repository breaker trips and how long it
// stays open before probing the wrapped repository again.
type BreakerConfig = corebreaker.Config

// ignoredForBreaker reports whether err matches one of sentinels: ledger's
// own errors representing an expected business outcome rather than Postgres
// being unhealthy, so it shouldn't count toward tripping the breaker even
// though it's still returned to the caller unchanged.
func ignoredForBreaker(err error, sentinels ...error) bool {
	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// TransactionRepositoryBreaker wraps a TransactionRepository with a circuit
// breaker that fails fast while Postgres appears unhealthy, rather than
// letting every reconcile call block on a degraded write path. It
// implements TransactionRepository itself, so it drops in wherever one is
// expected.
type TransactionRepositoryBreaker struct {
	next    TransactionRepository
	machine *corebreaker.Machine
}

var _ TransactionRepository = (*TransactionRepositoryBreaker)(nil)

// NewTransactionRepositoryBreaker builds a TransactionRepositoryBreaker
// that wraps next according to config. It starts closed.
func NewTransactionRepositoryBreaker(next TransactionRepository, config BreakerConfig) (*TransactionRepositoryBreaker, error) {
	if next == nil {
		return nil, fmt.Errorf("new transaction repository breaker: next TransactionRepository must not be nil")
	}
	machine, err := corebreaker.New(config)
	if err != nil {
		return nil, fmt.Errorf("new transaction repository breaker: %w", err)
	}
	return &TransactionRepositoryBreaker{next: next, machine: machine}, nil
}

// State reports the breaker's current state.
func (breaker *TransactionRepositoryBreaker) State() BreakerState {
	return breaker.machine.State()
}

// Insert forwards to the wrapped TransactionRepository unless the breaker
// is open, in which case it fails fast with ErrCircuitOpen. ErrDuplicateEvent
// is treated as an expected outcome, not a breaker failure.
func (breaker *TransactionRepositoryBreaker) Insert(ctx context.Context, txn Transaction) (Transaction, error) {
	allowed, isProbe := breaker.machine.Allow()
	if !allowed {
		return Transaction{}, ErrCircuitOpen
	}
	probeCtx, cancel := breaker.machine.ProbeContext(ctx, isProbe)
	defer cancel()

	inserted, err := breaker.next.Insert(probeCtx, txn)
	breaker.recordResult(err, ErrDuplicateEvent)
	return inserted, err
}

// GetByEventID forwards to the wrapped TransactionRepository unless the
// breaker is open, in which case it fails fast with ErrCircuitOpen.
// ErrNotFound is treated as an expected outcome, not a breaker failure.
func (breaker *TransactionRepositoryBreaker) GetByEventID(ctx context.Context, eventID uuid.UUID) (Transaction, error) {
	allowed, isProbe := breaker.machine.Allow()
	if !allowed {
		return Transaction{}, ErrCircuitOpen
	}
	probeCtx, cancel := breaker.machine.ProbeContext(ctx, isProbe)
	defer cancel()

	txn, err := breaker.next.GetByEventID(probeCtx, eventID)
	breaker.recordResult(err, ErrNotFound)
	return txn, err
}

// ListByTenant forwards to the wrapped TransactionRepository unless the
// breaker is open, in which case it fails fast with ErrCircuitOpen.
func (breaker *TransactionRepositoryBreaker) ListByTenant(ctx context.Context, tenantID string, since time.Time) ([]Transaction, error) {
	allowed, isProbe := breaker.machine.Allow()
	if !allowed {
		return nil, ErrCircuitOpen
	}
	probeCtx, cancel := breaker.machine.ProbeContext(ctx, isProbe)
	defer cancel()

	transactions, err := breaker.next.ListByTenant(probeCtx, tenantID, since)
	breaker.recordResult(err)
	return transactions, err
}

// recordResult reports err to the breaker's state machine, unless it
// matches one of ignored, in which case it's reported as nil.
func (breaker *TransactionRepositoryBreaker) recordResult(err error, ignored ...error) {
	if ignoredForBreaker(err, ignored...) {
		breaker.machine.Record(nil)
		return
	}
	breaker.machine.Record(err)
}

// ReconciledStateRepositoryBreaker wraps a ReconciledStateRepository with a
// circuit breaker that fails fast while Postgres appears unhealthy, rather
// than letting every reconcile call block on a degraded write path. It
// implements ReconciledStateRepository itself, so it drops in wherever one
// is expected.
type ReconciledStateRepositoryBreaker struct {
	next    ReconciledStateRepository
	machine *corebreaker.Machine
}

var _ ReconciledStateRepository = (*ReconciledStateRepositoryBreaker)(nil)

// NewReconciledStateRepositoryBreaker builds a ReconciledStateRepositoryBreaker
// that wraps next according to config. It starts closed.
func NewReconciledStateRepositoryBreaker(next ReconciledStateRepository, config BreakerConfig) (*ReconciledStateRepositoryBreaker, error) {
	if next == nil {
		return nil, fmt.Errorf("new reconciled state repository breaker: next ReconciledStateRepository must not be nil")
	}
	machine, err := corebreaker.New(config)
	if err != nil {
		return nil, fmt.Errorf("new reconciled state repository breaker: %w", err)
	}
	return &ReconciledStateRepositoryBreaker{next: next, machine: machine}, nil
}

// State reports the breaker's current state.
func (breaker *ReconciledStateRepositoryBreaker) State() BreakerState {
	return breaker.machine.State()
}

// Upsert forwards to the wrapped ReconciledStateRepository unless the
// breaker is open, in which case it fails fast with ErrCircuitOpen.
// ErrStaleReconciledState is treated as an expected outcome (a lost
// optimistic-concurrency race), not a breaker failure.
func (breaker *ReconciledStateRepositoryBreaker) Upsert(ctx context.Context, state ReconciledState) error {
	allowed, isProbe := breaker.machine.Allow()
	if !allowed {
		return ErrCircuitOpen
	}
	probeCtx, cancel := breaker.machine.ProbeContext(ctx, isProbe)
	defer cancel()

	err := breaker.next.Upsert(probeCtx, state)
	breaker.recordResult(err, ErrStaleReconciledState)
	return err
}

// Get forwards to the wrapped ReconciledStateRepository unless the breaker
// is open, in which case it fails fast with ErrCircuitOpen. ErrNotFound is
// treated as an expected outcome, not a breaker failure.
func (breaker *ReconciledStateRepositoryBreaker) Get(ctx context.Context, tenantID, instrument string) (ReconciledState, error) {
	allowed, isProbe := breaker.machine.Allow()
	if !allowed {
		return ReconciledState{}, ErrCircuitOpen
	}
	probeCtx, cancel := breaker.machine.ProbeContext(ctx, isProbe)
	defer cancel()

	state, err := breaker.next.Get(probeCtx, tenantID, instrument)
	breaker.recordResult(err, ErrNotFound)
	return state, err
}

// recordResult reports err to the breaker's state machine, unless it
// matches one of ignored, in which case it's reported as nil.
func (breaker *ReconciledStateRepositoryBreaker) recordResult(err error, ignored ...error) {
	if ignoredForBreaker(err, ignored...) {
		breaker.machine.Record(nil)
		return
	}
	breaker.machine.Record(err)
}
