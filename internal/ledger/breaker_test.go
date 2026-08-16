package ledger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeTransactionRepository is a TransactionRepository test double that
// records how many times each method was called and returns
// caller-configured errors, so breaker tests can drive success/failure
// without a live Postgres instance.
type fakeTransactionRepository struct {
	mutex sync.Mutex

	insertErr         error
	insertCalls       int
	getByEventIDErr   error
	getByEventIDCalls int
	listByTenantErr   error
	listByTenantCalls int

	// started, if non-nil, receives a value at the start of every Insert
	// call, letting a test synchronize with an in-flight probe.
	started chan struct{}
	// block, if non-nil, is read from before Insert returns, letting a test
	// hold a call open to deterministically order concurrent callers.
	block chan struct{}
}

func (fake *fakeTransactionRepository) Insert(_ context.Context, _ Transaction) (Transaction, error) {
	fake.mutex.Lock()
	fake.insertCalls++
	callErr := fake.insertErr
	startedChan := fake.started
	blockChan := fake.block
	fake.mutex.Unlock()

	if startedChan != nil {
		startedChan <- struct{}{}
	}
	if blockChan != nil {
		<-blockChan
	}
	return Transaction{}, callErr
}

func (fake *fakeTransactionRepository) GetByEventID(_ context.Context, _ uuid.UUID) (Transaction, error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.getByEventIDCalls++
	return Transaction{}, fake.getByEventIDErr
}

func (fake *fakeTransactionRepository) ListByTenant(_ context.Context, _ string, _ time.Time) ([]Transaction, error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.listByTenantCalls++
	return nil, fake.listByTenantErr
}

func (fake *fakeTransactionRepository) setInsertErr(err error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.insertErr = err
}

func (fake *fakeTransactionRepository) setGetByEventIDErr(err error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.getByEventIDErr = err
}

func (fake *fakeTransactionRepository) setListByTenantErr(err error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.listByTenantErr = err
}

func (fake *fakeTransactionRepository) setInsertSync(started, block chan struct{}) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.started = started
	fake.block = block
}

func (fake *fakeTransactionRepository) totalCalls() int {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	return fake.insertCalls + fake.getByEventIDCalls + fake.listByTenantCalls
}

// hangingTransactionRepository is a TransactionRepository test double whose
// Insert blocks until the caller's ctx is canceled, letting a test observe
// whether a bound (such as BreakerConfig.ProbeTimeout) was actually applied
// to a hung call.
type hangingTransactionRepository struct{}

func (hangingTransactionRepository) Insert(ctx context.Context, _ Transaction) (Transaction, error) {
	<-ctx.Done()
	return Transaction{}, ctx.Err()
}

func (hangingTransactionRepository) GetByEventID(ctx context.Context, _ uuid.UUID) (Transaction, error) {
	<-ctx.Done()
	return Transaction{}, ctx.Err()
}

func (hangingTransactionRepository) ListByTenant(ctx context.Context, _ string, _ time.Time) ([]Transaction, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// fakeReconciledStateRepository is a ReconciledStateRepository test double
// analogous to fakeTransactionRepository.
type fakeReconciledStateRepository struct {
	mutex sync.Mutex

	upsertErr   error
	upsertCalls int
	getErr      error
	getCalls    int
}

func (fake *fakeReconciledStateRepository) Upsert(_ context.Context, _ ReconciledState) error {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.upsertCalls++
	return fake.upsertErr
}

func (fake *fakeReconciledStateRepository) Get(_ context.Context, _, _ string) (ReconciledState, error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.getCalls++
	return ReconciledState{}, fake.getErr
}

func (fake *fakeReconciledStateRepository) setUpsertErr(err error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.upsertErr = err
}

func (fake *fakeReconciledStateRepository) setGetErr(err error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.getErr = err
}

func (fake *fakeReconciledStateRepository) totalCalls() int {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	return fake.upsertCalls + fake.getCalls
}

var errBackendFailed = errors.New("backend call failed")

func errorContains(err error, substring string) bool {
	return err != nil && strings.Contains(err.Error(), substring)
}

func TestNewTransactionRepositoryBreaker(t *testing.T) {
	testCases := []struct {
		name       string
		next       TransactionRepository
		config     BreakerConfig
		wantErr    error
		wantErrMsg string
	}{
		{
			name:   "valid config succeeds",
			next:   &fakeTransactionRepository{},
			config: BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second},
		},
		{
			name:       "nil next repository is rejected",
			next:       nil,
			config:     BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second},
			wantErrMsg: "next TransactionRepository must not be nil",
		},
		{
			name:    "zero failure threshold is rejected",
			next:    &fakeTransactionRepository{},
			config:  BreakerConfig{FailureThreshold: 0, OpenTimeout: time.Second},
			wantErr: ErrInvalidBreakerConfig,
		},
		{
			name:    "zero open timeout is rejected",
			next:    &fakeTransactionRepository{},
			config:  BreakerConfig{FailureThreshold: 3, OpenTimeout: 0},
			wantErr: ErrInvalidBreakerConfig,
		},
		{
			name:    "negative open timeout is rejected",
			next:    &fakeTransactionRepository{},
			config:  BreakerConfig{FailureThreshold: 3, OpenTimeout: -time.Second},
			wantErr: ErrInvalidBreakerConfig,
		},
		{
			name:    "negative probe timeout is rejected",
			next:    &fakeTransactionRepository{},
			config:  BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second, ProbeTimeout: -time.Millisecond},
			wantErr: ErrInvalidBreakerConfig,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			txnBreaker, err := NewTransactionRepositoryBreaker(testCase.next, testCase.config)

			if testCase.wantErr == nil && testCase.wantErrMsg == "" {
				if err != nil {
					t.Fatalf("NewTransactionRepositoryBreaker() error = %v, want nil", err)
				}
				if txnBreaker == nil {
					t.Fatal("NewTransactionRepositoryBreaker() returned nil breaker with nil error")
				}
				if state := txnBreaker.State(); state != BreakerClosed {
					t.Errorf("initial state = %s, want %s", state, BreakerClosed)
				}
				return
			}

			if err == nil {
				t.Fatal("NewTransactionRepositoryBreaker() error = nil, want non-nil")
			}
			if txnBreaker != nil {
				t.Errorf("NewTransactionRepositoryBreaker() breaker = %v, want nil on error", txnBreaker)
			}
			if testCase.wantErr != nil && !errors.Is(err, testCase.wantErr) {
				t.Errorf("error = %v, want it to wrap %v", err, testCase.wantErr)
			}
			if testCase.wantErrMsg != "" && !errorContains(err, testCase.wantErrMsg) {
				t.Errorf("error = %v, want it to contain %q", err, testCase.wantErrMsg)
			}
		})
	}
}

func TestNewReconciledStateRepositoryBreaker(t *testing.T) {
	testCases := []struct {
		name       string
		next       ReconciledStateRepository
		config     BreakerConfig
		wantErr    error
		wantErrMsg string
	}{
		{
			name:   "valid config succeeds",
			next:   &fakeReconciledStateRepository{},
			config: BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second},
		},
		{
			name:       "nil next repository is rejected",
			next:       nil,
			config:     BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second},
			wantErrMsg: "next ReconciledStateRepository must not be nil",
		},
		{
			name:    "invalid config is rejected",
			next:    &fakeReconciledStateRepository{},
			config:  BreakerConfig{FailureThreshold: 0, OpenTimeout: time.Second},
			wantErr: ErrInvalidBreakerConfig,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stateBreaker, err := NewReconciledStateRepositoryBreaker(testCase.next, testCase.config)

			if testCase.wantErr == nil && testCase.wantErrMsg == "" {
				if err != nil {
					t.Fatalf("NewReconciledStateRepositoryBreaker() error = %v, want nil", err)
				}
				if stateBreaker == nil {
					t.Fatal("NewReconciledStateRepositoryBreaker() returned nil breaker with nil error")
				}
				if state := stateBreaker.State(); state != BreakerClosed {
					t.Errorf("initial state = %s, want %s", state, BreakerClosed)
				}
				return
			}

			if err == nil {
				t.Fatal("NewReconciledStateRepositoryBreaker() error = nil, want non-nil")
			}
			if stateBreaker != nil {
				t.Errorf("NewReconciledStateRepositoryBreaker() breaker = %v, want nil on error", stateBreaker)
			}
			if testCase.wantErr != nil && !errors.Is(err, testCase.wantErr) {
				t.Errorf("error = %v, want it to wrap %v", err, testCase.wantErr)
			}
			if testCase.wantErrMsg != "" && !errorContains(err, testCase.wantErrMsg) {
				t.Errorf("error = %v, want it to contain %q", err, testCase.wantErrMsg)
			}
		})
	}
}

func TestTransactionRepositoryBreaker_ClosedState(t *testing.T) {
	testCases := []struct {
		name           string
		results        []error
		wantEndState   BreakerState
		wantFinalCalls int
	}{
		{
			name:           "successful calls pass through and stay closed",
			results:        []error{nil, nil, nil},
			wantEndState:   BreakerClosed,
			wantFinalCalls: 3,
		},
		{
			name:           "failures below threshold stay closed",
			results:        []error{errBackendFailed, errBackendFailed},
			wantEndState:   BreakerClosed,
			wantFinalCalls: 2,
		},
		{
			name:           "an interspersed success resets the failure count",
			results:        []error{errBackendFailed, errBackendFailed, nil, errBackendFailed, errBackendFailed},
			wantEndState:   BreakerClosed,
			wantFinalCalls: 5,
		},
		{
			name:           "reaching the threshold trips the breaker open",
			results:        []error{errBackendFailed, errBackendFailed, errBackendFailed},
			wantEndState:   BreakerOpen,
			wantFinalCalls: 3,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakeTransactionRepository{}
			txnBreaker, err := NewTransactionRepositoryBreaker(fake, BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Minute})
			if err != nil {
				t.Fatalf("NewTransactionRepositoryBreaker() error = %v", err)
			}

			for _, result := range testCase.results {
				fake.setInsertErr(result)
				_, insertErr := txnBreaker.Insert(context.Background(), Transaction{})
				if !errors.Is(insertErr, result) {
					t.Fatalf("Insert() error = %v, want %v", insertErr, result)
				}
			}

			if state := txnBreaker.State(); state != testCase.wantEndState {
				t.Errorf("state = %s, want %s", state, testCase.wantEndState)
			}
			if calls := fake.totalCalls(); calls != testCase.wantFinalCalls {
				t.Errorf("wrapped repository call count = %d, want %d", calls, testCase.wantFinalCalls)
			}
		})
	}
}

func TestTransactionRepositoryBreaker_IgnoredErrorsDoNotTripBreaker(t *testing.T) {
	const threshold = uint32(3)

	testCases := []struct {
		name         string
		method       string
		err          error
		wantEndState BreakerState
	}{
		{
			name:         "Insert: repeated ErrDuplicateEvent does not trip the breaker",
			method:       "Insert",
			err:          fmt.Errorf("db insert: %w", ErrDuplicateEvent),
			wantEndState: BreakerClosed,
		},
		{
			name:         "Insert: repeated non-ignored error trips the breaker",
			method:       "Insert",
			err:          errBackendFailed,
			wantEndState: BreakerOpen,
		},
		{
			name:         "GetByEventID: repeated ErrNotFound does not trip the breaker",
			method:       "GetByEventID",
			err:          fmt.Errorf("db lookup: %w", ErrNotFound),
			wantEndState: BreakerClosed,
		},
		{
			name:         "GetByEventID: repeated non-ignored error trips the breaker",
			method:       "GetByEventID",
			err:          errBackendFailed,
			wantEndState: BreakerOpen,
		},
		{
			name:         "ListByTenant: repeated error trips the breaker (no ignored sentinel)",
			method:       "ListByTenant",
			err:          errBackendFailed,
			wantEndState: BreakerOpen,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakeTransactionRepository{}
			txnBreaker, err := NewTransactionRepositoryBreaker(fake, BreakerConfig{FailureThreshold: threshold, OpenTimeout: time.Minute})
			if err != nil {
				t.Fatalf("NewTransactionRepositoryBreaker() error = %v", err)
			}

			for i := uint32(0); i < threshold; i++ {
				var callErr error
				switch testCase.method {
				case "Insert":
					fake.setInsertErr(testCase.err)
					_, callErr = txnBreaker.Insert(context.Background(), Transaction{})
				case "GetByEventID":
					fake.setGetByEventIDErr(testCase.err)
					_, callErr = txnBreaker.GetByEventID(context.Background(), uuid.New())
				case "ListByTenant":
					fake.setListByTenantErr(testCase.err)
					_, callErr = txnBreaker.ListByTenant(context.Background(), "tenant-a", time.Time{})
				default:
					t.Fatalf("unhandled method %q", testCase.method)
				}
				if !errors.Is(callErr, testCase.err) {
					t.Fatalf("call error = %v, want it to wrap %v (sentinel must still reach the caller)", callErr, testCase.err)
				}
			}

			if state := txnBreaker.State(); state != testCase.wantEndState {
				t.Errorf("state after %d calls = %s, want %s", threshold, state, testCase.wantEndState)
			}
			if calls := fake.totalCalls(); calls != int(threshold) {
				t.Errorf("wrapped repository call count = %d, want %d (breaker must not have opened early)", calls, threshold)
			}
		})
	}
}

func TestTransactionRepositoryBreaker_OpenState_FailsFast(t *testing.T) {
	fake := &fakeTransactionRepository{insertErr: errBackendFailed}
	txnBreaker, err := NewTransactionRepositoryBreaker(fake, BreakerConfig{FailureThreshold: 2, OpenTimeout: time.Minute})
	if err != nil {
		t.Fatalf("NewTransactionRepositoryBreaker() error = %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); !errors.Is(insertErr, errBackendFailed) {
			t.Fatalf("Insert() error = %v, want %v", insertErr, errBackendFailed)
		}
	}
	if state := txnBreaker.State(); state != BreakerOpen {
		t.Fatalf("state after threshold failures = %s, want %s", state, BreakerOpen)
	}

	callsBeforeOpenAttempts := fake.totalCalls()

	for i := 0; i < 3; i++ {
		_, insertErr := txnBreaker.Insert(context.Background(), Transaction{})
		if !errors.Is(insertErr, ErrCircuitOpen) {
			t.Errorf("Insert() error = %v, want %v", insertErr, ErrCircuitOpen)
		}
	}

	if calls := fake.totalCalls(); calls != callsBeforeOpenAttempts {
		t.Errorf("wrapped repository call count = %d, want unchanged from %d (open breaker must not reach it)", calls, callsBeforeOpenAttempts)
	}
	if state := txnBreaker.State(); state != BreakerOpen {
		t.Errorf("state = %s, want %s", state, BreakerOpen)
	}
}

func TestTransactionRepositoryBreaker_HalfOpen_RecoverySuccess(t *testing.T) {
	fake := &fakeTransactionRepository{insertErr: errBackendFailed}
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}
	txnBreaker, err := NewTransactionRepositoryBreaker(fake, config)
	if err != nil {
		t.Fatalf("NewTransactionRepositoryBreaker() error = %v", err)
	}

	openedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	txnBreaker.machine.SetNow(func() time.Time { return openedAt })
	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); !errors.Is(insertErr, errBackendFailed) {
		t.Fatalf("Insert() error = %v, want %v", insertErr, errBackendFailed)
	}
	if state := txnBreaker.State(); state != BreakerOpen {
		t.Fatalf("state after tripping = %s, want %s", state, BreakerOpen)
	}

	txnBreaker.machine.SetNow(func() time.Time { return openedAt.Add(config.OpenTimeout) })
	fake.setInsertErr(nil)

	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); insertErr != nil {
		t.Fatalf("probe Insert() error = %v, want nil", insertErr)
	}
	if state := txnBreaker.State(); state != BreakerClosed {
		t.Fatalf("state after successful probe = %s, want %s", state, BreakerClosed)
	}

	// consecutiveFailures must have reset: a single subsequent failure
	// (with FailureThreshold == 1) should trip the breaker again rather
	// than requiring the pre-trip failure to still be counted.
	fake.setInsertErr(errBackendFailed)
	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); !errors.Is(insertErr, errBackendFailed) {
		t.Fatalf("Insert() error = %v, want %v", insertErr, errBackendFailed)
	}
	if state := txnBreaker.State(); state != BreakerOpen {
		t.Errorf("state after post-recovery failure = %s, want %s", state, BreakerOpen)
	}
}

func TestTransactionRepositoryBreaker_HalfOpen_RecoveryFailure(t *testing.T) {
	fake := &fakeTransactionRepository{insertErr: errBackendFailed}
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}
	txnBreaker, err := NewTransactionRepositoryBreaker(fake, config)
	if err != nil {
		t.Fatalf("NewTransactionRepositoryBreaker() error = %v", err)
	}

	firstOpenedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	txnBreaker.machine.SetNow(func() time.Time { return firstOpenedAt })
	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); !errors.Is(insertErr, errBackendFailed) {
		t.Fatalf("Insert() error = %v, want %v", insertErr, errBackendFailed)
	}

	probeAt := firstOpenedAt.Add(config.OpenTimeout)
	txnBreaker.machine.SetNow(func() time.Time { return probeAt })

	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); !errors.Is(insertErr, errBackendFailed) {
		t.Fatalf("probe Insert() error = %v, want %v", insertErr, errBackendFailed)
	}
	if state := txnBreaker.State(); state != BreakerOpen {
		t.Fatalf("state after failed probe = %s, want %s", state, BreakerOpen)
	}

	// OpenTimeout must have reset from the failed-probe time, not the
	// original trip time: just after probeAt it should still fail fast.
	txnBreaker.machine.SetNow(func() time.Time { return probeAt.Add(time.Second) })
	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); !errors.Is(insertErr, ErrCircuitOpen) {
		t.Errorf("Insert() error = %v, want %v (timeout should have restarted from the failed probe)", insertErr, ErrCircuitOpen)
	}

	// Once the new OpenTimeout window (measured from probeAt) has fully
	// elapsed, a probe should be allowed through again.
	txnBreaker.machine.SetNow(func() time.Time { return probeAt.Add(config.OpenTimeout) })
	fake.setInsertErr(nil)
	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); insertErr != nil {
		t.Errorf("second probe Insert() error = %v, want nil", insertErr)
	}
	if state := txnBreaker.State(); state != BreakerClosed {
		t.Errorf("state after second successful probe = %s, want %s", state, BreakerClosed)
	}
}

func TestTransactionRepositoryBreaker_HalfOpen_ProbeTimeoutBoundsHungProbe(t *testing.T) {
	const probeTimeout = 20 * time.Millisecond
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute, ProbeTimeout: probeTimeout}
	txnBreaker, err := NewTransactionRepositoryBreaker(hangingTransactionRepository{}, config)
	if err != nil {
		t.Fatalf("NewTransactionRepositoryBreaker() error = %v", err)
	}

	openedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	txnBreaker.machine.ForceOpen(openedAt)

	txnBreaker.machine.SetNow(func() time.Time { return openedAt.Add(config.OpenTimeout) })

	probeErrChan := make(chan error, 1)
	go func() {
		_, insertErr := txnBreaker.Insert(context.Background(), Transaction{})
		probeErrChan <- insertErr
	}()

	select {
	case probeErr := <-probeErrChan:
		if !errors.Is(probeErr, context.DeadlineExceeded) {
			t.Errorf("probe Insert() error = %v, want %v", probeErr, context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("probe Insert() did not return within 1s, want it bounded by ProbeTimeout")
	}

	if state := txnBreaker.State(); state != BreakerOpen {
		t.Errorf("state after timed-out probe = %s, want %s", state, BreakerOpen)
	}
}

func TestTransactionRepositoryBreaker_HalfOpen_ConcurrentProbeGate(t *testing.T) {
	fake := &fakeTransactionRepository{}
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}
	txnBreaker, err := NewTransactionRepositoryBreaker(fake, config)
	if err != nil {
		t.Fatalf("NewTransactionRepositoryBreaker() error = %v", err)
	}

	openedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fake.setInsertErr(errBackendFailed)
	txnBreaker.machine.SetNow(func() time.Time { return openedAt })
	if _, insertErr := txnBreaker.Insert(context.Background(), Transaction{}); !errors.Is(insertErr, errBackendFailed) {
		t.Fatalf("Insert() error = %v, want %v", insertErr, errBackendFailed)
	}

	txnBreaker.machine.SetNow(func() time.Time { return openedAt.Add(config.OpenTimeout) })
	fake.setInsertErr(nil)
	// Only wire up the started/block synchronization for the probe call
	// itself, so the earlier trip call above doesn't consume the buffered
	// started signal the goroutine below waits on.
	startedChan := make(chan struct{}, 1)
	blockChan := make(chan struct{})
	fake.setInsertSync(startedChan, blockChan)

	probeErrChan := make(chan error, 1)
	go func() {
		_, insertErr := txnBreaker.Insert(context.Background(), Transaction{})
		probeErrChan <- insertErr
	}()

	// Wait until the probe has actually entered the wrapped repository
	// (state is already half-open by that point) before firing the
	// concurrent second call, so it deterministically observes half-open.
	<-startedChan

	_, secondErr := txnBreaker.Insert(context.Background(), Transaction{})
	if !errors.Is(secondErr, ErrCircuitOpen) {
		t.Errorf("concurrent second call error = %v, want %v", secondErr, ErrCircuitOpen)
	}

	close(blockChan)
	if probeErr := <-probeErrChan; probeErr != nil {
		t.Errorf("probe Insert() error = %v, want nil", probeErr)
	}

	if calls := fake.totalCalls(); calls != 2 {
		t.Errorf("wrapped repository call count = %d, want 2 (initial trip + single probe, no double-probe)", calls)
	}
	if state := txnBreaker.State(); state != BreakerClosed {
		t.Errorf("state after concurrent half-open window = %s, want %s", state, BreakerClosed)
	}
}

func TestReconciledStateRepositoryBreaker_IgnoredErrorsDoNotTripBreaker(t *testing.T) {
	const threshold = uint32(3)

	testCases := []struct {
		name         string
		method       string
		err          error
		wantEndState BreakerState
	}{
		{
			name:         "Upsert: repeated ErrStaleReconciledState does not trip the breaker",
			method:       "Upsert",
			err:          fmt.Errorf("db upsert: %w", ErrStaleReconciledState),
			wantEndState: BreakerClosed,
		},
		{
			name:         "Upsert: repeated non-ignored error trips the breaker",
			method:       "Upsert",
			err:          errBackendFailed,
			wantEndState: BreakerOpen,
		},
		{
			name:         "Get: repeated ErrNotFound does not trip the breaker",
			method:       "Get",
			err:          fmt.Errorf("db lookup: %w", ErrNotFound),
			wantEndState: BreakerClosed,
		},
		{
			name:         "Get: repeated non-ignored error trips the breaker",
			method:       "Get",
			err:          errBackendFailed,
			wantEndState: BreakerOpen,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakeReconciledStateRepository{}
			stateBreaker, err := NewReconciledStateRepositoryBreaker(fake, BreakerConfig{FailureThreshold: threshold, OpenTimeout: time.Minute})
			if err != nil {
				t.Fatalf("NewReconciledStateRepositoryBreaker() error = %v", err)
			}

			for i := uint32(0); i < threshold; i++ {
				var callErr error
				switch testCase.method {
				case "Upsert":
					fake.setUpsertErr(testCase.err)
					callErr = stateBreaker.Upsert(context.Background(), ReconciledState{})
				case "Get":
					fake.setGetErr(testCase.err)
					_, callErr = stateBreaker.Get(context.Background(), "tenant-a", "AAPL")
				default:
					t.Fatalf("unhandled method %q", testCase.method)
				}
				if !errors.Is(callErr, testCase.err) {
					t.Fatalf("call error = %v, want it to wrap %v (sentinel must still reach the caller)", callErr, testCase.err)
				}
			}

			if state := stateBreaker.State(); state != testCase.wantEndState {
				t.Errorf("state after %d calls = %s, want %s", threshold, state, testCase.wantEndState)
			}
			if calls := fake.totalCalls(); calls != int(threshold) {
				t.Errorf("wrapped repository call count = %d, want %d (breaker must not have opened early)", calls, threshold)
			}
		})
	}
}

func TestReconciledStateRepositoryBreaker_OpenState_FailsFast(t *testing.T) {
	fake := &fakeReconciledStateRepository{upsertErr: errBackendFailed}
	stateBreaker, err := NewReconciledStateRepositoryBreaker(fake, BreakerConfig{FailureThreshold: 2, OpenTimeout: time.Minute})
	if err != nil {
		t.Fatalf("NewReconciledStateRepositoryBreaker() error = %v", err)
	}

	for i := 0; i < 2; i++ {
		if upsertErr := stateBreaker.Upsert(context.Background(), ReconciledState{}); !errors.Is(upsertErr, errBackendFailed) {
			t.Fatalf("Upsert() error = %v, want %v", upsertErr, errBackendFailed)
		}
	}
	if state := stateBreaker.State(); state != BreakerOpen {
		t.Fatalf("state after threshold failures = %s, want %s", state, BreakerOpen)
	}

	callsBeforeOpenAttempts := fake.totalCalls()

	for i := 0; i < 3; i++ {
		upsertErr := stateBreaker.Upsert(context.Background(), ReconciledState{})
		if !errors.Is(upsertErr, ErrCircuitOpen) {
			t.Errorf("Upsert() error = %v, want %v", upsertErr, ErrCircuitOpen)
		}
	}
	if _, getErr := stateBreaker.Get(context.Background(), "tenant-a", "AAPL"); !errors.Is(getErr, ErrCircuitOpen) {
		t.Errorf("Get() error = %v, want %v", getErr, ErrCircuitOpen)
	}

	if calls := fake.totalCalls(); calls != callsBeforeOpenAttempts {
		t.Errorf("wrapped repository call count = %d, want unchanged from %d (open breaker must not reach it)", calls, callsBeforeOpenAttempts)
	}
	if state := stateBreaker.State(); state != BreakerOpen {
		t.Errorf("state = %s, want %s", state, BreakerOpen)
	}
}

func TestReconciledStateRepositoryBreaker_HalfOpen_Recovery(t *testing.T) {
	testCases := []struct {
		name         string
		probeErr     error
		wantEndState BreakerState
	}{
		{
			name:         "successful probe closes the breaker",
			probeErr:     nil,
			wantEndState: BreakerClosed,
		},
		{
			name:         "failed probe reopens the breaker",
			probeErr:     errBackendFailed,
			wantEndState: BreakerOpen,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakeReconciledStateRepository{upsertErr: errBackendFailed}
			config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}
			stateBreaker, err := NewReconciledStateRepositoryBreaker(fake, config)
			if err != nil {
				t.Fatalf("NewReconciledStateRepositoryBreaker() error = %v", err)
			}

			openedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			stateBreaker.machine.SetNow(func() time.Time { return openedAt })
			if upsertErr := stateBreaker.Upsert(context.Background(), ReconciledState{}); !errors.Is(upsertErr, errBackendFailed) {
				t.Fatalf("Upsert() error = %v, want %v", upsertErr, errBackendFailed)
			}
			if state := stateBreaker.State(); state != BreakerOpen {
				t.Fatalf("state after tripping = %s, want %s", state, BreakerOpen)
			}

			stateBreaker.machine.SetNow(func() time.Time { return openedAt.Add(config.OpenTimeout) })
			fake.setUpsertErr(testCase.probeErr)

			probeErr := stateBreaker.Upsert(context.Background(), ReconciledState{})
			if !errors.Is(probeErr, testCase.probeErr) {
				t.Fatalf("probe Upsert() error = %v, want %v", probeErr, testCase.probeErr)
			}
			if state := stateBreaker.State(); state != testCase.wantEndState {
				t.Errorf("state after probe = %s, want %s", state, testCase.wantEndState)
			}
		})
	}
}
