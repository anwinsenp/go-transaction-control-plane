package ingestion

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakePublisher is a Publisher test double that records how many times
// Publish was called and returns a caller-configured error, so breaker
// tests can drive success/failure without a live Kafka broker.
type fakePublisher struct {
	mutex     sync.Mutex
	callCount int
	err       error

	// started, if non-nil, receives a value at the start of every Publish
	// call, letting a test synchronize with an in-flight probe.
	started chan struct{}
	// block, if non-nil, is read from before Publish returns, letting a
	// test hold a call open to deterministically order concurrent callers.
	block chan struct{}
}

func (fake *fakePublisher) Publish(_ context.Context, _ Event) error {
	fake.mutex.Lock()
	fake.callCount++
	callErr := fake.err
	startedChan := fake.started
	blockChan := fake.block
	fake.mutex.Unlock()

	if startedChan != nil {
		startedChan <- struct{}{}
	}
	if blockChan != nil {
		<-blockChan
	}
	return callErr
}

func (fake *fakePublisher) calls() int {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	return fake.callCount
}

func (fake *fakePublisher) setErr(err error) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.err = err
}

func (fake *fakePublisher) setSync(started, block chan struct{}) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.started = started
	fake.block = block
}

var errPublishFailed = errors.New("publish failed")

func TestNewCircuitBreaker(t *testing.T) {
	testCases := []struct {
		name       string
		next       Publisher
		config     BreakerConfig
		wantErr    error
		wantErrMsg string
	}{
		{
			name:   "valid config succeeds",
			next:   &fakePublisher{},
			config: BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second},
		},
		{
			name:       "nil next publisher is rejected",
			next:       nil,
			config:     BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Second},
			wantErrMsg: "next Publisher must not be nil",
		},
		{
			name:    "zero failure threshold is rejected",
			next:    &fakePublisher{},
			config:  BreakerConfig{FailureThreshold: 0, OpenTimeout: time.Second},
			wantErr: ErrInvalidBreakerConfig,
		},
		{
			name:    "zero open timeout is rejected",
			next:    &fakePublisher{},
			config:  BreakerConfig{FailureThreshold: 3, OpenTimeout: 0},
			wantErr: ErrInvalidBreakerConfig,
		},
		{
			name:    "negative open timeout is rejected",
			next:    &fakePublisher{},
			config:  BreakerConfig{FailureThreshold: 3, OpenTimeout: -time.Second},
			wantErr: ErrInvalidBreakerConfig,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			breaker, err := NewCircuitBreaker(testCase.next, testCase.config)

			if testCase.wantErr == nil && testCase.wantErrMsg == "" {
				if err != nil {
					t.Fatalf("NewCircuitBreaker() error = %v, want nil", err)
				}
				if breaker == nil {
					t.Fatal("NewCircuitBreaker() returned nil breaker with nil error")
				}
				if state := breaker.State(); state != BreakerClosed {
					t.Errorf("initial state = %s, want %s", state, BreakerClosed)
				}
				return
			}

			if err == nil {
				t.Fatal("NewCircuitBreaker() error = nil, want non-nil")
			}
			if breaker != nil {
				t.Errorf("NewCircuitBreaker() breaker = %v, want nil on error", breaker)
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

func errorContains(err error, substring string) bool {
	return err != nil && strings.Contains(err.Error(), substring)
}

func TestCircuitBreaker_ClosedState(t *testing.T) {
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
			results:        []error{errPublishFailed, errPublishFailed},
			wantEndState:   BreakerClosed,
			wantFinalCalls: 2,
		},
		{
			name:           "an interspersed success resets the failure count",
			results:        []error{errPublishFailed, errPublishFailed, nil, errPublishFailed, errPublishFailed},
			wantEndState:   BreakerClosed,
			wantFinalCalls: 5,
		},
		{
			name:           "reaching the threshold trips the breaker open",
			results:        []error{errPublishFailed, errPublishFailed, errPublishFailed},
			wantEndState:   BreakerOpen,
			wantFinalCalls: 3,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakePublisher{}
			breaker, err := NewCircuitBreaker(fake, BreakerConfig{FailureThreshold: 3, OpenTimeout: time.Minute})
			if err != nil {
				t.Fatalf("NewCircuitBreaker() error = %v", err)
			}

			for _, result := range testCase.results {
				fake.setErr(result)
				publishErr := breaker.Publish(context.Background(), Event{})
				if !errors.Is(publishErr, result) {
					t.Fatalf("Publish() error = %v, want %v", publishErr, result)
				}
			}

			if state := breaker.State(); state != testCase.wantEndState {
				t.Errorf("state = %s, want %s", state, testCase.wantEndState)
			}
			if calls := fake.calls(); calls != testCase.wantFinalCalls {
				t.Errorf("wrapped publisher call count = %d, want %d", calls, testCase.wantFinalCalls)
			}
		})
	}
}

func TestCircuitBreaker_OpenState_FailsFast(t *testing.T) {
	fake := &fakePublisher{err: errPublishFailed}
	breaker, err := NewCircuitBreaker(fake, BreakerConfig{FailureThreshold: 2, OpenTimeout: time.Minute})
	if err != nil {
		t.Fatalf("NewCircuitBreaker() error = %v", err)
	}

	for i := 0; i < 2; i++ {
		if publishErr := breaker.Publish(context.Background(), Event{}); !errors.Is(publishErr, errPublishFailed) {
			t.Fatalf("Publish() error = %v, want %v", publishErr, errPublishFailed)
		}
	}
	if state := breaker.State(); state != BreakerOpen {
		t.Fatalf("state after threshold failures = %s, want %s", state, BreakerOpen)
	}

	callsBeforeOpenAttempts := fake.calls()

	for i := 0; i < 3; i++ {
		publishErr := breaker.Publish(context.Background(), Event{})
		if !errors.Is(publishErr, ErrCircuitOpen) {
			t.Errorf("Publish() error = %v, want %v", publishErr, ErrCircuitOpen)
		}
	}

	if calls := fake.calls(); calls != callsBeforeOpenAttempts {
		t.Errorf("wrapped publisher call count = %d, want unchanged from %d (open breaker must not reach it)", calls, callsBeforeOpenAttempts)
	}
	if state := breaker.State(); state != BreakerOpen {
		t.Errorf("state = %s, want %s", state, BreakerOpen)
	}
}

func TestCircuitBreaker_HalfOpen_RecoverySuccess(t *testing.T) {
	fake := &fakePublisher{err: errPublishFailed}
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}
	breaker, err := NewCircuitBreaker(fake, config)
	if err != nil {
		t.Fatalf("NewCircuitBreaker() error = %v", err)
	}

	openedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	breaker.now = func() time.Time { return openedAt }
	if publishErr := breaker.Publish(context.Background(), Event{}); !errors.Is(publishErr, errPublishFailed) {
		t.Fatalf("Publish() error = %v, want %v", publishErr, errPublishFailed)
	}
	if state := breaker.State(); state != BreakerOpen {
		t.Fatalf("state after tripping = %s, want %s", state, BreakerOpen)
	}
	breaker.mutex.Lock()
	consecutiveFailuresOnTrip := breaker.consecutiveFailures
	breaker.mutex.Unlock()
	if consecutiveFailuresOnTrip != 0 {
		t.Errorf("consecutiveFailures after Closed->Open trip = %d, want 0", consecutiveFailuresOnTrip)
	}

	breaker.now = func() time.Time { return openedAt.Add(config.OpenTimeout) }
	fake.setErr(nil)

	if publishErr := breaker.Publish(context.Background(), Event{}); publishErr != nil {
		t.Fatalf("probe Publish() error = %v, want nil", publishErr)
	}
	if state := breaker.State(); state != BreakerClosed {
		t.Fatalf("state after successful probe = %s, want %s", state, BreakerClosed)
	}

	// consecutiveFailures must have reset: a single subsequent failure
	// (with FailureThreshold == 1) should trip the breaker again rather
	// than requiring the pre-trip failure to still be counted.
	fake.setErr(errPublishFailed)
	if publishErr := breaker.Publish(context.Background(), Event{}); !errors.Is(publishErr, errPublishFailed) {
		t.Fatalf("Publish() error = %v, want %v", publishErr, errPublishFailed)
	}
	if state := breaker.State(); state != BreakerOpen {
		t.Errorf("state after post-recovery failure = %s, want %s", state, BreakerOpen)
	}
}

func TestCircuitBreaker_HalfOpen_RecoveryFailure(t *testing.T) {
	fake := &fakePublisher{err: errPublishFailed}
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}
	breaker, err := NewCircuitBreaker(fake, config)
	if err != nil {
		t.Fatalf("NewCircuitBreaker() error = %v", err)
	}

	firstOpenedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	breaker.now = func() time.Time { return firstOpenedAt }
	if publishErr := breaker.Publish(context.Background(), Event{}); !errors.Is(publishErr, errPublishFailed) {
		t.Fatalf("Publish() error = %v, want %v", publishErr, errPublishFailed)
	}

	probeAt := firstOpenedAt.Add(config.OpenTimeout)
	breaker.now = func() time.Time { return probeAt }

	if publishErr := breaker.Publish(context.Background(), Event{}); !errors.Is(publishErr, errPublishFailed) {
		t.Fatalf("probe Publish() error = %v, want %v", publishErr, errPublishFailed)
	}
	if state := breaker.State(); state != BreakerOpen {
		t.Fatalf("state after failed probe = %s, want %s", state, BreakerOpen)
	}
	breaker.mutex.Lock()
	consecutiveFailuresOnReopen := breaker.consecutiveFailures
	breaker.mutex.Unlock()
	if consecutiveFailuresOnReopen != 0 {
		t.Errorf("consecutiveFailures after HalfOpen->Open reopen = %d, want 0", consecutiveFailuresOnReopen)
	}

	// OpenTimeout must have reset from the failed-probe time, not the
	// original trip time: just after probeAt it should still fail fast.
	breaker.now = func() time.Time { return probeAt.Add(time.Second) }
	if publishErr := breaker.Publish(context.Background(), Event{}); !errors.Is(publishErr, ErrCircuitOpen) {
		t.Errorf("Publish() error = %v, want %v (timeout should have restarted from the failed probe)", publishErr, ErrCircuitOpen)
	}

	// Once the new OpenTimeout window (measured from probeAt) has fully
	// elapsed, a probe should be allowed through again.
	breaker.now = func() time.Time { return probeAt.Add(config.OpenTimeout) }
	fake.setErr(nil)
	if publishErr := breaker.Publish(context.Background(), Event{}); publishErr != nil {
		t.Errorf("second probe Publish() error = %v, want nil", publishErr)
	}
	if state := breaker.State(); state != BreakerClosed {
		t.Errorf("state after second successful probe = %s, want %s", state, BreakerClosed)
	}
}

// hangingPublisher is a Publisher test double whose Publish blocks until the
// caller's ctx is canceled, letting a test observe whether a bound (such as
// BreakerConfig.ProbeTimeout) was actually applied to a hung call.
type hangingPublisher struct{}

func (hangingPublisher) Publish(ctx context.Context, _ Event) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestCircuitBreaker_HalfOpen_ProbeTimeoutBoundsHungProbe(t *testing.T) {
	const probeTimeout = 20 * time.Millisecond
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute, ProbeTimeout: probeTimeout}
	breaker, err := NewCircuitBreaker(hangingPublisher{}, config)
	if err != nil {
		t.Fatalf("NewCircuitBreaker() error = %v", err)
	}

	openedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	breaker.now = func() time.Time { return openedAt }
	breaker.mutex.Lock()
	breaker.state = BreakerOpen
	breaker.openedAt = openedAt
	breaker.mutex.Unlock()

	breaker.now = func() time.Time { return openedAt.Add(config.OpenTimeout) }

	probeErrChan := make(chan error, 1)
	go func() {
		probeErrChan <- breaker.Publish(context.Background(), Event{})
	}()

	select {
	case probeErr := <-probeErrChan:
		if !errors.Is(probeErr, context.DeadlineExceeded) {
			t.Errorf("probe Publish() error = %v, want %v", probeErr, context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("probe Publish() did not return within 1s, want it bounded by ProbeTimeout")
	}

	if state := breaker.State(); state != BreakerOpen {
		t.Errorf("state after timed-out probe = %s, want %s", state, BreakerOpen)
	}
}

func TestCircuitBreaker_HalfOpen_ConcurrentProbeGate(t *testing.T) {
	fake := &fakePublisher{}
	config := BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Minute}
	breaker, err := NewCircuitBreaker(fake, config)
	if err != nil {
		t.Fatalf("NewCircuitBreaker() error = %v", err)
	}

	openedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fake.setErr(errPublishFailed)
	breaker.now = func() time.Time { return openedAt }
	if publishErr := breaker.Publish(context.Background(), Event{}); !errors.Is(publishErr, errPublishFailed) {
		t.Fatalf("Publish() error = %v, want %v", publishErr, errPublishFailed)
	}

	breaker.now = func() time.Time { return openedAt.Add(config.OpenTimeout) }
	fake.setErr(nil)
	// Only wire up the started/block synchronization for the probe call
	// itself, so the earlier trip call above doesn't consume the buffered
	// started signal the goroutine below waits on.
	startedChan := make(chan struct{}, 1)
	blockChan := make(chan struct{})
	fake.setSync(startedChan, blockChan)

	probeErrChan := make(chan error, 1)
	go func() {
		probeErrChan <- breaker.Publish(context.Background(), Event{})
	}()

	// Wait until the probe has actually entered the wrapped Publisher
	// (state is already half-open by that point) before firing the
	// concurrent second call, so it deterministically observes half-open.
	<-startedChan

	secondErr := breaker.Publish(context.Background(), Event{})
	if !errors.Is(secondErr, ErrCircuitOpen) {
		t.Errorf("concurrent second call error = %v, want %v", secondErr, ErrCircuitOpen)
	}

	close(blockChan)
	if probeErr := <-probeErrChan; probeErr != nil {
		t.Errorf("probe Publish() error = %v, want nil", probeErr)
	}

	if calls := fake.calls(); calls != 2 {
		t.Errorf("wrapped publisher call count = %d, want 2 (initial trip + single probe, no double-probe)", calls)
	}
	if state := breaker.State(); state != BreakerClosed {
		t.Errorf("state after concurrent half-open window = %s, want %s", state, BreakerClosed)
	}
}
