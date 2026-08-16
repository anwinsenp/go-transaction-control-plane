package ingestion

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewBackpressureLimiter(t *testing.T) {
	testCases := []struct {
		name       string
		next       Publisher
		config     BackpressureConfig
		wantErr    error
		wantErrMsg string
	}{
		{
			name:   "valid config succeeds",
			next:   &fakePublisher{},
			config: BackpressureConfig{MaxInFlight: 4},
		},
		{
			name:       "nil next publisher is rejected",
			next:       nil,
			config:     BackpressureConfig{MaxInFlight: 4},
			wantErrMsg: "next Publisher must not be nil",
		},
		{
			name:    "zero max in flight is rejected",
			next:    &fakePublisher{},
			config:  BackpressureConfig{MaxInFlight: 0},
			wantErr: ErrInvalidBackpressureConfig,
		},
		{
			name:    "negative max in flight is rejected",
			next:    &fakePublisher{},
			config:  BackpressureConfig{MaxInFlight: -1},
			wantErr: ErrInvalidBackpressureConfig,
		},
		{
			name:    "max in flight at ceiling succeeds",
			next:    &fakePublisher{},
			config:  BackpressureConfig{MaxInFlight: maxBackpressureInFlight},
			wantErr: nil,
		},
		{
			name:    "max in flight above ceiling is rejected",
			next:    &fakePublisher{},
			config:  BackpressureConfig{MaxInFlight: maxBackpressureInFlight + 1},
			wantErr: ErrInvalidBackpressureConfig,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			limiter, err := NewBackpressureLimiter(testCase.next, testCase.config)

			if testCase.wantErr == nil && testCase.wantErrMsg == "" {
				if err != nil {
					t.Fatalf("NewBackpressureLimiter() error = %v, want nil", err)
				}
				if limiter == nil {
					t.Fatal("NewBackpressureLimiter() returned nil limiter with nil error")
				}
				return
			}

			if err == nil {
				t.Fatal("NewBackpressureLimiter() error = nil, want non-nil")
			}
			if limiter != nil {
				t.Errorf("NewBackpressureLimiter() limiter = %v, want nil on error", limiter)
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

func TestBackpressureLimiter_QueueFull_FailsFastWithoutBlocking(t *testing.T) {
	const maxInFlight = 2

	fake := &fakePublisher{}
	startedChan := make(chan struct{}, maxInFlight)
	blockChan := make(chan struct{})
	fake.setSync(startedChan, blockChan)

	limiter, err := NewBackpressureLimiter(fake, BackpressureConfig{MaxInFlight: maxInFlight})
	if err != nil {
		t.Fatalf("NewBackpressureLimiter() error = %v", err)
	}

	var waitGroup sync.WaitGroup
	for i := 0; i < maxInFlight; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if publishErr := limiter.Publish(context.Background(), Event{}); publishErr != nil {
				t.Errorf("in-flight Publish() error = %v, want nil", publishErr)
			}
		}()
	}
	for i := 0; i < maxInFlight; i++ {
		<-startedChan
	}
	defer func() {
		close(blockChan)
		waitGroup.Wait()
	}()

	rejectedChan := make(chan error, 1)
	go func() {
		rejectedChan <- limiter.Publish(context.Background(), Event{})
	}()

	select {
	case rejectedErr := <-rejectedChan:
		if !errors.Is(rejectedErr, ErrPublishQueueFull) {
			t.Errorf("Publish() error = %v, want %v", rejectedErr, ErrPublishQueueFull)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Publish() did not return within 100ms, want it to fail fast without waiting for a slot")
	}

	if calls := fake.calls(); calls != maxInFlight {
		t.Errorf("wrapped publisher call count = %d, want %d (rejected call must not reach it)", calls, maxInFlight)
	}
}

func TestBackpressureLimiter_SlotFreedAfterInFlightCallCompletes(t *testing.T) {
	fake := &fakePublisher{}
	startedChan := make(chan struct{}, 1)
	blockChan := make(chan struct{})
	fake.setSync(startedChan, blockChan)

	limiter, err := NewBackpressureLimiter(fake, BackpressureConfig{MaxInFlight: 1})
	if err != nil {
		t.Fatalf("NewBackpressureLimiter() error = %v", err)
	}

	blockedErrChan := make(chan error, 1)
	go func() {
		blockedErrChan <- limiter.Publish(context.Background(), Event{})
	}()
	<-startedChan

	if rejectedErr := limiter.Publish(context.Background(), Event{}); !errors.Is(rejectedErr, ErrPublishQueueFull) {
		t.Fatalf("Publish() error = %v, want %v while the single slot is occupied", rejectedErr, ErrPublishQueueFull)
	}

	close(blockChan)
	if blockedErr := <-blockedErrChan; blockedErr != nil {
		t.Fatalf("blocked Publish() error = %v, want nil", blockedErr)
	}

	if publishErr := limiter.Publish(context.Background(), Event{}); publishErr != nil {
		t.Errorf("Publish() error = %v, want nil once the slot has been released", publishErr)
	}

	if calls := fake.calls(); calls != 2 {
		t.Errorf("wrapped publisher call count = %d, want 2 (initial blocked call + follow-up, no double count for the rejection)", calls)
	}
}

// concurrencyTrackingPublisher is a Publisher test double that records the
// highest number of Publish calls it observed running simultaneously, so a
// test can assert a limiter never lets more than its configured MaxInFlight
// through at once.
type concurrencyTrackingPublisher struct {
	current     int32
	maxObserved int32
}

func (tracker *concurrencyTrackingPublisher) Publish(_ context.Context, _ Event) error {
	current := atomic.AddInt32(&tracker.current, 1)
	for {
		observed := atomic.LoadInt32(&tracker.maxObserved)
		if current <= observed {
			break
		}
		if atomic.CompareAndSwapInt32(&tracker.maxObserved, observed, current) {
			break
		}
	}

	time.Sleep(time.Millisecond)

	atomic.AddInt32(&tracker.current, -1)
	return nil
}

func TestBackpressureLimiter_ConcurrentSafety_NeverExceedsMaxInFlight(t *testing.T) {
	const callerCount = 50

	tracker := &concurrencyTrackingPublisher{}
	limiter, err := NewBackpressureLimiter(tracker, BackpressureConfig{MaxInFlight: 1})
	if err != nil {
		t.Fatalf("NewBackpressureLimiter() error = %v", err)
	}

	var successCount int32
	var waitGroup sync.WaitGroup
	for i := 0; i < callerCount; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			publishErr := limiter.Publish(context.Background(), Event{})
			if publishErr == nil {
				atomic.AddInt32(&successCount, 1)
				return
			}
			if !errors.Is(publishErr, ErrPublishQueueFull) {
				t.Errorf("Publish() error = %v, want nil or %v", publishErr, ErrPublishQueueFull)
			}
		}()
	}
	waitGroup.Wait()

	if maxObserved := atomic.LoadInt32(&tracker.maxObserved); maxObserved > 1 {
		t.Errorf("max concurrent calls observed by wrapped publisher = %d, want at most 1", maxObserved)
	}
	if atomic.LoadInt32(&successCount) == 0 {
		t.Error("successCount = 0, want at least one Publish() call to succeed")
	}
}
