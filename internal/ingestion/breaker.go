package ingestion

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrCircuitOpen is returned by CircuitBreaker.Publish when the breaker is
// open (or a half-open probe is already in flight) and the call is failed
// fast without reaching the wrapped Publisher.
var ErrCircuitOpen = errors.New("circuit breaker open")

// ErrInvalidBreakerConfig indicates a BreakerConfig failed validation.
var ErrInvalidBreakerConfig = errors.New("invalid circuit breaker config")

// BreakerState is one of the three states a CircuitBreaker can be in.
type BreakerState int32

const (
	// BreakerClosed passes every call through to the wrapped Publisher.
	BreakerClosed BreakerState = iota
	// BreakerOpen fails every call fast without touching the wrapped
	// Publisher.
	BreakerOpen
	// BreakerHalfOpen allows a single probe call through to test whether
	// the wrapped Publisher has recovered.
	BreakerHalfOpen
)

// String returns the human-readable name of the state, used in error
// messages and test failure output.
func (state BreakerState) String() string {
	switch state {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("unknown(%d)", int32(state))
	}
}

// BreakerConfig controls when a CircuitBreaker trips and how long it stays
// open before probing the wrapped Publisher again.
type BreakerConfig struct {
	// FailureThreshold is the number of consecutive publish failures
	// required to trip the breaker from closed to open.
	FailureThreshold uint32
	// OpenTimeout is how long the breaker stays open before allowing a
	// single half-open probe call through.
	OpenTimeout time.Duration
	// ProbeTimeout bounds how long a half-open probe call may run before it
	// is canceled, so a hung wrapped Publisher can't hold the single-probe
	// gate open indefinitely. Zero disables this bound, relying entirely on
	// the caller's ctx; it does not affect normal closed-state calls.
	ProbeTimeout time.Duration
}

func (config BreakerConfig) validate() error {
	if config.FailureThreshold == 0 {
		return fmt.Errorf("%w: FailureThreshold must be positive", ErrInvalidBreakerConfig)
	}
	if config.OpenTimeout <= 0 {
		return fmt.Errorf("%w: OpenTimeout %s must be a positive duration", ErrInvalidBreakerConfig, config.OpenTimeout)
	}
	if config.ProbeTimeout < 0 {
		return fmt.Errorf("%w: ProbeTimeout %s must not be negative", ErrInvalidBreakerConfig, config.ProbeTimeout)
	}
	return nil
}

// CircuitBreaker wraps a Publisher and fails fast while it appears
// unhealthy, rather than letting every caller block on a degraded
// downstream. It implements Publisher itself, so it drops in wherever a
// Publisher is expected.
type CircuitBreaker struct {
	next   Publisher
	config BreakerConfig
	now    func() time.Time

	// mutex guards every field below: Publish can be called concurrently
	// by many request goroutines, and state transitions (especially the
	// single-probe gate in half-open) must be decided atomically.
	mutex               sync.Mutex
	state               BreakerState
	consecutiveFailures uint32
	openedAt            time.Time
}

var _ Publisher = (*CircuitBreaker)(nil)

// NewCircuitBreaker builds a CircuitBreaker that wraps next according to
// config. It starts closed.
func NewCircuitBreaker(next Publisher, config BreakerConfig) (*CircuitBreaker, error) {
	if next == nil {
		return nil, fmt.Errorf("new circuit breaker: next Publisher must not be nil")
	}
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("new circuit breaker: %w", err)
	}
	return &CircuitBreaker{
		next:   next,
		config: config,
		now:    time.Now,
		state:  BreakerClosed,
	}, nil
}

// State reports the breaker's current state. It's primarily useful for
// tests and observability, not for callers deciding whether to publish —
// Publish already makes that decision internally.
func (breaker *CircuitBreaker) State() BreakerState {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()
	return breaker.state
}

// Publish forwards event to the wrapped Publisher unless the breaker is
// open, in which case it fails fast with ErrCircuitOpen. A single probe
// call is allowed through once OpenTimeout has elapsed, to test whether the
// downstream has recovered.
func (breaker *CircuitBreaker) Publish(ctx context.Context, event Event) error {
	allowed, isProbe := breaker.allow()
	if !allowed {
		return ErrCircuitOpen
	}

	publishCtx := ctx
	if isProbe && breaker.config.ProbeTimeout > 0 {
		var cancel context.CancelFunc
		publishCtx, cancel = context.WithTimeout(ctx, breaker.config.ProbeTimeout)
		defer cancel()
	}

	err := breaker.next.Publish(publishCtx, event)
	breaker.recordResult(err)
	return err
}

// allow decides whether the call in progress should reach the wrapped
// Publisher, transitioning open to half-open once OpenTimeout has elapsed.
// The second return value reports whether this call is the half-open probe
// itself, as opposed to a normal closed-state call.
func (breaker *CircuitBreaker) allow() (bool, bool) {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()

	switch breaker.state {
	case BreakerClosed:
		return true, false
	case BreakerHalfOpen:
		// Only one probe is allowed in flight at a time; concurrent
		// callers fail fast until the probe resolves.
		return false, false
	case BreakerOpen:
		if breaker.now().Sub(breaker.openedAt) < breaker.config.OpenTimeout {
			return false, false
		}
		breaker.state = BreakerHalfOpen
		return true, true
	default:
		return false, false
	}
}

// recordResult applies the outcome of a call that was allowed through to
// the breaker's state.
func (breaker *CircuitBreaker) recordResult(err error) {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()

	switch breaker.state {
	case BreakerHalfOpen:
		if err != nil {
			breaker.state = BreakerOpen
			breaker.openedAt = breaker.now()
			breaker.consecutiveFailures = 0
			return
		}
		breaker.state = BreakerClosed
		breaker.consecutiveFailures = 0
	case BreakerClosed:
		if err != nil {
			breaker.consecutiveFailures++
			if breaker.consecutiveFailures >= breaker.config.FailureThreshold {
				breaker.state = BreakerOpen
				breaker.openedAt = breaker.now()
				breaker.consecutiveFailures = 0
			}
			return
		}
		breaker.consecutiveFailures = 0
	}
}
