package ingestion

import (
	"context"
	"fmt"

	"github.com/anwinsenp/go-transaction-control-plane/internal/corebreaker"
)

// ErrCircuitOpen is returned by CircuitBreaker.Publish when the breaker is
// open (or a half-open probe is already in flight) and the call is failed
// fast without reaching the wrapped Publisher.
var ErrCircuitOpen = corebreaker.ErrOpen

// ErrInvalidBreakerConfig indicates a BreakerConfig failed validation.
var ErrInvalidBreakerConfig = corebreaker.ErrInvalidConfig

// BreakerState is one of the three states a CircuitBreaker can be in.
type BreakerState = corebreaker.State

// The three states a CircuitBreaker can be in.
const (
	BreakerClosed   = corebreaker.Closed
	BreakerOpen     = corebreaker.Open
	BreakerHalfOpen = corebreaker.HalfOpen
)

// BreakerConfig controls when a CircuitBreaker trips and how long it stays
// open before probing the wrapped Publisher again.
type BreakerConfig = corebreaker.Config

// CircuitBreaker wraps a Publisher and fails fast while it appears
// unhealthy, rather than letting every caller block on a degraded
// downstream. It implements Publisher itself, so it drops in wherever a
// Publisher is expected.
type CircuitBreaker struct {
	next    Publisher
	machine *corebreaker.Machine
}

var _ Publisher = (*CircuitBreaker)(nil)

// NewCircuitBreaker builds a CircuitBreaker that wraps next according to
// config. It starts closed.
func NewCircuitBreaker(next Publisher, config BreakerConfig) (*CircuitBreaker, error) {
	if next == nil {
		return nil, fmt.Errorf("new circuit breaker: next Publisher must not be nil")
	}
	machine, err := corebreaker.New(config)
	if err != nil {
		return nil, fmt.Errorf("new circuit breaker: %w", err)
	}
	return &CircuitBreaker{next: next, machine: machine}, nil
}

// State reports the breaker's current state. It's primarily useful for
// tests and observability, not for callers deciding whether to publish —
// Publish already makes that decision internally.
func (breaker *CircuitBreaker) State() BreakerState {
	return breaker.machine.State()
}

// Publish forwards event to the wrapped Publisher unless the breaker is
// open, in which case it fails fast with ErrCircuitOpen. A single probe
// call is allowed through once OpenTimeout has elapsed, to test whether the
// downstream has recovered.
func (breaker *CircuitBreaker) Publish(ctx context.Context, event Event) error {
	allowed, isProbe := breaker.machine.Allow()
	if !allowed {
		return ErrCircuitOpen
	}

	publishCtx, cancel := breaker.machine.ProbeContext(ctx, isProbe)
	defer cancel()

	err := breaker.next.Publish(publishCtx, event)
	breaker.machine.Record(err)
	return err
}
