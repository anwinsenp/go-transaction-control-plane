package ingestion

import (
	"context"
	"errors"
	"fmt"
)

// ErrPublishQueueFull is returned by BackpressureLimiter.Publish when the
// bounded in-flight queue in front of the wrapped Publisher is full and the
// call is failed fast without waiting for a slot to free up.
var ErrPublishQueueFull = errors.New("publish queue full")

// ErrInvalidBackpressureConfig indicates a BackpressureConfig failed
// validation.
var ErrInvalidBackpressureConfig = errors.New("invalid backpressure config")

// maxBackpressureInFlight is the upper bound accepted for
// BackpressureConfig.MaxInFlight — far beyond any realistic concurrent
// in-flight publish volume for this service, it exists to guard against a
// fat-fingered env var silently allocating an oversized channel.
const maxBackpressureInFlight = 100_000

// BackpressureConfig controls how many concurrent calls a BackpressureLimiter
// allows into its wrapped Publisher before shedding load.
type BackpressureConfig struct {
	// MaxInFlight is the number of concurrent Publish calls allowed to reach
	// the wrapped Publisher at once. It sizes the limiter's bounded slot
	// queue.
	MaxInFlight int
}

func (config BackpressureConfig) validate() error {
	if config.MaxInFlight <= 0 {
		return fmt.Errorf("%w: MaxInFlight must be positive", ErrInvalidBackpressureConfig)
	}
	if config.MaxInFlight > maxBackpressureInFlight {
		return fmt.Errorf("%w: MaxInFlight must not exceed %d", ErrInvalidBackpressureConfig, maxBackpressureInFlight)
	}
	return nil
}

// BackpressureLimiter wraps a Publisher with a bounded queue of in-flight
// publish slots, so a slow or stuck downstream can't let unbounded request
// goroutines pile up on the hot path. It implements Publisher itself, so it
// drops in wherever a Publisher is expected.
type BackpressureLimiter struct {
	next  Publisher
	slots chan struct{}
}

var _ Publisher = (*BackpressureLimiter)(nil)

// NewBackpressureLimiter builds a BackpressureLimiter that wraps next,
// allowing at most config.MaxInFlight concurrent calls through to it.
func NewBackpressureLimiter(next Publisher, config BackpressureConfig) (*BackpressureLimiter, error) {
	if next == nil {
		return nil, fmt.Errorf("new backpressure limiter: next Publisher must not be nil")
	}
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("new backpressure limiter: %w", err)
	}
	return &BackpressureLimiter{
		next:  next,
		slots: make(chan struct{}, config.MaxInFlight),
	}, nil
}

// Publish forwards event to the wrapped Publisher if an in-flight slot is
// immediately available, or fails fast with ErrPublishQueueFull if
// MaxInFlight calls are already in flight. It never blocks waiting for a
// slot to free up — a stuck downstream must not be able to hold caller
// goroutines indefinitely.
func (limiter *BackpressureLimiter) Publish(ctx context.Context, event Event) error {
	select {
	case limiter.slots <- struct{}{}:
	default:
		return ErrPublishQueueFull
	}
	defer limiter.releaseSlot()

	return limiter.next.Publish(ctx, event)
}

// releaseSlot returns an in-flight slot acquired by Publish back to the
// bounded queue.
func (limiter *BackpressureLimiter) releaseSlot() {
	<-limiter.slots
}
