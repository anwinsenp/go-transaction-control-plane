// Package corebreaker implements the closed/open/half-open circuit breaker
// state machine shared by internal/ingestion and internal/ledger's
// dependency-specific circuit breakers. It has no knowledge of what it's
// gating: each caller decides what counts as a failure and reports it via
// Record.
package corebreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrOpen is returned by a wrapping breaker's call method when the Machine
// is open (or a half-open probe is already in flight) and the call is
// failed fast without reaching the wrapped dependency.
var ErrOpen = errors.New("circuit breaker open")

// ErrInvalidConfig indicates a Config failed validation.
var ErrInvalidConfig = errors.New("invalid circuit breaker config")

// State is one of the three states a Machine can be in.
type State int32

const (
	// Closed passes every call through to the wrapped dependency.
	Closed State = iota
	// Open fails every call fast without touching the wrapped dependency.
	Open
	// HalfOpen allows a single probe call through to test whether the
	// wrapped dependency has recovered.
	HalfOpen
)

// String returns the human-readable name of the state, used in error
// messages and test failure output.
func (state State) String() string {
	switch state {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("unknown(%d)", int32(state))
	}
}

// Config controls when a Machine trips and how long it stays open before
// probing the wrapped dependency again.
type Config struct {
	// FailureThreshold is the number of consecutive call failures required
	// to trip the breaker from closed to open.
	FailureThreshold uint32
	// OpenTimeout is how long the breaker stays open before allowing a
	// single half-open probe call through.
	OpenTimeout time.Duration
	// ProbeTimeout bounds how long a half-open probe call may run before it
	// is canceled, so a hung wrapped dependency can't hold the single-probe
	// gate open indefinitely. Zero disables this bound, relying entirely on
	// the caller's ctx; it does not affect normal closed-state calls.
	ProbeTimeout time.Duration
}

// Validate reports whether config's fields are usable, returning a
// non-nil error wrapping ErrInvalidConfig if not.
func (config Config) Validate() error {
	if config.FailureThreshold == 0 {
		return fmt.Errorf("%w: FailureThreshold must be positive", ErrInvalidConfig)
	}
	if config.OpenTimeout <= 0 {
		return fmt.Errorf("%w: OpenTimeout %s must be a positive duration", ErrInvalidConfig, config.OpenTimeout)
	}
	if config.ProbeTimeout < 0 {
		return fmt.Errorf("%w: ProbeTimeout %s must not be negative", ErrInvalidConfig, config.ProbeTimeout)
	}
	return nil
}

// Machine is a closed/open/half-open circuit breaker state machine, generic
// over whatever dependency call it's gating.
type Machine struct {
	config Config
	now    func() time.Time

	// mutex guards every field below: Allow and Record can be called
	// concurrently by many caller goroutines, and state transitions
	// (especially the single-probe gate in half-open) must be decided
	// atomically.
	mutex               sync.Mutex
	state               State
	consecutiveFailures uint32
	openedAt            time.Time
}

// New builds a Machine according to config. It starts closed.
func New(config Config) (*Machine, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Machine{config: config, now: time.Now, state: Closed}, nil
}

// State reports the machine's current state.
func (machine *Machine) State() State {
	machine.mutex.Lock()
	defer machine.mutex.Unlock()
	return machine.state
}

// Allow decides whether the call in progress should reach the wrapped
// dependency, transitioning open to half-open once OpenTimeout has elapsed.
// The second return value reports whether this call is the half-open probe
// itself, as opposed to a normal closed-state call.
func (machine *Machine) Allow() (bool, bool) {
	machine.mutex.Lock()
	defer machine.mutex.Unlock()

	switch machine.state {
	case Closed:
		return true, false
	case HalfOpen:
		// Only one probe is allowed in flight at a time; concurrent callers
		// fail fast until the probe resolves.
		return false, false
	case Open:
		if machine.now().Sub(machine.openedAt) < machine.config.OpenTimeout {
			return false, false
		}
		machine.state = HalfOpen
		return true, true
	default:
		return false, false
	}
}

// ProbeContext bounds ctx by ProbeTimeout when isProbe is true and
// ProbeTimeout is configured. Callers must always invoke the returned
// cancel func.
func (machine *Machine) ProbeContext(ctx context.Context, isProbe bool) (context.Context, context.CancelFunc) {
	if isProbe && machine.config.ProbeTimeout > 0 {
		return context.WithTimeout(ctx, machine.config.ProbeTimeout)
	}
	return ctx, func() {}
}

// Record applies the outcome of a call that Allow let through to the
// machine's state. Pass nil for both a genuine success and a failure the
// caller has decided doesn't reflect the wrapped dependency's health.
func (machine *Machine) Record(err error) {
	machine.mutex.Lock()
	defer machine.mutex.Unlock()

	switch machine.state {
	case HalfOpen:
		if err != nil {
			machine.state = Open
			machine.openedAt = machine.now()
			machine.consecutiveFailures = 0
			return
		}
		machine.state = Closed
		machine.consecutiveFailures = 0
	case Closed:
		if err != nil {
			machine.consecutiveFailures++
			if machine.consecutiveFailures >= machine.config.FailureThreshold {
				machine.state = Open
				machine.openedAt = machine.now()
				machine.consecutiveFailures = 0
			}
			return
		}
		machine.consecutiveFailures = 0
	}
}

// SetNow overrides the machine's clock. Exposed so callers' tests can
// deterministically control OpenTimeout elapsing rather than sleeping in
// real time; not used outside tests.
func (machine *Machine) SetNow(now func() time.Time) {
	machine.mutex.Lock()
	defer machine.mutex.Unlock()
	machine.now = now
}

// ConsecutiveFailures reports the current consecutive-failure count.
// Exposed so callers' tests can assert it resets across state transitions;
// not used outside tests.
func (machine *Machine) ConsecutiveFailures() uint32 {
	machine.mutex.Lock()
	defer machine.mutex.Unlock()
	return machine.consecutiveFailures
}

// ForceOpen seeds the machine directly into the open state as of openedAt.
// Exposed so callers' tests can exercise open/half-open behavior without
// first driving the machine through FailureThreshold real failures; not
// used outside tests.
func (machine *Machine) ForceOpen(openedAt time.Time) {
	machine.mutex.Lock()
	defer machine.mutex.Unlock()
	machine.state = Open
	machine.openedAt = openedAt
}
