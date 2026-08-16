package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// breakerStateClosed, breakerStateHalfOpen, and breakerStateOpen are the
// values reported on the ingestion_kafka_circuit_breaker_state gauge for
// each known BreakerState. breakerStateUnknown is reported for any
// BreakerState not recognized by breakerStateValue, so an unrecognized
// state never silently reads as "closed" (healthy) on the gauge.
const (
	breakerStateClosed   = 0
	breakerStateHalfOpen = 1
	breakerStateOpen     = 2
	breakerStateUnknown  = 3
)

// Metrics holds the Prometheus instruments reported for the ingestion
// service's publish hot path. Every Observer/Counter is obtained once at
// construction time via WithLabelValues and cached, so recording a
// transaction on the hot path never allocates a fresh label lookup.
type Metrics struct {
	transactionsSuccessTotal prometheus.Counter
	transactionsFailureTotal prometheus.Counter
	publishLatencySuccess    prometheus.Observer
	publishLatencyFailure    prometheus.Observer
	breakerState             prometheus.Gauge
}

// NewMetrics registers the ingestion service's publish-path metrics on reg
// and returns a Metrics ready to be passed to NewInstrumentedPublisher.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	transactionsTotal, err := metrics.NewCounter(reg, prometheus.CounterOpts{
		Name: "ingestion_transactions_processed_total",
		Help: "Total transaction events accepted by the ingestion publish path, by outcome.",
	}, "outcome")
	if err != nil {
		return nil, fmt.Errorf("new ingestion metrics: %w", err)
	}

	publishLatency, err := metrics.NewHistogram(reg, prometheus.HistogramOpts{
		Name:    "ingestion_publish_latency_seconds",
		Help:    "Latency of a transaction event's publish call, by outcome.",
		Buckets: metrics.LatencyBucketsSeconds,
	}, "outcome")
	if err != nil {
		return nil, fmt.Errorf("new ingestion metrics: %w", err)
	}

	breakerState, err := metrics.NewGauge(reg, prometheus.GaugeOpts{
		Name: "ingestion_kafka_circuit_breaker_state",
		Help: "Current state of the ingestion-to-Kafka publish circuit breaker: 0=closed, 1=half_open, 2=open, 3=unknown.",
	})
	if err != nil {
		return nil, fmt.Errorf("new ingestion metrics: %w", err)
	}

	return &Metrics{
		transactionsSuccessTotal: transactionsTotal.WithLabelValues("success"),
		transactionsFailureTotal: transactionsTotal.WithLabelValues("failure"),
		publishLatencySuccess:    publishLatency.WithLabelValues("success"),
		publishLatencyFailure:    publishLatency.WithLabelValues("failure"),
		breakerState:             breakerState.WithLabelValues(),
	}, nil
}

// BreakerStater reports a circuit breaker's current state. InstrumentedPublisher
// depends on this narrow interface rather than *CircuitBreaker directly, so
// it can report breaker state without importing the breaker's concrete
// type.
type BreakerStater interface {
	State() BreakerState
}

// InstrumentedPublisher wraps a Publisher and records throughput, publish
// latency, and (when breaker is non-nil) circuit breaker state to a
// Metrics. It implements Publisher itself, so it drops in wherever a
// Publisher is expected.
type InstrumentedPublisher struct {
	next    Publisher
	metrics *Metrics
	breaker BreakerStater
}

var _ Publisher = (*InstrumentedPublisher)(nil)

// NewInstrumentedPublisher builds an InstrumentedPublisher that wraps next,
// recording every Publish call to reportedMetrics. breaker is optional: pass
// nil if next's chain has no circuit breaker to report state for.
func NewInstrumentedPublisher(next Publisher, reportedMetrics *Metrics, breaker BreakerStater) (*InstrumentedPublisher, error) {
	if next == nil {
		return nil, fmt.Errorf("new instrumented publisher: next Publisher must not be nil")
	}
	if reportedMetrics == nil {
		return nil, fmt.Errorf("new instrumented publisher: metrics must not be nil")
	}
	return &InstrumentedPublisher{next: next, metrics: reportedMetrics, breaker: breaker}, nil
}

// Publish forwards event to the wrapped Publisher, then records its outcome
// and latency, and refreshes the circuit breaker state gauge if a breaker
// was configured.
func (instrumented *InstrumentedPublisher) Publish(ctx context.Context, event Event) error {
	start := time.Now()
	err := instrumented.next.Publish(ctx, event)
	elapsed := time.Since(start).Seconds()

	if err != nil {
		instrumented.metrics.transactionsFailureTotal.Inc()
		instrumented.metrics.publishLatencyFailure.Observe(elapsed)
	} else {
		instrumented.metrics.transactionsSuccessTotal.Inc()
		instrumented.metrics.publishLatencySuccess.Observe(elapsed)
	}

	if instrumented.breaker != nil {
		instrumented.metrics.breakerState.Set(breakerStateValue(instrumented.breaker.State()))
	}

	return err
}

// breakerStateValue maps a BreakerState to the numeric value reported on
// the circuit breaker state gauge. It reports breakerStateUnknown for any
// state outside the three corebreaker currently defines, rather than
// aliasing an unrecognized state to breakerStateClosed.
func breakerStateValue(state BreakerState) float64 {
	switch state {
	case BreakerClosed:
		return breakerStateClosed
	case BreakerOpen:
		return breakerStateOpen
	case BreakerHalfOpen:
		return breakerStateHalfOpen
	default:
		return breakerStateUnknown
	}
}
