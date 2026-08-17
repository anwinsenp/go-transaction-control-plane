package processor

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// breakerStateClosed, breakerStateHalfOpen, and breakerStateOpen are the
// values reported on the processor_postgres_circuit_breaker_state gauge for
// each known BreakerState. breakerStateUnknown is reported for any
// BreakerState not recognized by breakerStateValue, so an unrecognized
// state never silently reads as "closed" (healthy) on the gauge.
const (
	breakerStateClosed   = 0
	breakerStateHalfOpen = 1
	breakerStateOpen     = 2
	breakerStateUnknown  = 3
)

// repositoryTransactions and repositoryReconciledState are the two known
// values of the processor_postgres_circuit_breaker_state gauge's
// "repository" label — a fixed, bounded set, not a high-cardinality
// identifier.
const (
	repositoryTransactions    = "transactions"
	repositoryReconciledState = "reconciled_state"
)

// outcomeSuccess and outcomeFailure are the two known values of the
// "outcome" label used by this package's metrics.
const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
)

// Metrics holds the Prometheus instruments reported for the processor's
// reconciliation path. Every Counter/Observer/Gauge is obtained once at
// construction time via WithLabelValues and cached, so recording a
// reconciliation never does a fresh label lookup.
type Metrics struct {
	reconciledTotal   map[string]prometheus.Counter             // [outcome]
	durationObservers map[string]map[string]prometheus.Observer // [tenant][outcome]
	breakerState      map[string]prometheus.Gauge               // [repository]
	knownTenants      metrics.KnownTenants
}

// NewMetrics registers the processor's reconciliation-path metrics on reg
// and returns a Metrics ready to be passed to NewInstrumentedReconciler.
// knownTenants bounds which tenant IDs are used verbatim as the
// "processor_transaction_duration_seconds" histogram's "tenant" label value
// (see metrics.KnownTenants); every other tenant ID reports as
// metrics.UnknownTenantLabel instead, keeping the label's cardinality fixed
// at construction time.
func NewMetrics(reg prometheus.Registerer, knownTenants metrics.KnownTenants) (*Metrics, error) {
	reconciledTotal, err := metrics.NewCounter(reg, prometheus.CounterOpts{
		Name: "processor_transactions_processed_total",
		Help: "Total transaction events reconciled by the processor, by outcome.",
	}, "outcome")
	if err != nil {
		return nil, fmt.Errorf("new processor metrics: %w", err)
	}

	reconcileDuration, err := metrics.NewHistogram(reg, prometheus.HistogramOpts{
		Name:    "processor_transaction_duration_seconds",
		Help:    "Latency of reconciling one transaction event, by outcome and tenant.",
		Buckets: metrics.LatencyBucketsSeconds,
	}, "outcome", "tenant")
	if err != nil {
		return nil, fmt.Errorf("new processor metrics: %w", err)
	}

	breakerStateGauge, err := metrics.NewGauge(reg, prometheus.GaugeOpts{
		Name: "processor_postgres_circuit_breaker_state",
		Help: "Current state of a processor-to-Postgres repository circuit breaker: 0=closed, 1=half_open, 2=open, 3=unknown, by repository.",
	}, "repository")
	if err != nil {
		return nil, fmt.Errorf("new processor metrics: %w", err)
	}

	tenantLabels := append([]string{metrics.UnknownTenantLabel}, sortedTenantIDs(knownTenants)...)
	durationObservers := make(map[string]map[string]prometheus.Observer, len(tenantLabels))
	for _, tenantLabel := range tenantLabels {
		durationObservers[tenantLabel] = map[string]prometheus.Observer{
			outcomeSuccess: reconcileDuration.WithLabelValues(outcomeSuccess, tenantLabel),
			outcomeFailure: reconcileDuration.WithLabelValues(outcomeFailure, tenantLabel),
		}
	}

	return &Metrics{
		reconciledTotal: map[string]prometheus.Counter{
			outcomeSuccess: reconciledTotal.WithLabelValues(outcomeSuccess),
			outcomeFailure: reconciledTotal.WithLabelValues(outcomeFailure),
		},
		durationObservers: durationObservers,
		breakerState: map[string]prometheus.Gauge{
			repositoryTransactions:    breakerStateGauge.WithLabelValues(repositoryTransactions),
			repositoryReconciledState: breakerStateGauge.WithLabelValues(repositoryReconciledState),
		},
		knownTenants: knownTenants,
	}, nil
}

// sortedTenantIDs returns known's tenant IDs, so NewMetrics builds its
// pre-cached observer map deterministically rather than depending on Go's
// randomized map iteration order.
func sortedTenantIDs(known metrics.KnownTenants) []string {
	tenantIDs := make([]string, 0, len(known))
	for tenantID := range known {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	return tenantIDs
}

// durationObserver returns the pre-cached Observer for outcome and
// tenantID's resolved label. tenantID always resolves to either a known
// tenant (present in the map built by NewMetrics) or
// metrics.UnknownTenantLabel, so this never falls back to an uncached
// WithLabelValues call.
func (reportedMetrics *Metrics) durationObserver(outcome, tenantID string) prometheus.Observer {
	tenantLabel := reportedMetrics.knownTenants.TenantLabel(tenantID)
	return reportedMetrics.durationObservers[tenantLabel][outcome]
}

// BreakerStater reports a circuit breaker's current state. InstrumentedReconciler
// depends on this narrow interface rather than a concrete breaker type, so
// it can report breaker state without depending on which repository the
// breaker wraps.
type BreakerStater interface {
	State() ledger.BreakerState
}

// reconciler is the subset of *Reconciler that InstrumentedReconciler
// wraps, declared locally so this package doesn't tie the wrapper to one
// concrete implementation.
type reconciler interface {
	Reconcile(ctx context.Context, txn ledger.Transaction) error
}

// InstrumentedReconciler wraps a Reconciler and records throughput,
// reconciliation latency (by outcome and tenant), and (for any non-nil
// breaker) circuit breaker state to a Metrics. It implements the same
// Reconcile(ctx, txn) error signature the Kafka consumer depends on, so it
// drops in wherever a *Reconciler is expected.
type InstrumentedReconciler struct {
	next                reconciler
	metrics             *Metrics
	transactionsBreaker BreakerStater
	statesBreaker       BreakerStater
}

// NewInstrumentedReconciler builds an InstrumentedReconciler that wraps
// next, recording every Reconcile call to reportedMetrics. transactionsBreaker
// and statesBreaker are optional: pass nil for either to skip reporting its
// state.
func NewInstrumentedReconciler(next reconciler, reportedMetrics *Metrics, transactionsBreaker, statesBreaker BreakerStater) (*InstrumentedReconciler, error) {
	if next == nil {
		return nil, fmt.Errorf("new instrumented reconciler: next reconciler must not be nil")
	}
	if reportedMetrics == nil {
		return nil, fmt.Errorf("new instrumented reconciler: metrics must not be nil")
	}
	return &InstrumentedReconciler{
		next:                next,
		metrics:             reportedMetrics,
		transactionsBreaker: transactionsBreaker,
		statesBreaker:       statesBreaker,
	}, nil
}

// Reconcile forwards txn to the wrapped Reconciler, then records its
// outcome, latency, and refreshes the circuit breaker state gauges for any
// breaker that was configured.
func (instrumented *InstrumentedReconciler) Reconcile(ctx context.Context, txn ledger.Transaction) error {
	start := time.Now()
	err := instrumented.next.Reconcile(ctx, txn)
	elapsed := time.Since(start).Seconds()

	outcome := outcomeSuccess
	if err != nil {
		outcome = outcomeFailure
	}
	instrumented.metrics.reconciledTotal[outcome].Inc()
	instrumented.metrics.durationObserver(outcome, txn.TenantID).Observe(elapsed)

	if instrumented.transactionsBreaker != nil {
		instrumented.metrics.breakerState[repositoryTransactions].Set(breakerStateValue(instrumented.transactionsBreaker.State()))
	}
	if instrumented.statesBreaker != nil {
		instrumented.metrics.breakerState[repositoryReconciledState].Set(breakerStateValue(instrumented.statesBreaker.State()))
	}

	return err
}

// breakerStateValue maps a BreakerState to the numeric value reported on a
// circuit breaker state gauge. It reports breakerStateUnknown for any state
// outside the three corebreaker currently defines, rather than aliasing an
// unrecognized state to breakerStateClosed.
func breakerStateValue(state ledger.BreakerState) float64 {
	switch state {
	case ledger.BreakerClosed:
		return breakerStateClosed
	case ledger.BreakerOpen:
		return breakerStateOpen
	case ledger.BreakerHalfOpen:
		return breakerStateHalfOpen
	default:
		return breakerStateUnknown
	}
}
