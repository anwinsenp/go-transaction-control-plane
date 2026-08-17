package controller

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
)

// resultSuccess and resultError are the two known values of the "result"
// label reported on the reconcile duration histogram — a fixed, bounded
// set, not a high-cardinality identifier such as a tenant ID.
const (
	resultSuccess = "success"
	resultError   = "error"
)

// reconcileDurationBucketsSeconds are histogram bucket boundaries tuned for
// the reconcile loop's external calls (Kubernetes API, Prometheus query
// client), which run from a few milliseconds up to the default 5s
// ReconcileTimeout and beyond on a slow pass.
var reconcileDurationBucketsSeconds = []float64{ //nolint:gochecknoglobals // read-only lookup table, never mutated after package init
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10,
}

// isolationTransitionReasons are the fixed, known values of the
// tradingtenant_isolation_transitions_total counter's "transition" label: the
// four TradingTenantState values, DedicatedPoolCreated (the isolation spec
// update that isn't itself a status.state value), and
// DedicatedPoolProvisioned/DedicatedPoolTornDown (the dedicated
// Deployment/Service/ConfigMap actually being created or deleted, see
// #55). All seven are pre-cached via WithLabelValues in NewMetrics, so
// this is the single source of truth for what's low-cardinality here (see
// #58 and CLAUDE.md's no-high-cardinality-labels rule — no raw tenant ID
// is ever attached).
var isolationTransitionReasons = []string{ //nolint:gochecknoglobals // read-only lookup table, never mutated after package init
	string(tradingv1alpha1.TradingTenantStateStable),
	string(tradingv1alpha1.TradingTenantStateScaling),
	string(tradingv1alpha1.TradingTenantStateIsolated),
	string(tradingv1alpha1.TradingTenantStateDegraded),
	reasonDedicatedPoolCreated,
	reasonDedicatedPoolProvisioned,
	reasonDedicatedPoolTornDown,
}

// Metrics holds the Prometheus instruments reported for the TradingTenant
// reconcile loop. Each Observer/Counter is obtained once at construction
// time via WithLabelValues and cached, so recording a reconcile pass never
// does a fresh label lookup.
type Metrics struct {
	durationObservers           map[string]prometheus.Observer // [result]
	isolationTransitionCounters map[string]prometheus.Counter  // [transition]
}

// NewMetrics registers the operator's reconcile-loop-duration histogram and
// isolation-transitions counter on reg and returns a Metrics ready to be
// assigned to TradingTenantReconciler.Metrics. Pass
// sigs.k8s.io/controller-runtime's metrics.Registry as reg so the manager's
// built-in /metrics endpoint serves it.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	reconcileDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tradingtenant_reconcile_duration_seconds",
		Help:    "Duration of TradingTenant reconcile loop passes, by result.",
		Buckets: reconcileDurationBucketsSeconds,
	}, []string{"result"})
	if err := reg.Register(reconcileDuration); err != nil {
		return nil, fmt.Errorf("new operator metrics: register reconcile duration histogram: %w", err)
	}

	isolationTransitions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tradingtenant_isolation_transitions_total",
		Help: "Count of TradingTenant reconcile state transitions, by resulting state.",
	}, []string{"transition"})
	if err := reg.Register(isolationTransitions); err != nil {
		return nil, fmt.Errorf("new operator metrics: register isolation transitions counter: %w", err)
	}

	isolationTransitionCounters := make(map[string]prometheus.Counter, len(isolationTransitionReasons))
	for _, reason := range isolationTransitionReasons {
		isolationTransitionCounters[reason] = isolationTransitions.WithLabelValues(reason)
	}

	return &Metrics{
		durationObservers: map[string]prometheus.Observer{
			resultSuccess: reconcileDuration.WithLabelValues(resultSuccess),
			resultError:   reconcileDuration.WithLabelValues(resultError),
		},
		isolationTransitionCounters: isolationTransitionCounters,
	}, nil
}

// observeDuration records elapsedSeconds against result's pre-cached
// Observer. result values outside the known set (resultSuccess,
// resultError) are silently dropped rather than causing a nil-Observer
// panic — today's only caller never passes an unknown value, but this
// keeps the method safe if it's ever reused with one.
func (reportedMetrics *Metrics) observeDuration(result string, elapsedSeconds float64) {
	observer, ok := reportedMetrics.durationObservers[result]
	if !ok {
		return
	}
	observer.Observe(elapsedSeconds)
}

// observeIsolationTransition increments the pre-cached counter for reason.
// reason values outside isolationTransitionReasons are silently dropped
// rather than causing a nil-Counter panic, mirroring observeDuration.
func (reportedMetrics *Metrics) observeIsolationTransition(reason string) {
	counter, ok := reportedMetrics.isolationTransitionCounters[reason]
	if !ok {
		return
	}
	counter.Inc()
}
