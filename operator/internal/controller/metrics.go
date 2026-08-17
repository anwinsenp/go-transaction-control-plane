package controller

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
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

// Metrics holds the Prometheus instrument reported for the TradingTenant
// reconcile loop. The Observer is obtained once at construction time via
// WithLabelValues and cached, so recording a reconcile pass never does a
// fresh label lookup.
type Metrics struct {
	durationObservers map[string]prometheus.Observer // [result]
}

// NewMetrics registers the operator's reconcile-loop-duration histogram on
// reg and returns a Metrics ready to be assigned to
// TradingTenantReconciler.Metrics. Pass sigs.k8s.io/controller-runtime's
// metrics.Registry as reg so the manager's built-in /metrics endpoint
// serves it.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	reconcileDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tradingtenant_reconcile_duration_seconds",
		Help:    "Duration of TradingTenant reconcile loop passes, by result.",
		Buckets: reconcileDurationBucketsSeconds,
	}, []string{"result"})
	if err := reg.Register(reconcileDuration); err != nil {
		return nil, fmt.Errorf("new operator metrics: register reconcile duration histogram: %w", err)
	}

	return &Metrics{
		durationObservers: map[string]prometheus.Observer{
			resultSuccess: reconcileDuration.WithLabelValues(resultSuccess),
			resultError:   reconcileDuration.WithLabelValues(resultError),
		},
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
