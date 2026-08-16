// Package metrics provides shared constructors for Prometheus counters,
// gauges, and histograms. It enforces the project's naming convention
// (snake_case, unit-suffixed) and rejects known high-cardinality label
// names at registration time, so every service that reports metrics does
// so consistently.
package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ErrInvalidMetricName is returned when a metric name is not snake_case or
// does not carry the unit suffix required for its metric type.
var ErrInvalidMetricName = errors.New("metrics: invalid name")

// ErrHighCardinalityLabel is returned when a label name matches a known
// unbounded identifier (e.g. a raw transaction ID) that would blow up
// Prometheus's series cardinality.
var ErrHighCardinalityLabel = errors.New("metrics: disallowed high-cardinality label")

var nameFormat = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

const counterSuffix = "_total"

// gaugeHistogramSuffixes are the unit suffixes accepted for gauges and
// histograms. Counters use counterSuffix instead. This list covers the
// units used elsewhere in this project (latency, payload size, consumer
// lag, enum state) rather than every possible Prometheus unit. It's a
// read-only lookup table, never mutated after package init, so it's not
// the mutable global state CLAUDE.md's no-globals rule targets — Go
// simply has no const slice literal to declare it as instead.
//
//nolint:gochecknoglobals // read-only lookup table, see comment above
var gaugeHistogramSuffixes = []string{"_seconds", "_bytes", "_ratio", "_messages", "_percent", "_state"}

// deniedLabels are label names that identify a single entity rather than a
// bounded category, and so would make a metric's series count grow
// unboundedly with traffic. tenant_id is deliberately not included: tenant
// count is bounded by the number of provisioned TradingTenant resources,
// not by transaction volume.
//
// This is a denylist of known offenders, not an exhaustive check: spelling
// variants (txnid, customer_id) or new unbounded identifiers aren't caught.
// Engineers adding a label are still responsible for keeping it bounded.
// It's a read-only lookup table, never mutated after package init, so it's
// not the mutable global state CLAUDE.md's no-globals rule targets — Go
// simply has no const map literal to declare it as instead.
//
//nolint:gochecknoglobals // read-only lookup table, see comment above
var deniedLabels = map[string]struct{}{
	"transaction_id": {},
	"txn_id":         {},
	"trace_id":       {},
	"span_id":        {},
	"request_id":     {},
	"session_id":     {},
	"correlation_id": {},
	"order_id":       {},
	"user_id":        {},
	"account_id":     {},
}

// LatencyBucketsSeconds are histogram bucket boundaries tuned for hot-path
// latencies in the microsecond-to-low-millisecond range, expressed in
// seconds per Prometheus convention. It's a read-only lookup table, never
// mutated after package init, so it's not the mutable global state
// CLAUDE.md's no-globals rule targets — callers need a stable value to
// reference from prometheus.HistogramOpts.Buckets, and Go has no const
// slice literal to declare it as instead.
//
//nolint:gochecknoglobals // read-only lookup table, see comment above
var LatencyBucketsSeconds = []float64{
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
}

// NewCounter validates opts.Name and labelNames, registers a CounterVec on
// reg, and returns it.
func NewCounter(reg prometheus.Registerer, opts prometheus.CounterOpts, labelNames ...string) (*prometheus.CounterVec, error) {
	if err := validateName(opts.Name, counterSuffix); err != nil {
		return nil, err
	}
	if err := validateLabels(labelNames); err != nil {
		return nil, err
	}

	counter := prometheus.NewCounterVec(opts, labelNames)
	if err := reg.Register(counter); err != nil {
		return nil, fmt.Errorf("register counter %q: %w", opts.Name, err)
	}
	return counter, nil
}

// NewGauge validates opts.Name and labelNames, registers a GaugeVec on
// reg, and returns it.
func NewGauge(reg prometheus.Registerer, opts prometheus.GaugeOpts, labelNames ...string) (*prometheus.GaugeVec, error) {
	if err := validateName(opts.Name, gaugeHistogramSuffixes...); err != nil {
		return nil, err
	}
	if err := validateLabels(labelNames); err != nil {
		return nil, err
	}

	gauge := prometheus.NewGaugeVec(opts, labelNames)
	if err := reg.Register(gauge); err != nil {
		return nil, fmt.Errorf("register gauge %q: %w", opts.Name, err)
	}
	return gauge, nil
}

// NewHistogram validates opts.Name and labelNames, registers a
// HistogramVec on reg, and returns it. Callers on the transaction hot path
// should set opts.Buckets to LatencyBucketsSeconds unless they have a
// benchmark-backed reason to use different boundaries, and must call
// WithLabelValues once at startup to obtain and cache the Observer rather
// than calling it per-transaction.
func NewHistogram(reg prometheus.Registerer, opts prometheus.HistogramOpts, labelNames ...string) (*prometheus.HistogramVec, error) {
	if err := validateName(opts.Name, gaugeHistogramSuffixes...); err != nil {
		return nil, err
	}
	if err := validateLabels(labelNames); err != nil {
		return nil, err
	}

	histogram := prometheus.NewHistogramVec(opts, labelNames)
	if err := reg.Register(histogram); err != nil {
		return nil, fmt.Errorf("register histogram %q: %w", opts.Name, err)
	}
	return histogram, nil
}

// Handler returns the HTTP handler that serves gatherer's metrics in the
// Prometheus exposition format, for mounting at a service's /metrics
// endpoint.
func Handler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}

// validateName checks that name is snake_case and ends with one of the
// given allowed unit suffixes.
func validateName(name string, allowedSuffixes ...string) error {
	if !nameFormat.MatchString(name) {
		return fmt.Errorf("%w: %q must be lowercase snake_case", ErrInvalidMetricName, name)
	}

	for _, suffix := range allowedSuffixes {
		if strings.HasSuffix(name, suffix) {
			return nil
		}
	}
	return fmt.Errorf("%w: %q must end with one of %v", ErrInvalidMetricName, name, allowedSuffixes)
}

// validateLabels rejects label names known to identify a single entity
// rather than a bounded category.
func validateLabels(labelNames []string) error {
	for _, label := range labelNames {
		if _, denied := deniedLabels[strings.ToLower(label)]; denied {
			return fmt.Errorf("%w: %q is a common unbounded identifier", ErrHighCardinalityLabel, label)
		}
	}
	return nil
}
