package kafka

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// Metrics holds the Prometheus instruments reported by this package's Kafka
// consumer, separate from processor.Metrics (the reconciliation-path
// throughput/latency/breaker metrics) since this package sits below that
// domain layer and reports its own transport-level concerns.
type Metrics struct {
	consumerLagMessages *prometheus.GaugeVec
	knownTenants        metrics.KnownTenants
}

// NewMetrics registers this package's metrics on reg and returns a Metrics
// ready to be passed to NewConsumer. knownTenants bounds which tenant IDs
// are used verbatim as the lag gauge's "tenant" label value (see
// metrics.KnownTenants); every other tenant ID reports as
// metrics.UnknownTenantLabel instead.
func NewMetrics(reg prometheus.Registerer, knownTenants metrics.KnownTenants) (*Metrics, error) {
	consumerLag, err := metrics.NewGauge(reg, prometheus.GaugeOpts{
		Name: "processor_kafka_consumer_lag_messages",
		Help: "Difference between a partition's high watermark and the processor's last consumed offset on that partition, by tenant. Under ADR 0007's pool-exhaustion fallback a partition can carry more than one tenant; in that case this value is attributed only to the last tenant seen in a given poll batch, not to every tenant sharing the partition.",
	}, "tenant")
	if err != nil {
		return nil, fmt.Errorf("new processor kafka metrics: %w", err)
	}

	return &Metrics{consumerLagMessages: consumerLag, knownTenants: knownTenants}, nil
}

// observeLag reports lag (clamped to zero, since a partition briefly ahead
// of its last-observed high watermark during a leader change is not a
// negative lag) for tenantID's resolved label.
func (reportedMetrics *Metrics) observeLag(tenantID string, lag int64) {
	if lag < 0 {
		lag = 0
	}
	reportedMetrics.consumerLagMessages.WithLabelValues(reportedMetrics.knownTenants.TenantLabel(tenantID)).Set(float64(lag))
}
