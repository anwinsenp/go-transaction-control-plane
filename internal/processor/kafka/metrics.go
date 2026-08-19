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
	consumerLagMessages        *prometheus.GaugeVec
	activeConsumerCount        *prometheus.GaugeVec
	partitionReloadErrorsTotal prometheus.Counter
	knownTenants               metrics.KnownTenants
}

// NewMetrics registers this package's metrics on reg and returns a Metrics
// ready to be passed to NewConsumer. knownTenants bounds which tenant IDs
// are used verbatim as the lag and active-consumer-count gauges' "tenant"
// label value (see metrics.KnownTenants); every other tenant ID reports as
// metrics.UnknownTenantLabel instead.
func NewMetrics(reg prometheus.Registerer, knownTenants metrics.KnownTenants) (*Metrics, error) {
	consumerLag, err := metrics.NewGauge(reg, prometheus.GaugeOpts{
		Name: "processor_kafka_consumer_lag_messages",
		Help: "Difference between a partition's high watermark and the processor's last consumed offset on that partition, by tenant. Under ADR 0007's pool-exhaustion fallback a partition can carry more than one tenant; in that case this value is attributed only to the last tenant seen in a given poll batch, not to every tenant sharing the partition.",
	}, "tenant")
	if err != nil {
		return nil, fmt.Errorf("new processor kafka metrics: %w", err)
	}

	activeConsumerCount, err := metrics.NewGauge(reg, prometheus.GaugeOpts{
		Name: "processor_kafka_active_consumer_count",
		Help: "Number of this processor instance's assigned partitions that yielded at least one record for a tenant in the most recent poll batch, by tenant — a proxy for that tenant's active consumer parallelism on this instance. Since consumer group partitions are split across replicas, sum this across instances (e.g. `sum by (tenant)`) for a tenant's cluster-wide active consumer count.",
	}, "tenant")
	if err != nil {
		return nil, fmt.Errorf("new processor kafka metrics: %w", err)
	}

	partitionReloadErrors, err := metrics.NewCounter(reg, prometheus.CounterOpts{
		Name: "processor_kafka_tenant_partition_reload_errors_total",
		Help: "Total failed attempts to reload this dedicated processor's manually assigned partitions from TenantPartitionSource (ADR 0007, part 3) — e.g. a missing/unreadable ConfigMap file, malformed JSON, or a mapping missing this tenant's entry. Distinct from the ingestion side's tenant_reservation_dropped_total, which counts a different failure mode: a reservation that couldn't be honored because the topic has too few partitions. A failed reload keeps the last-known-good assignment rather than clearing it.",
	})
	if err != nil {
		return nil, fmt.Errorf("new processor kafka metrics: %w", err)
	}

	return &Metrics{
		consumerLagMessages:        consumerLag,
		activeConsumerCount:        activeConsumerCount,
		partitionReloadErrorsTotal: partitionReloadErrors.WithLabelValues(),
		knownTenants:               knownTenants,
	}, nil
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

// observeActiveConsumers reports one poll batch's per-tenant active-partition
// counts, keyed by raw tenant ID in counts. Multiple raw tenant IDs that
// resolve to the same label (most commonly metrics.UnknownTenantLabel, when
// several tenants outside knownTenants were active in the same batch) are
// summed before Set, so they don't overwrite each other on that shared
// label.
func (reportedMetrics *Metrics) observeActiveConsumers(counts map[string]int32) {
	totals := make(map[string]int32, len(counts))
	for tenantID, count := range counts {
		label := reportedMetrics.knownTenants.TenantLabel(tenantID)
		totals[label] += count
	}
	for label, total := range totals {
		reportedMetrics.activeConsumerCount.WithLabelValues(label).Set(float64(total))
	}
}
