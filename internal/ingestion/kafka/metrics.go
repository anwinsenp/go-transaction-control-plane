package kafka

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// Metrics holds the Prometheus instruments reported by this package's
// Kafka publisher, separate from ingestion.Metrics (the publish-path
// throughput/latency/breaker metrics) since this package sits below that
// domain layer and reports its own transport-level concerns.
type Metrics struct {
	tenantReservationDroppedTotal     prometheus.Counter
	tenantReservationReloadErrorTotal prometheus.Counter
	partitionCount                    *prometheus.GaugeVec
	partitionStart                    *prometheus.GaugeVec
	knownTenants                      metrics.KnownTenants
}

// NewMetrics registers this package's metrics on reg and returns a Metrics
// ready to be passed to NewPublisher. knownTenants bounds which tenant IDs
// are used verbatim as the partition-count gauge's "tenant" label value (see
// metrics.KnownTenants); every other tenant ID reports as
// metrics.UnknownTenantLabel instead.
func NewMetrics(reg prometheus.Registerer, knownTenants metrics.KnownTenants) (*Metrics, error) {
	tenantReservationDropped, err := metrics.NewCounter(reg, prometheus.CounterOpts{
		Name: "ingestion_kafka_tenant_reservation_dropped_total",
		Help: "Total tenants configured in Config.TenantPartitions that could not be given an exclusive partition range because the topic did not have enough partitions to honor every reservation.",
	})
	if err != nil {
		return nil, fmt.Errorf("new kafka metrics: %w", err)
	}

	tenantReservationReloadError, err := metrics.NewCounter(reg, prometheus.CounterOpts{
		Name: "ingestion_kafka_tenant_reservation_reload_errors_total",
		Help: "Total failed attempts to reload the tenant->partition reservation table from TenantPartitionSource (ADR 0007, part 3) — e.g. a missing/unreadable ConfigMap file or malformed JSON. Distinct from tenant_reservation_dropped_total, which counts a different failure mode: a successfully loaded reservation that couldn't be honored because the topic has too few partitions. A failed reload keeps the last-known-good reservations rather than clearing them.",
	})
	if err != nil {
		return nil, fmt.Errorf("new kafka metrics: %w", err)
	}

	partitionCount, err := metrics.NewGauge(reg, prometheus.GaugeOpts{
		Name: "ingestion_kafka_tenant_partition_count",
		Help: "Number of Kafka partitions reserved for a tenant in the tenant->partition reservation table (ADR 0007), by tenant. Read by the operator to populate TradingTenant status.observedPartitionCount.",
	}, "tenant")
	if err != nil {
		return nil, fmt.Errorf("new kafka metrics: %w", err)
	}

	partitionStart, err := metrics.NewGauge(reg, prometheus.GaugeOpts{
		Name: "ingestion_kafka_tenant_partition_start_count",
		Help: "Start index (inclusive) of the contiguous partition range reserved for a tenant in the tenant->partition reservation table (ADR 0007), by tenant. Read by the operator to configure the dedicated processor's manual partition assignment.",
	}, "tenant")
	if err != nil {
		return nil, fmt.Errorf("new kafka metrics: %w", err)
	}

	return &Metrics{
		tenantReservationDroppedTotal:     tenantReservationDropped.WithLabelValues(),
		tenantReservationReloadErrorTotal: tenantReservationReloadError.WithLabelValues(),
		partitionCount:                    partitionCount,
		partitionStart:                    partitionStart,
		knownTenants:                      knownTenants,
	}, nil
}

// observePartitionRange reports tenantID's resolved label's current
// partition-count and partition-start gauge values for its reserved range
// [start, start+count). Called only when a tenant's reservation range is
// first established (an explicit reservation at table-build time, or a pool
// assignment on first sight of that tenant) — never on the per-record
// Partition() hot path — so it never affects the publisher's zero-allocation
// steady state.
func (reportedMetrics *Metrics) observePartitionRange(tenantID string, start, count int32) {
	label := reportedMetrics.knownTenants.TenantLabel(tenantID)
	reportedMetrics.partitionCount.WithLabelValues(label).Set(float64(count))
	reportedMetrics.partitionStart.WithLabelValues(label).Set(float64(start))
}
