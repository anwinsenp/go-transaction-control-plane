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
	tenantReservationDroppedTotal prometheus.Counter
}

// NewMetrics registers this package's metrics on reg and returns a Metrics
// ready to be passed to NewPublisher.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	tenantReservationDropped, err := metrics.NewCounter(reg, prometheus.CounterOpts{
		Name: "ingestion_kafka_tenant_reservation_dropped_total",
		Help: "Total tenants configured in Config.TenantPartitions that could not be given an exclusive partition range because the topic did not have enough partitions to honor every reservation.",
	})
	if err != nil {
		return nil, fmt.Errorf("new kafka metrics: %w", err)
	}

	return &Metrics{tenantReservationDroppedTotal: tenantReservationDropped.WithLabelValues()}, nil
}
