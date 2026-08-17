package kafka

import (
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// scrapeMetrics renders registry's current state in Prometheus exposition
// format, mirroring internal/ingestion's metrics_test.go so assertions here
// read actual counter values rather than reaching into unexported fields.
func scrapeMetrics(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()

	handler := metrics.Handler(registry)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	handler.ServeHTTP(recorder, request)

	body, err := io.ReadAll(recorder.Body)
	if err != nil {
		t.Fatalf("reading scraped metrics body: %v", err)
	}
	return string(body)
}

func requireMetricsLine(t *testing.T, scraped, line string) {
	t.Helper()
	if !strings.Contains(scraped, line) {
		t.Errorf("scraped metrics missing line %q; got:\n%s", line, scraped)
	}
}

func TestNewMetricsRegistersSuccessfully(t *testing.T) {
	registry := prometheus.NewRegistry()

	kafkaMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	if kafkaMetrics == nil {
		t.Fatal("NewMetrics() returned nil Metrics with no error")
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, "ingestion_kafka_tenant_reservation_dropped_total 0")
}

func TestNewMetricsDuplicateRegistrationReturnsError(t *testing.T) {
	registry := prometheus.NewRegistry()

	if _, firstErr := NewMetrics(registry, nil); firstErr != nil {
		t.Fatalf("first NewMetrics() unexpected error: %v", firstErr)
	}

	secondMetrics, secondErr := NewMetrics(registry, nil)
	if secondErr == nil {
		t.Fatal("second NewMetrics() on the same registerer = nil error, want a registration conflict error")
	}
	if secondMetrics != nil {
		t.Fatal("second NewMetrics() returned non-nil Metrics alongside an error")
	}
}

// TestTenantTopicPartitionerPartitionReportsDroppedTenants guards against a
// regression where a dropped explicit reservation (see
// TestNewReservationTableRecordsDroppedTenants) never surfaced anywhere
// observable: Partition's first call, which builds the reservation table,
// must report every dropped tenant on tenantReservationDroppedTotal.
func TestTenantTopicPartitionerPartitionReportsDroppedTenants(t *testing.T) {
	registry := prometheus.NewRegistry()
	kafkaMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	// tenant-a(4) + tenant-b(3) fill explicitCapacity (7 = totalPartitions-1
	// for totalPartitions=8); tenant-c(3) can't fit and is dropped.
	reserved := TenantPartitionConfig{"tenant-a": 4, "tenant-b": 3, "tenant-c": 3}
	partitioner := newTenantPartitioner(reserved, 1, kafkaMetrics)
	topicPartitioner := partitioner.ForTopic("transaction-events")

	record := &kgo.Record{Key: []byte("tenant-a:AAPL")}
	topicPartitioner.Partition(record, 8)

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, "ingestion_kafka_tenant_reservation_dropped_total 1")

	// A second Partition call against the same (unchanged) partition count
	// must not rebuild the table or double-count the drop.
	topicPartitioner.Partition(record, 8)
	scraped = scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, "ingestion_kafka_tenant_reservation_dropped_total 1")
}

// TestMetricsObservePartitionRange confirms observePartitionRange resolves
// tenantID through knownTenants before labeling
// ingestion_kafka_tenant_partition_count and ingestion_kafka_tenant_partition_start,
// the same resolution contract processor/kafka's observeLag already follows.
func TestMetricsObservePartitionRange(t *testing.T) {
	testCases := []struct {
		name         string
		knownTenants metrics.KnownTenants
		tenantID     string
		start        int32
		count        int32
		wantLabel    string
		wantStart    int32
		wantCount    int32
	}{
		{
			name:         "known tenant is labeled verbatim",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			tenantID:     "tenant-1",
			start:        8,
			count:        4,
			wantLabel:    "tenant-1",
			wantStart:    8,
			wantCount:    4,
		},
		{
			name:         "unknown tenant falls back to UnknownTenantLabel",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			tenantID:     "tenant-unregistered",
			start:        0,
			count:        2,
			wantLabel:    metrics.UnknownTenantLabel,
			wantStart:    0,
			wantCount:    2,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			kafkaMetrics, err := NewMetrics(registry, testCase.knownTenants)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			kafkaMetrics.observePartitionRange(testCase.tenantID, testCase.start, testCase.count)

			scraped := scrapeMetrics(t, registry)
			requireMetricsLine(t, scraped, fmt.Sprintf(`ingestion_kafka_tenant_partition_count{tenant="%s"} %d`, testCase.wantLabel, testCase.wantCount))
			requireMetricsLine(t, scraped, fmt.Sprintf(`ingestion_kafka_tenant_partition_start_count{tenant="%s"} %d`, testCase.wantLabel, testCase.wantStart))
		})
	}
}

func TestTenantTopicPartitionerPartitionNilMetricsIsSafe(t *testing.T) {
	partitioner := newTenantPartitioner(TenantPartitionConfig{"tenant-a": 4, "tenant-b": 3, "tenant-c": 3}, 1, nil)
	topicPartitioner := partitioner.ForTopic("transaction-events")

	record := &kgo.Record{Key: []byte("tenant-a:AAPL")}
	topicPartitioner.Partition(record, 8)
}
