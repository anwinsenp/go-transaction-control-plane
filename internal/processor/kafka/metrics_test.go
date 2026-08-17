package kafka

import (
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// scrapeMetrics renders registry's current state in Prometheus exposition
// format, the same way internal/metrics's TestHandler inspects a
// registry's output, so assertions here read actual gauge values rather
// than reaching into unexported fields.
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

func requireMetricsLineAbsent(t *testing.T, scraped, substring string) {
	t.Helper()
	if strings.Contains(scraped, substring) {
		t.Errorf("scraped metrics unexpectedly contains %q; got:\n%s", substring, scraped)
	}
}

func TestNewMetrics_RegistersSuccessfully(t *testing.T) {
	registry := prometheus.NewRegistry()

	kafkaMetrics, err := NewMetrics(registry, metrics.NewKnownTenants("tenant-1"))
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	if kafkaMetrics == nil {
		t.Fatal("NewMetrics() returned nil Metrics with no error")
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLineAbsent(t, scraped, "processor_kafka_consumer_lag_messages{")
}

func TestNewMetrics_DuplicateRegistrationReturnsError(t *testing.T) {
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

func TestMetrics_ObserveLag(t *testing.T) {
	testCases := []struct {
		name         string
		knownTenants metrics.KnownTenants
		tenantID     string
		lag          int64
		wantLabel    string
		wantLag      int64
	}{
		{
			name:         "known tenant is labeled verbatim",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			tenantID:     "tenant-1",
			lag:          42,
			wantLabel:    "tenant-1",
			wantLag:      42,
		},
		{
			name:         "unknown tenant falls back to UnknownTenantLabel",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			tenantID:     "tenant-unregistered",
			lag:          7,
			wantLabel:    metrics.UnknownTenantLabel,
			wantLag:      7,
		},
		{
			name:         "negative lag is clamped to zero",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			tenantID:     "tenant-1",
			lag:          -5,
			wantLabel:    "tenant-1",
			wantLag:      0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			kafkaMetrics, err := NewMetrics(registry, testCase.knownTenants)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			kafkaMetrics.observeLag(testCase.tenantID, testCase.lag)

			scraped := scrapeMetrics(t, registry)
			requireMetricsLine(t, scraped, fmt.Sprintf(`processor_kafka_consumer_lag_messages{tenant="%s"} %d`, testCase.wantLabel, testCase.wantLag))
		})
	}
}

// TestMetrics_ObserveActiveConsumers covers observeActiveConsumers's
// resolve-then-sum contract: raw tenant IDs that resolve to the same label
// (most commonly metrics.UnknownTenantLabel, when several tenants outside
// knownTenants are active in the same poll batch) must be summed into one
// observation rather than the last one silently overwriting the others.
func TestMetrics_ObserveActiveConsumers(t *testing.T) {
	testCases := []struct {
		name         string
		knownTenants metrics.KnownTenants
		counts       map[string]int32
		wantLines    []string
	}{
		{
			name:         "single known tenant",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			counts:       map[string]int32{"tenant-1": 3},
			wantLines:    []string{`processor_kafka_active_consumer_count{tenant="tenant-1"} 3`},
		},
		{
			name:         "two unknown tenants sum into UnknownTenantLabel",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			counts:       map[string]int32{"tenant-x": 2, "tenant-y": 5},
			wantLines:    []string{fmt.Sprintf(`processor_kafka_active_consumer_count{tenant="%s"} 7`, metrics.UnknownTenantLabel)},
		},
		{
			name:         "one known and one unknown tenant report separately",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			counts:       map[string]int32{"tenant-1": 4, "tenant-x": 2},
			wantLines: []string{
				`processor_kafka_active_consumer_count{tenant="tenant-1"} 4`,
				fmt.Sprintf(`processor_kafka_active_consumer_count{tenant="%s"} 2`, metrics.UnknownTenantLabel),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			kafkaMetrics, err := NewMetrics(registry, testCase.knownTenants)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			kafkaMetrics.observeActiveConsumers(testCase.counts)

			scraped := scrapeMetrics(t, registry)
			for _, line := range testCase.wantLines {
				requireMetricsLine(t, scraped, line)
			}
		})
	}
}
