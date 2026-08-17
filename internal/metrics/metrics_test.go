package metrics

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewCounter(t *testing.T) {
	testCases := []struct {
		name       string
		metricName string
		labelNames []string
		wantErr    error
	}{
		{
			name:       "valid counter name and no labels",
			metricName: "transactions_processed_total",
		},
		{
			name:       "valid counter name with tenant_id label",
			metricName: "transactions_processed_total",
			labelNames: []string{"tenant_id"},
		},
		{
			name:       "uppercase name rejected",
			metricName: "Transactions_Processed_Total",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "missing total suffix rejected",
			metricName: "transactions_processed",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "empty name rejected",
			metricName: "",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "denied label rejected case-insensitively",
			metricName: "transactions_processed_total",
			labelNames: []string{"Transaction_ID"},
			wantErr:    ErrHighCardinalityLabel,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			opts := prometheus.CounterOpts{
				Name: testCase.metricName,
				Help: "test counter",
			}

			counterVec, constructErr := NewCounter(registry, opts, testCase.labelNames...)

			if testCase.wantErr != nil {
				if constructErr == nil {
					t.Fatalf("NewCounter(%q) = nil error, want error wrapping %v", testCase.metricName, testCase.wantErr)
				}
				if !errors.Is(constructErr, testCase.wantErr) {
					t.Fatalf("NewCounter(%q) error = %v, want it to wrap %v", testCase.metricName, constructErr, testCase.wantErr)
				}
				if counterVec != nil {
					t.Fatalf("NewCounter(%q) returned non-nil CounterVec alongside an error", testCase.metricName)
				}
				return
			}

			if constructErr != nil {
				t.Fatalf("NewCounter(%q) unexpected error: %v", testCase.metricName, constructErr)
			}
			if counterVec == nil {
				t.Fatalf("NewCounter(%q) returned nil CounterVec with no error", testCase.metricName)
			}
		})
	}
}

func TestNewGauge(t *testing.T) {
	testCases := []struct {
		name       string
		metricName string
		labelNames []string
		wantErr    error
	}{
		{name: "valid seconds suffix", metricName: "reconcile_loop_duration_seconds"},
		{name: "valid bytes suffix", metricName: "payload_size_bytes"},
		{name: "valid ratio suffix", metricName: "cache_hit_ratio"},
		{name: "valid messages suffix", metricName: "kafka_consumer_lag_messages"},
		{name: "valid percent suffix", metricName: "cpu_usage_percent"},
		{name: "valid state suffix", metricName: "circuit_breaker_state"},
		{name: "valid count suffix", metricName: "tenant_partition_count"},
		{
			name:       "valid gauge with tenant_id label",
			metricName: "kafka_consumer_lag_messages",
			labelNames: []string{"tenant_id"},
		},
		{
			name:       "unsupported suffix rejected",
			metricName: "queue_depth_total",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "uppercase name rejected",
			metricName: "Queue_Depth_Bytes",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "consecutive underscores rejected",
			metricName: "kafka__lag_messages",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "empty name rejected",
			metricName: "",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "denied label rejected case-insensitively",
			metricName: "kafka_consumer_lag_messages",
			labelNames: []string{"USER_ID"},
			wantErr:    ErrHighCardinalityLabel,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			opts := prometheus.GaugeOpts{
				Name: testCase.metricName,
				Help: "test gauge",
			}

			gaugeVec, constructErr := NewGauge(registry, opts, testCase.labelNames...)

			if testCase.wantErr != nil {
				if constructErr == nil {
					t.Fatalf("NewGauge(%q) = nil error, want error wrapping %v", testCase.metricName, testCase.wantErr)
				}
				if !errors.Is(constructErr, testCase.wantErr) {
					t.Fatalf("NewGauge(%q) error = %v, want it to wrap %v", testCase.metricName, constructErr, testCase.wantErr)
				}
				if gaugeVec != nil {
					t.Fatalf("NewGauge(%q) returned non-nil GaugeVec alongside an error", testCase.metricName)
				}
				return
			}

			if constructErr != nil {
				t.Fatalf("NewGauge(%q) unexpected error: %v", testCase.metricName, constructErr)
			}
			if gaugeVec == nil {
				t.Fatalf("NewGauge(%q) returned nil GaugeVec with no error", testCase.metricName)
			}
		})
	}
}

func TestNewHistogram(t *testing.T) {
	testCases := []struct {
		name       string
		metricName string
		labelNames []string
		wantErr    error
	}{
		{name: "valid seconds suffix with latency buckets", metricName: "ingestion_latency_seconds"},
		{
			name:       "valid histogram with tenant_id label",
			metricName: "ingestion_latency_seconds",
			labelNames: []string{"tenant_id"},
		},
		{
			name:       "unsupported suffix rejected",
			metricName: "ingestion_latency_total",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "uppercase name rejected",
			metricName: "Ingestion_Latency_Seconds",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "empty name rejected",
			metricName: "",
			wantErr:    ErrInvalidMetricName,
		},
		{
			name:       "denied label rejected case-insensitively",
			metricName: "ingestion_latency_seconds",
			labelNames: []string{"Trace_Id"},
			wantErr:    ErrHighCardinalityLabel,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			opts := prometheus.HistogramOpts{
				Name:    testCase.metricName,
				Help:    "test histogram",
				Buckets: LatencyBucketsSeconds,
			}

			histogramVec, constructErr := NewHistogram(registry, opts, testCase.labelNames...)

			if testCase.wantErr != nil {
				if constructErr == nil {
					t.Fatalf("NewHistogram(%q) = nil error, want error wrapping %v", testCase.metricName, testCase.wantErr)
				}
				if !errors.Is(constructErr, testCase.wantErr) {
					t.Fatalf("NewHistogram(%q) error = %v, want it to wrap %v", testCase.metricName, constructErr, testCase.wantErr)
				}
				if histogramVec != nil {
					t.Fatalf("NewHistogram(%q) returned non-nil HistogramVec alongside an error", testCase.metricName)
				}
				return
			}

			if constructErr != nil {
				t.Fatalf("NewHistogram(%q) unexpected error: %v", testCase.metricName, constructErr)
			}
			if histogramVec == nil {
				t.Fatalf("NewHistogram(%q) returned nil HistogramVec with no error", testCase.metricName)
			}
		})
	}
}

func TestNewCounter_DuplicateRegistrationReturnsError(t *testing.T) {
	registry := prometheus.NewRegistry()
	opts := prometheus.CounterOpts{
		Name: "duplicate_registration_total",
		Help: "test counter",
	}

	if _, firstErr := NewCounter(registry, opts); firstErr != nil {
		t.Fatalf("first NewCounter call unexpected error: %v", firstErr)
	}

	secondCounter, secondErr := NewCounter(registry, opts)
	if secondErr == nil {
		t.Fatal("second NewCounter call on the same registerer = nil error, want a wrapped registration error")
	}
	if secondCounter != nil {
		t.Fatal("second NewCounter call returned non-nil CounterVec alongside an error")
	}
	if errors.Is(secondErr, ErrInvalidMetricName) || errors.Is(secondErr, ErrHighCardinalityLabel) {
		t.Fatalf("second NewCounter call error = %v, want a registration conflict error, not a naming/label error", secondErr)
	}
}

func TestHandler(t *testing.T) {
	registry := prometheus.NewRegistry()
	opts := prometheus.CounterOpts{
		Name: "handler_test_requests_total",
		Help: "test counter served by Handler",
	}

	counterVec, constructErr := NewCounter(registry, opts, "tenant_id")
	if constructErr != nil {
		t.Fatalf("NewCounter unexpected error: %v", constructErr)
	}
	counterVec.WithLabelValues("tenant-a").Inc()

	handler := Handler(registry)
	if handler == nil {
		t.Fatal("Handler(registry) = nil, want a non-nil http.Handler")
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	handler.ServeHTTP(recorder, request)

	if recorder.Code != 200 {
		t.Fatalf("Handler response status = %d, want 200", recorder.Code)
	}

	body, readErr := io.ReadAll(recorder.Body)
	if readErr != nil {
		t.Fatalf("reading response body: %v", readErr)
	}
	if !strings.Contains(string(body), "handler_test_requests_total") {
		t.Fatalf("Handler response body does not contain the registered metric name; got: %s", body)
	}
}
