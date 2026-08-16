package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// fakeBreakerStater is a BreakerStater test double whose reported State is
// set directly by the test, so gauge-reporting tests don't need to drive a
// real CircuitBreaker through failures to reach each state.
type fakeBreakerStater struct {
	state BreakerState
}

func (fake *fakeBreakerStater) State() BreakerState {
	return fake.state
}

// scrapeMetrics renders registry's current state in Prometheus exposition
// format, the same way internal/metrics's TestHandler inspects a
// registry's output, so assertions here read actual counter/histogram/gauge
// values rather than reaching into unexported fields.
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

func TestNewMetrics_RegistersSuccessfully(t *testing.T) {
	registry := prometheus.NewRegistry()

	ingestionMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	if ingestionMetrics == nil {
		t.Fatal("NewMetrics() returned nil Metrics with no error")
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, `ingestion_transactions_processed_total{outcome="success"} 0`)
	requireMetricsLine(t, scraped, `ingestion_transactions_processed_total{outcome="failure"} 0`)
	requireMetricsLine(t, scraped, "ingestion_kafka_circuit_breaker_state 0")
}

func TestNewMetrics_DuplicateRegistrationReturnsError(t *testing.T) {
	registry := prometheus.NewRegistry()

	if _, firstErr := NewMetrics(registry); firstErr != nil {
		t.Fatalf("first NewMetrics() unexpected error: %v", firstErr)
	}

	secondMetrics, secondErr := NewMetrics(registry)
	if secondErr == nil {
		t.Fatal("second NewMetrics() on the same registerer = nil error, want a registration conflict error")
	}
	if secondMetrics != nil {
		t.Fatal("second NewMetrics() returned non-nil Metrics alongside an error")
	}
}

func TestNewInstrumentedPublisher(t *testing.T) {
	testCases := []struct {
		name            string
		next            Publisher
		reportedMetrics *Metrics
		wantErrMsg      string
	}{
		{
			name:            "valid inputs succeed",
			next:            &fakePublisher{},
			reportedMetrics: mustNewTestMetrics(t),
		},
		{
			name:            "nil next publisher is rejected",
			next:            nil,
			reportedMetrics: mustNewTestMetrics(t),
			wantErrMsg:      "next Publisher must not be nil",
		},
		{
			name:            "nil metrics is rejected",
			next:            &fakePublisher{},
			reportedMetrics: nil,
			wantErrMsg:      "metrics must not be nil",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			instrumented, err := NewInstrumentedPublisher(testCase.next, testCase.reportedMetrics, nil)

			if testCase.wantErrMsg == "" {
				if err != nil {
					t.Fatalf("NewInstrumentedPublisher() error = %v, want nil", err)
				}
				if instrumented == nil {
					t.Fatal("NewInstrumentedPublisher() returned nil publisher with nil error")
				}
				return
			}

			if err == nil {
				t.Fatal("NewInstrumentedPublisher() error = nil, want non-nil")
			}
			if instrumented != nil {
				t.Errorf("NewInstrumentedPublisher() publisher = %v, want nil on error", instrumented)
			}
			if !errorContains(err, testCase.wantErrMsg) {
				t.Errorf("error = %v, want it to contain %q", err, testCase.wantErrMsg)
			}
		})
	}
}

func TestInstrumentedPublisher_Publish(t *testing.T) {
	testCases := []struct {
		name         string
		publisherErr error
	}{
		{name: "successful publish records the success counter and histogram", publisherErr: nil},
		{name: "failing publish records the failure counter and histogram", publisherErr: errPublishFailed},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			ingestionMetrics, err := NewMetrics(registry)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			fake := &fakePublisher{err: testCase.publisherErr}
			instrumented, err := NewInstrumentedPublisher(fake, ingestionMetrics, nil)
			if err != nil {
				t.Fatalf("NewInstrumentedPublisher() unexpected error: %v", err)
			}

			publishErr := instrumented.Publish(context.Background(), Event{})
			if !errors.Is(publishErr, testCase.publisherErr) {
				t.Fatalf("Publish() error = %v, want %v", publishErr, testCase.publisherErr)
			}

			wantSuccessTotal, wantFailureTotal := 0, 0
			if testCase.publisherErr == nil {
				wantSuccessTotal = 1
			} else {
				wantFailureTotal = 1
			}

			scraped := scrapeMetrics(t, registry)
			requireMetricsLine(t, scraped, fmt.Sprintf(`ingestion_transactions_processed_total{outcome="success"} %d`, wantSuccessTotal))
			requireMetricsLine(t, scraped, fmt.Sprintf(`ingestion_transactions_processed_total{outcome="failure"} %d`, wantFailureTotal))
			requireMetricsLine(t, scraped, fmt.Sprintf(`ingestion_publish_latency_seconds_count{outcome="success"} %d`, wantSuccessTotal))
			requireMetricsLine(t, scraped, fmt.Sprintf(`ingestion_publish_latency_seconds_count{outcome="failure"} %d`, wantFailureTotal))
		})
	}
}

func TestInstrumentedPublisher_Publish_BreakerState(t *testing.T) {
	testCases := []struct {
		name      string
		state     BreakerState
		wantGauge int
	}{
		{name: "closed breaker reports 0", state: BreakerClosed, wantGauge: breakerStateClosed},
		{name: "half-open breaker reports 1", state: BreakerHalfOpen, wantGauge: breakerStateHalfOpen},
		{name: "open breaker reports 2", state: BreakerOpen, wantGauge: breakerStateOpen},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			ingestionMetrics, err := NewMetrics(registry)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			breaker := &fakeBreakerStater{state: testCase.state}
			instrumented, err := NewInstrumentedPublisher(&fakePublisher{}, ingestionMetrics, breaker)
			if err != nil {
				t.Fatalf("NewInstrumentedPublisher() unexpected error: %v", err)
			}

			if publishErr := instrumented.Publish(context.Background(), Event{}); publishErr != nil {
				t.Fatalf("Publish() error = %v, want nil", publishErr)
			}

			scraped := scrapeMetrics(t, registry)
			requireMetricsLine(t, scraped, fmt.Sprintf("ingestion_kafka_circuit_breaker_state %d", testCase.wantGauge))
		})
	}
}

func TestBreakerStateValue(t *testing.T) {
	testCases := []struct {
		name  string
		state BreakerState
		want  float64
	}{
		{name: "closed", state: BreakerClosed, want: breakerStateClosed},
		{name: "half-open", state: BreakerHalfOpen, want: breakerStateHalfOpen},
		{name: "open", state: BreakerOpen, want: breakerStateOpen},
		{name: "unrecognized state reports unknown, not closed", state: BreakerState(99), want: breakerStateUnknown},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := breakerStateValue(testCase.state); got != testCase.want {
				t.Errorf("breakerStateValue(%v) = %v, want %v", testCase.state, got, testCase.want)
			}
		})
	}
}

func TestInstrumentedPublisher_Publish_NilBreakerLeavesGaugeUntouched(t *testing.T) {
	registry := prometheus.NewRegistry()
	ingestionMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	instrumented, err := NewInstrumentedPublisher(&fakePublisher{}, ingestionMetrics, nil)
	if err != nil {
		t.Fatalf("NewInstrumentedPublisher() unexpected error: %v", err)
	}

	if publishErr := instrumented.Publish(context.Background(), Event{}); publishErr != nil {
		t.Fatalf("Publish() error = %v, want nil", publishErr)
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, "ingestion_kafka_circuit_breaker_state 0")
}

// mustNewTestMetrics builds a Metrics on a fresh registry, failing the test
// if construction fails.
func mustNewTestMetrics(t *testing.T) *Metrics {
	t.Helper()
	ingestionMetrics, err := NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	return ingestionMetrics
}
