package processor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// fakeReconciler is a reconciler test double that records every
// transaction it's asked to reconcile, so InstrumentedReconciler's
// wrapping behavior can be tested without a real Reconciler or Postgres.
type fakeReconciler struct {
	reconciled []ledger.Transaction
	err        error
}

func (fake *fakeReconciler) Reconcile(ctx context.Context, txn ledger.Transaction) error {
	fake.reconciled = append(fake.reconciled, txn)
	return fake.err
}

// fakeBreakerStater is a BreakerStater test double whose reported State is
// set directly by the test, so gauge-reporting tests don't need to drive a
// real breaker through failures to reach each state.
type fakeBreakerStater struct {
	state ledger.BreakerState
}

func (fake *fakeBreakerStater) State() ledger.BreakerState {
	return fake.state
}

// errorContains reports whether err's message contains substring, so
// tests can assert on wrapped error text without depending on the exact
// wrapping chain.
func errorContains(err error, substring string) bool {
	return err != nil && strings.Contains(err.Error(), substring)
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

// mustNewTestMetrics builds a Metrics on a fresh registry, failing the test
// if construction fails.
func mustNewTestMetrics(t *testing.T, knownTenants metrics.KnownTenants) *Metrics {
	t.Helper()
	reportedMetrics, err := NewMetrics(prometheus.NewRegistry(), knownTenants)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	return reportedMetrics
}

func TestNewMetrics_RegistersSuccessfully(t *testing.T) {
	registry := prometheus.NewRegistry()

	reportedMetrics, err := NewMetrics(registry, metrics.NewKnownTenants("tenant-1"))
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	if reportedMetrics == nil {
		t.Fatal("NewMetrics() returned nil Metrics with no error")
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, `processor_transactions_processed_total{outcome="success"} 0`)
	requireMetricsLine(t, scraped, `processor_transactions_processed_total{outcome="failure"} 0`)
	requireMetricsLine(t, scraped, `processor_postgres_circuit_breaker_state{repository="transactions"} 0`)
	requireMetricsLine(t, scraped, `processor_postgres_circuit_breaker_state{repository="reconciled_state"} 0`)
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

func TestNewInstrumentedReconciler(t *testing.T) {
	testCases := []struct {
		name            string
		next            reconciler
		reportedMetrics *Metrics
		wantErrMsg      string
	}{
		{
			name:            "valid inputs succeed",
			next:            &fakeReconciler{},
			reportedMetrics: mustNewTestMetrics(t, nil),
		},
		{
			name:            "nil next reconciler is rejected",
			next:            nil,
			reportedMetrics: mustNewTestMetrics(t, nil),
			wantErrMsg:      "next reconciler must not be nil",
		},
		{
			name:            "nil metrics is rejected",
			next:            &fakeReconciler{},
			reportedMetrics: nil,
			wantErrMsg:      "metrics must not be nil",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			instrumented, err := NewInstrumentedReconciler(testCase.next, testCase.reportedMetrics, nil, nil)

			if testCase.wantErrMsg == "" {
				if err != nil {
					t.Fatalf("NewInstrumentedReconciler() error = %v, want nil", err)
				}
				if instrumented == nil {
					t.Fatal("NewInstrumentedReconciler() returned nil reconciler with nil error")
				}
				return
			}

			if err == nil {
				t.Fatal("NewInstrumentedReconciler() error = nil, want non-nil")
			}
			if instrumented != nil {
				t.Errorf("NewInstrumentedReconciler() reconciler = %v, want nil on error", instrumented)
			}
			if !errorContains(err, testCase.wantErrMsg) {
				t.Errorf("error = %v, want it to contain %q", err, testCase.wantErrMsg)
			}
			if !errorContains(err, "new instrumented reconciler") {
				t.Errorf("error = %v, want it wrapped with \"new instrumented reconciler\"", err)
			}
		})
	}
}

func TestInstrumentedReconciler_Reconcile(t *testing.T) {
	testCases := []struct {
		name          string
		reconcilerErr error
	}{
		{name: "successful reconcile records the success counter and histogram", reconcilerErr: nil},
		{name: "failing reconcile records the failure counter and histogram", reconcilerErr: errors.New("reconcile failed")},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			reportedMetrics, err := NewMetrics(registry, nil)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			fake := &fakeReconciler{err: testCase.reconcilerErr}
			instrumented, err := NewInstrumentedReconciler(fake, reportedMetrics, nil, nil)
			if err != nil {
				t.Fatalf("NewInstrumentedReconciler() unexpected error: %v", err)
			}

			txn := ledger.Transaction{TenantID: "tenant-1"}
			reconcileErr := instrumented.Reconcile(context.Background(), txn)
			if !errors.Is(reconcileErr, testCase.reconcilerErr) {
				t.Fatalf("Reconcile() error = %v, want %v", reconcileErr, testCase.reconcilerErr)
			}
			if len(fake.reconciled) != 1 || fake.reconciled[0] != txn {
				t.Errorf("wrapped Reconcile was not called with %+v: got %+v", txn, fake.reconciled)
			}

			wantSuccessTotal, wantFailureTotal := 0, 0
			wantOutcome := outcomeSuccess
			if testCase.reconcilerErr == nil {
				wantSuccessTotal = 1
			} else {
				wantFailureTotal = 1
				wantOutcome = outcomeFailure
			}

			scraped := scrapeMetrics(t, registry)
			requireMetricsLine(t, scraped, fmt.Sprintf(`processor_transactions_processed_total{outcome="success"} %d`, wantSuccessTotal))
			requireMetricsLine(t, scraped, fmt.Sprintf(`processor_transactions_processed_total{outcome="failure"} %d`, wantFailureTotal))
			requireMetricsLine(t, scraped, fmt.Sprintf(`processor_transaction_duration_seconds_count{outcome="%s",tenant="%s"} 1`, wantOutcome, metrics.UnknownTenantLabel))
		})
	}
}

func TestInstrumentedReconciler_Reconcile_TenantLabel(t *testing.T) {
	testCases := []struct {
		name         string
		knownTenants metrics.KnownTenants
		tenantID     string
		wantLabel    string
	}{
		{
			name:         "known tenant is labeled verbatim",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			tenantID:     "tenant-1",
			wantLabel:    "tenant-1",
		},
		{
			name:         "unknown tenant falls back to UnknownTenantLabel",
			knownTenants: metrics.NewKnownTenants("tenant-1"),
			tenantID:     "tenant-unregistered",
			wantLabel:    metrics.UnknownTenantLabel,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			reportedMetrics, err := NewMetrics(registry, testCase.knownTenants)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			instrumented, err := NewInstrumentedReconciler(&fakeReconciler{}, reportedMetrics, nil, nil)
			if err != nil {
				t.Fatalf("NewInstrumentedReconciler() unexpected error: %v", err)
			}

			txn := ledger.Transaction{TenantID: testCase.tenantID}
			if err := instrumented.Reconcile(context.Background(), txn); err != nil {
				t.Fatalf("Reconcile() error = %v, want nil", err)
			}

			scraped := scrapeMetrics(t, registry)
			requireMetricsLine(t, scraped, fmt.Sprintf(`processor_transaction_duration_seconds_count{outcome="success",tenant="%s"} 1`, testCase.wantLabel))
		})
	}
}

func TestInstrumentedReconciler_Reconcile_BreakerState(t *testing.T) {
	testCases := []struct {
		name      string
		state     ledger.BreakerState
		wantGauge int
	}{
		{name: "closed breaker reports 0", state: ledger.BreakerClosed, wantGauge: breakerStateClosed},
		{name: "half-open breaker reports 1", state: ledger.BreakerHalfOpen, wantGauge: breakerStateHalfOpen},
		{name: "open breaker reports 2", state: ledger.BreakerOpen, wantGauge: breakerStateOpen},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			reportedMetrics, err := NewMetrics(registry, nil)
			if err != nil {
				t.Fatalf("NewMetrics() unexpected error: %v", err)
			}

			transactionsBreaker := &fakeBreakerStater{state: testCase.state}
			statesBreaker := &fakeBreakerStater{state: testCase.state}
			instrumented, err := NewInstrumentedReconciler(&fakeReconciler{}, reportedMetrics, transactionsBreaker, statesBreaker)
			if err != nil {
				t.Fatalf("NewInstrumentedReconciler() unexpected error: %v", err)
			}

			if err := instrumented.Reconcile(context.Background(), ledger.Transaction{}); err != nil {
				t.Fatalf("Reconcile() error = %v, want nil", err)
			}

			scraped := scrapeMetrics(t, registry)
			requireMetricsLine(t, scraped, fmt.Sprintf(`processor_postgres_circuit_breaker_state{repository="transactions"} %d`, testCase.wantGauge))
			requireMetricsLine(t, scraped, fmt.Sprintf(`processor_postgres_circuit_breaker_state{repository="reconciled_state"} %d`, testCase.wantGauge))
		})
	}
}

func TestInstrumentedReconciler_Reconcile_NilBreakersLeaveGaugesUntouched(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	instrumented, err := NewInstrumentedReconciler(&fakeReconciler{}, reportedMetrics, nil, nil)
	if err != nil {
		t.Fatalf("NewInstrumentedReconciler() unexpected error: %v", err)
	}

	if err := instrumented.Reconcile(context.Background(), ledger.Transaction{}); err != nil {
		t.Fatalf("Reconcile() error = %v, want nil", err)
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, `processor_postgres_circuit_breaker_state{repository="transactions"} 0`)
	requireMetricsLine(t, scraped, `processor_postgres_circuit_breaker_state{repository="reconciled_state"} 0`)
}

func TestInstrumentedReconciler_Reconcile_OneBreakerNilLeavesOnlyItsGaugeUntouched(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	transactionsBreaker := &fakeBreakerStater{state: ledger.BreakerOpen}
	instrumented, err := NewInstrumentedReconciler(&fakeReconciler{}, reportedMetrics, transactionsBreaker, nil)
	if err != nil {
		t.Fatalf("NewInstrumentedReconciler() unexpected error: %v", err)
	}

	if err := instrumented.Reconcile(context.Background(), ledger.Transaction{}); err != nil {
		t.Fatalf("Reconcile() error = %v, want nil", err)
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, fmt.Sprintf(`processor_postgres_circuit_breaker_state{repository="transactions"} %d`, breakerStateOpen))
	requireMetricsLine(t, scraped, `processor_postgres_circuit_breaker_state{repository="reconciled_state"} 0`)
}

func TestBreakerStateValue(t *testing.T) {
	testCases := []struct {
		name  string
		state ledger.BreakerState
		want  float64
	}{
		{name: "closed", state: ledger.BreakerClosed, want: breakerStateClosed},
		{name: "half-open", state: ledger.BreakerHalfOpen, want: breakerStateHalfOpen},
		{name: "open", state: ledger.BreakerOpen, want: breakerStateOpen},
		{name: "unrecognized state reports unknown, not closed", state: ledger.BreakerState(99), want: breakerStateUnknown},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := breakerStateValue(testCase.state); got != testCase.want {
				t.Errorf("breakerStateValue(%v) = %v, want %v", testCase.state, got, testCase.want)
			}
		})
	}
}
