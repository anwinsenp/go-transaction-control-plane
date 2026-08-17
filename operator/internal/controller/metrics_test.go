package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
)

const reconcileDurationMetricName = "tradingtenant_reconcile_duration_seconds"
const isolationTransitionsMetricName = "tradingtenant_isolation_transitions_total"

func TestNewMetrics_RegistersHistogram(t *testing.T) {
	registry := prometheus.NewRegistry()

	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	if reportedMetrics == nil {
		t.Fatal("NewMetrics returned nil Metrics with a nil error")
	}

	// NewMetrics eagerly caches an Observer per known result label via
	// WithLabelValues, so both the success and error child metrics already
	// exist in the gathered output even before any reconcile pass runs.
	gatheredCount := testutil.CollectAndCount(registry, reconcileDurationMetricName)
	if gatheredCount != 2 {
		t.Errorf("gathered metric count before any observation = %d, want 2 (success and error label children pre-cached)", gatheredCount)
	}

	if gotSuccess := sampleCountForLabel(t, registry, resultSuccess); gotSuccess != 0 {
		t.Errorf("success bucket sample count before any observation = %v, want 0", gotSuccess)
	}
	if gotError := sampleCountForLabel(t, registry, resultError); gotError != 0 {
		t.Errorf("error bucket sample count before any observation = %v, want 0", gotError)
	}

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather: %v", err)
	}
	foundExpectedName := false
	for _, family := range metricFamilies {
		if family.GetName() == reconcileDurationMetricName {
			foundExpectedName = true
			break
		}
	}
	if !foundExpectedName {
		t.Errorf("registry.Gather did not include %s", reconcileDurationMetricName)
	}
}

func TestNewMetrics_RegisterErrorOnDuplicateRegistration(t *testing.T) {
	registry := prometheus.NewRegistry()

	if _, err := NewMetrics(registry); err != nil {
		t.Fatalf("first NewMetrics call returned error: %v", err)
	}

	if _, err := NewMetrics(registry); err == nil {
		t.Fatal("second NewMetrics call on the same registerer returned nil error, want a duplicate-registration error")
	}
}

func TestMetrics_ObserveDuration(t *testing.T) {
	testCases := []struct {
		name        string
		observeAs   string
		wantSuccess float64
		wantError   float64
	}{
		{
			name:        "observing success increments only the success bucket",
			observeAs:   resultSuccess,
			wantSuccess: 1,
			wantError:   0,
		},
		{
			name:        "observing error increments only the error bucket",
			observeAs:   resultError,
			wantSuccess: 0,
			wantError:   1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			reportedMetrics, err := NewMetrics(registry)
			if err != nil {
				t.Fatalf("NewMetrics returned error: %v", err)
			}

			reportedMetrics.observeDuration(testCase.observeAs, 0.042)

			gotSuccess := sampleCountForLabel(t, registry, resultSuccess)
			gotError := sampleCountForLabel(t, registry, resultError)

			if gotSuccess != testCase.wantSuccess {
				t.Errorf("success bucket sample count = %v, want %v", gotSuccess, testCase.wantSuccess)
			}
			if gotError != testCase.wantError {
				t.Errorf("error bucket sample count = %v, want %v", gotError, testCase.wantError)
			}
		})
	}
}

func TestMetrics_ObserveIsolationTransition(t *testing.T) {
	testCases := []struct {
		name      string
		reason    string
		wantKnown bool
	}{
		{
			name:      "stable reason increments the stable counter",
			reason:    string(tradingv1alpha1.TradingTenantStateStable),
			wantKnown: true,
		},
		{
			name:      "scaling reason increments the scaling counter",
			reason:    string(tradingv1alpha1.TradingTenantStateScaling),
			wantKnown: true,
		},
		{
			name:      "isolated reason increments the isolated counter",
			reason:    string(tradingv1alpha1.TradingTenantStateIsolated),
			wantKnown: true,
		},
		{
			name:      "degraded reason increments the degraded counter",
			reason:    string(tradingv1alpha1.TradingTenantStateDegraded),
			wantKnown: true,
		},
		{
			name:      "dedicated pool created reason increments its counter",
			reason:    reasonDedicatedPoolCreated,
			wantKnown: true,
		},
		{
			name:      "unknown reason is a silent no-op",
			reason:    "SomethingUnexpected",
			wantKnown: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			reportedMetrics, err := NewMetrics(registry)
			if err != nil {
				t.Fatalf("NewMetrics returned error: %v", err)
			}

			reportedMetrics.observeIsolationTransition(testCase.reason)

			for _, knownReason := range isolationTransitionReasons {
				want := 0.0
				if testCase.wantKnown && knownReason == testCase.reason {
					want = 1
				}
				got := sampleCountForIsolationLabel(t, registry, knownReason)
				if got != want {
					t.Errorf("isolation transition count for state=%s = %v, want %v", knownReason, got, want)
				}
			}
		})
	}
}

// sampleCountForLabel returns the histogram sample count recorded against
// the reconcile duration histogram's "result" label with the given value.
// It fails the test via t.Fatalf if the metric family or the label value
// isn't present in the gathered output, so a typo'd name/label can't be
// mistaken for a genuine zero-count observation.
// The metric-family type returned by Gather is deliberately left
// inferred rather than named, so this test file doesn't need a direct
// import of the underlying client_model package.
func sampleCountForLabel(t *testing.T, gatherer prometheus.Gatherer, resultLabel string) float64 {
	t.Helper()

	metricFamilies, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gatherer.Gather: %v", err)
	}
	for _, family := range metricFamilies {
		if family.GetName() != reconcileDurationMetricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "result" && label.GetValue() == resultLabel {
					return float64(metric.GetHistogram().GetSampleCount())
				}
			}
		}
		t.Fatalf("metric family %s found but no sample with label result=%s", reconcileDurationMetricName, resultLabel)
	}
	t.Fatalf("metric family %s not found in gathered output", reconcileDurationMetricName)
	return 0
}

// sampleCountForIsolationLabel returns the counter value recorded against
// the isolation-transitions counter's "transition" label with the given
// value. Mirrors sampleCountForLabel's fail-loud-on-missing-sample behavior
// so a typo'd label can't be mistaken for a genuine zero-count observation.
func sampleCountForIsolationLabel(t *testing.T, gatherer prometheus.Gatherer, stateLabel string) float64 {
	t.Helper()

	metricFamilies, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gatherer.Gather: %v", err)
	}
	for _, family := range metricFamilies {
		if family.GetName() != isolationTransitionsMetricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "transition" && label.GetValue() == stateLabel {
					return metric.GetCounter().GetValue()
				}
			}
		}
		t.Fatalf("metric family %s found but no sample with label transition=%s", isolationTransitionsMetricName, stateLabel)
	}
	t.Fatalf("metric family %s not found in gathered output", isolationTransitionsMetricName)
	return 0
}
