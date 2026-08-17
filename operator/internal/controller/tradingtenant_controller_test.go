package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
	"github.com/anwinsenp/go-transaction-control-plane/operator/internal/promquery"
)

// fakeObserver is a canned TenantObserver for exercising Reconcile without
// hitting Prometheus.
type fakeObserver struct {
	lag            int64
	p99Ms          int32
	partitionCount int32
	err            error

	gotLabels []promquery.TenantLabel
}

func (observer *fakeObserver) ObservedKafkaLag(ctx context.Context, label promquery.TenantLabel) (int64, error) {
	observer.gotLabels = append(observer.gotLabels, label)
	if observer.err != nil {
		return 0, observer.err
	}
	return observer.lag, nil
}

func (observer *fakeObserver) ObservedP99Ms(ctx context.Context, label promquery.TenantLabel) (int32, error) {
	if observer.err != nil {
		return 0, observer.err
	}
	return observer.p99Ms, nil
}

func (observer *fakeObserver) ObservedPartitionCount(ctx context.Context, label promquery.TenantLabel) (int32, error) {
	if observer.err != nil {
		return 0, observer.err
	}
	return observer.partitionCount, nil
}

func newTestTenant() *tradingv1alpha1.TradingTenant {
	return &tradingv1alpha1.TradingTenant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tenant-alpha",
			Namespace: "default",
		},
		Spec: tradingv1alpha1.TradingTenantSpec{
			TenantID:    "tenant-alpha",
			MinReplicas: 2,
			MaxReplicas: 10,
			Resources: tradingv1alpha1.ResourceRequirements{
				CPURequest:    resource.MustParse("500m"),
				MemoryRequest: resource.MustParse("512Mi"),
			},
			Scaling: tradingv1alpha1.ScalingPolicy{
				KafkaLagThreshold:     1000,
				P99LatencyThresholdMs: 50,
			},
		},
		Status: tradingv1alpha1.TradingTenantStatus{
			CurrentReplicas: 3,
		},
	}
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := tradingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

func newReconciler(t *testing.T, tenant *tradingv1alpha1.TradingTenant, observer TenantObserver) (*TradingTenantReconciler, client.Client) {
	t.Helper()
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithStatusSubresource(&tradingv1alpha1.TradingTenant{}).
		WithObjects(tenant).
		Build()

	reconciler := &TradingTenantReconciler{
		Client:   fakeClient,
		Observer: observer,
	}
	return reconciler, fakeClient
}

func reconcileRequest(tenant *tradingv1alpha1.TradingTenant) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: tenant.Name, Namespace: tenant.Namespace}}
}

func TestReconcile_DecisionTableBranches(t *testing.T) {
	testCases := []struct {
		name              string
		observer          *fakeObserver
		wantState         tradingv1alpha1.TradingTenantState
		wantReplicas      int32
		wantDedicatedPool bool
	}{
		{
			name:              "scale up on lag and latency high",
			observer:          &fakeObserver{lag: 2000, p99Ms: 100, partitionCount: 12},
			wantState:         tradingv1alpha1.TradingTenantStateScaling,
			wantReplicas:      10,
			wantDedicatedPool: false,
		},
		{
			name:              "scale up on lag high latency normal with partition headroom",
			observer:          &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 6},
			wantState:         tradingv1alpha1.TradingTenantStateScaling,
			wantReplicas:      6,
			wantDedicatedPool: false,
		},
		{
			name:              "isolate on lag high latency normal at partition parity",
			observer:          &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 3},
			wantState:         tradingv1alpha1.TradingTenantStateIsolated,
			wantReplicas:      3,
			wantDedicatedPool: true,
		},
		{
			name:              "degraded on lag normal latency high",
			observer:          &fakeObserver{lag: 100, p99Ms: 100, partitionCount: 12},
			wantState:         tradingv1alpha1.TradingTenantStateDegraded,
			wantReplicas:      3,
			wantDedicatedPool: false,
		},
		{
			name:              "stable on lag normal latency normal",
			observer:          &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12},
			wantState:         tradingv1alpha1.TradingTenantStateStable,
			wantReplicas:      3,
			wantDedicatedPool: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tenant := newTestTenant()
			reconciler, fakeClient := newReconciler(t, tenant, testCase.observer)

			if _, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant)); err != nil {
				t.Fatalf("Reconcile returned error: %v", err)
			}

			var got tradingv1alpha1.TradingTenant
			if err := fakeClient.Get(context.Background(), reconcileRequest(tenant).NamespacedName, &got); err != nil {
				t.Fatalf("Get after Reconcile: %v", err)
			}

			if got.Status.State != testCase.wantState {
				t.Errorf("status.state = %q, want %q", got.Status.State, testCase.wantState)
			}
			if got.Status.CurrentReplicas != testCase.wantReplicas {
				t.Errorf("status.currentReplicas = %d, want %d", got.Status.CurrentReplicas, testCase.wantReplicas)
			}
			if got.Spec.Isolation.DedicatedNodePool != testCase.wantDedicatedPool {
				t.Errorf("spec.isolation.dedicatedNodePool = %v, want %v", got.Spec.Isolation.DedicatedNodePool, testCase.wantDedicatedPool)
			}
			if got.Status.LastReconcileTime.IsZero() {
				t.Error("status.lastReconcileTime was not set")
			}
		})
	}
}

func TestReconcile_ObserverLabel(t *testing.T) {
	testCases := []struct {
		name            string
		tenantLabelName string
		wantLabelName   string
	}{
		{
			name:          "default tenant label name is used when unset",
			wantLabelName: defaultTenantLabelName,
		},
		{
			name:            "configured tenant label name overrides the default",
			tenantLabelName: "custom_tenant_key",
			wantLabelName:   "custom_tenant_key",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tenant := newTestTenant()
			observer := &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12}
			reconciler, _ := newReconciler(t, tenant, observer)
			reconciler.TenantLabelName = testCase.tenantLabelName

			if _, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant)); err != nil {
				t.Fatalf("Reconcile returned error: %v", err)
			}

			wantLabel := promquery.TenantLabel{Name: testCase.wantLabelName, Value: tenant.Spec.TenantID}
			if len(observer.gotLabels) == 0 {
				t.Fatal("observer did not receive any labels")
			}
			for _, gotLabel := range observer.gotLabels {
				if gotLabel != wantLabel {
					t.Errorf("observer label = %+v, want %+v", gotLabel, wantLabel)
				}
			}
		})
	}
}

func TestReconcile_ObserverErrorIsWrappedNotSwallowed(t *testing.T) {
	sentinelErr := errors.New("prometheus unreachable")
	tenant := newTestTenant()
	reconciler, _ := newReconciler(t, tenant, &fakeObserver{err: sentinelErr})

	result, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant))
	if err == nil {
		t.Fatal("Reconcile returned nil error, want wrapped observer error")
	}
	if !errors.Is(err, sentinelErr) {
		t.Errorf("Reconcile error = %v, want it to wrap %v", err, sentinelErr)
	}
	if result != (ctrl.Result{}) {
		t.Errorf("Reconcile result = %+v, want zero value on error", result)
	}
}

func TestReconcile_IsIdempotent(t *testing.T) {
	tenant := newTestTenant()
	observer := &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 3}
	reconciler, fakeClient := newReconciler(t, tenant, observer)
	request := reconcileRequest(tenant)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	var firstPass tradingv1alpha1.TradingTenant
	if err := fakeClient.Get(context.Background(), request.NamespacedName, &firstPass); err != nil {
		t.Fatalf("Get after first Reconcile: %v", err)
	}

	// The fake client round-trips metav1.Time through JSON, which truncates
	// to second-level precision (matching real API server behavior). Sleep
	// past a second boundary so LastReconcileTime is guaranteed to advance
	// rather than flaking on fast test runs.
	time.Sleep(1100 * time.Millisecond)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	var secondPass tradingv1alpha1.TradingTenant
	if err := fakeClient.Get(context.Background(), request.NamespacedName, &secondPass); err != nil {
		t.Fatalf("Get after second Reconcile: %v", err)
	}

	if !secondPass.Status.LastReconcileTime.After(firstPass.Status.LastReconcileTime.Time) {
		t.Errorf("status.lastReconcileTime did not advance across Reconcile calls: first=%v second=%v",
			firstPass.Status.LastReconcileTime, secondPass.Status.LastReconcileTime)
	}

	firstStatus, secondStatus := firstPass.Status, secondPass.Status
	firstStatus.LastReconcileTime = metav1.Time{}
	secondStatus.LastReconcileTime = metav1.Time{}
	if !reflect.DeepEqual(firstStatus, secondStatus) {
		t.Errorf("status changed across idempotent Reconcile calls (excluding lastReconcileTime):\nfirst:  %+v\nsecond: %+v", firstStatus, secondStatus)
	}
	if firstPass.Spec.Isolation.DedicatedNodePool != secondPass.Spec.Isolation.DedicatedNodePool {
		t.Errorf("spec.isolation.dedicatedNodePool changed across idempotent Reconcile calls: %v -> %v",
			firstPass.Spec.Isolation.DedicatedNodePool, secondPass.Spec.Isolation.DedicatedNodePool)
	}
}

func TestReconcile_StatusOnlyReconcileDoesNotMutateSpec(t *testing.T) {
	tenant := newTestTenant()
	// Start below MinReplicas so the stable branch's baseline clamp forces
	// status.currentReplicas to visibly change. Without this, the stable
	// branch would leave CurrentReplicas untouched and this test could pass
	// even if status writes were broken entirely.
	tenant.Status.CurrentReplicas = 0
	wantSpec := tenant.Spec.DeepCopy()
	beforeStatus := tenant.Status.DeepCopy()

	// lag and latency both normal: the stable branch, which never sets
	// setDedicatedNodePool, so this pass should touch status only.
	observer := &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12}
	reconciler, fakeClient := newReconciler(t, tenant, observer)
	request := reconcileRequest(tenant)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var got tradingv1alpha1.TradingTenant
	if err := fakeClient.Get(context.Background(), request.NamespacedName, &got); err != nil {
		t.Fatalf("Get after Reconcile: %v", err)
	}

	if !reflect.DeepEqual(got.Spec, *wantSpec) {
		t.Errorf("spec changed on a status-only reconcile:\nbefore: %+v\nafter:  %+v", *wantSpec, got.Spec)
	}

	// Prove this is genuinely a "status updated, spec untouched" test rather
	// than a "nothing happened" test: status must actually have changed in
	// this same pass.
	if reflect.DeepEqual(got.Status, *beforeStatus) {
		t.Fatal("status did not change on reconcile; this test would not catch a broken status write")
	}
	if got.Status.CurrentReplicas != tenant.Spec.MinReplicas {
		t.Errorf("status.currentReplicas = %d, want %d (baseline clamped to MinReplicas)", got.Status.CurrentReplicas, tenant.Spec.MinReplicas)
	}
	if got.Status.State != tradingv1alpha1.TradingTenantStateStable {
		t.Errorf("status.state = %q, want %q", got.Status.State, tradingv1alpha1.TradingTenantStateStable)
	}
}

func TestReconcile_IsolationNeverAutoReverts(t *testing.T) {
	tenant := newTestTenant()
	tenant.Spec.Isolation.DedicatedNodePool = true
	tenant.Status.State = tradingv1alpha1.TradingTenantStateIsolated

	observer := &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12}
	reconciler, fakeClient := newReconciler(t, tenant, observer)
	request := reconcileRequest(tenant)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var got tradingv1alpha1.TradingTenant
	if err := fakeClient.Get(context.Background(), request.NamespacedName, &got); err != nil {
		t.Fatalf("Get after Reconcile: %v", err)
	}

	if got.Status.State != tradingv1alpha1.TradingTenantStateIsolated {
		t.Errorf("status.state = %q, want %q (isolation must not auto-revert)", got.Status.State, tradingv1alpha1.TradingTenantStateIsolated)
	}
	if !got.Spec.Isolation.DedicatedNodePool {
		t.Error("spec.isolation.dedicatedNodePool flipped back to false, want it to remain true")
	}
}

// TestReconcile_IsolationGuardSkipsRedundantSpecUpdateWhenAlreadyIsolated
// covers the !tenant.Spec.Isolation.DedicatedNodePool guard in Reconcile: when
// the tenant is already isolated and the observed signals repeat the isolate
// trigger, the spec Update must be skipped (a no-op, since the field is
// already true) while status writes still proceed normally.
func TestReconcile_IsolationGuardSkipsRedundantSpecUpdateWhenAlreadyIsolated(t *testing.T) {
	tenant := newTestTenant()
	tenant.Spec.Isolation.DedicatedNodePool = true
	tenant.Status.State = tradingv1alpha1.TradingTenantStateIsolated

	// lag high, latency normal, partition parity: the same isolate trigger
	// as TestReconcile_DecisionTableBranches's isolate case, so
	// decision.setDedicatedNodePool is true even though the field is
	// already set.
	observer := &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 3}
	reconciler, fakeClient := newReconciler(t, tenant, observer)
	request := reconcileRequest(tenant)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	var got tradingv1alpha1.TradingTenant
	if err := fakeClient.Get(context.Background(), request.NamespacedName, &got); err != nil {
		t.Fatalf("Get after Reconcile: %v", err)
	}

	if !got.Spec.Isolation.DedicatedNodePool {
		t.Error("spec.isolation.dedicatedNodePool = false, want it to remain true")
	}
	if got.Status.State != tradingv1alpha1.TradingTenantStateIsolated {
		t.Errorf("status.state = %q, want %q", got.Status.State, tradingv1alpha1.TradingTenantStateIsolated)
	}
	if got.Status.CurrentReplicas != 3 {
		t.Errorf("status.currentReplicas = %d, want 3", got.Status.CurrentReplicas)
	}
	if got.Status.LastReconcileTime.IsZero() {
		t.Error("status.lastReconcileTime was not set; status writes should still happen even when the spec write is skipped")
	}
}

func TestReconcile_RecordsSuccessMetric(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}

	tenant := newTestTenant()
	observer := &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12}
	reconciler, _ := newReconciler(t, tenant, observer)
	reconciler.Metrics = reportedMetrics

	if _, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant)); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	if gotSuccess := sampleCountForLabel(t, registry, resultSuccess); gotSuccess != 1 {
		t.Errorf("success bucket sample count = %v, want 1", gotSuccess)
	}
	if gotError := sampleCountForLabel(t, registry, resultError); gotError != 0 {
		t.Errorf("error bucket sample count = %v, want 0", gotError)
	}
}

func TestReconcile_RecordsErrorMetric(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}

	tenant := newTestTenant()
	observer := &fakeObserver{err: errors.New("prometheus unreachable")}
	reconciler, _ := newReconciler(t, tenant, observer)
	reconciler.Metrics = reportedMetrics

	if _, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant)); err == nil {
		t.Fatal("Reconcile returned nil error, want the observer error surfaced")
	}

	if gotError := sampleCountForLabel(t, registry, resultError); gotError != 1 {
		t.Errorf("error bucket sample count = %v, want 1", gotError)
	}
	if gotSuccess := sampleCountForLabel(t, registry, resultSuccess); gotSuccess != 0 {
		t.Errorf("success bucket sample count = %v, want 0", gotSuccess)
	}
}

// drainFakeRecorderEvents collects every event currently buffered on
// recorder.Events without blocking, so tests can assert on the exact count
// and content of events emitted by a single Reconcile call.
func drainFakeRecorderEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

func TestReconcile_FirstTransitionEmitsEventAndMetric(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	recorder := record.NewFakeRecorder(10)

	tenant := newTestTenant()
	// Status.State is the zero value "" on a brand-new tenant, so any
	// computed state counts as a transition worth recording.
	observer := &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12}
	reconciler, _ := newReconciler(t, tenant, observer)
	reconciler.Metrics = reportedMetrics
	reconciler.Recorder = recorder

	if _, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant)); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	events := drainFakeRecorderEvents(recorder)
	if len(events) != 1 {
		t.Fatalf("recorded events = %v, want exactly 1", events)
	}
	wantReason := string(tradingv1alpha1.TradingTenantStateStable)
	if !strings.Contains(events[0], wantReason) {
		t.Errorf("event = %q, want it to contain reason %q", events[0], wantReason)
	}

	if got := sampleCountForIsolationLabel(t, registry, wantReason); got != 1 {
		t.Errorf("isolation transition count for state=%s = %v, want 1", wantReason, got)
	}
}

func TestReconcile_GenuineTransitionEmitsWarningEvent(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	recorder := record.NewFakeRecorder(10)

	tenant := newTestTenant()
	tenant.Status.State = tradingv1alpha1.TradingTenantStateStable
	// lag high, latency normal, partition parity: drives classify to Isolated.
	observer := &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 3}
	reconciler, _ := newReconciler(t, tenant, observer)
	reconciler.Metrics = reportedMetrics
	reconciler.Recorder = recorder

	if _, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant)); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	events := drainFakeRecorderEvents(recorder)
	var sawTransitionEvent bool
	for _, event := range events {
		if strings.HasPrefix(event, "Warning Isolated ") {
			sawTransitionEvent = true
		}
	}
	if !sawTransitionEvent {
		t.Errorf("recorded events = %v, want a Warning event with reason Isolated", events)
	}

	if got := sampleCountForIsolationLabel(t, registry, string(tradingv1alpha1.TradingTenantStateIsolated)); got != 1 {
		t.Errorf("isolation transition count for state=Isolated = %v, want 1", got)
	}
}

func TestReconcile_IdempotentReconcileEmitsNoAdditionalEvents(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	recorder := record.NewFakeRecorder(10)

	tenant := newTestTenant()
	observer := &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12}
	reconciler, _ := newReconciler(t, tenant, observer)
	reconciler.Metrics = reportedMetrics
	reconciler.Recorder = recorder
	request := reconcileRequest(tenant)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	firstPassEvents := drainFakeRecorderEvents(recorder)
	if len(firstPassEvents) != 1 {
		t.Fatalf("events after first Reconcile = %v, want exactly 1", firstPassEvents)
	}
	wantReason := string(tradingv1alpha1.TradingTenantStateStable)
	if got := sampleCountForIsolationLabel(t, registry, wantReason); got != 1 {
		t.Fatalf("isolation transition count for state=%s after first Reconcile = %v, want 1", wantReason, got)
	}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}

	secondPassEvents := drainFakeRecorderEvents(recorder)
	if len(secondPassEvents) != 0 {
		t.Errorf("events after second (unchanged) Reconcile = %v, want none", secondPassEvents)
	}
	if got := sampleCountForIsolationLabel(t, registry, wantReason); got != 1 {
		t.Errorf("isolation transition count for state=%s after second Reconcile = %v, want still 1 (no additional increment)", wantReason, got)
	}
}

func TestReconcile_DedicatedPoolCreatedFiresOnce(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	recorder := record.NewFakeRecorder(10)

	tenant := newTestTenant()
	// Status.State already Isolated and the isolate branch is triggered
	// again below, so the state-transition event never fires here: this
	// test isolates the DedicatedPoolCreated signal from the state one.
	tenant.Status.State = tradingv1alpha1.TradingTenantStateIsolated
	tenant.Spec.Isolation.DedicatedNodePool = false
	observer := &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 3}
	reconciler, _ := newReconciler(t, tenant, observer)
	reconciler.Metrics = reportedMetrics
	reconciler.Recorder = recorder
	request := reconcileRequest(tenant)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}

	firstPassEvents := drainFakeRecorderEvents(recorder)
	if len(firstPassEvents) != 1 {
		t.Fatalf("events after first Reconcile = %v, want exactly 1 (DedicatedPoolCreated)", firstPassEvents)
	}
	if !strings.Contains(firstPassEvents[0], reasonDedicatedPoolCreated) {
		t.Errorf("event = %q, want it to contain reason %q", firstPassEvents[0], reasonDedicatedPoolCreated)
	}
	if got := sampleCountForIsolationLabel(t, registry, reasonDedicatedPoolCreated); got != 1 {
		t.Errorf("isolation transition count for state=%s after first Reconcile = %v, want 1", reasonDedicatedPoolCreated, got)
	}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}

	secondPassEvents := drainFakeRecorderEvents(recorder)
	if len(secondPassEvents) != 0 {
		t.Errorf("events after second Reconcile = %v, want none (spec flag already true)", secondPassEvents)
	}
	if got := sampleCountForIsolationLabel(t, registry, reasonDedicatedPoolCreated); got != 1 {
		t.Errorf("isolation transition count for state=%s after second Reconcile = %v, want still 1", reasonDedicatedPoolCreated, got)
	}
}

func TestReconcile_StatusUpdateFailureSkipsTransitionEvent(t *testing.T) {
	registry := prometheus.NewRegistry()
	reportedMetrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	recorder := record.NewFakeRecorder(10)

	sentinelErr := errors.New("status update rejected")
	tenant := newTestTenant()
	fakeClient := fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithStatusSubresource(&tradingv1alpha1.TradingTenant{}).
		WithObjects(tenant).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, innerClient client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if subResourceName == "status" {
					return sentinelErr
				}
				return innerClient.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()

	reconciler := &TradingTenantReconciler{
		Client:   fakeClient,
		Observer: &fakeObserver{lag: 100, p99Ms: 10, partitionCount: 12},
		Metrics:  reportedMetrics,
		Recorder: recorder,
	}

	_, err = reconciler.Reconcile(context.Background(), reconcileRequest(tenant))
	if err == nil {
		t.Fatal("Reconcile returned nil error, want the status update error surfaced")
	}
	if !errors.Is(err, sentinelErr) {
		t.Errorf("Reconcile error = %v, want it to wrap %v", err, sentinelErr)
	}

	events := drainFakeRecorderEvents(recorder)
	if len(events) != 0 {
		t.Errorf("events after failed status update = %v, want none", events)
	}
	wantReason := string(tradingv1alpha1.TradingTenantStateStable)
	if got := sampleCountForIsolationLabel(t, registry, wantReason); got != 0 {
		t.Errorf("isolation transition count for state=%s after failed status update = %v, want 0", wantReason, got)
	}
}

func TestReconcile_NilRecorderAndMetricsSafe(t *testing.T) {
	tenant := newTestTenant()
	// lag high, latency normal, partition parity: a state-changing pass
	// that would call recordTransition and, on the first pass, the
	// DedicatedPoolCreated path too, exercising both nil-guarded branches.
	observer := &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 3}
	reconciler, _ := newReconciler(t, tenant, observer)
	// reconciler.Recorder and reconciler.Metrics are left at their zero
	// value (nil) deliberately.

	if _, err := reconciler.Reconcile(context.Background(), reconcileRequest(tenant)); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
}
