package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
