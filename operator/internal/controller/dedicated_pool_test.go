package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
)

func newDedicatedPoolReconciler(t *testing.T, tenant *tradingv1alpha1.TradingTenant) *TradingTenantReconciler {
	t.Helper()
	reconciler, _ := newReconciler(t, tenant, &fakeObserver{})
	reconciler.IngestionImage = "registry.example/ingestion:v1"
	reconciler.ProcessorImage = "registry.example/processor:v1"
	reconciler.IngestionEnvFrom = []corev1.EnvFromSource{
		{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "shared-config"}}},
	}
	reconciler.ProcessorEnvFrom = []corev1.EnvFromSource{
		{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "shared-config"}}},
	}
	reconciler.DedicatedNodeSelector = map[string]string{"pool": "dedicated"}
	reconciler.DedicatedTolerations = []corev1.Toleration{
		{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule},
	}
	return reconciler
}

func TestEnsureDedicatedPool_CreatesAllResources(t *testing.T) {
	tenant := newTestTenant()
	reconciler := newDedicatedPoolReconciler(t, tenant)
	ctx := context.Background()

	created, err := reconciler.ensureDedicatedPool(ctx, tenant, 5, 3, tenant.Spec.MinReplicas)
	if err != nil {
		t.Fatalf("ensureDedicatedPool returned error: %v", err)
	}
	if !created {
		t.Error("ensureDedicatedPool created = false, want true on first call")
	}

	var configMap corev1.ConfigMap
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedConfigMapName(tenant), Namespace: tenant.Namespace}, &configMap); err != nil {
		t.Fatalf("Get ConfigMap: %v", err)
	}
	if got, want := configMap.Data["TENANT_ID"], tenant.Spec.TenantID; got != want {
		t.Errorf("ConfigMap Data[TENANT_ID] = %q, want %q", got, want)
	}
	if got, want := configMap.Data["KAFKA_MANUAL_PARTITIONS"], "5,6,7"; got != want {
		t.Errorf("ConfigMap Data[KAFKA_MANUAL_PARTITIONS] = %q, want %q", got, want)
	}
	requireSingleOwnerReference(t, tenant, configMap.OwnerReferences, "ConfigMap")

	var ingestionDeployment appsv1.Deployment
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedIngestionName(tenant), Namespace: tenant.Namespace}, &ingestionDeployment); err != nil {
		t.Fatalf("Get ingestion Deployment: %v", err)
	}
	requireSingleOwnerReference(t, tenant, ingestionDeployment.OwnerReferences, "ingestion Deployment")
	if len(ingestionDeployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("ingestion Deployment containers = %v, want exactly 1", ingestionDeployment.Spec.Template.Spec.Containers)
	}
	ingestionContainer := ingestionDeployment.Spec.Template.Spec.Containers[0]
	if ingestionContainer.Image != reconciler.IngestionImage {
		t.Errorf("ingestion container image = %q, want %q", ingestionContainer.Image, reconciler.IngestionImage)
	}
	if len(ingestionContainer.EnvFrom) != len(reconciler.IngestionEnvFrom) {
		t.Errorf("ingestion container envFrom = %v, want %v", ingestionContainer.EnvFrom, reconciler.IngestionEnvFrom)
	}
	if got := ingestionDeployment.Spec.Template.Spec.NodeSelector; got["pool"] != "dedicated" {
		t.Errorf("ingestion Deployment nodeSelector = %v, want pool=dedicated", got)
	}
	if len(ingestionDeployment.Spec.Template.Spec.Tolerations) != 1 {
		t.Errorf("ingestion Deployment tolerations = %v, want exactly 1", ingestionDeployment.Spec.Template.Spec.Tolerations)
	}
	if got, want := *ingestionDeployment.Spec.Replicas, tenant.Spec.MinReplicas; got != want {
		t.Errorf("ingestion Deployment replicas = %d, want %d", got, want)
	}

	var processorDeployment appsv1.Deployment
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedProcessorName(tenant), Namespace: tenant.Namespace}, &processorDeployment); err != nil {
		t.Fatalf("Get processor Deployment: %v", err)
	}
	requireSingleOwnerReference(t, tenant, processorDeployment.OwnerReferences, "processor Deployment")
	if len(processorDeployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("processor Deployment containers = %v, want exactly 1", processorDeployment.Spec.Template.Spec.Containers)
	}
	processorContainer := processorDeployment.Spec.Template.Spec.Containers[0]
	if processorContainer.Image != reconciler.ProcessorImage {
		t.Errorf("processor container image = %q, want %q", processorContainer.Image, reconciler.ProcessorImage)
	}
	// ProcessorEnvFrom (1 entry) plus the dedicated ConfigMap ref appended by
	// ensureDedicatedProcessorDeployment.
	wantProcessorEnvFromLen := len(reconciler.ProcessorEnvFrom) + 1
	if len(processorContainer.EnvFrom) != wantProcessorEnvFromLen {
		t.Fatalf("processor container envFrom = %v, want %d entries", processorContainer.EnvFrom, wantProcessorEnvFromLen)
	}
	lastEnvFrom := processorContainer.EnvFrom[len(processorContainer.EnvFrom)-1]
	if lastEnvFrom.ConfigMapRef == nil || lastEnvFrom.ConfigMapRef.Name != dedicatedConfigMapName(tenant) {
		t.Errorf("processor container's last envFrom = %+v, want ConfigMapRef to %q", lastEnvFrom, dedicatedConfigMapName(tenant))
	}
	if got := processorDeployment.Spec.Template.Spec.NodeSelector; got["pool"] != "dedicated" {
		t.Errorf("processor Deployment nodeSelector = %v, want pool=dedicated", got)
	}
	if len(processorDeployment.Spec.Template.Spec.Tolerations) != 1 {
		t.Errorf("processor Deployment tolerations = %v, want exactly 1", processorDeployment.Spec.Template.Spec.Tolerations)
	}

	var service corev1.Service
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedServiceName(tenant), Namespace: tenant.Namespace}, &service); err != nil {
		t.Fatalf("Get Service: %v", err)
	}
	requireSingleOwnerReference(t, tenant, service.OwnerReferences, "Service")
	wantSelector := dedicatedLabels(tenant, componentIngestion)
	if len(service.Spec.Selector) != len(wantSelector) {
		t.Errorf("Service selector = %v, want %v", service.Spec.Selector, wantSelector)
	}
	for key, value := range wantSelector {
		if service.Spec.Selector[key] != value {
			t.Errorf("Service selector[%q] = %q, want %q", key, service.Spec.Selector[key], value)
		}
	}
	if len(service.Spec.Ports) != 2 {
		t.Fatalf("Service ports = %v, want exactly 2", service.Spec.Ports)
	}
	gotPorts := map[string]int32{}
	for _, port := range service.Spec.Ports {
		gotPorts[port.Name] = port.Port
	}
	if gotPorts["http"] != ingestionHTTPPort {
		t.Errorf("Service http port = %d, want %d", gotPorts["http"], ingestionHTTPPort)
	}
	if gotPorts["grpc"] != ingestionGRPCPort {
		t.Errorf("Service grpc port = %d, want %d", gotPorts["grpc"], ingestionGRPCPort)
	}
}

// requireSingleOwnerReference confirms exactly one owner reference points
// back at tenant, the contract ensureDedicatedPool's four objects all rely
// on for garbage collection when the TradingTenant itself is deleted.
func requireSingleOwnerReference(t *testing.T, tenant *tradingv1alpha1.TradingTenant, owners []metav1.OwnerReference, objectDescription string) {
	t.Helper()
	if len(owners) != 1 {
		t.Fatalf("%s OwnerReferences = %v, want exactly 1", objectDescription, owners)
	}
	if owners[0].Name != tenant.Name || owners[0].Kind != "TradingTenant" {
		t.Errorf("%s owner reference = %+v, want it to reference TradingTenant %q", objectDescription, owners[0], tenant.Name)
	}
}

// TestEnsureDedicatedPool_IdempotentOnSecondCall confirms the get-or-create
// pattern this issue's acceptance criteria calls for: a second call with the
// same tenant and observed partition range reports created=false and leaves
// every object's ResourceVersion unchanged (no spurious Update).
func TestEnsureDedicatedPool_IdempotentOnSecondCall(t *testing.T) {
	tenant := newTestTenant()
	reconciler := newDedicatedPoolReconciler(t, tenant)
	ctx := context.Background()

	if _, err := reconciler.ensureDedicatedPool(ctx, tenant, 5, 3, tenant.Spec.MinReplicas); err != nil {
		t.Fatalf("first ensureDedicatedPool returned error: %v", err)
	}

	before := map[string]string{
		"configmap": getResourceVersion(t, ctx, reconciler, &corev1.ConfigMap{}, dedicatedConfigMapName(tenant), tenant.Namespace),
		"ingestion": getResourceVersion(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedIngestionName(tenant), tenant.Namespace),
		"processor": getResourceVersion(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedProcessorName(tenant), tenant.Namespace),
		"service":   getResourceVersion(t, ctx, reconciler, &corev1.Service{}, dedicatedServiceName(tenant), tenant.Namespace),
	}

	created, err := reconciler.ensureDedicatedPool(ctx, tenant, 5, 3, tenant.Spec.MinReplicas)
	if err != nil {
		t.Fatalf("second ensureDedicatedPool returned error: %v", err)
	}
	if created {
		t.Error("ensureDedicatedPool created = true on second call, want false")
	}

	after := map[string]string{
		"configmap": getResourceVersion(t, ctx, reconciler, &corev1.ConfigMap{}, dedicatedConfigMapName(tenant), tenant.Namespace),
		"ingestion": getResourceVersion(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedIngestionName(tenant), tenant.Namespace),
		"processor": getResourceVersion(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedProcessorName(tenant), tenant.Namespace),
		"service":   getResourceVersion(t, ctx, reconciler, &corev1.Service{}, dedicatedServiceName(tenant), tenant.Namespace),
	}

	for key, beforeVersion := range before {
		if after[key] != beforeVersion {
			t.Errorf("%s ResourceVersion changed on idempotent call: %q -> %q", key, beforeVersion, after[key])
		}
	}
}

// TestEnsureDedicatedPool_PreservesServerDefaultedFields simulates a real
// API server's defaulting of appsv1.DeploymentSpec fields (Strategy here)
// that a fake client never applies, then confirms a second
// ensureDedicatedPool call doesn't churn: the mutate closure must only touch
// the fields this reconciler owns, never overwrite Spec wholesale, or the
// defaulted field gets zeroed and CreateOrUpdate issues a spurious Update on
// every reconcile pass against a real cluster.
func TestEnsureDedicatedPool_PreservesServerDefaultedFields(t *testing.T) {
	tenant := newTestTenant()
	reconciler := newDedicatedPoolReconciler(t, tenant)
	ctx := context.Background()

	if _, err := reconciler.ensureDedicatedPool(ctx, tenant, 5, 3, tenant.Spec.MinReplicas); err != nil {
		t.Fatalf("first ensureDedicatedPool returned error: %v", err)
	}

	var ingestionDeployment appsv1.Deployment
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedIngestionName(tenant), Namespace: tenant.Namespace}, &ingestionDeployment); err != nil {
		t.Fatalf("Get ingestion Deployment: %v", err)
	}
	// A real API server defaults an unset Deployment Strategy to
	// RollingUpdate; the fake client used elsewhere in these tests doesn't,
	// which is exactly why this bug was invisible against it.
	ingestionDeployment.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType}
	if err := reconciler.Update(ctx, &ingestionDeployment); err != nil {
		t.Fatalf("Update to simulate server-defaulted Strategy: %v", err)
	}
	beforeVersion := ingestionDeployment.ResourceVersion

	result, err := reconciler.ensureDedicatedIngestionDeployment(ctx, tenant, tenant.Spec.MinReplicas)
	if err != nil {
		t.Fatalf("second ensureDedicatedIngestionDeployment returned error: %v", err)
	}
	if result != controllerutil.OperationResultNone {
		t.Errorf("ensureDedicatedIngestionDeployment result = %v, want OperationResultNone (no churn)", result)
	}

	var afterDeployment appsv1.Deployment
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedIngestionName(tenant), Namespace: tenant.Namespace}, &afterDeployment); err != nil {
		t.Fatalf("Get ingestion Deployment after second call: %v", err)
	}
	if afterDeployment.ResourceVersion != beforeVersion {
		t.Errorf("ingestion Deployment ResourceVersion changed: %q -> %q, want unchanged", beforeVersion, afterDeployment.ResourceVersion)
	}
	if afterDeployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType {
		t.Errorf("ingestion Deployment Spec.Strategy.Type = %q, want preserved %q", afterDeployment.Spec.Strategy.Type, appsv1.RollingUpdateDeploymentStrategyType)
	}
}

// TestEnsureDedicatedPool_UsesDecisionReplicasNotMinReplicas confirms the
// dedicated Deployments are provisioned with the caller-supplied replicas
// count, not tenant.Spec.MinReplicas — a tenant isolated after already being
// scaled above MinReplicas (classify's lagHigh && !latencyHigh branch, which
// sets replicas: baseline) must not be silently scaled back down to
// MinReplicas by ensureDedicatedPool.
func TestEnsureDedicatedPool_UsesDecisionReplicasNotMinReplicas(t *testing.T) {
	tenant := newTestTenant()
	tenant.Spec.MinReplicas = 2
	const decidedReplicas int32 = 7
	reconciler := newDedicatedPoolReconciler(t, tenant)
	ctx := context.Background()

	if _, err := reconciler.ensureDedicatedPool(ctx, tenant, 5, 3, decidedReplicas); err != nil {
		t.Fatalf("ensureDedicatedPool returned error: %v", err)
	}

	var ingestionDeployment appsv1.Deployment
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedIngestionName(tenant), Namespace: tenant.Namespace}, &ingestionDeployment); err != nil {
		t.Fatalf("Get ingestion Deployment: %v", err)
	}
	if ingestionDeployment.Spec.Replicas == nil || *ingestionDeployment.Spec.Replicas != decidedReplicas {
		t.Errorf("ingestion Deployment replicas = %v, want %d (decision.replicas, not MinReplicas %d)", ingestionDeployment.Spec.Replicas, decidedReplicas, tenant.Spec.MinReplicas)
	}

	var processorDeployment appsv1.Deployment
	if err := reconciler.Get(ctx, types.NamespacedName{Name: dedicatedProcessorName(tenant), Namespace: tenant.Namespace}, &processorDeployment); err != nil {
		t.Fatalf("Get processor Deployment: %v", err)
	}
	if processorDeployment.Spec.Replicas == nil || *processorDeployment.Spec.Replicas != decidedReplicas {
		t.Errorf("processor Deployment replicas = %v, want %d (decision.replicas, not MinReplicas %d)", processorDeployment.Spec.Replicas, decidedReplicas, tenant.Spec.MinReplicas)
	}
}

// getResourceVersion fetches obj by name/namespace and returns its
// ResourceVersion, the signal a client.Object's Update was (or wasn't)
// actually invoked against the fake client's tracker.
func getResourceVersion(t *testing.T, ctx context.Context, reconciler *TradingTenantReconciler, obj client.Object, name, namespace string) string {
	t.Helper()
	if err := reconciler.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, obj); err != nil {
		t.Fatalf("Get %T %s/%s: %v", obj, namespace, name, err)
	}
	return obj.GetResourceVersion()
}

// assertNotFound confirms obj no longer exists by name/namespace.
func assertNotFound(t *testing.T, ctx context.Context, reconciler *TradingTenantReconciler, obj client.Object, name, namespace string) {
	t.Helper()
	err := reconciler.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, obj)
	if err == nil {
		t.Errorf("%T %s/%s still exists, want it deleted", obj, namespace, name)
		return
	}
	if !apierrors.IsNotFound(err) {
		t.Errorf("Get %T %s/%s returned error = %v, want a NotFound error", obj, namespace, name, err)
	}
}

func TestTearDownDedicatedPool_DeletesAllResources(t *testing.T) {
	tenant := newTestTenant()
	reconciler := newDedicatedPoolReconciler(t, tenant)
	ctx := context.Background()

	if _, err := reconciler.ensureDedicatedPool(ctx, tenant, 5, 3, tenant.Spec.MinReplicas); err != nil {
		t.Fatalf("ensureDedicatedPool returned error: %v", err)
	}

	deleted, err := reconciler.tearDownDedicatedPool(ctx, tenant)
	if err != nil {
		t.Fatalf("tearDownDedicatedPool returned error: %v", err)
	}
	if !deleted {
		t.Error("tearDownDedicatedPool deleted = false, want true")
	}

	assertNotFound(t, ctx, reconciler, &corev1.ConfigMap{}, dedicatedConfigMapName(tenant), tenant.Namespace)
	assertNotFound(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedIngestionName(tenant), tenant.Namespace)
	assertNotFound(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedProcessorName(tenant), tenant.Namespace)
	assertNotFound(t, ctx, reconciler, &corev1.Service{}, dedicatedServiceName(tenant), tenant.Namespace)
}

func TestTearDownDedicatedPool_IdempotentWhenAlreadyGone(t *testing.T) {
	tenant := newTestTenant()
	reconciler := newDedicatedPoolReconciler(t, tenant)
	ctx := context.Background()

	deleted, err := reconciler.tearDownDedicatedPool(ctx, tenant)
	if err != nil {
		t.Fatalf("tearDownDedicatedPool on a never-provisioned tenant returned error: %v", err)
	}
	if deleted {
		t.Error("tearDownDedicatedPool deleted = true, want false when nothing exists")
	}

	if _, err := reconciler.ensureDedicatedPool(ctx, tenant, 5, 3, tenant.Spec.MinReplicas); err != nil {
		t.Fatalf("ensureDedicatedPool returned error: %v", err)
	}
	if _, err := reconciler.tearDownDedicatedPool(ctx, tenant); err != nil {
		t.Fatalf("first tearDownDedicatedPool returned error: %v", err)
	}

	deleted, err = reconciler.tearDownDedicatedPool(ctx, tenant)
	if err != nil {
		t.Fatalf("second tearDownDedicatedPool returned error: %v", err)
	}
	if deleted {
		t.Error("second tearDownDedicatedPool deleted = true, want false (already gone)")
	}
}

// TestReconcile_DedicatedPoolLifecycle drives a tenant into isolation
// through Reconcile and confirms the dedicated resources appear, then
// reverts spec.isolation.dedicatedNodePool by hand (the only path that
// clears it, since isolation never auto-reverts) and confirms a further
// Reconcile tears them down.
func TestReconcile_DedicatedPoolLifecycle(t *testing.T) {
	tenant := newTestTenant()
	observer := &fakeObserver{lag: 2000, p99Ms: 10, partitionCount: 3, partitionStart: 5}
	reconciler, fakeClient := newReconciler(t, tenant, observer)
	reconciler.IngestionImage = "registry.example/ingestion:v1"
	reconciler.ProcessorImage = "registry.example/processor:v1"
	request := reconcileRequest(tenant)
	ctx := context.Background()

	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}

	var afterIsolate tradingv1alpha1.TradingTenant
	if err := fakeClient.Get(ctx, request.NamespacedName, &afterIsolate); err != nil {
		t.Fatalf("Get after first Reconcile: %v", err)
	}
	if afterIsolate.Status.State != tradingv1alpha1.TradingTenantStateIsolated {
		t.Fatalf("status.state = %q, want %q", afterIsolate.Status.State, tradingv1alpha1.TradingTenantStateIsolated)
	}
	if !afterIsolate.Spec.Isolation.DedicatedNodePool {
		t.Fatal("spec.isolation.dedicatedNodePool = false, want true")
	}

	var configMap corev1.ConfigMap
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: dedicatedConfigMapName(&afterIsolate), Namespace: tenant.Namespace}, &configMap); err != nil {
		t.Fatalf("dedicated ConfigMap not found after isolation Reconcile: %v", err)
	}
	if got, want := configMap.Data["KAFKA_MANUAL_PARTITIONS"], "5,6,7"; got != want {
		t.Errorf("ConfigMap Data[KAFKA_MANUAL_PARTITIONS] = %q, want %q", got, want)
	}
	var ingestionDeployment appsv1.Deployment
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: dedicatedIngestionName(&afterIsolate), Namespace: tenant.Namespace}, &ingestionDeployment); err != nil {
		t.Fatalf("dedicated ingestion Deployment not found after isolation Reconcile: %v", err)
	}
	var processorDeployment appsv1.Deployment
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: dedicatedProcessorName(&afterIsolate), Namespace: tenant.Namespace}, &processorDeployment); err != nil {
		t.Fatalf("dedicated processor Deployment not found after isolation Reconcile: %v", err)
	}
	var service corev1.Service
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: dedicatedServiceName(&afterIsolate), Namespace: tenant.Namespace}, &service); err != nil {
		t.Fatalf("dedicated Service not found after isolation Reconcile: %v", err)
	}

	afterIsolate.Spec.Isolation.DedicatedNodePool = false
	if err := fakeClient.Update(ctx, &afterIsolate); err != nil {
		t.Fatalf("manual revert Update: %v", err)
	}
	// Also stop reporting isolate-triggering signals: otherwise
	// decision.setDedicatedNodePool would immediately flip the just-reverted
	// spec flag back to true (isolation never auto-reverts, but it also
	// never auto-re-isolates off a stale spec flag that was never touched by
	// classify — see the !tenant.Spec.Isolation.DedicatedNodePool guard in
	// Reconcile), masking the teardown path this test means to exercise.
	observer.lag = 100
	observer.p99Ms = 10
	observer.partitionCount = 12

	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatalf("second Reconcile (after manual revert) returned error: %v", err)
	}

	assertNotFound(t, ctx, reconciler, &corev1.ConfigMap{}, dedicatedConfigMapName(&afterIsolate), tenant.Namespace)
	assertNotFound(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedIngestionName(&afterIsolate), tenant.Namespace)
	assertNotFound(t, ctx, reconciler, &appsv1.Deployment{}, dedicatedProcessorName(&afterIsolate), tenant.Namespace)
	assertNotFound(t, ctx, reconciler, &corev1.Service{}, dedicatedServiceName(&afterIsolate), tenant.Namespace)
}

func TestPartitionListString(t *testing.T) {
	tests := []struct {
		name  string
		start int32
		count int32
		want  string
	}{
		{name: "typical range", start: 3, count: 3, want: "3,4,5"},
		{name: "zero count is empty", start: 3, count: 0, want: ""},
		{name: "negative count is empty", start: 3, count: -1, want: ""},
		{name: "single partition", start: 0, count: 1, want: "0"},
		{name: "zero start", start: 0, count: 2, want: "0,1"},
		{name: "negative start still enumerates count partitions", start: -2, count: 3, want: "-2,-1,0"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := partitionListString(testCase.start, testCase.count); got != testCase.want {
				t.Errorf("partitionListString(%d, %d) = %q, want %q", testCase.start, testCase.count, got, testCase.want)
			}
		})
	}
}
