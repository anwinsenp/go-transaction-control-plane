package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
)

const (
	componentIngestion = "ingestion"
	componentProcessor = "processor"

	labelTenant             = "controlplane.anwinsenp.dev/tenant"
	labelDedicated          = "controlplane.anwinsenp.dev/dedicated"
	labelComponent          = "controlplane.anwinsenp.dev/component"
	ingestionHTTPPort int32 = 8080
	ingestionGRPCPort int32 = 9090

	// tenantPartitionMapVolumeName, tenantPartitionMapMountDir, and
	// tenantPartitionMapMountPath wire the shared tenant-partition-map
	// ConfigMap (see tenant_partition_map.go, ADR 0007 part 3) into a
	// dedicated ingestion/processor pod as a projected file, so
	// internal/ingestion/kafka.FileTenantPartitionSource and
	// internal/processor/kafka.FileTenantPartitionSource can poll it.
	// TENANT_PARTITION_MAP_PATH (set on each dedicated container below)
	// points at tenantPartitionMapMountPath.
	tenantPartitionMapVolumeName = "tenant-partition-map"
	tenantPartitionMapMountDir   = "/etc/tenant-partition-map"
	tenantPartitionMapMountPath  = tenantPartitionMapMountDir + "/" + tenantPartitionMapDataKey
)

// tenantPartitionMapVolume and tenantPartitionMapVolumeMount are shared by
// both dedicated Deployments so the shared ConfigMap is projected into
// each pod identically.
func tenantPartitionMapVolume() corev1.Volume {
	optional := true
	return corev1.Volume{
		Name: tenantPartitionMapVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: tenantPartitionMapName},
				// Optional: ensureTenantPartitionMapEntry runs after
				// ensureDedicatedPool in Reconcile (see
				// tradingtenant_controller.go), so the very first tenant to
				// isolate in a namespace can have its dedicated pods
				// scheduled before the shared ConfigMap exists. A missing
				// mount is treated the same as a missing file by
				// FileTenantPartitionSource's reload fallback.
				Optional: &optional,
			},
		},
	}
}

func tenantPartitionMapVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: tenantPartitionMapVolumeName, MountPath: tenantPartitionMapMountDir, ReadOnly: true}
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// dedicatedIngestionName, dedicatedProcessorName, dedicatedServiceName, and
// dedicatedConfigMapName derive deterministic, namespaced names for an
// isolated tenant's dedicated resources from the owning TradingTenant's own
// name, so ensureDedicatedPool/tearDownDedicatedPool can get-or-create and
// get-or-delete them by name without tracking references in status.
func dedicatedIngestionName(tenant *tradingv1alpha1.TradingTenant) string {
	return tenant.Name + "-dedicated-ingestion"
}

func dedicatedProcessorName(tenant *tradingv1alpha1.TradingTenant) string {
	return tenant.Name + "-dedicated-processor"
}

func dedicatedServiceName(tenant *tradingv1alpha1.TradingTenant) string {
	return tenant.Name + "-dedicated"
}

func dedicatedConfigMapName(tenant *tradingv1alpha1.TradingTenant) string {
	return tenant.Name + "-dedicated-partitions"
}

// dedicatedLabels builds the label set applied to a dedicated resource and,
// for Deployments, doubles as their pod template labels / selector — so a
// Deployment's Selector, computed once from this function, matches its own
// pods forever, satisfying apps/v1's immutable-selector requirement across
// repeated idempotent reconcile passes. component is left empty for
// resources that aren't scoped to one specific Deployment's pods (the
// ConfigMap, and the aggregate label set applied to every dedicated
// object).
func dedicatedLabels(tenant *tradingv1alpha1.TradingTenant, component string) map[string]string {
	labels := map[string]string{
		labelTenant:    tenant.Spec.TenantID,
		labelDedicated: "true",
	}
	if component != "" {
		labels[labelComponent] = component
	}
	return labels
}

// partitionListString renders a tenant's reserved partition range
// [start, start+count) as a comma-separated list of partition indices, the
// format internal/processor/kafka.ConfigFromEnv's KAFKA_MANUAL_PARTITIONS
// parser expects. count<=0 returns "", which ConfigFromEnv treats as unset
// (falling back to consumer-group mode) rather than "reserve zero
// partitions" — accepted rather than guarded against here because an
// isolated tenant's observed partition count is sourced from Prometheus and
// should never be zero for a tenant that's actually publishing.
func partitionListString(start, count int32) string {
	if count <= 0 {
		return ""
	}
	partitions := make([]string, count)
	for index := int32(0); index < count; index++ {
		partitions[index] = strconv.Itoa(int(start + index))
	}
	return strings.Join(partitions, ",")
}

func int32Ptr(value int32) *int32 {
	return &value
}

// ensureDedicatedPool get-or-creates the dedicated ConfigMap, ingestion
// Deployment, processor Deployment, and Service for an isolated tenant
// (ADR 0007, part 2). It is idempotent: calling it repeatedly against the
// same tenant and observed partition range is a no-op after the first
// call. The returned bool reports whether any resource was newly created,
// so the caller can decide whether to emit a provisioning Event.
func (r *TradingTenantReconciler) ensureDedicatedPool(ctx context.Context, tenant *tradingv1alpha1.TradingTenant, partitionStart, partitionCount, replicas int32) (bool, error) {
	configMapResult, err := r.ensureDedicatedConfigMap(ctx, tenant, partitionStart, partitionCount)
	if err != nil {
		return false, fmt.Errorf("dedicated configmap: %w", err)
	}

	ingestionResult, err := r.ensureDedicatedIngestionDeployment(ctx, tenant, replicas)
	if err != nil {
		return false, fmt.Errorf("dedicated ingestion deployment: %w", err)
	}

	processorResult, err := r.ensureDedicatedProcessorDeployment(ctx, tenant, replicas)
	if err != nil {
		return false, fmt.Errorf("dedicated processor deployment: %w", err)
	}

	serviceResult, err := r.ensureDedicatedService(ctx, tenant)
	if err != nil {
		return false, fmt.Errorf("dedicated service: %w", err)
	}

	anyCreated := configMapResult == controllerutil.OperationResultCreated ||
		ingestionResult == controllerutil.OperationResultCreated ||
		processorResult == controllerutil.OperationResultCreated ||
		serviceResult == controllerutil.OperationResultCreated
	return anyCreated, nil
}

func (r *TradingTenantReconciler) ensureDedicatedConfigMap(ctx context.Context, tenant *tradingv1alpha1.TradingTenant, partitionStart, partitionCount int32) (controllerutil.OperationResult, error) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dedicatedConfigMapName(tenant),
			Namespace: tenant.Namespace,
		},
	}

	return controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		if err := ctrl.SetControllerReference(tenant, configMap, r.Scheme); err != nil {
			return fmt.Errorf("set controller reference: %w", err)
		}
		configMap.Labels = dedicatedLabels(tenant, "")
		configMap.Data = map[string]string{
			"TENANT_ID":               tenant.Spec.TenantID,
			"KAFKA_MANUAL_PARTITIONS": partitionListString(partitionStart, partitionCount),
		}
		return nil
	})
}

func (r *TradingTenantReconciler) ensureDedicatedIngestionDeployment(ctx context.Context, tenant *tradingv1alpha1.TradingTenant, replicas int32) (controllerutil.OperationResult, error) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dedicatedIngestionName(tenant),
			Namespace: tenant.Namespace,
		},
	}
	labels := dedicatedLabels(tenant, componentIngestion)

	return controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if err := ctrl.SetControllerReference(tenant, deployment, r.Scheme); err != nil {
			return fmt.Errorf("set controller reference: %w", err)
		}
		deployment.Labels = labels
		// Mutate only the fields this reconciler owns rather than replacing
		// deployment.Spec wholesale: CreateOrUpdate fetches the existing object
		// into deployment first, so on an update pass Spec already carries
		// API-server-defaulted fields (Strategy, RestartPolicy, DNSPolicy,
		// etc.) that a full-struct overwrite would zero out, causing an
		// Update on every reconcile pass instead of only when something this
		// reconciler owns actually changed.
		if deployment.Spec.Selector == nil {
			deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		}
		deployment.Spec.Replicas = int32Ptr(replicas)
		deployment.Spec.Template.Labels = labels
		deployment.Spec.Template.Spec.NodeSelector = r.DedicatedNodeSelector
		deployment.Spec.Template.Spec.Tolerations = r.DedicatedTolerations
		deployment.Spec.Template.Spec.Volumes = []corev1.Volume{tenantPartitionMapVolume()}
		deployment.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "ingestion",
				Image:   r.IngestionImage,
				EnvFrom: r.IngestionEnvFrom,
				Env: []corev1.EnvVar{
					{Name: "TENANT_PARTITION_MAP_PATH", Value: tenantPartitionMapMountPath},
				},
				VolumeMounts: []corev1.VolumeMount{tenantPartitionMapVolumeMount()},
				Ports: []corev1.ContainerPort{
					{Name: "http", ContainerPort: ingestionHTTPPort},
					{Name: "grpc", ContainerPort: ingestionGRPCPort},
				},
			},
		}
		return nil
	})
}

// ensureDedicatedProcessorDeployment get-or-creates the dedicated
// processor Deployment. Its container's EnvFrom includes the dedicated
// ConfigMap (see ensureDedicatedConfigMap), which supplies TENANT_ID and
// KAFKA_MANUAL_PARTITIONS — internal/processor/kafka.ConfigFromEnv reads
// KAFKA_MANUAL_PARTITIONS to switch the consumer from consumer-group
// subscription to manual partition assignment scoped to exactly this
// tenant's reserved range at startup, and TENANT_ID plus the mounted
// tenant-partition-map ConfigMap (see tenantPartitionMapVolume, ADR 0007
// part 3) to keep that assignment current afterward without a restart.
func (r *TradingTenantReconciler) ensureDedicatedProcessorDeployment(ctx context.Context, tenant *tradingv1alpha1.TradingTenant, replicas int32) (controllerutil.OperationResult, error) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dedicatedProcessorName(tenant),
			Namespace: tenant.Namespace,
		},
	}
	labels := dedicatedLabels(tenant, componentProcessor)

	envFrom := make([]corev1.EnvFromSource, 0, len(r.ProcessorEnvFrom)+1)
	envFrom = append(envFrom, r.ProcessorEnvFrom...)
	envFrom = append(envFrom, corev1.EnvFromSource{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: dedicatedConfigMapName(tenant)},
		},
	})

	return controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if err := ctrl.SetControllerReference(tenant, deployment, r.Scheme); err != nil {
			return fmt.Errorf("set controller reference: %w", err)
		}
		deployment.Labels = labels
		// See the matching comment in ensureDedicatedIngestionDeployment: only
		// touch the fields this reconciler owns, never replace Spec wholesale.
		if deployment.Spec.Selector == nil {
			deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		}
		deployment.Spec.Replicas = int32Ptr(replicas)
		deployment.Spec.Template.Labels = labels
		deployment.Spec.Template.Spec.NodeSelector = r.DedicatedNodeSelector
		deployment.Spec.Template.Spec.Tolerations = r.DedicatedTolerations
		deployment.Spec.Template.Spec.Volumes = []corev1.Volume{tenantPartitionMapVolume()}
		deployment.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "processor",
				Image:   r.ProcessorImage,
				EnvFrom: envFrom,
				Env: []corev1.EnvVar{
					{Name: "TENANT_PARTITION_MAP_PATH", Value: tenantPartitionMapMountPath},
				},
				VolumeMounts: []corev1.VolumeMount{tenantPartitionMapVolumeMount()},
			},
		}
		return nil
	})
}

// ensureDedicatedService get-or-creates the Service fronting the dedicated
// ingestion Deployment's pods (the processor has no inbound traffic to
// route to — it's a Kafka consumer, not a server other components call).
func (r *TradingTenantReconciler) ensureDedicatedService(ctx context.Context, tenant *tradingv1alpha1.TradingTenant) (controllerutil.OperationResult, error) {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dedicatedServiceName(tenant),
			Namespace: tenant.Namespace,
		},
	}
	selector := dedicatedLabels(tenant, componentIngestion)

	return controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := ctrl.SetControllerReference(tenant, service, r.Scheme); err != nil {
			return fmt.Errorf("set controller reference: %w", err)
		}
		service.Labels = dedicatedLabels(tenant, "")
		service.Spec.Selector = selector
		service.Spec.Ports = []corev1.ServicePort{
			{Name: "http", Port: ingestionHTTPPort, TargetPort: intstr.FromInt32(ingestionHTTPPort)},
			{Name: "grpc", Port: ingestionGRPCPort, TargetPort: intstr.FromInt32(ingestionGRPCPort)},
		}
		return nil
	})
}

// tearDownDedicatedPool deletes the dedicated ConfigMap, ingestion
// Deployment, processor Deployment, and Service for a tenant whose
// isolation was manually reverted (spec.isolation.dedicatedNodePool never
// auto-reverts — see docs/DESIGN-operator.md — so this is the only path
// that cleans them up). It's idempotent: a resource that's already gone is
// skipped, not an error. The returned bool reports whether any resource
// was actually deleted, so the caller can decide whether to emit a
// teardown Event.
func (r *TradingTenantReconciler) tearDownDedicatedPool(ctx context.Context, tenant *tradingv1alpha1.TradingTenant) (bool, error) {
	objects := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: dedicatedIngestionName(tenant), Namespace: tenant.Namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: dedicatedProcessorName(tenant), Namespace: tenant.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: dedicatedServiceName(tenant), Namespace: tenant.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: dedicatedConfigMapName(tenant), Namespace: tenant.Namespace}},
	}

	anyDeleted := false
	for _, object := range objects {
		if err := r.Delete(ctx, object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete %T %s/%s: %w", object, tenant.Namespace, object.GetName(), err)
		}
		anyDeleted = true
	}

	return anyDeleted, nil
}
