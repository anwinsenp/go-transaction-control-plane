package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TradingTenantState is the coarse-grained reconcile outcome for a
// TradingTenant, as classified by the joint Kafka-lag/P99-latency decision
// table in docs/DESIGN-operator.md.
// +kubebuilder:validation:Enum=Stable;Scaling;Isolated;Degraded
type TradingTenantState string

const (
	// TradingTenantStateStable means both lag and latency are within
	// threshold; no action was taken on the last reconcile pass.
	TradingTenantStateStable TradingTenantState = "Stable"
	// TradingTenantStateScaling means the reconciler adjusted (or attempted
	// to adjust, subject to maxReplicas/observedPartitionCount) replicas.
	TradingTenantStateScaling TradingTenantState = "Scaling"
	// TradingTenantStateIsolated means the tenant was moved to a dedicated
	// node pool because it was at partition parity with no scaling headroom
	// left. This state does not auto-revert.
	TradingTenantStateIsolated TradingTenantState = "Isolated"
	// TradingTenantStateDegraded means latency is high while lag is normal,
	// indicating a likely downstream bottleneck that scaling won't fix.
	TradingTenantStateDegraded TradingTenantState = "Degraded"
)

// ResourceRequirements is the per-pod CPU/memory request for a tenant's
// processor replicas.
type ResourceRequirements struct {
	// CPURequest is the per-pod CPU request, as a Kubernetes quantity
	// (e.g. "500m").
	// +kubebuilder:validation:Required
	CPURequest resource.Quantity `json:"cpuRequest"`

	// MemoryRequest is the per-pod memory request, as a Kubernetes quantity
	// (e.g. "512Mi").
	// +kubebuilder:validation:Required
	MemoryRequest resource.Quantity `json:"memoryRequest"`
}

// ScalingPolicy defines the thresholds the reconciler classifies observed
// Kafka lag and processor P99 latency against.
type ScalingPolicy struct {
	// KafkaLagThreshold is the consumer lag (in messages) above which lag
	// is considered "high" for this tenant.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	KafkaLagThreshold int64 `json:"kafkaLagThreshold"`

	// P99LatencyThresholdMs is the processor P99 latency (in milliseconds)
	// above which latency is considered "high" for this tenant.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	P99LatencyThresholdMs int32 `json:"p99LatencyThresholdMs"`
}

// IsolationPolicy carries the operator-owned node-pool isolation flag.
type IsolationPolicy struct {
	// DedicatedNodePool is set to true by the reconciler when the isolation
	// branch fires (lag high, latency normal, at partition parity). It is
	// not user-editable: a value supplied at creation time has no effect,
	// and the reconciler is the sole writer for this field. It is reset to
	// false only through a deliberate manual operational action, never
	// automatically by the reconciler, to avoid flapping a tenant on and
	// off a dedicated node pool whose metrics sit near the threshold.
	// +kubebuilder:validation:Optional
	DedicatedNodePool bool `json:"dedicatedNodePool"`
}

// TradingTenantSpec defines the desired state of a TradingTenant.
// +kubebuilder:validation:XValidation:rule="self.maxReplicas >= self.minReplicas",message="maxReplicas must be greater than or equal to minReplicas"
type TradingTenantSpec struct {
	// TenantID identifies the institutional client this resource represents.
	// Immutable after creation.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="tenantID is immutable"
	TenantID string `json:"tenantID"`

	// MinReplicas is the lower bound for status.currentReplicas.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	MinReplicas int32 `json:"minReplicas"`

	// MaxReplicas is the upper bound the scale-up branch must respect.
	// Must be greater than or equal to MinReplicas.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas"`

	// Resources is the per-pod CPU/memory request for this tenant's
	// processor replicas.
	// +kubebuilder:validation:Required
	Resources ResourceRequirements `json:"resources"`

	// Scaling holds the lag/latency thresholds the reconciler classifies
	// observed signals against.
	// +kubebuilder:validation:Required
	Scaling ScalingPolicy `json:"scaling"`

	// Isolation carries the operator-owned dedicated-node-pool flag.
	// +kubebuilder:validation:Optional
	Isolation IsolationPolicy `json:"isolation"`
}

// TradingTenantStatus defines the observed state of a TradingTenant, as
// populated by the reconciler on each pass.
type TradingTenantStatus struct {
	// CurrentReplicas is the last replica count set by the reconciler.
	// +kubebuilder:validation:Optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// State is the reconciler's most recent classification for this
	// tenant. See the decision table in docs/DESIGN-operator.md.
	// +kubebuilder:validation:Optional
	State TradingTenantState `json:"state,omitempty"`

	// ObservedKafkaLag is the most recently observed consumer lag (in
	// messages) for this tenant's partitions, as scraped from Prometheus.
	// +kubebuilder:validation:Optional
	ObservedKafkaLag int64 `json:"observedKafkaLag,omitempty"`

	// ObservedP99Ms is the most recently observed processor P99 latency
	// (in milliseconds) for this tenant, as scraped from Prometheus.
	// +kubebuilder:validation:Optional
	ObservedP99Ms int32 `json:"observedP99Ms,omitempty"`

	// ObservedPartitionCount is the tenant's Kafka topic partition count,
	// most recently observed. Used against CurrentReplicas to determine
	// whether there is still Kafka-side parallelism headroom before
	// falling back to isolation.
	// +kubebuilder:validation:Optional
	ObservedPartitionCount int32 `json:"observedPartitionCount,omitempty"`

	// LastReconcileTime is the timestamp of the most recent reconcile
	// pass, updated every pass regardless of whether action was taken.
	// +kubebuilder:validation:Optional
	LastReconcileTime metav1.Time `json:"lastReconcileTime,omitempty"`
}

// TradingTenant represents one institutional trading client on the
// platform. See docs/DESIGN-operator.md for the reconcile decision table.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.currentReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type TradingTenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TradingTenantSpec   `json:"spec,omitempty"`
	Status TradingTenantStatus `json:"status,omitempty"`
}

// TradingTenantList contains a list of TradingTenant.
// +kubebuilder:object:root=true
type TradingTenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TradingTenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TradingTenant{}, &TradingTenantList{})
}
