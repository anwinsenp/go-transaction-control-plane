package v1alpha1

import (
	"encoding/json"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newPopulatedTenant() *TradingTenant {
	return &TradingTenant{
		Spec: TradingTenantSpec{
			TenantID:    "tenant-alpha",
			MinReplicas: 2,
			MaxReplicas: 8,
			Resources: ResourceRequirements{
				CPURequest:    resource.MustParse("500m"),
				MemoryRequest: resource.MustParse("512Mi"),
			},
			Scaling: ScalingPolicy{
				KafkaLagThreshold:     1000,
				P99LatencyThresholdMs: 250,
			},
			Isolation: IsolationPolicy{
				DedicatedNodePool: false,
			},
		},
		Status: TradingTenantStatus{
			CurrentReplicas:        3,
			State:                  TradingTenantStateStable,
			ObservedKafkaLag:       42,
			ObservedP99Ms:          120,
			ObservedPartitionCount: 6,
			LastReconcileTime:      metav1.NewTime(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)),
		},
	}
}

func TestTradingTenantZeroValueJSONRoundTrip(t *testing.T) {
	original := TradingTenant{}

	marshaled, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal zero-value TradingTenant: %v", err)
	}

	var roundTripped TradingTenant
	if err := json.Unmarshal(marshaled, &roundTripped); err != nil {
		t.Fatalf("unmarshal zero-value TradingTenant: %v", err)
	}

	if roundTripped.Spec.TenantID != "" {
		t.Errorf("expected empty TenantID after round trip, got %q", roundTripped.Spec.TenantID)
	}
	if roundTripped.Status.State != "" {
		t.Errorf("expected empty State after round trip, got %q", roundTripped.Status.State)
	}
}

func TestTradingTenantJSONRoundTrip(t *testing.T) {
	original := newPopulatedTenant()

	marshaled, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal populated TradingTenant: %v", err)
	}

	var roundTripped TradingTenant
	if err := json.Unmarshal(marshaled, &roundTripped); err != nil {
		t.Fatalf("unmarshal populated TradingTenant: %v", err)
	}

	if roundTripped.Spec.TenantID != original.Spec.TenantID {
		t.Errorf("TenantID: got %q, want %q", roundTripped.Spec.TenantID, original.Spec.TenantID)
	}
	if roundTripped.Spec.MinReplicas != original.Spec.MinReplicas {
		t.Errorf("MinReplicas: got %d, want %d", roundTripped.Spec.MinReplicas, original.Spec.MinReplicas)
	}
	if roundTripped.Spec.MaxReplicas != original.Spec.MaxReplicas {
		t.Errorf("MaxReplicas: got %d, want %d", roundTripped.Spec.MaxReplicas, original.Spec.MaxReplicas)
	}

	if roundTripped.Spec.Resources.CPURequest.Cmp(original.Spec.Resources.CPURequest) != 0 {
		t.Errorf("CPURequest: got %s, want %s", roundTripped.Spec.Resources.CPURequest.String(), original.Spec.Resources.CPURequest.String())
	}
	if roundTripped.Spec.Resources.MemoryRequest.Cmp(original.Spec.Resources.MemoryRequest) != 0 {
		t.Errorf("MemoryRequest: got %s, want %s", roundTripped.Spec.Resources.MemoryRequest.String(), original.Spec.Resources.MemoryRequest.String())
	}

	if roundTripped.Spec.Scaling != original.Spec.Scaling {
		t.Errorf("Scaling: got %+v, want %+v", roundTripped.Spec.Scaling, original.Spec.Scaling)
	}
	if roundTripped.Spec.Isolation != original.Spec.Isolation {
		t.Errorf("Isolation: got %+v, want %+v", roundTripped.Spec.Isolation, original.Spec.Isolation)
	}

	if roundTripped.Status.CurrentReplicas != original.Status.CurrentReplicas {
		t.Errorf("CurrentReplicas: got %d, want %d", roundTripped.Status.CurrentReplicas, original.Status.CurrentReplicas)
	}
	if roundTripped.Status.State != original.Status.State {
		t.Errorf("State: got %q, want %q", roundTripped.Status.State, original.Status.State)
	}
	if roundTripped.Status.ObservedKafkaLag != original.Status.ObservedKafkaLag {
		t.Errorf("ObservedKafkaLag: got %d, want %d", roundTripped.Status.ObservedKafkaLag, original.Status.ObservedKafkaLag)
	}
	if roundTripped.Status.ObservedP99Ms != original.Status.ObservedP99Ms {
		t.Errorf("ObservedP99Ms: got %d, want %d", roundTripped.Status.ObservedP99Ms, original.Status.ObservedP99Ms)
	}
	if roundTripped.Status.ObservedPartitionCount != original.Status.ObservedPartitionCount {
		t.Errorf("ObservedPartitionCount: got %d, want %d", roundTripped.Status.ObservedPartitionCount, original.Status.ObservedPartitionCount)
	}
	if !roundTripped.Status.LastReconcileTime.Time.Equal(original.Status.LastReconcileTime.Time) {
		t.Errorf("LastReconcileTime: got %s, want %s", roundTripped.Status.LastReconcileTime.Time, original.Status.LastReconcileTime.Time)
	}
}

func TestTradingTenantDeepCopyIndependence(t *testing.T) {
	original := newPopulatedTenant()
	copied := original.DeepCopy()

	copied.Spec.TenantID = "tenant-beta"
	copied.Spec.Resources.CPURequest.Set(2000)
	copied.Spec.Resources.MemoryRequest.Set(1 << 30)
	copied.Status.LastReconcileTime = metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	copied.Status.State = TradingTenantStateDegraded

	if original.Spec.TenantID != "tenant-alpha" {
		t.Errorf("original TenantID mutated: got %q", original.Spec.TenantID)
	}
	if original.Spec.Resources.CPURequest.Cmp(resource.MustParse("500m")) != 0 {
		t.Errorf("original CPURequest mutated: got %s", original.Spec.Resources.CPURequest.String())
	}
	if original.Spec.Resources.MemoryRequest.Cmp(resource.MustParse("512Mi")) != 0 {
		t.Errorf("original MemoryRequest mutated: got %s", original.Spec.Resources.MemoryRequest.String())
	}
	if !original.Status.LastReconcileTime.Time.Equal(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("original LastReconcileTime mutated: got %s", original.Status.LastReconcileTime.Time)
	}
	if original.Status.State != TradingTenantStateStable {
		t.Errorf("original State mutated: got %q", original.Status.State)
	}
}

func TestTradingTenantListDeepCopyIndependence(t *testing.T) {
	original := &TradingTenantList{
		Items: []TradingTenant{
			*newPopulatedTenant(),
			*newPopulatedTenant(),
		},
	}
	original.Items[1].Spec.TenantID = "tenant-gamma"

	copied := original.DeepCopy()

	copied.Items[0].Spec.TenantID = "tenant-mutated"
	copied.Items[0].Spec.Resources.CPURequest.Set(9999)
	copied.Items = append(copied.Items, *newPopulatedTenant())

	if len(original.Items) != 2 {
		t.Fatalf("original Items length changed: got %d, want 2", len(original.Items))
	}
	if original.Items[0].Spec.TenantID != "tenant-alpha" {
		t.Errorf("original Items[0].TenantID mutated: got %q", original.Items[0].Spec.TenantID)
	}
	if original.Items[0].Spec.Resources.CPURequest.Cmp(resource.MustParse("500m")) != 0 {
		t.Errorf("original Items[0].CPURequest mutated: got %s", original.Items[0].Spec.Resources.CPURequest.String())
	}
	if original.Items[1].Spec.TenantID != "tenant-gamma" {
		t.Errorf("original Items[1].TenantID mutated: got %q", original.Items[1].Spec.TenantID)
	}
	if len(copied.Items) != 3 {
		t.Errorf("copied Items length: got %d, want 3", len(copied.Items))
	}
}

func TestTradingTenantStateValues(t *testing.T) {
	testCases := []struct {
		name  string
		state TradingTenantState
		want  string
	}{
		{name: "stable", state: TradingTenantStateStable, want: "Stable"},
		{name: "scaling", state: TradingTenantStateScaling, want: "Scaling"},
		{name: "isolated", state: TradingTenantStateIsolated, want: "Isolated"},
		{name: "degraded", state: TradingTenantStateDegraded, want: "Degraded"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if string(testCase.state) != testCase.want {
				t.Errorf("got %q, want %q", string(testCase.state), testCase.want)
			}
		})
	}
}
