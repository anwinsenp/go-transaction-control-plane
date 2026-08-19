package controller

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// getTenantPartitionMap fetches the shared tenant-partition-map ConfigMap
// and decodes its mapping.json Data key.
func getTenantPartitionMap(t *testing.T, ctx context.Context, reconciler *TradingTenantReconciler, namespace string) (corev1.ConfigMap, map[string]tenantPartitionEntry) {
	t.Helper()
	var configMap corev1.ConfigMap
	key := types.NamespacedName{Name: tenantPartitionMapName, Namespace: namespace}
	if err := reconciler.Get(ctx, key, &configMap); err != nil {
		t.Fatalf("Get tenant partition map: %v", err)
	}
	var mapping map[string]tenantPartitionEntry
	if err := json.Unmarshal([]byte(configMap.Data[tenantPartitionMapDataKey]), &mapping); err != nil {
		t.Fatalf("unmarshal tenant partition map: %v", err)
	}
	return configMap, mapping
}

func TestEnsureTenantPartitionMapEntry_CreatesConfigMapOnFirstTenant(t *testing.T) {
	tenant := newTestTenant()
	reconciler, _ := newReconciler(t, tenant, &fakeObserver{})
	ctx := context.Background()

	if err := reconciler.ensureTenantPartitionMapEntry(ctx, tenant, 5, 3); err != nil {
		t.Fatalf("ensureTenantPartitionMapEntry returned error: %v", err)
	}

	_, mapping := getTenantPartitionMap(t, ctx, reconciler, tenant.Namespace)
	if len(mapping) != 1 {
		t.Fatalf("mapping = %+v, want exactly 1 entry", mapping)
	}
	entry, ok := mapping[tenant.Spec.TenantID]
	if !ok {
		t.Fatalf("mapping missing entry for tenant %q: %+v", tenant.Spec.TenantID, mapping)
	}
	if entry != (tenantPartitionEntry{Start: 5, Count: 3}) {
		t.Errorf("mapping[%q] = %+v, want {Start:5 Count:3}", tenant.Spec.TenantID, entry)
	}
}

func TestEnsureTenantPartitionMapEntry_SecondTenantAddsEntryAlongsideExisting(t *testing.T) {
	firstTenant := newTestTenant()
	reconciler, fakeClient := newReconciler(t, firstTenant, &fakeObserver{})
	ctx := context.Background()

	if err := reconciler.ensureTenantPartitionMapEntry(ctx, firstTenant, 0, 3); err != nil {
		t.Fatalf("ensureTenantPartitionMapEntry (first tenant) returned error: %v", err)
	}

	secondTenant := newTestTenant()
	secondTenant.Name = "tenant-beta"
	secondTenant.Spec.TenantID = "tenant-beta"
	if err := fakeClient.Create(ctx, secondTenant); err != nil {
		t.Fatalf("Create second tenant: %v", err)
	}

	if err := reconciler.ensureTenantPartitionMapEntry(ctx, secondTenant, 3, 2); err != nil {
		t.Fatalf("ensureTenantPartitionMapEntry (second tenant) returned error: %v", err)
	}

	_, mapping := getTenantPartitionMap(t, ctx, reconciler, firstTenant.Namespace)
	if len(mapping) != 2 {
		t.Fatalf("mapping = %+v, want exactly 2 entries", mapping)
	}
	if got := mapping[firstTenant.Spec.TenantID]; got != (tenantPartitionEntry{Start: 0, Count: 3}) {
		t.Errorf("mapping[%q] = %+v, want unchanged {Start:0 Count:3}", firstTenant.Spec.TenantID, got)
	}
	if got := mapping[secondTenant.Spec.TenantID]; got != (tenantPartitionEntry{Start: 3, Count: 2}) {
		t.Errorf("mapping[%q] = %+v, want {Start:3 Count:2}", secondTenant.Spec.TenantID, got)
	}
}

// TestEnsureTenantPartitionMapEntry_IdempotentOnSecondCall confirms a second
// call with the same start/count doesn't produce a spurious Update: the
// ConfigMap's ResourceVersion must stay unchanged, matching the
// get-or-create idempotency pattern exercised in dedicated_pool_test.go.
func TestEnsureTenantPartitionMapEntry_IdempotentOnSecondCall(t *testing.T) {
	tenant := newTestTenant()
	reconciler, _ := newReconciler(t, tenant, &fakeObserver{})
	ctx := context.Background()

	if err := reconciler.ensureTenantPartitionMapEntry(ctx, tenant, 5, 3); err != nil {
		t.Fatalf("first ensureTenantPartitionMapEntry returned error: %v", err)
	}
	before, _ := getTenantPartitionMap(t, ctx, reconciler, tenant.Namespace)

	if err := reconciler.ensureTenantPartitionMapEntry(ctx, tenant, 5, 3); err != nil {
		t.Fatalf("second ensureTenantPartitionMapEntry returned error: %v", err)
	}
	after, _ := getTenantPartitionMap(t, ctx, reconciler, tenant.Namespace)

	if after.ResourceVersion != before.ResourceVersion {
		t.Errorf("ConfigMap ResourceVersion changed on idempotent call: %q -> %q", before.ResourceVersion, after.ResourceVersion)
	}
}

func TestRemoveTenantPartitionMapEntry_LeavesOtherTenantsIntact(t *testing.T) {
	firstTenant := newTestTenant()
	reconciler, fakeClient := newReconciler(t, firstTenant, &fakeObserver{})
	ctx := context.Background()

	secondTenant := newTestTenant()
	secondTenant.Name = "tenant-beta"
	secondTenant.Spec.TenantID = "tenant-beta"
	if err := fakeClient.Create(ctx, secondTenant); err != nil {
		t.Fatalf("Create second tenant: %v", err)
	}

	if err := reconciler.ensureTenantPartitionMapEntry(ctx, firstTenant, 0, 3); err != nil {
		t.Fatalf("ensureTenantPartitionMapEntry (first tenant) returned error: %v", err)
	}
	if err := reconciler.ensureTenantPartitionMapEntry(ctx, secondTenant, 3, 2); err != nil {
		t.Fatalf("ensureTenantPartitionMapEntry (second tenant) returned error: %v", err)
	}

	if err := reconciler.removeTenantPartitionMapEntry(ctx, firstTenant); err != nil {
		t.Fatalf("removeTenantPartitionMapEntry returned error: %v", err)
	}

	_, mapping := getTenantPartitionMap(t, ctx, reconciler, firstTenant.Namespace)
	if _, ok := mapping[firstTenant.Spec.TenantID]; ok {
		t.Errorf("mapping still contains removed tenant %q: %+v", firstTenant.Spec.TenantID, mapping)
	}
	if got := mapping[secondTenant.Spec.TenantID]; got != (tenantPartitionEntry{Start: 3, Count: 2}) {
		t.Errorf("mapping[%q] = %+v, want untouched {Start:3 Count:2}", secondTenant.Spec.TenantID, got)
	}
}

// TestRemoveTenantPartitionMapEntry_NoEntryIsNoOp confirms removing a
// tenant that was never added — both when the ConfigMap already exists
// (holding other tenants) and when it doesn't exist at all — is a no-op,
// not an error.
func TestRemoveTenantPartitionMapEntry_NoEntryIsNoOp(t *testing.T) {
	// mutateTenantPartitionMap's skip-write check compares the encoded
	// mapping against configMap.Data[tenantPartitionMapDataKey]
	// regardless of whether the ConfigMap existed: a not-found ConfigMap
	// decodes to an empty mapping (see decodeTenantPartitionMap), so a
	// delete on an already-empty mapping re-encodes to the same "" value
	// and short-circuits before Create — a remove against a wholly-absent
	// ConfigMap leaves it absent, not creating an empty one as a side
	// effect.
	t.Run("configmap absent entirely", func(t *testing.T) {
		tenant := newTestTenant()
		reconciler, _ := newReconciler(t, tenant, &fakeObserver{})
		ctx := context.Background()

		if err := reconciler.removeTenantPartitionMapEntry(ctx, tenant); err != nil {
			t.Fatalf("removeTenantPartitionMapEntry returned error: %v", err)
		}

		var configMap corev1.ConfigMap
		key := types.NamespacedName{Name: tenantPartitionMapName, Namespace: tenant.Namespace}
		err := reconciler.Get(ctx, key, &configMap)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("Get tenant partition map after no-op remove: got err %v, want NotFound", err)
		}
	})

	t.Run("configmap exists but tenant has no entry", func(t *testing.T) {
		presentTenant := newTestTenant()
		reconciler, fakeClient := newReconciler(t, presentTenant, &fakeObserver{})
		ctx := context.Background()

		if err := reconciler.ensureTenantPartitionMapEntry(ctx, presentTenant, 0, 3); err != nil {
			t.Fatalf("ensureTenantPartitionMapEntry returned error: %v", err)
		}

		absentTenant := newTestTenant()
		absentTenant.Name = "tenant-gamma"
		absentTenant.Spec.TenantID = "tenant-gamma"
		if err := fakeClient.Create(ctx, absentTenant); err != nil {
			t.Fatalf("Create absent tenant: %v", err)
		}

		if err := reconciler.removeTenantPartitionMapEntry(ctx, absentTenant); err != nil {
			t.Fatalf("removeTenantPartitionMapEntry returned error: %v", err)
		}

		_, mapping := getTenantPartitionMap(t, ctx, reconciler, presentTenant.Namespace)
		if len(mapping) != 1 {
			t.Fatalf("mapping = %+v, want exactly 1 entry (untouched)", mapping)
		}
		if _, ok := mapping[presentTenant.Spec.TenantID]; !ok {
			t.Errorf("mapping missing entry for tenant %q: %+v", presentTenant.Spec.TenantID, mapping)
		}
	})
}
