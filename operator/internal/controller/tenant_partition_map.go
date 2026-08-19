package controller

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
)

// tenantPartitionMapName is the shared ConfigMap every isolated tenant's
// reserved partition range is published into, one namespace-wide map rather
// than the per-tenant ensureDedicatedConfigMap ConfigMap: the shared
// ingestion publisher (every tenant's traffic, not just isolated ones) and
// every dedicated processor both need to read the whole mapping to hot
// reload their own view of it (ADR 0007, part 3), so a single tenant's
// per-tenant ConfigMap isn't enough on its own.
const tenantPartitionMapName = "tenant-partition-map"

// tenantPartitionMapDataKey is the single Data key the JSON-encoded mapping
// is stored under.
const tenantPartitionMapDataKey = "mapping.json"

// tenantPartitionEntry is one tenant's reserved, contiguous partition range
// [Start, Start+Count) within the shared ingestion topic, matching
// partitionListString's semantics in dedicated_pool.go.
type tenantPartitionEntry struct {
	Start int32 `json:"start"`
	Count int32 `json:"count"`
}

// ensureTenantPartitionMapEntry get-or-creates the shared tenant-partition-map
// ConfigMap and upserts tenant's entry. Unlike the per-tenant dedicated
// resources, this ConfigMap is shared across every isolated tenant in the
// namespace, so it can't carry a single owner reference (ctrl.
// SetControllerReference only supports one controller owner) — its
// lifecycle is managed entirely through entry upsert/removal here rather
// than garbage collection.
func (r *TradingTenantReconciler) ensureTenantPartitionMapEntry(ctx context.Context, tenant *tradingv1alpha1.TradingTenant, start, count int32) error {
	return r.mutateTenantPartitionMap(ctx, tenant.Namespace, func(mapping map[string]tenantPartitionEntry) {
		mapping[tenant.Spec.TenantID] = tenantPartitionEntry{Start: start, Count: count}
	})
}

// removeTenantPartitionMapEntry deletes tenant's entry from the shared
// tenant-partition-map ConfigMap, if present. A missing ConfigMap or a
// tenant with no entry is treated as already-removed, not an error, so
// tearDownDedicatedPool stays idempotent.
func (r *TradingTenantReconciler) removeTenantPartitionMapEntry(ctx context.Context, tenant *tradingv1alpha1.TradingTenant) error {
	return r.mutateTenantPartitionMap(ctx, tenant.Namespace, func(mapping map[string]tenantPartitionEntry) {
		delete(mapping, tenant.Spec.TenantID)
	})
}

// mutateTenantPartitionMap gets the shared ConfigMap (creating it if
// absent), decodes its current mapping, applies mutate, and writes it back
// only if the encoded result actually changed — a read-modify-write rather
// than controllerutil.CreateOrUpdate's single-owner mutate callback, since
// this object's desired state depends on every isolated tenant's Reconcile
// pass, not just the one currently running.
func (r *TradingTenantReconciler) mutateTenantPartitionMap(ctx context.Context, namespace string, mutate func(mapping map[string]tenantPartitionEntry)) error {
	var configMap corev1.ConfigMap
	key := types.NamespacedName{Name: tenantPartitionMapName, Namespace: namespace}
	notFound := false
	if err := r.Get(ctx, key, &configMap); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get tenant partition map: %w", err)
		}
		notFound = true
		configMap = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: tenantPartitionMapName, Namespace: namespace},
		}
	}

	mapping, err := decodeTenantPartitionMap(configMap.Data[tenantPartitionMapDataKey])
	if err != nil {
		return fmt.Errorf("decode tenant partition map: %w", err)
	}

	mutate(mapping)

	encoded, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("encode tenant partition map: %w", err)
	}

	// A not-found ConfigMap decodes to an empty mapping (see
	// decodeTenantPartitionMap): if mutate left it empty too (e.g. removing
	// an entry that was never there), skip Create entirely rather than
	// bringing an empty ConfigMap into existence as a side effect of a
	// no-op call. Once the ConfigMap exists, the plain string comparison
	// against its previously encoded value is enough — "{}"'s only
	// possible previous value on that path is itself.
	if notFound && len(mapping) == 0 {
		return nil
	}
	if !notFound && configMap.Data[tenantPartitionMapDataKey] == string(encoded) {
		return nil
	}

	if configMap.Data == nil {
		configMap.Data = make(map[string]string, 1)
	}
	configMap.Data[tenantPartitionMapDataKey] = string(encoded)

	if notFound {
		if err := r.Create(ctx, &configMap); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Lost a create race against another reconcile pass (e.g. a
				// second tenant isolating concurrently): retry as an update
				// against whatever now exists rather than erroring the whole
				// reconcile.
				return r.mutateTenantPartitionMap(ctx, namespace, mutate)
			}
			return fmt.Errorf("create tenant partition map: %w", err)
		}
		return nil
	}

	if err := r.Update(ctx, &configMap); err != nil {
		return fmt.Errorf("update tenant partition map: %w", err)
	}
	return nil
}

// decodeTenantPartitionMap parses raw (the ConfigMap's mapping.json value)
// into a mapping ready to mutate. An empty/missing value decodes to an
// empty mapping rather than an error, so the first tenant to isolate in a
// namespace doesn't need special-case handling.
func decodeTenantPartitionMap(raw string) (map[string]tenantPartitionEntry, error) {
	mapping := make(map[string]tenantPartitionEntry)
	if raw == "" {
		return mapping, nil
	}
	if err := json.Unmarshal([]byte(raw), &mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}
