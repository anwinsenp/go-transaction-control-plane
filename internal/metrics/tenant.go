package metrics

// UnknownTenantLabel is the label value reported for a tenant ID outside a
// KnownTenants set. Request-supplied tenant_id values are only
// charset/length-validated at the API boundary, not checked against the set
// of actually provisioned tenants, so using them verbatim as a metric label
// would let a caller drive unbounded series cardinality. Every tenant ID
// outside the known set collapses to this single value instead.
const UnknownTenantLabel = "unregistered"

// KnownTenants is a bounded set of tenant IDs allowed to appear verbatim as
// a metric label value. Any tenant ID outside the set resolves to
// UnknownTenantLabel via TenantLabel, so a metric labeled per tenant stays
// bounded by the number of provisioned tenants rather than by the number of
// distinct tenant_id strings observed on the wire. A nil KnownTenants is
// valid and resolves every tenant ID to UnknownTenantLabel.
type KnownTenants map[string]struct{}

// NewKnownTenants builds a KnownTenants set containing tenantIDs.
func NewKnownTenants(tenantIDs ...string) KnownTenants {
	known := make(KnownTenants, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		known[tenantID] = struct{}{}
	}
	return known
}

// TenantLabel resolves tenantID to itself if it's in known, or to
// UnknownTenantLabel otherwise.
func (known KnownTenants) TenantLabel(tenantID string) string {
	if _, ok := known[tenantID]; ok {
		return tenantID
	}
	return UnknownTenantLabel
}
