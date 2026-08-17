package metrics

import "testing"

func TestNewKnownTenants(t *testing.T) {
	testCases := []struct {
		name      string
		tenantIDs []string
	}{
		{name: "no tenant IDs builds an empty set", tenantIDs: nil},
		{name: "single tenant ID", tenantIDs: []string{"tenant-1"}},
		{name: "multiple tenant IDs", tenantIDs: []string{"tenant-1", "tenant-2", "tenant-3"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			known := NewKnownTenants(testCase.tenantIDs...)
			if len(known) != len(testCase.tenantIDs) {
				t.Fatalf("len(NewKnownTenants(%v)) = %d, want %d", testCase.tenantIDs, len(known), len(testCase.tenantIDs))
			}
			for _, tenantID := range testCase.tenantIDs {
				if _, ok := known[tenantID]; !ok {
					t.Errorf("NewKnownTenants(%v) missing tenant ID %q", testCase.tenantIDs, tenantID)
				}
			}
		})
	}
}

func TestKnownTenants_TenantLabel(t *testing.T) {
	testCases := []struct {
		name     string
		known    KnownTenants
		tenantID string
		want     string
	}{
		{
			name:     "known tenant resolves to itself",
			known:    NewKnownTenants("tenant-1", "tenant-2"),
			tenantID: "tenant-1",
			want:     "tenant-1",
		},
		{
			name:     "unknown tenant resolves to UnknownTenantLabel",
			known:    NewKnownTenants("tenant-1", "tenant-2"),
			tenantID: "tenant-99",
			want:     UnknownTenantLabel,
		},
		{
			name:     "empty tenant ID resolves to UnknownTenantLabel when not registered",
			known:    NewKnownTenants("tenant-1"),
			tenantID: "",
			want:     UnknownTenantLabel,
		},
		{
			name:     "empty KnownTenants resolves everything to UnknownTenantLabel",
			known:    NewKnownTenants(),
			tenantID: "tenant-1",
			want:     UnknownTenantLabel,
		},
		{
			name:     "nil KnownTenants resolves everything to UnknownTenantLabel",
			known:    nil,
			tenantID: "tenant-1",
			want:     UnknownTenantLabel,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.known.TenantLabel(testCase.tenantID); got != testCase.want {
				t.Errorf("TenantLabel(%q) = %q, want %q", testCase.tenantID, got, testCase.want)
			}
		})
	}
}
