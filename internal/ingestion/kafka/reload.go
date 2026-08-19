package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

// DefaultTenantPartitionReloadInterval is how often a TenantPartitionReloader
// polls its Source when Config.TenantPartitionReloadInterval is left at
// zero.
const DefaultTenantPartitionReloadInterval = 5 * time.Second

// tenantPartitionMapEntry mirrors the operator's
// operator/internal/controller.tenantPartitionEntry wire shape (ADR 0007,
// part 3). Only Count is used here: Start is recomputed independently by
// this package's own deterministic reservationTable algorithm from the
// full set of reserved counts, and is expected to agree with the
// operator's own observed Start by construction (see partitioner.go) —
// carrying it through unused would let the two sides silently disagree
// without either noticing.
type tenantPartitionMapEntry struct {
	Start int32 `json:"start"`
	Count int32 `json:"count"`
}

// TenantPartitionSource loads the current tenantID -> reserved-partition-count
// mapping from some external source (a mounted ConfigMap file in
// production, a fake in tests). A non-nil error means the mapping could
// not be loaded at all (missing/unreadable file, malformed JSON); callers
// must not treat a returned zero-value TenantPartitionConfig as "no
// reservations" in that case.
type TenantPartitionSource interface {
	Load() (TenantPartitionConfig, error)
}

// FileTenantPartitionSource reads the tenant partition mapping from a
// ConfigMap projected as a volume file — see docs/DESIGN-operator.md for
// why this is a polled file read rather than a Kubernetes API watch.
type FileTenantPartitionSource struct {
	// Path is the mounted file's path, e.g.
	// /etc/tenant-partition-map/mapping.json.
	Path string
}

// Load reads and parses Path. The file is expected to hold the JSON object
// the operator's tenant-partition-map ConfigMap's mapping.json key
// contains: {"<tenantID>": {"start": N, "count": N}, ...}.
func (source FileTenantPartitionSource) Load() (TenantPartitionConfig, error) {
	raw, err := os.ReadFile(source.Path)
	if err != nil {
		return nil, fmt.Errorf("read tenant partition map %q: %w", source.Path, err)
	}

	var entries map[string]tenantPartitionMapEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("parse tenant partition map %q: %w", source.Path, err)
	}

	reserved := make(TenantPartitionConfig, len(entries))
	for tenantID, entry := range entries {
		if entry.Count < 1 {
			return nil, fmt.Errorf("parse tenant partition map %q: tenant %q count %d must be >= 1", source.Path, tenantID, entry.Count)
		}
		reserved[tenantID] = entry.Count
	}
	return reserved, nil
}

// TenantPartitionReloader periodically polls Source and, on success, swaps
// Partitioner's live reservation config. A failed Load (missing file,
// malformed JSON, a zero-count entry) is reported on
// Metrics.tenantReservationReloadErrorTotal and otherwise ignored — the
// partitioner keeps whatever reservations it already had, so a transient
// ConfigMap-mount hiccup or a bad write never drops an isolated tenant's
// exclusive partitions down to the shared default. The very first Load
// happening before any successful reload leaves the partitioner on
// whatever TenantPartitionConfig NewPublisher was seeded with (empty,
// unless Config.TenantPartitions was also set), which itself falls back
// safely per-tenant via DefaultPartitionsPerTenant.
type TenantPartitionReloader struct {
	Source      TenantPartitionSource
	Partitioner *tenantPartitioner
	// DefaultSize is the DefaultPartitionsPerTenant every reload applies
	// alongside the loaded reservation counts.
	DefaultSize int32
	// Interval is how often Source is polled. DefaultTenantPartitionReloadInterval
	// is used if this is <= 0.
	Interval time.Duration
	Metrics  *Metrics
}

// Run polls r.Source on r.Interval until ctx is canceled. It is meant to be
// run in its own goroutine, owned and canceled by whatever constructs the
// Publisher this reloader feeds (see Publisher.Close).
func (reloader *TenantPartitionReloader) Run(ctx context.Context) {
	interval := reloader.Interval
	if interval <= 0 {
		interval = DefaultTenantPartitionReloadInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reloader.reloadOnce()
		}
	}
}

func (reloader *TenantPartitionReloader) reloadOnce() {
	reserved, err := reloader.Source.Load()
	if err != nil {
		log.Printf("ingestion: reloading tenant partition map: %v", err)
		if reloader.Metrics != nil {
			reloader.Metrics.tenantReservationReloadErrorTotal.Inc()
		}
		return
	}
	reloader.Partitioner.updateReservations(reserved, reloader.DefaultSize)
}
