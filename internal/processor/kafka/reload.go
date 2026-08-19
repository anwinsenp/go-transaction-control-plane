package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultTenantPartitionReloadInterval is how often a TenantPartitionReloader
// polls its Source when Config.TenantPartitionReloadInterval is left at
// zero.
const DefaultTenantPartitionReloadInterval = 5 * time.Second

// tenantPartitionMapEntry mirrors the operator's
// operator/internal/controller.tenantPartitionEntry wire shape and
// internal/ingestion/kafka's identically-named type (ADR 0007, part 3).
// Duplicated rather than shared across the ingestion/processor domain
// boundary: it's a two-field wire struct, not worth a new internal package
// for.
type tenantPartitionMapEntry struct {
	Start int32 `json:"start"`
	Count int32 `json:"count"`
}

// TenantPartitionSource loads the current tenantID -> reserved-partition
// mapping from some external source (a mounted ConfigMap file in
// production, a fake in tests). A non-nil error means the mapping could
// not be loaded at all (missing/unreadable file, malformed JSON); callers
// must not treat a returned nil map as "no tenants reserved" in that case.
type TenantPartitionSource interface {
	Load() (map[string]tenantPartitionMapEntry, error)
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
func (source FileTenantPartitionSource) Load() (map[string]tenantPartitionMapEntry, error) {
	raw, err := os.ReadFile(source.Path)
	if err != nil {
		return nil, fmt.Errorf("read tenant partition map %q: %w", source.Path, err)
	}

	var entries map[string]tenantPartitionMapEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("parse tenant partition map %q: %w", source.Path, err)
	}
	return entries, nil
}

// partitionsForEntry expands a tenantPartitionEntry's [Start, Start+Count)
// range into an explicit set of partition indices.
func partitionsForEntry(entry tenantPartitionMapEntry) (map[int32]struct{}, error) {
	if entry.Count < 1 {
		return nil, fmt.Errorf("count %d must be >= 1", entry.Count)
	}
	partitions := make(map[int32]struct{}, entry.Count)
	for index := int32(0); index < entry.Count; index++ {
		partitions[entry.Start+index] = struct{}{}
	}
	return partitions, nil
}

// TenantPartitionReloader periodically polls Source for TenantID's entry
// and, on success, adjusts Consumer's manually assigned partitions to
// match. A failed Load, a mapping missing TenantID's entry, or a malformed
// entry (count < 1) is reported on
// Metrics.partitionReloadErrorsTotal and otherwise ignored — Consumer keeps
// whatever partitions it already has assigned, so a transient
// ConfigMap-mount hiccup or a bad write never drops this processor's
// assignment to zero partitions and stalls consumption entirely.
type TenantPartitionReloader struct {
	Source   TenantPartitionSource
	Consumer *Consumer
	// TenantID selects which entry in the loaded mapping belongs to this
	// processor.
	TenantID string
	// Topic is the topic AddConsumePartitions/RemoveConsumePartitions are
	// scoped to — Consumer's single configured topic.
	Topic string
	// Interval is how often Source is polled. DefaultTenantPartitionReloadInterval
	// is used if this is <= 0.
	Interval time.Duration
	Metrics  *Metrics
}

// Run polls r.Source on r.Interval until ctx is canceled. It is meant to be
// run in its own goroutine, owned and canceled by whatever constructs the
// Consumer this reloader feeds (see Consumer.Close).
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
	target, err := reloader.loadTarget()
	if err != nil {
		log.Printf("processor: reloading tenant partition assignment: %v", err)
		if reloader.Metrics != nil {
			reloader.Metrics.partitionReloadErrorsTotal.Inc()
		}
		return
	}
	reloader.Consumer.applyAssignment(reloader.Topic, target)
}

func (reloader *TenantPartitionReloader) loadTarget() (map[int32]struct{}, error) {
	entries, err := reloader.Source.Load()
	if err != nil {
		return nil, err
	}
	entry, ok := entries[reloader.TenantID]
	if !ok {
		return nil, fmt.Errorf("tenant %q has no entry in tenant partition map", reloader.TenantID)
	}
	partitions, err := partitionsForEntry(entry)
	if err != nil {
		return nil, fmt.Errorf("tenant %q: %w", reloader.TenantID, err)
	}
	return partitions, nil
}

// applyAssignment diffs target against con's current manually assigned
// partition set and calls AddConsumePartitions for newly-reserved
// partitions before RemoveConsumePartitions for dropped ones, so a reload
// never has zero partitions assigned in between — a tenant whose range
// shifts (e.g. [0,4) -> [2,6)) keeps consuming partitions 2 and 3
// throughout, gaining 4-5 and losing 0-1 without a consumption gap.
func (con *Consumer) applyAssignment(topic string, target map[int32]struct{}) {
	con.assignmentMu.Lock()
	defer con.assignmentMu.Unlock()

	added := make(map[int32]kgo.Offset)
	for partition := range target {
		if _, ok := con.assignment[partition]; !ok {
			added[partition] = kgo.NewOffset().AtStart()
		}
	}
	var removed []int32
	for partition := range con.assignment {
		if _, ok := target[partition]; !ok {
			removed = append(removed, partition)
		}
	}

	if len(added) == 0 && len(removed) == 0 {
		return
	}

	if len(added) > 0 {
		con.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: added})
	}
	if len(removed) > 0 {
		con.client.RemoveConsumePartitions(map[string][]int32{topic: removed})
	}

	assignment := make(map[int32]struct{}, len(target))
	for partition := range target {
		assignment[partition] = struct{}{}
	}
	con.assignment = assignment
}
