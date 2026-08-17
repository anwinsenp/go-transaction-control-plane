package kafka

import (
	"bytes"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultPartitionsPerTenant is how many partitions an unreserved tenant is
// confined to when Config.DefaultPartitionsPerTenant is left at zero.
const DefaultPartitionsPerTenant int32 = 1

// TenantPartitionConfig reserves a fixed, exclusive block of partitions for
// specific tenants — e.g. tenants the operator has flagged noisy/isolated
// via TradingTenant's spec.isolation.dedicatedNodePool (see ADR 0007) — so
// they get more than the default per-tenant partition allowance. A tenant
// absent from this map gets Config.DefaultPartitionsPerTenant instead.
// Values must be >= 1.
type TenantPartitionConfig map[string]int32

// tenantPartitioner is a kgo.Partitioner that replaces the default
// key-hash partitioner with an explicit tenantID -> []partition
// reservation (ADR 0007, part 1): every tenant is confined to its own
// range of the topic's partitions, and instrument is hashed only within
// that range. This preserves per-tenant/per-instrument publish ordering
// (idempotent P&L reconciliation depends on it) while guaranteeing no two
// tenants ever share a partition, up to the number of distinct tenants the
// topic's partition count can hold — see reservationTable's doc comment
// for what happens beyond that.
type tenantPartitioner struct {
	reserved    TenantPartitionConfig
	defaultSize int32
	metrics     *Metrics
}

// newTenantPartitioner builds a Partitioner. defaultSize must be >= 1.
// metrics is optional: pass nil to skip reporting dropped reservations.
func newTenantPartitioner(reserved TenantPartitionConfig, defaultSize int32, reportedMetrics *Metrics) *tenantPartitioner {
	return &tenantPartitioner{reserved: reserved, defaultSize: defaultSize, metrics: reportedMetrics}
}

func (part *tenantPartitioner) ForTopic(string) kgo.TopicPartitioner {
	return &tenantTopicPartitioner{reserved: part.reserved, defaultSize: part.defaultSize, metrics: part.metrics}
}

// tenantTopicPartitioner partitions records for one topic. Per
// kgo.Partitioner's documented contract, kgo guarantees only one record
// uses a given TopicPartitioner at a time, so the lazily-built reservation
// table below needs no lock despite Publish being called concurrently.
type tenantTopicPartitioner struct {
	reserved    TenantPartitionConfig
	defaultSize int32
	metrics     *Metrics
	table       *reservationTable
}

// RequiresConsistency reports that a record must keep hashing to the same
// partition even while that partition is unavailable, matching the prior
// key-hash partitioner's behavior: per-instrument publish ordering must
// not shift onto a different partition just because one is briefly down.
func (*tenantTopicPartitioner) RequiresConsistency(*kgo.Record) bool { return true }

// Partition assigns record to a partition within its tenant's reserved
// range. The reservation table is built lazily from the first observed
// partition count and rebuilt only if that count changes (a topic resize),
// so steady-state calls do no new allocations beyond the occasional
// pool-assignment cache insert for a tenant seen for the first time. A
// table (re)build that drops any explicitly configured tenant's
// reservation reports it on metrics.tenantReservationDroppedTotal, so an
// under-provisioned topic is observable rather than a silent isolation
// gap.
func (part *tenantTopicPartitioner) Partition(record *kgo.Record, totalPartitions int) int {
	if part.table == nil || part.table.totalPartitions != int32(totalPartitions) {
		part.table = newReservationTable(part.reserved, part.defaultSize, int32(totalPartitions), part.metrics)
		if part.metrics != nil && len(part.table.droppedTenants) > 0 {
			part.metrics.tenantReservationDroppedTotal.Add(float64(len(part.table.droppedTenants)))
		}
	}

	tenantID, instrument := splitPartitionKey(record.Key)
	return int(part.table.partitionFor(tenantID, instrument))
}

// splitPartitionKey splits a "tenantID:instrument" key (see partitionKey)
// back into its two components without allocating: tenant_id and
// instrument charsets (internal/api's request validation) never contain
// ':', so the first colon is always the boundary.
func splitPartitionKey(key []byte) (tenantID, instrument []byte) {
	idx := bytes.IndexByte(key, ':')
	if idx < 0 {
		return key, nil
	}
	return key[:idx], key[idx+1:]
}

// partitionRange is a contiguous, half-open block of partition indices
// [start, start+size) reserved for one tenant.
type partitionRange struct {
	start int32
	size  int32
}

// reservationTable is a deterministic tenantID -> partition-range mapping
// computed for a fixed total partition count. Tenants explicitly listed in
// TenantPartitionConfig get a fixed, contiguous block of that many
// partitions, carved out first, in sorted tenant-ID order so the layout is
// reproducible run to run. Every other tenant draws its own exclusive
// block of defaultSize partitions from what's left (the "pool"), assigned
// on first sight and cached for the life of the table.
//
// Once the pool is exhausted — more distinct unreserved tenants than pool
// capacity — additional tenants fall back to sharing a pool partition
// chosen by hashing the tenant ID, the same strategy the old key-hash
// partitioner used for everyone. That fallback only changes behavior for a
// topic under-provisioned for its tenant count; it never lets an
// explicitly reserved tenant's range be shared.
type reservationTable struct {
	totalPartitions int32
	defaultSize     int32
	explicit        map[string]partitionRange
	poolStart       int32
	poolSize        int32
	poolNext        int32
	pool            map[string]partitionRange
	metrics         *Metrics

	// droppedTenants holds the IDs (sorted) of tenants configured in
	// TenantPartitionConfig that couldn't be given any explicit range at
	// all because explicitCapacity was already exhausted by the time the
	// table reached them, in sorted tenant-ID order. Those tenants fall
	// back to being treated as pool tenants, silently losing their
	// isolation guarantee. There's no logging/metrics sink wired into this
	// package today, so this field is the only place that loss is
	// observable — callers that care should inspect it.
	droppedTenants []string
}

// newReservationTable builds a reservationTable for totalPartitions
// partitions. defaultSize < 1 is treated as DefaultPartitionsPerTenant;
// totalPartitions < 1 is treated as 1, so the table is always usable even
// before a topic's real partition count is known. reportedMetrics is
// optional: pass nil to skip reporting partition counts (e.g. in
// benchmarks/tests that don't need it).
func newReservationTable(reserved TenantPartitionConfig, defaultSize, totalPartitions int32, reportedMetrics *Metrics) *reservationTable {
	if defaultSize < 1 {
		defaultSize = DefaultPartitionsPerTenant
	}
	if totalPartitions < 1 {
		totalPartitions = 1
	}

	// Explicit reservations may consume at most totalPartitions-1
	// partitions, always leaving at least one for the pool: without this,
	// a fully-consuming reservation set would force the pool-exhaustion
	// fallback below to hash unreserved tenants into an explicit tenant's
	// own range, which is exactly the overlap this table exists to
	// prevent. The sole exception is totalPartitions == 1, where explicit
	// and pool tenants have no choice but to share the only partition
	// there is.
	explicitCapacity := totalPartitions
	if totalPartitions > 1 {
		explicitCapacity = totalPartitions - 1
	}

	tenantIDs := make([]string, 0, len(reserved))
	for tenantID := range reserved {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)

	explicit := make(map[string]partitionRange, len(tenantIDs))
	var droppedTenants []string
	var cursor int32
	for index, tenantID := range tenantIDs {
		if cursor >= explicitCapacity {
			// No partitions left within explicitCapacity: this and every
			// remaining tenant (tenantIDs is sorted, so nothing later will
			// fare any better) falls back to the pool like an unreserved
			// tenant, so its isolation guarantee silently doesn't hold
			// under this under-provisioned config. Recorded in
			// droppedTenants since there's no metrics/logging hook wired
			// into this package to surface that today.
			droppedTenants = append(droppedTenants, tenantIDs[index:]...)
			break
		}
		size := reserved[tenantID]
		if size < 1 {
			size = 1
		}
		if cursor+size > explicitCapacity {
			// Not enough partitions left to honor this reservation in
			// full; give it whatever remains rather than overlap into
			// another tenant's range or the pool.
			size = explicitCapacity - cursor
		}
		explicit[tenantID] = partitionRange{start: cursor, size: size}
		if reportedMetrics != nil {
			reportedMetrics.observePartitionCount(tenantID, size)
		}
		cursor += size
	}

	poolStart := cursor
	poolSize := totalPartitions - cursor

	return &reservationTable{
		totalPartitions: totalPartitions,
		defaultSize:     defaultSize,
		explicit:        explicit,
		poolStart:       poolStart,
		poolSize:        poolSize,
		poolNext:        poolStart,
		pool:            make(map[string]partitionRange),
		droppedTenants:  droppedTenants,
		metrics:         reportedMetrics,
	}
}

// partitionFor returns the partition index for a record with the given
// tenantID and instrument.
func (table *reservationTable) partitionFor(tenantID, instrument []byte) int32 {
	rng, ok := table.explicit[string(tenantID)]
	if !ok {
		rng = table.poolRangeFor(tenantID)
	}
	return rng.start + int32(hashBytes(instrument)%uint32(rng.size))
}

// poolRangeFor returns the pool range assigned to tenantID, assigning and
// caching one on first sight.
func (table *reservationTable) poolRangeFor(tenantID []byte) partitionRange {
	if rng, ok := table.pool[string(tenantID)]; ok {
		return rng
	}

	size := table.defaultSize
	if remaining := table.poolStart + table.poolSize - table.poolNext; size > remaining {
		size = remaining
	}
	if size <= 0 {
		// Truly nothing left in the pool: fall back to sharing a pool
		// partition by hashing the tenant ID, same as every other
		// pool-exhaustion tenant. Not cached, since a later call could
		// observe a different table if the pool were ever to shrink again —
		// and, since it's never cached, this tenant's partition-count gauge
		// series never gets created at all (not merely stale). The
		// operator's promquery client treats "no series matched" as a query
		// error, which propagates as a Reconcile error and standard
		// requeue/backoff rather than a silent no-op — so this tenant's
		// TradingTenant simply never converges until the topic is
		// repartitioned, the same failure mode an unscraped lag/latency
		// series already has today.
		return partitionRange{start: table.poolStart + int32(hashBytes(tenantID)%uint32(table.poolSize)), size: 1}
	}

	rng := partitionRange{start: table.poolNext, size: size}
	table.poolNext += size
	table.pool[string(tenantID)] = rng
	if table.metrics != nil {
		table.metrics.observePartitionCount(string(tenantID), rng.size)
	}
	return rng
}

// hashBytes is a small FNV-1a hash used to distribute instruments (and, in
// the pool-exhaustion fallback, tenant IDs) across a partition range. It's
// not cryptographic — determinism and a reasonably even distribution are
// all that's required here.
func hashBytes(data []byte) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for _, singleByte := range data {
		hash ^= uint32(singleByte)
		hash *= prime32
	}
	return hash
}
