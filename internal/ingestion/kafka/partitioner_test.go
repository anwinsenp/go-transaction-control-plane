package kafka

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// rangesOverlap reports whether two half-open partition ranges intersect.
func rangesOverlap(first, second partitionRange) bool {
	return first.start < second.start+second.size && second.start < first.start+first.size
}

func TestNewReservationTableExplicitRangesDisjoint(t *testing.T) {
	reserved := TenantPartitionConfig{
		"tenant-a": 2,
		"tenant-b": 1,
		"tenant-c": 3,
	}
	table := newReservationTable(reserved, DefaultPartitionsPerTenant, 8, nil)

	if len(table.explicit) != len(reserved) {
		t.Fatalf("len(explicit) = %d, want %d", len(table.explicit), len(reserved))
	}

	for tenantID, size := range reserved {
		rng, ok := table.explicit[tenantID]
		if !ok {
			t.Fatalf("explicit range missing for tenant %q", tenantID)
		}
		if rng.size != size {
			t.Errorf("explicit[%q].size = %d, want %d", tenantID, rng.size, size)
		}
	}

	poolRange := partitionRange{start: table.poolStart, size: table.poolSize}

	tenantIDs := []string{"tenant-a", "tenant-b", "tenant-c"}
	for i, firstID := range tenantIDs {
		firstRange := table.explicit[firstID]

		if rangesOverlap(firstRange, poolRange) {
			t.Errorf("explicit range for %q (%+v) overlaps pool range %+v", firstID, firstRange, poolRange)
		}

		for _, secondID := range tenantIDs[i+1:] {
			secondRange := table.explicit[secondID]
			if rangesOverlap(firstRange, secondRange) {
				t.Errorf("explicit ranges for %q (%+v) and %q (%+v) overlap", firstID, firstRange, secondID, secondRange)
			}
		}
	}
}

func TestReservationTablePartitionForPoolTenantsUnique(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 2, "tenant-b": 1}
	table := newReservationTable(reserved, 1, 8, nil)

	poolTenants := []string{"tenant-c", "tenant-d", "tenant-e", "tenant-f"}
	if int32(len(poolTenants)) > table.poolSize {
		t.Fatalf("test setup: %d pool tenants exceeds pool capacity %d", len(poolTenants), table.poolSize)
	}

	seen := make(map[int32]string, len(poolTenants))
	for _, tenantID := range poolTenants {
		partition := table.partitionFor([]byte(tenantID), []byte("AAPL"))

		if owner, ok := seen[partition]; ok {
			t.Errorf("partition %d assigned to both %q and %q, want unique partitions", partition, owner, tenantID)
		}
		seen[partition] = tenantID

		for explicitTenant, explicitRange := range table.explicit {
			if partition >= explicitRange.start && partition < explicitRange.start+explicitRange.size {
				t.Errorf("pool tenant %q got partition %d, which falls inside explicit tenant %q's range %+v", tenantID, partition, explicitTenant, explicitRange)
			}
		}
	}
}

func TestReservationTablePartitionForDeterministic(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 3}
	table := newReservationTable(reserved, 2, 8, nil)

	tests := []struct {
		name       string
		tenantID   string
		instrument string
	}{
		{name: "explicit tenant", tenantID: "tenant-a", instrument: "AAPL"},
		{name: "pool tenant", tenantID: "tenant-z", instrument: "MSFT"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			first := table.partitionFor([]byte(testCase.tenantID), []byte(testCase.instrument))
			for i := 0; i < 10; i++ {
				got := table.partitionFor([]byte(testCase.tenantID), []byte(testCase.instrument))
				if got != first {
					t.Fatalf("partitionFor(%q, %q) call %d = %d, want %d (deterministic)", testCase.tenantID, testCase.instrument, i, got, first)
				}
			}
		})
	}
}

func TestReservationTablePartitionForInstrumentsStayWithinTenantRange(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 1, "tenant-b": 4}
	table := newReservationTable(reserved, 1, 12, nil)

	instruments := []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NFLX", "META", "NVDA", "INTC", "AMD"}

	tests := []struct {
		name                   string
		tenantID               string
		wantMultiplePartitions bool
	}{
		{name: "size 1 range", tenantID: "tenant-a", wantMultiplePartitions: false},
		{name: "size 4 range", tenantID: "tenant-b", wantMultiplePartitions: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			rng := table.explicit[testCase.tenantID]
			hit := make(map[int32]bool)

			for _, instrument := range instruments {
				partition := table.partitionFor([]byte(testCase.tenantID), []byte(instrument))
				if partition < rng.start || partition >= rng.start+rng.size {
					t.Errorf("partitionFor(%q, %q) = %d, want within range %+v", testCase.tenantID, instrument, partition, rng)
				}
				hit[partition] = true
			}

			if testCase.wantMultiplePartitions && len(hit) < 2 {
				t.Errorf("tenant %q with range size %d only ever hit %d distinct partitions across %d instruments, want more than 1", testCase.tenantID, rng.size, len(hit), len(instruments))
			}
		})
	}
}

func TestReservationTablePoolExhaustionFallback(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 3}
	table := newReservationTable(reserved, 1, 4, nil)

	if table.poolSize != 1 {
		t.Fatalf("test setup: poolSize = %d, want 1 so pool exhaustion is exercised", table.poolSize)
	}

	poolTenants := []string{"tenant-c", "tenant-d", "tenant-e", "tenant-f", "tenant-g"}
	for _, tenantID := range poolTenants {
		partition := table.partitionFor([]byte(tenantID), []byte("AAPL"))
		if partition < table.poolStart || partition >= table.poolStart+table.poolSize {
			t.Errorf("partitionFor(%q, ...) = %d, want within pool range [%d, %d)", tenantID, partition, table.poolStart, table.poolStart+table.poolSize)
		}
	}
}

// TestReservationTablePoolNeverOverlapsExplicitWhenReservationsFillTable
// guards against a regression where explicit reservations consuming every
// partition forced the pool-exhaustion fallback to hash unreserved tenants
// into an explicitly reserved tenant's own range.
func TestReservationTablePoolNeverOverlapsExplicitWhenReservationsFillTable(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 2, "tenant-b": 3}
	table := newReservationTable(reserved, 1, 5, nil)

	explicitPartitions := make(map[int32]string)
	for tenantID, rng := range table.explicit {
		for partition := rng.start; partition < rng.start+rng.size; partition++ {
			explicitPartitions[partition] = tenantID
		}
	}

	poolTenants := []string{"tenant-c", "tenant-d", "tenant-e"}
	for _, tenantID := range poolTenants {
		partition := table.partitionFor([]byte(tenantID), []byte("AAPL"))
		if owner, overlapped := explicitPartitions[partition]; overlapped {
			t.Errorf("partitionFor(%q, ...) = %d, which is reserved for explicit tenant %q", tenantID, partition, owner)
		}
	}
}

// TestReservationTablePoolRangeForClipsToRemainingCapacity guards against a
// regression where a pool tenant whose defaultSize didn't fit in what was
// left of the pool immediately fell back to the shared-hash strategy, even
// though a smaller-but-still-exclusive block was available.
func TestReservationTablePoolRangeForClipsToRemainingCapacity(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 6}
	table := newReservationTable(reserved, 3, 8, nil)

	if table.poolSize != 2 {
		t.Fatalf("test setup: poolSize = %d, want 2 so a defaultSize=3 tenant can't fully fit", table.poolSize)
	}

	rng := table.poolRangeFor([]byte("tenant-b"))
	if rng.size != 2 {
		t.Errorf("poolRangeFor(tenant-b).size = %d, want 2 (clipped to remaining pool capacity)", rng.size)
	}
	if rng.start != table.poolStart {
		t.Errorf("poolRangeFor(tenant-b).start = %d, want %d", rng.start, table.poolStart)
	}
	if cached, ok := table.pool["tenant-b"]; !ok || cached != rng {
		t.Errorf("poolRangeFor(tenant-b) was not cached in table.pool, got %+v ok=%v", cached, ok)
	}

	// Pool capacity is now fully consumed by tenant-b; the next pool tenant
	// must fall back to the shared-hash strategy instead of being handed a
	// size-0 range (which would panic in partitionFor's hashBytes call).
	partition := table.partitionFor([]byte("tenant-c"), []byte("AAPL"))
	if partition < table.poolStart || partition >= table.poolStart+table.poolSize {
		t.Errorf("partitionFor(tenant-c, ...) = %d, want within pool range [%d, %d)", partition, table.poolStart, table.poolStart+table.poolSize)
	}
	if _, cached := table.pool["tenant-c"]; cached {
		t.Errorf("tenant-c was cached in table.pool, want the zero-capacity fallback to stay uncached")
	}
}

// TestNewReservationTableRecordsDroppedTenants guards against a regression
// where explicit reservations that couldn't fit at all within
// explicitCapacity were silently dropped with no way to observe the loss.
func TestNewReservationTableRecordsDroppedTenants(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 4, "tenant-b": 3, "tenant-c": 3}
	table := newReservationTable(reserved, 1, 8, nil)

	// explicitCapacity is 7 (totalPartitions-1); tenant-a and tenant-b
	// consume it in full (sorted order), leaving tenant-c dropped.
	if _, ok := table.explicit["tenant-c"]; ok {
		t.Fatalf("test setup: tenant-c unexpectedly fit within explicitCapacity")
	}

	if len(table.droppedTenants) != 1 || table.droppedTenants[0] != "tenant-c" {
		t.Errorf("droppedTenants = %v, want [tenant-c]", table.droppedTenants)
	}
}

func TestNewReservationTableNoDroppedTenantsWhenAllFit(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 2, "tenant-b": 1}
	table := newReservationTable(reserved, 1, 8, nil)

	if table.droppedTenants != nil {
		t.Errorf("droppedTenants = %v, want nil", table.droppedTenants)
	}
}

// TestNewReservationTableReportsExplicitPartitionCounts confirms building a
// table with explicit reservations reports each explicit tenant's assigned
// size on the partition-count gauge, so an operator scraping this service
// can observe ADR 0007's reservation table directly.
func TestNewReservationTableReportsExplicitPartitionCounts(t *testing.T) {
	registry := prometheus.NewRegistry()
	kafkaMetrics, err := NewMetrics(registry, metrics.NewKnownTenants("tenant-a", "tenant-b"))
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	reserved := TenantPartitionConfig{"tenant-a": 2, "tenant-b": 3}
	newReservationTable(reserved, 1, 8, kafkaMetrics)

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, `ingestion_kafka_tenant_partition_count{tenant="tenant-a"} 2`)
	requireMetricsLine(t, scraped, `ingestion_kafka_tenant_partition_count{tenant="tenant-b"} 3`)
}

// TestReservationTablePoolRangeForReportsPartitionCountOnFirstAssignment
// confirms a pool tenant's assigned size is reported on first sight (a
// cache miss) and stays correct — not doubled, not erroring — after a
// second poolRangeFor call for the same tenant with the table unchanged (a
// cache hit, which never re-invokes observePartitionCount).
func TestReservationTablePoolRangeForReportsPartitionCountOnFirstAssignment(t *testing.T) {
	registry := prometheus.NewRegistry()
	kafkaMetrics, err := NewMetrics(registry, metrics.NewKnownTenants("tenant-c"))
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	reserved := TenantPartitionConfig{"tenant-a": 2}
	table := newReservationTable(reserved, 1, 8, kafkaMetrics)

	table.poolRangeFor([]byte("tenant-c"))
	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, `ingestion_kafka_tenant_partition_count{tenant="tenant-c"} 1`)

	table.poolRangeFor([]byte("tenant-c"))
	scraped = scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, `ingestion_kafka_tenant_partition_count{tenant="tenant-c"} 1`)
}

func TestSplitPartitionKey(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		wantTenantID   string
		wantInstrument string
	}{
		{
			name:           "typical key",
			key:            "tenant-a:AAPL",
			wantTenantID:   "tenant-a",
			wantInstrument: "AAPL",
		},
		{
			name:           "no separator",
			key:            "tenant-a",
			wantTenantID:   "tenant-a",
			wantInstrument: "",
		},
		{
			name:           "empty key",
			key:            "",
			wantTenantID:   "",
			wantInstrument: "",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tenantID, instrument := splitPartitionKey([]byte(testCase.key))
			if string(tenantID) != testCase.wantTenantID {
				t.Errorf("tenantID = %q, want %q", tenantID, testCase.wantTenantID)
			}
			if string(instrument) != testCase.wantInstrument {
				t.Errorf("instrument = %q, want %q", instrument, testCase.wantInstrument)
			}
		})
	}
}

func TestTenantTopicPartitionerPartition(t *testing.T) {
	reserved := TenantPartitionConfig{"tenant-a": 2}
	partitioner := &tenantTopicPartitioner{reserved: reserved, defaultSize: 1}

	record := &kgo.Record{Key: []byte("tenant-a:AAPL")}

	for _, totalPartitions := range []int{8, 16} {
		partition := partitioner.Partition(record, totalPartitions)
		if partition < 0 || partition >= totalPartitions {
			t.Errorf("Partition(totalPartitions=%d) = %d, want within [0, %d)", totalPartitions, partition, totalPartitions)
		}
		if partitioner.table.totalPartitions != int32(totalPartitions) {
			t.Errorf("table.totalPartitions = %d, want %d", partitioner.table.totalPartitions, totalPartitions)
		}
	}
}

func TestTenantTopicPartitionerPartitionRebuildsTableOnPartitionCountChange(t *testing.T) {
	partitioner := &tenantTopicPartitioner{reserved: TenantPartitionConfig{"tenant-a": 2}, defaultSize: 1}
	record := &kgo.Record{Key: []byte("tenant-a:AAPL")}

	partitioner.Partition(record, 8)
	firstTable := partitioner.table

	partitioner.Partition(record, 16)
	secondTable := partitioner.table

	if firstTable == secondTable {
		t.Error("table was reused across a change in totalPartitions, want a rebuilt table")
	}
	if secondTable.totalPartitions != 16 {
		t.Errorf("table.totalPartitions = %d, want 16", secondTable.totalPartitions)
	}
}

func TestTenantTopicPartitionerRequiresConsistency(t *testing.T) {
	partitioner := &tenantTopicPartitioner{}
	if !partitioner.RequiresConsistency(&kgo.Record{}) {
		t.Error("RequiresConsistency() = false, want true")
	}
}
