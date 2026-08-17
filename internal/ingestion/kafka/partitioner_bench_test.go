package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// discardProducerClient is a producerClient double that reports every
// record produced but never retains it, unlike fakeProducerClient's
// ever-growing slice — that growth would otherwise dominate an allocation
// benchmark with cost that belongs to the test double, not Publisher.
type discardProducerClient struct{}

func (discardProducerClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record}
	}
	return results
}

func (discardProducerClient) Close() {}

// BenchmarkTenantTopicPartitionerPartition measures the steady-state cost of
// the partitioner introduced to replace the old key-hash partitioner: once a
// tenant's reservation range has been assigned (the warm-up call below),
// repeated Partition calls for that tenant must do no new allocations, per
// CLAUDE.md's hot-path discipline.
func BenchmarkTenantTopicPartitionerPartition(b *testing.B) {
	partitioner := newTenantPartitioner(TenantPartitionConfig{"tenant-iso": 4}, 1, nil)
	topicPartitioner := partitioner.ForTopic("transaction-events")
	record := &kgo.Record{Key: []byte("tenant-a:AAPL")}

	// Warm up: the first call builds the reservation table and lazily
	// assigns tenant-a its pool range, both one-time allocations that
	// shouldn't be charged against the steady-state cost measured below.
	topicPartitioner.Partition(record, 32)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topicPartitioner.Partition(record, 32)
	}
}

// BenchmarkTenantTopicPartitionerPartitionExplicitTenant is the same
// steady-state measurement for a tenant with an explicit reservation
// (Config.TenantPartitions), the path used for isolated/noisy tenants.
func BenchmarkTenantTopicPartitionerPartitionExplicitTenant(b *testing.B) {
	partitioner := newTenantPartitioner(TenantPartitionConfig{"tenant-iso": 4}, 1, nil)
	topicPartitioner := partitioner.ForTopic("transaction-events")
	record := &kgo.Record{Key: []byte("tenant-iso:AAPL")}

	topicPartitioner.Partition(record, 32)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topicPartitioner.Partition(record, 32)
	}
}

// BenchmarkReservationTablePartitionFor isolates the reservation lookup and
// instrument hash from kgo.Record/Partitioner overhead.
func BenchmarkReservationTablePartitionFor(b *testing.B) {
	table := newReservationTable(TenantPartitionConfig{"tenant-iso": 4}, 1, 32)
	tenantID := []byte("tenant-a")
	instrument := []byte("AAPL")

	// Warm up tenant-a's pool assignment.
	table.partitionFor(tenantID, instrument)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.partitionFor(tenantID, instrument)
	}
}

// BenchmarkPublisherPublish measures Publisher.Publish's own hot-path cost
// (event marshaling and partition-key construction) against a fake
// producerClient, so allocation growth in the surrounding Publish path is
// caught independently of the real kgo.Client's internal partitioner call,
// which fakeProducerClient bypasses.
func BenchmarkPublisherPublish(b *testing.B) {
	publisher := &Publisher{client: discardProducerClient{}, topic: "transaction-events"}
	event := sampleEvent()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := publisher.Publish(ctx, event); err != nil {
			b.Fatalf("Publish() error = %v", err)
		}
	}
}
