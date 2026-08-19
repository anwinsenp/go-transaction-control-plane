package kafka

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// errFakeLoad is a sentinel Load failure used by TenantPartitionReloader
// tests exercising the error path.
var errFakeLoad = errors.New("fake load failure")

// writeTenantPartitionMapFile writes content to a fresh file in a temp
// directory and returns its path, so FileTenantPartitionSource tests don't
// need a real mounted ConfigMap.
func writeTenantPartitionMapFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mapping.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write tenant partition map file: %v", err)
	}
	return path
}

func TestFileTenantPartitionSourceLoad(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		missing    bool
		wantErr    bool
		wantConfig TenantPartitionConfig
	}{
		{
			name:       "success parses counts",
			content:    `{"tenant-a": {"start": 0, "count": 3}, "tenant-b": {"start": 3, "count": 2}}`,
			wantConfig: TenantPartitionConfig{"tenant-a": 3, "tenant-b": 2},
		},
		{
			name:    "missing file errors",
			missing: true,
			wantErr: true,
		},
		{
			name:    "malformed json errors",
			content: `{not json`,
			wantErr: true,
		},
		{
			name:    "zero count entry errors",
			content: `{"tenant-a": {"start": 0, "count": 0}}`,
			wantErr: true,
		},
		{
			name:    "negative count entry errors",
			content: `{"tenant-a": {"start": 0, "count": -1}}`,
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var path string
			if testCase.missing {
				path = filepath.Join(t.TempDir(), "does-not-exist.json")
			} else {
				path = writeTenantPartitionMapFile(t, testCase.content)
			}

			source := FileTenantPartitionSource{Path: path}
			got, err := source.Load()

			if testCase.wantErr {
				if err == nil {
					t.Fatalf("Load() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}
			if len(got) != len(testCase.wantConfig) {
				t.Fatalf("Load() = %+v, want %+v", got, testCase.wantConfig)
			}
			for tenantID, count := range testCase.wantConfig {
				if got[tenantID] != count {
					t.Errorf("Load()[%q] = %d, want %d", tenantID, got[tenantID], count)
				}
			}
		})
	}
}

// fakeTenantPartitionSource is a TenantPartitionSource test double whose
// Load result and error are set directly, so TenantPartitionReloader tests
// don't need a real file.
type fakeTenantPartitionSource struct {
	config TenantPartitionConfig
	err    error
}

func (fake fakeTenantPartitionSource) Load() (TenantPartitionConfig, error) {
	return fake.config, fake.err
}

func TestTenantPartitionReloaderReloadOnceUpdatesPartitionerOnSuccess(t *testing.T) {
	partitioner := newTenantPartitioner(TenantPartitionConfig{"tenant-a": 2}, 1, nil)
	reloader := &TenantPartitionReloader{
		Source:      fakeTenantPartitionSource{config: TenantPartitionConfig{"tenant-b": 4}},
		Partitioner: partitioner,
		DefaultSize: 1,
	}

	reloader.reloadOnce()

	topicPartitioner := partitioner.ForTopic("transaction-events").(*tenantTopicPartitioner)
	record := &kgo.Record{Key: []byte("tenant-b:AAPL")}
	partition := topicPartitioner.Partition(record, 8)

	if _, ok := topicPartitioner.table.explicit["tenant-b"]; !ok {
		t.Fatalf("table.explicit = %+v, want tenant-b reserved after reload", topicPartitioner.table.explicit)
	}
	if _, ok := topicPartitioner.table.explicit["tenant-a"]; ok {
		t.Errorf("table.explicit = %+v, want tenant-a no longer reserved after reload replaced the config", topicPartitioner.table.explicit)
	}
	rng := topicPartitioner.table.explicit["tenant-b"]
	if partition < int(rng.start) || partition >= int(rng.start+rng.size) {
		t.Errorf("Partition() = %d, want within tenant-b's reserved range %+v", partition, rng)
	}
}

func TestTenantPartitionReloaderReloadOnceLeavesPartitionerUntouchedOnError(t *testing.T) {
	registry := prometheus.NewRegistry()
	kafkaMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	initialConfig := TenantPartitionConfig{"tenant-a": 2}
	partitioner := newTenantPartitioner(initialConfig, 1, nil)
	stateBefore := partitioner.state.Load()

	reloader := &TenantPartitionReloader{
		Source:      fakeTenantPartitionSource{err: errFakeLoad},
		Partitioner: partitioner,
		DefaultSize: 1,
		Metrics:     kafkaMetrics,
	}

	reloader.reloadOnce()

	if partitioner.state.Load() != stateBefore {
		t.Error("partitioner state changed after a failed reload, want it untouched")
	}

	topicPartitioner := partitioner.ForTopic("transaction-events").(*tenantTopicPartitioner)
	record := &kgo.Record{Key: []byte("tenant-a:AAPL")}
	topicPartitioner.Partition(record, 8)
	rng, ok := topicPartitioner.table.explicit["tenant-a"]
	if !ok || rng.size != 2 {
		t.Errorf("table.explicit[tenant-a] = %+v ok=%v, want the original reservation (size 2) preserved", rng, ok)
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, "ingestion_kafka_tenant_reservation_reload_errors_total 1")
}

func TestTenantPartitionReloaderReloadOnceNilMetricsIsSafe(t *testing.T) {
	partitioner := newTenantPartitioner(TenantPartitionConfig{"tenant-a": 2}, 1, nil)
	reloader := &TenantPartitionReloader{
		Source:      fakeTenantPartitionSource{err: errFakeLoad},
		Partitioner: partitioner,
		DefaultSize: 1,
	}

	reloader.reloadOnce()
}

// TestTenantPartitionerConcurrentPartitionAndUpdateReservations drives
// Partition calls on a shared tenantTopicPartitioner concurrently with
// updateReservations calls, confirming no data race (run with -race) and
// that Partition never panics or returns an out-of-range partition despite
// the reservation state changing mid-flight.
func TestTenantPartitionerConcurrentPartitionAndUpdateReservations(t *testing.T) {
	partitioner := newTenantPartitioner(TenantPartitionConfig{"tenant-a": 2}, 1, nil)

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		count := int32(1)
		for {
			select {
			case <-stop:
				return
			default:
				partitioner.updateReservations(TenantPartitionConfig{"tenant-a": count%4 + 1}, 1)
				count++
			}
		}
	}()

	var readers sync.WaitGroup
	const readerCount = 4
	for i := 0; i < readerCount; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			topicPartitioner := partitioner.ForTopic("transaction-events").(*tenantTopicPartitioner)
			record := &kgo.Record{Key: []byte("tenant-a:AAPL")}
			for j := 0; j < 500; j++ {
				partition := topicPartitioner.Partition(record, 8)
				if partition < 0 || partition >= 8 {
					t.Errorf("Partition() = %d, want within [0, 8)", partition)
				}
			}
		}()
	}

	readers.Wait()
	close(stop)
	<-writerDone
}
