package kafka

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
		name    string
		content string
		missing bool
		wantErr bool
		want    map[string]tenantPartitionMapEntry
	}{
		{
			name:    "success parses start and count",
			content: `{"tenant-a": {"start": 0, "count": 3}, "tenant-b": {"start": 3, "count": 2}}`,
			want: map[string]tenantPartitionMapEntry{
				"tenant-a": {Start: 0, Count: 3},
				"tenant-b": {Start: 3, Count: 2},
			},
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
			if !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("Load() = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func sortedPartitions(set map[int32]struct{}) []int32 {
	partitions := make([]int32, 0, len(set))
	for partition := range set {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions
}

func TestPartitionsForEntry(t *testing.T) {
	tests := []struct {
		name    string
		entry   tenantPartitionMapEntry
		want    []int32
		wantErr bool
	}{
		{
			name:  "typical range expands to explicit indices",
			entry: tenantPartitionMapEntry{Start: 2, Count: 3},
			want:  []int32{2, 3, 4},
		},
		{
			name:  "single partition",
			entry: tenantPartitionMapEntry{Start: 0, Count: 1},
			want:  []int32{0},
		},
		{
			name:    "zero count errors",
			entry:   tenantPartitionMapEntry{Start: 0, Count: 0},
			wantErr: true,
		},
		{
			name:    "negative count errors",
			entry:   tenantPartitionMapEntry{Start: 0, Count: -1},
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := partitionsForEntry(testCase.entry)

			if testCase.wantErr {
				if err == nil {
					t.Fatalf("partitionsForEntry() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("partitionsForEntry() error = %v, want nil", err)
			}
			if got := sortedPartitions(got); !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("partitionsForEntry() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func partitionSet(partitions ...int32) map[int32]struct{} {
	set := make(map[int32]struct{}, len(partitions))
	for _, partition := range partitions {
		set[partition] = struct{}{}
	}
	return set
}

// TestConsumerApplyAssignment drives applyAssignment through a range shift
// and a no-change call, asserting both on the resulting Consumer.assignment
// and on exactly what was passed to the fake client's
// AddConsumePartitions/RemoveConsumePartitions calls (call count, ordering,
// and diff contents) — the fakeConsumerClient's added/removed tracking
// fields exist specifically to make that observable.
func TestConsumerApplyAssignment(t *testing.T) {
	t.Run("range shift adds before removing and updates assignment", func(t *testing.T) {
		fake := &fakeConsumerClient{}
		consumer := &Consumer{client: fake, assignment: partitionSet(0, 1)}

		consumer.applyAssignment("transaction-events", partitionSet(1, 2))

		if len(fake.added) != 1 {
			t.Fatalf("AddConsumePartitions called %d times, want 1", len(fake.added))
		}
		if len(fake.removed) != 1 {
			t.Fatalf("RemoveConsumePartitions called %d times, want 1", len(fake.removed))
		}

		wantAdded := map[string]map[int32]kgo.Offset{
			"transaction-events": {2: kgo.NewOffset().AtStart()},
		}
		if !reflect.DeepEqual(fake.added[0], wantAdded) {
			t.Errorf("AddConsumePartitions arg = %+v, want %+v", fake.added[0], wantAdded)
		}

		wantRemoved := map[string][]int32{"transaction-events": {0}}
		if !reflect.DeepEqual(fake.removed[0], wantRemoved) {
			t.Errorf("RemoveConsumePartitions arg = %+v, want %+v", fake.removed[0], wantRemoved)
		}

		wantAssignment := partitionSet(1, 2)
		if !reflect.DeepEqual(consumer.assignment, wantAssignment) {
			t.Errorf("consumer.assignment = %v, want %v", consumer.assignment, wantAssignment)
		}
	})

	t.Run("no change calls neither Add nor Remove", func(t *testing.T) {
		fake := &fakeConsumerClient{}
		original := partitionSet(0, 1)
		consumer := &Consumer{client: fake, assignment: original}

		consumer.applyAssignment("transaction-events", partitionSet(0, 1))

		if len(fake.added) != 0 {
			t.Errorf("AddConsumePartitions called %d times, want 0", len(fake.added))
		}
		if len(fake.removed) != 0 {
			t.Errorf("RemoveConsumePartitions called %d times, want 0", len(fake.removed))
		}
		if !reflect.DeepEqual(consumer.assignment, original) {
			t.Errorf("consumer.assignment = %v, want unchanged %v", consumer.assignment, original)
		}
	})

	t.Run("only additions calls Add but not Remove", func(t *testing.T) {
		fake := &fakeConsumerClient{}
		consumer := &Consumer{client: fake, assignment: partitionSet(0)}

		consumer.applyAssignment("transaction-events", partitionSet(0, 1))

		if len(fake.added) != 1 {
			t.Fatalf("AddConsumePartitions called %d times, want 1", len(fake.added))
		}
		if len(fake.removed) != 0 {
			t.Errorf("RemoveConsumePartitions called %d times, want 0", len(fake.removed))
		}
		wantAdded := map[string]map[int32]kgo.Offset{
			"transaction-events": {1: kgo.NewOffset().AtStart()},
		}
		if !reflect.DeepEqual(fake.added[0], wantAdded) {
			t.Errorf("AddConsumePartitions arg = %+v, want %+v", fake.added[0], wantAdded)
		}
	})

	t.Run("only removals calls Remove but not Add", func(t *testing.T) {
		fake := &fakeConsumerClient{}
		consumer := &Consumer{client: fake, assignment: partitionSet(0, 1)}

		consumer.applyAssignment("transaction-events", partitionSet(0))

		if len(fake.added) != 0 {
			t.Errorf("AddConsumePartitions called %d times, want 0", len(fake.added))
		}
		if len(fake.removed) != 1 {
			t.Fatalf("RemoveConsumePartitions called %d times, want 1", len(fake.removed))
		}
		wantRemoved := map[string][]int32{"transaction-events": {1}}
		if !reflect.DeepEqual(fake.removed[0], wantRemoved) {
			t.Errorf("RemoveConsumePartitions arg = %+v, want %+v", fake.removed[0], wantRemoved)
		}
	})
}

// fakeTenantPartitionSource is a TenantPartitionSource test double whose
// Load result and error are set directly, so TenantPartitionReloader tests
// don't need a real file.
type fakeTenantPartitionSource struct {
	entries map[string]tenantPartitionMapEntry
	err     error
}

func (fake fakeTenantPartitionSource) Load() (map[string]tenantPartitionMapEntry, error) {
	return fake.entries, fake.err
}

func TestTenantPartitionReloaderReloadOnceUpdatesAssignmentOnSuccess(t *testing.T) {
	fake := &fakeConsumerClient{}
	consumer := &Consumer{client: fake, assignment: partitionSet(0, 1)}
	reloader := &TenantPartitionReloader{
		Source: fakeTenantPartitionSource{entries: map[string]tenantPartitionMapEntry{
			"tenant-a": {Start: 1, Count: 2},
		}},
		Consumer: consumer,
		TenantID: "tenant-a",
		Topic:    "transaction-events",
	}

	reloader.reloadOnce()

	wantAssignment := partitionSet(1, 2)
	if !reflect.DeepEqual(consumer.assignment, wantAssignment) {
		t.Errorf("consumer.assignment = %v, want %v", consumer.assignment, wantAssignment)
	}
}

// TestTenantPartitionReloaderReloadOnceMissingTenantLeavesAssignmentUnchanged
// confirms a loaded mapping missing TenantID's entry leaves Consumer's
// assignment untouched and increments Metrics.partitionReloadErrorsTotal.
func TestTenantPartitionReloaderReloadOnceMissingTenantLeavesAssignmentUnchanged(t *testing.T) {
	registry := prometheus.NewRegistry()
	processorMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	fake := &fakeConsumerClient{}
	original := partitionSet(0, 1)
	consumer := &Consumer{client: fake, assignment: original}
	reloader := &TenantPartitionReloader{
		Source: fakeTenantPartitionSource{entries: map[string]tenantPartitionMapEntry{
			"tenant-other": {Start: 0, Count: 1},
		}},
		Consumer: consumer,
		TenantID: "tenant-a",
		Topic:    "transaction-events",
		Metrics:  processorMetrics,
	}

	reloader.reloadOnce()

	if !reflect.DeepEqual(consumer.assignment, original) {
		t.Errorf("consumer.assignment = %v, want unchanged %v", consumer.assignment, original)
	}
	if len(fake.added) != 0 || len(fake.removed) != 0 {
		t.Errorf("client Add/Remove called (added=%v removed=%v), want neither called", fake.added, fake.removed)
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, "processor_kafka_tenant_partition_reload_errors_total 1")
}

// TestTenantPartitionReloaderReloadOnceLoadFailureLeavesAssignmentUnchanged
// mirrors the missing-tenant case but for a Source.Load failure (e.g.
// unreadable/malformed file), confirming the same "keep last-known-good"
// contract.
func TestTenantPartitionReloaderReloadOnceLoadFailureLeavesAssignmentUnchanged(t *testing.T) {
	registry := prometheus.NewRegistry()
	processorMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}

	fake := &fakeConsumerClient{}
	original := partitionSet(0, 1)
	consumer := &Consumer{client: fake, assignment: original}
	reloader := &TenantPartitionReloader{
		Source:   fakeTenantPartitionSource{err: errFakeLoad},
		Consumer: consumer,
		TenantID: "tenant-a",
		Topic:    "transaction-events",
		Metrics:  processorMetrics,
	}

	reloader.reloadOnce()

	if !reflect.DeepEqual(consumer.assignment, original) {
		t.Errorf("consumer.assignment = %v, want unchanged %v", consumer.assignment, original)
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, "processor_kafka_tenant_partition_reload_errors_total 1")
}

func TestConfigValidateTenantPartitionSourceRequiresManualPartitionsAndTenantID(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "missing TenantID is rejected",
			config: Config{
				Brokers:               []string{"localhost:9092"},
				Topic:                 "t",
				DLQTopic:              "dlq",
				ManualPartitions:      []int32{0, 1},
				TenantPartitionSource: fakeTenantPartitionSource{},
			},
			wantErr: true,
		},
		{
			name: "missing ManualPartitions is rejected",
			config: Config{
				Brokers:               []string{"localhost:9092"},
				Topic:                 "t",
				DLQTopic:              "dlq",
				TenantID:              "tenant-a",
				TenantPartitionSource: fakeTenantPartitionSource{},
			},
			wantErr: true,
		},
		{
			name: "both set is accepted",
			config: Config{
				Brokers:               []string{"localhost:9092"},
				Topic:                 "t",
				DLQTopic:              "dlq",
				ManualPartitions:      []int32{0, 1},
				TenantID:              "tenant-a",
				TenantPartitionSource: fakeTenantPartitionSource{},
			},
			wantErr: false,
		},
		{
			name: "nil source is unaffected by TenantID/ManualPartitions being unset",
			config: Config{
				Brokers:  []string{"localhost:9092"},
				Topic:    "t",
				GroupID:  "g",
				DLQTopic: "dlq",
			},
			wantErr: false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.config.validate()
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("validate() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Errorf("validate() error = %v, want nil", err)
			}
		})
	}
}
