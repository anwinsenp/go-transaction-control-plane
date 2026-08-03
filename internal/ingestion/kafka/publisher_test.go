package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// fakeProducerClient is a producerClient test double that records the
// records it was asked to produce and returns a caller-configured result,
// so Publisher's behavior can be tested without a live Kafka broker.
type fakeProducerClient struct {
	produced []*kgo.Record
	err      error
	closed   bool
}

func (fake *fakeProducerClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	fake.produced = append(fake.produced, records...)

	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: fake.err}
	}
	return results
}

func (fake *fakeProducerClient) Close() {
	fake.closed = true
}

func sampleEvent() ingestion.Event {
	occurredAt, err := time.Parse(time.RFC3339, "2026-08-03T12:00:00Z")
	if err != nil {
		panic(err)
	}
	return ingestion.Event{
		EventID:       uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		TenantID:      "tenant-a",
		SchemaVersion: ingestion.CurrentSchemaVersion,
		Instrument:    "AAPL",
		Side:          ledger.SideBuy,
		Quantity:      10 * ledger.AmountScale,
		Price:         15025000000, // 150.25
		Currency:      "USD",
		OccurredAt:    occurredAt,
	}
}

func TestPublisherPublishSuccess(t *testing.T) {
	fake := &fakeProducerClient{}
	publisher := &Publisher{client: fake, topic: "transaction-events"}

	event := sampleEvent()
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}

	if len(fake.produced) != 1 {
		t.Fatalf("produced %d records, want 1", len(fake.produced))
	}

	record := fake.produced[0]
	if record.Topic != "transaction-events" {
		t.Errorf("record.Topic = %q, want %q", record.Topic, "transaction-events")
	}

	wantKey := "tenant-a:AAPL"
	if string(record.Key) != wantKey {
		t.Errorf("record.Key = %q, want %q", record.Key, wantKey)
	}

	var decoded wireEvent
	if err := json.Unmarshal(record.Value, &decoded); err != nil {
		t.Fatalf("decode record.Value: %v", err)
	}
	want := wireEvent{
		EventID:       "11111111-1111-1111-1111-111111111111",
		TenantID:      "tenant-a",
		SchemaVersion: ingestion.CurrentSchemaVersion,
		Instrument:    "AAPL",
		Side:          "BUY",
		Quantity:      "10.00000000",
		Price:         "150.25000000",
		Currency:      "USD",
		OccurredAt:    "2026-08-03T12:00:00Z",
	}
	if decoded != want {
		t.Errorf("decoded payload = %+v, want %+v", decoded, want)
	}
}

// TestPublisherPublishWrapsBrokerError ensures a broker-side produce
// failure surfaces to the caller as a wrapped error identifying the topic,
// rather than being swallowed.
func TestPublisherPublishWrapsBrokerError(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	fake := &fakeProducerClient{err: brokerErr}
	publisher := &Publisher{client: fake, topic: "transaction-events"}

	err := publisher.Publish(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("Publish() error = nil, want non-nil")
	}
	if !errors.Is(err, brokerErr) {
		t.Errorf("Publish() error = %v, want it to wrap %v", err, brokerErr)
	}
	if !strings.Contains(err.Error(), "transaction-events") {
		t.Errorf("Publish() error = %v, want it to name the topic", err)
	}
}

func TestPublisherClose(t *testing.T) {
	fake := &fakeProducerClient{}
	publisher := &Publisher{client: fake, topic: "transaction-events"}

	publisher.Close()

	if !fake.closed {
		t.Error("Close() did not close the underlying client")
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v, want nil", err)
	}
	want := LocalConfig()
	if len(config.Brokers) != 1 || config.Brokers[0] != want.Brokers[0] {
		t.Errorf("Brokers = %v, want %v", config.Brokers, want.Brokers)
	}
	if config.Topic != want.Topic {
		t.Errorf("Topic = %q, want %q", config.Topic, want.Topic)
	}
	if config.RequestTimeout != want.RequestTimeout {
		t.Errorf("RequestTimeout = %s, want %s", config.RequestTimeout, want.RequestTimeout)
	}
	if config.Linger != want.Linger {
		t.Errorf("Linger = %s, want %s", config.Linger, want.Linger)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker-a:9092,broker-b:9092")
	t.Setenv("KAFKA_TOPIC", "custom-events")
	t.Setenv("KAFKA_REQUEST_TIMEOUT", "2s")
	t.Setenv("KAFKA_LINGER", "10ms")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v, want nil", err)
	}

	wantBrokers := []string{"broker-a:9092", "broker-b:9092"}
	if len(config.Brokers) != len(wantBrokers) {
		t.Fatalf("Brokers = %v, want %v", config.Brokers, wantBrokers)
	}
	for i, broker := range wantBrokers {
		if config.Brokers[i] != broker {
			t.Errorf("Brokers[%d] = %q, want %q", i, config.Brokers[i], broker)
		}
	}
	if config.Topic != "custom-events" {
		t.Errorf("Topic = %q, want %q", config.Topic, "custom-events")
	}
	if config.RequestTimeout != 2*time.Second {
		t.Errorf("RequestTimeout = %s, want %s", config.RequestTimeout, 2*time.Second)
	}
	if config.Linger != 10*time.Millisecond {
		t.Errorf("Linger = %s, want %s", config.Linger, 10*time.Millisecond)
	}
}

func TestConfigFromEnvInvalidDuration(t *testing.T) {
	t.Setenv("KAFKA_REQUEST_TIMEOUT", "not-a-duration")

	if _, err := ConfigFromEnv(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("ConfigFromEnv() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{
			name:   "no brokers",
			config: Config{Topic: "t", RequestTimeout: time.Second},
		},
		{
			name:   "blank broker",
			config: Config{Brokers: []string{" "}, Topic: "t", RequestTimeout: time.Second},
		},
		{
			name:   "blank topic",
			config: Config{Brokers: []string{"localhost:9092"}, RequestTimeout: time.Second},
		},
		{
			name:   "non-positive request timeout",
			config: Config{Brokers: []string{"localhost:9092"}, Topic: "t"},
		},
		{
			name:   "negative linger",
			config: Config{Brokers: []string{"localhost:9092"}, Topic: "t", RequestTimeout: time.Second, Linger: -time.Millisecond},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.config.validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("validate() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
			}
		})
	}
}

func TestNewPublisherRejectsInvalidConfig(t *testing.T) {
	if _, err := NewPublisher(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("NewPublisher() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
	}
}

// TestNewPublisherValidConfig ensures a valid Config yields a usable
// Publisher without dialing a broker: kgo.NewClient only connects lazily
// once a request is produced (confirmed via godoc), so this stays a fast
// unit test rather than requiring live Kafka infrastructure.
func TestNewPublisherValidConfig(t *testing.T) {
	config := Config{
		Brokers:        []string{"127.0.0.1:9"},
		Topic:          "transaction-events",
		RequestTimeout: time.Second,
		Linger:         time.Millisecond,
	}

	publisher, err := NewPublisher(config)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v, want nil", err)
	}
	if publisher == nil {
		t.Fatal("NewPublisher() returned nil publisher, want non-nil")
	}
	defer publisher.Close()

	if publisher.topic != config.Topic {
		t.Errorf("publisher.topic = %q, want %q", publisher.topic, config.Topic)
	}
}

func TestPartitionKey(t *testing.T) {
	tests := []struct {
		name       string
		tenantID   string
		instrument string
		want       string
	}{
		{
			name:       "typical tenant and instrument",
			tenantID:   "tenant-a",
			instrument: "AAPL",
			want:       "tenant-a:AAPL",
		},
		{
			name:       "empty tenant",
			tenantID:   "",
			instrument: "AAPL",
			want:       ":AAPL",
		},
		{
			name:       "empty instrument",
			tenantID:   "tenant-a",
			instrument: "",
			want:       "tenant-a:",
		},
		{
			name:       "both empty",
			tenantID:   "",
			instrument: "",
			want:       ":",
		},
		{
			name:       "component containing separator is not escaped",
			tenantID:   "tenant:a",
			instrument: "b",
			want:       "tenant:a:b",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := partitionKey(testCase.tenantID, testCase.instrument)
			if got != testCase.want {
				t.Errorf("partitionKey(%q, %q) = %q, want %q", testCase.tenantID, testCase.instrument, got, testCase.want)
			}
		})
	}
}

// TestProduceResultsFirstErrDetectsAnyRecordError guards Publish's reliance
// on kgo.ProduceResults.FirstErr(): even though Publish only ever produces
// one record today, this pins the contract that FirstErr() surfaces an
// error regardless of which result in the slice carries it, so a future
// change to produce multiple records at once can't silently swallow a
// non-first failure.
func TestProduceResultsFirstErrDetectsAnyRecordError(t *testing.T) {
	secondRecordErr := errors.New("second record failed")

	firstRecord := &kgo.Record{Topic: "transaction-events", Value: []byte("first")}
	secondRecord := &kgo.Record{Topic: "transaction-events", Value: []byte("second")}

	results := kgo.ProduceResults{
		{Record: firstRecord, Err: nil},
		{Record: secondRecord, Err: secondRecordErr},
	}

	if err := results.FirstErr(); !errors.Is(err, secondRecordErr) {
		t.Errorf("FirstErr() = %v, want errors.Is(err, secondRecordErr)", err)
	}
}
