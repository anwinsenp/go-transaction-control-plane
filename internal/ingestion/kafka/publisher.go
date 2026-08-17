// Package kafka provides the Kafka-backed implementation of the
// ingestion.Publisher port, built on github.com/twmb/franz-go.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// ErrInvalidConfig indicates a Config failed validation.
var ErrInvalidConfig = errors.New("invalid kafka producer config")

// Config controls how the ingestion service's Kafka producer connects and
// how durably it writes. Every producer built for this service goes through
// Config rather than kgo's raw option list, so the durability/latency
// trade-off documented below is made in exactly one place.
type Config struct {
	// Brokers is the seed broker list (host:port), used for the client's
	// initial cluster metadata lookup.
	Brokers []string
	// Topic is the transaction-events topic every publish writes to.
	Topic string
	// RequestTimeout bounds how long a single produce request waits on the
	// broker before failing, so a stalled broker can't block a request
	// goroutine indefinitely.
	RequestTimeout time.Duration
	// Linger batches records produced within this window into a single
	// request, trading a small amount of added latency for materially
	// higher throughput under load. ProduceSync (used by Publish) cancels
	// any pending linger and drains immediately once called, so this only
	// helps when multiple publishes are in flight concurrently.
	Linger time.Duration
	// TenantPartitions reserves a fixed, exclusive block of partitions for
	// specific tenants (e.g. ones the operator has flagged noisy/isolated
	// per ADR 0007), instead of the DefaultPartitionsPerTenant every other
	// tenant gets. Values must be >= 1. Nil/empty means every tenant uses
	// the default.
	TenantPartitions TenantPartitionConfig
	// DefaultPartitionsPerTenant is how many partitions a tenant not
	// listed in TenantPartitions is confined to. Zero defaults to
	// DefaultPartitionsPerTenant (1) in NewPublisher.
	DefaultPartitionsPerTenant int32
}

// LocalConfig returns producer settings sized for the docker-compose local
// stack: a single-broker Kafka instance with no other tenants, so requests
// don't need generous timeouts or heavy batching.
func LocalConfig() Config {
	return Config{
		Brokers:        []string{"localhost:9092"},
		Topic:          "transaction-events",
		RequestTimeout: 10 * time.Second,
		Linger:         5 * time.Millisecond,
	}
}

// ConfigFromEnv resolves a Config from KAFKA_BROKERS (comma-separated,
// defaults to LocalConfig's single local broker), KAFKA_TOPIC (defaults to
// "transaction-events"), KAFKA_REQUEST_TIMEOUT, and KAFKA_LINGER (both
// accepting any format understood by time.ParseDuration, e.g. "10s").
func ConfigFromEnv() (Config, error) {
	config := LocalConfig()

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		config.Brokers = strings.Split(brokers, ",")
	}
	if topic := os.Getenv("KAFKA_TOPIC"); topic != "" {
		config.Topic = topic
	}
	if err := overrideDurationFromEnv("KAFKA_REQUEST_TIMEOUT", &config.RequestTimeout); err != nil {
		return Config{}, err
	}
	if err := overrideDurationFromEnv("KAFKA_LINGER", &config.Linger); err != nil {
		return Config{}, err
	}

	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (cfg Config) validate() error {
	if len(cfg.Brokers) == 0 {
		return fmt.Errorf("%w: at least one broker is required", ErrInvalidConfig)
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("%w: broker address must not be blank", ErrInvalidConfig)
		}
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return fmt.Errorf("%w: topic must not be blank", ErrInvalidConfig)
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("%w: RequestTimeout %s must be a positive duration", ErrInvalidConfig, cfg.RequestTimeout)
	}
	if cfg.Linger < 0 {
		return fmt.Errorf("%w: Linger %s must not be negative", ErrInvalidConfig, cfg.Linger)
	}
	if cfg.DefaultPartitionsPerTenant < 0 {
		return fmt.Errorf("%w: DefaultPartitionsPerTenant %d must not be negative", ErrInvalidConfig, cfg.DefaultPartitionsPerTenant)
	}
	for tenantID, size := range cfg.TenantPartitions {
		if strings.TrimSpace(tenantID) == "" {
			return fmt.Errorf("%w: TenantPartitions key must not be blank", ErrInvalidConfig)
		}
		if size < 1 {
			return fmt.Errorf("%w: TenantPartitions[%q] = %d must be >= 1", ErrInvalidConfig, tenantID, size)
		}
	}
	return nil
}

func overrideDurationFromEnv(name string, dest *time.Duration) error {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%w: parse %s: %w", ErrInvalidConfig, name, err)
	}
	*dest = parsed
	return nil
}

// producerClient is the subset of *kgo.Client that Publisher depends on,
// so tests can substitute a fake without a live Kafka broker.
type producerClient interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	Close()
}

// Publisher publishes ingestion.Event values to Kafka. It implements
// ingestion.Publisher.
type Publisher struct {
	client producerClient
	topic  string
}

var _ ingestion.Publisher = (*Publisher)(nil)

// NewPublisher builds a Publisher connected to the brokers in cfg,
// reporting the tenant partition reservation table's metrics (see
// NewMetrics) on reg. knownTenants bounds which tenant IDs are used verbatim
// as the partition-count gauge's "tenant" label value (see
// metrics.KnownTenants). Durability is set to wait for acknowledgment from
// all in-sync replicas (RequiredAcks(AllISRAcks())) rather than just the
// partition leader — a transaction event lost after a leader crash but
// before ISR replication would silently vanish from the ledger, which this
// service's correctness goals don't allow trading away for lower publish
// latency.
func NewPublisher(cfg Config, reg prometheus.Registerer, knownTenants metrics.KnownTenants) (*Publisher, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("new kafka publisher: %w", err)
	}
	if reg == nil {
		return nil, fmt.Errorf("new kafka publisher: reg must not be nil")
	}

	kafkaMetrics, err := NewMetrics(reg, knownTenants)
	if err != nil {
		return nil, fmt.Errorf("new kafka publisher: %w", err)
	}

	defaultPartitions := cfg.DefaultPartitionsPerTenant
	if defaultPartitions == 0 {
		defaultPartitions = DefaultPartitionsPerTenant
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("ingestion"),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProduceRequestTimeout(cfg.RequestTimeout),
		kgo.ProducerLinger(cfg.Linger),
		kgo.RecordPartitioner(newTenantPartitioner(cfg.TenantPartitions, defaultPartitions, kafkaMetrics)),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka publisher: create client: %w", err)
	}

	return &Publisher{client: client, topic: cfg.Topic}, nil
}

// wireEvent is the JSON shape written to the Kafka payload. It's kept
// distinct from ingestion.Event so the wire encoding (plain strings for
// decimal/UUID/time fields) can be pinned independently of the domain
// type's Go field types.
type wireEvent struct {
	EventID       string `json:"event_id"`
	TenantID      string `json:"tenant_id"`
	SchemaVersion int16  `json:"schema_version"`
	Instrument    string `json:"instrument"`
	Side          string `json:"side"`
	Quantity      string `json:"quantity"`
	Price         string `json:"price"`
	Currency      string `json:"currency"`
	OccurredAt    string `json:"occurred_at"`
}

// partitionKey returns the Kafka record key for an event: tenant and
// instrument together. tenantPartitioner (the client's RecordPartitioner)
// parses this key back into its two components to pick a partition within
// the tenant's reserved range, so every event for a given tenant's
// instrument lands on the same partition and is observed by the processor
// in publish order, which idempotent P&L reconciliation depends on. The
// two components are joined without escaping, so this relies on
// internal/api's validation charsets (tenant_id: lowercase
// alphanumeric/hyphen; instrument: uppercase alphanumeric/dot) never
// allowing ':' — if that constraint changes, this key could collide across
// tenant/instrument boundaries.
func partitionKey(tenantID, instrument string) string {
	return tenantID + ":" + instrument
}

// Publish serializes event and writes it to the configured topic, waiting
// synchronously for the broker's acknowledgment. A non-nil error means the
// event was not durably published and the caller must not treat it as
// accepted.
func (pub *Publisher) Publish(ctx context.Context, event ingestion.Event) error {
	payload, err := json.Marshal(wireEvent{
		EventID:       event.EventID.String(),
		TenantID:      event.TenantID,
		SchemaVersion: event.SchemaVersion,
		Instrument:    event.Instrument,
		Side:          string(event.Side),
		Quantity:      ledger.FormatAmount(event.Quantity),
		Price:         ledger.FormatAmount(event.Price),
		Currency:      event.Currency,
		OccurredAt:    event.OccurredAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("publish transaction event: marshal payload: %w", err)
	}

	record := &kgo.Record{
		Topic: pub.topic,
		Key:   []byte(partitionKey(event.TenantID, event.Instrument)),
		Value: payload,
	}

	results := pub.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish transaction event to kafka topic %q: %w", pub.topic, err)
	}
	return nil
}

// Close releases the underlying Kafka client's connections, flushing any
// buffered records first.
func (pub *Publisher) Close() {
	pub.client.Close()
}
