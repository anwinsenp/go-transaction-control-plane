// Package kafka provides the Kafka-backed consumer for the processor
// service: it connects to and reads from the configured transaction topic,
// decodes each record's wire payload, and hands the resulting transaction
// to a reconciler for idempotent P&L reconciliation. A record that can't be
// decoded, or whose reconciliation keeps failing past the configured retry
// bound, is routed to a dead-letter topic with failure context rather than
// blocking or silently dropping subsequent records.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// ErrInvalidConfig indicates a Config failed validation.
var ErrInvalidConfig = errors.New("invalid kafka consumer config")

// Config controls how the processor's Kafka consumer connects and which
// topic and group it consumes as. Every consumer built for this service
// goes through Config rather than kgo's raw option list, so connection
// settings are made in exactly one place.
type Config struct {
	// Brokers is the seed broker list (host:port), used for the client's
	// initial cluster metadata lookup.
	Brokers []string
	// Topic is the transaction-events topic every fetch reads from.
	Topic string
	// GroupID is the consumer group the processor joins, so multiple
	// processor replicas can share the topic's partitions.
	GroupID string
	// DLQTopic is where a record is published, with failure context, once
	// it can't be decoded or exhausts MaxRetries during reconciliation.
	DLQTopic string
	// MaxRetries bounds how many additional attempts a record's
	// reconciliation gets after its first failure before it's routed to
	// DLQTopic instead. Zero means a single attempt with no retries.
	MaxRetries int
}

// LocalConfig returns consumer settings sized for the docker-compose local
// stack: a single-broker Kafka instance with no other tenants, consuming
// the same topic the ingestion service publishes to.
func LocalConfig() Config {
	return Config{
		Brokers:    []string{"localhost:9092"},
		Topic:      "transaction-events",
		GroupID:    "processor",
		DLQTopic:   "transaction-events-dlq",
		MaxRetries: 3,
	}
}

// ConfigFromEnv resolves a Config from KAFKA_BROKERS (comma-separated,
// defaults to LocalConfig's single local broker), KAFKA_TOPIC (defaults to
// "transaction-events"), KAFKA_CONSUMER_GROUP (defaults to "processor"),
// KAFKA_DLQ_TOPIC (defaults to "transaction-events-dlq"), and
// KAFKA_MAX_RETRIES (a non-negative integer, defaults to 3).
func ConfigFromEnv() (Config, error) {
	config := LocalConfig()

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		config.Brokers = strings.Split(brokers, ",")
	}
	if topic := os.Getenv("KAFKA_TOPIC"); topic != "" {
		config.Topic = topic
	}
	if groupID := os.Getenv("KAFKA_CONSUMER_GROUP"); groupID != "" {
		config.GroupID = groupID
	}
	if dlqTopic := os.Getenv("KAFKA_DLQ_TOPIC"); dlqTopic != "" {
		config.DLQTopic = dlqTopic
	}
	if maxRetries := os.Getenv("KAFKA_MAX_RETRIES"); maxRetries != "" {
		parsed, err := strconv.Atoi(maxRetries)
		if err != nil {
			return Config{}, fmt.Errorf("%w: parse KAFKA_MAX_RETRIES: %w", ErrInvalidConfig, err)
		}
		config.MaxRetries = parsed
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
	if strings.TrimSpace(cfg.GroupID) == "" {
		return fmt.Errorf("%w: group ID must not be blank", ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.DLQTopic) == "" {
		return fmt.Errorf("%w: DLQ topic must not be blank", ErrInvalidConfig)
	}
	if cfg.MaxRetries < 0 {
		return fmt.Errorf("%w: MaxRetries must not be negative", ErrInvalidConfig)
	}
	return nil
}

// kafkaClient is the subset of *kgo.Client that Consumer depends on, so
// tests can substitute a fake without a live Kafka broker. It covers both
// consuming (PollFetches) and producing (ProduceSync), since Consumer
// reuses its single Kafka connection to publish failed records to the DLQ
// topic rather than opening a second client.
type kafkaClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	Close()
}

// reconciler applies a decoded transaction to reconciled P&L state.
// Satisfied by *processor.Reconciler; declared locally so this package
// doesn't need to import internal/processor just for the type name.
type reconciler interface {
	Reconcile(ctx context.Context, txn ledger.Transaction) error
}

// Consumer reads records from the processor's transaction topic, decodes
// each record's wire payload, and reconciles it via rec. A record that
// can't be decoded, or whose reconciliation keeps failing past maxRetries,
// is published to dlqTopic instead of blocking or dropping the record.
type Consumer struct {
	client     kafkaClient
	topic      string
	dlqTopic   string
	maxRetries int
	rec        reconciler
}

// NewConsumer builds a Consumer connected to the brokers in cfg, joined to
// cfg.GroupID and subscribed to cfg.Topic. Every decoded record is handed to
// rec for idempotent reconciliation; records that fail past cfg.MaxRetries
// are published to cfg.DLQTopic.
func NewConsumer(cfg Config, rec reconciler) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("new kafka consumer: %w", err)
	}

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("processor"),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.GroupID),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka consumer: create client: %w", err)
	}

	return &Consumer{
		client:     kafkaClient,
		topic:      cfg.Topic,
		dlqTopic:   cfg.DLQTopic,
		maxRetries: cfg.MaxRetries,
		rec:        rec,
	}, nil
}

// wireEvent is the JSON shape read from the Kafka payload, matching
// internal/ingestion/kafka's wireEvent encoding.
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

// decodeTransaction parses a Kafka record's JSON payload into a
// ledger.Transaction ready to be reconciled.
func decodeTransaction(value []byte) (ledger.Transaction, error) {
	var wire wireEvent
	if err := json.Unmarshal(value, &wire); err != nil {
		return ledger.Transaction{}, fmt.Errorf("decode transaction event: unmarshal payload: %w", err)
	}

	eventID, err := uuid.Parse(wire.EventID)
	if err != nil {
		return ledger.Transaction{}, fmt.Errorf("decode transaction event: parse event id: %w", err)
	}

	quantity, err := ledger.ParseAmount(wire.Quantity)
	if err != nil {
		return ledger.Transaction{}, fmt.Errorf("decode transaction event: parse quantity: %w", err)
	}
	price, err := ledger.ParseAmount(wire.Price)
	if err != nil {
		return ledger.Transaction{}, fmt.Errorf("decode transaction event: parse price: %w", err)
	}

	occurredAt, err := time.Parse(time.RFC3339Nano, wire.OccurredAt)
	if err != nil {
		return ledger.Transaction{}, fmt.Errorf("decode transaction event: parse occurred_at: %w", err)
	}

	return ledger.Transaction{
		EventID:       eventID,
		TenantID:      wire.TenantID,
		SchemaVersion: wire.SchemaVersion,
		Instrument:    wire.Instrument,
		Side:          ledger.Side(wire.Side),
		Quantity:      quantity,
		Price:         price,
		Currency:      wire.Currency,
		OccurredAt:    occurredAt,
	}, nil
}

// dlqEvent is the wire shape published to the DLQ topic for a record that
// couldn't be decoded, or whose reconciliation kept failing past
// maxRetries: the original record's location and payload, plus enough
// failure context (reason, attempt count, timestamp) to diagnose and
// potentially replay it.
type dlqEvent struct {
	OriginalTopic     string `json:"original_topic"`
	OriginalPartition int32  `json:"original_partition"`
	OriginalOffset    int64  `json:"original_offset"`
	Payload           string `json:"payload"`
	FailureReason     string `json:"failure_reason"`
	Attempts          int    `json:"attempts"`
	FailedAt          string `json:"failed_at"`
}

// publishToDLQ publishes record to the configured DLQ topic along with
// failureErr and the number of attempts made, preserving record's original
// partition key so downstream tooling can still group failures by
// tenant/instrument.
func (con *Consumer) publishToDLQ(ctx context.Context, record *kgo.Record, failureErr error, attempts int) error {
	event := dlqEvent{
		OriginalTopic:     record.Topic,
		OriginalPartition: record.Partition,
		OriginalOffset:    record.Offset,
		Payload:           string(record.Value),
		FailureReason:     failureErr.Error(),
		Attempts:          attempts,
		FailedAt:          time.Now().UTC().Format(time.RFC3339Nano),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("publish to dlq: marshal failure event: %w", err)
	}

	dlqRecord := &kgo.Record{Topic: con.dlqTopic, Key: record.Key, Value: payload}
	results := con.client.ProduceSync(ctx, dlqRecord)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish to dlq topic %q: %w", con.dlqTopic, err)
	}
	return nil
}

// reconcileWithRetry calls con.rec.Reconcile up to con.maxRetries+1 times,
// stopping at the first success. It returns the total number of attempts
// made and the last error seen (nil on success). Retries are attempted
// back-to-back with no delay: this workload's reconciliation failures are
// expected to be transient contention rather than sustained outages, so a
// backoff would only slow down recovery, not help it.
func (con *Consumer) reconcileWithRetry(ctx context.Context, txn ledger.Transaction) (int, error) {
	var lastErr error
	for attempt := 1; attempt <= con.maxRetries+1; attempt++ {
		if lastErr = con.rec.Reconcile(ctx, txn); lastErr == nil {
			return attempt, nil
		}
		if ctx.Err() != nil {
			return attempt, lastErr
		}
	}
	return con.maxRetries + 1, lastErr
}

// Run polls the configured topic until ctx is canceled, decoding and
// reconciling each record. It returns nil on a clean shutdown (ctx
// canceled) and a wrapped error if the broker reports a fetch failure or a
// record can't be routed to the DLQ topic. A record whose payload can't be
// decoded, or whose reconciliation keeps failing past con.maxRetries, is
// published to the DLQ topic and skipped rather than failing the whole
// consumer, since a single bad record shouldn't block every other tenant's
// events on the same partition.
//
// Because ctx is only checked between fetch batches (not per record), a
// record's retry loop or DLQ publish can still be in flight when ctx is
// canceled; that in-flight failure is a symptom of shutdown, not a genuine
// fault, so it's treated as one below rather than aborting Run with an
// error.
func (con *Consumer) Run(ctx context.Context) error {
	for {
		fetches := con.client.PollFetches(ctx)

		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			if ctx.Err() != nil {
				return nil
			}
			first := fetchErrs[0]
			return fmt.Errorf("consume transaction events from kafka topic %q partition %d: %w", first.Topic, first.Partition, first.Err)
		}

		if ctx.Err() != nil {
			return nil
		}

		var dlqErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if dlqErr != nil {
				return
			}

			txn, decodeErr := decodeTransaction(record.Value)
			if decodeErr != nil {
				log.Printf("processor: routing undecodable record on topic %q partition %d offset %d to dlq: %v", record.Topic, record.Partition, record.Offset, decodeErr)
				if err := con.publishToDLQ(ctx, record, decodeErr, 1); err != nil {
					dlqErr = fmt.Errorf("route undecodable record on topic %q partition %d offset %d to dlq: %w", record.Topic, record.Partition, record.Offset, err)
				}
				return
			}

			attempts, reconcileErr := con.reconcileWithRetry(ctx, txn)
			if reconcileErr == nil {
				return
			}
			log.Printf("processor: routing event %s to dlq after %d attempt(s): %v", txn.EventID, attempts, reconcileErr)
			if err := con.publishToDLQ(ctx, record, reconcileErr, attempts); err != nil {
				dlqErr = fmt.Errorf("route reconcile failure for event %s to dlq: %w", txn.EventID, err)
			}
		})
		if dlqErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return dlqErr
		}
	}
}

// Close releases the underlying Kafka client's connections and leaves the
// consumer group.
func (con *Consumer) Close() {
	con.client.Close()
}
