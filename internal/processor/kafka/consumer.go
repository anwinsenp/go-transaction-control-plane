// Package kafka provides the Kafka-backed consumer for the processor
// service: it connects to and reads from the configured transaction topic,
// decodes each record's wire payload, and hands the resulting transaction
// to a reconciler for idempotent P&L reconciliation.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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
}

// LocalConfig returns consumer settings sized for the docker-compose local
// stack: a single-broker Kafka instance with no other tenants, consuming
// the same topic the ingestion service publishes to.
func LocalConfig() Config {
	return Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "transaction-events",
		GroupID: "processor",
	}
}

// ConfigFromEnv resolves a Config from KAFKA_BROKERS (comma-separated,
// defaults to LocalConfig's single local broker), KAFKA_TOPIC (defaults to
// "transaction-events"), and KAFKA_CONSUMER_GROUP (defaults to
// "processor").
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
	return nil
}

// consumerClient is the subset of *kgo.Client that Consumer depends on, so
// tests can substitute a fake without a live Kafka broker.
type consumerClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
	Close()
}

// reconciler applies a decoded transaction to reconciled P&L state.
// Satisfied by *processor.Reconciler; declared locally so this package
// doesn't need to import internal/processor just for the type name.
type reconciler interface {
	Reconcile(ctx context.Context, txn ledger.Transaction) error
}

// Consumer reads records from the processor's transaction topic, decodes
// each record's wire payload, and reconciles it via rec.
type Consumer struct {
	client consumerClient
	topic  string
	rec    reconciler
}

// NewConsumer builds a Consumer connected to the brokers in cfg, joined to
// cfg.GroupID and subscribed to cfg.Topic. Every decoded record is handed to
// rec for idempotent reconciliation.
func NewConsumer(cfg Config, rec reconciler) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("new kafka consumer: %w", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("processor"),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.GroupID),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka consumer: create client: %w", err)
	}

	return &Consumer{client: client, topic: cfg.Topic, rec: rec}, nil
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

// Run polls the configured topic until ctx is canceled, decoding and
// reconciling each record. It returns nil on a clean shutdown (ctx
// canceled) and a wrapped error if the broker reports a fetch failure or
// reconciliation fails in a way that isn't attributable to shutdown. A
// record whose payload can't be decoded is logged and skipped rather than
// failing the whole consumer, since a single malformed message shouldn't
// block every other tenant's events on the same partition.
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

		var reconcileErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if reconcileErr != nil {
				return
			}
			txn, err := decodeTransaction(record.Value)
			if err != nil {
				log.Printf("processor: skipping undecodable record on topic %q partition %d offset %d: %v", record.Topic, record.Partition, record.Offset, err)
				return
			}
			if err := con.rec.Reconcile(ctx, txn); err != nil {
				reconcileErr = fmt.Errorf("reconcile transaction event %s: %w", txn.EventID, err)
			}
		})
		if reconcileErr != nil {
			return reconcileErr
		}
	}
}

// Close releases the underlying Kafka client's connections and leaves the
// consumer group.
func (con *Consumer) Close() {
	con.client.Close()
}
