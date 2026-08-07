// Package kafka provides the Kafka-backed consumer skeleton for the
// processor service. It connects to and reads from the configured
// transaction topic; decoding and reconciliation logic are added in a
// later change once the processor's domain layer exists.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
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

// Consumer reads records from the processor's transaction topic. It is a
// connectivity skeleton: Run logs what it consumes but does not yet decode
// or reconcile records.
type Consumer struct {
	client consumerClient
	topic  string
}

// NewConsumer builds a Consumer connected to the brokers in cfg, joined to
// cfg.GroupID and subscribed to cfg.Topic.
func NewConsumer(cfg Config) (*Consumer, error) {
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

	return &Consumer{client: client, topic: cfg.Topic}, nil
}

// Run polls the configured topic until ctx is canceled, logging the number
// of records consumed per fetch. It returns nil on a clean shutdown (ctx
// canceled) and a wrapped error if the broker reports a fetch failure that
// isn't attributable to shutdown.
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

		if recordCount := fetches.NumRecords(); recordCount > 0 {
			log.Printf("processor consumed %d record(s) from topic %q", recordCount, con.topic)
		}
	}
}

// Close releases the underlying Kafka client's connections and leaves the
// consumer group.
func (con *Consumer) Close() {
	con.client.Close()
}
