// Command processor consumes transaction events published by the ingestion
// service. This entrypoint is wiring only: it loads config, constructs the
// Kafka consumer, and manages its lifecycle. Reconciliation logic is added
// in a later change once the processor's domain layer exists.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger/storage"
	"github.com/anwinsenp/go-transaction-control-plane/internal/processor"
	"github.com/anwinsenp/go-transaction-control-plane/internal/processor/kafka"
)

// defaultDatabaseURL matches the docker-compose local stack's Postgres
// instance, mirroring how kafka.LocalConfig defaults to the local broker.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/transactions?sslmode=disable"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultDatabaseURL
	}
	poolConfig, err := storage.PoolConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve postgres pool config: %w", err)
	}
	pool, err := storage.NewPool(ctx, databaseURL, poolConfig)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()

	reconciler := processor.NewReconciler(
		storage.NewTransactionStore(pool),
		storage.NewReconciledStateStore(pool),
	)

	kafkaConfig, err := kafka.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve kafka config: %w", err)
	}

	consumer, err := kafka.NewConsumer(kafkaConfig, reconciler)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer consumer.Close()

	log.Printf("processor consuming topic %q as group %q, routing failures to %q after %d retries", kafkaConfig.Topic, kafkaConfig.GroupID, kafkaConfig.DLQTopic, kafkaConfig.MaxRetries)
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("processor consumer: %w", err)
	}

	log.Print("shutdown signal received, processor stopped")
	return nil
}
