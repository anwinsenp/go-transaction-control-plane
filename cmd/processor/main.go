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

	"github.com/anwinsenp/go-transaction-control-plane/internal/processor/kafka"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kafkaConfig, err := kafka.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve kafka config: %w", err)
	}

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer consumer.Close()

	log.Printf("processor consuming topic %q as group %q", kafkaConfig.Topic, kafkaConfig.GroupID)
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("processor consumer: %w", err)
	}

	log.Print("shutdown signal received, processor stopped")
	return nil
}
