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
	"strconv"
	"syscall"
	"time"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
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

	// Both repository breakers intentionally share one BreakerConfig: they
	// trip and recover on identical thresholds even though the two
	// repositories have different failure characteristics, which keeps
	// config surface area down until there's a concrete reason to split it.
	breakerConfig, err := breakerConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve circuit breaker config: %w", err)
	}

	transactions, err := ledger.NewTransactionRepositoryBreaker(storage.NewTransactionStore(pool), breakerConfig)
	if err != nil {
		return fmt.Errorf("create transaction repository circuit breaker: %w", err)
	}
	reconciledStates, err := ledger.NewReconciledStateRepositoryBreaker(storage.NewReconciledStateStore(pool), breakerConfig)
	if err != nil {
		return fmt.Errorf("create reconciled state repository circuit breaker: %w", err)
	}

	reconciler := processor.NewReconciler(transactions, reconciledStates)

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

// defaultBreakerFailureThreshold, defaultBreakerOpenTimeout, and
// defaultBreakerProbeTimeout are the Postgres write path circuit breaker
// settings applied when PROCESSOR_BREAKER_FAILURE_THRESHOLD /
// PROCESSOR_BREAKER_OPEN_TIMEOUT / PROCESSOR_BREAKER_PROBE_TIMEOUT are
// unset.
const (
	defaultBreakerFailureThreshold uint32        = 5
	defaultBreakerOpenTimeout      time.Duration = 30 * time.Second
	defaultBreakerProbeTimeout     time.Duration = 30 * time.Second
)

// breakerConfigFromEnv reads the Postgres write path circuit breaker config
// (shared by the transaction and reconciled state repository breakers) from
// PROCESSOR_BREAKER_FAILURE_THRESHOLD (positive integer),
// PROCESSOR_BREAKER_OPEN_TIMEOUT, and PROCESSOR_BREAKER_PROBE_TIMEOUT (any
// format understood by time.ParseDuration, e.g. "30s"), falling back to
// defaultBreakerFailureThreshold, defaultBreakerOpenTimeout, and
// defaultBreakerProbeTimeout when unset.
func breakerConfigFromEnv() (ledger.BreakerConfig, error) {
	config := ledger.BreakerConfig{
		FailureThreshold: defaultBreakerFailureThreshold,
		OpenTimeout:      defaultBreakerOpenTimeout,
		ProbeTimeout:     defaultBreakerProbeTimeout,
	}

	if raw := os.Getenv("PROCESSOR_BREAKER_FAILURE_THRESHOLD"); raw != "" {
		failureThreshold, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return ledger.BreakerConfig{}, fmt.Errorf("parse PROCESSOR_BREAKER_FAILURE_THRESHOLD: %w", err)
		}
		config.FailureThreshold = uint32(failureThreshold)
	}

	if raw := os.Getenv("PROCESSOR_BREAKER_OPEN_TIMEOUT"); raw != "" {
		openTimeout, err := time.ParseDuration(raw)
		if err != nil {
			return ledger.BreakerConfig{}, fmt.Errorf("parse PROCESSOR_BREAKER_OPEN_TIMEOUT: %w", err)
		}
		config.OpenTimeout = openTimeout
	}

	if raw := os.Getenv("PROCESSOR_BREAKER_PROBE_TIMEOUT"); raw != "" {
		probeTimeout, err := time.ParseDuration(raw)
		if err != nil {
			return ledger.BreakerConfig{}, fmt.Errorf("parse PROCESSOR_BREAKER_PROBE_TIMEOUT: %w", err)
		}
		config.ProbeTimeout = probeTimeout
	}

	return config, nil
}
