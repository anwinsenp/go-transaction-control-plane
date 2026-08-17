// Command processor consumes transaction events published by the ingestion
// service. This entrypoint is wiring only: it loads config, constructs the
// Kafka consumer, and manages its lifecycle. Reconciliation logic is added
// in a later change once the processor's domain layer exists.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger/storage"
	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
	"github.com/anwinsenp/go-transaction-control-plane/internal/processor"
	"github.com/anwinsenp/go-transaction-control-plane/internal/processor/kafka"
)

// defaultDatabaseURL matches the docker-compose local stack's Postgres
// instance, mirroring how kafka.LocalConfig defaults to the local broker.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/transactions?sslmode=disable"

// defaultMetricsAddr is the /metrics and /healthz listen address applied
// when METRICS_ADDR is unset.
const defaultMetricsAddr = ":8081"

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = defaultMetricsAddr
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	knownTenants := knownTenantsFromEnv()

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

	procMetrics, err := processor.NewMetrics(registry, knownTenants)
	if err != nil {
		return fmt.Errorf("create processor metrics: %w", err)
	}
	instrumentedReconciler, err := processor.NewInstrumentedReconciler(reconciler, procMetrics, transactions, reconciledStates)
	if err != nil {
		return fmt.Errorf("create instrumented reconciler: %w", err)
	}

	kafkaConfig, err := kafka.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve kafka config: %w", err)
	}

	consumer, err := kafka.NewConsumer(kafkaConfig, instrumentedReconciler, registry, knownTenants)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer consumer.Close()

	metricsServer := newMetricsServer(metricsAddr, registry)

	// runCtx is canceled either by the shutdown signal (via its parent ctx)
	// or by the metrics server failing to start, so a bind failure on the
	// metrics listener also stops the consumer loop rather than running
	// with silent metrics blindness.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var startErrOnce sync.Once
	var startErr error
	recordStartErr := func(err error) {
		startErrOnce.Do(func() {
			startErr = err
			cancelRun()
		})
	}

	metricsServerErrors := make(chan error, 1)
	go func() {
		log.Printf("processor metrics service listening on %s", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			wrapped := fmt.Errorf("processor metrics server: %w", err)
			recordStartErr(wrapped)
			metricsServerErrors <- wrapped
			return
		}
		metricsServerErrors <- nil
	}()

	log.Printf("processor consuming topic %q as group %q, routing failures to %q after %d retries", kafkaConfig.Topic, kafkaConfig.GroupID, kafkaConfig.DLQTopic, kafkaConfig.MaxRetries)
	consumeErr := consumer.Run(runCtx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := metricsServer.Shutdown(shutdownCtx)

	metricsErr := <-metricsServerErrors

	// Reading startErr here without a lock is safe: any write to it happens
	// before the corresponding send on metricsServerErrors above, and the
	// receive on that channel establishes a happens-before edge with this
	// read.
	switch {
	case startErr != nil:
		return startErr
	case consumeErr != nil:
		return fmt.Errorf("processor consumer: %w", consumeErr)
	case shutdownErr != nil:
		return fmt.Errorf("shutdown processor metrics server: %w", shutdownErr)
	case metricsErr != nil:
		return metricsErr
	}

	log.Print("shutdown signal received, processor stopped")
	return nil
}

// newMetricsServer builds the processor's /healthz and /metrics HTTP
// server, listening on addr and serving gatherer's metrics.
func newMetricsServer(addr string, gatherer prometheus.Gatherer) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler)
	mux.Handle("GET /metrics", metrics.Handler(gatherer))

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func healthHandler(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
}

// knownTenantsFromEnv reads PROCESSOR_KNOWN_TENANTS as a comma-separated
// list of tenant IDs (matching provisioned TradingTenant resources) allowed
// to appear verbatim as the "tenant" label on per-tenant metrics. Unset or
// empty means every tenant ID reports as metrics.UnknownTenantLabel.
func knownTenantsFromEnv() metrics.KnownTenants {
	raw := os.Getenv("PROCESSOR_KNOWN_TENANTS")
	if raw == "" {
		return nil
	}

	var tenantIDs []string
	for _, tenantID := range strings.Split(raw, ",") {
		tenantID = strings.TrimSpace(tenantID)
		if tenantID == "" {
			continue
		}
		tenantIDs = append(tenantIDs, tenantID)
	}

	return metrics.NewKnownTenants(tenantIDs...)
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
