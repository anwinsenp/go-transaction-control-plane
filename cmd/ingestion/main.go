// Command ingestion is the public entrypoint for mock high-throughput
// transaction events. This entrypoint is wiring only: it loads config,
// constructs the API server, and manages its lifecycle. Business logic
// lives in /internal/api and the domain packages it calls.
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
	"google.golang.org/grpc"

	"github.com/anwinsenp/go-transaction-control-plane/internal/api"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion/kafka"
)

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	grpcAddr := os.Getenv("GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = ":9090"
	}

	apiKeys, err := apiKeysFromEnv()
	if err != nil {
		return fmt.Errorf("resolve API key config: %w", err)
	}

	rateLimit, err := rateLimitConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve rate limit config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kafkaConfig, err := kafka.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve kafka config: %w", err)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	publisher, err := kafka.NewPublisher(kafkaConfig, registry)
	if err != nil {
		return fmt.Errorf("create kafka publisher: %w", err)
	}
	defer publisher.Close()

	breakerConfig, err := breakerConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve circuit breaker config: %w", err)
	}

	breaker, err := ingestion.NewCircuitBreaker(publisher, breakerConfig)
	if err != nil {
		return fmt.Errorf("create publish circuit breaker: %w", err)
	}

	backpressureConfig, err := backpressureConfigFromEnv()
	if err != nil {
		return fmt.Errorf("resolve backpressure config: %w", err)
	}

	limiter, err := ingestion.NewBackpressureLimiter(breaker, backpressureConfig)
	if err != nil {
		return fmt.Errorf("create publish backpressure limiter: %w", err)
	}

	ingestionMetrics, err := ingestion.NewMetrics(registry)
	if err != nil {
		return fmt.Errorf("create ingestion metrics: %w", err)
	}
	instrumentedPublisher, err := ingestion.NewInstrumentedPublisher(limiter, ingestionMetrics, breaker)
	if err != nil {
		return fmt.Errorf("create instrumented publisher: %w", err)
	}

	server, err := api.NewServer(addr, instrumentedPublisher, apiKeys, rateLimit, registry)
	if err != nil {
		return fmt.Errorf("create ingestion HTTP server: %w", err)
	}
	grpcServer, err := api.NewGRPCServer(grpcAddr, instrumentedPublisher, apiKeys, rateLimit)
	if err != nil {
		return fmt.Errorf("create ingestion gRPC server: %w", err)
	}

	// runCtx is canceled either by the shutdown signal (via its parent ctx)
	// or by the first server that fails to start, so a start failure on one
	// server always triggers a shutdown of the other rather than leaking its
	// goroutine and listener.
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

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("ingestion HTTP service listening on %s", addr)
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			wrapped := fmt.Errorf("ingestion HTTP server: %w", err)
			recordStartErr(wrapped)
			serverErrors <- wrapped
			return
		}
		serverErrors <- nil
	}()

	grpcServerErrors := make(chan error, 1)
	go func() {
		log.Printf("ingestion gRPC service listening on %s", grpcAddr)
		if err := grpcServer.Start(); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			wrapped := fmt.Errorf("ingestion gRPC server: %w", err)
			recordStartErr(wrapped)
			grpcServerErrors <- wrapped
			return
		}
		grpcServerErrors <- nil
	}()

	<-runCtx.Done()
	if startErr == nil {
		log.Print("shutdown signal received, draining connections")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Shut down both servers concurrently against the shared deadline so a
	// slow HTTP drain can't eat into the gRPC server's share of
	// shutdownTimeout (and vice versa).
	var shutdownWG sync.WaitGroup
	var shutdownMu sync.Mutex
	var shutdownErr error
	recordShutdownErr := func(err error) {
		shutdownMu.Lock()
		defer shutdownMu.Unlock()
		shutdownErr = errors.Join(shutdownErr, err)
	}

	shutdownWG.Add(2)
	go func() {
		defer shutdownWG.Done()
		if err := server.Shutdown(shutdownCtx); err != nil {
			recordShutdownErr(fmt.Errorf("shutdown ingestion HTTP server: %w", err))
		}
	}()
	go func() {
		defer shutdownWG.Done()
		if err := grpcServer.Shutdown(shutdownCtx); err != nil {
			recordShutdownErr(fmt.Errorf("shutdown ingestion gRPC server: %w", err))
		}
	}()
	shutdownWG.Wait()

	httpErr := <-serverErrors
	grpcErr := <-grpcServerErrors

	switch {
	case startErr != nil:
		return startErr
	case shutdownErr != nil:
		return shutdownErr
	case httpErr != nil:
		return httpErr
	case grpcErr != nil:
		return grpcErr
	}
	return nil
}

// apiKeysFromEnv reads INGESTION_API_KEYS as a comma-separated list of
// bearer tokens accepted by the gRPC ingestion endpoint. It returns an
// error if the variable is unset or empty, since the service must fail to
// start rather than accept unauthenticated traffic.
func apiKeysFromEnv() ([]string, error) {
	raw := os.Getenv("INGESTION_API_KEYS")
	if raw == "" {
		return nil, fmt.Errorf("INGESTION_API_KEYS must be set to a comma-separated list of accepted API keys")
	}

	var keys []string
	for _, key := range strings.Split(raw, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("INGESTION_API_KEYS must contain at least one non-empty key")
	}

	return keys, nil
}

// defaultRateLimitRequestsPerSecond and defaultRateLimitBurst are the
// per-API-key rate limits applied when INGESTION_RATE_LIMIT_RPS /
// INGESTION_RATE_LIMIT_BURST are unset. Unlike API keys, a missing rate
// limit config shouldn't fail startup — these defaults give every
// deployment baseline protection against being overwhelmed.
const (
	defaultRateLimitRequestsPerSecond = 50
	defaultRateLimitBurst             = 100
)

// rateLimitConfigFromEnv reads the per-API-key rate limit from
// INGESTION_RATE_LIMIT_RPS (requests per second, float) and
// INGESTION_RATE_LIMIT_BURST (integer), falling back to
// defaultRateLimitRequestsPerSecond and defaultRateLimitBurst when unset.
func rateLimitConfigFromEnv() (api.RateLimitConfig, error) {
	config := api.RateLimitConfig{
		RequestsPerSecond: defaultRateLimitRequestsPerSecond,
		Burst:             defaultRateLimitBurst,
	}

	if raw := os.Getenv("INGESTION_RATE_LIMIT_RPS"); raw != "" {
		requestsPerSecond, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return api.RateLimitConfig{}, fmt.Errorf("parse INGESTION_RATE_LIMIT_RPS: %w", err)
		}
		config.RequestsPerSecond = requestsPerSecond
	}

	if raw := os.Getenv("INGESTION_RATE_LIMIT_BURST"); raw != "" {
		burst, err := strconv.Atoi(raw)
		if err != nil {
			return api.RateLimitConfig{}, fmt.Errorf("parse INGESTION_RATE_LIMIT_BURST: %w", err)
		}
		config.Burst = burst
	}

	return config, nil
}

// defaultBreakerFailureThreshold, defaultBreakerOpenTimeout, and
// defaultBreakerProbeTimeout are the publish circuit breaker settings
// applied when INGESTION_BREAKER_FAILURE_THRESHOLD /
// INGESTION_BREAKER_OPEN_TIMEOUT / INGESTION_BREAKER_PROBE_TIMEOUT are
// unset.
const (
	defaultBreakerFailureThreshold uint32        = 5
	defaultBreakerOpenTimeout      time.Duration = 30 * time.Second
	defaultBreakerProbeTimeout     time.Duration = 30 * time.Second
)

// breakerConfigFromEnv reads the publish circuit breaker config from
// INGESTION_BREAKER_FAILURE_THRESHOLD (positive integer),
// INGESTION_BREAKER_OPEN_TIMEOUT, and INGESTION_BREAKER_PROBE_TIMEOUT (any
// format understood by time.ParseDuration, e.g. "30s"), falling back to
// defaultBreakerFailureThreshold, defaultBreakerOpenTimeout, and
// defaultBreakerProbeTimeout when unset.
func breakerConfigFromEnv() (ingestion.BreakerConfig, error) {
	config := ingestion.BreakerConfig{
		FailureThreshold: defaultBreakerFailureThreshold,
		OpenTimeout:      defaultBreakerOpenTimeout,
		ProbeTimeout:     defaultBreakerProbeTimeout,
	}

	if raw := os.Getenv("INGESTION_BREAKER_FAILURE_THRESHOLD"); raw != "" {
		failureThreshold, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return ingestion.BreakerConfig{}, fmt.Errorf("parse INGESTION_BREAKER_FAILURE_THRESHOLD: %w", err)
		}
		config.FailureThreshold = uint32(failureThreshold)
	}

	if raw := os.Getenv("INGESTION_BREAKER_OPEN_TIMEOUT"); raw != "" {
		openTimeout, err := time.ParseDuration(raw)
		if err != nil {
			return ingestion.BreakerConfig{}, fmt.Errorf("parse INGESTION_BREAKER_OPEN_TIMEOUT: %w", err)
		}
		config.OpenTimeout = openTimeout
	}

	if raw := os.Getenv("INGESTION_BREAKER_PROBE_TIMEOUT"); raw != "" {
		probeTimeout, err := time.ParseDuration(raw)
		if err != nil {
			return ingestion.BreakerConfig{}, fmt.Errorf("parse INGESTION_BREAKER_PROBE_TIMEOUT: %w", err)
		}
		config.ProbeTimeout = probeTimeout
	}

	return config, nil
}

// defaultBackpressureMaxInFlight is the publish backpressure limiter's
// concurrent in-flight cap applied when INGESTION_BACKPRESSURE_MAX_INFLIGHT
// is unset.
const defaultBackpressureMaxInFlight = 256

// backpressureConfigFromEnv reads the publish backpressure limiter's config
// from INGESTION_BACKPRESSURE_MAX_INFLIGHT (positive integer), falling back
// to defaultBackpressureMaxInFlight when unset.
func backpressureConfigFromEnv() (ingestion.BackpressureConfig, error) {
	config := ingestion.BackpressureConfig{
		MaxInFlight: defaultBackpressureMaxInFlight,
	}

	if raw := os.Getenv("INGESTION_BACKPRESSURE_MAX_INFLIGHT"); raw != "" {
		maxInFlight, err := strconv.Atoi(raw)
		if err != nil {
			return ingestion.BackpressureConfig{}, fmt.Errorf("parse INGESTION_BACKPRESSURE_MAX_INFLIGHT: %w", err)
		}
		config.MaxInFlight = maxInFlight
	}

	return config, nil
}
