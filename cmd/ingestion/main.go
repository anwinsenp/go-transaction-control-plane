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

	"google.golang.org/grpc"

	"github.com/anwinsenp/go-transaction-control-plane/internal/api"
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

	publisher, err := kafka.NewPublisher(kafkaConfig)
	if err != nil {
		return fmt.Errorf("create kafka publisher: %w", err)
	}
	defer publisher.Close()

	server, err := api.NewServer(addr, publisher, apiKeys, rateLimit)
	if err != nil {
		return fmt.Errorf("create ingestion HTTP server: %w", err)
	}
	grpcServer, err := api.NewGRPCServer(grpcAddr, publisher, apiKeys, rateLimit)
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
