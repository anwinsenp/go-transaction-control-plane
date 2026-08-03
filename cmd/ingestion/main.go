// Command ingestion is the public entrypoint for mock high-frequency
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
	"syscall"
	"time"

	"github.com/anwinsenp/go-transaction-control-plane/internal/api"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := api.NewServer(addr)

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("ingestion service listening on %s", addr)
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- fmt.Errorf("ingestion server: %w", err)
			return
		}
		serverErrors <- nil
	}()

	select {
	case err := <-serverErrors:
		return err
	case <-ctx.Done():
		log.Print("shutdown signal received, draining connections")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown ingestion server: %w", err)
	}
	return <-serverErrors
}
