// Package api provides the ingestion service's REST handlers, DTOs, and
// request validation. It talks to /internal/<domain> packages only through
// their exported interfaces, never reaching into storage directly.
package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// Server wraps the ingestion service's HTTP endpoints.
type Server struct {
	httpServer *http.Server
}

// NewServer builds a Server listening on addr with the ingestion service's
// routes registered. publisher is used to ship validated transaction events
// to Kafka. apiKeys is the set of bearer tokens required on the
// transactions route; /healthz and /metrics stay unauthenticated so
// orchestrator liveness/readiness probes and the Prometheus scraper don't
// need a key. At least one key is required, and empty-string keys are
// rejected. rateLimit configures the per-API-key request rate the
// transactions route accepts before rejecting with 429. gatherer supplies
// the metrics served at /metrics.
func NewServer(addr string, publisher ingestion.Publisher, apiKeys []string, rateLimit RateLimitConfig, gatherer prometheus.Gatherer) (*Server, error) {
	authenticator, err := newAPIKeyAuthenticator(apiKeys)
	if err != nil {
		return nil, fmt.Errorf("configure API key authenticator: %w", err)
	}

	limiter, err := newRateLimiter(rateLimit)
	if err != nil {
		return nil, fmt.Errorf("configure rate limiter: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler)
	mux.Handle("GET /metrics", metrics.Handler(gatherer))
	mux.Handle("POST /v1/transactions", authenticator.middleware(limiter.middleware(newTransactionHandler(publisher))))

	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}, nil
}

// Start begins serving requests and blocks until the server stops. It
// returns http.ErrServerClosed on a graceful Shutdown, which callers should
// treat as a normal exit rather than an error.
func (srv *Server) Start() error {
	return srv.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the server, waiting for in-flight requests to
// finish or ctx to be done, whichever comes first.
func (srv *Server) Shutdown(ctx context.Context) error {
	return srv.httpServer.Shutdown(ctx)
}
