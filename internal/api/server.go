// Package api provides the ingestion service's REST handlers, DTOs, and
// request validation. It talks to /internal/<domain> packages only through
// their exported interfaces, never reaching into storage directly.
package api

import (
	"context"
	"net/http"
	"time"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
)

// Server wraps the ingestion service's HTTP endpoints.
type Server struct {
	httpServer *http.Server
}

// NewServer builds a Server listening on addr with the ingestion service's
// routes registered. publisher is used to ship validated transaction events
// to Kafka.
func NewServer(addr string, publisher ingestion.Publisher) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("POST /v1/transactions", newTransactionHandler(publisher))

	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
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
