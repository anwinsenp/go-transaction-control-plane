package api

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"

	ingestionv1 "github.com/anwinsenp/go-transaction-control-plane/internal/api/pb/ingestion/v1"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
)

// transactionIngestionServiceName is the fully-qualified service name
// reported by the gRPC health checking protocol for
// TransactionIngestionService, matching the name grpc-go derives from the
// proto package and service.
const transactionIngestionServiceName = "ingestion.v1.TransactionIngestionService"

// GRPCServer wraps the ingestion service's gRPC endpoints: transaction
// ingestion and the standard gRPC health checking protocol.
type GRPCServer struct {
	addr   string
	server *grpc.Server
	health *health.Server
}

// NewGRPCServer builds a GRPCServer that will listen on addr once Start is
// called. publisher is used to ship validated transaction events to Kafka.
// apiKeys is the set of bearer tokens accepted on every RPC other than the
// gRPC health check; at least one key is required, and empty-string keys are
// rejected. rateLimit configures the per-API-key request rate accepted
// before rejecting with codes.ResourceExhausted.
func NewGRPCServer(addr string, publisher ingestion.Publisher, apiKeys []string, rateLimit RateLimitConfig) (*GRPCServer, error) {
	authenticator, err := newAPIKeyAuthenticator(apiKeys)
	if err != nil {
		return nil, fmt.Errorf("configure API key authenticator: %w", err)
	}

	limiter, err := newRateLimiter(rateLimit)
	if err != nil {
		return nil, fmt.Errorf("configure rate limiter: %w", err)
	}

	server := grpc.NewServer(grpc.ChainUnaryInterceptor(authenticator.unaryInterceptor(), limiter.unaryInterceptor()))

	ingestionv1.RegisterTransactionIngestionServiceServer(server, newTransactionIngestionServer(publisher))

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(transactionIngestionServiceName, healthgrpc.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(server, healthServer)

	return &GRPCServer{addr: addr, server: server, health: healthServer}, nil
}

// Start binds addr and begins serving gRPC requests, blocking until the
// server stops. It returns grpc.ErrServerStopped on a graceful Shutdown
// (including one that races Start itself), which callers should treat as a
// normal exit rather than an error.
func (srv *GRPCServer) Start() error {
	listener, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return err
	}

	if err := srv.server.Serve(listener); err != nil {
		return err
	}
	return grpc.ErrServerStopped
}

// Shutdown gracefully stops the server, waiting for in-flight RPCs to finish
// or ctx to be done, whichever comes first. If ctx is done first, it forces
// an immediate stop, dropping any in-flight RPCs.
func (srv *GRPCServer) Shutdown(ctx context.Context) error {
	srv.health.Shutdown()

	stopped := make(chan struct{})
	go func() {
		srv.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		srv.server.Stop()
		return ctx.Err()
	}
}
