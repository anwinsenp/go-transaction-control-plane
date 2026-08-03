package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"

	ingestionv1 "github.com/anwinsenp/go-transaction-control-plane/internal/api/pb/ingestion/v1"
)

// freeTCPAddr returns a loopback address with an OS-assigned free port,
// suitable for binding a server that a test client will dial by address
// rather than through the server's own Start method.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func TestGRPCServerStartAndShutdown(t *testing.T) {
	server := NewGRPCServer(freeTCPAddr(t), &fakePublisher{})

	startErrors := make(chan error, 1)
	go func() {
		startErrors <- server.Start()
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	select {
	case err := <-startErrors:
		if !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Start() error = %v, want errors.Is(err, grpc.ErrServerStopped)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start() did not return after Shutdown()")
	}
}

func TestGRPCServerServesTransactionIngestionAndHealth(t *testing.T) {
	addr := freeTCPAddr(t)
	server := NewGRPCServer(addr, &fakePublisher{})

	go server.Start()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	healthClient := healthgrpc.NewHealthClient(conn)
	healthResponse, err := healthClient.Check(context.Background(), &healthgrpc.HealthCheckRequest{Service: transactionIngestionServiceName})
	if err != nil {
		t.Fatalf("health Check() error = %v", err)
	}
	if healthResponse.GetStatus() != healthgrpc.HealthCheckResponse_SERVING {
		t.Errorf("health status = %v, want SERVING", healthResponse.GetStatus())
	}

	ingestionClient := ingestionv1.NewTransactionIngestionServiceClient(conn)
	ingestResponse, err := ingestionClient.IngestTransaction(context.Background(), &ingestionv1.IngestTransactionRequest{Event: validTransactionEventProto()})
	if err != nil {
		t.Fatalf("IngestTransaction() error = %v", err)
	}
	if !ingestResponse.GetAccepted() {
		t.Errorf("Accepted = false, want true")
	}
}

// TestGRPCServerConcurrentIngestTransaction drives concurrent
// IngestTransaction RPCs over a single shared connection against one running
// GRPCServer, so the handler and its publisher wiring can be validated for
// data races under -race and for correct per-request responses under
// concurrent load, mirroring
// TestTransactionHandlerConcurrentRequestsDoNotShareState for the REST path.
func TestGRPCServerConcurrentIngestTransaction(t *testing.T) {
	addr := freeTCPAddr(t)
	publisher := &fakePublisher{}
	server := NewGRPCServer(addr, publisher)

	go server.Start()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	conn := dialWithRetry(t, addr)
	defer conn.Close()

	ingestionClient := ingestionv1.NewTransactionIngestionServiceClient(conn)

	const goroutineCount = 8
	const requestsPerGoroutine = 20

	var waitGroup sync.WaitGroup
	failures := make(chan string, goroutineCount*requestsPerGoroutine)

	for goroutineIndex := 0; goroutineIndex < goroutineCount; goroutineIndex++ {
		waitGroup.Add(1)
		go func(goroutineIndex int) {
			defer waitGroup.Done()

			for requestIndex := 0; requestIndex < requestsPerGoroutine; requestIndex++ {
				event := validTransactionEventProto()
				event.EventId = uuid.New().String()

				response, err := ingestionClient.IngestTransaction(context.Background(), &ingestionv1.IngestTransactionRequest{Event: event})
				if err != nil {
					failures <- fmt.Sprintf("goroutine %d request %d: IngestTransaction() error = %v", goroutineIndex, requestIndex, err)
					continue
				}
				if !response.GetAccepted() {
					failures <- fmt.Sprintf("goroutine %d request %d: Accepted = false, want true", goroutineIndex, requestIndex)
					continue
				}
				if response.GetEventId() != event.GetEventId() {
					failures <- fmt.Sprintf("goroutine %d request %d: EventId = %q, want %q", goroutineIndex, requestIndex, response.GetEventId(), event.GetEventId())
				}
			}
		}(goroutineIndex)
	}

	waitGroup.Wait()
	close(failures)

	for failure := range failures {
		t.Error(failure)
	}

	if want := goroutineCount * requestsPerGoroutine; len(publisher.published) != want {
		t.Errorf("published %d events, want %d", len(publisher.published), want)
	}
}

// dialWithRetry dials addr, retrying briefly since the server started via
// server.Start() in a background goroutine may not have bound its listener
// yet.
func dialWithRetry(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}

		healthClient := healthgrpc.NewHealthClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err = healthClient.Check(ctx, &healthgrpc.HealthCheckRequest{})
		cancel()
		if err == nil {
			return conn
		}
		lastErr = err
		conn.Close()
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("dial %s: %v", addr, lastErr)
	return nil
}
