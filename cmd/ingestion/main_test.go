package main

import (
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunGracefulShutdown(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")

	runErrors := make(chan error, 1)
	go func() {
		runErrors <- run()
	}()

	// Give the goroutine time to reach the select/listening state before
	// signaling shutdown; cold-path test code, not the hot ingestion path.
	time.Sleep(100 * time.Millisecond)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	select {
	case err := <-runErrors:
		if err != nil {
			t.Errorf("run() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return after SIGTERM")
	}
}

func TestRunServerStartError(t *testing.T) {
	t.Setenv("HTTP_ADDR", "not-a-valid-addr")
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")

	err := run()
	if err == nil {
		t.Fatal("run() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "ingestion HTTP server:") {
		t.Errorf("run() error = %v, want it to wrap %q", err, "ingestion HTTP server:")
	}
}

func TestRunGRPCServerStartError(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GRPC_ADDR", "not-a-valid-addr")

	err := run()
	if err == nil {
		t.Fatal("run() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "ingestion gRPC server:") {
		t.Errorf("run() error = %v, want it to wrap %q", err, "ingestion gRPC server:")
	}
}
