package main

import (
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunGracefulShutdown(t *testing.T) {
	// 127.0.0.1:1 is unreachable but the client only needs to be able to
	// dial the address, not receive a broker; PollFetches respects context
	// cancellation, so run() returns as soon as SIGTERM arrives without a
	// live Kafka broker.
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_TOPIC", "transaction-events")
	t.Setenv("KAFKA_CONSUMER_GROUP", "processor-test")

	runErrors := make(chan error, 1)
	go func() {
		runErrors <- run()
	}()

	// Give the goroutine time to reach the poll loop before signaling
	// shutdown; cold-path test code, not the hot ingestion path.
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

func TestRunInvalidConfig(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", ",")

	err := run()
	if err == nil {
		t.Fatal("run() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "resolve kafka config:") {
		t.Errorf("run() error = %v, want it to wrap %q", err, "resolve kafka config:")
	}
}
