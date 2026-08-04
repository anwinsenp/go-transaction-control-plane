package main

import (
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunGracefulShutdown(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("INGESTION_API_KEYS", "test-api-key")

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
	t.Setenv("INGESTION_API_KEYS", "test-api-key")

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
	t.Setenv("INGESTION_API_KEYS", "test-api-key")

	err := run()
	if err == nil {
		t.Fatal("run() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "ingestion gRPC server:") {
		t.Errorf("run() error = %v, want it to wrap %q", err, "ingestion gRPC server:")
	}
}

func TestAPIKeysFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		envUnset  bool
		envValue  string
		wantKeys  []string
		wantError bool
	}{
		{name: "unset", envUnset: true, wantError: true},
		{name: "empty string", envValue: "", wantError: true},
		{name: "whitespace and empty entries only", envValue: " , , ", wantError: true},
		{name: "single key", envValue: "test-api-key", wantKeys: []string{"test-api-key"}},
		{name: "multiple comma-separated keys", envValue: "key-one,key-two,key-three", wantKeys: []string{"key-one", "key-two", "key-three"}},
		{name: "keys with surrounding whitespace trimmed", envValue: " key-one , key-two ", wantKeys: []string{"key-one", "key-two"}},
		{name: "mixed empty and valid entries skips empties", envValue: "key-one,,key-two", wantKeys: []string{"key-one", "key-two"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.envUnset {
				original, wasSet := lookupAndUnsetEnv(t, "INGESTION_API_KEYS")
				t.Cleanup(func() {
					if wasSet {
						t.Setenv("INGESTION_API_KEYS", original)
					}
				})
			} else {
				t.Setenv("INGESTION_API_KEYS", test.envValue)
			}

			keys, err := apiKeysFromEnv()

			if test.wantError {
				if err == nil {
					t.Fatalf("apiKeysFromEnv() error = nil, want error")
				}
				return
			}

			if err != nil {
				t.Fatalf("apiKeysFromEnv() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(keys, test.wantKeys) {
				t.Errorf("apiKeysFromEnv() = %v, want %v", keys, test.wantKeys)
			}
		})
	}
}

// lookupAndUnsetEnv removes key from the environment for the duration of the
// test, returning its prior value and whether it was set, so the caller can
// restore it afterward.
func lookupAndUnsetEnv(t *testing.T, key string) (string, bool) {
	t.Helper()

	original, wasSet := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	return original, wasSet
}

func TestRunMissingAPIKeys(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("INGESTION_API_KEYS", "")

	err := run()
	if err == nil {
		t.Fatal("run() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "API key config") {
		t.Errorf("run() error = %v, want it to wrap %q", err, "API key config")
	}
}
