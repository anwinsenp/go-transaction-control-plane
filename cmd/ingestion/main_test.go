package main

import (
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/anwinsenp/go-transaction-control-plane/internal/api"
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

func TestRateLimitConfigFromEnv(t *testing.T) {
	tests := []struct {
		name        string
		rpsUnset    bool
		rpsValue    string
		burstUnset  bool
		burstValue  string
		wantConfig  api.RateLimitConfig
		wantError   bool
		wantErrText string
	}{
		{
			name:       "both unset falls back to defaults",
			rpsUnset:   true,
			burstUnset: true,
			wantConfig: api.RateLimitConfig{RequestsPerSecond: defaultRateLimitRequestsPerSecond, Burst: defaultRateLimitBurst},
		},
		{
			name:       "both empty falls back to defaults",
			rpsValue:   "",
			burstValue: "",
			wantConfig: api.RateLimitConfig{RequestsPerSecond: defaultRateLimitRequestsPerSecond, Burst: defaultRateLimitBurst},
		},
		{
			name:       "valid RPS override with default burst",
			rpsValue:   "12.5",
			burstUnset: true,
			wantConfig: api.RateLimitConfig{RequestsPerSecond: 12.5, Burst: defaultRateLimitBurst},
		},
		{
			name:       "valid burst override with default RPS",
			rpsUnset:   true,
			burstValue: "250",
			wantConfig: api.RateLimitConfig{RequestsPerSecond: defaultRateLimitRequestsPerSecond, Burst: 250},
		},
		{
			name:       "both overridden",
			rpsValue:   "5",
			burstValue: "10",
			wantConfig: api.RateLimitConfig{RequestsPerSecond: 5, Burst: 10},
		},
		{
			name:        "invalid RPS float fails",
			rpsValue:    "not-a-number",
			burstUnset:  true,
			wantError:   true,
			wantErrText: "INGESTION_RATE_LIMIT_RPS",
		},
		{
			name:        "invalid burst integer fails",
			rpsUnset:    true,
			burstValue:  "not-a-number",
			wantError:   true,
			wantErrText: "INGESTION_RATE_LIMIT_BURST",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.rpsUnset {
				original, wasSet := lookupAndUnsetEnv(t, "INGESTION_RATE_LIMIT_RPS")
				t.Cleanup(func() {
					if wasSet {
						t.Setenv("INGESTION_RATE_LIMIT_RPS", original)
					}
				})
			} else {
				t.Setenv("INGESTION_RATE_LIMIT_RPS", test.rpsValue)
			}

			if test.burstUnset {
				original, wasSet := lookupAndUnsetEnv(t, "INGESTION_RATE_LIMIT_BURST")
				t.Cleanup(func() {
					if wasSet {
						t.Setenv("INGESTION_RATE_LIMIT_BURST", original)
					}
				})
			} else {
				t.Setenv("INGESTION_RATE_LIMIT_BURST", test.burstValue)
			}

			config, err := rateLimitConfigFromEnv()

			if test.wantError {
				if err == nil {
					t.Fatalf("rateLimitConfigFromEnv() error = nil, want error")
				}
				if !strings.Contains(err.Error(), test.wantErrText) {
					t.Errorf("rateLimitConfigFromEnv() error = %v, want it to mention %q", err, test.wantErrText)
				}
				return
			}

			if err != nil {
				t.Fatalf("rateLimitConfigFromEnv() error = %v, want nil", err)
			}
			if config != test.wantConfig {
				t.Errorf("rateLimitConfigFromEnv() = %+v, want %+v", config, test.wantConfig)
			}
		})
	}
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
