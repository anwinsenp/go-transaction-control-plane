package promquery

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// fakeQueryAPI implements promv1.API by embedding a nil promv1.API and
// overriding only Query, the single method queryScalar calls.
type fakeQueryAPI struct {
	promv1.API

	gotCtx   context.Context
	gotQuery string

	result  model.Value
	warning promv1.Warnings
	err     error
}

func (fake *fakeQueryAPI) Query(ctx context.Context, query string, ts time.Time, opts ...promv1.Option) (model.Value, promv1.Warnings, error) {
	fake.gotCtx = ctx
	fake.gotQuery = query
	return fake.result, fake.warning, fake.err
}

func vectorOf(value float64) model.Vector {
	return model.Vector{
		&model.Sample{Value: model.SampleValue(value)},
	}
}

func TestObservedKafkaLag(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(42)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	lag, err := client.ObservedKafkaLag(context.Background(), TenantLabel{Name: "tenant", Value: "acme"})
	if err != nil {
		t.Fatalf("ObservedKafkaLag returned error: %v", err)
	}
	if lag != 42 {
		t.Errorf("lag = %d, want 42", lag)
	}

	wantQuery := `processor_kafka_consumer_lag_messages{tenant="acme"}`
	if fake.gotQuery != wantQuery {
		t.Errorf("query = %q, want %q", fake.gotQuery, wantQuery)
	}
}

func TestTenantLabel_MatcherEscapesInjection(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(1)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	maliciousLabel := TenantLabel{Name: "tenant", Value: `acme"} or vector(1) or up{job="`}

	if _, err := client.ObservedKafkaLag(context.Background(), maliciousLabel); err != nil {
		t.Fatalf("ObservedKafkaLag returned error: %v", err)
	}

	wantQuery := `processor_kafka_consumer_lag_messages{tenant="acme\"} or vector(1) or up{job=\""}`
	if fake.gotQuery != wantQuery {
		t.Errorf("query = %q, want %q", fake.gotQuery, wantQuery)
	}
}

func TestTenantLabel_MatcherEscapesBackslash(t *testing.T) {
	label := TenantLabel{Name: "tenant", Value: `back\slash"quote`}

	wantMatcher := `tenant="back\\slash\"quote"`
	if got := label.matcher(); got != wantMatcher {
		t.Errorf("matcher() = %q, want %q", got, wantMatcher)
	}
}

func TestObservedP99Ms(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(0.123)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	p99, err := client.ObservedP99Ms(context.Background(), TenantLabel{Name: "tenant", Value: "acme"})
	if err != nil {
		t.Fatalf("ObservedP99Ms returned error: %v", err)
	}
	if p99 != 123 {
		t.Errorf("p99Ms = %d, want 123", p99)
	}

	wantQuery := `histogram_quantile(0.99, sum(rate(processor_transaction_duration_seconds_bucket{tenant="acme"}[5m])) by (le))`
	if fake.gotQuery != wantQuery {
		t.Errorf("query = %q, want %q", fake.gotQuery, wantQuery)
	}
}

func TestObservedPartitionCount(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(6)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	count, err := client.ObservedPartitionCount(context.Background(), TenantLabel{Name: "tenant", Value: "acme"})
	if err != nil {
		t.Fatalf("ObservedPartitionCount returned error: %v", err)
	}
	if count != 6 {
		t.Errorf("count = %d, want 6", count)
	}

	wantQuery := `ingestion_kafka_tenant_partition_count{tenant="acme"}`
	if fake.gotQuery != wantQuery {
		t.Errorf("query = %q, want %q", fake.gotQuery, wantQuery)
	}
}

func TestObservedPartitionStart(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(3)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	start, err := client.ObservedPartitionStart(context.Background(), TenantLabel{Name: "tenant", Value: "acme"})
	if err != nil {
		t.Fatalf("ObservedPartitionStart returned error: %v", err)
	}
	if start != 3 {
		t.Errorf("start = %d, want 3", start)
	}

	wantQuery := `ingestion_kafka_tenant_partition_start_count{tenant="acme"}`
	if fake.gotQuery != wantQuery {
		t.Errorf("query = %q, want %q", fake.gotQuery, wantQuery)
	}
}

func TestObservedPartitionStart_OutOfRange(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(math.MaxInt32 * 4.0)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	if _, err := client.ObservedPartitionStart(context.Background(), TenantLabel{Name: "tenant", Value: "acme"}); err == nil {
		t.Fatal("expected an error for an out-of-range value, got nil")
	} else if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("error = %q, want substring %q", err.Error(), "out of range")
	}
}

func TestObservedKafkaLag_OutOfRange(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(math.MaxInt64 * 4.0)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	if _, err := client.ObservedKafkaLag(context.Background(), TenantLabel{Name: "tenant", Value: "acme"}); err == nil {
		t.Fatal("expected an error for an out-of-range value, got nil")
	} else if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("error = %q, want substring %q", err.Error(), "out of range")
	}
}

func TestObservedP99Ms_OutOfRange(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(math.MaxInt32 * 4.0)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	if _, err := client.ObservedP99Ms(context.Background(), TenantLabel{Name: "tenant", Value: "acme"}); err == nil {
		t.Fatal("expected an error for an out-of-range value, got nil")
	} else if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("error = %q, want substring %q", err.Error(), "out of range")
	}
}

func TestObservedPartitionCount_OutOfRange(t *testing.T) {
	fake := &fakeQueryAPI{result: vectorOf(math.MaxInt32 * 4.0)}
	client := &Client{queryAPI: fake, timeout: time.Second}

	if _, err := client.ObservedPartitionCount(context.Background(), TenantLabel{Name: "tenant", Value: "acme"}); err == nil {
		t.Fatal("expected an error for an out-of-range value, got nil")
	} else if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("error = %q, want substring %q", err.Error(), "out of range")
	}
}

func TestQueryScalar(t *testing.T) {
	tests := []struct {
		name      string
		result    model.Value
		queryErr  error
		wantValue float64
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "single sample vector succeeds",
			result:    vectorOf(3.5),
			wantValue: 3.5,
		},
		{
			name:      "api error is wrapped",
			queryErr:  errors.New("connection refused"),
			wantErr:   true,
			errSubstr: "connection refused",
		},
		{
			name:      "non-vector result type errors",
			result:    &model.Scalar{Value: 1},
			wantErr:   true,
			errSubstr: "unexpected result type",
		},
		{
			name:      "empty vector errors",
			result:    model.Vector{},
			wantErr:   true,
			errSubstr: "no series matched",
		},
		{
			name: "multi-sample vector errors",
			result: model.Vector{
				&model.Sample{Value: 1},
				&model.Sample{Value: 2},
			},
			wantErr:   true,
			errSubstr: "expected a single series",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakeQueryAPI{result: testCase.result, err: testCase.queryErr}
			client := &Client{queryAPI: fake, timeout: time.Second}

			value, err := client.queryScalar(context.Background(), "up")

			if testCase.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if !strings.Contains(err.Error(), testCase.errSubstr) {
					t.Errorf("error = %q, want substring %q", err.Error(), testCase.errSubstr)
				}
				return
			}

			if err != nil {
				t.Fatalf("queryScalar returned error: %v", err)
			}
			if value != testCase.wantValue {
				t.Errorf("value = %v, want %v", value, testCase.wantValue)
			}
		})
	}
}

func TestQueryScalar_ContextDeadline(t *testing.T) {
	t.Run("existing deadline is passed through unchanged", func(t *testing.T) {
		fake := &fakeQueryAPI{result: vectorOf(1)}
		client := &Client{queryAPI: fake, timeout: time.Second}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		wantDeadline, _ := ctx.Deadline()

		if _, err := client.queryScalar(ctx, "up"); err != nil {
			t.Fatalf("queryScalar returned error: %v", err)
		}

		gotDeadline, hasDeadline := fake.gotCtx.Deadline()
		if !hasDeadline {
			t.Fatal("expected fake to receive a context with a deadline")
		}
		if !gotDeadline.Equal(wantDeadline) {
			t.Errorf("deadline = %v, want %v", gotDeadline, wantDeadline)
		}
	})

	t.Run("no deadline falls back to client timeout", func(t *testing.T) {
		fake := &fakeQueryAPI{result: vectorOf(1)}
		client := &Client{queryAPI: fake, timeout: time.Minute}

		before := time.Now()
		if _, err := client.queryScalar(context.Background(), "up"); err != nil {
			t.Fatalf("queryScalar returned error: %v", err)
		}

		after := time.Now()

		gotDeadline, hasDeadline := fake.gotCtx.Deadline()
		if !hasDeadline {
			t.Fatal("expected fake to receive a context with a deadline")
		}

		wantMin := before.Add(client.timeout)
		wantMax := after.Add(client.timeout)
		if gotDeadline.Before(wantMin) || gotDeadline.After(wantMax) {
			t.Errorf("deadline = %v, want within [%v, %v] (now + client.timeout)", gotDeadline, wantMin, wantMax)
		}
	})
}

func TestNewConfigFromEnv(t *testing.T) {
	tests := []struct {
		name        string
		address     string
		timeout     string
		wantAddress string
		wantTimeout time.Duration
	}{
		{
			name:        "unset env vars fall back to defaults",
			wantAddress: defaultAddress,
			wantTimeout: defaultTimeout,
		},
		{
			name:        "address and timeout overrides are applied",
			address:     "http://prom.example:9090",
			timeout:     "15s",
			wantAddress: "http://prom.example:9090",
			wantTimeout: 15 * time.Second,
		},
		{
			name:        "invalid timeout is ignored and falls back to default",
			timeout:     "not-a-duration",
			wantAddress: defaultAddress,
			wantTimeout: defaultTimeout,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.address != "" {
				t.Setenv(addressEnvVar, testCase.address)
			}
			if testCase.timeout != "" {
				t.Setenv(timeoutEnvVar, testCase.timeout)
			}

			cfg := NewConfigFromEnv()

			if cfg.Address != testCase.wantAddress {
				t.Errorf("Address = %q, want %q", cfg.Address, testCase.wantAddress)
			}
			if cfg.Timeout != testCase.wantTimeout {
				t.Errorf("Timeout = %v, want %v", cfg.Timeout, testCase.wantTimeout)
			}
		})
	}
}
