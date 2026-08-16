// Package promquery wraps prometheus/client_golang's api/prometheus/v1
// client to issue the per-tenant instant PromQL queries the TradingTenant
// reconciler needs on each pass, per docs/DESIGN-operator.md's "Lag and
// latency observation path" section.
package promquery

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const (
	defaultAddress = "http://prometheus:9090"
	defaultTimeout = 5 * time.Second

	addressEnvVar = "PROMETHEUS_ADDR"
	timeoutEnvVar = "PROMETHEUS_QUERY_TIMEOUT"

	// These metric names/label-scoping approach are placeholders pending
	// #23/#25/#42, which define the processor's and operator's actual
	// Prometheus instrumentation. Update alongside those once they land.
	kafkaLagMetric         = "processor_kafka_consumer_lag_messages"
	latencyBucketMetric    = "processor_transaction_duration_seconds_bucket"
	partitionCountMetric   = "kafka_tenant_partition_count"
	latencyQuantileWindow  = "5m"
	latencySecondsToMillis = 1000
)

// Config holds the settings needed to reach Prometheus's HTTP API.
type Config struct {
	// Address is Prometheus's base URL, e.g. "http://prometheus:9090".
	Address string
	// Timeout bounds each query call when the caller's context carries no
	// earlier deadline.
	Timeout time.Duration
}

// NewConfigFromEnv builds a Config from PROMETHEUS_ADDR and
// PROMETHEUS_QUERY_TIMEOUT, falling back to sensible defaults when unset.
// Intended to be wired to a CLI flag by the operator's main.go (see #20);
// no such entrypoint exists yet, so only the env-var path is provided here.
//
// PROMETHEUS_QUERY_TIMEOUT is parsed best-effort: an invalid value falls
// back to defaultTimeout silently rather than failing construction, since
// this function has no error return and no logger is wired in yet. Once
// main.go exists (#20) this should surface the parse error there (e.g. via
// a log line) instead of swallowing it here.
func NewConfigFromEnv() Config {
	cfg := Config{Address: defaultAddress, Timeout: defaultTimeout}
	if address := os.Getenv(addressEnvVar); address != "" {
		cfg.Address = address
	}
	if timeoutStr := os.Getenv(timeoutEnvVar); timeoutStr != "" {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			cfg.Timeout = timeout
		}
	}
	return cfg
}

// TenantLabel is the low-cardinality label match used to scope a query to
// one tenant. Its Name/Value are decided by whichever metric exporter
// produces the underlying series (see #25, #42) — this client stays
// agnostic to that choice.
type TenantLabel struct {
	Name  string
	Value string
}

func (label TenantLabel) matcher() string {
	return fmt.Sprintf(`%s="%s"`, escapePromQLString(label.Name), escapePromQLString(label.Value))
}

// escapePromQLString escapes backslashes and double quotes so a string can
// be safely interpolated into a PromQL double-quoted string literal.
// Backslashes must be escaped first, or a value ending in `\"` would have
// its escaped quote re-escaped into `\\"`, which re-opens the string.
func escapePromQLString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

// Client issues bounded, per-tenant-scoped PromQL instant queries against
// Prometheus.
type Client struct {
	queryAPI promv1.API
	timeout  time.Duration
}

// NewClient builds a Client for the given Config. It does not contact
// Prometheus; connection errors surface on the first query.
func NewClient(cfg Config) (*Client, error) {
	apiClient, err := api.NewClient(api.Config{Address: cfg.Address})
	if err != nil {
		return nil, fmt.Errorf("promquery: building prometheus api client: %w", err)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return &Client{queryAPI: promv1.NewAPI(apiClient), timeout: timeout}, nil
}

// ObservedKafkaLag returns the tenant's most recently scraped Kafka
// consumer lag, in messages.
func (client *Client) ObservedKafkaLag(ctx context.Context, label TenantLabel) (int64, error) {
	query := fmt.Sprintf("%s{%s}", kafkaLagMetric, label.matcher())

	value, err := client.queryScalar(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("promquery: observed kafka lag: %w", err)
	}
	rounded := math.Round(value)
	if rounded > math.MaxInt64 || rounded < math.MinInt64 {
		return 0, fmt.Errorf("promquery: observed kafka lag: value %v out of range", value)
	}
	return int64(rounded), nil
}

// ObservedP99Ms returns the tenant's most recently scraped P99 processing
// latency, in milliseconds.
func (client *Client) ObservedP99Ms(ctx context.Context, label TenantLabel) (int32, error) {
	query := fmt.Sprintf(
		"histogram_quantile(0.99, sum(rate(%s{%s}[%s])) by (le))",
		latencyBucketMetric, label.matcher(), latencyQuantileWindow,
	)

	value, err := client.queryScalar(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("promquery: observed p99 latency: %w", err)
	}
	rounded := math.Round(value * latencySecondsToMillis)
	if rounded > math.MaxInt32 || rounded < math.MinInt32 {
		return 0, fmt.Errorf("promquery: observed p99 latency: value %v out of range", value)
	}
	return int32(rounded), nil
}

// ObservedPartitionCount returns the tenant's most recently scraped Kafka
// topic partition count.
func (client *Client) ObservedPartitionCount(ctx context.Context, label TenantLabel) (int32, error) {
	query := fmt.Sprintf("%s{%s}", partitionCountMetric, label.matcher())

	value, err := client.queryScalar(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("promquery: observed partition count: %w", err)
	}
	rounded := math.Round(value)
	if rounded > math.MaxInt32 || rounded < math.MinInt32 {
		return 0, fmt.Errorf("promquery: observed partition count: value %v out of range", value)
	}
	return int32(rounded), nil
}

// queryScalar runs an instant query and extracts its single resulting
// value. ctx's deadline is honored as-is; if it carries none, the client's
// configured Timeout bounds the call.
func (client *Client) queryScalar(ctx context.Context, query string) (float64, error) {
	queryCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, client.timeout)
		defer cancel()
	}

	result, _, err := client.queryAPI.Query(queryCtx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("querying prometheus (%s): %w", query, err)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return 0, fmt.Errorf("querying prometheus (%s): unexpected result type %T", query, result)
	}
	if len(vector) == 0 {
		return 0, fmt.Errorf("querying prometheus (%s): no series matched", query)
	}
	if len(vector) > 1 {
		return 0, fmt.Errorf("querying prometheus (%s): expected a single series, got %d", query, len(vector))
	}

	return float64(vector[0].Value), nil
}
