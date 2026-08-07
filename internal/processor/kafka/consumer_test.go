package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// fakeReconciler is a reconciler test double that records every
// transaction it's asked to reconcile, so Consumer's decode-and-dispatch
// behavior can be tested without a real Reconciler or Postgres.
type fakeReconciler struct {
	reconciled []ledger.Transaction
	err        error
}

func (fake *fakeReconciler) Reconcile(ctx context.Context, txn ledger.Transaction) error {
	if fake.err != nil {
		return fake.err
	}
	fake.reconciled = append(fake.reconciled, txn)
	return nil
}

// fakeConsumerClient is a consumerClient test double that returns
// caller-configured fetches/errors on each PollFetches call, so Consumer's
// behavior can be tested without a live Kafka broker.
type fakeConsumerClient struct {
	fetches []kgo.Fetches
	call    int
	closed  bool
}

func (fake *fakeConsumerClient) PollFetches(ctx context.Context) kgo.Fetches {
	if fake.call < len(fake.fetches) {
		fetches := fake.fetches[fake.call]
		fake.call++
		return fetches
	}
	<-ctx.Done()
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Partitions: []kgo.FetchPartition{{Err: ctx.Err()}},
		}},
	}}
}

func (fake *fakeConsumerClient) Close() {
	fake.closed = true
}

func fetchesWithRecords(topic string, count int) kgo.Fetches {
	records := make([]*kgo.Record, count)
	for i := range records {
		records[i] = &kgo.Record{Topic: topic, Value: []byte("event")}
	}
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: topic,
			Partitions: []kgo.FetchPartition{{
				Partition: 0,
				Records:   records,
			}},
		}},
	}}
}

func fetchesWithError(topic string, partition int32, err error) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: topic,
			Partitions: []kgo.FetchPartition{{
				Partition: partition,
				Err:       err,
			}},
		}},
	}}
}

func TestDecodeTransaction(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    ledger.Transaction
		wantErr bool
	}{
		{
			name: "valid payload round-trips",
			payload: `{
				"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"tenant_id": "tenant-1",
				"schema_version": 1,
				"instrument": "AAPL",
				"side": "BUY",
				"quantity": "10",
				"price": "150.5",
				"currency": "USD",
				"occurred_at": "2026-01-02T15:04:05.999999999Z"
			}`,
			want: ledger.Transaction{
				EventID:       uuid.MustParse("3fa85f64-5717-4562-b3fc-2c963f66afa6"),
				TenantID:      "tenant-1",
				SchemaVersion: 1,
				Instrument:    "AAPL",
				Side:          ledger.SideBuy,
				Quantity:      10 * ledger.AmountScale,
				Price:         mustAmount(t, "150.5"),
				Currency:      "USD",
				OccurredAt:    time.Date(2026, 1, 2, 15, 4, 5, 999999999, time.UTC),
			},
		},
		{
			name:    "invalid json",
			payload: `{not json`,
			wantErr: true,
		},
		{
			name:    "bad uuid",
			payload: `{"event_id": "not-a-uuid", "occurred_at": "2026-01-02T15:04:05Z", "quantity": "1", "price": "1"}`,
			wantErr: true,
		},
		{
			name:    "bad quantity",
			payload: `{"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6", "occurred_at": "2026-01-02T15:04:05Z", "quantity": "not-a-number", "price": "1"}`,
			wantErr: true,
		},
		{
			name:    "bad price",
			payload: `{"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6", "occurred_at": "2026-01-02T15:04:05Z", "quantity": "1", "price": "not-a-number"}`,
			wantErr: true,
		},
		{
			name:    "bad occurred_at",
			payload: `{"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6", "occurred_at": "not-a-timestamp", "quantity": "1", "price": "1"}`,
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := decodeTransaction([]byte(testCase.payload))
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("decodeTransaction() = %+v, nil, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeTransaction() error = %v, want nil", err)
			}
			if got != testCase.want {
				t.Errorf("decodeTransaction() = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

// mustAmount parses a decimal literal known to be valid at compile time,
// failing the test immediately rather than returning a zero value on error.
func mustAmount(t *testing.T, value string) int64 {
	t.Helper()
	amount, err := ledger.ParseAmount(value)
	if err != nil {
		t.Fatalf("ParseAmount(%q): %v", value, err)
	}
	return amount
}

func TestConsumerRunReconcileErrorAbortsRun(t *testing.T) {
	validPayload := []byte(`{
		"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
		"tenant_id": "tenant-1",
		"instrument": "AAPL",
		"side": "BUY",
		"quantity": "10",
		"price": "150.5",
		"currency": "USD",
		"occurred_at": "2026-01-02T15:04:05Z"
	}`)
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "transaction-events",
			Partitions: []kgo.FetchPartition{{
				Partition: 0,
				Records:   []*kgo.Record{{Topic: "transaction-events", Value: validPayload}},
			}},
		}},
	}}
	reconcileErr := errors.New("reconcile: database unavailable")
	fake := &fakeConsumerClient{fetches: []kgo.Fetches{fetches}}
	consumer := &Consumer{client: fake, topic: "transaction-events", rec: &fakeReconciler{err: reconcileErr}}

	err := consumer.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if !errors.Is(err, reconcileErr) {
		t.Errorf("Run() error = %v, want it to wrap %v", err, reconcileErr)
	}
}

func TestConsumerRunStopsCleanlyOnContextCancel(t *testing.T) {
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{fetchesWithRecords("transaction-events", 3)},
	}
	consumer := &Consumer{client: fake, topic: "transaction-events", rec: &fakeReconciler{}}

	ctx, cancel := context.WithCancel(context.Background())
	runErrors := make(chan error, 1)
	go func() {
		runErrors <- consumer.Run(ctx)
	}()

	cancel()

	select {
	case err := <-runErrors:
		if err != nil {
			t.Errorf("Run() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}
}

// TestConsumerRunMultipleFetchesIncludingEmpty drives Run through several
// poll iterations, including a zero-record fetch in the middle, to confirm
// the loop keeps polling on an empty fetch instead of treating it as
// shutdown or an error. The final configured fetch carries a broker error so
// the test ends deterministically rather than racing a context cancellation
// against the fake's fetch queue.
func TestConsumerRunMultipleFetchesIncludingEmpty(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchesWithRecords("transaction-events", 2),
			{}, // zero-record fetch: an empty poll with no records and no error
			fetchesWithRecords("transaction-events", 5),
			fetchesWithError("transaction-events", 0, brokerErr),
		},
	}
	consumer := &Consumer{client: fake, topic: "transaction-events", rec: &fakeReconciler{}}

	err := consumer.Run(context.Background())
	if !errors.Is(err, brokerErr) {
		t.Errorf("Run() error = %v, want it to wrap %v", err, brokerErr)
	}

	if fake.call != len(fake.fetches) {
		t.Errorf("PollFetches called %d times, want %d (all configured fetches drained)", fake.call, len(fake.fetches))
	}
}

func TestConsumerRunWrapsBrokerFetchError(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{fetchesWithError("transaction-events", 2, brokerErr)},
	}
	consumer := &Consumer{client: fake, topic: "transaction-events", rec: &fakeReconciler{}}

	err := consumer.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if !errors.Is(err, brokerErr) {
		t.Errorf("Run() error = %v, want it to wrap %v", err, brokerErr)
	}
}

func TestConsumerClose(t *testing.T) {
	fake := &fakeConsumerClient{}
	consumer := &Consumer{client: fake, topic: "transaction-events", rec: &fakeReconciler{}}

	consumer.Close()

	if !fake.closed {
		t.Error("Close() did not close the underlying client")
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v, want nil", err)
	}
	want := LocalConfig()
	if len(config.Brokers) != 1 || config.Brokers[0] != want.Brokers[0] {
		t.Errorf("Brokers = %v, want %v", config.Brokers, want.Brokers)
	}
	if config.Topic != want.Topic {
		t.Errorf("Topic = %q, want %q", config.Topic, want.Topic)
	}
	if config.GroupID != want.GroupID {
		t.Errorf("GroupID = %q, want %q", config.GroupID, want.GroupID)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker-a:9092,broker-b:9092")
	t.Setenv("KAFKA_TOPIC", "custom-events")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-group")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v, want nil", err)
	}

	wantBrokers := []string{"broker-a:9092", "broker-b:9092"}
	if len(config.Brokers) != len(wantBrokers) {
		t.Fatalf("Brokers = %v, want %v", config.Brokers, wantBrokers)
	}
	for i, broker := range wantBrokers {
		if config.Brokers[i] != broker {
			t.Errorf("Brokers[%d] = %q, want %q", i, config.Brokers[i], broker)
		}
	}
	if config.Topic != "custom-events" {
		t.Errorf("Topic = %q, want %q", config.Topic, "custom-events")
	}
	if config.GroupID != "custom-group" {
		t.Errorf("GroupID = %q, want %q", config.GroupID, "custom-group")
	}
}

func TestConfigFromEnvInvalidBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " , ")

	_, err := ConfigFromEnv()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("ConfigFromEnv() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{
			name:   "no brokers",
			config: Config{Topic: "t", GroupID: "g"},
		},
		{
			name:   "blank broker",
			config: Config{Brokers: []string{" "}, Topic: "t", GroupID: "g"},
		},
		{
			name:   "blank topic",
			config: Config{Brokers: []string{"localhost:9092"}, GroupID: "g"},
		},
		{
			name:   "blank group ID",
			config: Config{Brokers: []string{"localhost:9092"}, Topic: "t"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.config.validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("validate() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
			}
		})
	}
}

func TestNewConsumerRejectsInvalidConfig(t *testing.T) {
	if _, err := NewConsumer(Config{}, &fakeReconciler{}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("NewConsumer() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
	}
}

// TestNewConsumerValidConfig ensures a valid Config yields a usable Consumer
// without dialing a broker: kgo.NewClient only connects lazily once a fetch
// is requested, so this stays a fast unit test rather than requiring live
// Kafka infrastructure.
func TestNewConsumerValidConfig(t *testing.T) {
	config := Config{
		Brokers: []string{"127.0.0.1:9"},
		Topic:   "transaction-events",
		GroupID: "processor",
	}

	consumer, err := NewConsumer(config, &fakeReconciler{})
	if err != nil {
		t.Fatalf("NewConsumer() error = %v, want nil", err)
	}
	if consumer == nil {
		t.Fatal("NewConsumer() returned nil consumer, want non-nil")
	}
	defer consumer.Close()

	if consumer.topic != config.Topic {
		t.Errorf("consumer.topic = %q, want %q", consumer.topic, config.Topic)
	}
}
