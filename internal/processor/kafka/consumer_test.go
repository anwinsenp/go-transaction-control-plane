package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
	"github.com/anwinsenp/go-transaction-control-plane/internal/metrics"
)

// fakeReconciler is a reconciler test double that records every
// transaction it's asked to reconcile, so Consumer's decode-and-dispatch
// behavior can be tested without a real Reconciler or Postgres. When
// failTimes is zero (the default) and err is set, every call fails,
// matching prior behavior; when failTimes is positive, only the first
// failTimes calls fail and subsequent calls succeed, so reconcileWithRetry
// can be tested with a reconciler that eventually recovers.
type fakeReconciler struct {
	reconciled []ledger.Transaction
	err        error
	failTimes  int
	calls      int
}

func (fake *fakeReconciler) Reconcile(ctx context.Context, txn ledger.Transaction) error {
	fake.calls++
	if fake.err != nil && (fake.failTimes <= 0 || fake.calls <= fake.failTimes) {
		return fake.err
	}
	fake.reconciled = append(fake.reconciled, txn)
	return nil
}

// fakeConsumerClient is a client test double that returns caller-configured
// fetches/errors on each PollFetches call and records every record handed
// to ProduceSync, so Consumer's behavior — including DLQ routing — can be
// tested without a live Kafka broker.
type fakeConsumerClient struct {
	fetches    []kgo.Fetches
	call       int
	closed     bool
	produced   []*kgo.Record
	produceErr error
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

// ProduceSync mirrors *kgo.Client's real behavior of failing once ctx is
// canceled (in addition to any caller-configured produceErr), so tests can
// exercise Run's handling of a DLQ publish that fails because the consumer
// is shutting down rather than because the broker rejected it.
func (fake *fakeConsumerClient) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	fake.produced = append(fake.produced, records...)
	err := fake.produceErr
	if err == nil {
		err = ctx.Err()
	}
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: err}
	}
	return results
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

// validTransactionPayload is a decodable wire payload, for tests that need
// a record to reach reconciliation rather than fail at decode time.
var validTransactionPayload = []byte(`{
	"event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
	"tenant_id": "tenant-1",
	"instrument": "AAPL",
	"side": "BUY",
	"quantity": "10",
	"price": "150.5",
	"currency": "USD",
	"occurred_at": "2026-01-02T15:04:05Z"
}`)

func fetchesWithValidRecords(topic string, count int) kgo.Fetches {
	records := make([]*kgo.Record, count)
	for i := range records {
		records[i] = &kgo.Record{Topic: topic, Value: validTransactionPayload}
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

// validTransactionPayloadForTenant returns a decodable wire payload for
// tenantID, so lag-reporting tests can control which tenant a partition's
// last record belongs to.
func validTransactionPayloadForTenant(tenantID string) []byte {
	payload, err := json.Marshal(wireEvent{
		EventID:    "3fa85f64-5717-4562-b3fc-2c963f66afa6",
		TenantID:   tenantID,
		Instrument: "AAPL",
		Side:       "BUY",
		Quantity:   "10",
		Price:      "150.5",
		Currency:   "USD",
		OccurredAt: "2026-01-02T15:04:05Z",
	})
	if err != nil {
		panic(err)
	}
	return payload
}

// fetchesWithHighWatermarkAndTenants builds a single-partition fetch batch
// whose records carry tenantIDs in order (offsets 0..len(tenantIDs)-1), so
// lag-reporting tests can control both the partition's HighWatermark and
// which tenant the last decoded record belongs to.
func fetchesWithHighWatermarkAndTenants(topic string, highWatermark int64, tenantIDs ...string) kgo.Fetches {
	records := make([]*kgo.Record, len(tenantIDs))
	for i, tenantID := range tenantIDs {
		records[i] = &kgo.Record{Topic: topic, Offset: int64(i), Value: validTransactionPayloadForTenant(tenantID)}
	}
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: topic,
			Partitions: []kgo.FetchPartition{{
				Partition:     0,
				HighWatermark: highWatermark,
				Records:       records,
			}},
		}},
	}}
}

// fetchesWithEmptyPartition builds a fetch batch containing a partition
// with a HighWatermark set but zero records, so a lag-reporting test can
// confirm the gauge is left untouched when there's nothing to report a
// tenant label for.
func fetchesWithEmptyPartition(topic string, highWatermark int64) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: topic,
			Partitions: []kgo.FetchPartition{{
				Partition:     0,
				HighWatermark: highWatermark,
				Records:       nil,
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

// TestConsumerRunRoutesExhaustedReconcileFailuresToDLQ drives Run through
// two records that both fail reconciliation on every attempt. It asserts
// Run doesn't abort on the first failure (no head-of-line blocking): both
// records are retried up to maxRetries+1 times, published to the DLQ topic
// with failure context, and the loop keeps going until a genuine broker
// error ends it.
func TestConsumerRunRoutesExhaustedReconcileFailuresToDLQ(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	reconcileErr := errors.New("reconcile: database unavailable")
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchesWithValidRecords("transaction-events", 2),
			fetchesWithError("transaction-events", 0, brokerErr),
		},
	}
	consumer := &Consumer{
		client:     fake,
		topic:      "transaction-events",
		dlqTopic:   "transaction-events-dlq",
		maxRetries: 1,
		rec:        &fakeReconciler{err: reconcileErr},
	}

	err := consumer.Run(context.Background())
	if !errors.Is(err, brokerErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v (Run must not abort on reconcile failure)", err, brokerErr)
	}

	if len(fake.produced) != 2 {
		t.Fatalf("produced %d DLQ records, want 2", len(fake.produced))
	}
	for _, record := range fake.produced {
		if record.Topic != "transaction-events-dlq" {
			t.Errorf("DLQ record topic = %q, want %q", record.Topic, "transaction-events-dlq")
		}
		var event dlqEvent
		if err := json.Unmarshal(record.Value, &event); err != nil {
			t.Fatalf("unmarshal dlq event: %v", err)
		}
		if event.Attempts != 2 {
			t.Errorf("dlqEvent.Attempts = %d, want 2 (maxRetries=1 -> 2 total attempts)", event.Attempts)
		}
		if event.FailureReason != reconcileErr.Error() {
			t.Errorf("dlqEvent.FailureReason = %q, want %q", event.FailureReason, reconcileErr.Error())
		}
		if event.OriginalTopic != "transaction-events" {
			t.Errorf("dlqEvent.OriginalTopic = %q, want %q", event.OriginalTopic, "transaction-events")
		}
	}
}

// TestConsumerRunAbortsWhenDLQPublishFails confirms a DLQ publish failure
// (as opposed to an exhausted reconcile) does abort Run, since silently
// dropping a record that couldn't even reach the DLQ isn't acceptable.
func TestConsumerRunAbortsWhenDLQPublishFails(t *testing.T) {
	reconcileErr := errors.New("reconcile: database unavailable")
	dlqPublishErr := errors.New("dlq topic unavailable")
	fake := &fakeConsumerClient{
		fetches:    []kgo.Fetches{fetchesWithValidRecords("transaction-events", 1)},
		produceErr: dlqPublishErr,
	}
	consumer := &Consumer{
		client:     fake,
		topic:      "transaction-events",
		dlqTopic:   "transaction-events-dlq",
		maxRetries: 0,
		rec:        &fakeReconciler{err: reconcileErr},
	}

	err := consumer.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want non-nil")
	}
	if !errors.Is(err, dlqPublishErr) {
		t.Errorf("Run() error = %v, want it to wrap %v", err, dlqPublishErr)
	}
}

// TestConsumerReconcileWithRetry drives reconcileWithRetry directly across
// the attempt-count boundaries: success on the first try, success after a
// bounded number of failures, exhausting maxRetries+1 attempts, and the
// MaxRetries=0 boundary where a single attempt is made with no retry.
func TestConsumerReconcileWithRetry(t *testing.T) {
	reconcileErr := errors.New("reconcile: database unavailable")

	tests := []struct {
		name         string
		maxRetries   int
		fake         *fakeReconciler
		wantAttempts int
		wantErr      error
	}{
		{
			name:         "succeeds on first attempt",
			maxRetries:   3,
			fake:         &fakeReconciler{},
			wantAttempts: 1,
			wantErr:      nil,
		},
		{
			name:         "succeeds after two failures",
			maxRetries:   3,
			fake:         &fakeReconciler{err: reconcileErr, failTimes: 2},
			wantAttempts: 3,
			wantErr:      nil,
		},
		{
			name:         "exhausts retries and returns last error",
			maxRetries:   2,
			fake:         &fakeReconciler{err: reconcileErr},
			wantAttempts: 3,
			wantErr:      reconcileErr,
		},
		{
			name:         "MaxRetries zero allows a single attempt with no retry",
			maxRetries:   0,
			fake:         &fakeReconciler{err: reconcileErr},
			wantAttempts: 1,
			wantErr:      reconcileErr,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			consumer := &Consumer{maxRetries: testCase.maxRetries, rec: testCase.fake}

			attempts, err := consumer.reconcileWithRetry(context.Background(), ledger.Transaction{})

			if attempts != testCase.wantAttempts {
				t.Errorf("reconcileWithRetry() attempts = %d, want %d", attempts, testCase.wantAttempts)
			}
			if !errors.Is(err, testCase.wantErr) {
				t.Errorf("reconcileWithRetry() error = %v, want %v", err, testCase.wantErr)
			}
			if testCase.fake.calls != testCase.wantAttempts {
				t.Errorf("Reconcile called %d times, want %d", testCase.fake.calls, testCase.wantAttempts)
			}
		})
	}
}

// TestConsumerPublishToDLQPublishesEventContent asserts publishToDLQ
// produces a record on the configured DLQ topic whose key matches the
// original record's key (so downstream tooling can still group failures by
// tenant/instrument) and whose JSON body carries every dlqEvent field
// derived from the original record and failure context.
func TestConsumerPublishToDLQPublishesEventContent(t *testing.T) {
	failureErr := errors.New("reconcile: database unavailable")
	record := &kgo.Record{
		Topic:     "transaction-events",
		Partition: 3,
		Offset:    42,
		Key:       []byte("tenant-1"),
		Value:     []byte(`{"event_id":"bad"}`),
	}
	fake := &fakeConsumerClient{}
	consumer := &Consumer{client: fake, dlqTopic: "transaction-events-dlq"}

	before := time.Now().UTC()
	err := consumer.publishToDLQ(context.Background(), record, failureErr, 2)
	if err != nil {
		t.Fatalf("publishToDLQ() error = %v, want nil", err)
	}

	if len(fake.produced) != 1 {
		t.Fatalf("produced %d records, want 1", len(fake.produced))
	}
	produced := fake.produced[0]
	if produced.Topic != "transaction-events-dlq" {
		t.Errorf("produced record topic = %q, want %q", produced.Topic, "transaction-events-dlq")
	}
	if string(produced.Key) != "tenant-1" {
		t.Errorf("produced record key = %q, want %q (original partition key must be preserved)", produced.Key, "tenant-1")
	}

	var event dlqEvent
	if err := json.Unmarshal(produced.Value, &event); err != nil {
		t.Fatalf("unmarshal dlq event: %v", err)
	}
	if event.OriginalTopic != record.Topic {
		t.Errorf("dlqEvent.OriginalTopic = %q, want %q", event.OriginalTopic, record.Topic)
	}
	if event.OriginalPartition != record.Partition {
		t.Errorf("dlqEvent.OriginalPartition = %d, want %d", event.OriginalPartition, record.Partition)
	}
	if event.OriginalOffset != record.Offset {
		t.Errorf("dlqEvent.OriginalOffset = %d, want %d", event.OriginalOffset, record.Offset)
	}
	if event.Payload != string(record.Value) {
		t.Errorf("dlqEvent.Payload = %q, want %q", event.Payload, string(record.Value))
	}
	if event.FailureReason != failureErr.Error() {
		t.Errorf("dlqEvent.FailureReason = %q, want %q", event.FailureReason, failureErr.Error())
	}
	if event.Attempts != 2 {
		t.Errorf("dlqEvent.Attempts = %d, want 2", event.Attempts)
	}
	failedAt, err := time.Parse(time.RFC3339Nano, event.FailedAt)
	if err != nil {
		t.Fatalf("dlqEvent.FailedAt = %q, want a valid RFC3339Nano timestamp: %v", event.FailedAt, err)
	}
	if failedAt.Before(before) {
		t.Errorf("dlqEvent.FailedAt = %v, want it at or after %v", failedAt, before)
	}
}

// TestConsumerPublishToDLQReturnsErrorOnProduceFailure confirms a
// ProduceSync failure is surfaced as a wrapped error rather than swallowed,
// since a record that couldn't even reach the DLQ must not be dropped
// silently.
func TestConsumerPublishToDLQReturnsErrorOnProduceFailure(t *testing.T) {
	produceErr := errors.New("dlq topic unavailable")
	fake := &fakeConsumerClient{produceErr: produceErr}
	consumer := &Consumer{client: fake, dlqTopic: "transaction-events-dlq"}
	record := &kgo.Record{Topic: "transaction-events", Value: []byte("payload")}

	err := consumer.publishToDLQ(context.Background(), record, errors.New("boom"), 1)
	if !errors.Is(err, produceErr) {
		t.Errorf("publishToDLQ() error = %v, want it to wrap %v", err, produceErr)
	}
}

// TestConsumerRunRoutesDecodeFailuresToDLQWithEventContent confirms a
// record that fails to decode (rather than one that fails reconciliation)
// is routed to the DLQ topic with attempts=1 and its original location,
// partition key, and raw payload intact.
func TestConsumerRunRoutesDecodeFailuresToDLQWithEventContent(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	badPayload := []byte(`{not json`)
	record := &kgo.Record{Topic: "transaction-events", Partition: 3, Offset: 42, Key: []byte("tenant-1"), Value: badPayload}
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "transaction-events",
			Partitions: []kgo.FetchPartition{{
				Partition: 3,
				Records:   []*kgo.Record{record},
			}},
		}},
	}}
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{fetches, fetchesWithError("transaction-events", 0, brokerErr)},
	}
	consumer := &Consumer{client: fake, topic: "transaction-events", dlqTopic: "transaction-events-dlq", rec: &fakeReconciler{}}

	err := consumer.Run(context.Background())
	if !errors.Is(err, brokerErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", err, brokerErr)
	}

	if len(fake.produced) != 1 {
		t.Fatalf("produced %d DLQ records, want 1", len(fake.produced))
	}
	produced := fake.produced[0]
	if produced.Topic != "transaction-events-dlq" {
		t.Errorf("DLQ record topic = %q, want %q", produced.Topic, "transaction-events-dlq")
	}
	if string(produced.Key) != "tenant-1" {
		t.Errorf("DLQ record key = %q, want %q (original partition key must be preserved)", produced.Key, "tenant-1")
	}

	var event dlqEvent
	if err := json.Unmarshal(produced.Value, &event); err != nil {
		t.Fatalf("unmarshal dlq event: %v", err)
	}
	if event.OriginalTopic != "transaction-events" {
		t.Errorf("dlqEvent.OriginalTopic = %q, want %q", event.OriginalTopic, "transaction-events")
	}
	if event.OriginalPartition != 3 {
		t.Errorf("dlqEvent.OriginalPartition = %d, want 3", event.OriginalPartition)
	}
	if event.OriginalOffset != 42 {
		t.Errorf("dlqEvent.OriginalOffset = %d, want 42", event.OriginalOffset)
	}
	if event.Payload != string(badPayload) {
		t.Errorf("dlqEvent.Payload = %q, want %q", event.Payload, string(badPayload))
	}
	if event.Attempts != 1 {
		t.Errorf("dlqEvent.Attempts = %d, want 1 (decode failures aren't retried)", event.Attempts)
	}
	if event.FailureReason == "" {
		t.Error("dlqEvent.FailureReason is empty, want the decode error message")
	}
}

// cancelingReconciler is a reconciler test double that cancels ctx as a
// side effect of the first Reconcile call and always fails, simulating a
// shutdown signal arriving mid-reconciliation for TestConsumerRunStopsCleanlyWhenCtxCanceledDuringDLQRoute.
type cancelingReconciler struct {
	cancel context.CancelFunc
	err    error
}

func (fake *cancelingReconciler) Reconcile(ctx context.Context, txn ledger.Transaction) error {
	fake.cancel()
	return fake.err
}

// TestConsumerRunStopsCleanlyWhenCtxCanceledDuringDLQRoute reproduces the
// shutdown race: ctx is canceled while a record's reconciliation is failing
// and it's being routed to the DLQ. The resulting DLQ publish failure is a
// symptom of shutdown (ProduceSync fails because ctx is already done), not
// a genuine broker fault, so Run must still return nil rather than
// surfacing it as an error.
func TestConsumerRunStopsCleanlyWhenCtxCanceledDuringDLQRoute(t *testing.T) {
	reconcileErr := errors.New("reconcile: database unavailable")
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{fetchesWithValidRecords("transaction-events", 1)},
	}
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client:     fake,
		topic:      "transaction-events",
		dlqTopic:   "transaction-events-dlq",
		maxRetries: 0,
		rec:        &cancelingReconciler{cancel: cancel, err: reconcileErr},
	}

	err := consumer.Run(ctx)
	if err != nil {
		t.Errorf("Run() error = %v, want nil (ctx cancellation during dlq routing must count as clean shutdown, not a fatal error)", err)
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
	if config.DLQTopic != want.DLQTopic {
		t.Errorf("DLQTopic = %q, want %q", config.DLQTopic, want.DLQTopic)
	}
	if config.MaxRetries != want.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", config.MaxRetries, want.MaxRetries)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker-a:9092,broker-b:9092")
	t.Setenv("KAFKA_TOPIC", "custom-events")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-group")
	t.Setenv("KAFKA_DLQ_TOPIC", "custom-events-dlq")
	t.Setenv("KAFKA_MAX_RETRIES", "5")

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
	if config.DLQTopic != "custom-events-dlq" {
		t.Errorf("DLQTopic = %q, want %q", config.DLQTopic, "custom-events-dlq")
	}
	if config.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want %d", config.MaxRetries, 5)
	}
}

// TestConfigFromEnvDLQTopicOnlyOverride confirms KAFKA_DLQ_TOPIC is applied
// on its own, without any of the other override env vars set, so the field
// isn't only exercised as part of the "everything overridden" case.
func TestConfigFromEnvDLQTopicOnlyOverride(t *testing.T) {
	t.Setenv("KAFKA_DLQ_TOPIC", "custom-events-dlq")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v, want nil", err)
	}

	want := LocalConfig()
	if config.DLQTopic != "custom-events-dlq" {
		t.Errorf("DLQTopic = %q, want %q", config.DLQTopic, "custom-events-dlq")
	}
	if len(config.Brokers) != 1 || config.Brokers[0] != want.Brokers[0] {
		t.Errorf("Brokers = %v, want %v", config.Brokers, want.Brokers)
	}
	if config.Topic != want.Topic {
		t.Errorf("Topic = %q, want %q", config.Topic, want.Topic)
	}
	if config.GroupID != want.GroupID {
		t.Errorf("GroupID = %q, want %q", config.GroupID, want.GroupID)
	}
	if config.MaxRetries != want.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", config.MaxRetries, want.MaxRetries)
	}
}

func TestConfigFromEnvInvalidBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " , ")

	_, err := ConfigFromEnv()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("ConfigFromEnv() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
	}
}

func TestConfigFromEnvInvalidMaxRetries(t *testing.T) {
	t.Setenv("KAFKA_MAX_RETRIES", "not-a-number")

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
			config: Config{Topic: "t", GroupID: "g", DLQTopic: "dlq"},
		},
		{
			name:   "blank broker",
			config: Config{Brokers: []string{" "}, Topic: "t", GroupID: "g", DLQTopic: "dlq"},
		},
		{
			name:   "blank topic",
			config: Config{Brokers: []string{"localhost:9092"}, GroupID: "g", DLQTopic: "dlq"},
		},
		{
			name:   "blank group ID",
			config: Config{Brokers: []string{"localhost:9092"}, Topic: "t", DLQTopic: "dlq"},
		},
		{
			name:   "blank DLQ topic",
			config: Config{Brokers: []string{"localhost:9092"}, Topic: "t", GroupID: "g"},
		},
		{
			name:   "negative max retries",
			config: Config{Brokers: []string{"localhost:9092"}, Topic: "t", GroupID: "g", DLQTopic: "dlq", MaxRetries: -1},
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

// TestConfigValidateAcceptsZeroMaxRetries confirms MaxRetries=0 is a legal
// boundary (a single reconciliation attempt with no retry), distinct from
// the negative value TestConfigValidate already rejects.
func TestConfigValidateAcceptsZeroMaxRetries(t *testing.T) {
	config := Config{Brokers: []string{"localhost:9092"}, Topic: "t", GroupID: "g", DLQTopic: "dlq", MaxRetries: 0}

	if err := config.validate(); err != nil {
		t.Errorf("validate() error = %v, want nil", err)
	}
}

func TestNewConsumerRejectsInvalidConfig(t *testing.T) {
	if _, err := NewConsumer(Config{}, &fakeReconciler{}, prometheus.NewRegistry(), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("NewConsumer() error = %v, want errors.Is(err, ErrInvalidConfig)", err)
	}
}

// TestNewConsumerRejectsNilRegistry confirms a nil Registerer is rejected
// with a specific error message rather than panicking once NewMetrics
// tries to register against it.
func TestNewConsumerRejectsNilRegistry(t *testing.T) {
	config := Config{
		Brokers:  []string{"127.0.0.1:9"},
		Topic:    "transaction-events",
		GroupID:  "processor",
		DLQTopic: "transaction-events-dlq",
	}

	consumer, err := NewConsumer(config, &fakeReconciler{}, nil, nil)
	if err == nil {
		t.Fatal("NewConsumer() error = nil, want non-nil")
	}
	if consumer != nil {
		t.Errorf("NewConsumer() consumer = %v, want nil on error", consumer)
	}
	wantErrMsg := "new kafka consumer: reg must not be nil"
	if err.Error() != wantErrMsg {
		t.Errorf("NewConsumer() error = %q, want %q", err.Error(), wantErrMsg)
	}
}

// TestNewConsumerValidConfig ensures a valid Config yields a usable Consumer
// without dialing a broker: kgo.NewClient only connects lazily once a fetch
// is requested, so this stays a fast unit test rather than requiring live
// Kafka infrastructure.
func TestNewConsumerValidConfig(t *testing.T) {
	config := Config{
		Brokers:  []string{"127.0.0.1:9"},
		Topic:    "transaction-events",
		GroupID:  "processor",
		DLQTopic: "transaction-events-dlq",
	}

	consumer, err := NewConsumer(config, &fakeReconciler{}, prometheus.NewRegistry(), nil)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v, want nil", err)
	}
	defer consumer.Close()

	if consumer.topic != config.Topic {
		t.Errorf("consumer.topic = %q, want %q", consumer.topic, config.Topic)
	}
}

// TestConsumerRunReportsConsumerLag drives Run through a single fetch batch
// on one partition with a known HighWatermark and two records, then
// confirms the lag gauge on the registry passed to NewMetrics reflects
// HighWatermark-1-lastOffset, labeled by the partition's last decoded
// record's tenant.
func TestConsumerRunReportsConsumerLag(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchesWithHighWatermarkAndTenants("transaction-events", 10, "tenant-1", "tenant-2"),
			fetchesWithError("transaction-events", 0, brokerErr),
		},
	}
	registry := prometheus.NewRegistry()
	consumerMetrics, err := NewMetrics(registry, metrics.NewKnownTenants("tenant-2"))
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	consumer := &Consumer{
		client:  fake,
		topic:   "transaction-events",
		rec:     &fakeReconciler{},
		metrics: consumerMetrics,
	}

	if err := consumer.Run(context.Background()); !errors.Is(err, brokerErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", err, brokerErr)
	}

	// Two records at offsets 0 and 1: last offset is 1, HighWatermark is 10,
	// so lag = 10 - 1 - 1 = 8, labeled by the last record's tenant (tenant-2).
	scraped := scrapeMetrics(t, registry)
	requireMetricsLine(t, scraped, `processor_kafka_consumer_lag_messages{tenant="tenant-2"} 8`)
	requireMetricsLineAbsent(t, scraped, `processor_kafka_consumer_lag_messages{tenant="tenant-1"}`)
}

// TestConsumerRunEmptyPartitionSkipsLagGauge confirms a partition fetch
// with zero records leaves the lag gauge untouched: no series is created,
// since there's no last-decoded-record tenant to label it with.
func TestConsumerRunEmptyPartitionSkipsLagGauge(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	fake := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchesWithEmptyPartition("transaction-events", 10),
			fetchesWithError("transaction-events", 0, brokerErr),
		},
	}
	registry := prometheus.NewRegistry()
	consumerMetrics, err := NewMetrics(registry, nil)
	if err != nil {
		t.Fatalf("NewMetrics() unexpected error: %v", err)
	}
	consumer := &Consumer{
		client:  fake,
		topic:   "transaction-events",
		rec:     &fakeReconciler{},
		metrics: consumerMetrics,
	}

	if err := consumer.Run(context.Background()); !errors.Is(err, brokerErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", err, brokerErr)
	}

	scraped := scrapeMetrics(t, registry)
	requireMetricsLineAbsent(t, scraped, "processor_kafka_consumer_lag_messages{")
}
