package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// fakePublisher is an ingestion.Publisher test double that records the
// events it was asked to publish and returns a caller-configured error, so
// the handler's publish wiring can be tested without a live Kafka broker.
type fakePublisher struct {
	mu        sync.Mutex
	published []ingestion.Event
	err       error
}

func (fake *fakePublisher) Publish(_ context.Context, event ingestion.Event) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.err != nil {
		return fake.err
	}
	fake.published = append(fake.published, event)
	return nil
}

func validTransactionEvent() TransactionEventRequest {
	return TransactionEventRequest{
		EventID:    "11111111-1111-1111-1111-111111111111",
		TenantID:   "tenant-a",
		Instrument: "AAPL",
		Side:       "BUY",
		Quantity:   "10",
		Price:      "150.25",
		Currency:   "USD",
		OccurredAt: "2026-08-03T12:00:00Z",
	}
}

func TestTransactionHandler(t *testing.T) {
	validEvent := validTransactionEvent()

	withField := func(mutate func(*TransactionEventRequest)) TransactionEventRequest {
		event := validTransactionEvent()
		mutate(&event)
		return event
	}

	tests := []struct {
		name              string
		body              string
		wantStatus        int
		wantEvent         TransactionEventRequest
		wantErrorContains string
	}{
		{
			name:       "valid event is accepted",
			body:       mustMarshal(t, validEvent),
			wantStatus: http.StatusAccepted,
			wantEvent:  validEvent,
		},
		{
			name:              "malformed json is rejected",
			body:              "{not json",
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "invalid request body",
		},
		{
			name:              "missing required field is rejected",
			body:              `{"tenant_id":"tenant-a"}`,
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "missing required fields",
		},
		{
			name:              "oversized body is rejected",
			body:              oversizedBody(t, validEvent),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "invalid request body",
		},
		{
			name: "non-uuid event_id is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.EventID = "not-a-uuid"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "event_id must be a valid UUID",
		},
		{
			name: "tenant_id with uppercase letters is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.TenantID = "Tenant-A"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "tenant_id must be",
		},
		{
			name: "tenant_id with an invalid character is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.TenantID = "tenant_a"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "tenant_id must be",
		},
		{
			name: "oversized tenant_id is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.TenantID = strings.Repeat("a", 65)
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "tenant_id must be",
		},
		{
			name: "instrument with lowercase letters is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Instrument = "aapl"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "instrument must be",
		},
		{
			name: "instrument with an invalid character is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Instrument = "AAPL$"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "instrument must be",
		},
		{
			name: "oversized instrument is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Instrument = strings.Repeat("A", 17)
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "instrument must be",
		},
		{
			name: "instrument with a dot is accepted",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Instrument = "BRK.B"
			})),
			wantStatus: http.StatusAccepted,
			wantEvent: withField(func(event *TransactionEventRequest) {
				event.Instrument = "BRK.B"
			}),
		},
		{
			name: "unknown side is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Side = "HOLD"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "side must be",
		},
		{
			name: "non-numeric quantity is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Quantity = "ten"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "quantity must be a positive decimal number",
		},
		{
			name: "zero quantity is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Quantity = "0"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "quantity must be a positive decimal number",
		},
		{
			name: "zero price is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Price = "0"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "price must be a positive decimal number",
		},
		{
			name: "negative quantity is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Quantity = "-5"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "quantity must be a positive decimal number",
		},
		{
			name: "negative price is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Price = "-1.50"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "price must be a positive decimal number",
		},
		{
			name: "lowercase side is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Side = "buy"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "side must be",
		},
		{
			name: "lowercase currency is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Currency = "usd"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "currency must be",
		},
		{
			name: "non-iso-length currency is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Currency = "US"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "currency must be",
		},
		{
			name: "currency with a digit is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Currency = "US1"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "currency must be",
		},
		{
			name: "mixed-case currency is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Currency = "Usd"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "currency must be",
		},
		{
			name: "non-rfc3339 occurred_at is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.OccurredAt = "2026-08-03"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "occurred_at must be",
		},
		{
			name: "oversized quantity magnitude is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Quantity = "1e400"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "quantity must be a positive decimal number",
		},
		{
			name: "oversized price magnitude is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Price = "1e400"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "price must be a positive decimal number",
		},
		{
			name: "scientific notation quantity and price is accepted",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Quantity = "1e2"
				event.Price = "1e2"
			})),
			wantStatus: http.StatusAccepted,
			wantEvent: withField(func(event *TransactionEventRequest) {
				event.Quantity = "1e2"
				event.Price = "1e2"
			}),
		},
		{
			name: "quantity exactly at max magnitude is accepted",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Quantity = "10000000000"
			})),
			wantStatus: http.StatusAccepted,
			wantEvent: withField(func(event *TransactionEventRequest) {
				event.Quantity = "10000000000"
			}),
		},
		{
			name: "quantity one unit above max magnitude is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Quantity = "10000000001"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "quantity must be a positive decimal number",
		},
		{
			name: "price exactly at max magnitude is accepted",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Price = "10000000000"
			})),
			wantStatus: http.StatusAccepted,
			wantEvent: withField(func(event *TransactionEventRequest) {
				event.Price = "10000000000"
			}),
		},
		{
			name: "price one unit above max magnitude is rejected",
			body: mustMarshal(t, withField(func(event *TransactionEventRequest) {
				event.Price = "10000000001"
			})),
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "price must be a positive decimal number",
		},
		{
			name:              "whitespace then trailing garbage after JSON value is rejected",
			body:              mustMarshal(t, validEvent) + "  x",
			wantStatus:        http.StatusBadRequest,
			wantErrorContains: "invalid request body: trailing data",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(testCase.body))
			recorder := httptest.NewRecorder()

			newTransactionHandler(&fakePublisher{})(recorder, request)

			if recorder.Code != testCase.wantStatus {
				t.Errorf("status = %d, want %d", recorder.Code, testCase.wantStatus)
			}

			if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
				t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
			}

			if testCase.wantStatus != http.StatusAccepted {
				var response errorResponse
				if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
					t.Fatalf("decode error response: %v", err)
				}
				if response.Error == "" {
					t.Errorf("error message is empty, want a clear description")
				}
				if !strings.Contains(response.Error, testCase.wantErrorContains) {
					t.Errorf("error message = %q, want it to contain %q", response.Error, testCase.wantErrorContains)
				}
				return
			}

			var echoedEvent TransactionEventRequest
			if err := json.NewDecoder(recorder.Body).Decode(&echoedEvent); err != nil {
				t.Fatalf("decode response body: %v", err)
			}
			if echoedEvent != testCase.wantEvent {
				t.Errorf("echoed event = %+v, want %+v", echoedEvent, testCase.wantEvent)
			}
		})
	}
}

// TestTransactionHandlerBodyBufferDoesNotLeakBetweenRequests exercises the
// pooled request-body buffer and response codec across a sequence of
// requests on the same goroutine, so a Reset() that failed to clear stale
// bytes/length would surface as garbled JSON or a leftover response body.
// TestTransactionHandlerPublishesValidatedEvent locks in that a valid
// request is handed to the publisher as a fully-parsed ingestion.Event
// before the client is acknowledged.
func TestTransactionHandlerPublishesValidatedEvent(t *testing.T) {
	validEvent := validTransactionEvent()
	publisher := &fakePublisher{}

	request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(mustMarshal(t, validEvent)))
	recorder := httptest.NewRecorder()

	newTransactionHandler(publisher)(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published %d events, want 1", len(publisher.published))
	}

	published := publisher.published[0]
	if published.EventID.String() != validEvent.EventID {
		t.Errorf("published EventID = %v, want %v", published.EventID, validEvent.EventID)
	}
	if published.TenantID != validEvent.TenantID {
		t.Errorf("published TenantID = %q, want %q", published.TenantID, validEvent.TenantID)
	}
	if published.Instrument != validEvent.Instrument {
		t.Errorf("published Instrument = %q, want %q", published.Instrument, validEvent.Instrument)
	}
	if string(published.Side) != validEvent.Side {
		t.Errorf("published Side = %q, want %q", published.Side, validEvent.Side)
	}
	if published.Quantity != 10*ledger.AmountScale {
		t.Errorf("published Quantity = %d, want %d", published.Quantity, 10*ledger.AmountScale)
	}
	if published.Price != 15025000000 {
		t.Errorf("published Price = %d, want 150.25", published.Price)
	}
	if published.Currency != validEvent.Currency {
		t.Errorf("published Currency = %q, want %q", published.Currency, validEvent.Currency)
	}
}

// TestTransactionHandlerSurfacesPublishError ensures a Kafka publish
// failure is surfaced to the client as a 503, not swallowed as a 202.
func TestTransactionHandlerSurfacesPublishError(t *testing.T) {
	publishErr := errors.New("kafka broker unreachable")
	publisher := &fakePublisher{err: publishErr}

	request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(mustMarshal(t, validTransactionEvent())))
	recorder := httptest.NewRecorder()

	newTransactionHandler(publisher)(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}

	var response errorResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error == "" {
		t.Error("error message is empty, want a clear description")
	}
}

// TestTransactionHandlerRejectsTrailingDataAfterJSONValue locks in the
// handler's stricter-than-default json.Decoder behavior: unlike a bare
// Decoder, which consumes only the first JSON value and silently ignores
// anything after it, the handler must reject non-whitespace bytes left
// over past the decoded value.
func TestTransactionHandlerRejectsTrailingDataAfterJSONValue(t *testing.T) {
	body := mustMarshal(t, validTransactionEvent()) + `{"trailing":"garbage"}`

	request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	recorder := httptest.NewRecorder()

	newTransactionHandler(&fakePublisher{})(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

// TestTransactionHandlerAcceptsTrailingWhitespaceAfterJSONValue verifies
// that trailing JSON whitespace (a common artifact of pretty-printed or
// newline-terminated request bodies) is still tolerated, since it isn't
// "garbage" in any meaningful sense.
func TestTransactionHandlerAcceptsTrailingWhitespaceAfterJSONValue(t *testing.T) {
	body := mustMarshal(t, validTransactionEvent()) + "\n  \t\n"

	request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	recorder := httptest.NewRecorder()

	newTransactionHandler(&fakePublisher{})(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}

	var echoedEvent TransactionEventRequest
	if err := json.NewDecoder(recorder.Body).Decode(&echoedEvent); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if echoedEvent != validTransactionEvent() {
		t.Errorf("echoed event = %+v, want %+v", echoedEvent, validTransactionEvent())
	}
}

func TestTransactionHandlerBodyBufferDoesNotLeakBetweenRequests(t *testing.T) {
	largeEvent := validTransactionEvent()
	largeEvent.TenantID = strings.Repeat("a", 64)
	largeEvent.Instrument = strings.Repeat("A", 16)

	handler := newTransactionHandler(&fakePublisher{})

	firstRequest := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(mustMarshal(t, largeEvent)))
	firstRecorder := httptest.NewRecorder()
	handler(firstRecorder, firstRequest)

	if firstRecorder.Code != http.StatusAccepted {
		t.Fatalf("first request status = %d, want %d", firstRecorder.Code, http.StatusAccepted)
	}

	var firstEchoed TransactionEventRequest
	if err := json.Unmarshal(firstRecorder.Body.Bytes(), &firstEchoed); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if firstEchoed != largeEvent {
		t.Errorf("first echoed event = %+v, want %+v", firstEchoed, largeEvent)
	}

	secondRequest := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader("{}"))
	secondRecorder := httptest.NewRecorder()
	handler(secondRecorder, secondRequest)

	if secondRecorder.Code != http.StatusBadRequest {
		t.Fatalf("second request status = %d, want %d", secondRecorder.Code, http.StatusBadRequest)
	}

	var secondError errorResponse
	if err := json.Unmarshal(secondRecorder.Body.Bytes(), &secondError); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	wantSecondError := "missing required fields"
	if secondError.Error != wantSecondError {
		t.Errorf("second error = %q, want %q (leftover pooled bytes would surface as a different error or a decode failure)", secondError.Error, wantSecondError)
	}

	thirdRequest := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(mustMarshal(t, largeEvent)))
	thirdRecorder := httptest.NewRecorder()
	handler(thirdRecorder, thirdRequest)

	if thirdRecorder.Code != http.StatusAccepted {
		t.Fatalf("third request status = %d, want %d", thirdRecorder.Code, http.StatusAccepted)
	}

	var thirdEchoed TransactionEventRequest
	if err := json.Unmarshal(thirdRecorder.Body.Bytes(), &thirdEchoed); err != nil {
		t.Fatalf("decode third response: %v", err)
	}
	if thirdEchoed != largeEvent {
		t.Errorf("third echoed event = %+v, want %+v", thirdEchoed, largeEvent)
	}
}

// TestTransactionHandlerConcurrentRequestsDoNotShareState drives concurrent
// requests through transactionHandler so sync.Pool's Get/Put usage on the
// hot path can be validated under -race, and so pooled buffers/codecs
// can't leak one goroutine's request or response into another's.
func TestTransactionHandlerConcurrentRequestsDoNotShareState(t *testing.T) {
	const goroutineCount = 8
	const requestsPerGoroutine = 20

	var waitGroup sync.WaitGroup
	failures := make(chan string, goroutineCount*requestsPerGoroutine)
	handler := newTransactionHandler(&fakePublisher{})

	for goroutineIndex := 0; goroutineIndex < goroutineCount; goroutineIndex++ {
		waitGroup.Add(1)
		go func(goroutineIndex int) {
			defer waitGroup.Done()

			for requestIndex := 0; requestIndex < requestsPerGoroutine; requestIndex++ {
				event := validTransactionEvent()
				event.EventID = uuid.New().String()
				event.TenantID = fmt.Sprintf("tenant-%d-%d", goroutineIndex, requestIndex)

				payload, err := json.Marshal(event)
				if err != nil {
					failures <- fmt.Sprintf("goroutine %d request %d: marshal event: %v", goroutineIndex, requestIndex, err)
					continue
				}

				request := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(payload))
				recorder := httptest.NewRecorder()
				handler(recorder, request)

				if recorder.Code != http.StatusAccepted {
					failures <- fmt.Sprintf("goroutine %d request %d: status = %d, want %d", goroutineIndex, requestIndex, recorder.Code, http.StatusAccepted)
					continue
				}

				var echoedEvent TransactionEventRequest
				if err := json.Unmarshal(recorder.Body.Bytes(), &echoedEvent); err != nil {
					failures <- fmt.Sprintf("goroutine %d request %d: decode response: %v", goroutineIndex, requestIndex, err)
					continue
				}
				if echoedEvent != event {
					failures <- fmt.Sprintf("goroutine %d request %d: echoed event = %+v, want %+v", goroutineIndex, requestIndex, echoedEvent, event)
				}
			}
		}(goroutineIndex)
	}

	waitGroup.Wait()
	close(failures)

	for failure := range failures {
		t.Error(failure)
	}
}

// TestWriteJSONEncodeFailureReturnsErrorStatus ensures that when the value
// passed to writeJSON can't be JSON-encoded, the client still receives an
// explicit error status rather than an implicit 200 OK with an empty body.
func TestWriteJSONEncodeFailureReturnsErrorStatus(t *testing.T) {
	recorder := httptest.NewRecorder()

	// chan int has no JSON representation, so json.Encoder.Encode fails.
	writeJSON(recorder, http.StatusAccepted, struct {
		Unencodable chan int `json:"unencodable"`
	}{})

	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	if recorder.Body.Len() == 0 {
		t.Errorf("body is empty, want an error message describing the failure")
	}
}

// BenchmarkTransactionHandler reports allocs/op for a full request/response
// round trip through transactionHandler (decode -> validate -> publish),
// including httptest.NewRequest and httptest.NewRecorder scaffolding
// allocated fresh each iteration. That scaffolding overhead is mixed into
// the reported number alongside the handler's own allocations, so this is
// not a pure hot-path allocation figure for the handler in isolation.
//
// Sub-benchmarks cover the realistic range of payload sizes validation
// allows for the two variable-length fields (tenant_id: 1-64 chars,
// instrument: 1-16 chars), since allocation behavior on the JSON decode and
// publish path can vary with payload size.
func BenchmarkTransactionHandler(b *testing.B) {
	tests := []struct {
		name  string
		event TransactionEventRequest
	}{
		{
			name:  "typical",
			event: validTransactionEvent(),
		},
		{
			name: "min_length_fields",
			event: withMutation(validTransactionEvent(), func(event *TransactionEventRequest) {
				event.TenantID = "a"
				event.Instrument = "A"
			}),
		},
		{
			name: "max_length_fields",
			event: withMutation(validTransactionEvent(), func(event *TransactionEventRequest) {
				event.TenantID = strings.Repeat("a", 64)
				event.Instrument = strings.Repeat("A", 16)
			}),
		},
	}

	for _, testCase := range tests {
		b.Run(testCase.name, func(b *testing.B) {
			payload, err := json.Marshal(testCase.event)
			if err != nil {
				b.Fatalf("marshal event: %v", err)
			}
			body := string(payload)
			handler := newTransactionHandler(&fakePublisher{})

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
				recorder := httptest.NewRecorder()
				handler(recorder, request)
			}
		})
	}
}

// TestToIngestionEvent directly exercises toIngestionEvent's field mapping,
// independent of the handler, so a drift between validate()'s accepted
// shapes and toIngestionEvent's parsing (e.g. scientific notation, decimal
// precision) is caught even if a future handler change stops calling both
// in sequence.
func TestToIngestionEvent(t *testing.T) {
	tests := []struct {
		name    string
		request TransactionEventRequest
	}{
		{
			name:    "typical buy event",
			request: validTransactionEvent(),
		},
		{
			name: "sell side",
			request: withMutation(validTransactionEvent(), func(event *TransactionEventRequest) {
				event.Side = "SELL"
			}),
		},
		{
			name: "scientific notation quantity and price",
			request: withMutation(validTransactionEvent(), func(event *TransactionEventRequest) {
				event.Quantity = "1e2"
				event.Price = "1e2"
			}),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			event, err := testCase.request.toIngestionEvent()
			if err != nil {
				t.Fatalf("toIngestionEvent() error = %v, want nil", err)
			}

			if event.EventID.String() != testCase.request.EventID {
				t.Errorf("EventID = %v, want %v", event.EventID, testCase.request.EventID)
			}
			if event.TenantID != testCase.request.TenantID {
				t.Errorf("TenantID = %q, want %q", event.TenantID, testCase.request.TenantID)
			}
			if event.Instrument != testCase.request.Instrument {
				t.Errorf("Instrument = %q, want %q", event.Instrument, testCase.request.Instrument)
			}
			if string(event.Side) != testCase.request.Side {
				t.Errorf("Side = %q, want %q", event.Side, testCase.request.Side)
			}
			if event.Currency != testCase.request.Currency {
				t.Errorf("Currency = %q, want %q", event.Currency, testCase.request.Currency)
			}
			if event.SchemaVersion != ingestion.CurrentSchemaVersion {
				t.Errorf("SchemaVersion = %d, want %d", event.SchemaVersion, ingestion.CurrentSchemaVersion)
			}

			wantQuantity, err := ledger.ParseAmount(testCase.request.Quantity)
			if err != nil {
				t.Fatalf("parse want quantity: %v", err)
			}
			if event.Quantity != wantQuantity {
				t.Errorf("Quantity = %d, want %d", event.Quantity, wantQuantity)
			}

			wantPrice, err := ledger.ParseAmount(testCase.request.Price)
			if err != nil {
				t.Fatalf("parse want price: %v", err)
			}
			if event.Price != wantPrice {
				t.Errorf("Price = %d, want %d", event.Price, wantPrice)
			}

			wantOccurredAt, err := time.Parse(time.RFC3339, testCase.request.OccurredAt)
			if err != nil {
				t.Fatalf("parse want occurred_at: %v", err)
			}
			if !event.OccurredAt.Equal(wantOccurredAt) {
				t.Errorf("OccurredAt = %v, want %v", event.OccurredAt, wantOccurredAt)
			}
		})
	}
}

// contextCapturingPublisher is an ingestion.Publisher test double that
// records the context.Context it was called with, so a test can assert the
// handler forwards the request's own context rather than a detached one.
type contextCapturingPublisher struct {
	capturedCtx context.Context
}

func (fake *contextCapturingPublisher) Publish(ctx context.Context, _ ingestion.Event) error {
	fake.capturedCtx = ctx
	return nil
}

type requestContextKey struct{}

// TestTransactionHandlerPublishesUsingRequestContext ensures the handler
// forwards r.Context() to Publish rather than context.Background(), which
// matters once the Kafka client starts honoring cancellation/deadlines.
func TestTransactionHandlerPublishesUsingRequestContext(t *testing.T) {
	publisher := &contextCapturingPublisher{}

	request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(mustMarshal(t, validTransactionEvent())))
	ctx := context.WithValue(request.Context(), requestContextKey{}, "marker")
	request = request.WithContext(ctx)
	recorder := httptest.NewRecorder()

	newTransactionHandler(publisher)(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	if publisher.capturedCtx == nil {
		t.Fatal("Publish() was called with a nil context")
	}
	if value := publisher.capturedCtx.Value(requestContextKey{}); value != "marker" {
		t.Errorf("Publish() context value = %v, want %q (handler must forward r.Context(), not context.Background())", value, "marker")
	}
}

// TestTransactionEventRequestReset verifies Reset() clears every field back
// to its zero value directly, independent of the handler. The handler's
// pool-reuse tests can't surface a partial Reset() bug because validate()
// requires every field to be non-empty, so JSON decoding always overwrites
// every field anyway; this test exercises Reset() on its own so a future
// field added to TransactionEventRequest without updating Reset() is caught
// here rather than silently leaking stale data through the pool.
func TestTransactionEventRequestReset(t *testing.T) {
	event := TransactionEventRequest{
		EventID:    "11111111-1111-1111-1111-111111111111",
		TenantID:   "tenant-a",
		Instrument: "AAPL",
		Side:       "BUY",
		Quantity:   "10",
		Price:      "150.25",
		Currency:   "USD",
		OccurredAt: "2026-08-03T12:00:00Z",
	}

	event.Reset()

	if event != (TransactionEventRequest{}) {
		t.Errorf("event after Reset() = %+v, want zero value", event)
	}
}

// TestIsJSONWhitespace directly exercises isJSONWhitespace's classification
// of trailing bytes, independent of the handler, covering the RFC 8259 §2
// whitespace set plus the mixed whitespace/non-whitespace shapes that would
// otherwise only be reachable indirectly through a full handler request.
func TestIsJSONWhitespace(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{
			name: "nil data is whitespace",
			data: nil,
			want: true,
		},
		{
			name: "empty data is whitespace",
			data: []byte(""),
			want: true,
		},
		{
			name: "space is whitespace",
			data: []byte("   "),
			want: true,
		},
		{
			name: "tab, carriage return, and newline are whitespace",
			data: []byte("\t\r\n\t\r\n"),
			want: true,
		},
		{
			name: "single non-whitespace byte is not whitespace",
			data: []byte("x"),
			want: false,
		},
		{
			name: "leading whitespace followed by garbage is not whitespace",
			data: []byte("  x"),
			want: false,
		},
		{
			name: "garbage followed by trailing whitespace is not whitespace",
			data: []byte("x  "),
			want: false,
		},
		{
			name: "whitespace surrounding a single garbage byte is not whitespace",
			data: []byte(" \t x \n "),
			want: false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isJSONWhitespace(testCase.data); got != testCase.want {
				t.Errorf("isJSONWhitespace(%q) = %v, want %v", testCase.data, got, testCase.want)
			}
		})
	}
}

// withMutation returns a copy of event with mutate applied, so table-driven
// tests can derive variants from validTransactionEvent() without repeating
// its full field list.
func withMutation(event TransactionEventRequest, mutate func(*TransactionEventRequest)) TransactionEventRequest {
	mutate(&event)
	return event
}

// oversizedBody returns a JSON body for event padded past
// maxTransactionEventBytes via an oversized OccurredAt value, so decoding
// hits http.MaxBytesReader's limit rather than a schema error.
func oversizedBody(t *testing.T, event TransactionEventRequest) string {
	t.Helper()
	event.OccurredAt = strings.Repeat("0", maxTransactionEventBytes)
	return mustMarshal(t, event)
}

func mustMarshal(t *testing.T, event TransactionEventRequest) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(event); err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return buffer.String()
}
