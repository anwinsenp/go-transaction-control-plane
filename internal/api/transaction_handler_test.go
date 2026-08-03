package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

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
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(testCase.body))
			recorder := httptest.NewRecorder()

			transactionHandler(recorder, request)

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
// TestTransactionHandlerIgnoresTrailingDataAfterJSONValue locks in
// json.Decoder's trailing-data tolerance: decoding via a Decoder consumes
// only the first JSON value and ignores anything after it, unlike
// json.Unmarshal which would reject trailing non-whitespace bytes. This
// behavior must be preserved by the pooled-buffer decode path.
func TestTransactionHandlerIgnoresTrailingDataAfterJSONValue(t *testing.T) {
	body := mustMarshal(t, validTransactionEvent()) + `{"trailing":"garbage"}`

	request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	recorder := httptest.NewRecorder()

	transactionHandler(recorder, request)

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

	firstRequest := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(mustMarshal(t, largeEvent)))
	firstRecorder := httptest.NewRecorder()
	transactionHandler(firstRecorder, firstRequest)

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
	transactionHandler(secondRecorder, secondRequest)

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
	transactionHandler(thirdRecorder, thirdRequest)

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
				transactionHandler(recorder, request)

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
// round trip through transactionHandler, including httptest.NewRequest and
// httptest.NewRecorder scaffolding allocated fresh each iteration. That
// scaffolding overhead is mixed into the reported number alongside the
// handler's own allocations, so this is not a pure hot-path allocation
// figure for the handler in isolation.
func BenchmarkTransactionHandler(b *testing.B) {
	payload, err := json.Marshal(validTransactionEvent())
	if err != nil {
		b.Fatalf("marshal event: %v", err)
	}
	body := string(payload)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
		recorder := httptest.NewRecorder()
		transactionHandler(recorder, request)
	}
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
