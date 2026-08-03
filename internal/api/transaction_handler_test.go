package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
