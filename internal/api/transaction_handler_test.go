package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTransactionHandler(t *testing.T) {
	validEvent := TransactionEventRequest{
		EventID:    "11111111-1111-1111-1111-111111111111",
		TenantID:   "tenant-a",
		Instrument: "AAPL",
		Side:       "BUY",
		Quantity:   "10",
		Price:      "150.25",
		Currency:   "USD",
		OccurredAt: "2026-08-03T12:00:00Z",
	}

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "valid event is accepted",
			body:       mustMarshal(t, validEvent),
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "malformed json is rejected",
			body:       "{not json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing required field is rejected",
			body:       `{"tenant_id":"tenant-a"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "oversized body is rejected",
			body:       oversizedBody(t, validEvent),
			wantStatus: http.StatusBadRequest,
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

			if testCase.wantStatus != http.StatusAccepted {
				return
			}

			if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
				t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
			}

			var echoedEvent TransactionEventRequest
			if err := json.NewDecoder(recorder.Body).Decode(&echoedEvent); err != nil {
				t.Fatalf("decode response body: %v", err)
			}
			if echoedEvent != validEvent {
				t.Errorf("echoed event = %+v, want %+v", echoedEvent, validEvent)
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
