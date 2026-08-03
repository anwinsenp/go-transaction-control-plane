package api

import (
	"encoding/json"
	"log"
	"net/http"
)

// maxTransactionEventBytes caps the request body size for a single
// transaction event, which is expected to be a small, fixed-shape JSON
// object.
const maxTransactionEventBytes = 4 << 10

// TransactionEventRequest is the wire format for a mock transaction event
// submitted to the ingestion service. Amount fields are strings so the
// handler can validate presence without committing to a numeric decoding
// strategy before the hot-path ingestion logic lands.
type TransactionEventRequest struct {
	EventID    string `json:"event_id"`
	TenantID   string `json:"tenant_id"`
	Instrument string `json:"instrument"`
	Side       string `json:"side"`
	Quantity   string `json:"quantity"`
	Price      string `json:"price"`
	Currency   string `json:"currency"`
	OccurredAt string `json:"occurred_at"`
}

// transactionHandler accepts a mock transaction event over REST. It is a
// wiring skeleton: it validates that the request is well-formed JSON with
// the required fields present, then acknowledges receipt. It does not yet
// validate field contents, publish to Kafka, or write to the ledger — that
// logic lands with the hot-path ingestion work.
func transactionHandler(w http.ResponseWriter, r *http.Request) {
	var event TransactionEventRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxTransactionEventBytes)
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if missingTransactionFields(event) {
		http.Error(w, "missing required fields", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(event); err != nil {
		log.Printf("encode transaction response: %v", err)
	}
}

// missingTransactionFields reports whether any field required to accept a
// mock transaction event is empty.
func missingTransactionFields(event TransactionEventRequest) bool {
	return event.EventID == "" ||
		event.TenantID == "" ||
		event.Instrument == "" ||
		event.Side == "" ||
		event.Quantity == "" ||
		event.Price == "" ||
		event.Currency == "" ||
		event.OccurredAt == ""
}
