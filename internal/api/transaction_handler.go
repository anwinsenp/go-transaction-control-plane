package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// maxTransactionEventBytes caps the request body size for a single
// transaction event, which is expected to be a small, fixed-shape JSON
// object.
const maxTransactionEventBytes = 4 << 10

// maxDecimalMagnitude bounds quantity and price so a malformed or
// malicious payload (e.g. "1e400") can't reach the ledger's decimal
// arithmetic or Postgres numeric columns with a value large enough to
// overflow them. 10^15 is far above any realistic trade quantity or price.
var maxDecimalMagnitude = decimal.RequireFromString("1000000000000000")

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

// errorResponse is the wire format for a client-facing 4xx error.
type errorResponse struct {
	Error string `json:"error"`
}

// transactionHandler accepts a mock transaction event over REST. It is a
// wiring skeleton: it validates that the request is well-formed JSON with
// the required fields present and well-formed, then acknowledges receipt.
// It does not yet publish to Kafka or write to the ledger — that logic
// lands with the hot-path ingestion work.
func transactionHandler(w http.ResponseWriter, r *http.Request) {
	var event TransactionEventRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxTransactionEventBytes)
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := event.validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(event); err != nil {
		log.Printf("encode transaction response: %v", err)
	}
}

// validate reports the first invalid or missing field in event, or nil if
// the event is well-formed and safe to accept.
func (event TransactionEventRequest) validate() error {
	if event.EventID == "" || event.TenantID == "" || event.Instrument == "" ||
		event.Side == "" || event.Quantity == "" || event.Price == "" ||
		event.Currency == "" || event.OccurredAt == "" {
		return fmt.Errorf("missing required fields")
	}

	if _, err := uuid.Parse(event.EventID); err != nil {
		return fmt.Errorf("event_id must be a valid UUID")
	}

	if !isValidTenantID(event.TenantID) {
		return fmt.Errorf("tenant_id must be 1-64 lowercase letters, digits, or hyphens")
	}

	if !isValidInstrument(event.Instrument) {
		return fmt.Errorf("instrument must be 1-16 uppercase letters, digits, or dots")
	}

	if ledger.Side(event.Side) != ledger.SideBuy && ledger.Side(event.Side) != ledger.SideSell {
		return fmt.Errorf("side must be %q or %q", ledger.SideBuy, ledger.SideSell)
	}

	quantity, err := decimal.NewFromString(event.Quantity)
	if err != nil || !quantity.IsPositive() || quantity.GreaterThan(maxDecimalMagnitude) {
		return fmt.Errorf("quantity must be a positive decimal number no greater than %s", maxDecimalMagnitude)
	}

	price, err := decimal.NewFromString(event.Price)
	if err != nil || !price.IsPositive() || price.GreaterThan(maxDecimalMagnitude) {
		return fmt.Errorf("price must be a positive decimal number no greater than %s", maxDecimalMagnitude)
	}

	if !isValidCurrencyCode(event.Currency) {
		return fmt.Errorf("currency must be a 3-letter uppercase ISO 4217 code")
	}

	if _, err := time.Parse(time.RFC3339, event.OccurredAt); err != nil {
		return fmt.Errorf("occurred_at must be an RFC3339 timestamp")
	}

	return nil
}

// isValidCurrencyCode reports whether code has the shape of an ISO 4217
// currency code: exactly three uppercase ASCII letters.
func isValidCurrencyCode(code string) bool {
	if len(code) != 3 {
		return false
	}
	for _, letter := range code {
		if letter < 'A' || letter > 'Z' {
			return false
		}
	}
	return true
}

// isValidTenantID reports whether tenantID is a safe identifier: 1-64
// lowercase ASCII letters, digits, or hyphens.
func isValidTenantID(tenantID string) bool {
	if len(tenantID) == 0 || len(tenantID) > 64 {
		return false
	}
	for _, char := range tenantID {
		if !isLowerAlphanumeric(char) && char != '-' {
			return false
		}
	}
	return true
}

// isValidInstrument reports whether instrument has the shape of a ticker
// symbol: 1-16 uppercase ASCII letters, digits, or dots.
func isValidInstrument(instrument string) bool {
	if len(instrument) == 0 || len(instrument) > 16 {
		return false
	}
	for _, char := range instrument {
		if !isUpperAlphanumeric(char) && char != '.' {
			return false
		}
	}
	return true
}

func isLowerAlphanumeric(char rune) bool {
	return (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')
}

func isUpperAlphanumeric(char rune) bool {
	return (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9')
}

// writeJSONError writes a JSON error body with the given HTTP status.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(errorResponse{Error: message}); err != nil {
		log.Printf("encode error response: %v", err)
	}
}
