package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// maxTransactionEventBytes caps the request body size for a single
// transaction event, which is expected to be a small, fixed-shape JSON
// object.
const maxTransactionEventBytes = 4 << 10

// transactionBodyPool reuses request-body buffers on the ingestion hot
// path instead of letting json.Decoder allocate a fresh scan buffer per
// request.
var transactionBodyPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, maxTransactionEventBytes))
	},
}

// transactionReaderPool reuses the *bytes.Reader wrapped around the body
// buffer so decoding a request doesn't allocate a fresh reader per call.
var transactionReaderPool = sync.Pool{
	New: func() any {
		return bytes.NewReader(nil)
	},
}

// transactionEventPool reuses TransactionEventRequest values across
// requests so decoding doesn't allocate the struct itself on the hot path.
var transactionEventPool = sync.Pool{
	New: func() any {
		return new(TransactionEventRequest)
	},
}

// jsonResponseCodec pairs a reusable buffer with the json.Encoder bound to
// it, so the encoder (and its boxing of the underlying writer) isn't
// rebuilt on every response.
type jsonResponseCodec struct {
	buf *bytes.Buffer
	enc *json.Encoder
}

var jsonResponsePool = sync.Pool{
	New: func() any {
		buf := bytes.NewBuffer(make([]byte, 0, maxTransactionEventBytes))
		return &jsonResponseCodec{buf: buf, enc: json.NewEncoder(buf)}
	},
}

// writeJSON encodes value into a pooled buffer and writes it as a JSON
// response with the given status code.
func writeJSON(w http.ResponseWriter, status int, value any) {
	codec := jsonResponsePool.Get().(*jsonResponseCodec)
	defer func() {
		codec.buf.Reset()
		jsonResponsePool.Put(codec)
	}()

	if err := codec.enc.Encode(value); err != nil {
		log.Printf("encode json response: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(codec.buf.Bytes()); err != nil {
		log.Printf("write json response: %v", err)
	}
}

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

// Reset clears event back to its zero value so it can be safely reused
// from transactionEventPool for the next request.
func (event *TransactionEventRequest) Reset() {
	*event = TransactionEventRequest{}
}

// errorResponse is the wire format for a client-facing 4xx error.
type errorResponse struct {
	Error string `json:"error"`
}

// newTransactionHandler returns an http.HandlerFunc that accepts a mock
// transaction event over REST, validates it, publishes it to Kafka via
// publisher, and acknowledges receipt only once the publish has durably
// succeeded.
func newTransactionHandler(publisher ingestion.Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxTransactionEventBytes)

		body := transactionBodyPool.Get().(*bytes.Buffer)
		defer func() {
			body.Reset()
			transactionBodyPool.Put(body)
		}()

		if _, err := body.ReadFrom(r.Body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		reader := transactionReaderPool.Get().(*bytes.Reader)
		reader.Reset(body.Bytes())
		defer transactionReaderPool.Put(reader)

		event := transactionEventPool.Get().(*TransactionEventRequest)
		defer func() {
			event.Reset()
			transactionEventPool.Put(event)
		}()

		decoder := json.NewDecoder(reader)
		if err := decoder.Decode(event); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if trailing := body.Bytes()[decoder.InputOffset():]; !isJSONWhitespace(trailing) {
			writeJSONError(w, http.StatusBadRequest, "invalid request body: trailing data")
			return
		}

		if err := event.validate(); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		ingestionEvent, err := event.toIngestionEvent()
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := publisher.Publish(r.Context(), ingestionEvent); err != nil {
			log.Printf("%v", fmt.Errorf("publish transaction event %s: %w", event.EventID, err))
			writeJSONError(w, http.StatusServiceUnavailable, "failed to publish transaction event")
			return
		}

		writeJSON(w, http.StatusAccepted, event)
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

// toIngestionEvent converts an already-validated event into the
// transport-agnostic ingestion.Event published to Kafka. It must only be
// called after validate() has returned nil: it re-parses the same fields
// validate() already checked, so a parse failure here would indicate
// validate() and toIngestionEvent have drifted out of sync with each
// other, not a bad request.
func (event TransactionEventRequest) toIngestionEvent() (ingestion.Event, error) {
	eventID, err := uuid.Parse(event.EventID)
	if err != nil {
		return ingestion.Event{}, fmt.Errorf("event_id must be a valid UUID: %w", err)
	}

	quantity, err := decimal.NewFromString(event.Quantity)
	if err != nil {
		return ingestion.Event{}, fmt.Errorf("quantity must be a valid decimal number: %w", err)
	}

	price, err := decimal.NewFromString(event.Price)
	if err != nil {
		return ingestion.Event{}, fmt.Errorf("price must be a valid decimal number: %w", err)
	}

	occurredAt, err := time.Parse(time.RFC3339, event.OccurredAt)
	if err != nil {
		return ingestion.Event{}, fmt.Errorf("occurred_at must be an RFC3339 timestamp: %w", err)
	}

	return ingestion.Event{
		EventID:       eventID,
		TenantID:      event.TenantID,
		SchemaVersion: ingestion.CurrentSchemaVersion,
		Instrument:    event.Instrument,
		Side:          ledger.Side(event.Side),
		Quantity:      quantity,
		Price:         price,
		Currency:      event.Currency,
		OccurredAt:    occurredAt,
	}, nil
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

// isJSONWhitespace reports whether data contains only JSON whitespace
// characters (RFC 8259 §2), i.e. it is safe to ignore as padding after a
// decoded value rather than rejecting it as trailing garbage.
func isJSONWhitespace(data []byte) bool {
	for _, char := range data {
		switch char {
		case ' ', '\t', '\r', '\n':
		default:
			return false
		}
	}
	return true
}

// writeJSONError writes a JSON error body with the given HTTP status.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
