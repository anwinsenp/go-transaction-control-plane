package api

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ingestionv1 "github.com/anwinsenp/go-transaction-control-plane/internal/api/pb/ingestion/v1"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// transactionIngestionServer implements ingestionv1.TransactionIngestionServiceServer,
// validating and publishing transaction events submitted over gRPC.
type transactionIngestionServer struct {
	ingestionv1.UnimplementedTransactionIngestionServiceServer

	publisher ingestion.Publisher
}

// newTransactionIngestionServer returns a transactionIngestionServer that
// publishes validated events via publisher.
func newTransactionIngestionServer(publisher ingestion.Publisher) *transactionIngestionServer {
	return &transactionIngestionServer{publisher: publisher}
}

// IngestTransaction validates req's event, publishes it to Kafka via the
// configured publisher, and acknowledges receipt only once the publish has
// durably succeeded.
func (server *transactionIngestionServer) IngestTransaction(ctx context.Context, req *ingestionv1.IngestTransactionRequest) (*ingestionv1.IngestTransactionResponse, error) {
	event := req.GetEvent()
	if event == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}

	if err := validateTransactionEvent(event); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ingestionEvent, err := transactionEventToIngestionEvent(event)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := server.publisher.Publish(ctx, ingestionEvent); err != nil {
		log.Printf("publish transaction event %s: %v", event.GetEventId(), err)
		return nil, status.Error(codes.Unavailable, "failed to publish transaction event")
	}

	return &ingestionv1.IngestTransactionResponse{
		EventId:  event.GetEventId(),
		Accepted: true,
	}, nil
}

// validateTransactionEvent reports the first invalid or missing field in
// event, or nil if the event is well-formed and safe to accept. It mirrors
// TransactionEventRequest.validate but operates on the generated proto
// message's typed fields directly, rather than a JSON string DTO.
func validateTransactionEvent(event *ingestionv1.TransactionEvent) error {
	if event.GetEventId() == "" || event.GetTenantId() == "" || event.GetInstrument() == "" ||
		event.GetCurrency() == "" || event.GetOccurredAt() == nil {
		return fmt.Errorf("missing required fields")
	}

	if _, err := uuid.Parse(event.GetEventId()); err != nil {
		return fmt.Errorf("event_id must be a valid UUID")
	}

	if !isValidTenantID(event.GetTenantId()) {
		return fmt.Errorf("tenant_id must be 1-64 lowercase letters, digits, or hyphens")
	}

	if !isValidInstrument(event.GetInstrument()) {
		return fmt.Errorf("instrument must be 1-16 uppercase letters, digits, or dots")
	}

	if _, err := sideFromProto(event.GetSide()); err != nil {
		return err
	}

	if event.GetQuantity() <= 0 || event.GetQuantity() > ledger.MaxAmount {
		return fmt.Errorf("quantity must be a positive decimal number no greater than %s", ledger.FormatAmount(ledger.MaxAmount))
	}

	if event.GetPrice() <= 0 || event.GetPrice() > ledger.MaxAmount {
		return fmt.Errorf("price must be a positive decimal number no greater than %s", ledger.FormatAmount(ledger.MaxAmount))
	}

	if !isValidCurrencyCode(event.GetCurrency()) {
		return fmt.Errorf("currency must be a 3-letter uppercase ISO 4217 code")
	}

	if err := event.GetOccurredAt().CheckValid(); err != nil {
		return fmt.Errorf("occurred_at must be a valid timestamp: %w", err)
	}

	return nil
}

// transactionEventToIngestionEvent converts an already-validated proto event
// into the transport-agnostic ingestion.Event published to Kafka. It must
// only be called after validateTransactionEvent has returned nil: it
// re-derives the same fields validateTransactionEvent already checked, so a
// failure here would indicate the two have drifted out of sync with each
// other, not a bad request.
func transactionEventToIngestionEvent(event *ingestionv1.TransactionEvent) (ingestion.Event, error) {
	eventID, err := uuid.Parse(event.GetEventId())
	if err != nil {
		return ingestion.Event{}, fmt.Errorf("event_id must be a valid UUID: %w", err)
	}

	side, err := sideFromProto(event.GetSide())
	if err != nil {
		return ingestion.Event{}, err
	}

	return ingestion.Event{
		EventID:       eventID,
		TenantID:      event.GetTenantId(),
		SchemaVersion: ingestion.CurrentSchemaVersion,
		Instrument:    event.GetInstrument(),
		Side:          side,
		Quantity:      event.GetQuantity(),
		Price:         event.GetPrice(),
		Currency:      event.GetCurrency(),
		OccurredAt:    event.GetOccurredAt().AsTime(),
	}, nil
}

// sideFromProto maps a proto Side to its ledger.Side equivalent, rejecting
// SIDE_UNSPECIFIED and any unrecognized value.
func sideFromProto(side ingestionv1.Side) (ledger.Side, error) {
	switch side {
	case ingestionv1.Side_SIDE_BUY:
		return ledger.SideBuy, nil
	case ingestionv1.Side_SIDE_SELL:
		return ledger.SideSell, nil
	default:
		return "", fmt.Errorf("side must be SIDE_BUY or SIDE_SELL")
	}
}
