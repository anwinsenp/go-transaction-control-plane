package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ingestionv1 "github.com/anwinsenp/go-transaction-control-plane/internal/api/pb/ingestion/v1"
	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

func validTransactionEventProto() *ingestionv1.TransactionEvent {
	return &ingestionv1.TransactionEvent{
		EventId:    "11111111-1111-1111-1111-111111111111",
		TenantId:   "tenant-a",
		Instrument: "AAPL",
		Side:       ingestionv1.Side_SIDE_BUY,
		Quantity:   1_000_000_000,
		Price:      15_025_000_000,
		Currency:   "USD",
		OccurredAt: timestamppb.New(mustParseRFC3339("2026-08-03T12:00:00Z")),
	}
}

func TestTransactionIngestionServer_IngestTransaction(t *testing.T) {
	withEvent := func(mutate func(*ingestionv1.TransactionEvent)) *ingestionv1.TransactionEvent {
		event := validTransactionEventProto()
		mutate(event)
		return event
	}

	tests := []struct {
		name              string
		event             *ingestionv1.TransactionEvent
		wantCode          codes.Code
		wantErrorContains string
	}{
		{
			name:     "valid event is accepted",
			event:    validTransactionEventProto(),
			wantCode: codes.OK,
		},
		{
			name:              "missing event_id",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.EventId = "" }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "missing required fields",
		},
		{
			name:              "invalid event_id",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.EventId = "not-a-uuid" }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "event_id must be a valid UUID",
		},
		{
			name:              "invalid tenant_id",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.TenantId = "Tenant_A" }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "tenant_id must be",
		},
		{
			name:              "invalid instrument",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Instrument = "aapl" }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "instrument must be",
		},
		{
			name:              "unspecified side",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Side = ingestionv1.Side_SIDE_UNSPECIFIED }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "side must be SIDE_BUY or SIDE_SELL",
		},
		{
			name:              "zero quantity",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Quantity = 0 }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "quantity must be",
		},
		{
			name:              "negative price",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Price = -1 }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "price must be",
		},
		{
			name:              "invalid currency",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Currency = "us" }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "currency must be",
		},
		{
			name:              "missing occurred_at",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.OccurredAt = nil }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "missing required fields",
		},
		{
			name:     "tenant_id minimum length is accepted",
			event:    withEvent(func(event *ingestionv1.TransactionEvent) { event.TenantId = "a" }),
			wantCode: codes.OK,
		},
		{
			name:     "tenant_id maximum length is accepted",
			event:    withEvent(func(event *ingestionv1.TransactionEvent) { event.TenantId = strings.Repeat("a", 64) }),
			wantCode: codes.OK,
		},
		{
			name:              "tenant_id one char over max length is rejected",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.TenantId = strings.Repeat("a", 65) }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "tenant_id must be",
		},
		{
			name:     "instrument minimum length is accepted",
			event:    withEvent(func(event *ingestionv1.TransactionEvent) { event.Instrument = "A" }),
			wantCode: codes.OK,
		},
		{
			name:     "instrument maximum length is accepted",
			event:    withEvent(func(event *ingestionv1.TransactionEvent) { event.Instrument = strings.Repeat("A", 16) }),
			wantCode: codes.OK,
		},
		{
			name:              "instrument one char over max length is rejected",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Instrument = strings.Repeat("A", 17) }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "instrument must be",
		},
		{
			name:     "instrument with a dot is accepted",
			event:    withEvent(func(event *ingestionv1.TransactionEvent) { event.Instrument = "BRK.B" }),
			wantCode: codes.OK,
		},
		{
			name:     "quantity at MaxAmount is accepted",
			event:    withEvent(func(event *ingestionv1.TransactionEvent) { event.Quantity = ledger.MaxAmount }),
			wantCode: codes.OK,
		},
		{
			name:     "price at MaxAmount is accepted",
			event:    withEvent(func(event *ingestionv1.TransactionEvent) { event.Price = ledger.MaxAmount }),
			wantCode: codes.OK,
		},
		{
			name:              "quantity over MaxAmount is rejected",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Quantity = ledger.MaxAmount + 1 }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "quantity must be",
		},
		{
			name:              "price over MaxAmount is rejected",
			event:             withEvent(func(event *ingestionv1.TransactionEvent) { event.Price = ledger.MaxAmount + 1 }),
			wantCode:          codes.InvalidArgument,
			wantErrorContains: "price must be",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			server := newTransactionIngestionServer(&fakePublisher{})

			response, err := server.IngestTransaction(context.Background(), &ingestionv1.IngestTransactionRequest{Event: testCase.event})

			if testCase.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("IngestTransaction() error = %v, want nil", err)
				}
				if !response.GetAccepted() {
					t.Errorf("Accepted = false, want true")
				}
				if response.GetEventId() != testCase.event.GetEventId() {
					t.Errorf("EventId = %q, want %q", response.GetEventId(), testCase.event.GetEventId())
				}
				return
			}

			if err == nil {
				t.Fatalf("IngestTransaction() error = nil, want code %v", testCase.wantCode)
			}
			statusErr, ok := status.FromError(err)
			if !ok {
				t.Fatalf("error = %v, want a gRPC status error", err)
			}
			if statusErr.Code() != testCase.wantCode {
				t.Errorf("code = %v, want %v", statusErr.Code(), testCase.wantCode)
			}
			if !strings.Contains(statusErr.Message(), testCase.wantErrorContains) {
				t.Errorf("message = %q, want substring %q", statusErr.Message(), testCase.wantErrorContains)
			}
		})
	}
}

func TestTransactionIngestionServer_NilEvent(t *testing.T) {
	server := newTransactionIngestionServer(&fakePublisher{})

	_, err := server.IngestTransaction(context.Background(), &ingestionv1.IngestTransactionRequest{})

	statusErr, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error = %v, want a gRPC status error", err)
	}
	if statusErr.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want %v", statusErr.Code(), codes.InvalidArgument)
	}
	if !strings.Contains(statusErr.Message(), "event is required") {
		t.Errorf("message = %q, want substring %q", statusErr.Message(), "event is required")
	}
}

// TestTransactionIngestionServer_PublishesUsingRequestContext ensures the
// gRPC handler forwards the RPC's own context to Publish rather than a
// detached one, mirroring TestTransactionHandlerPublishesUsingRequestContext
// for the REST path so the same guarantee holds across both transports.
func TestTransactionIngestionServer_PublishesUsingRequestContext(t *testing.T) {
	publisher := &contextCapturingPublisher{}
	server := newTransactionIngestionServer(publisher)

	ctx := context.WithValue(context.Background(), requestContextKey{}, "marker")
	request := &ingestionv1.IngestTransactionRequest{Event: validTransactionEventProto()}

	if _, err := server.IngestTransaction(ctx, request); err != nil {
		t.Fatalf("IngestTransaction() error = %v", err)
	}

	if publisher.capturedCtx == nil {
		t.Fatal("Publish() was called with a nil context")
	}
	if value := publisher.capturedCtx.Value(requestContextKey{}); value != "marker" {
		t.Errorf("Publish() context value = %v, want %q (handler must forward the RPC context, not context.Background())", value, "marker")
	}
}

// TestTransactionIngestionServer_AlreadyCanceledContextReachesPublisher
// verifies IngestTransaction does not short-circuit on an already-canceled
// context itself: it defers that decision to the publisher, which is the
// component that owns Kafka I/O and cancellation semantics.
func TestTransactionIngestionServer_AlreadyCanceledContextReachesPublisher(t *testing.T) {
	publisher := &contextCapturingPublisher{}
	server := newTransactionIngestionServer(publisher)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := &ingestionv1.IngestTransactionRequest{Event: validTransactionEventProto()}

	if _, err := server.IngestTransaction(ctx, request); err != nil {
		t.Fatalf("IngestTransaction() error = %v, want nil (fakePublisher ignores context state)", err)
	}

	if publisher.capturedCtx == nil {
		t.Fatal("Publish() was called with a nil context")
	}
	if err := publisher.capturedCtx.Err(); err == nil {
		t.Error("Publish() context Err() = nil, want context.Canceled to have propagated")
	}
}

func TestTransactionIngestionServer_PublishFailureReturnsUnavailable(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("kafka broker unreachable")}
	server := newTransactionIngestionServer(publisher)

	_, err := server.IngestTransaction(context.Background(), &ingestionv1.IngestTransactionRequest{Event: validTransactionEventProto()})

	statusErr, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error = %v, want a gRPC status error", err)
	}
	if statusErr.Code() != codes.Unavailable {
		t.Errorf("code = %v, want %v", statusErr.Code(), codes.Unavailable)
	}
}

func TestTransactionIngestionServer_PublishesConvertedEvent(t *testing.T) {
	publisher := &fakePublisher{}
	server := newTransactionIngestionServer(publisher)

	event := validTransactionEventProto()
	if _, err := server.IngestTransaction(context.Background(), &ingestionv1.IngestTransactionRequest{Event: event}); err != nil {
		t.Fatalf("IngestTransaction() error = %v", err)
	}

	if len(publisher.published) != 1 {
		t.Fatalf("published %d events, want 1", len(publisher.published))
	}

	published := publisher.published[0]
	if published.EventID.String() != event.GetEventId() {
		t.Errorf("EventID = %q, want %q", published.EventID.String(), event.GetEventId())
	}
	if published.TenantID != event.GetTenantId() {
		t.Errorf("TenantID = %q, want %q", published.TenantID, event.GetTenantId())
	}
	if published.Quantity != event.GetQuantity() {
		t.Errorf("Quantity = %d, want %d", published.Quantity, event.GetQuantity())
	}
	if published.Price != event.GetPrice() {
		t.Errorf("Price = %d, want %d", published.Price, event.GetPrice())
	}
}

// BenchmarkTransactionIngestionServer_IngestTransaction reports allocs/op
// for the gRPC handler's own decode-free validate -> convert -> publish
// path, calling IngestTransaction directly rather than over a network
// connection. This isolates the handler's allocation behavior from
// transport/codec overhead, unlike BenchmarkTransactionHandler which
// includes httptest scaffolding.
//
// Sub-benchmarks mirror BenchmarkTransactionHandler's variable-length field
// cases so the two hot paths can be compared directly.
func BenchmarkTransactionIngestionServer_IngestTransaction(b *testing.B) {
	tests := []struct {
		name  string
		event *ingestionv1.TransactionEvent
	}{
		{
			name:  "typical",
			event: validTransactionEventProto(),
		},
		{
			name: "min_length_fields",
			event: withMutationProto(validTransactionEventProto(), func(event *ingestionv1.TransactionEvent) {
				event.TenantId = "a"
				event.Instrument = "A"
			}),
		},
		{
			name: "max_length_fields",
			event: withMutationProto(validTransactionEventProto(), func(event *ingestionv1.TransactionEvent) {
				event.TenantId = strings.Repeat("a", 64)
				event.Instrument = strings.Repeat("A", 16)
			}),
		},
	}

	for _, testCase := range tests {
		b.Run(testCase.name, func(b *testing.B) {
			server := newTransactionIngestionServer(&fakePublisher{})
			request := &ingestionv1.IngestTransactionRequest{Event: testCase.event}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := server.IngestTransaction(context.Background(), request); err != nil {
					b.Fatalf("IngestTransaction() error = %v", err)
				}
			}
		})
	}
}

func withMutationProto(event *ingestionv1.TransactionEvent, mutate func(*ingestionv1.TransactionEvent)) *ingestionv1.TransactionEvent {
	mutate(event)
	return event
}

func mustParseRFC3339(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
