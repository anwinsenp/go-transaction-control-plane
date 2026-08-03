package ingestionv1

import (
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestTransactionEventRoundTrip confirms TransactionEvent survives a
// proto.Marshal/Unmarshal wire round trip unchanged, across the enum values
// Side can take, boundary int64 magnitudes for the 1e8-scaled quantity/price
// fields, and both a populated and an unset occurred_at timestamp.
func TestTransactionEventRoundTrip(t *testing.T) {
	occurredAt := timestamppb.New(time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))

	tests := []struct {
		name  string
		event *TransactionEvent
	}{
		{
			name: "side unspecified",
			event: &TransactionEvent{
				EventId:    "11111111-1111-1111-1111-111111111111",
				TenantId:   "tenant-a",
				Instrument: "AAPL",
				Side:       Side_SIDE_UNSPECIFIED,
				Quantity:   1_000_000_000,
				Price:      15_025_000_000,
				Currency:   "USD",
				OccurredAt: occurredAt,
			},
		},
		{
			name: "side buy",
			event: &TransactionEvent{
				EventId:    "22222222-2222-2222-2222-222222222222",
				TenantId:   "tenant-a",
				Instrument: "AAPL",
				Side:       Side_SIDE_BUY,
				Quantity:   1_000_000_000,
				Price:      15_025_000_000,
				Currency:   "USD",
				OccurredAt: occurredAt,
			},
		},
		{
			name: "side sell",
			event: &TransactionEvent{
				EventId:    "33333333-3333-3333-3333-333333333333",
				TenantId:   "tenant-a",
				Instrument: "AAPL",
				Side:       Side_SIDE_SELL,
				Quantity:   1_000_000_000,
				Price:      15_025_000_000,
				Currency:   "USD",
				OccurredAt: occurredAt,
			},
		},
		{
			name: "zero-value occurred_at is unset",
			event: &TransactionEvent{
				EventId:    "44444444-4444-4444-4444-444444444444",
				TenantId:   "tenant-a",
				Instrument: "AAPL",
				Side:       Side_SIDE_BUY,
				Quantity:   1_000_000_000,
				Price:      15_025_000_000,
				Currency:   "USD",
				OccurredAt: nil,
			},
		},
		{
			name: "zero quantity and price",
			event: &TransactionEvent{
				EventId:    "55555555-5555-5555-5555-555555555555",
				TenantId:   "tenant-a",
				Instrument: "AAPL",
				Side:       Side_SIDE_BUY,
				Quantity:   0,
				Price:      0,
				Currency:   "USD",
				OccurredAt: occurredAt,
			},
		},
		{
			name: "max int64 quantity and price",
			event: &TransactionEvent{
				EventId:    "66666666-6666-6666-6666-666666666666",
				TenantId:   "tenant-a",
				Instrument: "AAPL",
				Side:       Side_SIDE_BUY,
				Quantity:   math.MaxInt64,
				Price:      math.MaxInt64,
				Currency:   "USD",
				OccurredAt: occurredAt,
			},
		},
		{
			name: "min int64 quantity and price",
			event: &TransactionEvent{
				EventId:    "77777777-7777-7777-7777-777777777777",
				TenantId:   "tenant-a",
				Instrument: "AAPL",
				Side:       Side_SIDE_SELL,
				Quantity:   math.MinInt64,
				Price:      math.MinInt64,
				Currency:   "USD",
				OccurredAt: occurredAt,
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			wireBytes, err := proto.Marshal(testCase.event)
			if err != nil {
				t.Fatalf("proto.Marshal() error = %v, want nil", err)
			}

			gotEvent := &TransactionEvent{}
			if err := proto.Unmarshal(wireBytes, gotEvent); err != nil {
				t.Fatalf("proto.Unmarshal() error = %v, want nil", err)
			}

			if !proto.Equal(gotEvent, testCase.event) {
				t.Errorf("round trip = %+v, want %+v", gotEvent, testCase.event)
			}

			if testCase.event.OccurredAt == nil && gotEvent.OccurredAt != nil {
				t.Errorf("OccurredAt = %v, want nil (unset message field must not round trip as a zero-valued Timestamp)", gotEvent.OccurredAt)
			}
		})
	}
}
