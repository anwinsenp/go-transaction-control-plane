// Package ingestion defines the domain model for a validated transaction
// event and the Publisher port that ships it downstream. Per this repo's
// ports-and-adapters layout, this package has no import dependency on any
// transport or messaging library — internal/ingestion/kafka provides the
// concrete Kafka-backed implementation of Publisher.
package ingestion

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// CurrentSchemaVersion is the Kafka payload schema version stamped on every
// Event published by this build of the ingestion service. Full
// version-negotiation between ingestion and the processor is tracked
// separately; for now every event carries this fixed value.
const CurrentSchemaVersion int16 = 1

// Event is a validated transaction event ready to be published to Kafka. It
// is transport-agnostic: internal/api builds one from an already-validated
// request DTO, and internal/ingestion/kafka serializes it onto the wire.
type Event struct {
	EventID       uuid.UUID
	TenantID      string
	SchemaVersion int16
	Instrument    string
	Side          ledger.Side
	Quantity      int64 // fixed-point, scaled by ledger.AmountScale
	Price         int64 // fixed-point, scaled by ledger.AmountScale
	Currency      string
	OccurredAt    time.Time
}

// Publisher ships a validated Event downstream. Implementations must
// distinguish publish failures from success — callers rely on a non-nil
// error to reject the originating request rather than acknowledging an
// event that never reached Kafka.
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}
