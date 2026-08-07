// Package processor implements idempotent P&L reconciliation: applying
// ledger transactions to per-tenant, per-instrument reconciled state under
// Kafka's at-least-once delivery, where redelivering the same event must
// produce the same end state as delivering it once. Per this repo's
// ports-and-adapters layout, this package depends only on the repository
// interfaces defined in internal/ledger, never on a storage or transport
// implementation directly.
package processor

import (
	"context"
	"errors"
	"fmt"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ledger"
)

// Reconciler applies ledger transactions to reconciled P&L state.
type Reconciler struct {
	Transactions ledger.TransactionRepository
	States       ledger.ReconciledStateRepository
}

// NewReconciler returns a Reconciler backed by transactions and states.
func NewReconciler(transactions ledger.TransactionRepository, states ledger.ReconciledStateRepository) *Reconciler {
	return &Reconciler{Transactions: transactions, States: states}
}

// Reconcile persists txn and folds it into its tenant/instrument's
// reconciled state, using weighted-average-cost P&L accounting. It is safe
// to call repeatedly with a transaction carrying the same EventID: a
// redelivery either no-ops (if reconciliation already completed for it) or
// resumes and completes the reconciled-state update (if a prior delivery
// inserted the transaction but crashed before updating state), rather than
// double-applying it.
//
// This relies on every transaction for a given (tenant, instrument) being
// delivered to a single Reconcile caller in order — guaranteed upstream by
// Kafka partitioning on tenant+instrument — so no additional locking is
// done here across concurrent callers for the same instrument.
func (rec *Reconciler) Reconcile(ctx context.Context, txn ledger.Transaction) error {
	inserted, err := rec.Transactions.Insert(ctx, txn)
	if err != nil {
		if !errors.Is(err, ledger.ErrDuplicateEvent) {
			return fmt.Errorf("reconcile: insert transaction: %w", err)
		}
		inserted, err = rec.Transactions.GetByEventID(ctx, txn.EventID)
		if err != nil {
			return fmt.Errorf("reconcile: get duplicate transaction: %w", err)
		}
	}

	state, err := rec.States.Get(ctx, inserted.TenantID, inserted.Instrument)
	if err != nil {
		if !errors.Is(err, ledger.ErrNotFound) {
			return fmt.Errorf("reconcile: get reconciled state: %w", err)
		}
		state = ledger.ReconciledState{TenantID: inserted.TenantID, Instrument: inserted.Instrument}
	}

	if state.LastTransactionID != nil && *state.LastTransactionID >= inserted.ID {
		return nil
	}

	updated, err := applyTransaction(state, inserted)
	if err != nil {
		return fmt.Errorf("reconcile: apply transaction %s: %w", inserted.EventID, err)
	}

	if err := rec.States.Upsert(ctx, updated); err != nil {
		if errors.Is(err, ledger.ErrStaleReconciledState) {
			// Lost the compare-and-swap to a concurrent apply that already
			// advanced the watermark past this transaction (e.g. a brief
			// overlap during a consumer-group rebalance) — the state
			// already reflects this transaction's effect one way or
			// another, so there's nothing left for this call to do.
			return nil
		}
		return fmt.Errorf("reconcile: upsert reconciled state: %w", err)
	}
	return nil
}

// applyTransaction folds txn into state under weighted-average-cost
// accounting and advances state's LastTransactionID watermark to txn.ID.
func applyTransaction(state ledger.ReconciledState, txn ledger.Transaction) (ledger.ReconciledState, error) {
	var err error
	switch txn.Side {
	case ledger.SideBuy:
		state, err = applyBuy(state, txn)
	case ledger.SideSell:
		state, err = applySell(state, txn)
	default:
		return ledger.ReconciledState{}, fmt.Errorf("unknown transaction side %q", txn.Side)
	}
	if err != nil {
		return ledger.ReconciledState{}, err
	}

	transactionID := txn.ID
	state.LastTransactionID = &transactionID
	return state, nil
}

// applyBuy increases position by txn.Quantity under weighted-average-cost
// accounting; see applyTrade for the full algorithm, including how it
// handles a buy that covers an existing short and flips the position long.
func applyBuy(state ledger.ReconciledState, txn ledger.Transaction) (ledger.ReconciledState, error) {
	return applyTrade(state, txn.Quantity, txn.Price)
}

// applySell decreases position by txn.Quantity under weighted-average-cost
// accounting; see applyTrade for the full algorithm, including how it
// handles a sell that closes an existing long and flips the position short.
func applySell(state ledger.ReconciledState, txn ledger.Transaction) (ledger.ReconciledState, error) {
	return applyTrade(state, -txn.Quantity, txn.Price)
}

// applyTrade folds a trade of signedQuantity (positive for a buy, negative
// for a sell) at price into state under weighted-average-cost accounting.
//
// When the trade extends the existing position (same sign, or no existing
// position), AverageCost becomes the weighted average of the existing cost
// basis and the new trade's cost:
// (position*averageCost + signedQuantity*price) / (position+signedQuantity).
//
// When the trade opposes the existing position, it first closes up to
// min(|signedQuantity|, |position|) of it, realizing PnL on the closed
// quantity at (price - averageCost) per unit for a long, or
// (averageCost - price) per unit for a short. If signedQuantity's
// magnitude exceeds the existing position, the remainder flips the
// position to the other side at a fresh cost basis of price — treating it
// as closing the old position and opening a new one in the same trade,
// rather than blending the old (now-irrelevant) cost basis into the new
// position's average.
func applyTrade(state ledger.ReconciledState, signedQuantity, price int64) (ledger.ReconciledState, error) {
	sameDirection := state.Position == 0 || (state.Position > 0) == (signedQuantity > 0)
	if sameDirection {
		existingCost, err := ledger.MulAmount(state.Position, state.AverageCost)
		if err != nil {
			return ledger.ReconciledState{}, fmt.Errorf("compute existing cost basis: %w", err)
		}
		addedCost, err := ledger.MulAmount(signedQuantity, price)
		if err != nil {
			return ledger.ReconciledState{}, fmt.Errorf("compute trade cost: %w", err)
		}

		newPosition := state.Position + signedQuantity
		averageCost, err := ledger.DivAmount(existingCost+addedCost, newPosition)
		if err != nil {
			return ledger.ReconciledState{}, fmt.Errorf("compute new average cost: %w", err)
		}
		state.AverageCost = averageCost
		state.Position = newPosition
		return state, nil
	}

	positionMagnitude := state.Position
	if positionMagnitude < 0 {
		positionMagnitude = -positionMagnitude
	}
	quantityMagnitude := signedQuantity
	if quantityMagnitude < 0 {
		quantityMagnitude = -quantityMagnitude
	}

	closingQuantity := quantityMagnitude
	if positionMagnitude < closingQuantity {
		closingQuantity = positionMagnitude
	}

	var pnlPerUnit int64
	if state.Position > 0 {
		pnlPerUnit = price - state.AverageCost
	} else {
		pnlPerUnit = state.AverageCost - price
	}
	realized, err := ledger.MulAmount(closingQuantity, pnlPerUnit)
	if err != nil {
		return ledger.ReconciledState{}, fmt.Errorf("compute realized pnl: %w", err)
	}
	state.RealizedPnL += realized

	flipQuantity := quantityMagnitude - closingQuantity
	state.Position += signedQuantity
	switch {
	case state.Position == 0:
		state.AverageCost = 0
	case flipQuantity > 0:
		state.AverageCost = price
	}
	return state, nil
}
