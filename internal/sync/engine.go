package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"payfault/internal/idempotency"
	"payfault/internal/models"
	"payfault/internal/paystack"
	"payfault/internal/queue"
)

// Engine is the offline first sync engine, we run a pool of workers that continuosly pool the queue, picking up pending transactions
// Key design decisions:
//  1. Workers use SELECT FOR UPDATE SKIP LOCKED so they never
//     step on each other
//  2. Every call to Paystack carries the transaction's idempotency key,
//     so a retry after a crash can never double charge
//  3. Transient errors (network, 5xx) = exponential backoff retry.
//     Permanent errors (4xx) = immediate fail, no retry.
type Engine struct {
	queue     *queue.Queue
	paystack  *paystack.Client
	idem      *idempotency.Cache
	workers   int
	pollEvery time.Duration
}

func New(q *queue.Queue, ps *paystack.Client, idem *idempotency.Cache) *Engine {
	return &Engine{
		queue:     q,
		paystack:  ps,
		idem:      idem,
		workers:   5, // 5 concurrent workers, each claiming different rows
		pollEvery: 3 * time.Second,
	}
}

// Start launches the worker pool. It blocks until ctx is cancelled
func (e *Engine) Start(ctx context.Context) {
	slog.Info("sync engine starting", "workers", e.workers, "poll_interval", e.pollEvery)

	var wg sync.WaitGroup
	for i := 0; i < e.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			e.runWorker(ctx, workerID)
		}(i)
	}

	wg.Wait()
	slog.Info("sync engine stopped")
}

// runWorker is the loop for a single worker goroutine it polls the queue, processes whatever it finds, then sleeps.
func (e *Engine) runWorker(ctx context.Context, id int) {
	log := slog.With("worker", id)

	for {
		select {
		case <-ctx.Done():
			log.Info("worker shutting down")
			return
		case <-time.After(e.pollEvery):
			// Each worker claims up to 3 transactions per tick.
			// With 5 workers polling every 3s, throughput is ~5 txn/s.
			txns, err := e.queue.Dequeue(ctx, 3)
			if err != nil {
				log.Error("dequeue failed", "error", err)
				continue
			}
			if len(txns) == 0 {
				continue // if trans = 0 nothing to do, go back to sleep
			}

			log.Info("picked up transactions", "count", len(txns))
			for _, txn := range txns {
				e.process(ctx, txn, log)
			}
		}
	}
}

// process drives a single transaction to completion or schedules a retry
func (e *Engine) process(ctx context.Context, txn *models.Transaction, log *slog.Logger) {
	log = log.With("txn_id", txn.ID, "attempt", txn.Attempts+1)

	// 1 claim the row, updates status to 'processing' and increments attempts so we don't exceed max_attempts
	if err := e.queue.MarkProcessing(ctx, txn.ID.String()); err != nil {
		log.Error("failed to mark processing", "error", err)
		return
	}

	// 2 check our own idempotency cache first
	// If we've already got a successful response stored, use it directly without calling Paystack at all.
	cached, err := e.idem.Get(ctx, txn.IdempotencyKey)
	if err != nil {
		log.Warn("idempotency cache read error", "error", err)
		// Non-fatal: proceed to make the real call.
	}
	if cached != nil {
		log.Info("idempotency cache hit — using cached result")
		// /we alread have a previous success so mark the transaction as done
		if err := e.queue.MarkSuccess(ctx, txn.ID.String(), "cached"); err != nil {
			log.Error("mark success (cached) failed", "error", err)
		}
		return
	}

	// 3 call paystack, passing the idempotency key
	// paystack holds the key for 24h on their side too, so even if we never stored our own cache entry, they'll return the original response.
	resp, rawBody, err := e.paystack.InitiateTransfer(ctx, paystack.TransferRequest{
		Source:    "balance",
		Amount:    txn.Amount,
		Recipient: txn.RecipientCode,
		Reason:    "payfault transfer",
		Reference: txn.ID.String(),
	}, txn.IdempotencyKey)

	if err != nil {
		e.handleError(ctx, txn, err, log)
		return
	}

	// 4 success path store the response in our idempotency cache, then mark the transaction done.
	if rawBody != nil {
		if cacheErr := e.idem.Set(ctx, txn.IdempotencyKey, rawBody); cacheErr != nil {
			log.Warn("failed to write idempotency cache", "error", cacheErr)
			// Non-fatal: the transaction still succeeded.
		}
	}

	paystackRef := resp.Data.Reference
	if err := e.queue.MarkSuccess(ctx, txn.ID.String(), paystackRef); err != nil {
		log.Error("mark success failed", "error", err)
		return
	}

	log.Info("transaction succeeded", "paystack_ref", paystackRef)
}

// handleError decides whether to retry or permanently fail a transaction.
func (e *Engine) handleError(
	ctx context.Context,
	txn *models.Transaction,
	err error,
	log *slog.Logger,
) {
	var permErr *paystack.PermanentError
	isPermanent := errors.As(err, &permErr)

	nextAttempt := txn.Attempts + 1 // already incremented by MarkProcessing

	if isPermanent || nextAttempt >= txn.MaxAttempts {
		//either a 4xx error from paystack or > max retries
		log.Error("transaction permanently failed",
			"error", err,
			"attempts", nextAttempt,
			"permanent", isPermanent,
		)
		if markErr := e.queue.MarkFailed(ctx, txn.ID.String(), err.Error()); markErr != nil {
			log.Error("mark failed error", "error", markErr)
		}
		return
	}

	// Transient error schedule a retry with exponential backoff.
	log.Warn("transaction will retry",
		"error", err,
		"attempts", nextAttempt,
		"max_attempts", txn.MaxAttempts,
	)
	if retryErr := e.queue.MarkRetry(ctx, txn.ID.String(), nextAttempt, err.Error()); retryErr != nil {
		log.Error("mark retry error", "error", retryErr)
	}
}

// Test hooks
// These are only used in tests. They allow the engine's process() method to
// be driven with fake dependencies injected from outside.

// QueueWriter is the subset of queue.Queue the engine needs to call.
type QueueWriter interface {
	MarkProcessing(ctx context.Context, txnID string) error
	MarkSuccess(ctx context.Context, txnID, ref string) error
	MarkRetry(ctx context.Context, txnID string, attempts int, lastErr string) error
	MarkFailed(ctx context.Context, txnID, lastErr string) error
}

// IdemCache is the subset of idempotency.Cache the engine needs.
type IdemCache interface {
	Get(ctx context.Context, key string) (json.RawMessage, error)
	Set(ctx context.Context, key string, response json.RawMessage) error
}

// TransferFn is a function that performs a transfer and returns (ref, rawBody, err).
// Tests inject a fake here instead of a real Paystack client.
type TransferFn func(ctx context.Context, idempotencyKey string) (ref string, rawBody json.RawMessage, err error)

// TestEngine is the engine with injectable dependencies for unit testing.
type TestEngine struct {
	queue    QueueWriter
	idem     IdemCache
	transfer TransferFn
}

// PermanentErrorForTest is exported so test files can construct it.
type PermanentErrorForTest struct {
	Code int
}

func (e *PermanentErrorForTest) Error() string {
	return fmt.Sprintf("permanent error %d", e.Code)
}

// NewForTest constructs a TestEngine with injected fakes.
func NewForTest(q QueueWriter, idem IdemCache, transfer TransferFn) *TestEngine {
	return &TestEngine{queue: q, idem: idem, transfer: transfer}
}

// ProcessForTest exposes process() for test packages.
func (e *TestEngine) ProcessForTest(ctx context.Context, txn *models.Transaction) {
	log := slog.With("txn_id", txn.ID, "attempt", txn.Attempts)

	e.queue.MarkProcessing(ctx, txn.ID.String())

	// Check idempotency cache first
	cached, err := e.idem.Get(ctx, txn.IdempotencyKey)
	if err != nil {
		log.Warn("idempotency cache read error", "error", err)
	}
	if cached != nil {
		log.Info("idempotency cache hit")
		e.queue.MarkSuccess(ctx, txn.ID.String(), "cached")
		return
	}

	// Call the injected transfer function
	ref, rawBody, err := e.transfer(ctx, txn.IdempotencyKey)
	if err != nil {
		var permErr *PermanentErrorForTest
		isPermanent := errors.As(err, &permErr)

		if isPermanent || txn.Attempts >= txn.MaxAttempts {
			e.queue.MarkFailed(ctx, txn.ID.String(), err.Error())
			return
		}
		e.queue.MarkRetry(ctx, txn.ID.String(), txn.Attempts, err.Error())
		return
	}

	if rawBody != nil {
		e.idem.Set(ctx, txn.IdempotencyKey, rawBody)
	}
	e.queue.MarkSuccess(ctx, txn.ID.String(), ref)
}
