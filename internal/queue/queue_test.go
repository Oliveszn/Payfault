package queue_test

// Integration tests meed a real Postgres database.
// Set DATABASE_URL in your environment before running:

import (
	"context"
	"sync"
	"testing"
	"time"

	"payfault/internal/models"
	"payfault/internal/queue"
	"payfault/internal/testhelper"

	"github.com/google/uuid"
)

// helpers

// makeTransaction builds a minimal valid transaction for tests.
// Using a helper avoids repeating the same struct literal in every test.
func makeTransaction() *models.Transaction {
	return &models.Transaction{
		ID:             uuid.New(),
		IdempotencyKey: uuid.New().String(), // unique per transaction
		Amount:         100000,
		Currency:       "NGN",
		SenderRef:      "user_test",
		RecipientCode:  "RCP_test",
		Status:         models.StatusPending,
		MaxAttempts:    5,
		NextRetryAt:    time.Now().Add(-time.Second), // due immediately
	}
}

// Test 2: Enqueue + Dequeue round-trip

// TestEnqueue_Dequeue_RoundTrip verifies that a transaction written by Enqueue
// is returned by Dequeue with all fields intact.
//
// This is the most fundamental test — if this fails, nothing else matters.
func TestEnqueue_Dequeue_RoundTrip(t *testing.T) {
	pool := testhelper.NewPool(t)
	q := queue.New(pool)
	ctx := context.Background()

	original := makeTransaction()

	// ACT: write it
	if err := q.Enqueue(ctx, original); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// ACT: read it back
	txns, err := q.Dequeue(ctx, 10)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// ASSERT: we got exactly one transaction back
	if len(txns) != 1 {
		t.Fatalf("Dequeue returned %d transactions, want 1", len(txns))
	}

	got := txns[0]

	// ASSERT: key fields match what we wrote
	if got.ID != original.ID {
		t.Errorf("ID: got %v, want %v", got.ID, original.ID)
	}
	if got.IdempotencyKey != original.IdempotencyKey {
		t.Errorf("IdempotencyKey: got %q, want %q", got.IdempotencyKey, original.IdempotencyKey)
	}
	if got.Amount != original.Amount {
		t.Errorf("Amount: got %d, want %d", got.Amount, original.Amount)
	}
	if got.RecipientCode != original.RecipientCode {
		t.Errorf("RecipientCode: got %q, want %q", got.RecipientCode, original.RecipientCode)
	}

	// ASSERT: Dequeue should have flipped status to processing
	if got.Status != models.StatusProcessing {
		t.Errorf("Status after Dequeue: got %q, want %q", got.Status, models.StatusProcessing)
	}

	// ASSERT: Dequeue increments attempts
	if got.Attempts != 1 {
		t.Errorf("Attempts after Dequeue: got %d, want 1", got.Attempts)
	}
}

// TestDequeue_RespectsNextRetryAt verifies that transactions not yet due
// are not returned by Dequeue.
//
// This is what prevents the retry loop from firing early — if this broke,
// every transaction would be retried at maximum speed.
func TestDequeue_RespectsNextRetryAt(t *testing.T) {
	pool := testhelper.NewPool(t)
	q := queue.New(pool)
	ctx := context.Background()

	txn := makeTransaction()
	txn.NextRetryAt = time.Now().Add(10 * time.Minute) // due in 10 minutes, not now

	if err := q.Enqueue(ctx, txn); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	txns, err := q.Dequeue(ctx, 10)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Should be empty — the transaction isn't due yet
	if len(txns) != 0 {
		t.Errorf("Dequeue returned %d transactions, want 0 (transaction not yet due)", len(txns))
	}
}

// TestDequeue_SkipsMaxedOutTransactions verifies that transactions which have
// hit max_attempts are not returned by Dequeue.
func TestDequeue_SkipsMaxedOutTransactions(t *testing.T) {
	pool := testhelper.NewPool(t)
	q := queue.New(pool)
	ctx := context.Background()

	txn := makeTransaction()

	if err := q.Enqueue(ctx, txn); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Manually set attempts = max_attempts to simulate an exhausted transaction
	_, err := pool.Exec(ctx, `
		UPDATE transactions SET attempts = max_attempts WHERE id = $1
	`, txn.ID)
	if err != nil {
		t.Fatalf("setup: set attempts to max: %v", err)
	}

	txns, err := q.Dequeue(ctx, 10)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if len(txns) != 0 {
		t.Errorf("Dequeue returned %d transactions, want 0 (attempts exhausted)", len(txns))
	}
}

//  Test 3: Concurrency the FOR UPDATE SKIP LOCKED guarantee

// TestDequeue_NoDuplicatesUnderConcurrency is the most important test in this file.
//
// It simulates what happens in production: 10 goroutines all call Dequeue at
// the same moment, racing to claim the same pool of transactions.
//
// The invariant: every transaction must be claimed by exactly one goroutine.
// If any transaction appears in two goroutines' results, we have a bug that
// could cause duplicate charges.
//
// This test caught the exact bug from your earlier logs (all 5 workers claiming
// the same transaction) — FOR UPDATE SKIP LOCKED must run inside an explicit
// transaction, not in autocommit mode.
func TestDequeue_NoDuplicatesUnderConcurrency(t *testing.T) {
	pool := testhelper.NewPool(t)
	q := queue.New(pool)
	ctx := context.Background()

	// Write 5 transactions — fewer than the number of goroutines,
	// so goroutines must genuinely compete for a limited number of rows.
	const numTransactions = 5
	const numWorkers = 10

	for i := 0; i < numTransactions; i++ {
		if err := q.Enqueue(ctx, makeTransaction()); err != nil {
			t.Fatalf("Enqueue %d failed: %v", i, err)
		}
	}

	// Launch numWorkers goroutines simultaneously, all calling Dequeue.
	// We use a WaitGroup to wait for all of them to finish.
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex                // protects claimed map
		claimed = make(map[uuid.UUID]int) // txn ID → how many times it was claimed
	)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			txns, err := q.Dequeue(ctx, 5)
			if err != nil {
				// Don't call t.Fatal from a goroutine — it panics.
				// t.Errorf is safe; the test will be marked failed.
				t.Errorf("worker %d: Dequeue error: %v", workerID, err)
				return
			}

			mu.Lock()
			for _, txn := range txns {
				claimed[txn.ID]++
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// ASSERT: every transaction was claimed exactly once.
	// claimed[id] > 1 means two workers got the same row — that's the bug.
	// claimed[id] == 0 means a transaction was never picked up — also wrong.
	for id, count := range claimed {
		if count != 1 {
			t.Errorf("transaction %v was claimed %d times (want exactly 1)", id, count)
		}
	}

	// ASSERT: all 5 transactions were claimed by someone
	if len(claimed) != numTransactions {
		t.Errorf("total claimed transactions = %d, want %d", len(claimed), numTransactions)
	}
}

// Mark* lifecycle tests

// TestMarkSuccess verifies the success path updates status and stores the ref.
func TestMarkSuccess(t *testing.T) {
	pool := testhelper.NewPool(t)
	q := queue.New(pool)
	ctx := context.Background()

	txn := makeTransaction()
	if err := q.Enqueue(ctx, txn); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	q.Dequeue(ctx, 1) // claim it first

	paystackRef := "mock_ref_12345"
	if err := q.MarkSuccess(ctx, txn.ID.String(), paystackRef); err != nil {
		t.Fatalf("MarkSuccess: %v", err)
	}

	result, err := q.GetByID(ctx, txn.ID.String())
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if result.Status != models.StatusSuccess {
		t.Errorf("status = %q, want %q", result.Status, models.StatusSuccess)
	}
	if result.PaystackRef == nil || *result.PaystackRef != paystackRef {
		t.Errorf("paystack_ref = %v, want %q", result.PaystackRef, paystackRef)
	}
}

// TestMarkFailed verifies the permanent failure path.
func TestMarkFailed(t *testing.T) {
	pool := testhelper.NewPool(t)
	q := queue.New(pool)
	ctx := context.Background()

	txn := makeTransaction()
	if err := q.Enqueue(ctx, txn); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	q.Dequeue(ctx, 1)

	errMsg := "mock permanent error 400: invalid recipient"
	if err := q.MarkFailed(ctx, txn.ID.String(), errMsg); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	result, err := q.GetByID(ctx, txn.ID.String())
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if result.Status != models.StatusFailed {
		t.Errorf("status = %q, want %q", result.Status, models.StatusFailed)
	}
	if result.LastError == nil || *result.LastError != errMsg {
		t.Errorf("last_error = %v, want %q", result.LastError, errMsg)
	}
}

// TestMarkRetry verifies that a retried transaction goes back to pending
// with a future next_retry_at meaning it won't be picked up immediately.
func TestMarkRetry(t *testing.T) {
	pool := testhelper.NewPool(t)
	q := queue.New(pool)
	ctx := context.Background()

	txn := makeTransaction()
	if err := q.Enqueue(ctx, txn); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	q.Dequeue(ctx, 1)

	before := time.Now()
	if err := q.MarkRetry(ctx, txn.ID.String(), 1, "mock network error: timeout"); err != nil {
		t.Fatalf("MarkRetry: %v", err)
	}

	result, err := q.GetByID(ctx, txn.ID.String())
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if result.Status != models.StatusPending {
		t.Errorf("status = %q, want %q", result.Status, models.StatusPending)
	}

	// next_retry_at should be in the future — the transaction is not immediately due
	if !result.NextRetryAt.After(before) {
		t.Errorf("next_retry_at %v is not after %v — transaction would retry immediately", result.NextRetryAt, before)
	}
}
