package queue

import (
	"context"
	"fmt"
	"log/slog"
	"payfault/internal/models"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// this wraps our transaction table and provides the enque and dequeue interface that sync engine will use
type Queue struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// Pool exposes the underlying connection pool for ad-hoc queries
// (used by GET /transaction/{id} handler).
func (q *Queue) Pool() *pgxpool.Pool {
	return q.pool
}

// this writes a new pending transaction to the DB before any network call is made, so as not to loase a payment intent
func (q *Queue) Enqueue(ctx context.Context, txn *models.Transaction) error {
	slog.Info("enqueue debug", "txn", txn)
	_, err := q.pool.Exec(ctx, `
INSERT INTO transactions (id, idempotency_key, amount, currency, sender_ref, recipient_code, status, attempts, max_attempts, next_retry_at) 
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
`,
		txn.ID,
		txn.IdempotencyKey,
		txn.Amount,
		txn.Currency,
		txn.SenderRef,
		txn.RecipientCode,
		models.StatusPending,
		0,
		txn.MaxAttempts,
		time.Now(),
	)
	if err != nil {
		return fmt.Errorf("enqueue transaction %s: %w", txn.ID, err)
	}
	return nil
}

// Dequeue claims up to the limit of pending transactions that are due for processing
// FOR UPDATE - locks the row so no other worker can touch it
// SKIP LOCKED - if a row is lockd already, skip it instead of waiting
// this will allow up to 5 gorountines claim diff rows with no collison no duplicate processing
func (q *Queue) Dequeue(ctx context.Context, limit int) ([]*models.Transaction, error) {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin dequeue tx: %w", err)
	}
	// Always rollback if we don't commit — harmless if already committed.
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, idempotency_key, amount, currency,
		       sender_ref, recipient_code, status,
		       attempts, max_attempts, next_retry_at,
		       last_error, paystack_ref, created_at, updated_at
		FROM   transactions
		WHERE  status = 'pending'
		  AND  next_retry_at <= NOW()
		  AND  attempts < max_attempts
		ORDER  BY next_retry_at ASC
		LIMIT  $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("dequeue query: %w", err)
	}

	var txns []*models.Transaction
	for rows.Next() {
		var t models.Transaction
		if err := rows.Scan(
			&t.ID, &t.IdempotencyKey, &t.Amount, &t.Currency,
			&t.SenderRef, &t.RecipientCode, &t.Status,
			&t.Attempts, &t.MaxAttempts, &t.NextRetryAt,
			&t.LastError, &t.PaystackRef, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan transaction: %w", err)
		}
		txns = append(txns, &t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(txns) == 0 {
		return nil, nil
	}

	//atomically flip all claimed rows to processing before releasing the lock ,
	//afther the UPDATE commits no other worker can claim the row since its no longer pending
	ids := make([]string, len(txns))
	for i, t := range txns {
		ids[i] = t.ID.String()
	}

	_, err = tx.Exec(ctx, `
		UPDATE transactions
		SET    status = 'processing', attempts = attempts + 1
		WHERE  id = ANY($1::uuid[])
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("claim transactions: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit dequeue tx: %w", err)
	}

	// reflect the incremented attempts in memory so engine logic is accurate
	for _, t := range txns {
		t.Attempts++
		t.Status = models.StatusProcessing
	}

	return txns, nil
}

// Markprocessing changed to do nothing, since claiming happens in Dequeue
func (q *Queue) MarkProcessing(ctx context.Context, txnID string) error {
	return nil
}

// MarkSuccess marks a transaction as successfully processed
func (q *Queue) MarkSuccess(ctx context.Context, txnID, paystackRef string) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE transactions
		SET    status = 'success', paystack_ref = $2, last_error = NULL
		WHERE  id = $1
	`, txnID, paystackRef)
	return err
}

// MarkRetry schedules a failed attempt for retry using exponential backoff.
func (q *Queue) MarkRetry(ctx context.Context, txnID string, attempts int, lastErr string) error {
	backoff := backoffDuration(attempts)
	_, err := q.pool.Exec(ctx, `
		UPDATE transactions
		SET    status        = 'pending',
		       last_error    = $2,
		       next_retry_at = NOW() + $3::interval
		WHERE  id = $1
	`, txnID, lastErr, backoff.String())
	return err
}

// MarkFailed permanently marks a transaction as failed (max attempts exceeded).
func (q *Queue) MarkFailed(ctx context.Context, txnID, lastErr string) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE transactions
		SET    status = 'failed', last_error = $2
		WHERE  id = $1
	`, txnID, lastErr)
	return err
}

// Backoff formula: wait = base * 2^attempt (capped at 30s) + 20% jitter
//
//	attempt 1 -  2s
//	attempt 2 -  4s
//	attempt 3 -  8s
//	attempt 4 -  16s
//	attempt 5 -  30s (capped)
//
// Jitter prevents a situation where all retries fire simultaneously.
func backoffDuration(attempt int) time.Duration {
	base := 2 * time.Second
	max := 30 * time.Second

	wait := base
	for i := 0; i < attempt; i++ {
		wait *= 2
	}
	if wait > max {
		wait = max
	}

	// Simple deterministic jitter: +20% based on attempt parity
	jitter := time.Duration(int64(wait) / 5)
	if attempt%2 == 0 {
		wait += jitter
	} else {
		wait -= jitter
	}
	return wait
}

// GetByID fetches a single transaction for status polling.
func (q *Queue) GetByID(ctx context.Context, id string) (*models.Transaction, error) {
	var t models.Transaction
	err := q.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, amount, currency,
		       sender_ref, recipient_code, status,
		       attempts, max_attempts, next_retry_at,
		       last_error, paystack_ref, created_at, updated_at
		FROM   transactions WHERE id = $1
	`, id).Scan(
		&t.ID, &t.IdempotencyKey, &t.Amount, &t.Currency,
		&t.SenderRef, &t.RecipientCode, &t.Status,
		&t.Attempts, &t.MaxAttempts, &t.NextRetryAt,
		&t.LastError, &t.PaystackRef, &t.CreatedAt, &t.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get transaction: %w", err)
	}
	return &t, nil
}
