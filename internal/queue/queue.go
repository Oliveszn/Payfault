package queue

import (
	"context"
	"fmt"
	"payfault/internal/models"
	"time"

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
	_, err := q.pool.Exec(ctx, `
	INSERT INTO transactions (
	id, idempotency_key, amount, currency,
	sender_ref, recipient_code, status,
	attempts, max_attempts, next_retry_at
	) VALUES (
	 $1, $2. $3, $4,
	 $5, $6, $7,
	 0, $8, NOW()
	 )
	 `,
		txn.ID, txn.IdempotencyKey, txn.Amount, txn.Currency,
		txn.SenderRef, txn.RecipientCode, models.StatusPending,
		txn.MaxAttempts,
	)
	if err != nil {
		return fmt.Errorf("enqueue transaction %s: %w", txn.ID, err)
	}
	return nil
}

// this claims up to the limit of pending transactions that are due for processing
// FOR UPDATE - locks the row so no other worker can touch it
// SKIP LOCKED - if a row is lockd already, skip it instead of waiting
// this will allow up to 5 gorountines claim diff rows with no collison no duplicate processing
func (q *Queue) Dequeue(ctx context.Context, limit int) ([]*models.Transaction, error) {
	rows, err := q.pool.Query(ctx, `
	SELECT id, idempotency_key, amount, currency,
				sender_ref, recipient_code, status,
				attempts, max_attempts, next_retry_at,
				last_error, paystack_ref, created_at, updated_at
	FROM transactions
	WHERE status IN ('pending', 'processing')	
	AND next_retry_at <= NOW()
	AND attempts < max_attempts
	ORDER BY next_retry_at ASC
	LIMIT $1
	FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("dequeue query: %w", err)
	}
	defer rows.Close()

	var txns []*models.Transaction
	for rows.Next() {
		var t models.Transaction
		if err := rows.Scan(
			&t.ID, &t.IdempotencyKey, &t.Amount, &t.Currency,
			&t.SenderRef, &t.RecipientCode, &t.Status,
			&t.Attempts, &t.MaxAttempts, &t.NextRetryAt,
			&t.LastError, &t.PaystackRef, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan transaction: %w", err)
		}
		txns = append(txns, &t)
	}
	return txns, rows.Err()
}

// MarkProcessing sets status to processing and increments attempts, called when a worker claims a transaction
func (q *Queue) MarkProcessing(ctx context.Context, txnID string) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE transactions
		SET    status = 'processing', attempts = attempts + 1
		WHERE  id = $1
	`, txnID)
	return err
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
