// this file is only compiled during tests
// it gives every package a real postgres connection and a clean schema
// Usage in a test file:
//
//	pool := testhelper.NewPool(t)   // connects, migrates, auto-cleans up
//	q := queue.New(pool)
package testhelper

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates a real Postgres connection pool for tests.
//
// It reads DATABASE_URL from the environment this is set  to a test-only DB, not your production one.
//
// t.Cleanup registers pool.Close() so the connection is released when the test finishes you never need to close it manually.
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper() // marks this as a helper so failures point to the caller, not here

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// Skip rather than fail if the env var isn't set, the test is skipped rather than erroring out.
		t.Skip("DATABASE_URL not set — skipping integration test")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect to test DB: %v", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping test DB: %v", err)
	}

	// Wipe and recreate the tables before each test so tests are independent.
	// Order matters: drop child tables before parents (foreign key constraints).
	_, err = pool.Exec(context.Background(), `
		DROP TABLE IF EXISTS idempotency_cache;
		DROP TABLE IF EXISTS transactions;

		CREATE TABLE transactions (
			id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			idempotency_key  TEXT NOT NULL UNIQUE,
			amount           BIGINT NOT NULL,
			currency         TEXT NOT NULL DEFAULT 'NGN',
			sender_ref       TEXT NOT NULL,
			recipient_code   TEXT NOT NULL,
			status           TEXT NOT NULL DEFAULT 'pending'
			                     CHECK (status IN ('pending','processing','success','failed')),
			attempts         INT NOT NULL DEFAULT 0,
			max_attempts     INT NOT NULL DEFAULT 5,
			next_retry_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_error       TEXT,
			paystack_ref     TEXT UNIQUE,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		CREATE TABLE idempotency_cache (
			idempotency_key  TEXT PRIMARY KEY,
			response_body    JSONB NOT NULL,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at       TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours'
		);
	`)
	if err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}

	// t.Cleanup runs after the test finishes (pass or fail).
	// This is Go's equivalent of teardown / defer in test frameworks.
	t.Cleanup(func() {
		pool.Exec(context.Background(), `
			DROP TABLE IF EXISTS idempotency_cache;
			DROP TABLE IF EXISTS transactions;
		`)
		pool.Close()
	})

	return pool
}
