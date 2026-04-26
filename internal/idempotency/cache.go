package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Keyfor gets a deterministic idempotency key for a transaction ID
// using hash means the key is always the same for the same transaction, retries never generate a new key
func KeyFor(txnID string) string {
	h := sha256.Sum256([]byte("payfault" + txnID))
	return fmt.Sprintf("%x", h[:16]) //we retunr a 32char hex
}

// cache wraps the idempotency_cache table
type Cache struct {
	pool *pgxpool.Pool
}

func NewCache(pool *pgxpool.Pool) *Cache {
	return &Cache{pool: pool}
}

// Get returns the cached response for a key or nil if not found
// a nil response means continue with te original call
func (c *Cache) Get(ctx context.Context, key string) (json.RawMessage, error) {
	var body json.RawMessage
	err := c.pool.QueryRow(ctx, `
	SELECT response_body
	FROM idempotency_cache
	WHERE idempotency_KEY = $1
	AND expires_at > NOW()
	`, key).Scan(&body)

	// if err == pgx.ErrNoRows {
	// 	return nil, nil //its just a cache miss not an error
	// }
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("idempotency cache get: %w", err)
	}
	return body, nil
}

// Set stores a response in the cache
func (c *Cache) Set(ctx context.Context, key string, response json.RawMessage) error {
	_, err := c.pool.Exec(ctx, `
	INSERT INTO idempotency_cache (idempotency_key, response_body)
	VALUES ($1, $2)
	ON CONFLICT (idempotency_key) DO NOTHING
	`, key, response)
	if err != nil {
		return fmt.Errorf("idempotency cache set: %w", err)
	}
	return nil
}
