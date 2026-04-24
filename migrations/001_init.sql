BEGIN;

-- The source of truth for every payment attempt.
CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL UNIQUE,
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL DEFAULT 'NGN' CHECK (currency IN ('NGN','USD','EUR')),
    sender_ref TEXT NOT NULL,
    recipient_code TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','success','failed')),
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error       TEXT,
    paystack_ref     TEXT UNIQUE,                    -- Paystack's reference, filled on success
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "give me pending rows that are due for processing, oldest first"
CREATE INDEX IF NOT EXISTS idx_transactions_sync
    ON transactions (status, next_retry_at)
    WHERE status IN ('pending', 'processing');

-- Server-side cache: if we've seen this key before, return the stored response without calling Paystack again.
CREATE TABLE IF NOT EXISTS idempotency_cache (
    idempotency_key  TEXT PRIMARY KEY,
    response_body    JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- auto-expire cache entries after 24h
    expires_at       TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours'
);

-- updated_at trigger
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE TRIGGER trg_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
 
COMMIT;