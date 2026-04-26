# payfault

An offline-first payment service in Go, built to understand resilient payment systems.
Integrates with Paystack. Handles unreliable networks, crashes, and retries correctly.

## Core concepts implemented

**Idempotency** — Every transaction gets an idempotency key derived from its UUID.
The key is sent to Paystack on every attempt. Paystack returns the original response
if they've seen the key before no double charges, ever.
We also maintain our own `idempotency_cache` table so we can detect duplicates
before even reaching Paystack.

**Offline-first queue** — Payment intents are written to the DB before any network call.
`POST /pay` returns `202 Accepted` immediately. A background sync engine drives
transactions to completion. Restart the process mid-payment — it picks up where it left off.

**Concurrency control** — The sync engine runs 5 worker goroutines, all polling the
same DB table. `SELECT FOR UPDATE SKIP LOCKED` ensures each worker claims a distinct
set of rows. No mutexes, no external coordination, the DB does the work.

**Exponential backoff** — Transient failures (network errors, Paystack 5xx) are retried
with increasing wait times: ~2s, ~4s, ~8s, ~16s, 30s. Jitter prevents thundering herds.
Permanent failures (Paystack 4xx) are marked `failed` immediately with no retry.

---

## Project structure

```
payfault/
├── cmd/server/
│   ├── main.go          # Entrypoint — wires dependencies, starts HTTP + sync engine
│   └── handler.go       # HTTP handlers: POST /pay, GET /transaction/{id}
├── internal/
│   ├── db/              # Postgres connection pool
│   ├── models/          # Transaction struct, status enums, request/response types
│   ├── queue/           # DB-backed persistent queue (enqueue, dequeue, mark*)
│   ├── sync/            # Sync engine + worker pool
│   ├── paystack/        # Paystack HTTP client (transfer, idempotency header)
│   └── idempotency/     # Key derivation + idempotency_cache table wrapper
├── migrations/
│   └── 001_init.sql     # transactions + idempotency_cache tables
├── .env.example
└── Makefile
```

---

## Getting started

### 1. Prerequisites

- Go 1.22+
- Postgres 14+ (local, Neon, or Supabase)
- A Paystack test account (free at paystack.com)

### 2. Setup

```bash
git clone <repo>
cd payfault

# Install dependencies
go mod tidy

# Create your .env
cp .env.example .env
# Edit .env: add DATABASE_URL and PAYSTACK_SECRET_KEY

# Create the DB (if local)
createdb payfault

# Run migrations
make migrate

# Start the server
make run
```

### 3. Try it

```bash
# Initiate a payment (returns 202 immediately)
make curl-pay

# Copy the transaction_id from the response, then poll status:
TXN_ID=<paste-id-here> make curl-status
```

Watch the server logs — you'll see workers pick up the transaction, attach the
idempotency key, call Paystack, and update the status.

---

## Transaction lifecycle

```
POST /pay
  → DB write (status: pending)
  → 202 returned to client

Sync engine (background, every 3s):
  → SELECT FOR UPDATE SKIP LOCKED  ← concurrent-safe dequeue
  → check idempotency cache        ← avoid redundant Paystack calls
  → POST /transfer to Paystack     ← with Idempotency-Key header
      success → status: success, paystack_ref stored
      4xx     → status: failed (permanent)
      5xx/net → status: pending, next_retry_at = now + backoff
```

---
