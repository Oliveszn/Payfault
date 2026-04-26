# payfault

An offline-first payment service in Go, built to understand resilient payment systems. Demonstrates idempotency, transactional logic, concurrency control, and retry with exponential backoff, the same patterns used in production fintech infrastructure.

---

## Core concepts implemented

**Idempotency**
Every transaction gets an idempotency key derived from its UUID via SHA-256:

```go
func KeyFor(txnID string) string {
    h := sha256.Sum256([]byte("payfault:" + txnID))
    return fmt.Sprintf("%x", h[:16])
}
```

The key is attached to every payment attempt. The mock client stores responses against this key — if it has seen the key before, it returns the original response immediately without processing again. This means retries after a crash or timeout can never produce a duplicate payment.

**Offline-first queue**
Payment intents are written to the DB before any processing happens. `POST /pay` returns `202 Accepted` immediately. The sync engine drives transactions to completion in the background. Kill the process mid-payment and restart it — it picks up from where it left off.

**Concurrency control with `SELECT FOR UPDATE SKIP LOCKED`**
The sync engine runs 5 worker goroutines, all polling the same DB table. The critical detail: `FOR UPDATE SKIP LOCKED` only works as intended inside an explicit transaction. In autocommit mode the lock evaporates the moment the query returns, causing all workers to claim the same row.

The fix — `Dequeue` opens an explicit `tx.Begin()`, does the `SELECT FOR UPDATE SKIP LOCKED`, immediately `UPDATE`s the claimed rows to `processing` inside the same transaction, then commits. By the time other workers run their `SELECT`, those rows are already `processing` and `SKIP LOCKED` skips them. No mutexes, no external coordination — the DB does the work.

**Exponential backoff with jitter**
Transient failures are retried with increasing wait times:

```
attempt 1 →  ~2s
attempt 2 →  ~4s
attempt 3 →  ~8s
attempt 4 → ~16s
attempt 5 →  30s (capped)
```

±20% jitter is added to prevent a thundering herd — the situation where every queued retry fires at the same moment and slams the downstream service simultaneously.

Permanent failures (4xx equivalent) are marked `failed` immediately with no retry — retrying a bad recipient code or malformed request will never succeed regardless of how many times you try.

---

## Mock payment client

The project uses an in-memory mock client instead of hitting a real payment API, which lets you observe all the failure modes locally:

```
60%  → success       (stores response against idempotency key)
25%  → transient error (mock network timeout — will retry with backoff)
15%  → permanent error (mock invalid recipient — fails immediately)
```

Each outcome exercises a different code path. Running multiple payments back-to-back lets you see all three in the logs.

The mock client also simulates Paystack's idempotency behaviour: the first call for a given key processes and stores the result; subsequent calls with the same key return the stored result instantly, never re-entering the success/failure logic.

---

## Project structure

```
payfault/
├── cmd/server/
│   ├── main.go              # Entrypoint — wires deps, starts HTTP + sync engine
│   └── handler.go           # POST /pay, GET /transaction/{id}, GET /health
├── internal/
│   ├── db/                  # Postgres connection pool
│   ├── models/              # Transaction struct, status enums, request/response types
│   ├── queue/               # DB-backed queue — enqueue, atomic dequeue, mark* methods
│   ├── sync/                # Worker pool — polls queue, drives transactions to completion
│   ├── paystack/            # Mock payment client with idempotency, random outcomes
│   └── idempotency/         # Key derivation + idempotency_cache table wrapper
├── migrations/
│   └── 001_init.sql         # transactions + idempotency_cache schema
├── .env.example
└── Makefile
```

---

## Getting started

**Prerequisites**: Go 1.22+, Postgres 14+

```bash
cp .env.example .env     # set DATABASE_URL
go mod tidy
make migrate
make run
```

**Fire a payment:**

```bash
make curl-pay
# returns 202 immediately with a transaction_id
```

**Poll status:**

```bash
TXN_ID=<paste-id> make curl-status
```

Watch the server logs. Within 3 seconds a worker picks up the transaction, runs it through the mock client, and updates the status. Run `make curl-pay` several times and you'll see all three outcomes — success, retry-then-succeed, and permanent failure — across different transactions.

---

## Transaction lifecycle

```
POST /pay
  → write to DB (status: pending)
  → 202 returned immediately

Sync engine (background, 5 workers, polls every 3s):
  → SELECT FOR UPDATE SKIP LOCKED  inside explicit tx  ← atomic claim
  → UPDATE status = processing                         ← same tx, commits together
  → check idempotency cache                            ← skip mock if already processed
  → call mock client with idempotency key
      success      → cache response → status: success
      transient    → status: pending, next_retry_at = now + backoff
      permanent    → status: failed, no retry
```

---
