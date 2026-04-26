package sync_test

//these tests verify the sync engine routing logic
// -success MarkSuccess called
// -transient Markretry called
// -permanet MarkFailed called, no retry

//we use a fake Queue and fake Paystack instead of real implementations as we are testing the engine decision making and not the
//queue or paystack client itself

//a fake is a struct that implements the same interface as the real thing but does something simple and predictable instead

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"payfault/internal/models"
	synce "payfault/internal/sync"

	"github.com/google/uuid"
)

// Interfaces the engine depends on
// We define minimal interfaces here so we can swap in fakes.
// The real engine uses concrete types, but for testing we program to behaviour.

type transferResult struct {
	resp    *transferResponse
	rawBody json.RawMessage
	err     error
}

type transferResponse struct {
	Reference string
}

// fakeQueue records every call made to it so tests can assert on what happened.
type fakeQueue struct {
	successCalled bool
	retryCalled   bool
	failedCalled  bool
	lastRef       string
	lastErr       string
}

func (f *fakeQueue) MarkProcessing(_ context.Context, _ string) error { return nil }

func (f *fakeQueue) MarkSuccess(_ context.Context, _ string, ref string) error {
	f.successCalled = true
	f.lastRef = ref
	return nil
}

func (f *fakeQueue) MarkRetry(_ context.Context, _ string, _ int, errMsg string) error {
	f.retryCalled = true
	f.lastErr = errMsg
	return nil
}

func (f *fakeQueue) MarkFailed(_ context.Context, _ string, errMsg string) error {
	f.failedCalled = true
	f.lastErr = errMsg
	return nil
}

// fakePaystack lets each test control exactly what the mock client returns.
type fakePaystack struct {
	outcome string // "success", "transient", "permanent"
}

type permanentErr struct{ msg string }

func (e *permanentErr) Error() string { return e.msg }

// fakeIdempotencyCache

type fakeIdem struct {
	hit bool // if true, Get returns a cached result
}

func (f *fakeIdem) Get(_ context.Context, _ string) (json.RawMessage, error) {
	if f.hit {
		return json.RawMessage(`{"cached":true}`), nil
	}
	return nil, nil
}

func (f *fakeIdem) Set(_ context.Context, _ string, _ json.RawMessage) error {
	return nil
}

//  test transaction builder

func testTxn(attempts, maxAttempts int) *models.Transaction {
	return &models.Transaction{
		ID:             uuid.New(),
		IdempotencyKey: uuid.New().String(),
		Amount:         100000,
		Currency:       "NGN",
		SenderRef:      "user_test",
		RecipientCode:  "RCP_test",
		Status:         models.StatusProcessing,
		Attempts:       attempts,
		MaxAttempts:    maxAttempts,
		NextRetryAt:    time.Now(),
	}
}

// Tests

// TestEngine_SuccessPath verifies that when the payment client returns success,
// the engine calls MarkSuccess and stores the reference.
func TestEngine_SuccessPath(t *testing.T) {
	fq := &fakeQueue{}
	fi := &fakeIdem{}

	// Build engine with test doubles injected via the new constructor
	engine := synce.NewForTest(fq, fi, func(_ context.Context, idempotencyKey string) (string, json.RawMessage, error) {
		// simulate success
		ref := "mock_ref_success"
		raw, _ := json.Marshal(map[string]any{"data": map[string]string{"reference": ref}})
		return ref, raw, nil
	})

	txn := testTxn(1, 5)
	engine.ProcessForTest(context.Background(), txn)

	if !fq.successCalled {
		t.Error("expected MarkSuccess to be called, but it wasn't")
	}
	if fq.retryCalled {
		t.Error("expected MarkRetry NOT to be called, but it was")
	}
	if fq.failedCalled {
		t.Error("expected MarkFailed NOT to be called, but it was")
	}
}

// TestEngine_TransientError verifies that a network error triggers MarkRetry,
// not MarkFailed transient errors should be retried.
func TestEngine_TransientError(t *testing.T) {
	fq := &fakeQueue{}
	fi := &fakeIdem{}

	engine := synce.NewForTest(fq, fi, func(_ context.Context, _ string) (string, json.RawMessage, error) {
		// simulate a transient network error
		return "", nil, errors.New("mock network error: timeout")
	})

	txn := testTxn(1, 5) // attempt 1 of 5 — should retry
	engine.ProcessForTest(context.Background(), txn)

	if fq.successCalled {
		t.Error("expected MarkSuccess NOT to be called")
	}
	if !fq.retryCalled {
		t.Error("expected MarkRetry to be called, but it wasn't")
	}
	if fq.failedCalled {
		t.Error("expected MarkFailed NOT to be called")
	}
}

// TestEngine_PermanentError verifies that a 4xx error triggers MarkFailed
// immediately, with no retry retrying a bad recipient won't help.
func TestEngine_PermanentError(t *testing.T) {
	fq := &fakeQueue{}
	fi := &fakeIdem{}

	engine := synce.NewForTest(fq, fi, func(_ context.Context, _ string) (string, json.RawMessage, error) {
		// simulate a permanent error (4xx)
		return "", []byte(`{"status":false}`), &synce.PermanentErrorForTest{Code: 400}
	})

	txn := testTxn(1, 5) // still has retries left, but shouldn't use them
	engine.ProcessForTest(context.Background(), txn)

	if fq.successCalled {
		t.Error("expected MarkSuccess NOT to be called")
	}
	if fq.retryCalled {
		t.Error("expected MarkRetry NOT to be called — permanent errors don't retry")
	}
	if !fq.failedCalled {
		t.Error("expected MarkFailed to be called, but it wasn't")
	}
}

// TestEngine_MaxAttemptsExhausted verifies that a transient error on the final
// attempt triggers MarkFailed rather than MarkRetry.
// Without this, a stuck transaction would retry forever.
func TestEngine_MaxAttemptsExhausted(t *testing.T) {
	fq := &fakeQueue{}
	fi := &fakeIdem{}

	engine := synce.NewForTest(fq, fi, func(_ context.Context, _ string) (string, json.RawMessage, error) {
		return "", nil, errors.New("mock network error: timeout")
	})

	txn := testTxn(5, 5) // attempts == max_attempts — no retries left
	engine.ProcessForTest(context.Background(), txn)

	if fq.retryCalled {
		t.Error("expected MarkRetry NOT to be called — max attempts exhausted")
	}
	if !fq.failedCalled {
		t.Error("expected MarkFailed to be called, but it wasn't")
	}
}

// TestEngine_IdempotencyCacheHit verifies that a cached result short-circuits
// the payment client entirely we call MarkSuccess without touching the client.
func TestEngine_IdempotencyCacheHit(t *testing.T) {
	fq := &fakeQueue{}
	fi := &fakeIdem{hit: true} // cache returns a result

	clientCalled := false
	engine := synce.NewForTest(fq, fi, func(_ context.Context, _ string) (string, json.RawMessage, error) {
		clientCalled = true // this should NOT be called
		return "ref", nil, nil
	})

	txn := testTxn(1, 5)
	engine.ProcessForTest(context.Background(), txn)

	if clientCalled {
		t.Error("payment client was called despite idempotency cache hit — should have used cached result")
	}
	if !fq.successCalled {
		t.Error("expected MarkSuccess to be called with cached result")
	}
}
