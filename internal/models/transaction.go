package models

import (
	"time"

	"github.com/google/uuid"
)

// Status is the lifecycle of the transaction the lifecycle of a transaction.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSuccess    Status = "success"
	StatusFailed     Status = "failed"
)

// Transaction is the main thing, we write to db before a network call is made so the system can recover incase of crash or disconnect
type Transaction struct {
	ID             uuid.UUID `db:"id"`
	IdempotencyKey string    `db:"idempotency_key"`
	Amount         int64     `db:"amount"` // kobo
	Currency       string    `db:"currency"`
	SenderRef      string    `db:"sender_ref"`     // internal user/account ref
	RecipientCode  string    `db:"recipient_code"` // Paystack transfer recipient
	Status         Status    `db:"status"`
	Attempts       int       `db:"attempts"`
	MaxAttempts    int       `db:"max_attempts"`
	NextRetryAt    time.Time `db:"next_retry_at"`
	LastError      *string   `db:"last_error"`
	PaystackRef    *string   `db:"paystack_ref"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}

// IdempotencyCache stores the Payment (Paystack) response the first time a key is processed, so duplicate requests return instantly.
type IdempotencyCache struct {
	IdempotencyKey string    `db:"idempotency_key"`
	ResponseBody   []byte    `db:"response_body"`
	CreatedAt      time.Time `db:"created_at"`
	ExpiresAt      time.Time `db:"expires_at"`
}

// CreatePaymentRequest is what the HTTP handler receives from the client.
type CreatePaymentRequest struct {
	Amount        int64  `json:"amount"`         // amount is converted to kobo
	RecipientCode string `json:"recipient_code"` // paystack code
	SenderRef     string `json:"sender_ref"`
}

// PaymentResponse is returned to the client immediately after the transaction is written to DB (before network processing).
type PaymentResponse struct {
	TransactionID  string `json:"transaction_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Status         Status `json:"status"`
	Message        string `json:"message"`
}
