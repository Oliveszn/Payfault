package paystack

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Every request automatically attaches: Authorization: Bearer and Idempotency-Key
type Client struct {
	mu    sync.Mutex
	store map[string]json.RawMessage //idempotency store
}

func New() *Client {
	return &Client{
		store: make(map[string]json.RawMessage),
	}
}

// TransferRequest is the payload for Paystack's initiate transfer endpoint.
type TransferRequest struct {
	Source    string `json:"source"`
	Amount    int64  `json:"amount"`
	Recipient string `json:"recipient"`
	Reason    string `json:"reason"`
	Reference string `json:"reference"` // internal txn ID
}

// TransferResponse is Paystack's response to a transfer initiation.
type TransferResponse struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
	Data    struct {
		Reference string `json:"reference"`
		Status    string `json:"status"`
		Amount    int64  `json:"amount"`
	} `json:"data"`
}

// InitiateTransfer sends a transfer request to our mock api
// idempotencyKey is sent as the Idempotency-Key header our mock api returns the original response if theyve seen this key before
func (c *Client) InitiateTransfer(ctx context.Context, req TransferRequest, idempotencyKey string) (*TransferResponse, json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Idempotency simulation
	if cached, ok := c.store[idempotencyKey]; ok {
		var resp TransferResponse
		_ = json.Unmarshal(cached, &resp)
		return &resp, cached, nil
	}

	// simulate network delay
	time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

	// Random outcome simulation
	r := rand.Float64()

	switch {
	// SUCCESS (60%)
	case r < 0.6:
		resp := TransferResponse{
			Status:  true,
			Message: "Transfer successful",
		}
		resp.Data.Reference = fmt.Sprintf("mock_ref_%d", time.Now().UnixNano())
		resp.Data.Status = "success"
		resp.Data.Amount = req.Amount

		raw, _ := json.Marshal(resp)
		c.store[idempotencyKey] = raw

		return &resp, raw, nil

	//  TRANSIENT ERROR (25%)
	case r < 0.85:
		return nil, nil, fmt.Errorf("mock network error: timeout")

	// PERMANENT ERROR (15%)
	default:
		body := `{"status":false,"message":"invalid recipient"}`
		return nil, []byte(body), &PermanentError{
			StatusCode: 400,
			Body:       body,
		}
	}
}

// PermanentError signals a 4xx from paystack
// we have the sync engine checking for this type of error to decide retry or fail
type PermanentError struct {
	StatusCode int
	Body       string
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("mock permanent error %d: %s", e.StatusCode, e.Body)
}
