package paystack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const baseURL = "https://api.paystack.co"

// Every request automatically attaches: Authorization: Bearer and Idempotency-Key
type Client struct {
	secretKey  string
	httpClient *http.Client
}

func New() *Client {
	return &Client{
		secretKey: os.Getenv("PAYSTACK_SECRET_KEY"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
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

// InitiateTransfer sends a transfer request to Paystack
// idempotencyKey is sent as the Idempotency-Key header Paystack returns the original response if theyve seen this key before
func (c *Client) InitiateTransfer(
	ctx context.Context,
	req TransferRequest,
	idempotencyKey string,
) (*TransferResponse, json.RawMessage, error) {

	body, err := json.Marshal(req)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal transfer request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/transfer",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.secretKey)

	// This is the idempotency header. paystack stores responses against this key for 24h
	// If the client retries, paystack returns the same response rather than processing a second transfer
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// Network level error like a timeout or connection refused
		// This is safe to retry cos we never reached paystack
		return nil, nil, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read response body: %w", err)
	}

	// 4xx errors are permanent could be bad request or invalid smth
	// 5xx errors are transient, so safe to retry.
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, rawBody, &PermanentError{
			StatusCode: resp.StatusCode,
			Body:       string(rawBody),
		}
	}
	if resp.StatusCode >= 500 {
		return nil, rawBody, fmt.Errorf("paystack server error %d: %s", resp.StatusCode, rawBody)
	}

	var transfer TransferResponse
	if err := json.Unmarshal(rawBody, &transfer); err != nil {
		return nil, rawBody, fmt.Errorf("unmarshal transfer response: %w", err)
	}

	return &transfer, rawBody, nil
}

// PermanentError signals a 4xx from paystack
// we have the sync engine checking for this type of error to decide retry or fail
type PermanentError struct {
	StatusCode int
	Body       string
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("paystack permanent error %d: %s", e.StatusCode, e.Body)
}
