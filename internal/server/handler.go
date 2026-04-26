package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"payfault/internal/idempotency"
	"payfault/internal/models"
	"payfault/internal/queue"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	queue *queue.Queue
	idem  *idempotency.Cache
}

func NewHandler(q *queue.Queue, idem *idempotency.Cache) *Handler {
	return &Handler{queue: q, idem: idem}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /pay", h.handlePay)
	mux.HandleFunc("GET /transaction/{id}", h.handleGetTransaction)
	mux.HandleFunc("GET /health", h.handleHealth)
}

// handle pay is our entry point, this writes to DB and returns a 202
// doesnt call paystack or check network availability plus sync engine handles delivery in background
func (h *Handler) handlePay(w http.ResponseWriter, r *http.Request) {
	var req models.CreatePaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validatePayRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	txnID := uuid.New()
	idemKey := idempotency.KeyFor(txnID.String())

	txn := &models.Transaction{
		ID:             txnID,
		IdempotencyKey: idemKey,
		Amount:         req.Amount,
		Currency:       "NGN",
		SenderRef:      req.SenderRef,
		RecipientCode:  req.RecipientCode,
		Status:         models.StatusPending,
		MaxAttempts:    5,
		NextRetryAt:    time.Now(),
	}

	if err := h.queue.Enqueue(r.Context(), txn); err != nil {
		slog.Error("failed to enqueue transaction", "error", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	slog.Info("transaction enqueued", "txn_id", txnID, "amount_kobo", req.Amount)
	writeJSON(w, http.StatusAccepted, models.PaymentResponse{
		TransactionID:  txnID.String(),
		IdempotencyKey: idemKey,
		Status:         models.StatusPending,
		Message:        "Payment queued. It will be processed shortly.",
	})
}

// handleGetTransaction lets the client poll for status
//
// i	usie polling here but should be using webhook or sockets in a real app
func (h *Handler) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing transaction id")
		return
	}

	txn, err := h.queue.GetByID(r.Context(), id)
	if err != nil {
		slog.Error("get transaction failed", "error", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if txn == nil {
		writeError(w, http.StatusNotFound, "transaction not found")
		return
	}

	writeJSON(w, http.StatusOK, txn)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func validatePayRequest(req models.CreatePaymentRequest) error {
	if req.Amount <= 0 {
		return fmt.Errorf("amount must be greater than 0 kobo")
	}
	if req.RecipientCode == "" {
		return fmt.Errorf("recipient_code is required")
	}
	if req.SenderRef == "" {
		return fmt.Errorf("sender_ref is required")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
