package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"payfault/internal/db"
	"payfault/internal/idempotency"
	"payfault/internal/paystack"
	"payfault/internal/queue"
	server "payfault/internal/server"
	synce "payfault/internal/sync"

	"github.com/joho/godotenv"
)

func main() {
	// Load .env
	_ = godotenv.Load()

	//logging
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Root context — cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	//DB
	if err := db.Connect(ctx); err != nil {
		slog.Error("db connect failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("database connected")

	// dependencies
	q := queue.New(db.Pool)
	idem := idempotency.NewCache(db.Pool)
	ps := paystack.New()

	// Sync engine
	// Starts the worker pool in the background workers poll the DB every 3s and process pending transactions.
	engine := synce.New(q, ps, idem)
	go engine.Start(ctx)

	// server
	mux := http.NewServeMux()
	h := server.NewHandler(q, idem)
	h.RegisterRoutes(mux)

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	addr = ":" + addr

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start HTTP server in a goroutine so we can listen for shutdown signals.
	go func() {
		slog.Info("HTTP server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Block until signal received.
	<-ctx.Done()
	slog.Info("shutdown signal received")

	// Give in-flight requests 10s to complete before hard exit.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	} else {
		slog.Info("server shut down cleanly")
	}
}
