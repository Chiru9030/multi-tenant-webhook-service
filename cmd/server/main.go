package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Chiru9030/multi-tenant-webhook-service/internal/api"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/backoff"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/crypto"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/dispatcher"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/metrics"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/model"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/storage"
	"github.com/Chiru9030/multi-tenant-webhook-service/web"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	port := flag.Int("port", 8080, "HTTP server port")
	dbPath := flag.String("db", "webhooks.db", "SQLite database file path")
	workers := flag.Int("workers", 10, "Worker pool size for dispatching webhooks")
	pollInterval := flag.Duration("poll", 1*time.Second, "Polling interval for pending deliveries")
	flag.Parse()

	visited := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { visited[f.Name] = true })

	if envPort := os.Getenv("PORT"); envPort != "" && !visited["port"] {
		if parsed, convErr := strconv.Atoi(envPort); convErr == nil && parsed > 0 {
			*port = parsed
		}
	}
	if envDB := os.Getenv("DB_PATH"); envDB != "" && !visited["db"] {
		*dbPath = envDB
	}

	log.Printf("Starting Relay Multi-Tenant Webhook Service on port %d...", *port)

	// 1. Initialize Storage
	store, err := storage.NewStore(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer store.Close()

	// Seed demo tenant and demo endpoint if empty
	seedDemoData(store)

	// 2. Metrics
	promReg := prometheus.DefaultRegisterer
	appMetrics := metrics.NewMetrics(promReg)

	// 3. Dispatcher
	dispConfig := dispatcher.Config{
		WorkerPoolSize: *workers,
		PollInterval:   *pollInterval,
		BatchSize:      50,
		BackoffConfig:  backoff.DefaultConfig,
	}
	disp := dispatcher.NewDispatcher(store, appMetrics, dispConfig)
	disp.Start()

	// 4. Embedded Web Static Assets
	staticFS, err := web.GetStaticFS()
	if err != nil {
		log.Printf("Warning: failed to initialize embedded static files: %v", err)
	}

	// 5. HTTP API Server
	apiServer := api.NewServerWithConfig(
		store,
		disp,
		appMetrics,
		staticFS,
		os.Getenv("WEBHOOK_ADMIN_API_KEY"),
		os.Getenv("ALLOWED_ORIGINS"),
	)
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      apiServer.Handler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Printf("⚡ Relay Webhook Engine listening at http://localhost:%d", *port)
		log.Printf("📊 Live Dashboard & Replay UI: http://localhost:%d/dashboard/", *port)
		log.Printf("📈 Prometheus metrics: http://localhost:%d/metrics", *port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Relay Webhook Engine...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = httpServer.Shutdown(ctx)
	disp.Stop()
	log.Println("Engine stopped cleanly.")
}

func seedDemoData(store *storage.Store) {
	ctx := context.Background()
	existing, _ := store.GetTenant(ctx, "ten_demo")
	if existing == nil {
		demoSecret := os.Getenv("DEMO_WEBHOOK_SECRET")
		if demoSecret == "" {
			demoSecret, _ = crypto.GenerateSecretKey()
		}
		_ = store.CreateTenant(ctx, &model.Tenant{
			ID:   "ten_demo",
			Name: "Acme Payments Demo",
		})
		_ = store.CreateEndpoint(ctx, &model.Endpoint{
			ID:             "ep_demo_receiver",
			TenantID:       "ten_demo",
			URL:            "https://httpbin.org/post",
			Description:    "HttpBin Echo Demo Receiver",
			SecretKey:      demoSecret,
			EventTypes:     []string{"*"},
			IsActive:       true,
			MaxRetries:     3,
			TimeoutSeconds: 5,
		})
	}
}
