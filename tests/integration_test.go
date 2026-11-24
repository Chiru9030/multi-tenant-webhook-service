package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chiru9030/multi-tenant-webhook-service/internal/api"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/backoff"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/crypto"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/dispatcher"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/metrics"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/model"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
)

func setupTestApp(t *testing.T) (*api.Server, *dispatcher.Dispatcher, *storage.Store, func()) {
	f, err := os.CreateTemp("", "integration_wh_*.db")
	if err != nil {
		t.Fatalf("temp file error: %v", err)
	}
	dbPath := f.Name()
	f.Close()

	store, err := storage.NewStore(dbPath)
	if err != nil {
		os.Remove(dbPath)
		t.Fatalf("store error: %v", err)
	}

	m := metrics.NewMetrics(prometheus.NewRegistry())
	dispConfig := dispatcher.Config{
		WorkerPoolSize: 4,
		PollInterval:   50 * time.Millisecond,
		BatchSize:      10,
		BackoffConfig: backoff.BackoffConfig{
			BaseInterval: 100 * time.Millisecond,
			MaxInterval:  500 * time.Millisecond,
			Multiplier:   1.5,
		},
	}
	disp := dispatcher.NewDispatcher(store, m, dispConfig)
	disp.Start()

	server := api.NewServer(store, disp, m, nil)

	cleanup := func() {
		disp.Stop()
		store.Close()
		os.Remove(dbPath)
	}

	return server, disp, store, cleanup
}

func TestEndToEndSuccessfulDelivery(t *testing.T) {
	server, _, store, cleanup := setupTestApp(t)
	defer cleanup()

	// 1. Setup mock receiver with signature verification
	var receivedPayload []byte
	var receivedSig string
	var mu sync.Mutex
	receiverDone := make(chan struct{})

	const endpointSecret = "whsec_supersecretkey1234567890abcdef"
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sig := r.Header.Get("X-Webhook-Signature")

		mu.Lock()
		receivedPayload = body
		receivedSig = sig
		mu.Unlock()

		// Verify HMAC signature
		valid, err := crypto.VerifySignature(sig, body, endpointSecret, 5*time.Minute)
		if !valid || err != nil {
			http.Error(w, fmt.Sprintf("invalid signature: %v", err), http.StatusUnauthorized)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received": true}`))
		close(receiverDone)
	}))
	defer receiver.Close()

	// 2. Register Tenant & Endpoint
	ctx := context.Background()
	_ = store.CreateTenant(ctx, &model.Tenant{ID: "ten_e2e", Name: "E2E Tenant"})
	_ = store.CreateEndpoint(ctx, &model.Endpoint{
		ID:             "ep_e2e",
		TenantID:       "ten_e2e",
		URL:            receiver.URL,
		SecretKey:      endpointSecret,
		EventTypes:     []string{"order.*"},
		IsActive:       true,
		MaxRetries:     3,
		TimeoutSeconds: 5,
	})

	// 3. Ingest Event via HTTP API
	payload := `{"order_id":"ord_999","amount":4900,"currency":"usd"}`
	reqBody := fmt.Sprintf(`{"event_type":"order.created","payload":%s}`, payload)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/ten_e2e/events", bytes.NewReader([]byte(reqBody)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected HTTP 202 Accepted, got %d: %s", w.Code, w.Body.String())
	}

	// Wait for receiver to process
	select {
	case <-receiverDone:
		mu.Lock()
		if len(receivedPayload) == 0 || receivedSig == "" {
			t.Errorf("expected received payload and signature to be populated")
		}
		mu.Unlock()
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for webhook delivery to mock receiver")
	}

	// 4. Verify Delivery recorded as 'success' with poll
	deadline := time.Now().Add(3 * time.Second)
	var deliveries []model.Delivery
	for time.Now().Before(deadline) {
		deliveries, _ = store.ListDeliveries(ctx, "ten_e2e", model.StatusSuccess, 10, 0)
		if len(deliveries) > 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 successful delivery in store, got %d", len(deliveries))
	}

	if deliveries[0].AttemptsCount != 1 {
		t.Errorf("expected 1 attempt, got %d", deliveries[0].AttemptsCount)
	}

	// 5. Verify Attempt audit record exists
	attempts, err := store.ListAttempts(ctx, deliveries[0].ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("expected 1 attempt trace, got %d", len(attempts))
	}
	if attempts[0].ResponseStatus != 200 {
		t.Errorf("expected response status 200, got %d", attempts[0].ResponseStatus)
	}
}

func TestRetryAndDLQEscalation(t *testing.T) {
	server, _, store, cleanup := setupTestApp(t)
	defer cleanup()

	// Mock receiver that always fails with 503
	var attemptsSeen int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attemptsSeen, 1)
		http.Error(w, "Service Unavailable (Simulated)", http.StatusServiceUnavailable)
	}))
	defer receiver.Close()

	ctx := context.Background()
	_ = store.CreateTenant(ctx, &model.Tenant{ID: "ten_retry", Name: "Retry Tenant"})
	_ = store.CreateEndpoint(ctx, &model.Endpoint{
		ID:             "ep_retry",
		TenantID:       "ten_retry",
		URL:            receiver.URL,
		SecretKey:      "whsec_retry_secret",
		EventTypes:     []string{"*"},
		IsActive:       true,
		MaxRetries:     2, // Fail after 2 attempts -> DLQ
		TimeoutSeconds: 2,
	})

	reqBody := `{"event_type":"user.signup","payload":{"user_id":"usr_123"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/ten_retry/events", bytes.NewReader([]byte(reqBody)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	// Wait for retry cycle and DLQ escalation
	deadline := time.Now().Add(5 * time.Second)
	var dlqFound bool
	for time.Now().Before(deadline) {
		deliveries, _ := store.ListDeliveries(ctx, "ten_retry", model.StatusDeadLetter, 10, 0)
		if len(deliveries) > 0 {
			dlqFound = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !dlqFound {
		t.Fatalf("expected delivery to escalate to dead_letter status after exceeding max_retries")
	}

	// Verify stats reflect dead letter
	stats, err := store.GetStats(ctx, "ten_retry")
	if err != nil || stats.DeadLetterCount != 1 {
		t.Fatalf("expected 1 dead letter count in stats, got %v", stats)
	}
}


func TestManualReplayAPI(t *testing.T) {
	server, _, store, cleanup := setupTestApp(t)
	defer cleanup()

	var shouldFail int32 = 1
	var deliveredSuccessfully int32

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&shouldFail) == 1 {
			http.Error(w, "Temporary outage", http.StatusInternalServerError)
			return
		}
		atomic.StoreInt32(&deliveredSuccessfully, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	defer receiver.Close()

	ctx := context.Background()
	_ = store.CreateTenant(ctx, &model.Tenant{ID: "ten_replay", Name: "Replay Tenant"})
	_ = store.CreateEndpoint(ctx, &model.Endpoint{
		ID:             "ep_replay",
		TenantID:       "ten_replay",
		URL:            receiver.URL,
		SecretKey:      "whsec_replay_key",
		EventTypes:     []string{"*"},
		IsActive:       true,
		MaxRetries:     1, // 1 retry then DLQ
		TimeoutSeconds: 2,
	})

	// Ingest event
	reqBody := `{"event_type":"payment.failed","payload":{"reason":"insufficient_funds"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/ten_replay/events", bytes.NewReader([]byte(reqBody)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	// Wait for DLQ
	var dlqDel *model.Delivery
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		deliveries, _ := store.ListDeliveries(ctx, "ten_replay", model.StatusDeadLetter, 10, 0)
		if len(deliveries) > 0 {
			dlqDel = &deliveries[0]
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if dlqDel == nil {
		t.Fatalf("expected delivery to reach dead_letter")
	}

	// Now fix receiver endpoint
	atomic.StoreInt32(&shouldFail, 0)

	// Trigger manual replay via HTTP POST /v1/deliveries/{id}/replay
	replayReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/deliveries/%s/replay", dlqDel.ID), nil)
	rw := httptest.NewRecorder()
	server.Handler().ServeHTTP(rw, replayReq)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from replay API, got %d: %s", rw.Code, rw.Body.String())
	}

	// Wait for dispatcher to replay and achieve success
	successDeadline := time.Now().Add(4 * time.Second)
	var finalSuccess bool
	for time.Now().Before(successDeadline) {
		d, _ := store.GetDelivery(ctx, dlqDel.ID)
		if d != nil && d.Status == model.StatusSuccess {
			finalSuccess = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !finalSuccess {
		t.Fatalf("expected replayed delivery to transition to success")
	}
	if atomic.LoadInt32(&deliveredSuccessfully) != 1 {
		t.Errorf("expected receiver to receive replayed webhook successfully")
	}
}
