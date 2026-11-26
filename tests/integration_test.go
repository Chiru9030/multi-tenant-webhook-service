package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
		WorkerPoolSize:           4,
		PollInterval:             50 * time.Millisecond,
		BatchSize:                10,
		AllowPrivateDestinations: true,
		BackoffConfig: backoff.BackoffConfig{
			BaseInterval: 100 * time.Millisecond,
			MaxInterval:  500 * time.Millisecond,
			Multiplier:   1.5,
		},
	}
	disp := dispatcher.NewDispatcher(store, m, dispConfig)
	disp.Start()

	server := api.NewServerWithOptions(store, disp, m, nil, api.ServerOptions{AllowPrivateDestinations: true})

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
	var deliveryHits int64
	var mu sync.Mutex
	var once sync.Once
	receiverDone := make(chan struct{})

	const endpointSecret = "whsec_supersecretkey1234567890abcdef"
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&deliveryHits, 1)
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
		once.Do(func() { close(receiverDone) })
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
	if hits := atomic.LoadInt64(&deliveryHits); hits != 1 {
		t.Fatalf("expected exactly 1 webhook delivery without duplicate dispatch, got %d", hits)
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

func TestAdminAuthReadyzSecurityHeadersAndURLValidation(t *testing.T) {
	f, err := os.CreateTemp("", "auth_wh_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := f.Name()
	f.Close()
	defer os.Remove(dbPath)

	store, err := storage.NewStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	disp := dispatcher.NewDispatcher(store, m, dispatcher.Config{WorkerPoolSize: 2, PollInterval: 50 * time.Millisecond})
	srv := api.NewServerWithConfig(store, disp, m, nil, "admin_secret_v1", "https://dashboard.example.com")

	// /healthz and /readyz remain public with security headers
	for _, probe := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, probe, nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 on %s, got %d", probe, rec.Code)
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("missing security header on %s", probe)
		}
		if rec.Header().Get("Access-Control-Allow-Origin") != "https://dashboard.example.com" {
			t.Fatalf("unexpected CORS origin: %s", rec.Header().Get("Access-Control-Allow-Origin"))
		}
	}

	// Missing key rejected with 401
	unauth := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	uRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(uRec, unauth)
	if uRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without API key, got %d", uRec.Code)
	}

	// Authorized tenant creation + invalid URL rejection
	_ = store.CreateTenant(context.Background(), &model.Tenant{ID: "ten_sec", Name: "Sec Tenant"})
	badEp := httptest.NewRequest(http.MethodPost, "/v1/tenants/ten_sec/endpoints", bytes.NewBufferString(`{"url":"ftp://invalid.local/hook"}`))
	badEp.Header.Set("Content-Type", "application/json")
	badEp.Header.Set("X-API-Key", "admin_secret_v1")
	badRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(badRec, badEp)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 BadRequest for non-http URL scheme, got %d", badRec.Code)
	}
}

func TestWebhookAuditRegressions(t *testing.T) {
	f, err := os.CreateTemp("", "regress_wh_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := f.Name()
	f.Close()
	defer os.Remove(dbPath)

	store, err := storage.NewStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	disp := dispatcher.NewDispatcher(store, m, dispatcher.Config{
		WorkerPoolSize:           2,
		PollInterval:             50 * time.Millisecond,
		BatchSize:                5,
		AllowPrivateDestinations: true,
	})
	disp.Start()
	// 1. Idempotent Dispatcher.Stop() must never panic on multiple calls
	disp.Stop()
	disp.Stop()

	srv := api.NewServerWithOptions(store, disp, m, nil, api.ServerOptions{
		AdminAPIKey:              "admin_key_v1",
		AllowedOrigins:           "https://app1.example.com, https://app2.example.com",
		AllowPrivateDestinations: false,
	})

	// 2. Multi-origin CORS matching + Vary: Origin, and public /metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	reqMetrics.Header.Set("Origin", "https://app2.example.com")
	recMetrics := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recMetrics, reqMetrics)
	if recMetrics.Code != http.StatusOK || recMetrics.Header().Get("Access-Control-Allow-Origin") != "https://app2.example.com" || recMetrics.Header().Get("Vary") != "Origin" {
		t.Fatalf("unexpected /metrics CORS response: code=%d origin=%q vary=%q", recMetrics.Code, recMetrics.Header().Get("Access-Control-Allow-Origin"), recMetrics.Header().Get("Vary"))
	}

	// Strict Bearer prefix required
	badAuth := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	badAuth.Header.Set("Authorization", "admin_key_v1")
	badAuthRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(badAuthRec, badAuth)
	if badAuthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when Bearer scheme is omitted, got %d", badAuthRec.Code)
	}

	ctx := context.Background()
	_ = store.CreateTenant(ctx, &model.Tenant{ID: "t_alpha", Name: "Alpha"})
	_ = store.CreateTenant(ctx, &model.Tenant{ID: "t_beta", Name: "Beta"})

	// 3. SSRF blocking on loopback, private IPs, and cloud metadata
	for _, target := range []string{
		"http://127.0.0.1:8080/hook",
		"http://169.254.169.254/latest/meta-data",
		"http://10.0.0.1/internal",
		"http://localhost/admin",
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/t_alpha/endpoints", bytes.NewBufferString(fmt.Sprintf(`{"url":%q}`, target)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", "admin_key_v1")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected SSRF block 400 for %s, got %d", target, rec.Code)
		}
	}

	// 4. 1 MiB Body cap (413), JSON null rejection (400), nonexistent tenant (404), limit/offset bounds
	huge := strings.Repeat("a", (1<<20)+128)
	hugeReq := httptest.NewRequest(http.MethodPost, "/v1/tenants/t_alpha/events", bytes.NewBufferString(fmt.Sprintf(`{"event_type":"x","payload":{"d":%q}}`, huge)))
	hugeReq.Header.Set("Content-Type", "application/json")
	hugeReq.Header.Set("X-API-Key", "admin_key_v1")
	hugeRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(hugeRec, hugeReq)
	if hugeRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 on >1MiB event body, got %d", hugeRec.Code)
	}

	nullReq := httptest.NewRequest(http.MethodPost, "/v1/tenants/t_alpha/events", bytes.NewBufferString(`{"event_type":"x","payload":null}`))
	nullReq.Header.Set("Content-Type", "application/json")
	nullReq.Header.Set("X-API-Key", "admin_key_v1")
	nullRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(nullRec, nullReq)
	if nullRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on null payload, got %d", nullRec.Code)
	}

	missReq := httptest.NewRequest(http.MethodPost, "/v1/tenants/t_missing/events", bytes.NewBufferString(`{"event_type":"x","payload":{"ok":true}}`))
	missReq.Header.Set("Content-Type", "application/json")
	missReq.Header.Set("X-API-Key", "admin_key_v1")
	missRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(missRec, missReq)
	if missRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on unknown tenant, got %d", missRec.Code)
	}

	// 5. Concurrent Idempotency-Key HTTP ingestion (only 1 new, others hydrate cleanly as 200 duplicate)
	var status202, status200 atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodPost, "/v1/tenants/t_alpha/events", bytes.NewBufferString(`{"event_type":"order.paid","payload":{"amount":42}}`))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("X-API-Key", "admin_key_v1")
			r.Header.Set("Idempotency-Key", "idem-concurrent-key")
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, r)
			if rec.Code == http.StatusAccepted {
				status202.Add(1)
			} else if rec.Code == http.StatusOK {
				var out struct {
					Event model.Event `json:"event"`
				}
				_ = json.Unmarshal(rec.Body.Bytes(), &out)
				if out.Event.EventType == "order.paid" {
					status200.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if status202.Load() != 1 || status200.Load() != 5 {
		t.Fatalf("expected 1x202 and 5x200 hydrated duplicate, got 202=%d 200=%d", status202.Load(), status200.Load())
	}

	// 6. Concurrent FetchDueDeliveries locking, replay-all, and tenant-scoped AvgLatencyMs
	if err := store.CreateEndpoint(ctx, &model.Endpoint{ID: "ep_a", TenantID: "t_alpha", URL: "https://example.com/a", SecretKey: "s1", EventTypes: []string{"*"}, IsActive: true, MaxRetries: 3, TimeoutSeconds: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateEvent(ctx, &model.Event{ID: "evt_a1", TenantID: "t_alpha", EventType: "order.created", Payload: []byte(`{"id":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDelivery(ctx, &model.Delivery{ID: "del_a1", TenantID: "t_alpha", EventID: "evt_a1", EndpointID: "ep_a", EndpointURL: "https://example.com/a", Status: model.StatusPending, MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDelivery(ctx, &model.Delivery{ID: "del_a2", TenantID: "t_alpha", EventID: "evt_a1", EndpointID: "ep_a", EndpointURL: "https://example.com/a", Status: model.StatusDeadLetter, MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAttempt(ctx, &model.Attempt{ID: "att_a1", DeliveryID: "del_a1", AttemptNumber: 1, ResponseStatus: 200, LatencyMs: 120}); err != nil {
		t.Fatal(err)
	}

	var claimedTotal atomic.Int32
	var fwg sync.WaitGroup
	for g := 0; g < 4; g++ {
		fwg.Add(1)
		go func() {
			defer fwg.Done()
			list, _ := store.FetchDueDeliveries(ctx, 10)
			claimedTotal.Add(int32(len(list)))
		}()
	}
	fwg.Wait()
	if claimedTotal.Load() != 1 {
		t.Fatalf("expected exactly 1 exclusive pending claim across concurrent pollers, got %d", claimedTotal.Load())
	}

	// Replay-all DLQ resets del_a2 -> pending
	repReq := httptest.NewRequest(http.MethodPost, "/v1/endpoints/ep_a/replay-all", nil)
	repReq.Header.Set("X-API-Key", "admin_key_v1")
	repRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(repRec, repReq)
	if repRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on replay-all, got %d", repRec.Code)
	}
	reloaded, _ := store.GetDelivery(ctx, "del_a2")
	if reloaded == nil || reloaded.Status != model.StatusPending {
		t.Fatalf("expected del_a2 to reset to pending after replay-all, got %+v", reloaded)
	}

	// Stats tenant isolation: Beta has 0 attempts so AvgLatencyMs must be 0, while Alpha is 120
	alphaStats, _ := store.GetStats(ctx, "t_alpha")
	betaStats, _ := store.GetStats(ctx, "t_beta")
	if alphaStats.AvgLatencyMs != 120 || betaStats.AvgLatencyMs != 0 {
		t.Fatalf("tenant stats AvgLatencyMs isolation failed: alpha=%v beta=%v", alphaStats.AvgLatencyMs, betaStats.AvgLatencyMs)
	}
}

