package storage

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Chiru9030/multi-tenant-webhook-service/internal/model"
)

func setupTestStore(t *testing.T) (*Store, func()) {
	f, err := os.CreateTemp("", "test_webhook_*.db")
	if err != nil {
		t.Fatalf("failed to create temp db file: %v", err)
	}
	dbPath := f.Name()
	f.Close()

	store, err := NewStore(dbPath)
	if err != nil {
		os.Remove(dbPath)
		t.Fatalf("failed to init store: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.Remove(dbPath)
	}
	return store, cleanup
}

func TestTenantAndEndpointCRUD(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Create Tenant
	tenant := &model.Tenant{
		ID:   "ten_test_01",
		Name: "Test Tenant Inc",
	}
	if err := store.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("CreateTenant failed: %v", err)
	}

	gotTenant, err := store.GetTenant(ctx, "ten_test_01")
	if err != nil || gotTenant == nil || gotTenant.Name != "Test Tenant Inc" {
		t.Fatalf("GetTenant failed: %v, got %v", err, gotTenant)
	}

	// 2. Create Endpoint
	ep := &model.Endpoint{
		ID:             "ep_test_01",
		TenantID:       "ten_test_01",
		URL:            "https://api.example.com/webhooks",
		SecretKey:      "whsec_test123456789",
		EventTypes:     []string{"payment.*", "customer.created"},
		IsActive:       true,
		MaxRetries:     3,
		TimeoutSeconds: 5,
	}
	if err := store.CreateEndpoint(ctx, ep); err != nil {
		t.Fatalf("CreateEndpoint failed: %v", err)
	}

	endpoints, err := store.ListEndpointsByTenant(ctx, "ten_test_01")
	if err != nil || len(endpoints) != 1 {
		t.Fatalf("ListEndpointsByTenant failed: %v, count=%d", err, len(endpoints))
	}

	// 3. Test Event matching with wildcards
	matched, err := store.FindMatchingEndpoints(ctx, "ten_test_01", "payment.succeeded")
	if err != nil || len(matched) != 1 {
		t.Fatalf("FindMatchingEndpoints expected 1 match for payment.succeeded, got %d", len(matched))
	}

	matchedUnrelated, _ := store.FindMatchingEndpoints(ctx, "ten_test_01", "order.refunded")
	if len(matchedUnrelated) != 0 {
		t.Fatalf("FindMatchingEndpoints expected 0 match for order.refunded, got %d", len(matchedUnrelated))
	}
}

func TestEventIdempotency(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	_ = store.CreateTenant(ctx, &model.Tenant{ID: "ten_idem", Name: "Idem Tenant"})

	ev1 := &model.Event{
		ID:             "evt_01",
		TenantID:       "ten_idem",
		EventType:      "invoice.paid",
		IdempotencyKey: "idem_key_unique_123",
		Payload:        json.RawMessage(`{"amount": 500}`),
	}

	isNew, err := store.CreateEvent(ctx, ev1)
	if err != nil || !isNew {
		t.Fatalf("expected ev1 to be new, got isNew=%v, err=%v", isNew, err)
	}

	// Try inserting identical idempotency key
	ev2 := &model.Event{
		ID:             "evt_02",
		TenantID:       "ten_idem",
		EventType:      "invoice.paid",
		IdempotencyKey: "idem_key_unique_123",
		Payload:        json.RawMessage(`{"amount": 500}`),
	}

	isNew2, err2 := store.CreateEvent(ctx, ev2)
	if err2 != nil {
		t.Fatalf("unexpected error on idempotent replay: %v", err2)
	}
	if isNew2 {
		t.Fatalf("expected second event to NOT be new")
	}
	if ev2.ID != "evt_01" {
		t.Fatalf("expected existing event ID to be returned, got %s", ev2.ID)
	}
}

func TestDeliveryLifecycleAndDLQReplay(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	_ = store.CreateTenant(ctx, &model.Tenant{ID: "ten_dlq", Name: "DLQ Tenant"})
	_ = store.CreateEndpoint(ctx, &model.Endpoint{
		ID: "ep_dlq", TenantID: "ten_dlq", URL: "http://target", SecretKey: "whsec_123", EventTypes: []string{"*"}, IsActive: true, MaxRetries: 3,
	})
	_, _ = store.CreateEvent(ctx, &model.Event{
		ID: "evt_dlq", TenantID: "ten_dlq", EventType: "test.event", Payload: json.RawMessage(`{}`),
	})

	del := &model.Delivery{
		ID:          "del_01",
		TenantID:    "ten_dlq",
		EventID:     "evt_dlq",
		EndpointID:  "ep_dlq",
		EndpointURL: "http://target",
		Status:      model.StatusPending,
		MaxAttempts: 3,
	}
	if err := store.CreateDelivery(ctx, del); err != nil {
		t.Fatalf("CreateDelivery failed: %v", err)
	}

	// Simulate failures leading to Dead Letter
	retryTime := time.Now().UTC().Add(10 * time.Minute)
	_ = store.UpdateDeliveryStatus(ctx, "del_01", model.StatusRetrying, 1, &retryTime, "HTTP 500")

	// Escalate to Dead Letter
	_ = store.UpdateDeliveryStatus(ctx, "del_01", model.StatusDeadLetter, 3, nil, "Exhausted retries")

	d, _ := store.GetDelivery(ctx, "del_01")
	if d.Status != model.StatusDeadLetter {
		t.Fatalf("expected status to be dead_letter, got %s", d.Status)
	}

	// Replay DLQ
	replayedCount, err := store.ReplayAllDLQ(ctx, "ep_dlq")
	if err != nil || replayedCount != 1 {
		t.Fatalf("expected 1 replayed delivery, got %d (err: %v)", replayedCount, err)
	}

	dAfter, _ := store.GetDelivery(ctx, "del_01")
	if dAfter.Status != model.StatusPending {
		t.Fatalf("expected delivery to be reset to pending, got %s", dAfter.Status)
	}
}
