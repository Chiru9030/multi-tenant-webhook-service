package model

import (
	"encoding/json"
	"time"
)

type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"
	StatusSending   DeliveryStatus = "sending"
	StatusSuccess   DeliveryStatus = "success"
	StatusRetrying  DeliveryStatus = "retrying"
	StatusDeadLetter DeliveryStatus = "dead_letter"
)

// Tenant represents an organizational tenant using the webhook service.
type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Endpoint represents an external destination registered by a tenant to receive events.
type Endpoint struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	URL            string    `json:"url"`
	Description    string    `json:"description,omitempty"`
	SecretKey      string    `json:"secret_key"`       // Used for HMAC-SHA256 signing
	EventTypes     []string  `json:"event_types"`     // e.g. ["payment.success", "invoice.*", "*"]
	IsActive       bool      `json:"is_active"`
	MaxRetries     int       `json:"max_retries"`     // Default 5
	TimeoutSeconds int       `json:"timeout_seconds"` // Default 10s
	RateLimitPerSec int      `json:"rate_limit_per_sec"` // 0 = unlimited
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Event represents an incoming business event published by a tenant.
type Event struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	EventType      string          `json:"event_type"`      // e.g. "payment.succeeded"
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	CreatedAt      time.Time       `json:"created_at"`
}

// Delivery represents an individual dispatch attempt cycle for an Event to a specific Endpoint.
type Delivery struct {
	ID            string         `json:"id"`
	TenantID      string         `json:"tenant_id"`
	EventID       string         `json:"event_id"`
	EndpointID    string         `json:"endpoint_id"`
	EndpointURL   string         `json:"endpoint_url"`
	Status        DeliveryStatus `json:"status"`
	AttemptsCount int            `json:"attempts_count"`
	MaxAttempts   int            `json:"max_attempts"`
	NextRetryAt   *time.Time     `json:"next_retry_at,omitempty"`
	LastAttemptAt *time.Time     `json:"last_attempt_at,omitempty"`
	LastError     string         `json:"last_error,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`

	// Enriched fields for API / Replay views
	Event         *Event         `json:"event,omitempty"`
	Endpoint      *Endpoint      `json:"endpoint,omitempty"`
}

// Attempt records the immutable HTTP execution trace of a delivery try.
type Attempt struct {
	ID             string         `json:"id"`
	DeliveryID     string         `json:"delivery_id"`
	AttemptNumber  int            `json:"attempt_number"`
	ResponseStatus int            `json:"response_status"`
	LatencyMs      int64          `json:"latency_ms"`
	RequestHeaders map[string]string `json:"request_headers"`
	ResponseBody   string         `json:"response_body"`
	ErrorReason    string         `json:"error_reason,omitempty"`
	ExecutedAt     time.Time      `json:"executed_at"`
}

// Stats provides an aggregate dashboard view of tenant or system health.
type Stats struct {
	TotalEvents      int64 `json:"total_events"`
	TotalDeliveries  int64 `json:"total_deliveries"`
	SuccessfulCount  int64 `json:"successful_count"`
	PendingCount     int64 `json:"pending_count"`
	RetryingCount    int64 `json:"retrying_count"`
	DeadLetterCount  int64 `json:"dead_letter_count"`
	SuccessRatePct   float64 `json:"success_rate_pct"`
	AvgLatencyMs     float64 `json:"avg_latency_ms"`
}
