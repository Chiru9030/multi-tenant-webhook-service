package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Chiru9030/multi-tenant-webhook-service/internal/model"
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewStore(dbPath string) (*Store, error) {
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(10 * time.Minute)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS tenants (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL
	);

	CREATE TABLE IF NOT EXISTS endpoints (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL,
		url TEXT NOT NULL,
		description TEXT,
		secret_key TEXT NOT NULL,
		event_types TEXT NOT NULL, -- JSON array of strings
		is_active INTEGER NOT NULL DEFAULT 1,
		max_retries INTEGER NOT NULL DEFAULT 5,
		timeout_seconds INTEGER NOT NULL DEFAULT 10,
		rate_limit_per_sec INTEGER NOT NULL DEFAULT 0,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS events (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		idempotency_key TEXT,
		payload TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL,
		FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idempotency ON events(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
	CREATE INDEX IF NOT EXISTS idx_events_tenant_created ON events(tenant_id, created_at DESC);

	CREATE TABLE IF NOT EXISTS deliveries (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL,
		event_id TEXT NOT NULL,
		endpoint_id TEXT NOT NULL,
		endpoint_url TEXT NOT NULL,
		status TEXT NOT NULL, -- pending, sending, success, retrying, dead_letter
		attempts_count INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 5,
		next_retry_at TIMESTAMP,
		last_attempt_at TIMESTAMP,
		last_error TEXT,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
		FOREIGN KEY (endpoint_id) REFERENCES endpoints(id) ON DELETE CASCADE
	);
	CREATE INDEX IF NOT EXISTS idx_deliveries_status_retry ON deliveries(status, next_retry_at);
	CREATE INDEX IF NOT EXISTS idx_deliveries_tenant_created ON deliveries(tenant_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_deliveries_endpoint ON deliveries(endpoint_id, status);

	CREATE TABLE IF NOT EXISTS attempts (
		id TEXT PRIMARY KEY,
		delivery_id TEXT NOT NULL,
		attempt_number INTEGER NOT NULL,
		response_status INTEGER NOT NULL,
		latency_ms INTEGER NOT NULL,
		request_headers TEXT NOT NULL, -- JSON
		response_body TEXT,
		error_reason TEXT,
		executed_at TIMESTAMP NOT NULL,
		FOREIGN KEY (delivery_id) REFERENCES deliveries(id) ON DELETE CASCADE
	);
	CREATE INDEX IF NOT EXISTS idx_attempts_delivery ON attempts(delivery_id, executed_at ASC);
	`
	_, err := s.db.Exec(schema)
	return err
}

// --- Tenants ---

func (s *Store) CreateTenant(ctx context.Context, t *model.Tenant) error {
	now := time.Now().UTC()
	t.CreatedAt = now
	t.UpdatedAt = now
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tenants (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)`,
		t.ID, t.Name, t.CreatedAt, t.UpdatedAt,
	)
	return err
}

func (s *Store) GetTenant(ctx context.Context, id string) (*model.Tenant, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, name, created_at, updated_at FROM tenants WHERE id = ?`, id)
	var t model.Tenant
	if err := row.Scan(&t.ID, &t.Name, &t.CreatedAt, &t.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

func (s *Store) ListTenants(ctx context.Context) ([]model.Tenant, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, created_at, updated_at FROM tenants ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []model.Tenant
	for rows.Next() {
		var t model.Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		list = append(list, t)
	}
	return list, rows.Err()
}

// --- Endpoints ---

func (s *Store) CreateEndpoint(ctx context.Context, ep *model.Endpoint) error {
	now := time.Now().UTC()
	ep.CreatedAt = now
	ep.UpdatedAt = now
	if ep.MaxRetries <= 0 {
		ep.MaxRetries = 5
	}
	if ep.TimeoutSeconds <= 0 {
		ep.TimeoutSeconds = 10
	}

	eventTypesJSON, err := json.Marshal(ep.EventTypes)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO endpoints (id, tenant_id, url, description, secret_key, event_types, is_active, max_retries, timeout_seconds, rate_limit_per_sec, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ep.ID, ep.TenantID, ep.URL, ep.Description, ep.SecretKey, string(eventTypesJSON),
		boolToInt(ep.IsActive), ep.MaxRetries, ep.TimeoutSeconds, ep.RateLimitPerSec, ep.CreatedAt, ep.UpdatedAt,
	)
	return err
}

func (s *Store) GetEndpoint(ctx context.Context, id string) (*model.Endpoint, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, url, description, secret_key, event_types, is_active, max_retries, timeout_seconds, rate_limit_per_sec, created_at, updated_at
		 FROM endpoints WHERE id = ?`, id,
	)
	var ep model.Endpoint
	var eventTypesJSON string
	var isActive int
	if err := row.Scan(
		&ep.ID, &ep.TenantID, &ep.URL, &ep.Description, &ep.SecretKey, &eventTypesJSON,
		&isActive, &ep.MaxRetries, &ep.TimeoutSeconds, &ep.RateLimitPerSec, &ep.CreatedAt, &ep.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ep.IsActive = (isActive == 1)
	_ = json.Unmarshal([]byte(eventTypesJSON), &ep.EventTypes)
	return &ep, nil
}

func (s *Store) ListEndpointsByTenant(ctx context.Context, tenantID string) ([]model.Endpoint, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, url, description, secret_key, event_types, is_active, max_retries, timeout_seconds, rate_limit_per_sec, created_at, updated_at
		 FROM endpoints WHERE tenant_id = ? ORDER BY created_at DESC`, tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []model.Endpoint
	for rows.Next() {
		var ep model.Endpoint
		var eventTypesJSON string
		var isActive int
		if err := rows.Scan(
			&ep.ID, &ep.TenantID, &ep.URL, &ep.Description, &ep.SecretKey, &eventTypesJSON,
			&isActive, &ep.MaxRetries, &ep.TimeoutSeconds, &ep.RateLimitPerSec, &ep.CreatedAt, &ep.UpdatedAt,
		); err != nil {
			return nil, err
		}
		ep.IsActive = (isActive == 1)
		_ = json.Unmarshal([]byte(eventTypesJSON), &ep.EventTypes)
		list = append(list, ep)
	}
	return list, rows.Err()
}

func (s *Store) FindMatchingEndpoints(ctx context.Context, tenantID, eventType string) ([]model.Endpoint, error) {
	endpoints, err := s.ListEndpointsByTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	var matched []model.Endpoint
	for _, ep := range endpoints {
		if !ep.IsActive {
			continue
		}
		if matchesEventType(ep.EventTypes, eventType) {
			matched = append(matched, ep)
		}
	}
	return matched, nil
}

func matchesEventType(subscriptions []string, eventType string) bool {
	for _, sub := range subscriptions {
		if sub == "*" || sub == eventType {
			return true
		}
		if strings.HasSuffix(sub, ".*") {
			prefix := strings.TrimSuffix(sub, ".*")
			if strings.HasPrefix(eventType, prefix+".") {
				return true
			}
		}
	}
	return false
}

// --- Events ---

func (s *Store) CreateEvent(ctx context.Context, ev *model.Event) (bool, error) {
	now := time.Now().UTC()
	ev.CreatedAt = now

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	if ev.IdempotencyKey != "" {
		var existingID, existingType, payloadStr string
		var createdAt time.Time
		err := tx.QueryRowContext(ctx,
			`SELECT id, event_type, payload, created_at FROM events WHERE tenant_id = ? AND idempotency_key = ?`,
			ev.TenantID, ev.IdempotencyKey,
		).Scan(&existingID, &existingType, &payloadStr, &createdAt)
		if err == nil {
			ev.ID = existingID
			ev.EventType = existingType
			ev.Payload = []byte(payloadStr)
			ev.CreatedAt = createdAt
			return false, nil
		}
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO events (id, tenant_id, event_type, idempotency_key, payload, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		ev.ID, ev.TenantID, ev.EventType, sqlNullString(ev.IdempotencyKey), string(ev.Payload), ev.CreatedAt,
	)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) GetEvent(ctx context.Context, id string) (*model.Event, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, event_type, idempotency_key, payload, created_at FROM events WHERE id = ?`, id,
	)
	var ev model.Event
	var idemKey sql.NullString
	var payloadStr string
	if err := row.Scan(&ev.ID, &ev.TenantID, &ev.EventType, &idemKey, &payloadStr, &ev.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if idemKey.Valid {
		ev.IdempotencyKey = idemKey.String
	}
	ev.Payload = json.RawMessage(payloadStr)
	return &ev, nil
}

// --- Deliveries ---

func (s *Store) CreateDelivery(ctx context.Context, d *model.Delivery) error {
	now := time.Now().UTC()
	d.CreatedAt = now
	d.UpdatedAt = now
	if d.Status == "" {
		d.Status = model.StatusPending
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO deliveries (id, tenant_id, event_id, endpoint_id, endpoint_url, status, attempts_count, max_attempts, next_retry_at, last_attempt_at, last_error, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.ID, d.TenantID, d.EventID, d.EndpointID, d.EndpointURL, string(d.Status),
		d.AttemptsCount, d.MaxAttempts, d.NextRetryAt, d.LastAttemptAt, d.LastError, d.CreatedAt, d.UpdatedAt,
	)
	return err
}

func (s *Store) GetDelivery(ctx context.Context, id string) (*model.Delivery, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, event_id, endpoint_id, endpoint_url, status, attempts_count, max_attempts, next_retry_at, last_attempt_at, last_error, created_at, updated_at
		 FROM deliveries WHERE id = ?`, id,
	)
	var d model.Delivery
	var status string
	var nextRetry, lastAttempt sql.NullTime
	var lastErr sql.NullString
	if err := row.Scan(
		&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.EndpointURL, &status,
		&d.AttemptsCount, &d.MaxAttempts, &nextRetry, &lastAttempt, &lastErr, &d.CreatedAt, &d.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	d.Status = model.DeliveryStatus(status)
	if nextRetry.Valid {
		d.NextRetryAt = &nextRetry.Time
	}
	if lastAttempt.Valid {
		d.LastAttemptAt = &lastAttempt.Time
	}
	if lastErr.Valid {
		d.LastError = lastErr.String
	}

	// Fetch linked event
	ev, _ := s.GetEvent(ctx, d.EventID)
	d.Event = ev

	return &d, nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, id string, status model.DeliveryStatus, attemptsCount int, nextRetry *time.Time, lastError string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET status = ?, attempts_count = ?, next_retry_at = ?, last_attempt_at = ?, last_error = ?, updated_at = ? WHERE id = ?`,
		string(status), attemptsCount, nextRetry, now, lastError, now, id,
	)
	return err
}

func (s *Store) FetchDueDeliveries(ctx context.Context, limit int) ([]model.Delivery, error) {
	now := time.Now().UTC()
	staleCutoff := now.Add(-120 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Reclaim stale 'sending' deliveries whose worker crashed >60s ago
	_, _ = tx.ExecContext(ctx,
		`UPDATE deliveries SET status = 'pending', updated_at = ? WHERE status = 'sending' AND updated_at <= ?`,
		now, staleCutoff,
	)

	rows, err := tx.QueryContext(ctx,
		`SELECT id, tenant_id, event_id, endpoint_id, endpoint_url, status, attempts_count, max_attempts, next_retry_at, last_attempt_at, last_error, created_at, updated_at
		 FROM deliveries
		 WHERE (status = 'pending' OR (status = 'retrying' AND next_retry_at <= ?))
		 ORDER BY created_at ASC LIMIT ?`, now, limit,
	)
	if err != nil {
		return nil, err
	}

	var list []model.Delivery
	for rows.Next() {
		var d model.Delivery
		var status string
		var nextRetry, lastAttempt sql.NullTime
		var lastErr sql.NullString
		if err := rows.Scan(
			&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.EndpointURL, &status,
			&d.AttemptsCount, &d.MaxAttempts, &nextRetry, &lastAttempt, &lastErr, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			rows.Close()
			return nil, err
		}
		d.Status = model.DeliveryStatus(status)
		if nextRetry.Valid {
			d.NextRetryAt = &nextRetry.Time
		}
		if lastAttempt.Valid {
			d.LastAttemptAt = &lastAttempt.Time
		}
		if lastErr.Valid {
			d.LastError = lastErr.String
		}
		list = append(list, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	// Atomically claim claimed deliveries as 'sending' so concurrent pollers never double-dispatch
	for i := range list {
		if _, err := tx.ExecContext(ctx,
			`UPDATE deliveries SET status = 'sending', updated_at = ? WHERE id = ?`,
			now, list[i].ID,
		); err != nil {
			return nil, err
		}
		list[i].Status = model.StatusSending
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	for i := range list {
		ev, _ := s.GetEvent(ctx, list[i].EventID)
		list[i].Event = ev
	}
	return list, nil
}

func (s *Store) ListDeliveries(ctx context.Context, tenantID string, status model.DeliveryStatus, limit, offset int) ([]model.Delivery, error) {
	if limit <= 0 {
		limit = 50
	}

	query := `SELECT id, tenant_id, event_id, endpoint_id, endpoint_url, status, attempts_count, max_attempts, next_retry_at, last_attempt_at, last_error, created_at, updated_at
	          FROM deliveries WHERE 1=1`
	var args []any

	if tenantID != "" {
		query += " AND tenant_id = ?"
		args = append(args, tenantID)
	}
	if status != "" {
		query += " AND status = ?"
		args = append(args, string(status))
	}
	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []model.Delivery
	for rows.Next() {
		var d model.Delivery
		var st string
		var nextRetry, lastAttempt sql.NullTime
		var lastErr sql.NullString
		if err := rows.Scan(
			&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.EndpointURL, &st,
			&d.AttemptsCount, &d.MaxAttempts, &nextRetry, &lastAttempt, &lastErr, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, err
		}
		d.Status = model.DeliveryStatus(st)
		if nextRetry.Valid {
			d.NextRetryAt = &nextRetry.Time
		}
		if lastAttempt.Valid {
			d.LastAttemptAt = &lastAttempt.Time
		}
		if lastErr.Valid {
			d.LastError = lastErr.String
		}

		list = append(list, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	for i := range list {
		ev, _ := s.GetEvent(ctx, list[i].EventID)
		list[i].Event = ev
	}
	return list, nil
}

// ResetForReplay resets a delivery back to pending status for immediate re-execution.
func (s *Store) ResetForReplay(ctx context.Context, deliveryID string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET status = 'pending', next_retry_at = ?, updated_at = ? WHERE id = ?`,
		now, now, deliveryID,
	)
	return err
}

// ReplayAllDLQ resets all dead_letter deliveries for an endpoint back to pending.
func (s *Store) ReplayAllDLQ(ctx context.Context, endpointID string) (int64, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET status = 'pending', next_retry_at = ?, updated_at = ? WHERE endpoint_id = ? AND status = 'dead_letter'`,
		now, now, endpointID,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// --- Attempts ---

func (s *Store) RecordAttempt(ctx context.Context, att *model.Attempt) error {
	now := time.Now().UTC()
	att.ExecutedAt = now

	headersJSON, _ := json.Marshal(att.RequestHeaders)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO attempts (id, delivery_id, attempt_number, response_status, latency_ms, request_headers, response_body, error_reason, executed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		att.ID, att.DeliveryID, att.AttemptNumber, att.ResponseStatus, att.LatencyMs,
		string(headersJSON), att.ResponseBody, att.ErrorReason, att.ExecutedAt,
	)
	return err
}

func (s *Store) ListAttempts(ctx context.Context, deliveryID string) ([]model.Attempt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, delivery_id, attempt_number, response_status, latency_ms, request_headers, response_body, error_reason, executed_at
		 FROM attempts WHERE delivery_id = ? ORDER BY attempt_number ASC`, deliveryID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []model.Attempt
	for rows.Next() {
		var att model.Attempt
		var headersJSON string
		var respBody, errReason sql.NullString
		if err := rows.Scan(
			&att.ID, &att.DeliveryID, &att.AttemptNumber, &att.ResponseStatus, &att.LatencyMs,
			&headersJSON, &respBody, &errReason, &att.ExecutedAt,
		); err != nil {
			return nil, err
		}
		if respBody.Valid {
			att.ResponseBody = respBody.String
		}
		if errReason.Valid {
			att.ErrorReason = errReason.String
		}
		_ = json.Unmarshal([]byte(headersJSON), &att.RequestHeaders)
		list = append(list, att)
	}
	return list, rows.Err()
}

// --- Aggregate Stats ---

func (s *Store) GetStats(ctx context.Context, tenantID string) (*model.Stats, error) {
	where := ""
	var args []any
	if tenantID != "" {
		where = " WHERE tenant_id = ?"
		args = append(args, tenantID)
	}

	var stats model.Stats
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events"+where, args...).Scan(&stats.TotalEvents)
	if err != nil {
		return nil, err
	}

	delQuery := `SELECT 
		COUNT(*),
		SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END),
		SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
		SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END),
		SUM(CASE WHEN status = 'dead_letter' THEN 1 ELSE 0 END)
		FROM deliveries` + where

	var succ, pend, retr, dl sql.NullInt64
	err = s.db.QueryRowContext(ctx, delQuery, args...).Scan(&stats.TotalDeliveries, &succ, &pend, &retr, &dl)
	if err != nil {
		return nil, err
	}

	if succ.Valid {
		stats.SuccessfulCount = succ.Int64
	}
	if pend.Valid {
		stats.PendingCount = pend.Int64
	}
	if retr.Valid {
		stats.RetryingCount = retr.Int64
	}
	if dl.Valid {
		stats.DeadLetterCount = dl.Int64
	}

	if stats.TotalDeliveries > 0 {
		stats.SuccessRatePct = (float64(stats.SuccessfulCount) / float64(stats.TotalDeliveries)) * 100.0
	}

	// Tenant-scoped average latency when tenantID filter is provided
	latQuery := `SELECT AVG(a.latency_ms) FROM attempts a JOIN deliveries d ON a.delivery_id = d.id` + where
	var avgLat sql.NullFloat64
	_ = s.db.QueryRowContext(ctx, latQuery, args...).Scan(&avgLat)
	if avgLat.Valid {
		stats.AvgLatencyMs = avgLat.Float64
	}

	return &stats, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func sqlNullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}
