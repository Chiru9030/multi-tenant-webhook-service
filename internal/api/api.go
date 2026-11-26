package api

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"

	"github.com/Chiru9030/multi-tenant-webhook-service/internal/crypto"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/dispatcher"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/metrics"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/model"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/storage"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	maxRequestBodyBytes = 1 << 20 // 1 MiB cap on tenant, endpoint, and event payloads
	maxQueryLimit       = 500
)

type Server struct {
	store                    *storage.Store
	dispatcher               *dispatcher.Dispatcher
	metrics                  *metrics.Metrics
	webFS                    fs.FS
	adminAPIKey              string
	allowedOrigins           []string
	allowPrivateDestinations bool
	mux                      *http.ServeMux
}

type ServerOptions struct {
	AdminAPIKey              string
	AllowedOrigins           string
	AllowPrivateDestinations bool
}

func NewServer(store *storage.Store, d *dispatcher.Dispatcher, m *metrics.Metrics, webFS fs.FS) *Server {
	return NewServerWithOptions(store, d, m, webFS, ServerOptions{AllowPrivateDestinations: false})
}

func NewServerWithConfig(store *storage.Store, d *dispatcher.Dispatcher, m *metrics.Metrics, webFS fs.FS, adminAPIKey, allowedOrigin string) *Server {
	return NewServerWithOptions(store, d, m, webFS, ServerOptions{
		AdminAPIKey:    adminAPIKey,
		AllowedOrigins: allowedOrigin,
	})
}

func NewServerWithOptions(store *storage.Store, d *dispatcher.Dispatcher, m *metrics.Metrics, webFS fs.FS, opts ServerOptions) *Server {
	var origins []string
	for _, part := range strings.Split(opts.AllowedOrigins, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			origins = append(origins, trimmed)
		}
	}
	s := &Server{
		store:                    store,
		dispatcher:               d,
		metrics:                  m,
		webFS:                    webFS,
		adminAPIKey:              opts.AdminAPIKey,
		allowedOrigins:           origins,
		allowPrivateDestinations: opts.AllowPrivateDestinations,
		mux:                      http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.securityAndAuthMiddleware(s.mux)
}

func constantTimeMatches(provided, expected string) bool {
	if provided == "" || expected == "" {
		return false
	}
	pHash := sha256.Sum256([]byte(provided))
	eHash := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(pHash[:], eHash[:]) == 1
}

func extractBearer(header string) string {
	trimmed := strings.TrimSpace(header)
	if len(trimmed) > 7 && strings.EqualFold(trimmed[:7], "bearer ") {
		return strings.TrimSpace(trimmed[7:])
	}
	return ""
}

func (s *Server) resolveCORSOrigin(reqOrigin string) string {
	if len(s.allowedOrigins) == 0 {
		return "*"
	}
	for _, allowed := range s.allowedOrigins {
		if allowed == "*" {
			return "*"
		}
		if reqOrigin != "" && strings.EqualFold(allowed, reqOrigin) {
			return allowed
		}
	}
	if reqOrigin == "" && len(s.allowedOrigins) == 1 {
		return s.allowedOrigins[0]
	}
	return ""
}

func (s *Server) securityAndAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")

		if len(s.allowedOrigins) > 0 && s.allowedOrigins[0] != "*" {
			w.Header().Add("Vary", "Origin")
		}
		if corsOrigin := s.resolveCORSOrigin(strings.TrimSpace(r.Header.Get("Origin"))); corsOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", corsOrigin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, Idempotency-Key")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Protect control-plane and tenant ingestion routes (/v1/*); keep /healthz, /readyz, /metrics, and /dashboard shell public.
		isProtected := strings.HasPrefix(r.URL.Path, "/v1/")
		if s.adminAPIKey != "" && isProtected {
			bearer := extractBearer(r.Header.Get("Authorization"))
			apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
			if !constantTimeMatches(apiKey, s.adminAPIKey) && !constantTimeMatches(bearer, s.adminAPIKey) {
				writeError(w, http.StatusUnauthorized, "Unauthorized: invalid or missing API key")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func decodeLimitedJSON(w http.ResponseWriter, r *http.Request, dst any) (status int, msg string) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return http.StatusRequestEntityTooLarge, "Request body exceeds 1 MiB limit"
		}
		return http.StatusBadRequest, "Invalid JSON payload"
	}
	return 0, ""
}

func (s *Server) registerRoutes() {
	// Health & Metrics
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("GET /metrics", promhttp.Handler())

	// Tenants
	s.mux.HandleFunc("POST /v1/tenants", s.handleCreateTenant)
	s.mux.HandleFunc("GET /v1/tenants", s.handleListTenants)
	s.mux.HandleFunc("GET /v1/tenants/{id}", s.handleGetTenant)

	// Endpoints
	s.mux.HandleFunc("POST /v1/tenants/{tenant_id}/endpoints", s.handleCreateEndpoint)
	s.mux.HandleFunc("GET /v1/tenants/{tenant_id}/endpoints", s.handleListEndpoints)

	// Events Ingestion
	s.mux.HandleFunc("POST /v1/tenants/{tenant_id}/events", s.handleIngestEvent)

	// Deliveries & Replays
	s.mux.HandleFunc("GET /v1/deliveries", s.handleListDeliveries)
	s.mux.HandleFunc("GET /v1/deliveries/{id}", s.handleGetDelivery)
	s.mux.HandleFunc("POST /v1/deliveries/{id}/replay", s.handleReplayDelivery)
	s.mux.HandleFunc("POST /v1/endpoints/{id}/replay-all", s.handleReplayAllDLQ)

	// Stats
	s.mux.HandleFunc("GET /v1/stats", s.handleGetStats)

	// Dashboard UI (static embed.FS)
	if s.webFS != nil {
		fileServer := http.FileServer(http.FS(s.webFS))
		s.mux.Handle("GET /dashboard/", http.StripPrefix("/dashboard/", fileServer))
		s.mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				http.Redirect(w, r, "/dashboard/", http.StatusFound)
				return
			}
			writeError(w, http.StatusNotFound, "endpoint not found")
		})
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "healthy",
		"service": "relay-webhook-service",
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "ready",
		"database": "sqlite_wal_ok",
	})
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if status, msg := decodeLimitedJSON(w, r, &req); status != 0 {
		writeError(w, status, msg)
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "Invalid request payload; 'name' is required")
		return
	}

	tenant := &model.Tenant{
		ID:   "ten_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12],
		Name: name,
	}

	if err := s.store.CreateTenant(r.Context(), tenant); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to create tenant: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, tenant)
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := s.store.ListTenants(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tenants == nil {
		tenants = []model.Tenant{}
	}
	writeJSON(w, http.StatusOK, tenants)
}

func (s *Server) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenant, err := s.store.GetTenant(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tenant == nil {
		writeError(w, http.StatusNotFound, "Tenant not found")
		return
	}
	writeJSON(w, http.StatusOK, tenant)
}

func (s *Server) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	tenant, err := s.store.GetTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tenant == nil {
		writeError(w, http.StatusNotFound, "Tenant not found")
		return
	}

	var req struct {
		URL            string   `json:"url"`
		Description    string   `json:"description"`
		EventTypes     []string `json:"event_types"`
		MaxRetries     int      `json:"max_retries"`
		TimeoutSeconds int      `json:"timeout_seconds"`
	}
	if status, msg := decodeLimitedJSON(w, r, &req); status != 0 {
		writeError(w, status, msg)
		return
	}
	cleanURL, err := dispatcher.ValidateDestinationURL(req.URL, s.allowPrivateDestinations)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.MaxRetries < 0 || req.MaxRetries > 20 {
		writeError(w, http.StatusBadRequest, "max_retries must be between 0 and 20")
		return
	}
	if req.MaxRetries == 0 {
		req.MaxRetries = 5
	}
	if req.TimeoutSeconds < 0 || req.TimeoutSeconds > 60 {
		writeError(w, http.StatusBadRequest, "timeout_seconds must be between 1 and 60")
		return
	}
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = 10
	}

	secretKey, err := crypto.GenerateSecretKey()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to generate secret key")
		return
	}

	if len(req.EventTypes) == 0 {
		req.EventTypes = []string{"*"}
	}

	ep := &model.Endpoint{
		ID:             "ep_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12],
		TenantID:       tenantID,
		URL:            cleanURL,
		Description:    strings.TrimSpace(req.Description),
		SecretKey:      secretKey,
		EventTypes:     req.EventTypes,
		IsActive:       true,
		MaxRetries:     req.MaxRetries,
		TimeoutSeconds: req.TimeoutSeconds,
	}

	if err := s.store.CreateEndpoint(r.Context(), ep); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to register endpoint: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, ep)
}

func (s *Server) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	endpoints, err := s.store.ListEndpointsByTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if endpoints == nil {
		endpoints = []model.Endpoint{}
	}
	writeJSON(w, http.StatusOK, endpoints)
}

func (s *Server) handleIngestEvent(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	tenant, err := s.store.GetTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tenant == nil {
		writeError(w, http.StatusNotFound, "Tenant not found")
		return
	}

	var req struct {
		EventType      string          `json:"event_type"`
		Payload        json.RawMessage `json:"payload"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	if status, msg := decodeLimitedJSON(w, r, &req); status != 0 {
		writeError(w, status, msg)
		return
	}
	trimmedPayload := bytes.TrimSpace(req.Payload)
	if strings.TrimSpace(req.EventType) == "" || len(trimmedPayload) == 0 || bytes.Equal(trimmedPayload, []byte("null")) {
		writeError(w, http.StatusBadRequest, "Invalid payload: 'event_type' and non-null 'payload' are required")
		return
	}

	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}

	event := &model.Event{
		ID:             "evt_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12],
		TenantID:       tenantID,
		EventType:      strings.TrimSpace(req.EventType),
		IdempotencyKey: strings.TrimSpace(req.IdempotencyKey),
		Payload:        trimmedPayload,
	}

	isNew, err := s.store.CreateEvent(r.Context(), event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to persist event: "+err.Error())
		return
	}

	if !isNew {
		writeJSON(w, http.StatusOK, map[string]any{
			"message": "Event already ingested (idempotent duplicate)",
			"event":   event,
		})
		return
	}

	if s.metrics != nil {
		s.metrics.EventsIngested.WithLabelValues(tenantID, event.EventType).Inc()
	}

	endpoints, err := s.store.FindMatchingEndpoints(r.Context(), tenantID, event.EventType)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to resolve endpoints: "+err.Error())
		return
	}

	var createdDeliveries []model.Delivery
	for _, ep := range endpoints {
		del := model.Delivery{
			ID:          "del_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12],
			TenantID:    tenantID,
			EventID:     event.ID,
			EndpointID:  ep.ID,
			EndpointURL: ep.URL,
			Status:      model.StatusPending,
			MaxAttempts: ep.MaxRetries,
		}
		if err := s.store.CreateDelivery(r.Context(), &del); err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to schedule webhook delivery: "+err.Error())
			return
		}
		del.Event = event
		createdDeliveries = append(createdDeliveries, del)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"event":            event,
		"dispatched_count": len(createdDeliveries),
		"deliveries":       createdDeliveries,
	})
}

func (s *Server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	statusStr := r.URL.Query().Get("status")

	limit := 50
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if parsed > maxQueryLimit {
			parsed = maxQueryLimit
		}
		limit = parsed
	}

	offset := 0
	if rawOffset := strings.TrimSpace(r.URL.Query().Get("offset")); rawOffset != "" {
		parsed, err := strconv.Atoi(rawOffset)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		offset = parsed
	}

	deliveries, err := s.store.ListDeliveries(r.Context(), tenantID, model.DeliveryStatus(statusStr), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if deliveries == nil {
		deliveries = []model.Delivery{}
	}
	writeJSON(w, http.StatusOK, deliveries)
}

func (s *Server) handleGetDelivery(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	delivery, err := s.store.GetDelivery(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if delivery == nil {
		writeError(w, http.StatusNotFound, "Delivery not found")
		return
	}

	attempts, _ := s.store.ListAttempts(r.Context(), id)

	writeJSON(w, http.StatusOK, map[string]any{
		"delivery": delivery,
		"attempts": attempts,
	})
}

func (s *Server) handleReplayDelivery(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	delivery, err := s.store.GetDelivery(r.Context(), id)
	if err != nil || delivery == nil {
		writeError(w, http.StatusNotFound, "Delivery not found")
		return
	}

	if err := s.store.ResetForReplay(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to schedule replay: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message":     "Delivery successfully scheduled for immediate replay",
		"delivery_id": id,
		"status":      model.StatusPending,
	})
}

func (s *Server) handleReplayAllDLQ(w http.ResponseWriter, r *http.Request) {
	endpointID := r.PathValue("id")
	count, err := s.store.ReplayAllDLQ(r.Context(), endpointID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to replay DLQ: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message":        fmt.Sprintf("Scheduled %d dead-letter deliveries for replay", count),
		"replayed_count": count,
		"endpoint_id":    endpointID,
	})
}

func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	stats, err := s.store.GetStats(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func writeJSON(w http.ResponseWriter, code int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  msg,
		"status": code,
	})
}
