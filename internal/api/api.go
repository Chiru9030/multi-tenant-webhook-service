package api

import (
	"encoding/json"
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

type Server struct {
	store      *storage.Store
	dispatcher *dispatcher.Dispatcher
	metrics    *metrics.Metrics
	webFS      fs.FS
	mux        *http.ServeMux
}

func NewServer(store *storage.Store, d *dispatcher.Dispatcher, m *metrics.Metrics, webFS fs.FS) *Server {
	s := &Server{
		store:      store,
		dispatcher: d,
		metrics:    m,
		webFS:      webFS,
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.corsMiddleware(s.mux)
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) registerRoutes() {
	// Health & Metrics
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
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

	// Deliveries & Replay
	s.mux.HandleFunc("GET /v1/deliveries", s.handleListDeliveries)
	s.mux.HandleFunc("GET /v1/deliveries/{id}", s.handleGetDelivery)
	s.mux.HandleFunc("POST /v1/deliveries/{id}/replay", s.handleReplayDelivery)
	s.mux.HandleFunc("POST /v1/endpoints/{id}/replay-all", s.handleReplayAllDLQ)

	// Stats
	s.mux.HandleFunc("GET /v1/stats", s.handleGetStats)

	// Dashboard UI
	if s.webFS != nil {
		fileServer := http.FileServer(http.FS(s.webFS))
		s.mux.Handle("GET /dashboard/", http.StripPrefix("/dashboard/", fileServer))
		s.mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				http.Redirect(w, r, "/dashboard/", http.StatusFound)
				return
			}
			http.NotFound(w, r)
		})
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "healthy",
		"service": "relay-webhook-service",
	})
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		writeError(w, http.StatusBadRequest, "Invalid request payload; 'name' is required")
		return
	}

	tenant := &model.Tenant{
		ID:   "ten_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12],
		Name: req.Name,
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
	if err != nil || tenant == nil {
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.URL) == "" {
		writeError(w, http.StatusBadRequest, "Invalid payload; 'url' is required")
		return
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
		URL:            req.URL,
		Description:    req.Description,
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

	var req struct {
		EventType      string          `json:"event_type"`
		Payload        json.RawMessage `json:"payload"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.EventType) == "" || len(req.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "Invalid payload: 'event_type' and 'payload' are required")
		return
	}

	// Read optional Idempotency-Key header if not in JSON body
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}

	event := &model.Event{
		ID:             "evt_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:12],
		TenantID:       tenantID,
		EventType:      req.EventType,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
	}

	isNew, err := s.store.CreateEvent(r.Context(), event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to persist event: "+err.Error())
		return
	}

	if !isNew {
		// Event was already ingested previously under the same idempotency key
		writeJSON(w, http.StatusOK, map[string]any{
			"message": "Event already ingested (idempotent duplicate)",
			"event":   event,
		})
		return
	}

	if s.metrics != nil {
		s.metrics.EventsIngested.WithLabelValues(tenantID, req.EventType).Inc()
	}

	// Fanout: match active registered endpoints
	endpoints, err := s.store.FindMatchingEndpoints(r.Context(), tenantID, req.EventType)
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
		if err := s.store.CreateDelivery(r.Context(), &del); err == nil {
			del.Event = event
			createdDeliveries = append(createdDeliveries, del)
		}
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
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

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
		"message":       fmt.Sprintf("Scheduled %d dead-letter deliveries for replay", count),
		"replayed_count": count,
		"endpoint_id":   endpointID,
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
		"error":   msg,
		"status":  code,
	})
}
