package dispatcher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Chiru9030/multi-tenant-webhook-service/internal/backoff"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/crypto"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/metrics"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/model"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/storage"
	"github.com/google/uuid"
)

type Config struct {
	WorkerPoolSize int
	PollInterval   time.Duration
	BatchSize      int
	BackoffConfig  backoff.BackoffConfig
}

type Dispatcher struct {
	store      *storage.Store
	httpClient *http.Client
	cfg        Config
	metrics    *metrics.Metrics
	stopCh     chan struct{}
	wg         sync.WaitGroup
	workQueue  chan model.Delivery
}

func NewDispatcher(store *storage.Store, m *metrics.Metrics, cfg Config) *Dispatcher {
	if cfg.WorkerPoolSize <= 0 {
		cfg.WorkerPoolSize = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 25
	}
	if cfg.BackoffConfig.BaseInterval == 0 {
		cfg.BackoffConfig = backoff.DefaultConfig
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	return &Dispatcher{
		store: store,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		cfg:       cfg,
		metrics:   m,
		stopCh:    make(chan struct{}),
		workQueue: make(chan model.Delivery, cfg.WorkerPoolSize*2),
	}
}

// Start spawns the poller and worker pool.
func (d *Dispatcher) Start() {
	for i := 0; i < d.cfg.WorkerPoolSize; i++ {
		d.wg.Add(1)
		go d.workerLoop(i)
	}

	d.wg.Add(1)
	go d.pollLoop()
}

// Stop gracefully stops all workers and waits for in-flight deliveries to complete.
func (d *Dispatcher) Stop() {
	close(d.stopCh)
	d.wg.Wait()
}

func (d *Dispatcher) pollLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			close(d.workQueue)
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			due, err := d.store.FetchDueDeliveries(ctx, d.cfg.BatchSize)
			cancel()

			if err != nil {
				log.Printf("[Dispatcher] Error fetching due deliveries: %v", err)
				continue
			}

			if d.metrics != nil {
				d.metrics.QueueDepth.Set(float64(len(due)))
			}

			for _, del := range due {
				select {
				case <-d.stopCh:
					close(d.workQueue)
					return
				case d.workQueue <- del:
				}
			}
		}
	}
}

func (d *Dispatcher) workerLoop(workerID int) {
	defer d.wg.Done()

	for del := range d.workQueue {
		if d.metrics != nil {
			d.metrics.ActiveWorkers.Inc()
		}

		d.executeDelivery(context.Background(), del)

		if d.metrics != nil {
			d.metrics.ActiveWorkers.Dec()
		}
	}
}

func (d *Dispatcher) executeDelivery(ctx context.Context, del model.Delivery) {
	// 1. Fetch endpoint info for secret & config
	endpoint, err := d.store.GetEndpoint(ctx, del.EndpointID)
	if err != nil || endpoint == nil || !endpoint.IsActive {
		_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusDeadLetter, del.AttemptsCount, nil, "Endpoint not found or inactive")
		if d.metrics != nil {
			d.metrics.DeadLetterCount.WithLabelValues(del.TenantID, del.EndpointID).Inc()
		}
		return
	}

	// 2. Fetch event payload if not loaded
	var payload []byte
	if del.Event != nil && len(del.Event.Payload) > 0 {
		payload = del.Event.Payload
	} else {
		ev, err := d.store.GetEvent(ctx, del.EventID)
		if err != nil || ev == nil {
			_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusDeadLetter, del.AttemptsCount, nil, "Event payload missing")
			return
		}
		payload = ev.Payload
	}

	attemptNum := del.AttemptsCount + 1
	timeout := time.Duration(endpoint.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	nowUnix := time.Now().Unix()
	signature := crypto.SignPayload(nowUnix, payload, endpoint.SecretKey)

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint.URL, bytes.NewReader(payload))
	if err != nil {
		d.handleFailure(ctx, del, attemptNum, 0, 0, nil, "", fmt.Sprintf("invalid request: %v", err))
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Relay-Webhook-Service/1.0")
	req.Header.Set("X-Webhook-ID", del.ID)
	req.Header.Set("X-Webhook-Event", del.EventID)
	req.Header.Set("X-Webhook-Timestamp", fmt.Sprintf("%d", nowUnix))
	req.Header.Set("X-Webhook-Signature", signature)

	start := time.Now()
	resp, reqErr := d.httpClient.Do(req)
	latency := time.Since(start)
	latencyMs := latency.Milliseconds()

	reqHeaders := map[string]string{
		"Content-Type":        "application/json",
		"X-Webhook-ID":        del.ID,
		"X-Webhook-Timestamp": fmt.Sprintf("%d", nowUnix),
		"X-Webhook-Signature": signature,
	}

	if reqErr != nil {
		d.handleFailure(ctx, del, attemptNum, 0, latencyMs, reqHeaders, "", reqErr.Error())
		return
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*4)) // limit 4KB preview
	bodyPreview := string(bodyBytes)

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		// SUCCESS!
		d.handleSuccess(ctx, del, attemptNum, resp.StatusCode, latencyMs, reqHeaders, bodyPreview)
	} else if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		// Transient retryable failure (rate limited or server error)
		d.handleFailure(ctx, del, attemptNum, resp.StatusCode, latencyMs, reqHeaders, bodyPreview, fmt.Sprintf("HTTP %d", resp.StatusCode))
	} else {
		// 4xx client errors (400, 401, 404) are permanent; no point in retrying bad endpoint URL/auth
		d.handlePermanentFailure(ctx, del, attemptNum, resp.StatusCode, latencyMs, reqHeaders, bodyPreview, fmt.Sprintf("HTTP %d (permanent client error)", resp.StatusCode))
	}
}

func (d *Dispatcher) handleSuccess(ctx context.Context, del model.Delivery, attemptNum, statusCode int, latencyMs int64, headers map[string]string, body string) {
	_ = d.store.RecordAttempt(ctx, &model.Attempt{
		ID:             uuid.New().String(),
		DeliveryID:     del.ID,
		AttemptNumber:  attemptNum,
		ResponseStatus: statusCode,
		LatencyMs:      latencyMs,
		RequestHeaders: headers,
		ResponseBody:   body,
		ErrorReason:    "",
	})

	_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusSuccess, attemptNum, nil, "")

	if d.metrics != nil {
		d.metrics.DeliveryAttempts.WithLabelValues(del.TenantID, "2xx", "success").Inc()
		d.metrics.DeliveryLatency.WithLabelValues(del.TenantID).Observe(float64(latencyMs) / 1000.0)
	}
}

func (d *Dispatcher) handleFailure(ctx context.Context, del model.Delivery, attemptNum, statusCode int, latencyMs int64, headers map[string]string, body, reason string) {
	_ = d.store.RecordAttempt(ctx, &model.Attempt{
		ID:             uuid.New().String(),
		DeliveryID:     del.ID,
		AttemptNumber:  attemptNum,
		ResponseStatus: statusCode,
		LatencyMs:      latencyMs,
		RequestHeaders: headers,
		ResponseBody:   body,
		ErrorReason:    reason,
	})

	if attemptNum >= del.MaxAttempts {
		// DLQ escalation
		_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusDeadLetter, attemptNum, nil, reason)
		if d.metrics != nil {
			d.metrics.DeadLetterCount.WithLabelValues(del.TenantID, del.EndpointID).Inc()
			d.metrics.DeliveryAttempts.WithLabelValues(del.TenantID, "dlq", "failed").Inc()
		}
	} else {
		// Schedule retry with exponential backoff and full jitter
		nextDelay := backoff.CalculateNextRetry(attemptNum, d.cfg.BackoffConfig)
		nextRetryAt := time.Now().UTC().Add(nextDelay)
		_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusRetrying, attemptNum, &nextRetryAt, reason)

		if d.metrics != nil {
			statusClass := "5xx"
			if statusCode == 429 {
				statusClass = "429"
			} else if statusCode == 0 {
				statusClass = "network_err"
			}
			d.metrics.DeliveryAttempts.WithLabelValues(del.TenantID, statusClass, "retry_scheduled").Inc()
		}
	}
}

func (d *Dispatcher) handlePermanentFailure(ctx context.Context, del model.Delivery, attemptNum, statusCode int, latencyMs int64, headers map[string]string, body, reason string) {
	_ = d.store.RecordAttempt(ctx, &model.Attempt{
		ID:             uuid.New().String(),
		DeliveryID:     del.ID,
		AttemptNumber:  attemptNum,
		ResponseStatus: statusCode,
		LatencyMs:      latencyMs,
		RequestHeaders: headers,
		ResponseBody:   body,
		ErrorReason:    reason,
	})

	_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusDeadLetter, attemptNum, nil, reason)
	if d.metrics != nil {
		d.metrics.DeadLetterCount.WithLabelValues(del.TenantID, del.EndpointID).Inc()
		d.metrics.DeliveryAttempts.WithLabelValues(del.TenantID, "4xx", "permanent_failure").Inc()
	}
}
