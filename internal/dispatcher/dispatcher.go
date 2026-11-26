package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Chiru9030/multi-tenant-webhook-service/internal/backoff"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/crypto"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/metrics"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/model"
	"github.com/Chiru9030/multi-tenant-webhook-service/internal/storage"
	"github.com/google/uuid"
)

type Config struct {
	WorkerPoolSize           int
	PollInterval             time.Duration
	BatchSize                int
	BackoffConfig            backoff.BackoffConfig
	AllowPrivateDestinations bool // Enables loopback/private targets strictly for local unit/integration tests.
}

type Dispatcher struct {
	store      *storage.Store
	httpClient *http.Client
	cfg        Config
	metrics    *metrics.Metrics

	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
	workQueue chan model.Delivery
}

func isDisallowedAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	return addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() ||
		addr.IsUnspecified()
}

// ValidateDestinationURL checks scheme, host, literal IPs, and DNS resolution against SSRF targets.
func ValidateDestinationURL(raw string, allowPrivate bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", errors.New("invalid webhook destination URL; must be http:// or https://")
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" {
		return "", errors.New("webhook URL must contain a valid hostname")
	}
	if !allowPrivate {
		if host == "localhost" || strings.HasSuffix(host, ".localhost") ||
			strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
			return "", errors.New("private or local webhook destination hostnames are prohibited")
		}
		if ip, err := netip.ParseAddr(host); err == nil {
			if isDisallowedAddr(ip) {
				return "", errors.New("private, loopback, or link-local IP webhook targets are prohibited")
			}
		} else if ips, lookupErr := net.LookupIP(host); lookupErr == nil {
			for _, ip := range ips {
				if addr, ok := netip.AddrFromSlice(ip); ok && isDisallowedAddr(addr) {
					return "", errors.New("webhook destination hostname resolves to prohibited internal network")
				}
			}
		}
	}
	return trimmed, nil
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

	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	if !cfg.AllowPrivateDestinations {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				host = address
			}
			addr, err := netip.ParseAddr(host)
			if err != nil || isDisallowedAddr(addr) {
				return errors.New("ssrf protection blocked connection to non-public IP")
			}
			return nil
		}
	}

	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	queueCap := cfg.WorkerPoolSize * 2
	if queueCap < cfg.BatchSize {
		queueCap = cfg.BatchSize
	}

	return &Dispatcher{
		store: store,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   0, // Enforced per-delivery via request context timeout (endpoint.TimeoutSeconds).
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 3 {
					return errors.New("too many redirects")
				}
				_, err := ValidateDestinationURL(req.URL.String(), cfg.AllowPrivateDestinations)
				return err
			},
		},
		cfg:       cfg,
		metrics:   m,
		stopCh:    make(chan struct{}),
		workQueue: make(chan model.Delivery, queueCap),
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

// Stop gracefully stops all workers idempotently and waits for in-flight deliveries to complete.
func (d *Dispatcher) Stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
	})
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
			available := cap(d.workQueue) - len(d.workQueue)
			if available <= 0 {
				continue
			}
			fetchLimit := d.cfg.BatchSize
			if available < fetchLimit {
				fetchLimit = available
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			due, err := d.store.FetchDueDeliveries(ctx, fetchLimit)
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

func (d *Dispatcher) workerLoop(_ int) {
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
	endpoint, err := d.store.GetEndpoint(ctx, del.EndpointID)
	if err != nil {
		log.Printf("[Dispatcher] Transient error reading endpoint %s: %v", del.EndpointID, err)
		return
	}
	if endpoint == nil || !endpoint.IsActive {
		_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusDeadLetter, del.AttemptsCount, nil, "Endpoint not found or inactive")
		if d.metrics != nil {
			d.metrics.DeadLetterCount.WithLabelValues(del.TenantID, del.EndpointID).Inc()
		}
		return
	}

	var payload []byte
	if del.Event != nil && len(del.Event.Payload) > 0 {
		payload = del.Event.Payload
	} else {
		ev, err := d.store.GetEvent(ctx, del.EventID)
		if err != nil {
			log.Printf("[Dispatcher] Transient error reading event %s: %v", del.EventID, err)
			return
		}
		if ev == nil {
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

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*4))
	bodyPreview := string(bodyBytes)

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		d.handleSuccess(ctx, del, attemptNum, resp.StatusCode, latencyMs, reqHeaders, bodyPreview)
	} else if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		d.handleFailure(ctx, del, attemptNum, resp.StatusCode, latencyMs, reqHeaders, bodyPreview, fmt.Sprintf("HTTP %d", resp.StatusCode))
	} else {
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
		d.metrics.DeliveryAttempts.WithLabelValues(del.TenantID, "success", fmt.Sprintf("%d", statusCode)).Inc()
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

	if d.metrics != nil {
		codeStr := fmt.Sprintf("%d", statusCode)
		if statusCode == 0 {
			codeStr = "network_err"
		}
		d.metrics.DeliveryAttempts.WithLabelValues(del.TenantID, "failed", codeStr).Inc()
		d.metrics.DeliveryLatency.WithLabelValues(del.TenantID).Observe(float64(latencyMs) / 1000.0)
	}

	if attemptNum >= del.MaxAttempts {
		_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusDeadLetter, attemptNum, nil, fmt.Sprintf("Max retries reached (%d). Last error: %s", del.MaxAttempts, reason))
		if d.metrics != nil {
			d.metrics.DeadLetterCount.WithLabelValues(del.TenantID, del.EndpointID).Inc()
		}
		return
	}

	delay := backoff.CalculateNextRetry(attemptNum, d.cfg.BackoffConfig)
	nextRetry := time.Now().UTC().Add(delay)
	_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusRetrying, attemptNum, &nextRetry, reason)
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

	if d.metrics != nil {
		d.metrics.DeliveryAttempts.WithLabelValues(del.TenantID, "permanent_error", fmt.Sprintf("%d", statusCode)).Inc()
		d.metrics.DeadLetterCount.WithLabelValues(del.TenantID, del.EndpointID).Inc()
	}

	_ = d.store.UpdateDeliveryStatus(ctx, del.ID, model.StatusDeadLetter, attemptNum, nil, reason)
}
