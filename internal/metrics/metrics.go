package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	EventsIngested    *prometheus.CounterVec
	DeliveryAttempts  *prometheus.CounterVec
	DeliveryLatency   *prometheus.HistogramVec
	DeadLetterCount   *prometheus.CounterVec
	ActiveWorkers     prometheus.Gauge
	QueueDepth        prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)
	return &Metrics{
		EventsIngested: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "webhook_service",
				Name:      "events_ingested_total",
				Help:      "Total number of events ingested",
			},
			[]string{"tenant_id", "event_type"},
		),
		DeliveryAttempts: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "webhook_service",
				Name:      "delivery_attempts_total",
				Help:      "Total number of webhook dispatch attempts",
			},
			[]string{"tenant_id", "status_class", "outcome"},
		),
		DeliveryLatency: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "webhook_service",
				Name:      "delivery_latency_seconds",
				Help:      "Histogram of webhook dispatch HTTP roundtrip latency in seconds",
				Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			},
			[]string{"tenant_id"},
		),
		DeadLetterCount: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "webhook_service",
				Name:      "dead_letter_total",
				Help:      "Total number of deliveries escalated to dead-letter queue",
			},
			[]string{"tenant_id", "endpoint_id"},
		),
		ActiveWorkers: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "webhook_service",
				Name:      "active_workers",
				Help:      "Current number of active worker goroutines",
			},
		),
		QueueDepth: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "webhook_service",
				Name:      "queue_depth",
				Help:      "Number of pending deliveries waiting for dispatch",
			},
		),
	}
}
