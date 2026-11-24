package backoff

import (
	"testing"
	"time"
)

func TestCalculateNextRetry(t *testing.T) {
	cfg := BackoffConfig{
		BaseInterval: 2 * time.Second,
		MaxInterval:  30 * time.Second,
		Multiplier:   2.0,
	}

	for attempt := 1; attempt <= 10; attempt++ {
		duration := CalculateNextRetry(attempt, cfg)
		if duration < 1*time.Second {
			t.Errorf("duration %v is below minimum 1s for attempt %d", duration, attempt)
		}
		if duration > cfg.MaxInterval {
			t.Errorf("duration %v exceeded MaxInterval %v for attempt %d", duration, cfg.MaxInterval, attempt)
		}
	}
}
