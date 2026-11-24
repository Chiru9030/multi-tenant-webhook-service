package backoff

import (
	"math"
	"math/rand"
	"time"
)

// BackoffConfig defines exponential backoff with full jitter parameters.
type BackoffConfig struct {
	BaseInterval time.Duration
	MaxInterval  time.Duration
	Multiplier   float64
}

var DefaultConfig = BackoffConfig{
	BaseInterval: 3 * time.Second,
	MaxInterval:  1 * time.Hour,
	Multiplier:   2.5,
}

// CalculateNextRetry calculates exponential backoff with Full Jitter:
// Sleep = rand(0, min(MaxInterval, BaseInterval * Multiplier^(attempt-1)))
func CalculateNextRetry(attempt int, cfg BackoffConfig) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	// Exponential factor: base * multiplier^(attempt-1)
	factor := math.Pow(cfg.Multiplier, float64(attempt-1))
	maxSleepSec := float64(cfg.BaseInterval.Seconds()) * factor

	if maxSleepSec > cfg.MaxInterval.Seconds() {
		maxSleepSec = cfg.MaxInterval.Seconds()
	}

	// Full Jitter: uniformly distributed between 0 and maxSleepSec
	jitterSec := rand.Float64() * maxSleepSec
	if jitterSec < 1.0 {
		jitterSec = 1.0 // minimum 1 second
	}

	return time.Duration(jitterSec * float64(time.Second))
}
