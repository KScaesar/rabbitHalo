package rabbitHalo

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

func retry(retryMaxTime time.Duration, fn func() error) error {
	cfg := backoff.NewExponentialBackOff()
	cfg.InitialInterval = 500 * time.Millisecond
	cfg.Multiplier = 1.5
	cfg.RandomizationFactor = 0.5
	cfg.MaxElapsedTime = retryMaxTime
	return backoff.Retry(fn, cfg)
}
