package tables

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

const (
	writeRetryBaseDelay = 50 * time.Millisecond
	writeRetryMaxDelay  = time.Second
)

// retryWriteTimeouts retries write timeouts after a jittered delay, so writers that timed out together don't retry in lockstep.
func retryWriteTimeouts(ctx context.Context, logger *zap.Logger, operation string, attempt func() error) error {
	return retryWriteTimeoutsUsing(ctx, logger, operation, writeRetryDelay, attempt)
}

// retryWriteTimeoutsUsing runs attempt until it succeeds, fails with anything but a write timeout, or ctx ends.
func retryWriteTimeoutsUsing(ctx context.Context, logger *zap.Logger, operation string, delayFor func(failedAttempts int) time.Duration, attempt func() error) error {
	st := time.Now()
	var lastTimeout error

	for attempts := 1; ; attempts++ {
		err := attempt()
		if err == nil {
			return nil
		}

		var wto *gocql.RequestErrWriteTimeout
		if !errors.As(err, &wto) {
			if lastTimeout == nil {
				return err
			}
			return fmt.Errorf("retrying %v write timeouts [Attempts=%v]: %w (last write timeout: %w)", operation, attempts, err, lastTimeout)
		}
		lastTimeout = err

		delay := delayFor(attempts)
		logger.Debug("retrying write timeout",
			zap.String("operation", operation),
			zap.Int("attempt", attempts),
			zap.Duration("delay", delay),
			zap.String("consistency", wto.Consistency.String()),
			zap.Int("received", wto.Received),
			zap.Int("block_for", wto.BlockFor),
			zap.String("write_type", wto.WriteType),
			zap.Duration("elapsed", time.Since(st)),
		)

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		timer.Stop()

		// Both cases can be ready at once, and select picks either.
		if errCtx := ctx.Err(); errCtx != nil {
			return fmt.Errorf("retrying %v write timeouts [Attempts=%v]: %w (last write timeout: %w)", operation, attempts, errCtx, lastTimeout)
		}
	}
}

// writeRetryDelay returns a random delay below a ceiling that doubles per failed attempt from writeRetryBaseDelay to writeRetryMaxDelay.
func writeRetryDelay(failedAttempts int) time.Duration {
	ceiling := writeRetryBaseDelay
	for i := 1; i < failedAttempts && ceiling < writeRetryMaxDelay; i++ {
		ceiling *= 2
	}
	return rand.N(min(ceiling, writeRetryMaxDelay)) // #nosec G404 - Spreads retries and does not require cryptographic security
}
