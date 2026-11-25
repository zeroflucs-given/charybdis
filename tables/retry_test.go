package tables

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func fixedDelay(d time.Duration) func(int) time.Duration {
	return func(int) time.Duration { return d }
}

func TestRetryWriteTimeoutsWaitsBetweenAttempts(t *testing.T) {
	var requested []int
	delayFor := func(failedAttempts int) time.Duration {
		requested = append(requested, failedAttempts)
		return 10 * time.Millisecond
	}

	attempts := 0
	st := time.Now()
	err := retryWriteTimeoutsUsing(context.Background(), zap.NewNop(), "insert", delayFor, func() error {
		attempts++
		if attempts <= 3 {
			return &gocql.RequestErrWriteTimeout{WriteType: "CAS"}
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 4, attempts)
	require.Equal(t, []int{1, 2, 3}, requested)
	require.GreaterOrEqual(t, time.Since(st), 30*time.Millisecond)
}

func TestRetryWriteTimeoutsReturnsOtherErrorsUnchanged(t *testing.T) {
	errUnavailable := errors.New("unavailable")
	delayFor := func(int) time.Duration {
		t.Fatal("should not wait before returning a non-retryable error")
		return 0
	}

	attempts := 0
	err := retryWriteTimeoutsUsing(context.Background(), zap.NewNop(), "insert", delayFor, func() error {
		attempts++
		return errUnavailable
	})

	require.Equal(t, errUnavailable, err)
	require.Equal(t, 1, attempts)
}

func TestRetryWriteTimeoutsWrapsLastTimeout(t *testing.T) {
	timeouts := []error{
		&gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 1},
		&gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 2},
		context.DeadlineExceeded,
	}

	attempts := 0
	err := retryWriteTimeoutsUsing(context.Background(), zap.NewNop(), "update", fixedDelay(0), func() error {
		attempts++
		return timeouts[attempts-1]
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	var wto *gocql.RequestErrWriteTimeout
	require.ErrorAs(t, err, &wto)
	require.Same(t, timeouts[1], wto)
	require.ErrorContains(t, err, "Attempts=3")
}

func TestRetryWriteTimeoutsStopsWhenContextEndsDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(20*time.Millisecond, cancel)

	attempts := 0
	err := retryWriteTimeoutsUsing(ctx, zap.NewNop(), "upsert", fixedDelay(time.Hour), func() error {
		attempts++
		return &gocql.RequestErrWriteTimeout{WriteType: "SIMPLE"}
	})

	require.ErrorIs(t, err, context.Canceled)
	var wto *gocql.RequestErrWriteTimeout
	require.ErrorAs(t, err, &wto)
	require.Equal(t, 1, attempts)
	require.ErrorContains(t, err, "Attempts=1")
}

func TestRetryWriteTimeoutsStopsWhenContextEndsAsWaitEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := 0
	err := retryWriteTimeoutsUsing(ctx, zap.NewNop(), "upsert", fixedDelay(0), func() error {
		attempts++
		if attempts == 2 {
			cancel()
		}
		return &gocql.RequestErrWriteTimeout{WriteType: "SIMPLE"}
	})

	require.ErrorIs(t, err, context.Canceled)
	var wto *gocql.RequestErrWriteTimeout
	require.ErrorAs(t, err, &wto)
	require.Equal(t, 2, attempts)
	require.ErrorContains(t, err, "Attempts=2")
}

func TestWriteRetryDelayStaysUnderCeiling(t *testing.T) {
	ceilings := map[int]time.Duration{
		1:  50 * time.Millisecond,
		2:  100 * time.Millisecond,
		3:  200 * time.Millisecond,
		4:  400 * time.Millisecond,
		5:  800 * time.Millisecond,
		6:  time.Second,
		50: time.Second,
	}

	for failedAttempts, ceiling := range ceilings {
		var longest time.Duration
		for range 1000 {
			delay := writeRetryDelay(failedAttempts)
			require.GreaterOrEqual(t, delay, time.Duration(0))
			require.Less(t, delay, ceiling, "failed attempts %d", failedAttempts)
			longest = max(longest, delay)
		}
		require.Greater(t, longest, ceiling/2, "failed attempts %d", failedAttempts)
	}
}
