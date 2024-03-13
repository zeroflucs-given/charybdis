package generics

import (
	"context"
	"fmt"
	"sync/atomic"
)

// CachedTask holds a packet of work that is executed once, and the result retrieved many times.
type CachedTask[T any] struct {
	value *T
	err   error
	proc  func(ctx context.Context) (*T, error)
	flag  atomic.Bool
	ch    chan struct{}
}

// ExecuteOnce creates a new CachedTask for which the work is provided by fn.
func ExecuteOnce[T any](fn func(ctx context.Context) (*T, error)) CachedTask[T] {
	return CachedTask[T]{proc: fn, ch: make(chan struct{})}
}

// Get executes the task using the current context, or returns the value if it has already been calculated.
func (t *CachedTask[T]) Get(ctx context.Context) (value *T, err error) {
	if t.flag.CompareAndSwap(false, true) {
		// Unblock other waiters if something bad happens.
		defer func() {
			if r := recover(); r != nil {
				t.value = nil

				if err, ok := r.(error); ok {
					t.err = fmt.Errorf("panic recovered: %w", err)
				} else {
					t.err = fmt.Errorf("panic recovered: %v", r)
				}

				value = t.value
				err = t.err
			}
			close(t.ch)
		}()

		t.value, t.err = t.proc(ctx)
	} else {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.ch: // NB: A closed channel will always yield the zero value.
			break
		}
	}

	return t.value, t.err
}
