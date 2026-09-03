package tables

import "context"

// execGate limits how many statement executions run at once. Bulk operations prepare
// every statement in parallel (hooks, query building, binding) and only queue on the
// gate for the round-trip to the cluster. A nil gate imposes no limit.
type execGate chan struct{}

// enter blocks until an execution slot is free, or the context ends.
func (g execGate) enter(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case g <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// exit releases an execution slot.
func (g execGate) exit() {
	if g == nil {
		return
	}
	<-g
}
