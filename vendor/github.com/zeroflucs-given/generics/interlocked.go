package generics

import (
	"context"
	"sync"
)

// Resolver is a type that lets us do hard work once, and share the results across
// multiple callers. Also handles parallelism.
type Resolver[K comparable, V any] struct {
	mtx      sync.Mutex               // Internal master mutex
	resolved map[K]*resolverResult[V] // Resolved state
	WorkFn   ResolverWorkFn[K, V]     // Work function
}

// resolverResult is our internal state tracker for the particular work
// for a given key.
type resolverResult[V any] struct {
	Complete bool
	Mutex    sync.Mutex
	Value    V
	Error    error
}

// ResolverWorkFn is a function that resolvers use to do the underlying work
type ResolverWorkFn[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Get fetches the resolved state of the object
func (r *Resolver[K, V]) Get(ctx context.Context, key K) (V, error) {
	// Brief global lock to perform map operations
	r.mtx.Lock()
	if r.resolved == nil {
		r.resolved = make(map[K]*resolverResult[V])
	}
	target, ok := r.resolved[key]
	if !ok {
		target = &resolverResult[V]{}
		r.resolved[key] = target
	}
	r.mtx.Unlock()

	target.Mutex.Lock()
	defer target.Mutex.Unlock()

	// Lock the object
	if !target.Complete {
		value, err := r.WorkFn(ctx, key)
		target.Complete = true
		target.Value = value
		target.Error = err
	}

	return target.Value, target.Error
}
