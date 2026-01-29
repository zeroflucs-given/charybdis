package generics

import (
	"context"

	"golang.org/x/sync/singleflight"
)

// NewTypedSingleflight is a wrapper that provides a typed single-flight function, ensuring only
// one request for the same key occurs concurrently, and that the result is shared.
func NewTypedSingleflight[K SingleFlightKey, V any](fn func(ctx context.Context, key K) (V, error)) func(ctx context.Context, key K) (V, error) {
	sf := &TypedGroup[K, V]{
		fn: fn,
	}

	return sf.Execute
}

// SingleFlightKey is an interface that implements the requirements of a singleflight group, namely
// to covner to a key
type SingleFlightKey interface {
	String() string
}

type TypedGroup[K SingleFlightKey, V any] struct {
	g  singleflight.Group
	fn func(ctx context.Context, key K) (V, error)
}

func (tg *TypedGroup[K, V]) Execute(ctx context.Context, key K) (V, error) {
	var blank V

	v, err, _ := tg.g.Do(key.String(), func() (interface{}, error) {
		return tg.fn(ctx, key)
	})

	if err != nil {
		return blank, err
	}

	return v.(V), nil
}
