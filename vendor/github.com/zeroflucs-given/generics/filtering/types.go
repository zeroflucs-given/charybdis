package filtering

import (
	"context"
)

// Expression is a type used to represent a boolean inclusion filter.
type Expression[T any] func(index int, v T) bool

// ExpressionWithContext is a type used to represent a context aware boolean inclusion filter
type ExpressionWithContext[T any] func(ctx context.Context, index int, v T) (bool, error)

// WrapWithContext adapts a non-context aware filter for use with a context aware filtering path.
func WrapWithContext[T any](filter Expression[T]) ExpressionWithContext[T] {
	return func(ctx context.Context, index int, v T) (bool, error) {
		return filter(index, v), nil
	}
}
