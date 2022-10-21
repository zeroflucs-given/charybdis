package filtering

import (
	"context"
	"fmt"
)

// And creates a composite filter that checks multiple filters. If there are no filters
// that fail (or the list of filters is empty), then the items pass.
func And[T any](filters ...Expression[T]) func(index int, v T) bool {
	return func(index int, v T) bool {
		for _, filter := range filters {
			if !filter(index, v) {
				return false
			}
		}

		return true
	}
}

// AndWithContext creates a composite filter that checks multiple filters. If there are no filters
// that fail (or the list of filters is empty), then the items pass.
func AndWithContext[T any](filters ...ExpressionWithContext[T]) func(ctx context.Context, index int, v T) (bool, error) {
	return func(ctx context.Context, index int, v T) (bool, error) {
		for i, filter := range filters {
			match, err := filter(ctx, index, v)
			if err != nil {
				return false, fmt.Errorf("error applying aggregated filter index %d to item index %d: %w", i, index, err)
			}
			if !match {
				return false, nil
			}
		}

		return true, nil
	}
}

// Or creates a composite filter that passes an item if any filter meets it. If there
// are no matching filters, the item fails the filter.
func Or[T any](filters ...Expression[T]) func(index int, v T) bool {
	return func(index int, v T) bool {
		for _, filter := range filters {
			if filter(index, v) {
				return true
			}
		}

		return false
	}
}

// OrWithContext creates a composite filter that passes an item if any filter meets it. If there
// are no matching filters, the item fails the filter.
func OrWithContext[T any](filters ...ExpressionWithContext[T]) func(ctx context.Context, index int, v T) (bool, error) {
	return func(ctx context.Context, index int, v T) (bool, error) {
		for i, filter := range filters {
			match, err := filter(ctx, index, v)
			if err != nil {
				return false, fmt.Errorf("error applying aggregated filter index %d to item index %d: %w", i, index, err)
			}
			if match {
				return true, nil
			}
		}

		return false, nil
	}
}

// Not negates a filter/inverts it.
func Not[T any](filter Expression[T]) Expression[T] {
	return func(index int, v T) bool {
		return !filter(index, v)
	}
}

// NotWithContext negates a context aware filter.
func NotWithContext[T any](filter ExpressionWithContext[T]) ExpressionWithContext[T] {
	return func(ctx context.Context, index int, v T) (bool, error) {
		match, err := filter(ctx, index, v)
		if err != nil {
			return false, err
		}

		return !match, nil
	}
}
