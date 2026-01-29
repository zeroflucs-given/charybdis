package generics

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/generics/filtering"
)

// All returns true if no item in the list fails to meet the predicates. If multiple
// predicates are passed, they are treated as logical AND operations.
func All[S ~[]T, T any](items S, filters ...filtering.Expression[T]) bool {
	filter := filtering.And(filters...)

	for i, v := range items {
		if !filter(i, v) {
			return false
		}
	}

	return true
}

// AllWithContext is a context aware function that returns true if no item in the list fails to meet the predicate.
// If multiple predicate passed, they are treated as logical AND operations.
func AllWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (bool, error) {
	filter := filtering.AndWithContext(filters...)

	for i, v := range items {
		match, err := filter(ctx, i, v)
		if err != nil {
			return false, fmt.Errorf("error applying filter to index %d: %w", i, err)
		}

		if !match {
			return false, nil
		}
	}

	return true, nil
}

// Any returns true if any item in the slice matches the filter. If multiple predicates are passed,
// they are treated as logical AND operations.
func Any[S ~[]T, T any](items S, filters ...filtering.Expression[T]) bool {
	filter := filtering.And(filters...)

	for i, v := range items {
		if filter(i, v) {
			return true
		}
	}

	return false
}

// AnyWithContext is a context aware function that returns true if any item in the slice matches the filter.
func AnyWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (bool, error) {
	filter := filtering.AndWithContext(filters...)

	for i, v := range items {
		match, err := filter(ctx, i, v)
		if err != nil {
			return false, fmt.Errorf("error applying filter to index %d: %w", i, err)
		} else if match {
			return true, nil
		}
	}

	return false, nil
}

// Count returns how many items pass the filters
func Count[S ~[]T, T any](items S, filters ...filtering.Expression[T]) int {
	if len(filters) == 0 {
		return len(items)
	}
	filter := filtering.And(filters...)

	count := 0
	for i, v := range items {
		if filter(i, v) {
			count++
		}
	}

	return count
}

// CountWithContext counts how many items pass the filter
func CountWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (int, error) {
	if len(filters) == 0 {
		return len(items), nil
	}
	filter := filtering.AndWithContext(filters...)

	count := 0
	for i, v := range items {
		match, err := filter(ctx, i, v)
		if err != nil {
			return 0, fmt.Errorf("error applying filter to index %d: %w", i, err)
		}

		if match {
			count++
		}
	}

	return count, nil
}
