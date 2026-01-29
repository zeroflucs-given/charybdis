package generics

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/generics/filtering"
)

// First item in a slice that passes the filters. If multiple filters are set, they are treated
// as a logical AND. If no filters are set, will return the first item in the slice. If no items
// match, the type default is returned.
func First[S ~[]T, T any](items S, filters ...filtering.Expression[T]) T {
	filter := filtering.And(filters...)

	var def T
	for i, v := range items {
		if filter(i, v) {
			return v
		}
	}

	return def
}

// FirstWithContext gets the first item in a slice that passes the filters. If multiple filters are set, they are treated
// as a logical AND. If no filters are set, will return the first item in the slice. If no items
// match, the type default is returned.
func FirstWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (T, error) {
	filter := filtering.AndWithContext(filters...)

	var def T
	for i, v := range items {
		match, err := filter(ctx, i, v)
		if err != nil {
			return def, fmt.Errorf("error applying filter to item %d: %w", i, err)
		}

		if match {
			return v, nil
		}
	}

	return def, nil
}

// Except returns a slice containing all the elements from the slice `items` that don't match the elements in `exclusions`
func Except[S ~[]T, T comparable](items S, exclusions ...T) S {
	result := make(S, 0, len(items))
	for _, item := range items {
		if Contains(exclusions, item) {
			continue
		}
		result = append(result, item)
	}

	return result
}

// Intersect returns the intersection of the sets of `items` and `others`
func Intersect[S ~[]T, T comparable](items S, others ...T) S {
	result := make(S, 0, len(items))

	for _, item := range items {
		if Contains(others, item) {
			result = append(result, item)
		}
	}

	return result
}

// Filter returns the elements from `items` that match the filtering rules `filters`
func Filter[S ~[]T, T any](items S, filters ...filtering.Expression[T]) S {
	filter := filtering.And(filters...)

	output := make(S, 0, len(items))
	for i, v := range items {
		if filter(i, v) {
			output = append(output, v)
		}
	}
	return output
}

// FilterWithContext filters item in a list
func FilterWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (S, error) {
	filter := filtering.AndWithContext(filters...)

	output := make(S, 0, len(items))
	for i, v := range items {
		ok, err := filter(ctx, i, v)
		if err != nil {
			return nil, fmt.Errorf("error applying filter to item %d: %w", i, err)
		}
		if ok {
			output = append(output, v)
		}
	}

	return output, nil
}

// Last item in a slice that matches the specified filters. Returns the type
// default if none found.
func Last[S ~[]T, T any](items S, filters ...filtering.Expression[T]) T {
	filter := filtering.And(filters...)

	var def T
	for reverseIndex := len(items) - 1; reverseIndex >= 0; reverseIndex-- {
		match := filter(reverseIndex, items[reverseIndex])
		if match {
			return items[reverseIndex]
		}
	}

	return def
}

// LastWithContext item in a slice that matches the specified filters. Returns the type
// default if none found.
func LastWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (T, error) {
	filter := filtering.AndWithContext(filters...)

	var def T
	for reverseIndex := len(items) - 1; reverseIndex >= 0; reverseIndex-- {
		match, err := filter(ctx, reverseIndex, items[reverseIndex])
		if err != nil {
			return def, fmt.Errorf("error applying filter to index %d: %w", reverseIndex, err)
		}
		if match {
			return items[reverseIndex], nil
		}
	}

	return def, nil
}
