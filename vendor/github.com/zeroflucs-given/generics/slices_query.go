package generics

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/generics/filtering"
)

// First item in a slice that passes the filters. If multiple filters are set, they are treated
// as a logical AND. If no filters are set, will return the first item in the slice. If no items
// match, the type default is returned.
func First[T any](items []T, filters ...filtering.Expression[T]) T {
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
func FirstWithContext[T any](ctx context.Context, items []T, filters ...filtering.ExpressionWithContext[T]) (T, error) {
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

// Except
func Except[T comparable](items []T, exclusions ...T) []T {
	result := make([]T, 0, len(items))
	for _, item := range items {
		if Contains(exclusions, item) {
			continue
		}
		result = append(result, item)
	}

	return result
}

// Intersect
func Intersect[T comparable](items []T, others ...T) []T {
	result := make([]T, 0, len(items))

	for _, item := range items {
		if Contains(others, item) {
			result = append(result, item)
		}
	}

	return result
}

// Filter filters item in a list
func Filter[T any](items []T, filters ...filtering.Expression[T]) []T {
	filter := filtering.And(filters...)

	output := make([]T, 0, len(items))
	for i, v := range items {
		if filter(i, v) {
			output = append(output, v)
		}
	}
	return output
}

// FilterWithContext filters item in a list
func FilterWithContext[T any](ctx context.Context, items []T, filters ...filtering.ExpressionWithContext[T]) ([]T, error) {
	filter := filtering.AndWithContext(filters...)

	output := make([]T, 0, len(items))
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
func Last[T any](items []T, filters ...filtering.Expression[T]) T {
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
func LastWithContext[T any](ctx context.Context, items []T, filters ...filtering.ExpressionWithContext[T]) (T, error) {
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
