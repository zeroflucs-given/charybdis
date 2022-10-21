package generics

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/generics/filtering"
)

// Concatenate all lists together
func Concatenate[T any](lists ...[]T) []T {
	c := 0
	for _, v := range lists {
		c += len(v)
	}

	result := make([]T, 0, c)
	for _, v := range lists {
		result = append(result, v...)
	}

	return result
}

// Contains returns true if the set contains the specified value
func Contains[T comparable](items []T, value T) bool {
	for _, v := range items {
		if v == value {
			return true
		}
	}
	return false
}

// DefaultIfEmpty checks to see if the specified slice is empty
// and if so, creates a slice with a specified default value.
func DefaultIfEmpty[T any](items []T, def T) []T {
	if len(items) == 0 {
		return []T{
			def,
		}
	}

	return items
}

// Reverse creates a reversed copy of the slice
func Reverse[T any](items []T) []T {
	output := make([]T, len(items))
	for i, v := range items {
		output[len(items)-i-1] = v
	}
	return output
}

// Skip the first N items of the slice.
func Skip[T any](items []T, n int) []T {
	if len(items) <= n {
		return nil
	}

	return items[n:]
}

// Take up to N items from the slice
func Take[T any](items []T, n int) []T {
	if len(items) == 0 {
		return nil
	} else if len(items) < n {
		return items[0:]
	}

	return items[0:n]
}

// TakeUntil takes items from the slice until the first item that passes the predicate.
func TakeUntil[T any](items []T, filters ...filtering.Expression[T]) []T {
	filter := filtering.Not(filtering.And(filters...))
	var result []T

	for i, v := range items {
		if !filter(i, v) {
			break
		}

		result = append(result, v)
	}

	return result
}

// TakeUntilWithContext takes items from the slice until the first item that passes the predicate.
func TakeUntilWithContext[T any](ctx context.Context, items []T, filters ...filtering.ExpressionWithContext[T]) ([]T, error) {
	filter := filtering.NotWithContext(filtering.AndWithContext(filters...))
	var result []T

	for i, v := range items {
		match, err := filter(ctx, i, v)
		if err != nil {
			return nil, fmt.Errorf("error applying take filter to item %d: %w", i, err)
		}
		if !match {
			break
		}

		result = append(result, v)
	}

	return result, nil
}

// TakeWhile takes items from the slice until the first item that fails the predicate.
func TakeWhile[T any](items []T, filters ...filtering.Expression[T]) []T {
	filter := filtering.And(filters...)
	var result []T

	for i, v := range items {
		if !filter(i, v) {
			break
		}

		result = append(result, v)
	}

	return result
}

// TakeWhileWithContext takes items from the slice until the first item that fails the predicate.
func TakeWhileWithContext[T any](ctx context.Context, items []T, filters ...filtering.ExpressionWithContext[T]) ([]T, error) {
	filter := filtering.AndWithContext(filters...)
	var result []T

	for i, v := range items {
		match, err := filter(ctx, i, v)
		if err != nil {
			return nil, fmt.Errorf("error applying take filter to item %d: %w", i, err)
		}
		if !match {
			break
		}

		result = append(result, v)
	}

	return result, nil
}
