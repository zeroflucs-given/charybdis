package generics

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/generics/filtering"
)

// Concatenate all lists together
func Concatenate[S ~[]T, T any](lists ...S) S {
	c := 0
	for _, v := range lists {
		c += len(v)
	}

	result := make(S, 0, c)
	for _, v := range lists {
		result = append(result, v...)
	}

	return result
}

// Contains returns true if the set contains the specified value
func Contains[S ~[]T, T comparable](items S, value T) bool {
	for _, v := range items {
		if v == value {
			return true
		}
	}
	return false
}

// Cut removes the head of a list, returning it and the remainder of the list.
// If the input list is empty, cut returns the type-default.
func Cut[S ~[]T, T any](items S) (T, S) {
	var dflt T
	if len(items) == 0 {
		return dflt, nil
	} else if len(items) == 1 {
		return items[0], nil
	}

	return items[0], items[1:]
}

// DefaultIfEmpty checks to see if the specified slice is empty
// and if so, creates a slice with a specified default value.
func DefaultIfEmpty[S ~[]T, T any](items S, def T) S {
	if len(items) == 0 {
		return S{
			def,
		}
	}

	return items
}

// FirstIndexOf returns the first index of an item in the slice
func FirstIndexOf[S ~[]T, T comparable](v T, items S) int {
	for i := 0; i < len(items); i++ {
		if items[i] == v {
			return i
		}
	}

	return -1
}

// LastIndexOf returns the last index of an item in the slice
func LastIndexOf[S ~[]T, T comparable](v T, items S) int {
	for i := len(items) - 1; i >= 0; i-- {
		if items[i] == v {
			return i
		}
	}

	return -1
}

// Reverse creates a reversed copy of the slice
func Reverse[S ~[]T, T any](items S) S {
	output := make([]T, len(items))
	for i, v := range items {
		output[len(items)-i-1] = v
	}
	return output
}

// Skip the first N items of the slice.
func Skip[S ~[]T, T any](items S, n int) S {
	if len(items) <= n {
		return nil
	}

	return items[n:]
}

// Take up to N items from the slice
func Take[S ~[]T, T any](items S, n int) S {
	if len(items) == 0 {
		return nil
	} else if len(items) < n {
		return items[0:]
	}

	return items[0:n]
}

// TakeUntil takes items from the slice until the first item that passes the predicate.
func TakeUntil[S ~[]T, T any](items S, filters ...filtering.Expression[T]) S {
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
func TakeUntilWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (S, error) {
	filter := filtering.NotWithContext(filtering.AndWithContext(filters...))
	var result S

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
func TakeWhile[S ~[]T, T any](items S, filters ...filtering.Expression[T]) S {
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
func TakeWhileWithContext[S ~[]T, T any](ctx context.Context, items S, filters ...filtering.ExpressionWithContext[T]) (S, error) {
	filter := filtering.AndWithContext(filters...)
	var result S

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
