package generics

import (
	"context"
	"fmt"
	"sort"
)

// SliceMapperExpression is a type that represents a generic mapper
type SliceMapperExpression[T any, V any] func(index int, input T) V

// MapperContextExp is a type used to represent a context aware generic mapper
type SliceMapperExpressionWithContext[T any, V any] func(ctx context.Context, index int, v T) (V, error)

// Group rolls up items into groups based on a mapper function that provides a key per item
func Group[T any, K Comparable](items []T, keyMapper SliceMapperExpression[T, K]) map[K][]T {
	output := make(map[K][]T)
	for i, value := range items {
		key := keyMapper(i, value)
		output[key] = append(output[key], value)
	}
	return output
}

type compactionSurvivor[T any] struct {
	lastPosition int
	value        T
}

// Compact takes a slice and compacts it, by reducing to only the last occurence of a given key. This
// is akin to Kafka topic compaction, and used for scenarios where you have a slice of mixed updates
// but want to take only the final update for a given predicate. The result order is determined by the
// final position(s) of the surviving elements relative to each other.
func Compact[T any, K Comparable](input []T, keyMapper SliceMapperExpression[T, K]) []T {
	// Create a the set of last updates per item, tracking their
	// index
	survivors := make(map[K]compactionSurvivor[T])
	for i, v := range input {
		key := keyMapper(i, v)
		survivors[key] = compactionSurvivor[T]{
			lastPosition: i,
			value:        v,
		}
	}

	// Flatten to a map
	kvp := ToKeyValues(survivors)
	sort.Slice(kvp, func(i, j int) bool {
		return kvp[i].Value.lastPosition < kvp[j].Value.lastPosition
	})

	result := make([]T, len(kvp))
	for i, v := range kvp {
		result[i] = v.Value.value
	}

	return result
}

// GroupWithContext rolls up items into groups based on a mapper function that provides a key per item
func GroupWithContext[T any, K Comparable](ctx context.Context, items []T, keyMapper SliceMapperExpressionWithContext[T, K]) (map[K][]T, error) {
	output := make(map[K][]T)
	for i, value := range items {
		key, err := keyMapper(ctx, i, value)
		if err != nil {
			return nil, fmt.Errorf("error mapping index %d: %w", i, err)
		}

		output[key] = append(output[key], value)
	}
	return output, nil
}

// Map converts values in a slice from one type to another
func Map[T any, V any](items []T, mapper SliceMapperExpression[T, V]) []V {
	output := make([]V, len(items))
	for i, value := range items {
		output[i] = mapper(i, value)
	}
	return output
}

// MapWithContext executes a mappper over the members of a slice using the specified context
func MapWithContext[T any, V any](ctx context.Context, items []T, mapper SliceMapperExpressionWithContext[T, V]) ([]V, error) {
	output := make([]V, len(items))
	for i, value := range items {
		mapped, err := mapper(ctx, i, value)
		if err != nil {
			return nil, fmt.Errorf("error mapping item %d: %w", i, err)
		}
		output[i] = mapped
	}

	return output, nil
}

// ToMap converts a slice of items into a dictionary using mappers for the key and value pairs. If
// multiple items yield the same key, the last key in the set will be the one kept.
func ToMap[T any, K Comparable, V any](items []T, keyMapper SliceMapperExpression[T, K], valueMapper SliceMapperExpression[T, V]) map[K]V {
	output := make(map[K]V, len(items))
	for i, item := range items {
		key := keyMapper(i, item)
		value := valueMapper(i, item)
		output[key] = value
	}

	return output
}

// ToMapWithContext converts a slice of items into a dictionary using mappers for the key and value pairs. If
// multiple items yield the same key, the last key in the set will be the one kept. If any mapper fails, the
// operation as a whole fails.
func ToMapWithContext[T any, K Comparable, V any](ctx context.Context, items []T, keyMapper SliceMapperExpressionWithContext[T, K], valueMapper SliceMapperExpressionWithContext[T, V]) (map[K]V, error) {
	output := make(map[K]V, len(items))
	for i, item := range items {
		key, err := keyMapper(ctx, i, item)
		if err != nil {
			return nil, fmt.Errorf("error mapping key for index %d: %w", i, err)
		}

		value, err := valueMapper(ctx, i, item)
		if err != nil {
			return nil, fmt.Errorf("error mapping value for index %d: %w", i, err)
		}

		output[key] = value
	}

	return output, nil
}
