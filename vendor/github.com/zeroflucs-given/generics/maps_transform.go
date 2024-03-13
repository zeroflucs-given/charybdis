package generics

import (
	"context"
	"fmt"
)

// KeyValuePair is a pairing of key/values, for when we have to represent map
// sets as a list/slice.
type KeyValuePair[K comparable, V any] struct {
	Key   K
	Value V
}

// KeyValuesToMap converts a slice of key-value pairs to a map
func KeyValuesToMap[K comparable, V any](input []KeyValuePair[K, V]) map[K]V {
	result := make(map[K]V, len(input))
	for _, item := range input {
		result[item.Key] = item.Value
	}
	return result
}

// Keys gets the set of keys in a map
func Keys[K comparable, V any](input map[K]V) []K {
	result := make([]K, 0, len(input))
	for k := range input {
		result = append(result, k)
	}
	return result
}

// MapValues translates all values in a map to new values
func MapValues[K comparable, V any, NV any](input map[K]V, mapper func(k K, v V) NV) map[K]NV {
	result := make(map[K]NV, len(input))
	for k, v := range input {
		result[k] = mapper(k, v)
	}
	return result
}

// MapValuesWithContext translates all values in a map to new values
func MapValuesWithContext[K comparable, V any, NV any](ctx context.Context, input map[K]V, mapper func(ctx context.Context, k K, v V) (NV, error)) (map[K]NV, error) {
	result := make(map[K]NV, len(input))
	for k, v := range input {
		mapped, err := mapper(ctx, k, v)
		if err != nil {
			return nil, fmt.Errorf("failed to map key %v: %w", k, err)
		}

		result[k] = mapped
	}
	return result, nil
}

// ToKeyValues converts a map into a set of key/value pairs
func ToKeyValues[K comparable, V any](input map[K]V) []KeyValuePair[K, V] {
	result := make([]KeyValuePair[K, V], 0, len(input))
	for k, v := range input {
		result = append(result, KeyValuePair[K, V]{
			Key:   k,
			Value: v,
		})
	}

	return result
}

// Values gets the set of keys in a map
func Values[K comparable, V any](input map[K]V) []V {
	result := make([]V, 0, len(input))
	for _, v := range input {
		result = append(result, v)
	}
	return result
}
