package generics

// KeyValuePair is a pairing of key/values, for when we have to represent map
// sets as a list/slice.
type KeyValuePair[K Comparable, V any] struct {
	Key   K
	Value V
}

// KeyValuesToMap converts a slice of key-value pairs to a map
func KeyValuesToMap[K Comparable, V any](input []KeyValuePair[K, V]) map[K]V {
	result := make(map[K]V, len(input))
	for _, item := range input {
		result[item.Key] = item.Value
	}
	return result
}

// Keys gets the set of keys in a map
func Keys[K Comparable, V any](input map[K]V) []K {
	result := make([]K, 0, len(input))
	for k := range input {
		result = append(result, k)
	}
	return result
}

// ToKeyValues converts a map into a set of key/value pairs
func ToKeyValues[K Comparable, V any](input map[K]V) []KeyValuePair[K, V] {
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
func Values[K Comparable, V any](input map[K]V) []V {
	result := make([]V, 0, len(input))
	for _, v := range input {
		result = append(result, v)
	}
	return result
}
