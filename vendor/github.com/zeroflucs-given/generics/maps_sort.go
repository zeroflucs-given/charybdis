package generics

import "sort"

// SortedByKey sorts a map by key and returns it as a slice of key-value pairs
// enabling working with the map in a reliable/repeatable order
func SortedByKey[K Comparable, V any](input map[K]V) []KeyValuePair[K, V] {
	kvps := ToKeyValues(input)
	sort.Slice(kvps, func(i, j int) bool {
		return kvps[i].Key < kvps[j].Key
	})
	return kvps
}
