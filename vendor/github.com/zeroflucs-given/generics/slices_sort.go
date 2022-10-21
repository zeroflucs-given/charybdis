package generics

import (
	"sort"
)

// Sort creates a sorted version of the list, whilst leaving
// the original list intact.
func Sort[T Comparable](in []T) []T {
	out := make([]T, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

// SortDescending creates a reverse sorted version of the list, whilst
// leaving the original list intact.
func SortDescending[T Comparable](in []T) []T {
	out := make([]T, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i] > out[j]
	})
	return out
}
