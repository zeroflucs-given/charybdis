package generics

import (
	"sort"
)

// Sort creates a sorted version of the list, whilst leaving
// the original list intact.
func Sort[S ~[]T, T Comparable](in S) S {
	out := make(S, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

// SortDescending creates a reverse sorted version of the list, whilst
// leaving the original list intact.
func SortDescending[S ~[]T, T Comparable](in S) S {
	out := make(S, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i] > out[j]
	})
	return out
}
