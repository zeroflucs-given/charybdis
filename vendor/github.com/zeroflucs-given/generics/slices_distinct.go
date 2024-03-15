package generics

import (
	"cmp"
	"slices"
)

// Distinct sorts and removes any duplicate elements from a slice,
// returning a new copy. The input slice is unchanged.
func Distinct[S ~[]E, E cmp.Ordered](in S) S {
	return DistinctFunc(in, cmp.Compare[E])
}

// DistinctFunc is like [Distinct] but uses a custom comparison function
// on each pair of elements.
//
// DistinctFunc requires that cmp is a strict weak ordering.
// See https://en.wikipedia.org/wiki/Weak_ordering#Strict_weak_orderings.
func DistinctFunc[S ~[]E, E any](x S, cmp func(a, b E) int) S {
	s := slices.Clone(x)
	slices.SortFunc(s, cmp)
	return slices.CompactFunc(s, func(a E, b E) bool { return cmp(a, b) == 0 })
}

func distinctStableInternal[S ~[]E, E any, K comparable](in S, key func(v E) K) S {
	dups := make(map[K]struct{}, len(in))
	out := make(S, 0, len(in))

	for _, v := range in {
		h := key(v)
		if _, exists := dups[h]; !exists {
			out = append(out, v)
			dups[h] = struct{}{}
		}
	}

	return out
}

// DistinctStable removes any duplicates from a slice, keeping iteration order
// and returning a new copy. The input slice is unchanged.
func DistinctStable[S ~[]E, E comparable](in S) S {
	return distinctStableInternal[S, E, E](in, func(v E) E { return v })
}

// DistinctStableFunc is like [DistinctStable] but uses a custom hash function
// to deduplicate the elements.
//
// Elements with the same hash are considered to be equal, collisions are NOT considered.
func DistinctStableFunc[S ~[]E, E any](in S, hash func(val E) uint64) S {
	return distinctStableInternal[S, E, uint64](in, hash)
}
