package generics

// CheckBijection checks if there is a bijection from left into right under the given predicate, i.e.
// - Each right element must be matched by exactly one left.
// - Each left may match at most one right.
// - No lefts may remain unmatched.
// Returns the mapping and true on success; nil and false otherwise.
// NB: Runs in O(len(left) * len(right)).
func CheckBijection[T comparable](left, right []T, pred func(a T, b T) bool) (map[T]T, bool) {
	if len(left) != len(right) {
		return nil, false
	}

	usedRight := make([]bool, len(left))
	mapping := make(map[T]T, len(left))

	for _, l := range left {
		matchIdx := -1

		for ri, r := range right {
			if pred(l, r) {
				if matchIdx != -1 {
					// l matches more than one right entry == FAIL!
					return nil, false
				}
				matchIdx = ri
			}
		}

		if matchIdx == -1 {
			// l didn't match anything
			return nil, false
		}

		if usedRight[matchIdx] {
			// multiple left entries match the same right
			return nil, false
		}

		usedRight[matchIdx] = true
		mapping[l] = right[matchIdx]
	}

	return mapping, true
}

// CheckPartialBijection checks if there is a partial bijection from left onto right under the given predicate, i.e.
// - Each right element must be matched by exactly one left.
// - Each left may match at most one right.
// - Some lefts may remain unmatched.
// Returns the mapping (only including matched lefts) and true on success; nil and false otherwise.
// NB: Runs in O(len(left) * len(right)).
func CheckPartialBijection[T comparable](left, right []T, pred func(a T, b T) bool) (map[T]T, bool) {
	if len(left) < len(right) {
		// not enough lefts to cover all rights
		return nil, false
	}

	usedLeft := make([]bool, len(left))
	mapping := make(map[T]T, len(right))

	for _, r := range right {
		matchIdx := -1

		for li, l := range left {
			if pred(l, r) {
				if matchIdx != -1 {
					// r matches more than one left element == FAIL!
					return nil, false
				}
				matchIdx = li
			}
		}

		if matchIdx == -1 {
			// r didn't match any left
			return nil, false
		}

		if usedLeft[matchIdx] {
			// multiple rights match the same left
			return nil, false
		}

		usedLeft[matchIdx] = true
		mapping[left[matchIdx]] = r
	}

	return mapping, true
}
