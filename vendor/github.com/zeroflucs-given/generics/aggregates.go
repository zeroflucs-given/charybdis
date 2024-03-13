package generics

// Min gets the minimum of numeric values. If the slice is empty then
// a value of 0 is returned.
func Min[T Numeric](items []T) T {
	var result T
	if len(items) == 0 {
		return result
	} else {
		result = items[0]
	}

	for _, v := range items {
		if v < result {
			result = v
		}
	}

	return result
}

// Max gets the maximum of numeric values. If the slice is empty then
// a value of 0 is returned.
func Max[T Numeric](items []T) T {
	var result T
	if len(items) == 0 {
		return result
	} else {
		result = items[0]
	}

	for _, v := range items {
		if v > result {
			result = v
		}
	}

	return result
}

// Sum numeric values
func Sum[T Numeric](items []T) T {
	var result T
	for _, v := range items {
		result += v
	}
	return result
}
