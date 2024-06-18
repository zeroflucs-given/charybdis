package generics

// Chunk will uniformly distribute the given over n bins/buckets.
// Note that buckets are slices referencing the same memory.
func Chunk[T any](items []T, n int) [][]T {
	if n <= 0 {
		return nil
	}

	if n == 1 {
		return [][]T{items}
	}

	bins := make([][]T, n)

	baseCount := len(items) / n
	leftover := len(items) % n

	idx := 0
	for i := 0; i < n; i += 1 {
		count := baseCount
		if leftover > 0 {
			count += 1
			leftover -= 1
		}

		bins[i] = items[idx : idx+count]
		idx += count
	}

	return bins
}

// ChunkMap will uniformly distribute the given map over n bins/buckets.
func ChunkMap[K comparable, V any](items map[K]V, n int) []map[K]V {
	if n <= 0 {
		return nil
	}

	if n == 1 {
		return []map[K]V{items}
	}

	baseCount := len(items) / n
	leftover := len(items) % n

	// Pass 1 - preallocate the map memory
	bins := make([]map[K]V, n)
	for i := range bins {
		count := baseCount
		if i < leftover {
			count += 1
		}

		bins[i] = make(map[K]V, count)
	}

	// Pass 2 - distribute the items.
	binIdx := 0
	curCount := 0
	for k, v := range items {
		count := baseCount
		if binIdx < leftover {
			count += 1
		}

		bins[binIdx][k] = v
		curCount += 1

		if curCount == count {
			curCount = 0
			binIdx += 1
		}
	}

	return bins
}
