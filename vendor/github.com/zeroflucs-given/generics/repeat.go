package generics

// Repeat a value N times
func Repeat[T any](v T, count int) []T {
	result := make([]T, count)
	for i := 0; i < count; i++ {
		result[i] = v
	}

	return result
}
