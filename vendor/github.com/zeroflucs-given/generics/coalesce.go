package generics

// Coalesce returns the first value in the input that is not the default value forthe type.
func Coalesce[T comparable](items ...T) T {
	var dflt T
	for _, item := range items {
		if item != dflt {
			return item
		}
	}
	return dflt
}
