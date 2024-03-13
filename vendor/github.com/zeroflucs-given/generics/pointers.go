package generics

// PointerTo returns the pointer to a value
func PointerTo[T any](v T) *T {
	return &v
}

// PointerOrNil returns a nil if the value is its type default, or
// a pointer to the value if it's set.
func PointerOrNil[T comparable](v T) *T {
	var dflt T
	if v == dflt {
		return nil
	}

	return &v
}

// ValueOrDefault safely resolves a pointer to a type
func ValueOrDefault[T any](v *T) T {
	var dflt T
	if v == nil {
		return dflt
	}

	return *v
}
