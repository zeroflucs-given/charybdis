package generics

// If returns the equivalent value based on the predicate
func If[T any](condition bool, ifTrue, ifFalse T) T {
	if condition {
		return ifTrue
	}
	return ifFalse
}
