package filtering

import "context"

// True is an always-pass filter for use with non-context aware operations
func True[T any](index int, v T) bool {
	return true
}

// TrueWithContext is an always-pass filter for use with context aware operations
func TrueWithContext[T any](ctx context.Context, index int, v T) (bool, error) {
	return true, nil
}

// False is an always-false filter for use with non-context aware operations
func False[T any](index int, v T) bool {
	return false
}

// FalseWithContext is an always-false filter for use with context aware operations
func FalseWithContext[T any](ctx context.Context, index int, v T) (bool, error) {
	return false, nil
}

// IsZero is a filter that returns true if the item provided has the zero value for its type - for use with non-context aware operations
func IsZero[T comparable](_ int, v T) bool {
	var z T
	return v == z
}

// IsZeroWithContext is a filter that returns true if the item provided has the zero value for its type - for use with context aware operations
func IsZeroWithContext[T comparable](_ context.Context, _ int, v T) bool {
	var z T
	return v == z
}
