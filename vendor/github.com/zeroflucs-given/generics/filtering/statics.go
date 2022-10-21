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

// FilterFalse is an always-false filter for use with non-context aware operations
func False[T any](index int, v T) bool {
	return false
}

// FalseWithContext is an always-false filter for use with context aware operations
func FalseWithContext[T any](ctx context.Context, index int, v T) (bool, error) {
	return false, nil
}
