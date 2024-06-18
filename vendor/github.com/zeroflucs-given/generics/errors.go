package generics

import (
	"errors"
	"testing"
)

// ErrEmptySlice indicates an error applying an operator to an empty slice without a suitable
// default or other fallback.
var ErrEmptySlice = errors.New("operator cannot be applied to empty slice")

// ErrTupleHasError is our
var ErrTupleHasError = errors.New("the tail member of the input tuple contained an error")

// Must enforces that a value/error pair contains no error, and returns the value.
// If an error is present, the code will panic.
// If you require a default value instead use MustDefault instead.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(ErrTupleHasError)
	}

	return v
}

// ValueOrError returns an error only if the error is set, otherwise returns
// the value and nil. This replaces the value with the default/nil for its type.
func ValueOrError[T any](value T, err error) (T, error) {
	if err != nil {
		var blank T
		return blank, err
	}

	return value, nil
}

// ValueOrFailTest gets a value, or fails a test if an error is
// present. This simplifies some test code.
func ValueOrFailTest[T any](t *testing.T, v T, err error) T {
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	return v
}

// ValueOrPanic consumes an error and panics, simplifying some code.
// We recommend only using this in the context of tools.
func ValueOrPanic[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
