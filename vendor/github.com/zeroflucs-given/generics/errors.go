package generics

import (
	"errors"
)

// ErrEmptySlice indicates an errror applying an operator to an empty slice without a suitable
// default or other fallback.
var ErrEmptySlice = errors.New("operator cannot be applied to empty slice")

// ErrTupleHasError is our
var ErrTupleHasError = errors.New("the tail member of the input tuple contained an error")

// Must enforces that a value/error pair contains no error, and returns the value. However
// if an error is present, the code will panic. If you require a default value instead use
// MustDefault instead.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(ErrTupleHasError)
	}

	return v
}

// ValueOrError returns an error only if the error is set, otherwise returns
// the value and nil. This replaces the value with the default/nil for its
// type.
func ValueOrError[T any](value T, err error) (T, error) {
	if err != nil {
		var blank T
		return blank, err
	}

	return value, nil
}
