package generator

import "errors"

var (
	ErrInvalidInput     = errors.New("invalid input or object not specified") // ErrInvalidInput indicates an input object was not specifier
	ErrOutOfRange       = errors.New("value is out of range")
	ErrUnknownOperation = errors.New("the requested operation does not exist")
)
