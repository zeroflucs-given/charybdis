package metadata

import "errors"

// ErrNoObject indicates an object is missing
var ErrNoObject = errors.New("object missing or undefined")

// ErrInvalidTableOrViewName indicates a table or view name is invalid
var ErrInvalidTableOrViewName = errors.New("invalid table or view name")

// ErrInvalidColumnName indicates a column name is invalid
var ErrInvalidColumnName = errors.New("invalid column name")

// ErrNoPartitioningKey indicates we have no partitioning key
var ErrNoPartitioningKey = errors.New("no partitioning key key")

// ErrMismatchedColumns indicates one of the columns referenced in an object is
// not present in the column list.
var ErrMismatchedColumns = errors.New("mismatched columns on object")

// ErrInconsistentMetadata indicates a value is inconsistent, such as the column
// being marked for clustering, but not present in the list of clustering columns
var ErrInconsistentMetadata = errors.New("inconsistent metadata")

// ErrViewKeyUnsuitable indicates the view key definition was incorrect. It either
// is missing a base table key, or has multiple additional fields.
var ErrViewKeyUnsuitable = errors.New("view keys must contain all table keys, plus at most one extra")
