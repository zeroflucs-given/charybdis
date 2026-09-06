package tables

import "errors"

// ErrPreconditionFailed indicates an IF predicate on an LWT was not satisfied
var ErrPreconditionFailed = errors.New("precondition failed for LWT operation")

// ErrNoSession indicates WithSession was given a nil session
var ErrNoSession = errors.New("no session supplied")

// ErrNoQueryTimeout indicates WithSession was given a non-positive query timeout
var ErrNoQueryTimeout = errors.New("query timeout must be positive")

// ErrSessionKeyspaceMismatch indicates a session given to WithSession defaults to a different keyspace
// than the manager is configured for
var ErrSessionKeyspaceMismatch = errors.New("session default keyspace does not match manager keyspace")
