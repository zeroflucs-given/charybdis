package tables

import "errors"

// ErrPreconditionFailed indicates an IF predicate on an LWT was not satisfied
var ErrPreconditionFailed = errors.New("precondition failed for LWT operation")
