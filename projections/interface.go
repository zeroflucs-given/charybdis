package projections

import (
	"context"

	"github.com/zeroflucs-given/charybdis/tables"
)

// ProjectionManager is an interface that describes the behaviours of a projection manager
type ProjectionManager[T any] interface {
	// Projection gets a projection by name
	Projection(name string) tables.ViewManager[T]

	// ProcessChange performs the processing of a given change
	ProcessChange(ctx context.Context, update *T) error

	// ProcessDelete performs the processing of a deleted object
	ProcessDelete(ctx context.Context, deleted *T) error
}

// Projection is our type that describes the API of a single project.
type Projection[T any] interface {
}

// PrimaryKeyExtractor extracts primary key fields from the input instance
type PrimaryKeyExtractor func(instance interface{}) ([]interface{}, error)
