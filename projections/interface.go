package projections

import "context"

// ProjectionManager is an interface that describes the behaviours of a projection manager
type ProjectionManager[T any] interface {
	// ProcessChange performs the processing of a given change
	ProcessChange(ctx context.Context, update *T) error
}

// Projection is our type that describes the API of a single project.
type Projection[T any] interface {
}

// PrimaryKeyExtractor extracts primary key fields from the input instance
type PrimaryKeyExtractor func(instance interface{}) ([]interface{}, error)
