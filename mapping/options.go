package mapping

import (
	"context"
	"errors"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/tables"
	"github.com/zeroflucs-given/generics"
)

// WithAutomaticTableSpecification creates a table-manager option that sets the table
// specification by reflecting over the structure.
func WithAutomaticTableSpecification[T any](name string) tables.ManagerOption {
	var instance T
	spec, err := CreateTableSpecificationFromExample(name, &instance)

	// This is a little unclean, but in my heart I'm comfortable
	// with this as tag errors are clearly a compile-time issue and
	// can't be "handled".
	if err != nil {
		panic(err)
	}

	types, err := CreateTypeSpecificationsFromTableExample(&instance)
	if err != nil {
		panic(err)
	}

	spec.CustomTypes = types

	return tables.WithTableSpecification(spec)
}

// WithAutomaticTypeSpecification creates a table-manager option that sets the table specification by reflecting over the structure.
func WithAutomaticTypeSpecification[T any](name string) tables.ManagerOption {
	var instance T
	spec, err := CreateTypeSpecificationFromExample(name, &instance)

	if err != nil {
		panic(err)
	}

	return tables.WithTypeSpecification(spec)
}

// WithSimpleView attaches a view definition to the table manager, based on named columns.
// All columns of the base table are part of the view
func WithSimpleView(name string, partitionKeys []string, clusteringKeys []string) tables.ManagerOption {
	return tables.WithSpecMutator(func(ctx context.Context, table *metadata.TableSpecification, originalView *metadata.ViewSpecification) (*metadata.TableSpecification, *metadata.ViewSpecification, error) {
		failed := false

		view := &metadata.ViewSpecification{
			Name:  name,
			Table: table,
			Partitioning: generics.Map(partitionKeys, func(i int, k string) *metadata.PartitioningColumn {
				for _, col := range table.Columns {
					if col.Name == k {
						return &metadata.PartitioningColumn{
							Column: col,
							Order:  i + 1,
						}
					}
				}

				failed = true
				return nil
			}),
			Clustering: generics.Map(clusteringKeys, func(i int, k string) *metadata.ClusteringColumn {
				for _, col := range table.Columns {
					if col.Name == k {
						return &metadata.ClusteringColumn{
							Column: col,
							Order:  i + 1,
						}
					}
				}

				failed = true
				return nil
			}),
		}

		if failed {
			return table, originalView, errors.New("missing view columns on base table")
		}

		return table, view, nil
	})
}
