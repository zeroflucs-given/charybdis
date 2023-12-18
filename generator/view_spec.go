package generator

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/generics"
)

// CreateDDLFromViewSpecification creates the DDL to create a view from its spec
func CreateDDLFromViewSpecification(keyspace string, spec *metadata.ViewSpecification, existing *gocql.ViewMetadata) ([]metadata.DDLOperation, error) {
	// Validate input
	if keyspace == "" || spec == nil {
		return nil, ErrInvalidInput
	}

	errSpec := spec.Validate()
	if errSpec != nil {
		return nil, fmt.Errorf("error validating table spec: %w", errSpec)
	}

	keyPredicates := generics.Map(
		generics.Concatenate(
			generics.Map(spec.Partitioning, func(i int, p *metadata.PartitioningColumn) *metadata.ColumnSpecification {
				return p.Column
			}),
			generics.Map(spec.Clustering, func(i int, c *metadata.ClusteringColumn) *metadata.ColumnSpecification {
				return c.Column
			}),
		), func(i int, c *metadata.ColumnSpecification) string {
			return fmt.Sprintf("%v IS NOT NULL", c.Name)
		})

	var commands []metadata.DDLOperation

	if existing == nil {
		// Create the shell of the view if it does not exist
		initialCreate := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %v.%v AS SELECT * FROM %v.%v WHERE %v PRIMARY KEY (%v) %v",
			keyspace,
			spec.Name,
			keyspace,
			spec.Table.Name,
			strings.Join(keyPredicates, " AND "),
			getKeySpec(spec.Partitioning, spec.Clustering),
			getClusteringSuffix(spec.Clustering),
		)

		commands = append(commands, metadata.DDLOperation{
			Description:  fmt.Sprintf("Create the view %q.", spec.Name),
			Command:      initialCreate,
			IgnoreErrors: []string{},
		})
	}

	return commands, nil
}
