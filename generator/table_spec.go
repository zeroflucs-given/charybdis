package generator

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/generics"
)

// CreateDDLFromTableSpecification creates the DDL to create and extend a table from it's table specification
func CreateDDLFromTableSpecification(keyspace string, spec *metadata.TableSpecification, existingMetadata *tableMetadata) ([]metadata.DDLOperation, error) {
	// Validate input
	if keyspace == "" || spec == nil {
		return nil, ErrInvalidInput
	}

	errSpec := spec.Validate()
	if errSpec != nil {
		return nil, fmt.Errorf("error validating table spec: %w", errSpec)
	}

	var commands []metadata.DDLOperation

	var existingTable *gocql.TableMetadata
	if existingMetadata != nil && existingMetadata.Table != nil {
		existingTable = existingMetadata.Table
	}

	if existingTable == nil {
		// Create the shell of the table if it does not already exist
		initialCreate := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v.%v (%v, PRIMARY KEY(%v))",
			keyspace,
			spec.Name,
			strings.Join(generics.Map(generics.Filter(spec.Columns, func(i int, c *metadata.ColumnSpecification) bool {
				return c.IsPartitioningKey || c.IsClusteringKey
			}), func(i int, c *metadata.ColumnSpecification) string {
				return c.Name + " " + c.CQLType
			}), ", "),
			getKeySpec(spec.Partitioning, spec.Clustering),
		) + getClusteringSuffix(spec.Clustering)
		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Create the table %q with columns relating to the key.", spec.Name),
			Command:     initialCreate,
		})
	}

	// Now create all other columns
	others := generics.Filter(spec.Columns, func(i int, c *metadata.ColumnSpecification) bool {
		return !(c.IsPartitioningKey || c.IsClusteringKey)
	})
	for _, column := range others {
		if existingTable != nil && existingTable.Columns != nil {
			if _, ok := existingTable.Columns[column.Name]; ok {
				continue // this column already exists, we can skip
			}
		}
		addColumnStatement := fmt.Sprintf(`ALTER TABLE %v.%v ADD %v %v`,
			keyspace,
			spec.Name,
			column.Name,
			column.CQLType)
		commands = append(commands, metadata.DDLOperation{
			Description:  fmt.Sprintf("Extend the table %q with the column %q if needed.", spec.Name, column.Name),
			Command:      addColumnStatement,
			IgnoreErrors: []string{MessageColumnExists},
		})
	}

	// Now indexes
	keys := generics.Keys(spec.Indexes)
	sort.Strings(keys)

	var existingIndexes map[string]*gocql.IndexMetadata
	if existingMetadata != nil && existingMetadata.Indexes != nil {
		existingIndexes = existingMetadata.Indexes
	}

	for _, key := range keys {
		if existingIndexes != nil {
			if _, ok := existingIndexes[key]; ok {
				continue // this index already exists, we can skip
			}
		}
		col := spec.Indexes[key]
		createIndexStatement := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %v ON %v.%v (%v)",
			key,
			keyspace,
			spec.Name,
			col.Name)
		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Create an index called %q on the table %v for column %q", key, spec.Name, col.Name),
			Command:     createIndexStatement,
		})

	}

	return commands, nil
}

// getKeySpec creates the key specifier, i.e. "(pk1, pk2), ck1, ck2" that describes
// the physical order of the data
func getKeySpec(partitionCols []*metadata.PartitioningColumn, clusteringCols []*metadata.ClusteringColumn) string {
	keys := make([]string, len(partitionCols))
	for i, item := range partitionCols {
		keys[i] = item.Column.Name
	}

	keyString := "(" + strings.Join(keys, ", ") + ")"
	for _, item := range clusteringCols {
		keyString += ", " + item.Column.Name
	}

	return keyString
}

// getClusteringSuffix gets a WITH CLUSTERING ORDER clause if appropriate
func getClusteringSuffix(clusteringCols []*metadata.ClusteringColumn) string {
	sortStrings := []string{}
	for _, item := range clusteringCols {
		if item.Descending {
			sortStrings = append(sortStrings, fmt.Sprintf("%v DESC", item.Column.Name))
		} else {
			sortStrings = append(sortStrings, fmt.Sprintf("%v ASC", item.Column.Name))
		}
	}
	if len(sortStrings) == 0 {
		return ""
	}

	return " WITH CLUSTERING ORDER BY (" + strings.Join(sortStrings, ", ") + ")"
}
