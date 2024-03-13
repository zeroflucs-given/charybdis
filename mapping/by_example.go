package mapping

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/zeroflucs-given/charybdis/metadata"
)

// CreateTableSpecificationFromExample creates a table specification object by example from a structure
func CreateTableSpecificationFromExample(name string, example any) (*metadata.TableSpecification, error) {
	t := reflect.TypeOf(example)
	if t == nil {
		return nil, errors.New("no_input_type_provided")
	}

	output := &metadata.TableSpecification{
		Name:    name,
		Indexes: map[string]*metadata.ColumnSpecification{},
	}

	// Loop through all the columns with "cql" tags attached.
	structMap := tagMapper.TypeMap(t)
	for _, mappedField := range structMap.Index {
		field := mappedField.Field

		// Part 1 - Get the column name
		cqlTag := field.Tag.Get(TagNameCassandra)
		if cqlTag == "" {
			continue
		}
		columnName := strings.TrimSpace(strings.Split(cqlTag, ",")[0])
		if columnName == "" {
			continue
		}

		// Determine the type associated with this
		var columnTypeString string
		explicitType := field.Tag.Get(TagNameExplicitType)
		if explicitType != "" {
			columnTypeString = explicitType
		} else {
			detected, errDetect := GetScyllaTypeForType(field.Type)
			if errDetect != nil {
				return nil, fmt.Errorf("could not detect type for %v: %w", columnName, errDetect)
			}
			columnTypeString = detected
		}

		// We've now got a column
		columnSpec := &metadata.ColumnSpecification{
			Name:    columnName,
			CQLType: columnTypeString,
		}
		output.Columns = append(output.Columns, columnSpec)

		// Partitioning
		partitionVal := field.Tag.Get(TagNamePartitioning)
		if partitionVal != "" {
			order, errParse := strconv.ParseInt(partitionVal, 10, 64)
			if errParse != nil {
				return nil, fmt.Errorf("error parsing partition struct value %v: %w", partitionVal, errParse)
			}

			columnSpec.IsPartitioningKey = true

			output.Partitioning = append(output.Partitioning, &metadata.PartitioningColumn{
				Column: columnSpec,
				Order:  int(order),
			})
		}

		// Sort key
		sortVal := field.Tag.Get(TagNameSorting)
		if sortVal != "" {
			order, errParse := strconv.ParseInt(sortVal, 10, 64)
			if errParse != nil {
				return nil, fmt.Errorf("error parsing sort struct value %v: %w", sortVal, errParse)
			}

			columnSpec.IsClusteringKey = true

			output.Clustering = append(output.Clustering, &metadata.ClusteringColumn{
				Column:     columnSpec,
				Order:      int(math.Abs(float64(order))),
				Descending: order < 0,
			})
		}

		// Index?
		indexVal := field.Tag.Get(TagNameIndex)
		if indexVal != "" {
			output.Indexes[indexVal] = columnSpec
		}

	}

	return output, nil
}
