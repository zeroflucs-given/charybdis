package projections

import (
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/zeroflucs-given/charybdis/mapping"
	"github.com/zeroflucs-given/charybdis/metadata"
)

// extractPrimaryKey extracts the ordered primary key fields using reflection
func extractPrimaryKey(tableSpec *metadata.TableSpecification, instance any) ([]any, error) {
	if instance == nil {
		return nil, errors.New("invalid instance specifier")
	}

	tableSpec.Canonicalize()

	// Extract all fields
	columns := map[string]any{}
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: mapping.TagNameCassandra,
		Result:  &columns,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating decoer: %w", err)
	}
	errDecode := decoder.Decode(instance)
	if errDecode != nil {
		return nil, fmt.Errorf("error converting instance to map: %w", errDecode)
	}

	var results []any

	for _, col := range tableSpec.Partitioning {
		val, ok := columns[col.Column.Name]
		if !ok {
			return nil, fmt.Errorf("missing column from partition spec: %v", col.Column.Name)
		}
		results = append(results, val)
	}

	for _, col := range tableSpec.Clustering {
		val, ok := columns[col.Column.Name]
		if !ok {
			return nil, fmt.Errorf("missing column from clustering spec: %v", col.Column.Name)
		}
		results = append(results, val)
	}

	return results, nil
}
