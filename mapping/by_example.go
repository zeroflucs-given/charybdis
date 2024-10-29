package mapping

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/generics"
)

// CreateTableSpecificationFromExample creates a table specification object by example from a structure
func CreateTableSpecificationFromExample(name string, example any) (*metadata.TableSpecification, error) {
	t := reflect.TypeOf(example)
	if t == nil {
		return nil, errors.New("no_input_type_provided")
	}

	output := &metadata.TableSpecification{
		Name:    name,
		Indexes: make(map[string]*metadata.ColumnSpecification),
	}

	// Loop through all the columns with "cql" tags attached.
	structMap := tagMapper.TypeMap(t)
	for _, mappedField := range structMap.Index {
		if strings.Contains(mappedField.Path, ".") {
			continue // we don't include subtype paths as part of our structure
		}

		field := mappedField.Field

		columnName := getNameForField(field)
		if columnName == "" {
			continue
		}

		// Determine the type associated with this
		columnTypeString, errDetect := getTypeForField(field)
		if errDetect != nil {
			return nil, fmt.Errorf("could not detect type for %v: %w", columnName, errDetect)
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

// CreateTypeSpecificationsFromTableExample creates a type specification object by example from a table structure
func CreateTypeSpecificationsFromTableExample(example any) ([]*metadata.TypeSpecification, error) {
	types, err := CollectSubTypesFromType(reflect.TypeOf(example))
	if err != nil {
		return nil, err
	}

	var res []*metadata.TypeSpecification

	for typeName := range maps.Keys(types) {
		outputSpec, errSpec := createTypeSpecificationFromGoType(typeName, types[typeName])
		if errSpec != nil {
			return nil, errSpec
		}
		res = append(res, outputSpec)
	}

	return res, nil
}

// CreateTypeSpecificationFromExample creates a type specification object by example from a structure
func CreateTypeSpecificationFromExample(typeName string, example any) (*metadata.TypeSpecification, error) {
	t := reflect.TypeOf(example)
	return createTypeSpecificationFromGoType(typeName, t)
}

func createTypeSpecificationFromGoType(typeName string, goType reflect.Type) (*metadata.TypeSpecification, error) {
	outputType := &metadata.TypeSpecification{
		Name: typeName,
	}

	structMap := tagMapper.TypeMap(goType)
	for _, info := range structMap.Index {
		if strings.Contains(info.Path, ".") {
			continue // we don't include subtype paths as part of our structure
		}
		field := info.Field

		// Get the column name
		cqlTag := field.Tag.Get(TagNameCassandra)
		if cqlTag == "" {
			continue
		}

		columnName := strings.TrimSpace(strings.Split(cqlTag, ",")[0])
		if columnName == "" {
			continue
		}

		ft, err := getTypeForField(field)
		if err != nil {
			return nil, fmt.Errorf("could not detect type for %s.%s: %w", typeName, field.Name, err)
		}

		f := &metadata.FieldSpecification{
			Name:    columnName,
			CQLType: ft,
		}

		outputType.Fields = append(outputType.Fields, f)
	}

	return outputType, nil
}

func CollectSubTypesFromType(parentType reflect.Type) (map[string]reflect.Type, error) {
	all := make(map[string]reflect.Type)

	var containerKinds = []reflect.Kind{
		reflect.Ptr,
		reflect.Array,
		reflect.Map,
		reflect.Slice,
	}

	// Loop through all the columns with "cql" tags attached.
	structMap := tagMapper.TypeMap(parentType)
	for _, mappedField := range structMap.Index {
		field := mappedField.Field
		ft := field.Type

		// Reduce indirection to base type
		for slices.Contains(containerKinds, ft.Kind()) {
			ft = ft.Elem()
		}

		if ft.Kind() != reflect.Struct {
			continue
		}

		columnName := getNameForField(field)
		if columnName == "" {
			continue
		}

		var (
			columnType string
			err        error
		)

		if et := field.Tag.Get(TagNameExplicitType); et != "" {
			columnType, err = getBaseTypeForScyllaTag(et)
		} else {
			columnType, err = GetScyllaTypeForGoType(ft)
		}
		if err != nil {
			return nil, err
		}

		if slices.Contains(scyllaTypes, columnType) {
			continue // the type is a built-in one, so skip
		}

		all[columnType] = ft

		subtypes, err := CollectSubTypesFromType(ft)
		if err != nil {
			return nil, err
		}

		maps.Insert(all, maps.All(subtypes))
	}

	return all, nil
}

func getNameForField(field reflect.StructField) string {
	cqlTag := field.Tag.Get(TagNameCassandra)
	if cqlTag == "" {
		return ""
	}
	return strings.TrimSpace(strings.Split(cqlTag, ",")[0])
}

// getTypeForField returns the scylla type to use for a given struct field
func getTypeForField(field reflect.StructField) (string, error) {
	explicitType := field.Tag.Get(TagNameExplicitType)
	if explicitType != "" {
		return explicitType, nil
	}
	return GetScyllaTypeForGoType(field.Type)
}

func getBaseTypesForScyllaTag(tag string) ([]string, error) {
	subs := strings.Count(tag, "<") + strings.Count(tag, ",")
	if subs == 0 {
		if strings.Contains(tag, ">") {
			return nil, fmt.Errorf("mismatched brackets in %s", tag)
		}
		return []string{tag}, nil
	}

	buf := make([]bytes.Buffer, 0, subs)
	buf = append(buf, bytes.Buffer{})

	level := 0
	for _, r := range tag {
		switch {
		case r == '<':
			level++
			buf[len(buf)-1].Reset() // been capturing container type up to here
		case r == '>':
			level--
			if level == 0 {
				break
			}
		case r == ',':
			buf = append(buf, bytes.Buffer{}) // tuple/map next field
		case level > 0:
			buf[len(buf)-1].WriteRune(r)
		}
	}

	if level != 0 {
		return nil, fmt.Errorf("mismatched brackets in %s", tag)
	}

	return generics.Map(buf, func(i int, b bytes.Buffer) string {
		return b.String()
	}), nil
}

// Returns the last type found in a complex scylla type.
// Only tuples will have more than 1 type that we care about, and they can only be expressed as a map[K,any],
// so they'll need explict definition of their type elsewhere
func getBaseTypeForScyllaTag(tag string) (string, error) {
	t, err := getBaseTypesForScyllaTag(tag)
	if err != nil {
		return "", err
	}
	if len(t) == 0 {
		return "", nil
	}
	return t[len(t)-1], nil
}
