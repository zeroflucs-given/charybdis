package mapping

import (
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-reflectx"
)

const (
	// TagNameCassandra is the tag that indicates a  column in our data
	TagNameCassandra = "cql"

	// TagNameExplicitType indicates a tag that lets us specify a custom CQL type
	// that does not depend on our mapping lookups. This allows users to use types
	// such as UDT's.
	TagNameExplicitType = "cqltype"

	// TagNamePartitioning indicates the tag name to use when identifying partitioning
	// keys within the table. The order of the partition values is the absolute value,
	// with negative numeric values indicating descending sorts.
	TagNamePartitioning = "cqlpartitioning"

	// TagNameSorting indicates the tag name to use when identifying clustering keys
	// within the table. The order of the clustering values is the absolute value, with
	// negative numeric values indicating descending sorts.
	TagNameSorting = "cqlclustering"

	// TagNameIndex indicates to create a named index over the table for a given column.
	// Scylla only supports a singular index.
	TagNameIndex = "cqlindex"
)

var tagMapper = reflectx.NewMapper(TagNameCassandra)

// Known types for referencing
var knownTypeBool = reflect.TypeFor[bool]()
var knownTypeInt32 = reflect.TypeFor[int32]()
var knownTypeInt64 = reflect.TypeFor[int64]()
var knownTypeInt = reflect.TypeFor[int]()
var knownTypeString = reflect.TypeFor[string]()
var knownTypeDuration = reflect.TypeFor[time.Duration]()
var knownTypeTime = reflect.TypeFor[time.Time]()
var knownTypeFloat32 = reflect.TypeFor[float32]()
var knownTypeFloat64 = reflect.TypeFor[float64]()
var knownTypeUUID = reflect.TypeFor[gocql.UUID]()
var knownTypeByteSlice = reflect.TypeFor[[]byte]()
