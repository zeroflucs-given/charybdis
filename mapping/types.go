package mapping

import (
	"fmt"
	"reflect"
	"strings"
)

// GetScyllaTypeForGoType determines the type to use in Scylla
func GetScyllaTypeForGoType(t reflect.Type) (string, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t {
	case knownTypeBool:
		return "boolean", nil
	case knownTypeString:
		return "text", nil
	case knownTypeUUID:
		return "uuid", nil
	case knownTypeInt64:
		return "bigint", nil
	case knownTypeInt:
		return "int", nil
	case knownTypeInt32:
		return "int", nil
	case knownTypeFloat32:
		return "float", nil
	case knownTypeFloat64:
		return "double", nil
	case knownTypeTime:
		return "timestamp", nil
	case knownTypeDuration:
		return "bigint", nil
	case knownTypeByteSlice:
		return "blob", nil
	}

	switch t.Kind() {
	case reflect.Struct:
		return strings.ToLower(t.Name()), nil
	default:
		// unhandled
	}

	return "", fmt.Errorf("unknown type: %v (%s)", t.String(), t.Kind())
}

var scyllaTypes = []string{
	"ascii",
	"bigint",
	"blob",
	"boolean",
	"counter",
	"date",
	"decimal",
	"double",
	"float",
	"frozen",
	"inet",
	"int",
	"list",
	"map",
	"set",
	"smallint",
	"text",
	"time",
	"timestamp",
	"timeuuid",
	"tinyint",
	"tuple",
	"uuid",
	"varchar",
	"varint",
}
