package mapping

import (
	"fmt"
	"reflect"
)

// GetScyllaTypeForType determines the type to use in Scylla
func GetScyllaTypeForType(t reflect.Type) (string, error) {
	if t.Kind() == reflect.Ptr {
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
	case knownTypeByteSlice:
		return "blob", nil
	}
	return "", fmt.Errorf("unknown type: %v", t.String())
}
