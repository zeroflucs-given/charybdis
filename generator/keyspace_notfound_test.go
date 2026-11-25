package generator

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsKeyspaceNotFound covers the driver's string-flattened DESCRIBE error
func TestIsKeyspaceNotFound(t *testing.T) {
	flattened := fmt.Errorf("error querying keyspace schema: %v", errors.New("'unittest_content_1788597603160553430' not found in keyspaces"))
	require.True(t, isKeyspaceNotFound(flattened), "Flattened DESCRIBE error should read as keyspace-absent")
	require.False(t, isKeyspaceNotFound(errors.New("connection refused")), "Unrelated errors must not read as keyspace-absent")
	require.False(t, isKeyspaceNotFound(nil))
}
