package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/generator"
	"github.com/zeroflucs-given/charybdis/metadata"
)

func TestGenerateTypeDDLRequiresSpec(t *testing.T) {
	// Arrange
	var spec *metadata.TypeSpecification

	// Act
	ddl, err := generator.CreateDDLFromTypeSpecification("test_keyspace", spec, nil)

	// Assert
	require.Nil(t, ddl, "Should not get any DDL back")
	require.ErrorIs(t, err, generator.ErrInvalidInput)
}

// TestGenerateTableDDL checks we we can create the DDL we expect for a table from a few elements
func TestGenerateTypeDDL(t *testing.T) {
	// Arrange
	colUser := &metadata.FieldSpecification{
		Name:    "user_id",
		CQLType: "varchar",
	}
	colEmail := &metadata.FieldSpecification{
		Name:    "email_address",
		CQLType: "varchar",
	}
	colTime := &metadata.FieldSpecification{
		Name:    "change_time",
		CQLType: "timestamp",
	}
	typeSpec := &metadata.TypeSpecification{
		Name: "email_changes",
		Fields: []*metadata.FieldSpecification{
			colUser,
			colEmail,
			colTime,
		},
	}

	// Act
	ddl, errDDL := generator.CreateDDLFromTypeSpecification("test_keyspace", typeSpec, nil)

	// Assert
	expected := []metadata.DDLOperation{
		{
			Description: `Create the table "email_changes" with columns relating to the key.`,
			Command:     "CREATE TYPE test_keyspace.email_changes (user_id varchar)",
		},
		{
			Description: `Extend the table "email_changes" with the column "email_address" if needed.`,
			Command:     "ALTER TABLE test_keyspace.email_changes ADD email_address varchar",
			IgnoreErrors: []string{
				generator.MessageColumnExists,
			},
		},
		{
			Description: `Extend the table "email_changes" with the column "email_address" if needed.`,
			Command:     "ALTER TABLE test_keyspace.email_changes ADD change_time timestamp",
			IgnoreErrors: []string{
				generator.MessageColumnExists,
			},
		},
	}
	require.NoError(t, errDDL, "Should not error generating DDL")
	require.Len(t, ddl, len(expected))
}
