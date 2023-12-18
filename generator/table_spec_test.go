package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeroflucs-given/charybdis/generator"
	"github.com/zeroflucs-given/charybdis/metadata"
)

func TestGenerateTableDDLRequiresSpec(t *testing.T) {
	// Arrange
	var spec *metadata.TableSpecification

	// Act
	ddl, err := generator.CreateDDLFromTableSpecification("test_keyspace", spec, nil)

	// Assert
	require.Nil(t, ddl, "Should not get any DDL back")
	require.ErrorIs(t, err, generator.ErrInvalidInput)
}

// TestGenerateTableDDL checks we we can create the DDL we expect for a table from a few elements
func TestGenerateTableDDL(t *testing.T) {
	// Arrange
	colUser := &metadata.ColumnSpecification{
		Name:              "user_id",
		CQLType:           "varchar",
		IsPartitioningKey: true,
	}
	colEmail := &metadata.ColumnSpecification{
		Name:    "email_address",
		CQLType: "varchar",
	}
	colTime := &metadata.ColumnSpecification{
		Name:            "change_time",
		CQLType:         "timestamp",
		IsClusteringKey: true,
	}
	tableSpec := &metadata.TableSpecification{
		Name: "email_changes",
		Columns: []*metadata.ColumnSpecification{
			colUser,
			colEmail,
			colTime,
		},
		Partitioning: []*metadata.PartitioningColumn{
			{
				Column: colUser,
				Order:  1,
			},
		},
		Clustering: []*metadata.ClusteringColumn{
			{
				Column:     colTime,
				Order:      1,
				Descending: true,
			},
		},
	}

	// Act
	ddl, errDDL := generator.CreateDDLFromTableSpecification("test_keyspace", tableSpec, nil)

	// Assert
	expected := []metadata.DDLOperation{
		{
			Description: `Create the table "email_changes" with columns relating to the key.`,
			Command:     "CREATE TABLE test_keyspace.email_changes (user_id VARCHAR, change_time TIMESTAMP) WITH CLUSTERING ORDER BY (change_time DESC)",
		},
		{
			Description: `Extend the table "email_changes" with the column "email_address" if needed.`,
			Command:     "ALTER TABLE test_keyspace.email_changes ADD email_address varchar DESC",
			IgnoreErrors: []string{
				generator.MessageColumnExists,
			},
		},
	}
	require.NoError(t, errDDL, "Should not error generating DDL")
	require.Len(t, ddl, len(expected))
}

// TestGenerateTableDDLNoClustering checks we can create a table with just a partitioning key
func TestGenerateTableDDLNoClustering(t *testing.T) {
	// Arrange
	colUser := &metadata.ColumnSpecification{
		Name:              "user_id",
		CQLType:           "varchar",
		IsPartitioningKey: true,
	}
	colEmail := &metadata.ColumnSpecification{
		Name:    "email_address",
		CQLType: "varchar",
	}
	tableSpec := &metadata.TableSpecification{
		Name: "current_email",
		Columns: []*metadata.ColumnSpecification{
			colUser,
			colEmail,
		},
		Partitioning: []*metadata.PartitioningColumn{
			{
				Column: colUser,
				Order:  1,
			},
		},
	}

	// Act
	ddl, errDDL := generator.CreateDDLFromTableSpecification("test_keyspace", tableSpec, nil)

	// Assert
	expected := []metadata.DDLOperation{
		{
			Description: `Create the table "email_changes" with columns relating to the key.`,
			Command:     "CREATE TABLE test_keyspace.email_changes (user_id VARCHAR)",
		},
		{
			Description: `Extend the table "email_changes2" with the column "email_address" if needed.`,
			Command:     "ALTER TABLE test_keyspace.email_changes ADD email_address varchar DESC",
			IgnoreErrors: []string{
				generator.MessageColumnExists,
			},
		},
	}
	require.NoError(t, errDDL, "Should not error generating DDL")
	require.Len(t, ddl, len(expected))
}
