package tables

import (
	"fmt"
	"slices"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"github.com/scylladb/gocqlx/v3/table"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// baseManagerImpl is our underlying base manager implementation type, common to views and tables
type baseManagerImpl[T any] struct {
	Name            string               // Name of object
	Session         gocqlx.Session       // Session
	Logger          *zap.Logger          // Logger
	Tracer          trace.Tracer         // OpenTelemetry tracer
	DoTracing       bool                 // Do we want to add tracing to operations
	TraceAttributes []attribute.KeyValue // Common trace attributes
	Table           *table.Table         // Table helper
	TableMetadata   table.Metadata       // Table metadata

	// Helper data
	readConsistency        gocql.Consistency // Read consistency
	qualifiedTableName     string            // Qualified table-name
	allColumnNames         []string          // Set of all column names
	nonKeyColumns          []string          // Non-key column names
	partitionKeyPredicates []qb.Cmp          // Partition key predicates
	allKeyPredicates       []qb.Cmp          // All key predicates, including partition key, in order
	queryTimeout           time.Duration     // Timeout for queries - copied through from the Session settings

	// Precomputed statements for fixed-shape operations
	getByPartitionStmt  string   // Select by partition key
	getByPartitionNames []string // Binding names for getByPartitionStmt
	getByPrimaryStmt    string   // Select by full primary key
	getByPrimaryNames   []string // Binding names for getByPrimaryStmt

	codec RowCodec[T] // Generated row codec; nil falls back to reflection-based mapping
}

// precomputeStatements builds the statement text for operations whose shape never varies,
// so per-call paths can skip the query-builder when no options are passed.
func (t *baseManagerImpl[T]) precomputeStatements() {
	t.getByPartitionStmt, t.getByPartitionNames = t.selectBuilder().Where(t.partitionKeyPredicates...).ToCql()
	t.getByPrimaryStmt, t.getByPrimaryNames = t.selectBuilder().Where(t.allKeyPredicates...).ToCql()
}

// selectBuilder starts a select statement, naming columns explicitly when a codec will scan
// the rows so that column order is deterministic rather than schema-defined.
func (t *baseManagerImpl[T]) selectBuilder() *qb.SelectBuilder {
	builder := qb.Select(t.Table.Name())
	if t.codec != nil {
		builder = builder.Columns(t.allColumnNames...)
	}
	return builder
}

// adoptCodec validates and installs a row codec; a codec that does not match the managed type
// or the specification's column set is a misconfiguration and fails construction.
func (t *baseManagerImpl[T]) adoptCodec(codec any) error {
	if codec == nil {
		return nil
	}
	typed, ok := codec.(RowCodec[T])
	if !ok {
		return fmt.Errorf("row codec for %s does not match the managed row type", t.Name)
	}
	if !slices.Equal(typed.Columns(), t.allColumnNames) {
		return fmt.Errorf("row codec for %s was generated against columns %v, table specification has %v",
			t.Name, typed.Columns(), t.allColumnNames)
	}
	t.codec = typed
	return nil
}

// QualifiedName gets the keyspace-qualified name of the table or view
func (t *baseManagerImpl[T]) QualifiedName() string {
	return t.qualifiedTableName
}
