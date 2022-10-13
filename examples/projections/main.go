package main

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/generator"
	"github.com/zeroflucs-given/charybdis/mapping"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/projections"
	"github.com/zeroflucs-given/charybdis/tables"
	"go.uber.org/zap"
)

type Record struct {
	UserID    string `cql:"user_id" cqlpartitioning:"1"`         // User ID - Partition key
	FirstName string `cql:"first_name" cqlindex:"by_first_name"` // Name, indexed
	LastName  string `cql:"last_name"`                           // Last name
	Region    string `cql:"region"`                              // The region a user belongs to
	Visits    int    `cql:"visits"`                              // Our value
}

func main() {
	hosts := []string{"127.0.0.1:9042"}

	ctx := context.TODO() // Replace with your app contexts
	cluster := gocql.NewCluster(hosts...)
	log, _ := zap.NewDevelopment()

	// Example Part 1 - Creating a table manager with automatic DDL management
	manager, err := tables.NewTableManager[Record](ctx,
		tables.WithCluster(cluster),                                    // Used to create connections
		tables.WithLogger(log),                                         // Use a custom logger
		tables.WithKeyspace("examples"),                                // The keyspace the table belongs to
		mapping.WithAutomaticTableSpecification[Record]("user_visits"), // Extract metadata from [Record] type
		generator.WithSimpleKeyspaceManagement(log, cluster, 1),        // Simple keyspace with RF1 (create if needed)
		generator.WithAutomaticTableManagement(log, cluster),           // Create the table if needed
	)
	if err != nil {
		panic(err)
	}

	// Example Part 2 - Add a projection manager, and register it on the table-manager.
	// We're going to project into a tree of Region, Last Name, First Name. All other
	// fields are laid out as leaf-level fields.
	log.Info("Starting projection manager")
	proj, err := projections.NewProjectionManager[Record](ctx,
		projections.WithCluster(cluster),
		projections.WithLogger(log),
		projections.WithKeyspace("examples"),
		projections.WithBaseTable(manager.GetTableSpec()),
		projections.WithTrackedNonKeyColumns(
			"region", "last_name", "first_name",
		),
		projections.WithSimpleProjection(&projections.ProjectionSpecification{
			Name: "users_lookup",
			Partitioning: []*metadata.PartitioningColumnLookup{
				{Column: "region", Order: 1},
			},
			Clustering: []*metadata.ClusteringColumnLookup{
				{Column: "last_name", Order: 1},
				{Column: "first_name", Order: 2},
				{Column: "user_id", Order: 3},
			},
		}),
	)
	if err != nil {
		panic(err)
	}
	manager.AddPreDeleteHook(proj.ProcessDelete)
	manager.AddPostChangeHook(proj.ProcessChange)

	// Example Part 3 - Write to base table
	log.Info("Writing to base table")
	errWrite := manager.Upsert(ctx, &Record{
		UserID:    "test-user",
		FirstName: "John",
		LastName:  "Smith",
		Region:    "APAC",
		Visits:    64,
	})
	if errWrite != nil {
		panic(errWrite)
	}

	// Query back out. We can Select, Scan, etc from this projection
	lookup, err := proj.Projection("users_lookup").GetByPartitionKey(ctx, "APAC")
	if err != nil {
		panic(err)
	}

	log.With(zap.Any("result", lookup)).Info("Fetched from projection")
}
