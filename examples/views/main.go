package main

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/generator"
	"github.com/zeroflucs-given/charybdis/mapping"
	"github.com/zeroflucs-given/charybdis/tables"
	"go.uber.org/zap"
)

type Record struct {
	UserID    string `cql:"user_id" cqlpartitioning:"1"`         // User ID - Partition key
	FirstName string `cql:"first_name" cqlindex:"by_first_name"` // Name, indeexed
	Region    string `cql:"region"`                              // The region a user belongs to
	Visits    int    `cql:"visits"`                              // Our value
}

func main() {
	hosts := []string{"127.0.0.1:9042"}

	ctx := context.TODO() // Replace with your app contexts
	cluster := func() *gocql.ClusterConfig {
		return gocql.NewCluster(hosts...)
	}
	log, _ := zap.NewDevelopment()

	// Example Part 1 - Creating a table manager with automatic DDL management
	tableManager, err := tables.NewTableManager[Record](ctx,
		tables.WithCluster(cluster),                                    // Used to create connections
		tables.WithLogger(log),                                         // Use a custom logger
		tables.WithKeyspace("examples"),                                // The keyspace the table belongs to
		mapping.WithAutomaticTableSpecification[Record]("user_visits"), // Extract metadata from [Record] type
		generator.WithSimpleKeyspaceManagement(log, cluster, 1),        // Simple keyspace with RF1 (create if needed)
		generator.WithAutomaticTableManagement(log, cluster),           // Create the table if needed
	)
	if err != nil || tableManager == nil {
		panic(err)
	}

	// Example Part 2 - Creating a view manager with automatic DDL management
	viewManager, err := tables.NewViewManager[Record](ctx,
		tables.WithCluster(cluster),                                    // Used to create connections
		tables.WithLogger(log),                                         // Use a custom logger
		tables.WithKeyspace("examples"),                                // The keyspace the table belongs to
		mapping.WithAutomaticTableSpecification[Record]("user_visits"), // The table upon which the view is based
		mapping.WithSimpleView( // Create a view-spec from our table-spec
			"vw_regional",        // View name
			[]string{"region"},   // Partition by region
			[]string{"user_id"}), // Cluster by users
		generator.WithSimpleKeyspaceManagement(log, cluster, 1), // Simple keyspace with RF1 (create if needed)
		generator.WithAutomaticViewManagement(log, cluster),     // Create the view if needed
	)

	if err != nil || viewManager == nil {
		panic(err)
	}

}
