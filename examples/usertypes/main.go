package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/charybdis/generator"
	"github.com/zeroflucs-given/charybdis/mapping"
	"github.com/zeroflucs-given/charybdis/tables"
)

type Record struct {
	UserID    string     `cql:"user_id" cqlpartitioning:"1"`              // User ID - Partition key
	FirstName string     `cql:"first_name" cqlindex:"by_first_name"`      // Name, indexed
	LastName  string     `cql:"last_name"`                                // Last name
	Region    string     `cql:"region"`                                   // The region a user belongs to
	Locations []*Address `cql:"locations" cqltype:"frozen<set<address>>"` // Addresses associated with the user
}

type Address struct {
	Number string `cql:"number"`
	Street string `cql:"street"`
	City   string `cql:"city"`
}

func main() {
	testRecordsToProduce := 300

	hosts := []string{"127.0.0.1:9042"}

	ctx := context.TODO() // Replace with your app contexts
	cluster := func() *gocql.ClusterConfig {
		return gocql.NewCluster(hosts...)
	}
	log, _ := zap.NewDevelopment()

	// Example Part 1 - Creating a table manager with automatic DDL management
	manager, err := tables.NewTableManager[Record](ctx,
		tables.WithCluster(cluster),     // Used to create connections
		tables.WithLogger(log),          // Use a custom logger
		tables.WithKeyspace("examples"), // The keyspace the table belongs to
		mapping.WithAutomaticTableSpecification[Record]("user_with_address"), // Extract metadata from [Record] type
		generator.WithSimpleKeyspaceManagement(log, cluster, 1),              // Simple keyspace with RF1 (create if needed)
		generator.WithAutomaticTableManagement(log, cluster),                 // Create the table if needed
	)
	if err != nil {
		panic(err)
	}

	// Example Part 2 - Scan all records, removing them
	log.Info("Scanning records to remove old ones")
	errScan := manager.Scan(ctx, func(ctx context.Context, records []*Record, pageState []byte, newPageState []byte) (bool, error) {
		log.With(
			zap.Int("page_size", len(records))).
			Info("Clearing page....")
		for _, rec := range records {
			errDelete := manager.Delete(ctx, rec)
			if errDelete != nil {
				return false, errDelete
			}
		}
		return true, nil
	}, tables.WithPaging(100, nil))
	if errScan != nil {
		panic(errScan)
	}

	// Example Part 3 - Insert by Example
	log.Info("Inserting records")
	for i := 1; i <= testRecordsToProduce; i++ {
		if i%100 == 0 {
			log.With(zap.Int("progress", i)).Info("Insert progress")
		}
		errUpsert := manager.Insert(ctx, &Record{
			UserID:    fmt.Sprintf("test-user-%d", i),
			FirstName: fmt.Sprintf("User %d", i),
			Locations: []*Address{
				{
					Number: fmt.Sprintf("%d", rand.Uint()%1000),
					Street: pick(someStreets),
					City:   pick(someTowns),
				},
			},
		}, tables.WithTTL(time.Minute))
		if errUpsert != nil {
			panic(errUpsert)
		}
	}

	// Example Part 4 - Insert operations with conflicting keys will cause an ErrPreconditionFailed
	// to be returned.
	log.Info("Checking that duplicated inserts will error")
	errLWT := manager.Insert(ctx, &Record{
		UserID:    "test-user-1", // Matches the key above
		FirstName: "wrong",
	})
	if !errors.Is(errLWT, tables.ErrPreconditionFailed) {
		panic("Should have failed here")
	}

	// Example Part 4 - Read top record back by partition key
	log.Info("Reading back by partition key")
	record, err := manager.GetByPartitionKey(ctx, "test-user-1")
	if err != nil || record == nil {
		panic(err)
	}
}

func pick[S ~[]E, E any](s S) E {
	if len(s) == 0 {
		var z E
		return z
	}
	return s[rand.IntN(len(s))]
}

var someStreets = []string{
	"Lime Tree Way",
	"Savile Drive",
	"Landscape Road",
	"Iona Crescent",
	"Chase Close",
	"Estate Way",
	"Winter Row",
	"Railway Route",
	"Vista Boulevard",
	"Bright Street",
	"Amber Avenue",
	"Greenfield Way",
	"Silver Lane",
}

var someTowns = []string{
	"San Angelo",
	"Dartmouth",
	"Fort Pierce",
	"Sale",
	"Rogers",
	"Nottingham",
	"Carmel",
	"Sandiacre",
	"Mississauga",
	"Fort Scott",
	"Idaho City",
	"Irving",
	"Smith Center",
}
