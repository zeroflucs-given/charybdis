package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/zeroflucs-given/charybdis/generator"
	"github.com/zeroflucs-given/charybdis/mapping"
	"github.com/zeroflucs-given/charybdis/tables"
)

// Example that triggers the concurrent write timeout (may need to be run a few times)

const (
	numRecords    int = 1000
	numConcurrent int = 10
	numUpdates    int = 1000
)

var log *zap.Logger

var hosts = []string{"127.0.0.1:9041", "127.0.0.1:9042", "127.0.0.1:9043"}

type Record struct {
	ID    int `cql:"id" cqlpartitioning:"1"`
	Thing int `cql:"thing" cqlclustering:"1"`
	Value int `cql:"value"`
}

func main() {
	logCfg := zap.NewDevelopmentConfig()
	logCfg.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	log, _ = logCfg.Build()

	prep(hosts)

	nodes := Map(hosts, newSingleNodeConn)

	log.Info("performing concurrent writes", zap.Int("num", numConcurrent))

	ctx := context.Background()
	grp, gCtx := errgroup.WithContext(ctx)
	grp.SetLimit(numConcurrent)

	for i := 0; i < numUpdates; i++ {
		grp.Go(
			func() error {
				n := rand.Int() % 3
				return updateRandom(gCtx, nodes[n])
			},
		)
	}

	err := grp.Wait()
	if err != nil {
		panic(err)
	}
	log.Info("done")

}

func Map[S ~[]E, E, V any](s S, fn func(E) V) []V {
	var res []V
	for _, e := range s {
		res = append(res, fn(e))
	}
	return res
}

func prep(hosts []string) {
	m := newManager(hosts...)
	deleteAll(m)
	createSamples(m)
}

func newSingleNodeConn(host string) tables.TableManager[Record] {
	return newManager(host)
}

func newManager(hosts ...string) tables.TableManager[Record] {
	ctx := context.Background()

	cluster := func() *gocql.ClusterConfig {
		cfg := gocql.NewCluster(hosts...)
		return cfg
	}

	manager, err := tables.NewTableManager[Record](
		ctx,
		tables.WithCluster(cluster),
		tables.WithLogger(log),
		tables.WithKeyspace("some_keyspace"),
		mapping.WithAutomaticTableSpecification[Record]("some_table"),
		generator.WithSimpleKeyspaceManagement(log, cluster, 3),
		generator.WithAutomaticTableManagement(log, cluster),
	)
	if err != nil {
		panic(err)
	}

	return manager
}

func updateRandom(ctx context.Context, manager tables.TableManager[Record]) error {
	st := time.Now()
	uid := rand.Int() % numRecords

	r, errRead := manager.GetByPrimaryKey(ctx, 7, uid)
	if errRead != nil {
		return fmt.Errorf("failed to fetch id %d: %w", uid, errRead)
	}

	r.Value++

	errUpdate := manager.Update(ctx, r)
	if errUpdate != nil {
		log.Info("have error on update", zap.Duration("execution_time", time.Since(st)), zap.Error(errUpdate))
		return nil
	}

	return nil
}

func createSamples(manager tables.TableManager[Record]) {
	ctx := context.Background()

	log.Info("Inserting records")
	for i := 0; i < numRecords; i++ {
		if (i+1)%1000 == 0 {
			log.With(zap.Int("progress", i+1)).Info("Insert progress")
		}
		errUpsert := manager.Insert(ctx, &Record{
			ID:    7,
			Thing: i,
			Value: 0,
		}, tables.WithTTL(time.Minute))
		if errUpsert != nil {
			panic(errUpsert)
		}
	}
}

func deleteAll(manager tables.TableManager[Record]) {
	ctx := context.Background()

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
	}, tables.WithPaging(1000, nil))
	if errScan != nil {
		panic(errScan)
	}
}
