package export

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/pingcap/dumpling/v4/log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	pd "github.com/pingcap/pd/v4/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func Dump(pCtx context.Context, conf *Config) (err error) {
	if err = adjustConfig(conf); err != nil {
		return withStack(err)
	}

	go func() {
		if conf.StatusAddr != "" {
			err1 := startDumplingService(conf.StatusAddr)
			if err1 != nil {
				log.Error("dumpling stops to serving service", zap.Error(err1))
			}
		}
	}()

	pool, err := sql.Open("mysql", conf.getDSN(""))
	if err != nil {
		return withStack(err)
	}
	defer pool.Close()

	conf.ServerInfo, err = detectServerInfo(pool)
	if err != nil {
		return withStack(err)
	}
	resolveAutoConsistency(conf)

	ctx, cancel := context.WithCancel(pCtx)
	defer cancel()

	var doPdGC bool
	var pdClient pd.Client
	if conf.ServerInfo.ServerType == ServerTypeTiDB && conf.ServerInfo.ServerVersion.Compare(*gcSafePointVersion) >= 0 {
		pdAddrs, err := GetPdAddrs(pool)
		if err != nil {
			return err
		}
		if len(pdAddrs) > 0 {
			doPdGC, err = checkSameCluster(ctx, pool, pdAddrs)
			if err != nil {
				log.Warn("meet error while check whether fetched pd addr and TiDB belongs to one cluster", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
			} else if doPdGC {
				pdClient, err = pd.NewClientWithContext(ctx, pdAddrs, pd.SecurityOption{})
				if err != nil {
					log.Warn("create pd client to control GC failed", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
					doPdGC = false
				}
			}
		}
	}

	if conf.Snapshot == "" && (doPdGC || conf.Consistency == "snapshot") {
		conn, err := pool.Conn(ctx)
		if err != nil {
			conn.Close()
			return withStack(err)
		}
		conf.Snapshot, err = getSnapshot(conn)
		conn.Close()
		if err != nil {
			return err
		}
	}

	if conf.Snapshot != "" {
		if conf.ServerInfo.ServerType != ServerTypeTiDB {
			return errors.New("snapshot consistency is not supported for this server")
		}
		if conf.Consistency == "snapshot" {
			hasTiKV, err := CheckTiDBWithTiKV(pool)
			if err != nil {
				return err
			}
			if hasTiKV {
				conf.SessionParams["tidb_snapshot"] = conf.Snapshot
			}
		}
	}

	if doPdGC {
		snapshotTS, err := parseSnapshotToTSO(pool, conf.Snapshot)
		if err != nil {
			return err
		}
		go updateServiceSafePoint(ctx, pdClient, defaultDumpGCSafePointTTL, snapshotTS)
	} else if conf.ServerInfo.ServerType == ServerTypeTiDB {
		log.Warn("If the amount of data to dump is large, criteria: (data more than 60GB or dumped time more than 10 minutes)\n" +
			"you'd better adjust the tikv_gc_life_time to avoid export failure due to TiDB GC during the dump process.\n" +
			"Before dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '720h' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n" +
			"After dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '10m' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n")
	}

	if newPool, err := resetDBWithSessionParams(pool, conf.getDSN(""), conf.SessionParams); err != nil {
		return withStack(err)
	} else {
		pool = newPool
	}

	m := newGlobalMetadata(conf.OutputDirPath)
	// write metadata even if dump failed
	defer m.writeGlobalMetaData()

	// for consistency lock, we should lock tables at first to get the tables we want to lock & dump
	// for consistency lock, record meta pos before lock tables because other tables may still be modified while locking tables
	if conf.Consistency == "lock" {
		conn, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			return err
		}
		m.recordStartTime(time.Now())
		err = m.recordGlobalMetaData(conn, conf.ServerInfo.ServerType)
		if err != nil {
			log.Info("get global metadata failed", zap.Error(err))
		}
		if err = prepareTableListToDump(conf, conn); err != nil {
			conn.Close()
			return err
		}
		conn.Close()
	}

	conCtrl, err := NewConsistencyController(ctx, conf, pool)
	if err != nil {
		return err
	}
	if err = conCtrl.Setup(ctx); err != nil {
		return err
	}

	connectPool, err := newConnectionsPool(ctx, conf.Threads, pool)
	if err != nil {
		return err
	}

	defer connectPool.Close()
	// for other consistencies, we should get table list after consistency is set up and GlobalMetaData is cached
	// for other consistencies, record snapshot after whole tables are locked. The recorded meta info is exactly the locked snapshot.
	if conf.Consistency != "lock" {
		m.recordStartTime(time.Now())
		conn := connectPool.getConn()
		err = m.recordGlobalMetaData(conn, conf.ServerInfo.ServerType)
		if err != nil {
			log.Info("get global metadata failed", zap.Error(err))
		}
		if err = prepareTableListToDump(conf, conn); err != nil {
			connectPool.releaseConn(conn)
			return err
		}
		connectPool.releaseConn(conn)
	}

	if err = conCtrl.TearDown(ctx); err != nil {
		return err
	}

	failpoint.Inject("ConsistencyCheck", nil)

	var writer Writer
	switch strings.ToLower(conf.FileType) {
	case "sql":
		writer, err = NewSimpleWriter(conf)
	case "csv":
		writer, err = NewCsvWriter(conf)
	}
	if err != nil {
		return err
	}

	if conf.Sql == "" {
		if err = dumpDatabases(ctx, conf, connectPool, writer); err != nil {
			return err
		}
	} else {
		if err = dumpSql(ctx, conf, connectPool, writer); err != nil {
			return err
		}
	}

	m.recordFinishTime(time.Now())
	return nil
}

func dumpDatabases(ctx context.Context, conf *Config, connectPool *connectionsPool, writer Writer) error {
	allTables := conf.Tables
	var g errgroup.Group
	for dbName, tables := range allTables {
		conn := connectPool.getConn()
		createDatabaseSQL, err := ShowCreateDatabase(conn, dbName)
		connectPool.releaseConn(conn)
		if err != nil {
			return err
		}
		if err := writer.WriteDatabaseMeta(ctx, dbName, createDatabaseSQL); err != nil {
			return err
		}

		if len(tables) == 0 {
			continue
		}
		for _, table := range tables {
			table := table
			conn := connectPool.getConn()
			tableDataIRArray, err := dumpTable(ctx, conf, conn, dbName, table, writer)
			connectPool.releaseConn(conn)
			if err != nil {
				return err
			}
			for _, tableIR := range tableDataIRArray {
				tableIR := tableIR
				g.Go(func() error {
					conn := connectPool.getConn()
					defer connectPool.releaseConn(conn)
					err := tableIR.Start(ctx, conn)
					if err != nil {
						return err
					}
					return writer.WriteTableData(ctx, tableIR)
				})
			}
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func prepareTableListToDump(conf *Config, pool *sql.Conn) error {
	databases, err := prepareDumpingDatabases(conf, pool)
	if err != nil {
		return err
	}

	conf.Tables, err = listAllTables(pool, databases)
	if err != nil {
		return err
	}

	if !conf.NoViews {
		views, err := listAllViews(pool, databases)
		if err != nil {
			return err
		}
		conf.Tables.Merge(views)
	}

	filterTables(conf)
	return nil
}

func dumpSql(ctx context.Context, conf *Config, connectPool *connectionsPool, writer Writer) error {
	conn := connectPool.getConn()
	tableIR, err := SelectFromSql(conf, conn)
	connectPool.releaseConn(conn)
	if err != nil {
		return err
	}

	return writer.WriteTableData(ctx, tableIR)
}

func dumpTable(ctx context.Context, conf *Config, db *sql.Conn, dbName string, table *TableInfo, writer Writer) ([]TableDataIR, error) {
	tableName := table.Name
	if !conf.NoSchemas {
		if table.Type == TableTypeView {
			viewName := table.Name
			createViewSQL, err := ShowCreateView(db, dbName, viewName)
			if err != nil {
				return nil, err
			}
			return nil, writer.WriteTableMeta(ctx, dbName, viewName, createViewSQL)
		}
		createTableSQL, err := ShowCreateTable(db, dbName, tableName)
		if err != nil {
			return nil, err
		}
		if err := writer.WriteTableMeta(ctx, dbName, tableName, createTableSQL); err != nil {
			return nil, err
		}
	}
	// Do not dump table data and return nil
	if conf.NoData {
		return nil, nil
	}

	if conf.Rows != UnspecifiedSize {
		finished, chunksIterArray, err := concurrentDumpTable(ctx, conf, db, dbName, tableName)
		if err != nil || finished {
			return chunksIterArray, err
		}
	}
	tableIR, err := SelectAllFromTable(conf, db, dbName, tableName)
	if err != nil {
		return nil, err
	}

	return []TableDataIR{tableIR}, nil
}

func concurrentDumpTable(ctx context.Context, conf *Config, db *sql.Conn, dbName string, tableName string) (bool, []TableDataIR, error) {
	// try dump table concurrently by split table to chunks
	chunksIterCh := make(chan TableDataIR, defaultDumpThreads)
	errCh := make(chan error, defaultDumpThreads)
	linear := make(chan struct{})

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	var g errgroup.Group
	chunksIterArray := make([]TableDataIR, 0)
	g.Go(func() error {
		splitTableDataIntoChunks(ctx1, chunksIterCh, errCh, linear, dbName, tableName, db, conf)
		return nil
	})

Loop:
	for {
		select {
		case <-ctx.Done():
			return true, chunksIterArray, nil
		case <-linear:
			return false, chunksIterArray, nil
		case chunksIter, ok := <-chunksIterCh:
			if !ok {
				break Loop
			}
			chunksIterArray = append(chunksIterArray, chunksIter)
		case err := <-errCh:
			return false, chunksIterArray, err
		}
	}
	if err := g.Wait(); err != nil {
		return true, chunksIterArray, err
	}
	return true, chunksIterArray, nil
}

func updateServiceSafePoint(ctx context.Context, pdClient pd.Client, ttl int64, snapshotTS uint64) {
	updateInterval := time.Duration(ttl/2) * time.Second
	tick := time.NewTicker(updateInterval)

	for {
		log.Debug("update PD safePoint limit with ttl",
			zap.Uint64("safePoint", snapshotTS),
			zap.Int64("ttl", ttl))
		for retryCnt := 0; retryCnt <= 10; retryCnt++ {
			_, err := pdClient.UpdateServiceGCSafePoint(ctx, dumplingServiceSafePointID, ttl, snapshotTS)
			if err == nil {
				break
			}
			log.Debug("update PD safePoint failed", zap.Error(err), zap.Int("retryTime", retryCnt))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
