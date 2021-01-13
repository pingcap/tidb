// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/pingcap/dumpling/v4/log"

	// import mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Dumper is the dump progress structure
type Dumper struct {
	ctx       context.Context
	conf      *Config
	cancelCtx context.CancelFunc

	extStore storage.ExternalStorage
	dbHandle *sql.DB

	tidbPDClientForGC pd.Client
}

// NewDumper returns a new Dumper
func NewDumper(ctx context.Context, conf *Config) (*Dumper, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	d := &Dumper{
		ctx:       ctx,
		conf:      conf,
		cancelCtx: cancelFn,
	}
	err := adjustConfig(conf,
		initLogger,
		registerTLSConfig,
		validateSpecifiedSQL,
		validateFileFormat)
	if err != nil {
		return nil, err
	}
	err = runSteps(d,
		createExternalStore,
		startHTTPService,
		openSQLDB,
		detectServerInfo,
		resolveAutoConsistency,

		tidbSetPDClientForGC,
		tidbGetSnapshot,
		tidbStartGCSavepointUpdateService,

		setSessionParam)
	return d, err
}

// Dump dumps table from database
// nolint: gocyclo
func (d *Dumper) Dump() (dumpErr error) {
	initColTypeRowReceiverMap()
	var (
		conn    *sql.Conn
		err     error
		conCtrl ConsistencyController
	)
	ctx, conf, pool := d.ctx, d.conf, d.dbHandle
	m := newGlobalMetadata(d.extStore, conf.Snapshot)
	defer func() {
		if dumpErr == nil {
			_ = m.writeGlobalMetaData(ctx)
		}
	}()

	// for consistency lock, we should get table list at first to generate the lock tables SQL
	if conf.Consistency == consistencyTypeLock {
		conn, err = createConnWithConsistency(ctx, pool)
		if err != nil {
			return errors.Trace(err)
		}
		if err = prepareTableListToDump(conf, conn); err != nil {
			conn.Close()
			return err
		}
		conn.Close()
	}

	conCtrl, err = NewConsistencyController(ctx, conf, pool)
	if err != nil {
		return err
	}
	if err = conCtrl.Setup(ctx); err != nil {
		return errors.Trace(err)
	}
	// To avoid lock is not released
	defer func() {
		err = conCtrl.TearDown(ctx)
		if err != nil {
			log.Error("fail to tear down consistency controller", zap.Error(err))
		}
	}()

	metaConn, err := createConnWithConsistency(ctx, pool)
	if err != nil {
		return err
	}
	defer metaConn.Close()
	m.recordStartTime(time.Now())
	// for consistency lock, we can write snapshot info after all tables are locked.
	// the binlog pos may changed because there is still possible write between we lock tables and write master status.
	// but for the locked tables doing replication that starts from metadata is safe.
	// for consistency flush, record snapshot after whole tables are locked. The recorded meta info is exactly the locked snapshot.
	// for consistency snapshot, we should use the snapshot that we get/set at first in metadata. TiDB will assure the snapshot of TSO.
	// for consistency none, the binlog pos in metadata might be earlier than dumped data. We need to enable safe-mode to assure data safety.
	err = m.recordGlobalMetaData(metaConn, conf.ServerInfo.ServerType, false)
	if err != nil {
		log.Info("get global metadata failed", zap.Error(err))
	}

	// for other consistencies, we should get table list after consistency is set up and GlobalMetaData is cached
	if conf.Consistency != consistencyTypeLock {
		if err = prepareTableListToDump(conf, metaConn); err != nil {
			return err
		}
	}

	rebuildConn := func(conn *sql.Conn) (*sql.Conn, error) {
		// make sure that the lock connection is still alive
		err1 := conCtrl.PingContext(ctx)
		if err1 != nil {
			return conn, errors.Trace(err1)
		}
		// give up the last broken connection
		conn.Close()
		newConn, err1 := createConnWithConsistency(ctx, pool)
		if err1 != nil {
			return conn, errors.Trace(err1)
		}
		conn = newConn
		// renew the master status after connection. dm can't close safe-mode until dm reaches current pos
		if conf.PosAfterConnect {
			err1 = m.recordGlobalMetaData(conn, conf.ServerInfo.ServerType, true)
			if err1 != nil {
				return conn, errors.Trace(err1)
			}
		}
		return conn, nil
	}

	taskChan := make(chan Task, defaultDumpThreads)
	taskChannelCapacity.With(conf.Labels).Add(defaultDumpThreads)
	wg, writingCtx := errgroup.WithContext(ctx)
	writers, tearDownWriters, err := d.startWriters(writingCtx, wg, taskChan, rebuildConn)
	if err != nil {
		return err
	}
	defer tearDownWriters()

	if conf.TransactionalConsistency {
		if conf.Consistency == consistencyTypeFlush || conf.Consistency == consistencyTypeLock {
			log.Info("All the dumping transactions have started. Start to unlock tables")
		}
		if err = conCtrl.TearDown(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	// Inject consistency failpoint test after we release the table lock
	failpoint.Inject("ConsistencyCheck", nil)

	if conf.PosAfterConnect {
		// record again, to provide a location to exit safe mode for DM
		err = m.recordGlobalMetaData(metaConn, conf.ServerInfo.ServerType, true)
		if err != nil {
			log.Info("get global metadata (after connection pool established) failed", zap.Error(err))
		}
	}

	summary.SetLogCollector(summary.NewLogCollector(log.Info))
	summary.SetUnit(summary.BackupUnit)
	defer summary.Summary(summary.BackupUnit)

	logProgressCtx, logProgressCancel := context.WithCancel(ctx)
	go d.runLogProgress(logProgressCtx)
	defer logProgressCancel()

	tableDataStartTime := time.Now()

	failpoint.Inject("PrintTiDBMemQuotaQuery", func(_ failpoint.Value) {
		row := d.dbHandle.QueryRowContext(ctx, "select @@tidb_mem_quota_query;")
		var s string
		err = row.Scan(&s)
		if err != nil {
			fmt.Println(errors.Trace(err))
		} else {
			fmt.Printf("tidb_mem_quota_query == %s\n", s)
		}
	})

	if conf.SQL == "" {
		if err = d.dumpDatabases(metaConn, taskChan); err != nil {
			return err
		}
	} else {
		d.dumpSQL(taskChan)
	}
	close(taskChan)
	if err := wg.Wait(); err != nil {
		summary.CollectFailureUnit("dump table data", err)
		return errors.Trace(err)
	}
	summary.CollectSuccessUnit("dump cost", countTotalTask(writers), time.Since(tableDataStartTime))

	summary.SetSuccessStatus(true)
	m.recordFinishTime(time.Now())
	return nil
}

func (d *Dumper) startWriters(ctx context.Context, wg *errgroup.Group, taskChan <-chan Task,
	rebuildConnFn func(*sql.Conn) (*sql.Conn, error)) ([]*Writer, func(), error) {
	conf, pool := d.conf, d.dbHandle
	writers := make([]*Writer, conf.Threads)
	for i := 0; i < conf.Threads; i++ {
		conn, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			return nil, func() {}, err
		}
		writer := NewWriter(ctx, int64(i), conf, conn, d.extStore)
		writer.rebuildConnFn = rebuildConnFn
		writer.setFinishTableCallBack(func(task Task) {
			if td, ok := task.(*TaskTableData); ok {
				finishedTablesCounter.With(conf.Labels).Inc()
				log.Debug("finished dumping table data",
					zap.String("database", td.Meta.DatabaseName()),
					zap.String("table", td.Meta.TableName()))
			}
		})
		writer.setFinishTaskCallBack(func(task Task) {
			taskChannelCapacity.With(conf.Labels).Inc()
		})
		wg.Go(func() error {
			return writer.run(taskChan)
		})
		writers[i] = writer
	}
	tearDown := func() {
		for _, w := range writers {
			w.conn.Close()
		}
	}
	return writers, tearDown, nil
}

func (d *Dumper) dumpDatabases(metaConn *sql.Conn, taskChan chan<- Task) error {
	conf := d.conf
	allTables := conf.Tables
	for dbName, tables := range allTables {
		createDatabaseSQL, err := ShowCreateDatabase(metaConn, dbName)
		if err != nil {
			return err
		}
		task := NewTaskDatabaseMeta(dbName, createDatabaseSQL)
		d.sendTaskToChan(task, taskChan)

		for _, table := range tables {
			log.Debug("start dumping table...", zap.String("database", dbName),
				zap.String("table", table.Name))
			meta, err := dumpTableMeta(conf, metaConn, dbName, table)
			if err != nil {
				return err
			}

			if table.Type == TableTypeView {
				task := NewTaskViewMeta(dbName, table.Name, meta.ShowCreateTable(), meta.ShowCreateView())
				d.sendTaskToChan(task, taskChan)
			} else {
				task := NewTaskTableMeta(dbName, table.Name, meta.ShowCreateTable())
				d.sendTaskToChan(task, taskChan)
				err = d.dumpTableData(metaConn, meta, taskChan)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (d *Dumper) dumpTableData(conn *sql.Conn, meta TableMeta, taskChan chan<- Task) error {
	conf := d.conf
	if conf.NoData {
		return nil
	}
	if conf.Rows == UnspecifiedSize {
		return d.sequentialDumpTable(conn, meta, taskChan)
	}
	return d.concurrentDumpTable(conn, meta, taskChan)
}

func (d *Dumper) sequentialDumpTable(conn *sql.Conn, meta TableMeta, taskChan chan<- Task) error {
	conf := d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()
	tableIR, err := SelectAllFromTable(conf, conn, db, tbl)
	if err != nil {
		return err
	}
	task := NewTaskTableData(meta, tableIR, 0, 1)
	d.sendTaskToChan(task, taskChan)

	return nil
}

func (d *Dumper) concurrentDumpTable(conn *sql.Conn, meta TableMeta, taskChan chan<- Task) error {
	conf := d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()
	if conf.ServerInfo.ServerType == ServerTypeTiDB &&
		conf.ServerInfo.ServerVersion != nil &&
		conf.ServerInfo.ServerVersion.Compare(*tableSampleVersion) >= 0 {
		log.Debug("dumping TiDB tables with TABLESAMPLE",
			zap.String("database", db), zap.String("table", tbl))
		return d.concurrentDumpTiDBTables(conn, meta, taskChan)
	}
	field, err := pickupPossibleField(db, tbl, conn, conf)
	if err != nil {
		return nil
	}
	if field == "" {
		// skip split chunk logic if not found proper field
		log.Warn("fallback to sequential dump due to no proper field",
			zap.String("database", db), zap.String("table", tbl))
		return d.sequentialDumpTable(conn, meta, taskChan)
	}

	min, max, err := d.selectMinAndMaxIntValue(conn, db, tbl, field)
	if err != nil {
		return err
	}
	log.Debug("get int bounding values",
		zap.String("lower", min.String()),
		zap.String("upper", max.String()))

	count := estimateCount(db, tbl, conn, field, conf)
	log.Info("get estimated rows count",
		zap.String("database", db),
		zap.String("table", tbl),
		zap.Uint64("estimateCount", count))
	if count < conf.Rows {
		// skip chunk logic if estimates are low
		log.Warn("skip concurrent dump due to estimate count < rows",
			zap.Uint64("estimate count", count),
			zap.Uint64("conf.rows", conf.Rows),
			zap.String("database", db),
			zap.String("table", tbl))
		return d.sequentialDumpTable(conn, meta, taskChan)
	}

	// every chunk would have eventual adjustments
	estimatedChunks := count / conf.Rows
	estimatedStep := new(big.Int).Sub(max, min).Uint64()/estimatedChunks + 1
	bigEstimatedStep := new(big.Int).SetUint64(estimatedStep)
	cutoff := new(big.Int).Set(min)
	totalChunks := estimatedChunks
	if estimatedStep == 1 {
		totalChunks = new(big.Int).Sub(max, min).Uint64()
	}

	selectField, selectLen, err := buildSelectField(conn, db, tbl, conf.CompleteInsert)
	if err != nil {
		return err
	}

	orderByClause, err := buildOrderByClause(conf, conn, db, tbl)
	if err != nil {
		return err
	}

	chunkIndex := 0
	nullValueCondition := fmt.Sprintf("`%s` IS NULL OR ", escapeString(field))
	for max.Cmp(cutoff) >= 0 {
		nextCutOff := new(big.Int).Add(cutoff, bigEstimatedStep)
		where := fmt.Sprintf("%s(`%s` >= %d AND `%s` < %d)", nullValueCondition, escapeString(field), cutoff, escapeString(field), nextCutOff)
		query := buildSelectQuery(db, tbl, selectField, buildWhereCondition(conf, where), orderByClause)
		if len(nullValueCondition) > 0 {
			nullValueCondition = ""
		}
		task := NewTaskTableData(meta, newTableData(query, selectLen, false), chunkIndex, int(totalChunks))
		ctxDone := d.sendTaskToChan(task, taskChan)
		if ctxDone {
			break
		}
		cutoff = nextCutOff
		chunkIndex++
	}
	return nil
}

func (d *Dumper) sendTaskToChan(task Task, taskChan chan<- Task) (ctxDone bool) {
	ctx, conf := d.ctx, d.conf
	select {
	case <-ctx.Done():
		return true
	case taskChan <- task:
		log.Debug("send task to writer",
			zap.String("task", task.Brief()))
		taskChannelCapacity.With(conf.Labels).Desc()
		return false
	}
}

func (d *Dumper) selectMinAndMaxIntValue(conn *sql.Conn, db, tbl, field string) (*big.Int, *big.Int, error) {
	ctx, conf, zero := d.ctx, d.conf, &big.Int{}
	query := fmt.Sprintf("SELECT MIN(`%s`),MAX(`%s`) FROM `%s`.`%s`",
		escapeString(field), escapeString(field), escapeString(db), escapeString(tbl))
	if conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, conf.Where)
	}
	log.Debug("split chunks", zap.String("query", query))

	var smin sql.NullString
	var smax sql.NullString
	row := conn.QueryRowContext(ctx, query)
	err := row.Scan(&smin, &smax)
	if err != nil {
		log.Error("split chunks - get max min failed", zap.String("query", query), zap.Error(err))
		return zero, zero, errors.Trace(err)
	}
	if !smax.Valid || !smin.Valid {
		// found no data
		log.Warn("no data to dump", zap.String("database", db), zap.String("table", tbl))
		return zero, zero, nil
	}

	max := new(big.Int)
	min := new(big.Int)
	var ok bool
	if max, ok = max.SetString(smax.String, 10); !ok {
		return zero, zero, errors.Errorf("fail to convert max value %s in query %s", smax.String, query)
	}
	if min, ok = min.SetString(smin.String, 10); !ok {
		return zero, zero, errors.Errorf("fail to convert min value %s in query %s", smin.String, query)
	}
	return min, max, nil
}

func (d *Dumper) concurrentDumpTiDBTables(conn *sql.Conn, meta TableMeta, taskChan chan<- Task) error {
	conf := d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()

	handleColNames, handleVals, err := selectTiDBTableSample(conn, db, tbl)
	if err != nil {
		return err
	}
	if len(handleVals) == 0 {
		return nil
	}
	selectField, selectLen, err := buildSelectField(conn, db, tbl, conf.CompleteInsert)
	if err != nil {
		return err
	}
	where := buildWhereClauses(handleColNames, handleVals)
	orderByClause := buildOrderByClauseString(handleColNames)

	for i, w := range where {
		query := buildSelectQuery(db, tbl, selectField, buildWhereCondition(conf, w), orderByClause)
		task := NewTaskTableData(meta, newTableData(query, selectLen, false), i, len(where))
		ctxDone := d.sendTaskToChan(task, taskChan)
		if ctxDone {
			break
		}
	}
	return nil
}

func selectTiDBTableSample(conn *sql.Conn, dbName, tableName string) (pkFields []string, pkVals []string, err error) {
	pkFields, pkColTypes, err := GetPrimaryKeyAndColumnTypes(conn, dbName, tableName)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	hasImplicitRowID, err := SelectTiDBRowID(conn, dbName, tableName)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if hasImplicitRowID {
		pkFields, pkColTypes = []string{"_tidb_rowid"}, []string{"BIGINT"}
	}
	query := buildTiDBTableSampleQuery(pkFields, dbName, tableName)
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	iter := newRowIter(rows, len(pkFields))
	defer iter.Close()
	rowRec := MakeRowReceiver(pkColTypes)
	buf := new(bytes.Buffer)
	for iter.HasNext() {
		err = iter.Decode(rowRec)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		rowRec.WriteToBuffer(buf, true)
		pkVals = append(pkVals, buf.String())
		buf.Reset()
		iter.Next()
	}
	return pkFields, pkVals, nil
}

func buildTiDBTableSampleQuery(pkFields []string, dbName, tblName string) string {
	template := "SELECT %s FROM `%s`.`%s` TABLESAMPLE REGIONS() ORDER BY %s"
	quotaPk := make([]string, len(pkFields))
	for i, s := range pkFields {
		quotaPk[i] = fmt.Sprintf("`%s`", escapeString(s))
	}
	pks := strings.Join(quotaPk, ",")
	return fmt.Sprintf(template, pks, escapeString(dbName), escapeString(tblName), pks)
}

func prepareTableListToDump(conf *Config, db *sql.Conn) error {
	databases, err := prepareDumpingDatabases(conf, db)
	if err != nil {
		return err
	}

	conf.Tables, err = listAllTables(db, databases)
	if err != nil {
		return err
	}

	if !conf.NoViews {
		views, err := listAllViews(db, databases)
		if err != nil {
			return err
		}
		conf.Tables.Merge(views)
	}

	filterTables(conf)
	return nil
}

func dumpTableMeta(conf *Config, conn *sql.Conn, db string, table *TableInfo) (TableMeta, error) {
	tbl := table.Name
	selectField, _, err := buildSelectField(conn, db, tbl, conf.CompleteInsert)
	if err != nil {
		return nil, err
	}

	var colTypes []*sql.ColumnType
	// If all columns are generated
	if selectField == "" {
		colTypes, err = GetColumnTypes(conn, "*", db, tbl)
	} else {
		colTypes, err = GetColumnTypes(conn, selectField, db, tbl)
	}
	if err != nil {
		return nil, err
	}

	meta := &tableMeta{
		database:      db,
		table:         tbl,
		colTypes:      colTypes,
		selectedField: selectField,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}

	if conf.NoSchemas {
		return meta, nil
	}
	if table.Type == TableTypeView {
		viewName := table.Name
		createTableSQL, createViewSQL, err1 := ShowCreateView(conn, db, viewName)
		if err1 != nil {
			return meta, err1
		}
		meta.showCreateTable = createTableSQL
		meta.showCreateView = createViewSQL
		return meta, nil
	}
	createTableSQL, err := ShowCreateTable(conn, db, tbl)
	if err != nil {
		return nil, err
	}
	meta.showCreateTable = createTableSQL
	return meta, nil
}

func (d *Dumper) dumpSQL(taskChan chan<- Task) {
	conf := d.conf
	meta := &tableMeta{}
	data := newTableData(conf.SQL, 0, true)
	task := NewTaskTableData(meta, data, 0, 1)
	d.sendTaskToChan(task, taskChan)
}

func canRebuildConn(consistency string, trxConsistencyOnly bool) bool {
	switch consistency {
	case consistencyTypeLock, consistencyTypeFlush:
		return !trxConsistencyOnly
	case consistencyTypeSnapshot, consistencyTypeNone:
		return true
	default:
		return false
	}
}

// Close closes a Dumper and stop dumping immediately
func (d *Dumper) Close() error {
	d.cancelCtx()
	return d.dbHandle.Close()
}

func runSteps(d *Dumper, steps ...func(*Dumper) error) error {
	for _, st := range steps {
		err := st(d)
		if err != nil {
			return err
		}
	}
	return nil
}

// createExternalStore is an initialization step of Dumper.
func createExternalStore(d *Dumper) error {
	ctx, conf := d.ctx, d.conf
	b, err := storage.ParseBackend(conf.OutputDirPath, &conf.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	extStore, err := storage.Create(ctx, b, false)
	if err != nil {
		return errors.Trace(err)
	}
	d.extStore = extStore
	return nil
}

// startHTTPService is an initialization step of Dumper.
func startHTTPService(d *Dumper) error {
	conf := d.conf
	if conf.StatusAddr != "" {
		go func() {
			err := startDumplingService(conf.StatusAddr)
			if err != nil {
				log.Warn("meet error when stopping dumpling http service", zap.Error(err))
			}
		}()
	}
	return nil
}

// openSQLDB is an initialization step of Dumper.
func openSQLDB(d *Dumper) error {
	conf := d.conf
	pool, err := sql.Open("mysql", conf.GetDSN(""))
	if err != nil {
		return errors.Trace(err)
	}
	d.dbHandle = pool
	return nil
}

// detectServerInfo is an initialization step of Dumper.
func detectServerInfo(d *Dumper) error {
	db, conf := d.dbHandle, d.conf
	versionStr, err := SelectVersion(db)
	if err != nil {
		conf.ServerInfo = ServerInfoUnknown
		return err
	}
	conf.ServerInfo = ParseServerInfo(versionStr)
	return nil
}

// resolveAutoConsistency is an initialization step of Dumper.
func resolveAutoConsistency(d *Dumper) error {
	conf := d.conf
	if conf.Consistency != "auto" {
		return nil
	}
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		conf.Consistency = "snapshot"
	case ServerTypeMySQL, ServerTypeMariaDB:
		conf.Consistency = "flush"
	default:
		conf.Consistency = "none"
	}
	return nil
}

// tidbSetPDClientForGC is an initialization step of Dumper.
func tidbSetPDClientForGC(d *Dumper) error {
	ctx, si, pool := d.ctx, d.conf.ServerInfo, d.dbHandle
	if si.ServerType != ServerTypeTiDB ||
		si.ServerVersion == nil ||
		si.ServerVersion.Compare(*gcSafePointVersion) < 0 {
		return nil
	}
	pdAddrs, err := GetPdAddrs(pool)
	if err != nil {
		return err
	}
	if len(pdAddrs) > 0 {
		doPdGC, err := checkSameCluster(ctx, pool, pdAddrs)
		if err != nil {
			log.Warn("meet error while check whether fetched pd addr and TiDB belong to one cluster", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
		} else if doPdGC {
			pdClient, err := pd.NewClientWithContext(ctx, pdAddrs, pd.SecurityOption{})
			if err != nil {
				log.Warn("create pd client to control GC failed", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
			}
			d.tidbPDClientForGC = pdClient
		}
	}
	return nil
}

// tidbGetSnapshot is an initialization step of Dumper.
func tidbGetSnapshot(d *Dumper) error {
	conf, doPdGC := d.conf, d.tidbPDClientForGC != nil
	consistency := conf.Consistency
	pool, ctx := d.dbHandle, d.ctx
	if conf.Snapshot == "" && (doPdGC || consistency == "snapshot") {
		conn, err := pool.Conn(ctx)
		if err != nil {
			log.Warn("cannot get snapshot from TiDB", zap.Error(err))
			return nil
		}
		snapshot, err := getSnapshot(conn)
		_ = conn.Close()
		if err != nil {
			log.Warn("cannot get snapshot from TiDB", zap.Error(err))
			return nil
		}
		conf.Snapshot = snapshot
		return nil
	}
	return nil
}

// tidbStartGCSavepointUpdateService is an initialization step of Dumper.
func tidbStartGCSavepointUpdateService(d *Dumper) error {
	ctx, pool, conf := d.ctx, d.dbHandle, d.conf
	snapshot, si := conf.Snapshot, conf.ServerInfo
	if d.tidbPDClientForGC != nil {
		snapshotTS, err := parseSnapshotToTSO(pool, snapshot)
		if err != nil {
			return err
		}
		go updateServiceSafePoint(ctx, d.tidbPDClientForGC, defaultDumpGCSafePointTTL, snapshotTS)
	} else if si.ServerType == ServerTypeTiDB {
		log.Warn("If the amount of data to dump is large, criteria: (data more than 60GB or dumped time more than 10 minutes)\n" +
			"you'd better adjust the tikv_gc_life_time to avoid export failure due to TiDB GC during the dump process.\n" +
			"Before dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '720h' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n" +
			"After dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '10m' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n")
	}
	return nil
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

// setSessionParam is an initialization step of Dumper.
func setSessionParam(d *Dumper) error {
	conf, pool := d.conf, d.dbHandle
	si := conf.ServerInfo
	consistency, snapshot := conf.Consistency, conf.Snapshot
	sessionParam := conf.SessionParams
	if si.ServerType == ServerTypeTiDB && conf.TiDBMemQuotaQuery != UnspecifiedSize {
		sessionParam[TiDBMemQuotaQueryName] = conf.TiDBMemQuotaQuery
	}
	if snapshot != "" {
		if si.ServerType != ServerTypeTiDB {
			return errors.New("snapshot consistency is not supported for this server")
		}
		if consistency == consistencyTypeSnapshot {
			hasTiKV, err := CheckTiDBWithTiKV(pool)
			if err != nil {
				return err
			}
			if hasTiKV {
				sessionParam["tidb_snapshot"] = snapshot
			}
		}
	}
	var err error
	if d.dbHandle, err = resetDBWithSessionParams(pool, conf.GetDSN(""), conf.SessionParams); err != nil {
		return errors.Trace(err)
	}
	return nil
}
