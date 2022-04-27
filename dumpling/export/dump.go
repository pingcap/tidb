// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	// import mysql driver
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pclog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/dumpling/cli"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

var openDBFunc = sql.Open

var emptyHandleValsErr = errors.New("empty handleVals for TiDB table")

// Dumper is the dump progress structure
type Dumper struct {
	tctx      *tcontext.Context
	conf      *Config
	cancelCtx context.CancelFunc

	extStore storage.ExternalStorage
	dbHandle *sql.DB

	tidbPDClientForGC             pd.Client
	selectTiDBTableRegionFunc     func(tctx *tcontext.Context, conn *BaseConn, meta TableMeta) (pkFields []string, pkVals [][]string, err error)
	totalTables                   int64
	charsetAndDefaultCollationMap map[string]string
}

// NewDumper returns a new Dumper
func NewDumper(ctx context.Context, conf *Config) (*Dumper, error) {
	failpoint.Inject("setExtStorage", func(val failpoint.Value) {
		path := val.(string)
		b, err := storage.ParseBackend(path, nil)
		if err != nil {
			panic(err)
		}
		s, err := storage.New(context.Background(), b, &storage.ExternalStorageOptions{})
		if err != nil {
			panic(err)
		}
		conf.ExtStorage = s
	})

	tctx, cancelFn := tcontext.Background().WithContext(ctx).WithCancel()
	d := &Dumper{
		tctx:                      tctx,
		conf:                      conf,
		cancelCtx:                 cancelFn,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	err := adjustConfig(conf,
		registerTLSConfig,
		validateSpecifiedSQL,
		adjustFileFormat)
	if err != nil {
		return nil, err
	}
	err = runSteps(d,
		initLogger,
		createExternalStore,
		startHTTPService,
		openSQLDB,
		detectServerInfo,
		resolveAutoConsistency,

		validateResolveAutoConsistency,
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
	tctx, conf, pool := d.tctx, d.conf, d.dbHandle
	tctx.L().Info("begin to run Dump", zap.Stringer("conf", conf))
	m := newGlobalMetadata(tctx, d.extStore, conf.Snapshot)
	repeatableRead := needRepeatableRead(conf.ServerInfo.ServerType, conf.Consistency)
	defer func() {
		if dumpErr == nil {
			_ = m.writeGlobalMetaData()
		}
	}()

	// for consistency lock, we should get table list at first to generate the lock tables SQL
	if conf.Consistency == consistencyTypeLock {
		conn, err = createConnWithConsistency(tctx, pool, repeatableRead)
		if err != nil {
			return errors.Trace(err)
		}
		if err = prepareTableListToDump(tctx, conf, conn); err != nil {
			conn.Close()
			return err
		}
		conn.Close()
	}

	conCtrl, err = NewConsistencyController(tctx, conf, pool)
	if err != nil {
		return err
	}
	if err = conCtrl.Setup(tctx); err != nil {
		return errors.Trace(err)
	}
	// To avoid lock is not released
	defer func() {
		err = conCtrl.TearDown(tctx)
		if err != nil {
			tctx.L().Warn("fail to tear down consistency controller", zap.Error(err))
		}
	}()

	metaConn, err := createConnWithConsistency(tctx, pool, repeatableRead)
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
		tctx.L().Info("get global metadata failed", log.ShortError(err))
	}

	if d.conf.CollationCompatible == StrictCollationCompatible {
		//init charset and default collation map
		d.charsetAndDefaultCollationMap, err = GetCharsetAndDefaultCollation(tctx.Context, metaConn)
		if err != nil {
			return err
		}
	}

	// for other consistencies, we should get table list after consistency is set up and GlobalMetaData is cached
	if conf.Consistency != consistencyTypeLock {
		if err = prepareTableListToDump(tctx, conf, metaConn); err != nil {
			return err
		}
	}
	if err = d.renewSelectTableRegionFuncForLowerTiDB(tctx); err != nil {
		tctx.L().Info("cannot update select table region info for TiDB", log.ShortError(err))
	}

	atomic.StoreInt64(&d.totalTables, int64(calculateTableCount(conf.Tables)))

	rebuildConn := func(conn *sql.Conn, updateMeta bool) (*sql.Conn, error) {
		// make sure that the lock connection is still alive
		err1 := conCtrl.PingContext(tctx)
		if err1 != nil {
			return conn, errors.Trace(err1)
		}
		// give up the last broken connection
		conn.Close()
		newConn, err1 := createConnWithConsistency(tctx, pool, repeatableRead)
		if err1 != nil {
			return conn, errors.Trace(err1)
		}
		conn = newConn
		// renew the master status after connection. dm can't close safe-mode until dm reaches current pos
		if updateMeta && conf.PosAfterConnect {
			err1 = m.recordGlobalMetaData(conn, conf.ServerInfo.ServerType, true)
			if err1 != nil {
				return conn, errors.Trace(err1)
			}
		}
		return conn, nil
	}

	taskChan := make(chan Task, defaultDumpThreads)
	AddGauge(taskChannelCapacity, conf.Labels, defaultDumpThreads)
	wg, writingCtx := errgroup.WithContext(tctx)
	writerCtx := tctx.WithContext(writingCtx)
	writers, tearDownWriters, err := d.startWriters(writerCtx, wg, taskChan, rebuildConn)
	if err != nil {
		return err
	}
	defer tearDownWriters()

	if conf.TransactionalConsistency {
		if conf.Consistency == consistencyTypeFlush || conf.Consistency == consistencyTypeLock {
			tctx.L().Info("All the dumping transactions have started. Start to unlock tables")
		}
		if err = conCtrl.TearDown(tctx); err != nil {
			return errors.Trace(err)
		}
	}
	// Inject consistency failpoint test after we release the table lock
	failpoint.Inject("ConsistencyCheck", nil)

	if conf.PosAfterConnect {
		// record again, to provide a location to exit safe mode for DM
		err = m.recordGlobalMetaData(metaConn, conf.ServerInfo.ServerType, true)
		if err != nil {
			tctx.L().Info("get global metadata (after connection pool established) failed", log.ShortError(err))
		}
	}

	summary.SetLogCollector(summary.NewLogCollector(tctx.L().Info))
	summary.SetUnit(summary.BackupUnit)
	defer summary.Summary(summary.BackupUnit)

	logProgressCtx, logProgressCancel := tctx.WithCancel()
	go d.runLogProgress(logProgressCtx)
	defer logProgressCancel()

	tableDataStartTime := time.Now()

	failpoint.Inject("PrintTiDBMemQuotaQuery", func(_ failpoint.Value) {
		row := d.dbHandle.QueryRowContext(tctx, "select @@tidb_mem_quota_query;")
		var s string
		err = row.Scan(&s)
		if err != nil {
			fmt.Println(errors.Trace(err))
		} else {
			fmt.Printf("tidb_mem_quota_query == %s\n", s)
		}
	})
	baseConn := newBaseConn(metaConn, canRebuildConn(conf.Consistency, conf.TransactionalConsistency), rebuildConn)

	if conf.SQL == "" {
		if err = d.dumpDatabases(writerCtx, baseConn, taskChan); err != nil && !errors.ErrorEqual(err, context.Canceled) {
			return err
		}
	} else {
		d.dumpSQL(writerCtx, baseConn, taskChan)
	}
	close(taskChan)
	_ = baseConn.DBConn.Close()
	if err := wg.Wait(); err != nil {
		summary.CollectFailureUnit("dump table data", err)
		return errors.Trace(err)
	}
	summary.CollectSuccessUnit("dump cost", countTotalTask(writers), time.Since(tableDataStartTime))

	summary.SetSuccessStatus(true)
	m.recordFinishTime(time.Now())
	return nil
}

func (d *Dumper) startWriters(tctx *tcontext.Context, wg *errgroup.Group, taskChan <-chan Task,
	rebuildConnFn func(*sql.Conn, bool) (*sql.Conn, error)) ([]*Writer, func(), error) {
	conf, pool := d.conf, d.dbHandle
	writers := make([]*Writer, conf.Threads)
	for i := 0; i < conf.Threads; i++ {
		conn, err := createConnWithConsistency(tctx, pool, needRepeatableRead(conf.ServerInfo.ServerType, conf.Consistency))
		if err != nil {
			return nil, func() {}, err
		}
		writer := NewWriter(tctx, int64(i), conf, conn, d.extStore)
		writer.rebuildConnFn = rebuildConnFn
		writer.setFinishTableCallBack(func(task Task) {
			if _, ok := task.(*TaskTableData); ok {
				IncCounter(finishedTablesCounter, conf.Labels)
				// FIXME: actually finishing the last chunk doesn't means this table is 'finished'.
				//  We can call this table is 'finished' if all its chunks are finished.
				//  Comment this log now to avoid ambiguity.
				// tctx.L().Debug("finished dumping table data",
				//	zap.String("database", td.Meta.DatabaseName()),
				//	zap.String("table", td.Meta.TableName()))
			}
		})
		writer.setFinishTaskCallBack(func(task Task) {
			IncGauge(taskChannelCapacity, conf.Labels)
			if td, ok := task.(*TaskTableData); ok {
				tctx.L().Debug("finish dumping table data task",
					zap.String("database", td.Meta.DatabaseName()),
					zap.String("table", td.Meta.TableName()),
					zap.Int("chunkIdx", td.ChunkIndex))
			}
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

func (d *Dumper) dumpDatabases(tctx *tcontext.Context, metaConn *BaseConn, taskChan chan<- Task) error {
	conf := d.conf
	allTables := conf.Tables

	// policy should be created before database
	// placement policy in other server type can be different, so we only handle the tidb server
	if conf.ServerInfo.ServerType == version.ServerTypeTiDB {
		policyNames, err := ListAllPlacementPolicyNames(tctx, metaConn)
		if err != nil {
			errCause := errors.Cause(err)
			if mysqlErr, ok := errCause.(*mysql.MySQLError); ok && mysqlErr.Number == ErrNoSuchTable {
				// some old tidb version and other server type doesn't support placement rules, we can skip it.
				tctx.L().Debug("cannot dump placement policy, maybe the server doesn't support it", log.ShortError(err))
			} else {
				tctx.L().Warn("fail to dump placement policy: ", log.ShortError(err))
			}
		}
		for _, policy := range policyNames {
			createPolicySQL, err := ShowCreatePlacementPolicy(tctx, metaConn, policy)
			if err != nil {
				return errors.Trace(err)
			}
			wrappedCreatePolicySQL := fmt.Sprintf("/*T![placement] %s */", createPolicySQL)
			task := NewTaskPolicyMeta(policy, wrappedCreatePolicySQL)
			ctxDone := d.sendTaskToChan(tctx, task, taskChan)
			if ctxDone {
				return tctx.Err()
			}
		}
	}

	parser1 := parser.New()
	for dbName, tables := range allTables {
		if !conf.NoSchemas {
			createDatabaseSQL, err := ShowCreateDatabase(tctx, metaConn, dbName)
			if err != nil {
				return errors.Trace(err)
			}

			// adjust db collation
			createDatabaseSQL, err = adjustDatabaseCollation(tctx, d.conf.CollationCompatible, parser1, createDatabaseSQL, d.charsetAndDefaultCollationMap)
			if err != nil {
				return errors.Trace(err)
			}

			task := NewTaskDatabaseMeta(dbName, createDatabaseSQL)
			ctxDone := d.sendTaskToChan(tctx, task, taskChan)
			if ctxDone {
				return tctx.Err()
			}
		}

		for _, table := range tables {
			tctx.L().Debug("start dumping table...", zap.String("database", dbName),
				zap.String("table", table.Name))
			meta, err := dumpTableMeta(tctx, conf, metaConn, dbName, table)
			if err != nil {
				return errors.Trace(err)
			}

			if !conf.NoSchemas {
				switch table.Type {
				case TableTypeView:
					task := NewTaskViewMeta(dbName, table.Name, meta.ShowCreateTable(), meta.ShowCreateView())
					ctxDone := d.sendTaskToChan(tctx, task, taskChan)
					if ctxDone {
						return tctx.Err()
					}
				case TableTypeSequence:
					task := NewTaskSequenceMeta(dbName, table.Name, meta.ShowCreateTable())
					ctxDone := d.sendTaskToChan(tctx, task, taskChan)
					if ctxDone {
						return tctx.Err()
					}
				default:
					// adjust table collation
					newCreateSQL, err := adjustTableCollation(tctx, d.conf.CollationCompatible, parser1, meta.ShowCreateTable(), d.charsetAndDefaultCollationMap)
					if err != nil {
						return errors.Trace(err)
					}
					meta.(*tableMeta).showCreateTable = newCreateSQL

					task := NewTaskTableMeta(dbName, table.Name, meta.ShowCreateTable())
					ctxDone := d.sendTaskToChan(tctx, task, taskChan)
					if ctxDone {
						return tctx.Err()
					}
				}
			}
			if table.Type == TableTypeBase {
				err = d.dumpTableData(tctx, metaConn, meta, taskChan)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	return nil
}

// adjustDatabaseCollation adjusts db collation and return new create sql and collation
func adjustDatabaseCollation(tctx *tcontext.Context, collationCompatible string, parser *parser.Parser, originSQL string, charsetAndDefaultCollationMap map[string]string) (string, error) {
	if collationCompatible != StrictCollationCompatible {
		return originSQL, nil
	}
	stmt, err := parser.ParseOneStmt(originSQL, "", "")
	if err != nil {
		tctx.L().Warn("parse create database error, maybe tidb parser doesn't support it", zap.String("originSQL", originSQL), log.ShortError(err))
		return originSQL, nil
	}
	createStmt, ok := stmt.(*ast.CreateDatabaseStmt)
	if !ok {
		return originSQL, nil
	}
	var charset string
	for _, createOption := range createStmt.Options {
		// already have 'Collation'
		if createOption.Tp == ast.DatabaseOptionCollate {
			return originSQL, nil
		}
		if createOption.Tp == ast.DatabaseOptionCharset {
			charset = createOption.Value
		}
	}
	// get db collation
	collation, ok := charsetAndDefaultCollationMap[strings.ToLower(charset)]
	if !ok {
		tctx.L().Warn("not found database charset default collation.", zap.String("originSQL", originSQL), zap.String("charset", strings.ToLower(charset)))
		return originSQL, nil
	}
	// add collation
	createStmt.Options = append(createStmt.Options, &ast.DatabaseOption{Tp: ast.DatabaseOptionCollate, Value: collation})
	// rewrite sql
	var b []byte
	bf := bytes.NewBuffer(b)
	err = createStmt.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment,
		In:    bf,
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	return bf.String(), nil
}

// adjustTableCollation adjusts table collation
func adjustTableCollation(tctx *tcontext.Context, collationCompatible string, parser *parser.Parser, originSQL string, charsetAndDefaultCollationMap map[string]string) (string, error) {
	if collationCompatible != StrictCollationCompatible {
		return originSQL, nil
	}
	stmt, err := parser.ParseOneStmt(originSQL, "", "")
	if err != nil {
		tctx.L().Warn("parse create table error, maybe tidb parser doesn't support it", zap.String("originSQL", originSQL), log.ShortError(err))
		return originSQL, nil
	}
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return originSQL, nil
	}
	var charset string
	var collation string
	for _, createOption := range createStmt.Options {
		// already have 'Collation'
		if createOption.Tp == ast.TableOptionCollate {
			collation = createOption.StrValue
			break
		}
		if createOption.Tp == ast.TableOptionCharset {
			charset = createOption.StrValue
		}
	}

	if collation == "" && charset != "" {
		// get db collation
		collation, ok := charsetAndDefaultCollationMap[strings.ToLower(charset)]
		if !ok {
			tctx.L().Warn("not found table charset default collation.", zap.String("originSQL", originSQL), zap.String("charset", strings.ToLower(charset)))
			return originSQL, nil
		}

		// add collation
		createStmt.Options = append(createStmt.Options, &ast.TableOption{Tp: ast.TableOptionCollate, StrValue: collation})
	}

	// adjust columns collation
	adjustColumnsCollation(tctx, createStmt, charsetAndDefaultCollationMap)

	// rewrite sql
	var b []byte
	bf := bytes.NewBuffer(b)
	err = createStmt.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment,
		In:    bf,
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	return bf.String(), nil
}

// adjustColumnsCollation adds column's collation.
func adjustColumnsCollation(tctx *tcontext.Context, createStmt *ast.CreateTableStmt, charsetAndDefaultCollationMap map[string]string) {
	for _, col := range createStmt.Cols {
		for _, options := range col.Options {
			// already have 'Collation'
			if options.Tp == ast.ColumnOptionCollate {
				continue
			}
		}
		fieldType := col.Tp
		if fieldType.GetCollate() != "" {
			continue
		}
		if fieldType.GetCharset() != "" {
			// just have charset
			collation, ok := charsetAndDefaultCollationMap[strings.ToLower(fieldType.GetCharset())]
			if !ok {
				tctx.L().Warn("not found charset default collation for column.", zap.String("table", createStmt.Table.Name.String()), zap.String("column", col.Name.String()), zap.String("charset", strings.ToLower(fieldType.GetCharset())))
				continue
			}
			fieldType.SetCollate(collation)
		}
	}
}

func (d *Dumper) dumpTableData(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task) error {
	conf := d.conf
	if conf.NoData {
		return nil
	}

	// Update total rows
	fieldName, _ := pickupPossibleField(tctx, meta, conn)
	c := estimateCount(tctx, meta.DatabaseName(), meta.TableName(), conn, fieldName, conf)
	AddCounter(estimateTotalRowsCounter, conf.Labels, float64(c))

	if conf.Rows == UnspecifiedSize {
		return d.sequentialDumpTable(tctx, conn, meta, taskChan)
	}
	return d.concurrentDumpTable(tctx, conn, meta, taskChan)
}

func (d *Dumper) buildConcatTask(tctx *tcontext.Context, conn *BaseConn, meta TableMeta) (*TaskTableData, error) {
	tableChan := make(chan Task, 128)
	errCh := make(chan error, 1)
	go func() {
		// adjust rows to suitable rows for this table
		d.conf.Rows = GetSuitableRows(meta.AvgRowLength())
		err := d.concurrentDumpTable(tctx, conn, meta, tableChan)
		d.conf.Rows = UnspecifiedSize
		if err != nil {
			errCh <- err
		} else {
			close(errCh)
		}
	}()
	tableDataArr := make([]*tableData, 0)
	handleSubTask := func(task Task) {
		tableTask, ok := task.(*TaskTableData)
		if !ok {
			tctx.L().Warn("unexpected task when splitting table chunks", zap.String("task", tableTask.Brief()))
			return
		}
		tableDataInst, ok := tableTask.Data.(*tableData)
		if !ok {
			tctx.L().Warn("unexpected task.Data when splitting table chunks", zap.String("task", tableTask.Brief()))
			return
		}
		tableDataArr = append(tableDataArr, tableDataInst)
	}
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				// make sure all the subtasks in tableChan are handled
				for len(tableChan) > 0 {
					task := <-tableChan
					handleSubTask(task)
				}
				if len(tableDataArr) <= 1 {
					return nil, nil
				}
				queries := make([]string, 0, len(tableDataArr))
				colLen := tableDataArr[0].colLen
				for _, tableDataInst := range tableDataArr {
					queries = append(queries, tableDataInst.query)
					if colLen != tableDataInst.colLen {
						tctx.L().Warn("colLen varies for same table",
							zap.Int("oldColLen", colLen),
							zap.String("oldQuery", queries[0]),
							zap.Int("newColLen", tableDataInst.colLen),
							zap.String("newQuery", tableDataInst.query))
						return nil, nil
					}
				}
				return NewTaskTableData(meta, newMultiQueriesChunk(queries, colLen), 0, 1), nil
			}
			return nil, err
		case task := <-tableChan:
			handleSubTask(task)
		}
	}
}

func (d *Dumper) dumpWholeTableDirectly(tctx *tcontext.Context, meta TableMeta, taskChan chan<- Task, partition, orderByClause string, currentChunk, totalChunks int) error {
	conf := d.conf
	tableIR := SelectAllFromTable(conf, meta, partition, orderByClause)
	task := NewTaskTableData(meta, tableIR, currentChunk, totalChunks)
	ctxDone := d.sendTaskToChan(tctx, task, taskChan)
	if ctxDone {
		return tctx.Err()
	}
	return nil
}

func (d *Dumper) sequentialDumpTable(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task) error {
	conf := d.conf
	if conf.ServerInfo.ServerType == version.ServerTypeTiDB {
		task, err := d.buildConcatTask(tctx, conn, meta)
		if err != nil {
			return errors.Trace(err)
		}
		if task != nil {
			ctxDone := d.sendTaskToChan(tctx, task, taskChan)
			if ctxDone {
				return tctx.Err()
			}
			return nil
		}
		tctx.L().Info("didn't build tidb concat sqls, will select all from table now",
			zap.String("database", meta.DatabaseName()),
			zap.String("table", meta.TableName()))
	}
	orderByClause, err := buildOrderByClause(tctx, conf, conn, meta.DatabaseName(), meta.TableName(), meta.HasImplicitRowID())
	if err != nil {
		return err
	}
	return d.dumpWholeTableDirectly(tctx, meta, taskChan, "", orderByClause, 0, 1)
}

// concurrentDumpTable tries to split table into several chunks to dump
func (d *Dumper) concurrentDumpTable(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task) error {
	conf := d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()
	if conf.ServerInfo.ServerType == version.ServerTypeTiDB &&
		conf.ServerInfo.ServerVersion != nil &&
		(conf.ServerInfo.ServerVersion.Compare(*tableSampleVersion) >= 0 ||
			(conf.ServerInfo.HasTiKV && conf.ServerInfo.ServerVersion.Compare(*decodeRegionVersion) >= 0)) {
		err := d.concurrentDumpTiDBTables(tctx, conn, meta, taskChan)
		// don't retry on context error and successful tasks
		if err2 := errors.Cause(err); err2 == nil || err2 == context.DeadlineExceeded || err2 == context.Canceled {
			return err
		} else if err2 != emptyHandleValsErr {
			tctx.L().Info("fallback to concurrent dump tables using rows due to some problem. This won't influence the whole dump process",
				zap.String("database", db), zap.String("table", tbl), log.ShortError(err))
		}
	}

	orderByClause, err := buildOrderByClause(tctx, conf, conn, db, tbl, meta.HasImplicitRowID())
	if err != nil {
		return err
	}

	field, err := pickupPossibleField(tctx, meta, conn)
	if err != nil || field == "" {
		// skip split chunk logic if not found proper field
		tctx.L().Info("fallback to sequential dump due to no proper field. This won't influence the whole dump process",
			zap.String("database", db), zap.String("table", tbl), log.ShortError(err))
		return d.dumpWholeTableDirectly(tctx, meta, taskChan, "", orderByClause, 0, 1)
	}

	count := estimateCount(d.tctx, db, tbl, conn, field, conf)
	tctx.L().Info("get estimated rows count",
		zap.String("database", db),
		zap.String("table", tbl),
		zap.Uint64("estimateCount", count))
	if count < conf.Rows {
		// skip chunk logic if estimates are low
		tctx.L().Info("fallback to sequential dump due to estimate count < rows. This won't influence the whole dump process",
			zap.Uint64("estimate count", count),
			zap.Uint64("conf.rows", conf.Rows),
			zap.String("database", db),
			zap.String("table", tbl))
		return d.dumpWholeTableDirectly(tctx, meta, taskChan, "", orderByClause, 0, 1)
	}

	min, max, err := d.selectMinAndMaxIntValue(tctx, conn, db, tbl, field)
	if err != nil {
		tctx.L().Info("fallback to sequential dump due to cannot get bounding values. This won't influence the whole dump process",
			log.ShortError(err))
		return d.dumpWholeTableDirectly(tctx, meta, taskChan, "", orderByClause, 0, 1)
	}
	tctx.L().Debug("get int bounding values",
		zap.String("lower", min.String()),
		zap.String("upper", max.String()))

	// every chunk would have eventual adjustments
	estimatedChunks := count / conf.Rows
	estimatedStep := new(big.Int).Sub(max, min).Uint64()/estimatedChunks + 1
	bigEstimatedStep := new(big.Int).SetUint64(estimatedStep)
	cutoff := new(big.Int).Set(min)
	totalChunks := estimatedChunks
	if estimatedStep == 1 {
		totalChunks = new(big.Int).Sub(max, min).Uint64() + 1
	}

	selectField, selectLen := meta.SelectedField(), meta.SelectedLen()

	chunkIndex := 0
	nullValueCondition := ""
	if conf.Where == "" {
		nullValueCondition = fmt.Sprintf("`%s` IS NULL OR ", escapeString(field))
	}
	for max.Cmp(cutoff) >= 0 {
		nextCutOff := new(big.Int).Add(cutoff, bigEstimatedStep)
		where := fmt.Sprintf("%s(`%s` >= %d AND `%s` < %d)", nullValueCondition, escapeString(field), cutoff, escapeString(field), nextCutOff)
		query := buildSelectQuery(db, tbl, selectField, "", buildWhereCondition(conf, where), orderByClause)
		if len(nullValueCondition) > 0 {
			nullValueCondition = ""
		}
		task := NewTaskTableData(meta, newTableData(query, selectLen, false), chunkIndex, int(totalChunks))
		ctxDone := d.sendTaskToChan(tctx, task, taskChan)
		if ctxDone {
			return tctx.Err()
		}
		cutoff = nextCutOff
		chunkIndex++
	}
	return nil
}

func (d *Dumper) sendTaskToChan(tctx *tcontext.Context, task Task, taskChan chan<- Task) (ctxDone bool) {
	conf := d.conf
	select {
	case <-tctx.Done():
		return true
	case taskChan <- task:
		tctx.L().Debug("send task to writer",
			zap.String("task", task.Brief()))
		DecGauge(taskChannelCapacity, conf.Labels)
		return false
	}
}

func (d *Dumper) selectMinAndMaxIntValue(tctx *tcontext.Context, conn *BaseConn, db, tbl, field string) (*big.Int, *big.Int, error) {
	conf, zero := d.conf, &big.Int{}
	query := fmt.Sprintf("SELECT MIN(`%s`),MAX(`%s`) FROM `%s`.`%s`",
		escapeString(field), escapeString(field), escapeString(db), escapeString(tbl))
	if conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, conf.Where)
	}
	tctx.L().Debug("split chunks", zap.String("query", query))

	var smin sql.NullString
	var smax sql.NullString
	err := conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		err := rows.Scan(&smin, &smax)
		rows.Close()
		return err
	}, func() {}, query)
	if err != nil {
		return zero, zero, errors.Annotatef(err, "can't get min/max values to split chunks, query: %s", query)
	}
	if !smax.Valid || !smin.Valid {
		// found no data
		return zero, zero, errors.Errorf("no invalid min/max value found in query %s", query)
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

func (d *Dumper) concurrentDumpTiDBTables(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task) error {
	db, tbl := meta.DatabaseName(), meta.TableName()

	var (
		handleColNames []string
		handleVals     [][]string
		err            error
	)
	// for TiDB v5.0+, we can use table sample directly
	if d.conf.ServerInfo.ServerVersion.Compare(*tableSampleVersion) >= 0 {
		tctx.L().Debug("dumping TiDB tables with TABLESAMPLE",
			zap.String("database", db), zap.String("table", tbl))
		handleColNames, handleVals, err = selectTiDBTableSample(tctx, conn, meta)
	} else {
		// for TiDB v3.0+, we can use table region decode in TiDB directly
		tctx.L().Debug("dumping TiDB tables with TABLE REGIONS",
			zap.String("database", db), zap.String("table", tbl))
		var partitions []string
		if d.conf.ServerInfo.ServerVersion.Compare(*gcSafePointVersion) >= 0 {
			partitions, err = GetPartitionNames(tctx, conn, db, tbl)
		}
		if err == nil {
			if len(partitions) == 0 {
				handleColNames, handleVals, err = d.selectTiDBTableRegionFunc(tctx, conn, meta)
			} else {
				return d.concurrentDumpTiDBPartitionTables(tctx, conn, meta, taskChan, partitions)
			}
		}
	}
	if err != nil {
		return err
	}
	return d.sendConcurrentDumpTiDBTasks(tctx, meta, taskChan, handleColNames, handleVals, "", 0, len(handleVals)+1)
}

func (d *Dumper) concurrentDumpTiDBPartitionTables(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task, partitions []string) error {
	db, tbl := meta.DatabaseName(), meta.TableName()
	tctx.L().Debug("dumping TiDB tables with TABLE REGIONS for partition table",
		zap.String("database", db), zap.String("table", tbl), zap.Strings("partitions", partitions))

	startChunkIdx := 0
	totalChunk := 0
	cachedHandleVals := make([][][]string, len(partitions))

	handleColNames, _, err := selectTiDBRowKeyFields(tctx, conn, meta, checkTiDBTableRegionPkFields)
	if err != nil {
		return err
	}
	// cache handleVals here to calculate the total chunks
	for i, partition := range partitions {
		handleVals, err := selectTiDBPartitionRegion(tctx, conn, db, tbl, partition)
		if err != nil {
			return err
		}
		totalChunk += len(handleVals) + 1
		cachedHandleVals[i] = handleVals
	}
	for i, partition := range partitions {
		err := d.sendConcurrentDumpTiDBTasks(tctx, meta, taskChan, handleColNames, cachedHandleVals[i], partition, startChunkIdx, totalChunk)
		if err != nil {
			return err
		}
		startChunkIdx += len(cachedHandleVals[i]) + 1
	}
	return nil
}

func (d *Dumper) sendConcurrentDumpTiDBTasks(tctx *tcontext.Context,
	meta TableMeta, taskChan chan<- Task,
	handleColNames []string, handleVals [][]string, partition string, startChunkIdx, totalChunk int) error {
	db, tbl := meta.DatabaseName(), meta.TableName()
	if len(handleVals) == 0 {
		if partition == "" {
			// return error to make outside function try using rows method to dump data
			return errors.Annotatef(emptyHandleValsErr, "table: `%s`.`%s`", escapeString(db), escapeString(tbl))
		}
		return d.dumpWholeTableDirectly(tctx, meta, taskChan, partition, buildOrderByClauseString(handleColNames), startChunkIdx, totalChunk)
	}
	conf := d.conf
	selectField, selectLen := meta.SelectedField(), meta.SelectedLen()
	where := buildWhereClauses(handleColNames, handleVals)
	orderByClause := buildOrderByClauseString(handleColNames)

	for i, w := range where {
		query := buildSelectQuery(db, tbl, selectField, partition, buildWhereCondition(conf, w), orderByClause)
		task := NewTaskTableData(meta, newTableData(query, selectLen, false), i+startChunkIdx, totalChunk)
		ctxDone := d.sendTaskToChan(tctx, task, taskChan)
		if ctxDone {
			return tctx.Err()
		}
	}
	return nil
}

// L returns real logger
func (d *Dumper) L() log.Logger {
	return d.tctx.L()
}

func selectTiDBTableSample(tctx *tcontext.Context, conn *BaseConn, meta TableMeta) (pkFields []string, pkVals [][]string, err error) {
	pkFields, pkColTypes, err := selectTiDBRowKeyFields(tctx, conn, meta, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	query := buildTiDBTableSampleQuery(pkFields, meta.DatabaseName(), meta.TableName())
	pkValNum := len(pkFields)
	var iter SQLRowIter
	rowRec := MakeRowReceiver(pkColTypes)
	buf := new(bytes.Buffer)

	err = conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		if iter == nil {
			iter = &rowIter{
				rows: rows,
				args: make([]interface{}, pkValNum),
			}
		}
		err = iter.Decode(rowRec)
		if err != nil {
			return errors.Trace(err)
		}
		pkValRow := make([]string, 0, pkValNum)
		for _, rec := range rowRec.receivers {
			rec.WriteToBuffer(buf, true)
			pkValRow = append(pkValRow, buf.String())
			buf.Reset()
		}
		pkVals = append(pkVals, pkValRow)
		return nil
	}, func() {
		if iter != nil {
			iter.Close()
			iter = nil
		}
		rowRec = MakeRowReceiver(pkColTypes)
		pkVals = pkVals[:0]
		buf.Reset()
	}, query)
	if err == nil && iter != nil && iter.Error() != nil {
		err = iter.Error()
	}

	return pkFields, pkVals, err
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

func selectTiDBRowKeyFields(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, checkPkFields func([]string, []string) error) (pkFields, pkColTypes []string, err error) {
	if meta.HasImplicitRowID() {
		pkFields, pkColTypes = []string{"_tidb_rowid"}, []string{"BIGINT"}
	} else {
		pkFields, pkColTypes, err = GetPrimaryKeyAndColumnTypes(tctx, conn, meta)
		if err == nil {
			if checkPkFields != nil {
				err = checkPkFields(pkFields, pkColTypes)
			}
		}
	}
	return
}

func checkTiDBTableRegionPkFields(pkFields, pkColTypes []string) (err error) {
	if len(pkFields) != 1 || len(pkColTypes) != 1 {
		err = errors.Errorf("unsupported primary key for selectTableRegion. pkFields: [%s], pkColTypes: [%s]", strings.Join(pkFields, ", "), strings.Join(pkColTypes, ", "))
		return
	}
	if _, ok := dataTypeInt[pkColTypes[0]]; !ok {
		err = errors.Errorf("unsupported primary key type for selectTableRegion. pkFields: [%s], pkColTypes: [%s]", strings.Join(pkFields, ", "), strings.Join(pkColTypes, ", "))
	}
	return
}

func selectTiDBTableRegion(tctx *tcontext.Context, conn *BaseConn, meta TableMeta) (pkFields []string, pkVals [][]string, err error) {
	pkFields, _, err = selectTiDBRowKeyFields(tctx, conn, meta, checkTiDBTableRegionPkFields)
	if err != nil {
		return
	}

	var (
		startKey, decodedKey sql.NullString
		rowID                = -1
	)
	const (
		tableRegionSQL = "SELECT START_KEY,tidb_decode_key(START_KEY) from INFORMATION_SCHEMA.TIKV_REGION_STATUS s WHERE s.DB_NAME = ? AND s.TABLE_NAME = ? AND IS_INDEX = 0 ORDER BY START_KEY;"
		tidbRowID      = "_tidb_rowid="
	)
	dbName, tableName := meta.DatabaseName(), meta.TableName()
	logger := tctx.L().With(zap.String("database", dbName), zap.String("table", tableName))
	err = conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		rowID++
		err = rows.Scan(&startKey, &decodedKey)
		if err != nil {
			return errors.Trace(err)
		}
		// first region's start key has no use. It may come from another table or might be invalid
		if rowID == 0 {
			return nil
		}
		if !startKey.Valid {
			logger.Debug("meet invalid start key", zap.Int("rowID", rowID))
			return nil
		}
		if !decodedKey.Valid {
			logger.Debug("meet invalid decoded start key", zap.Int("rowID", rowID), zap.String("startKey", startKey.String))
			return nil
		}
		pkVal, err2 := extractTiDBRowIDFromDecodedKey(tidbRowID, decodedKey.String)
		if err2 != nil {
			logger.Debug("cannot extract pkVal from decoded start key",
				zap.Int("rowID", rowID), zap.String("startKey", startKey.String), zap.String("decodedKey", decodedKey.String), log.ShortError(err2))
		} else {
			pkVals = append(pkVals, []string{pkVal})
		}
		return nil
	}, func() {
		pkFields = pkFields[:0]
		pkVals = pkVals[:0]
	}, tableRegionSQL, dbName, tableName)

	return pkFields, pkVals, errors.Trace(err)
}

func selectTiDBPartitionRegion(tctx *tcontext.Context, conn *BaseConn, dbName, tableName, partition string) (pkVals [][]string, err error) {
	var startKeys [][]string
	const (
		partitionRegionSQL = "SHOW TABLE `%s`.`%s` PARTITION(`%s`) REGIONS"
		regionRowKey       = "r_"
	)
	logger := tctx.L().With(zap.String("database", dbName), zap.String("table", tableName), zap.String("partition", partition))
	startKeys, err = conn.QuerySQLWithColumns(tctx, []string{"START_KEY"}, fmt.Sprintf(partitionRegionSQL, escapeString(dbName), escapeString(tableName), escapeString(partition)))
	if err != nil {
		return
	}
	for rowID, startKey := range startKeys {
		if rowID == 0 || len(startKey) != 1 {
			continue
		}
		pkVal, err2 := extractTiDBRowIDFromDecodedKey(regionRowKey, startKey[0])
		if err2 != nil {
			logger.Debug("show table region start key doesn't have rowID",
				zap.Int("rowID", rowID), zap.String("startKey", startKey[0]), zap.Error(err2))
		} else {
			pkVals = append(pkVals, []string{pkVal})
		}
	}

	return pkVals, nil
}

func extractTiDBRowIDFromDecodedKey(indexField, key string) (string, error) {
	if p := strings.Index(key, indexField); p != -1 {
		p += len(indexField)
		return key[p:], nil
	}
	return "", errors.Errorf("decoded key %s doesn't have %s field", key, indexField)
}

func getListTableTypeByConf(conf *Config) listTableType {
	// use listTableByShowTableStatus by default because it has better performance
	listType := listTableByShowTableStatus
	if conf.Consistency == consistencyTypeLock {
		// for consistency lock, we need to build the tables to dump as soon as possible
		listType = listTableByInfoSchema
	} else if conf.Consistency == consistencyTypeFlush && matchMysqlBugversion(conf.ServerInfo) {
		// For some buggy versions of mysql, we need a workaround to get a list of table names.
		listType = listTableByShowFullTables
	}
	return listType
}

func prepareTableListToDump(tctx *tcontext.Context, conf *Config, db *sql.Conn) error {
	if conf.specifiedTables {
		return nil
	}
	databases, err := prepareDumpingDatabases(tctx, conf, db)
	if err != nil {
		return err
	}

	tableTypes := []TableType{TableTypeBase}
	if !conf.NoViews {
		tableTypes = append(tableTypes, TableTypeView)
	}
	if !conf.NoSequences {
		tableTypes = append(tableTypes, TableTypeSequence)
	}

	ifSeqExists, err := CheckIfSeqExists(db)
	if err != nil {
		return err
	}
	var listType listTableType
	if ifSeqExists {
		listType = listTableByShowFullTables
	} else {
		listType = getListTableTypeByConf(conf)
	}

	conf.Tables, err = ListAllDatabasesTables(tctx, db, databases, listType, tableTypes...)
	if err != nil {
		return err
	}

	filterTables(tctx, conf)
	return nil
}

func dumpTableMeta(tctx *tcontext.Context, conf *Config, conn *BaseConn, db string, table *TableInfo) (TableMeta, error) {
	tbl := table.Name
	selectField, selectLen, err := buildSelectField(tctx, conn, db, tbl, conf.CompleteInsert)
	if err != nil {
		return nil, err
	}
	var (
		colTypes         []*sql.ColumnType
		hasImplicitRowID bool
	)
	if conf.ServerInfo.ServerType == version.ServerTypeTiDB {
		hasImplicitRowID, err = SelectTiDBRowID(tctx, conn, db, tbl)
		if err != nil {
			tctx.L().Info("check implicit rowID failed", zap.String("database", db), zap.String("table", tbl), log.ShortError(err))
		}
	}

	// If all columns are generated
	if table.Type == TableTypeBase {
		if selectField == "" {
			colTypes, err = GetColumnTypes(tctx, conn, "*", db, tbl)
		} else {
			colTypes, err = GetColumnTypes(tctx, conn, selectField, db, tbl)
		}
	}
	if err != nil {
		return nil, err
	}

	meta := &tableMeta{
		avgRowLength:     table.AvgRowLength,
		database:         db,
		table:            tbl,
		colTypes:         colTypes,
		selectedField:    selectField,
		selectedLen:      selectLen,
		hasImplicitRowID: hasImplicitRowID,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}

	if conf.NoSchemas {
		return meta, nil
	}
	switch table.Type {
	case TableTypeView:
		viewName := table.Name
		createTableSQL, createViewSQL, err1 := ShowCreateView(tctx, conn, db, viewName)
		if err1 != nil {
			return meta, err1
		}
		meta.showCreateTable = createTableSQL
		meta.showCreateView = createViewSQL
		return meta, nil
	case TableTypeSequence:
		sequenceName := table.Name
		createSequenceSQL, err2 := ShowCreateSequence(tctx, conn, db, sequenceName, conf)
		if err2 != nil {
			return meta, err2
		}
		meta.showCreateTable = createSequenceSQL
		return meta, nil
	}

	createTableSQL, err := ShowCreateTable(tctx, conn, db, tbl)
	if err != nil {
		return nil, err
	}
	meta.showCreateTable = createTableSQL
	return meta, nil
}

func (d *Dumper) dumpSQL(tctx *tcontext.Context, metaConn *BaseConn, taskChan chan<- Task) {
	conf := d.conf
	meta := &tableMeta{}
	data := newTableData(conf.SQL, 0, true)
	task := NewTaskTableData(meta, data, 0, 1)
	c := detectEstimateRows(tctx, metaConn, fmt.Sprintf("EXPLAIN %s", conf.SQL), []string{"rows", "estRows", "count"})
	AddCounter(estimateTotalRowsCounter, conf.Labels, float64(c))
	atomic.StoreInt64(&d.totalTables, int64(1))
	d.sendTaskToChan(tctx, task, taskChan)
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
	if d.dbHandle != nil {
		return d.dbHandle.Close()
	}
	return nil
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

func initLogger(d *Dumper) error {
	conf := d.conf
	var (
		logger log.Logger
		err    error
		props  *pclog.ZapProperties
	)
	// conf.Logger != nil means dumpling is used as a library
	if conf.Logger != nil {
		logger = log.NewAppLogger(conf.Logger)
	} else {
		logger, props, err = log.InitAppLogger(&log.Config{
			Level:  conf.LogLevel,
			File:   conf.LogFile,
			Format: conf.LogFormat,
		})
		if err != nil {
			return errors.Trace(err)
		}
		pclog.ReplaceGlobals(logger.Logger, props)
		cli.LogLongVersion(logger)
	}
	d.tctx = d.tctx.WithLogger(logger)
	return nil
}

// createExternalStore is an initialization step of Dumper.
func createExternalStore(d *Dumper) error {
	tctx, conf := d.tctx, d.conf
	extStore, err := conf.createExternalStorage(tctx)
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
			err := startDumplingService(d.tctx, conf.StatusAddr)
			if err != nil {
				d.L().Info("meet error when stopping dumpling http service", log.ShortError(err))
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
	versionStr, err := version.FetchVersion(d.tctx.Context, db)
	if err != nil {
		conf.ServerInfo = ServerInfoUnknown
		return err
	}
	conf.ServerInfo = version.ParseServerInfo(versionStr)
	return nil
}

// resolveAutoConsistency is an initialization step of Dumper.
func resolveAutoConsistency(d *Dumper) error {
	conf := d.conf
	if conf.Consistency != consistencyTypeAuto {
		return nil
	}
	switch conf.ServerInfo.ServerType {
	case version.ServerTypeTiDB:
		conf.Consistency = consistencyTypeSnapshot
	case version.ServerTypeMySQL, version.ServerTypeMariaDB:
		conf.Consistency = consistencyTypeFlush
	default:
		conf.Consistency = consistencyTypeNone
	}
	return nil
}

func validateResolveAutoConsistency(d *Dumper) error {
	conf := d.conf
	if conf.Consistency != consistencyTypeSnapshot && conf.Snapshot != "" {
		return errors.Errorf("can't specify --snapshot when --consistency isn't snapshot, resolved consistency: %s", conf.Consistency)
	}
	return nil
}

// tidbSetPDClientForGC is an initialization step of Dumper.
func tidbSetPDClientForGC(d *Dumper) error {
	tctx, si, pool := d.tctx, d.conf.ServerInfo, d.dbHandle
	if si.ServerType != version.ServerTypeTiDB ||
		si.ServerVersion == nil ||
		si.ServerVersion.Compare(*gcSafePointVersion) < 0 {
		return nil
	}
	pdAddrs, err := GetPdAddrs(tctx, pool)
	if err != nil {
		tctx.L().Info("meet some problem while fetching pd addrs. This won't affect dump process", log.ShortError(err))
		return nil
	}
	if len(pdAddrs) > 0 {
		doPdGC, err := checkSameCluster(tctx, pool, pdAddrs)
		if err != nil {
			tctx.L().Info("meet error while check whether fetched pd addr and TiDB belong to one cluster. This won't affect dump process", log.ShortError(err), zap.Strings("pdAddrs", pdAddrs))
		} else if doPdGC {
			pdClient, err := pd.NewClientWithContext(tctx, pdAddrs, pd.SecurityOption{})
			if err != nil {
				tctx.L().Info("create pd client to control GC failed. This won't affect dump process", log.ShortError(err), zap.Strings("pdAddrs", pdAddrs))
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
	pool, tctx := d.dbHandle, d.tctx
	snapshotConsistency := consistency == "snapshot"
	if conf.Snapshot == "" && (doPdGC || snapshotConsistency) {
		conn, err := pool.Conn(tctx)
		if err != nil {
			tctx.L().Warn("fail to open connection to get snapshot from TiDB", log.ShortError(err))
			// for consistency snapshot, we must get a snapshot here, or we will dump inconsistent data, but for other consistency we can ignore this error.
			if !snapshotConsistency {
				err = nil
			}
			return err
		}
		snapshot, err := getSnapshot(conn)
		_ = conn.Close()
		if err != nil {
			tctx.L().Warn("fail to get snapshot from TiDB", log.ShortError(err))
			// for consistency snapshot, we must get a snapshot here, or we will dump inconsistent data, but for other consistency we can ignore this error.
			if !snapshotConsistency {
				err = nil
			}
			return err
		}
		conf.Snapshot = snapshot
	}
	return nil
}

// tidbStartGCSavepointUpdateService is an initialization step of Dumper.
func tidbStartGCSavepointUpdateService(d *Dumper) error {
	tctx, pool, conf := d.tctx, d.dbHandle, d.conf
	snapshot, si := conf.Snapshot, conf.ServerInfo
	if d.tidbPDClientForGC != nil {
		snapshotTS, err := parseSnapshotToTSO(pool, snapshot)
		if err != nil {
			return err
		}
		go updateServiceSafePoint(tctx, d.tidbPDClientForGC, defaultDumpGCSafePointTTL, snapshotTS)
	} else if si.ServerType == version.ServerTypeTiDB {
		tctx.L().Warn("If the amount of data to dump is large, criteria: (data more than 60GB or dumped time more than 10 minutes)\n" +
			"you'd better adjust the tikv_gc_life_time to avoid export failure due to TiDB GC during the dump process.\n" +
			"Before dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '720h' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n" +
			"After dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '10m' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n")
	}
	return nil
}

func updateServiceSafePoint(tctx *tcontext.Context, pdClient pd.Client, ttl int64, snapshotTS uint64) {
	updateInterval := time.Duration(ttl/2) * time.Second
	tick := time.NewTicker(updateInterval)
	dumplingServiceSafePointID := fmt.Sprintf("%s_%d", dumplingServiceSafePointPrefix, time.Now().UnixNano())
	tctx.L().Info("generate dumpling gc safePoint id", zap.String("id", dumplingServiceSafePointID))

	for {
		tctx.L().Debug("update PD safePoint limit with ttl",
			zap.Uint64("safePoint", snapshotTS),
			zap.Int64("ttl", ttl))
		for retryCnt := 0; retryCnt <= 10; retryCnt++ {
			_, err := pdClient.UpdateServiceGCSafePoint(tctx, dumplingServiceSafePointID, ttl, snapshotTS)
			if err == nil {
				break
			}
			tctx.L().Debug("update PD safePoint failed", log.ShortError(err), zap.Int("retryTime", retryCnt))
			select {
			case <-tctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		select {
		case <-tctx.Done():
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
	if si.ServerType == version.ServerTypeTiDB && conf.TiDBMemQuotaQuery != UnspecifiedSize {
		sessionParam[TiDBMemQuotaQueryName] = conf.TiDBMemQuotaQuery
	}
	var err error
	if snapshot != "" {
		if si.ServerType != version.ServerTypeTiDB {
			return errors.New("snapshot consistency is not supported for this server")
		}
		if consistency == consistencyTypeSnapshot {
			conf.ServerInfo.HasTiKV, err = CheckTiDBWithTiKV(pool)
			if err != nil {
				d.L().Info("cannot check whether TiDB has TiKV, will apply tidb_snapshot by default. This won't affect dump process", log.ShortError(err))
			}
			if conf.ServerInfo.HasTiKV {
				sessionParam["tidb_snapshot"] = snapshot
			}
		}
	}
	if d.dbHandle, err = resetDBWithSessionParams(d.tctx, pool, conf.GetDSN(""), conf.SessionParams); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *Dumper) renewSelectTableRegionFuncForLowerTiDB(tctx *tcontext.Context) error {
	conf := d.conf
	if !(conf.ServerInfo.ServerType == version.ServerTypeTiDB && conf.ServerInfo.ServerVersion != nil && conf.ServerInfo.HasTiKV &&
		conf.ServerInfo.ServerVersion.Compare(*decodeRegionVersion) >= 0 &&
		conf.ServerInfo.ServerVersion.Compare(*gcSafePointVersion) < 0) {
		tctx.L().Debug("no need to build region info because database is not TiDB 3.x")
		return nil
	}
	// for TiDB v3.0+, the original selectTiDBTableRegionFunc will always fail,
	// because TiDB v3.0 doesn't have `tidb_decode_key` function nor `DB_NAME`,`TABLE_NAME` columns in `INFORMATION_SCHEMA.TIKV_REGION_STATUS`.
	// reference: https://github.com/pingcap/tidb/blob/c497d5c/dumpling/export/dump.go#L775
	// To avoid this function continuously returning errors and confusing users because we fail to init this function at first,
	// selectTiDBTableRegionFunc is set to always return an ignorable error at first.
	d.selectTiDBTableRegionFunc = func(_ *tcontext.Context, _ *BaseConn, meta TableMeta) (pkFields []string, pkVals [][]string, err error) {
		return nil, nil, errors.Annotatef(emptyHandleValsErr, "table: `%s`.`%s`", escapeString(meta.DatabaseName()), escapeString(meta.TableName()))
	}
	dbHandle, err := openDBFunc("mysql", conf.GetDSN(""))
	if err != nil {
		return errors.Trace(err)
	}
	defer dbHandle.Close()
	conn, err := dbHandle.Conn(tctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()
	dbInfos, err := GetDBInfo(conn, DatabaseTablesToMap(conf.Tables))
	if err != nil {
		return errors.Trace(err)
	}
	regionsInfo, err := GetRegionInfos(conn)
	if err != nil {
		return errors.Trace(err)
	}
	tikvHelper := &helper.Helper{}
	tableInfos := tikvHelper.GetRegionsTableInfo(regionsInfo, dbInfos)

	tableInfoMap := make(map[string]map[string][]int64, len(conf.Tables))
	for _, region := range regionsInfo.Regions {
		tableList := tableInfos[region.ID]
		for _, table := range tableList {
			db, tbl := table.DB.Name.O, table.Table.Name.O
			if _, ok := tableInfoMap[db]; !ok {
				tableInfoMap[db] = make(map[string][]int64, len(conf.Tables[db]))
			}

			key, err := hex.DecodeString(region.StartKey)
			if err != nil {
				d.L().Debug("invalid region start key", log.ShortError(err), zap.String("key", region.StartKey))
				continue
			}
			// Auto decode byte if needed.
			_, bs, err := codec.DecodeBytes(key, nil)
			if err == nil {
				key = bs
			}
			// Try to decode it as a record key.
			tableID, handle, err := tablecodec.DecodeRecordKey(key)
			if err != nil {
				d.L().Debug("cannot decode region start key", log.ShortError(err), zap.String("key", region.StartKey), zap.Int64("tableID", tableID))
				continue
			}
			if handle.IsInt() {
				tableInfoMap[db][tbl] = append(tableInfoMap[db][tbl], handle.IntValue())
			} else {
				d.L().Debug("not an int handle", log.ShortError(err), zap.Stringer("handle", handle))
			}
		}
	}
	for _, tbInfos := range tableInfoMap {
		for _, tbInfoLoop := range tbInfos {
			// make sure tbInfo is only used in this loop
			tbInfo := tbInfoLoop
			sort.Slice(tbInfo, func(i, j int) bool {
				return tbInfo[i] < tbInfo[j]
			})
		}
	}

	d.selectTiDBTableRegionFunc = func(tctx *tcontext.Context, conn *BaseConn, meta TableMeta) (pkFields []string, pkVals [][]string, err error) {
		pkFields, _, err = selectTiDBRowKeyFields(tctx, conn, meta, checkTiDBTableRegionPkFields)
		if err != nil {
			return
		}
		dbName, tableName := meta.DatabaseName(), meta.TableName()
		if tbInfos, ok := tableInfoMap[dbName]; ok {
			if tbInfo, ok := tbInfos[tableName]; ok {
				pkVals = make([][]string, len(tbInfo))
				for i, val := range tbInfo {
					pkVals[i] = []string{strconv.FormatInt(val, 10)}
				}
			}
		}
		return
	}

	return nil
}
