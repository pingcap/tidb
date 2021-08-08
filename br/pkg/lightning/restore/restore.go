// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/importer"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/lightning/web"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/util/collate"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"modernc.org/mathutil"
)

const (
	FullLevelCompact = -1
	Level1Compact    = 1
)

const (
	defaultGCLifeTime = 100 * time.Hour
)

const (
	indexEngineID = -1
)

const (
	compactStateIdle int32 = iota
	compactStateDoing
)

const (
	taskMetaTableName  = "task_meta"
	tableMetaTableName = "table_meta"
	// CreateTableMetadataTable stores the per-table sub jobs information used by TiDB Lightning
	CreateTableMetadataTable = `CREATE TABLE IF NOT EXISTS %s (
		task_id 			BIGINT(20) UNSIGNED,
		table_id 			BIGINT(64) NOT NULL,
		table_name 			VARCHAR(64) NOT NULL,
		row_id_base 		BIGINT(20) NOT NULL DEFAULT 0,
		row_id_max 			BIGINT(20) NOT NULL DEFAULT 0,
		total_kvs_base 		BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		total_bytes_base 	BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		checksum_base 		BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		total_kvs 			BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		total_bytes 		BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		checksum 			BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		status 				VARCHAR(32) NOT NULL,
		PRIMARY KEY (table_id, task_id)
	);`
	// CreateTaskMetaTable stores the pre-lightning metadata used by TiDB Lightning
	CreateTaskMetaTable = `CREATE TABLE IF NOT EXISTS %s (
		task_id BIGINT(20) UNSIGNED NOT NULL,
		pd_cfgs VARCHAR(2048) NOT NULL DEFAULT '',
		status  VARCHAR(32) NOT NULL,
		PRIMARY KEY (task_id)
	);`

	compactionLowerThreshold = 512 * units.MiB
	compactionUpperThreshold = 32 * units.GiB
)

// DeliverPauser is a shared pauser to pause progress to (*chunkRestore).encodeLoop
var DeliverPauser = common.NewPauser()

// nolint:gochecknoinits // TODO: refactor
func init() {
	failpoint.Inject("SetMinDeliverBytes", func(v failpoint.Value) {
		minDeliverBytes = uint64(v.(int))
	})
}

type saveCp struct {
	tableName string
	merger    checkpoints.TableCheckpointMerger
}

type errorSummary struct {
	status checkpoints.CheckpointStatus
	err    error
}

type errorSummaries struct {
	sync.Mutex
	logger  log.Logger
	summary map[string]errorSummary
}

// makeErrorSummaries returns an initialized errorSummaries instance
func makeErrorSummaries(logger log.Logger) errorSummaries {
	return errorSummaries{
		logger:  logger,
		summary: make(map[string]errorSummary),
	}
}

func (es *errorSummaries) emitLog() {
	es.Lock()
	defer es.Unlock()

	if errorCount := len(es.summary); errorCount > 0 {
		logger := es.logger
		logger.Error("tables failed to be imported", zap.Int("count", errorCount))
		for tableName, errorSummary := range es.summary {
			logger.Error("-",
				zap.String("table", tableName),
				zap.String("status", errorSummary.status.MetricName()),
				log.ShortError(errorSummary.err),
			)
		}
	}
}

func (es *errorSummaries) record(tableName string, err error, status checkpoints.CheckpointStatus) {
	es.Lock()
	defer es.Unlock()
	es.summary[tableName] = errorSummary{status: status, err: err}
}

const (
	diskQuotaStateIdle int32 = iota
	diskQuotaStateChecking
	diskQuotaStateImporting

	diskQuotaMaxReaders = 1 << 30
)

// diskQuotaLock is essentially a read/write lock. The implement here is inspired by sync.RWMutex.
// diskQuotaLock removed the unnecessary blocking `RLock` method and add a non-blocking `TryRLock` method.
type diskQuotaLock struct {
	w           sync.Mutex    // held if there are pending writers
	writerSem   chan struct{} // semaphore for writers to wait for completing readers
	readerCount atomic.Int32  // number of pending readers
	readerWait  atomic.Int32  // number of departing readers
}

func newDiskQuotaLock() *diskQuotaLock {
	return &diskQuotaLock{writerSem: make(chan struct{})}
}

func (d *diskQuotaLock) Lock() {
	d.w.Lock()
	// Announce to readers there is a pending writer.
	r := d.readerCount.Sub(diskQuotaMaxReaders) + diskQuotaMaxReaders
	if r != 0 && d.readerWait.Add(r) != 0 {
		// Wait for active readers.
		<-d.writerSem
	}
}

func (d *diskQuotaLock) Unlock() {
	d.readerCount.Add(diskQuotaMaxReaders)
	d.w.Unlock()
}

func (d *diskQuotaLock) TryRLock() (locked bool) {
	r := d.readerCount.Load()
	for r >= 0 {
		if d.readerCount.CAS(r, r+1) {
			return true
		}
		r = d.readerCount.Load()
	}
	return false
}

func (d *diskQuotaLock) RUnlock() {
	if d.readerCount.Dec() < 0 {
		if d.readerWait.Dec() == 0 {
			// The last reader unblocks the writer.
			d.writerSem <- struct{}{}
		}
	}
}

type Controller struct {
	cfg           *config.Config
	dbMetas       []*mydump.MDDatabaseMeta
	dbInfos       map[string]*checkpoints.TidbDBInfo
	tableWorkers  *worker.Pool
	indexWorkers  *worker.Pool
	regionWorkers *worker.Pool
	ioWorkers     *worker.Pool
	checksumWorks *worker.Pool
	pauser        *common.Pauser
	backend       backend.Backend
	tidbGlue      glue.Glue

	alterTableLock sync.Mutex
	sysVars        map[string]string
	tls            *common.TLS
	checkTemplate  Template

	errorSummaries errorSummaries

	checkpointsDB checkpoints.DB
	saveCpCh      chan saveCp
	checkpointsWg sync.WaitGroup

	closedEngineLimit *worker.Pool
	store             storage.ExternalStorage
	metaMgrBuilder    metaMgrBuilder

	diskQuotaLock  *diskQuotaLock
	diskQuotaState atomic.Int32
	compactState   atomic.Int32
}

func NewRestoreController(
	ctx context.Context,
	dbMetas []*mydump.MDDatabaseMeta,
	cfg *config.Config,
	s storage.ExternalStorage,
	g glue.Glue,
) (*Controller, error) {
	return NewRestoreControllerWithPauser(ctx, dbMetas, cfg, s, DeliverPauser, g)
}

func NewRestoreControllerWithPauser(
	ctx context.Context,
	dbMetas []*mydump.MDDatabaseMeta,
	cfg *config.Config,
	s storage.ExternalStorage,
	pauser *common.Pauser,
	g glue.Glue,
) (*Controller, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		return nil, err
	}

	cpdb, err := g.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return nil, errors.Annotate(err, "open checkpoint db failed")
	}

	taskCp, err := cpdb.TaskCheckpoint(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "get task checkpoint failed")
	}
	if err := verifyCheckpoint(cfg, taskCp); err != nil {
		return nil, errors.Trace(err)
	}
	// reuse task id to reuse task meta correctly.
	if taskCp != nil {
		cfg.TaskID = taskCp.TaskID
	}

	var backend backend.Backend
	switch cfg.TikvImporter.Backend {
	case config.BackendImporter:
		var err error
		backend, err = importer.NewImporter(ctx, tls, cfg.TikvImporter.Addr, cfg.TiDB.PdAddr)
		if err != nil {
			return nil, errors.Annotate(err, "open importer backend failed")
		}
	case config.BackendTiDB:
		db, err := DBFromConfig(cfg.TiDB)
		if err != nil {
			return nil, errors.Annotate(err, "open tidb backend failed")
		}
		backend = tidb.NewTiDBBackend(db, cfg.TikvImporter.OnDuplicate)
	case config.BackendLocal:
		var rLimit local.Rlim_t
		rLimit, err = local.GetSystemRLimit()
		if err != nil {
			return nil, err
		}
		maxOpenFiles := int(rLimit / local.Rlim_t(cfg.App.TableConcurrency))
		// check overflow
		if maxOpenFiles < 0 {
			maxOpenFiles = math.MaxInt32
		}

		backend, err = local.NewLocalBackend(ctx, tls, cfg.TiDB.PdAddr, &cfg.TikvImporter,
			cfg.Checkpoint.Enable, g, maxOpenFiles)
		if err != nil {
			return nil, errors.Annotate(err, "build local backend failed")
		}
		err = verifyLocalFile(ctx, cpdb, cfg.TikvImporter.SortedKVDir)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown backend: " + cfg.TikvImporter.Backend)
	}

	var metaBuilder metaMgrBuilder
	switch cfg.TikvImporter.Backend {
	case config.BackendLocal, config.BackendImporter:
		// TODO: support Lightning via SQL
		db, err := g.GetDB()
		if err != nil {
			return nil, errors.Trace(err)
		}
		metaBuilder = &dbMetaMgrBuilder{
			db:           db,
			taskID:       cfg.TaskID,
			schema:       cfg.App.MetaSchemaName,
			needChecksum: cfg.PostRestore.Checksum != config.OpLevelOff,
		}
	default:
		metaBuilder = noopMetaMgrBuilder{}
	}

	rc := &Controller{
		cfg:           cfg,
		dbMetas:       dbMetas,
		tableWorkers:  worker.NewPool(ctx, cfg.App.TableConcurrency, "table"),
		indexWorkers:  worker.NewPool(ctx, cfg.App.IndexConcurrency, "index"),
		regionWorkers: worker.NewPool(ctx, cfg.App.RegionConcurrency, "region"),
		ioWorkers:     worker.NewPool(ctx, cfg.App.IOConcurrency, "io"),
		checksumWorks: worker.NewPool(ctx, cfg.TiDB.ChecksumTableConcurrency, "checksum"),
		pauser:        pauser,
		backend:       backend,
		tidbGlue:      g,
		sysVars:       defaultImportantVariables,
		tls:           tls,
		checkTemplate: NewSimpleTemplate(),

		errorSummaries:    makeErrorSummaries(log.L()),
		checkpointsDB:     cpdb,
		saveCpCh:          make(chan saveCp),
		closedEngineLimit: worker.NewPool(ctx, cfg.App.TableConcurrency*2, "closed-engine"),

		store:          s,
		metaMgrBuilder: metaBuilder,
		diskQuotaLock:  newDiskQuotaLock(),
	}

	return rc, nil
}

func (rc *Controller) Close() {
	rc.backend.Close()
	rc.tidbGlue.GetSQLExecutor().Close()
}

func (rc *Controller) Run(ctx context.Context) error {
	opts := []func(context.Context) error{
		rc.preCheckRequirements,
		rc.setGlobalVariables,
		rc.restoreSchema,
		rc.restoreTables,
		rc.fullCompact,
		rc.switchToNormalMode,
		rc.cleanCheckpoints,
	}

	task := log.L().Begin(zap.InfoLevel, "the whole procedure")

	var err error
	finished := false
outside:
	for i, process := range opts {
		err = process(ctx)
		if i == len(opts)-1 {
			finished = true
		}
		logger := task.With(zap.Int("step", i), log.ShortError(err))

		switch {
		case err == nil:
		case log.IsContextCanceledError(err):
			logger.Info("task canceled")
			err = nil
			break outside
		default:
			logger.Error("run failed")
			fmt.Fprintf(os.Stderr, "Error: %s\n", err)
			break outside // ps : not continue
		}
	}

	// if process is cancelled, should make sure checkpoints are written to db.
	if !finished {
		rc.waitCheckpointFinish()
	}

	task.End(zap.ErrorLevel, err)
	rc.errorSummaries.emitLog()

	return errors.Trace(err)
}

type schemaStmtType int

func (stmtType schemaStmtType) String() string {
	switch stmtType {
	case schemaCreateDatabase:
		return "restore database schema"
	case schemaCreateTable:
		return "restore table schema"
	case schemaCreateView:
		return "restore view schema"
	}
	return "unknown statement of schema"
}

const (
	schemaCreateDatabase schemaStmtType = iota
	schemaCreateTable
	schemaCreateView
)

type schemaJob struct {
	dbName   string
	tblName  string // empty for create db jobs
	stmtType schemaStmtType
	stmts    []*schemaStmt
}

type schemaStmt struct {
	sql string
}

type restoreSchemaWorker struct {
	ctx   context.Context
	quit  context.CancelFunc
	jobCh chan *schemaJob
	errCh chan error
	wg    sync.WaitGroup
	glue  glue.Glue
	store storage.ExternalStorage
}

func (worker *restoreSchemaWorker) makeJobs(
	dbMetas []*mydump.MDDatabaseMeta,
	getTables func(context.Context, string) ([]*model.TableInfo, error),
) error {
	defer func() {
		close(worker.jobCh)
		worker.quit()
	}()
	var err error
	// 1. restore databases, execute statements concurrency
	for _, dbMeta := range dbMetas {
		restoreSchemaJob := &schemaJob{
			dbName:   dbMeta.Name,
			stmtType: schemaCreateDatabase,
			stmts:    make([]*schemaStmt, 0, 1),
		}
		restoreSchemaJob.stmts = append(restoreSchemaJob.stmts, &schemaStmt{
			sql: createDatabaseIfNotExistStmt(dbMeta.Name),
		})
		err = worker.appendJob(restoreSchemaJob)
		if err != nil {
			return err
		}
	}
	err = worker.wait()
	if err != nil {
		return err
	}
	// 2. restore tables, execute statements concurrency
	for _, dbMeta := range dbMetas {
		// we can ignore error here, and let check failed later if schema not match
		tables, _ := getTables(worker.ctx, dbMeta.Name)
		tableMap := make(map[string]struct{})
		for _, t := range tables {
			tableMap[t.Name.L] = struct{}{}
		}
		for _, tblMeta := range dbMeta.Tables {
			if _, ok := tableMap[strings.ToLower(tblMeta.Name)]; ok {
				// we already has this table in TiDB.
				// we should skip ddl job and let SchemaValid check.
				continue
			} else if tblMeta.SchemaFile.FileMeta.Path == "" {
				return errors.Errorf("table `%s`.`%s` schema not found", dbMeta.Name, tblMeta.Name)
			}
			sql, err := tblMeta.GetSchema(worker.ctx, worker.store)
			if sql != "" {
				stmts, err := createTableIfNotExistsStmt(worker.glue.GetParser(), sql, dbMeta.Name, tblMeta.Name)
				if err != nil {
					return err
				}
				restoreSchemaJob := &schemaJob{
					dbName:   dbMeta.Name,
					tblName:  tblMeta.Name,
					stmtType: schemaCreateTable,
					stmts:    make([]*schemaStmt, 0, len(stmts)),
				}
				for _, sql := range stmts {
					restoreSchemaJob.stmts = append(restoreSchemaJob.stmts, &schemaStmt{
						sql: sql,
					})
				}
				err = worker.appendJob(restoreSchemaJob)
				if err != nil {
					return err
				}
			}
			if err != nil {
				return err
			}
		}
	}
	err = worker.wait()
	if err != nil {
		return err
	}
	// 3. restore views. Since views can cross database we must restore views after all table schemas are restored.
	for _, dbMeta := range dbMetas {
		for _, viewMeta := range dbMeta.Views {
			sql, err := viewMeta.GetSchema(worker.ctx, worker.store)
			if sql != "" {
				stmts, err := createTableIfNotExistsStmt(worker.glue.GetParser(), sql, dbMeta.Name, viewMeta.Name)
				if err != nil {
					return err
				}
				restoreSchemaJob := &schemaJob{
					dbName:   dbMeta.Name,
					tblName:  viewMeta.Name,
					stmtType: schemaCreateView,
					stmts:    make([]*schemaStmt, 0, len(stmts)),
				}
				for _, sql := range stmts {
					restoreSchemaJob.stmts = append(restoreSchemaJob.stmts, &schemaStmt{
						sql: sql,
					})
				}
				err = worker.appendJob(restoreSchemaJob)
				if err != nil {
					return err
				}
				// we don't support restore views concurrency, cauz it maybe will raise a error
				err = worker.wait()
				if err != nil {
					return err
				}
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (worker *restoreSchemaWorker) doJob() {
	var session *sql.Conn
	defer func() {
		if session != nil {
			_ = session.Close()
		}
	}()
loop:
	for {
		select {
		case <-worker.ctx.Done():
			// don't `return` or throw `worker.ctx.Err()`here,
			// if we `return`, we can't mark cancelled jobs as done,
			// if we `throw(worker.ctx.Err())`, it will be blocked to death
			break loop
		case job := <-worker.jobCh:
			if job == nil {
				// successful exit
				return
			}
			var err error
			if session == nil {
				session, err = func() (*sql.Conn, error) {
					// TODO: support lightning in SQL
					db, err := worker.glue.GetDB()
					if err != nil {
						return nil, errors.Trace(err)
					}
					return db.Conn(worker.ctx)
				}()
				if err != nil {
					worker.wg.Done()
					worker.throw(err)
					// don't return
					break loop
				}
			}
			logger := log.With(zap.String("db", job.dbName), zap.String("table", job.tblName))
			sqlWithRetry := common.SQLWithRetry{
				Logger: log.L(),
				DB:     session,
			}
			for _, stmt := range job.stmts {
				task := logger.Begin(zap.DebugLevel, fmt.Sprintf("execute SQL: %s", stmt.sql))
				err = sqlWithRetry.Exec(worker.ctx, "run create schema job", stmt.sql)
				task.End(zap.ErrorLevel, err)
				if err != nil {
					err = errors.Annotatef(err, "%s %s failed", job.stmtType.String(), common.UniqueTable(job.dbName, job.tblName))
					worker.wg.Done()
					worker.throw(err)
					// don't return
					break loop
				}
			}
			worker.wg.Done()
		}
	}
	// mark the cancelled job as `Done`, a little tricky,
	// cauz we need make sure `worker.wg.Wait()` wouldn't blocked forever
	for range worker.jobCh {
		worker.wg.Done()
	}
}

func (worker *restoreSchemaWorker) wait() error {
	// avoid to `worker.wg.Wait()` blocked forever when all `doJob`'s goroutine exited.
	// don't worry about goroutine below, it never become a zombie,
	// cauz we have mechanism to clean cancelled jobs in `worker.jobCh`.
	// means whole jobs has been send to `worker.jobCh` would be done.
	waitCh := make(chan struct{})
	go func() {
		worker.wg.Wait()
		close(waitCh)
	}()
	select {
	case err := <-worker.errCh:
		return err
	case <-worker.ctx.Done():
		return worker.ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (worker *restoreSchemaWorker) throw(err error) {
	select {
	case <-worker.ctx.Done():
		// don't throw `worker.ctx.Err()` again, it will be blocked to death.
		return
	case worker.errCh <- err:
		worker.quit()
	}
}

func (worker *restoreSchemaWorker) appendJob(job *schemaJob) error {
	worker.wg.Add(1)
	select {
	case err := <-worker.errCh:
		// cancel the job
		worker.wg.Done()
		return err
	case <-worker.ctx.Done():
		// cancel the job
		worker.wg.Done()
		return worker.ctx.Err()
	case worker.jobCh <- job:
		return nil
	}
}

func (rc *Controller) restoreSchema(ctx context.Context) error {
	// create table with schema file
	// we can handle the duplicated created with createIfNotExist statement
	// and we will check the schema in TiDB is valid with the datafile in DataCheck later.
	logTask := log.L().Begin(zap.InfoLevel, "restore all schema")
	concurrency := utils.MinInt(rc.cfg.App.RegionConcurrency, 8)
	childCtx, cancel := context.WithCancel(ctx)
	worker := restoreSchemaWorker{
		ctx:   childCtx,
		quit:  cancel,
		jobCh: make(chan *schemaJob, concurrency),
		errCh: make(chan error),
		glue:  rc.tidbGlue,
		store: rc.store,
	}
	for i := 0; i < concurrency; i++ {
		go worker.doJob()
	}
	getTableFunc := rc.backend.FetchRemoteTableModels
	if !rc.tidbGlue.OwnsSQLExecutor() {
		getTableFunc = rc.tidbGlue.GetTables
	}
	err := worker.makeJobs(rc.dbMetas, getTableFunc)
	logTask.End(zap.ErrorLevel, err)
	if err != nil {
		return err
	}

	dbInfos, err := LoadSchemaInfo(ctx, rc.dbMetas, getTableFunc)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dbInfos = dbInfos

	if rc.cfg.App.CheckRequirements && rc.tidbGlue.OwnsSQLExecutor() {
		if err = rc.DataCheck(ctx); err != nil {
			return errors.Trace(err)
		}
		// print check template only if check requirements is true.
		fmt.Println(rc.checkTemplate.Output())
		if !rc.checkTemplate.Success() {
			return errors.Errorf("tidb-lightning pre-check failed." +
				" Please fix the failed check(s) or set --check-requirements=false to skip checks")
		}
	}

	// Load new checkpoints
	err = rc.checkpointsDB.Initialize(ctx, rc.cfg, dbInfos)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("InitializeCheckpointExit", func() {
		log.L().Warn("exit triggered", zap.String("failpoint", "InitializeCheckpointExit"))
		os.Exit(0)
	})

	go rc.listenCheckpointUpdates()

	rc.sysVars = ObtainImportantVariables(ctx, rc.tidbGlue.GetSQLExecutor())

	// Estimate the number of chunks for progress reporting
	err = rc.estimateChunkCountIntoMetrics(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// verifyCheckpoint check whether previous task checkpoint is compatible with task config
func verifyCheckpoint(cfg *config.Config, taskCp *checkpoints.TaskCheckpoint) error {
	if taskCp == nil {
		return nil
	}
	// always check the backend value even with 'check-requirements = false'
	retryUsage := "destroy all checkpoints"
	if cfg.Checkpoint.Driver == config.CheckpointDriverFile {
		retryUsage = fmt.Sprintf("delete the file '%s'", cfg.Checkpoint.DSN)
	}
	retryUsage += " and remove all restored tables and try again"

	if cfg.TikvImporter.Backend != taskCp.Backend {
		return errors.Errorf("config 'tikv-importer.backend' value '%s' different from checkpoint value '%s', please %s", cfg.TikvImporter.Backend, taskCp.Backend, retryUsage)
	}

	if cfg.App.CheckRequirements {
		if build.ReleaseVersion != taskCp.LightningVer {
			var displayVer string
			if len(taskCp.LightningVer) != 0 {
				displayVer = fmt.Sprintf("at '%s'", taskCp.LightningVer)
			} else {
				displayVer = "before v4.0.6/v3.0.19"
			}
			return errors.Errorf("lightning version is '%s', but checkpoint was created %s, please %s", build.ReleaseVersion, displayVer, retryUsage)
		}

		errorFmt := "config '%s' value '%s' different from checkpoint value '%s'. You may set 'check-requirements = false' to skip this check or " + retryUsage
		if cfg.Mydumper.SourceDir != taskCp.SourceDir {
			return errors.Errorf(errorFmt, "mydumper.data-source-dir", cfg.Mydumper.SourceDir, taskCp.SourceDir)
		}

		if cfg.TikvImporter.Backend == config.BackendLocal && cfg.TikvImporter.SortedKVDir != taskCp.SortedKVDir {
			return errors.Errorf(errorFmt, "mydumper.sorted-kv-dir", cfg.TikvImporter.SortedKVDir, taskCp.SortedKVDir)
		}

		if cfg.TikvImporter.Backend == config.BackendImporter && cfg.TikvImporter.Addr != taskCp.ImporterAddr {
			return errors.Errorf(errorFmt, "tikv-importer.addr", cfg.TikvImporter.Backend, taskCp.Backend)
		}

		if cfg.TiDB.Host != taskCp.TiDBHost {
			return errors.Errorf(errorFmt, "tidb.host", cfg.TiDB.Host, taskCp.TiDBHost)
		}

		if cfg.TiDB.Port != taskCp.TiDBPort {
			return errors.Errorf(errorFmt, "tidb.port", cfg.TiDB.Port, taskCp.TiDBPort)
		}

		if cfg.TiDB.PdAddr != taskCp.PdAddr {
			return errors.Errorf(errorFmt, "tidb.pd-addr", cfg.TiDB.PdAddr, taskCp.PdAddr)
		}
	}

	return nil
}

// for local backend, we should check if local SST exists in disk, otherwise we'll lost data
func verifyLocalFile(ctx context.Context, cpdb checkpoints.DB, dir string) error {
	targetTables, err := cpdb.GetLocalStoringTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for tableName, engineIDs := range targetTables {
		for _, engineID := range engineIDs {
			_, eID := backend.MakeUUID(tableName, engineID)
			file := local.File{UUID: eID}
			err := file.Exist(dir)
			if err != nil {
				log.L().Error("can't find local file",
					zap.String("table name", tableName),
					zap.Int32("engine ID", engineID))
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (rc *Controller) estimateChunkCountIntoMetrics(ctx context.Context) error {
	estimatedChunkCount := 0.0
	estimatedEngineCnt := int64(0)
	batchSize := int64(rc.cfg.Mydumper.BatchSize)
	for _, dbMeta := range rc.dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			tableName := common.UniqueTable(dbMeta.Name, tableMeta.Name)
			dbCp, err := rc.checkpointsDB.Get(ctx, tableName)
			if err != nil {
				return errors.Trace(err)
			}

			fileChunks := make(map[string]float64)
			for engineID, eCp := range dbCp.Engines {
				if eCp.Status < checkpoints.CheckpointStatusImported {
					estimatedEngineCnt++
				}
				if engineID == indexEngineID {
					continue
				}
				for _, c := range eCp.Chunks {
					if _, ok := fileChunks[c.Key.Path]; !ok {
						fileChunks[c.Key.Path] = 0.0
					}
					remainChunkCnt := float64(c.Chunk.EndOffset-c.Chunk.Offset) / float64(c.Chunk.EndOffset-c.Key.Offset)
					fileChunks[c.Key.Path] += remainChunkCnt
				}
			}
			// estimate engines count if engine cp is empty
			if len(dbCp.Engines) == 0 {
				estimatedEngineCnt += ((tableMeta.TotalSize + batchSize - 1) / batchSize) + 1
			}
			for _, fileMeta := range tableMeta.DataFiles {
				if cnt, ok := fileChunks[fileMeta.FileMeta.Path]; ok {
					estimatedChunkCount += cnt
					continue
				}
				if fileMeta.FileMeta.Type == mydump.SourceTypeCSV {
					cfg := rc.cfg.Mydumper
					if fileMeta.FileMeta.FileSize > int64(cfg.MaxRegionSize) && cfg.StrictFormat && !cfg.CSV.Header {
						estimatedChunkCount += math.Round(float64(fileMeta.FileMeta.FileSize) / float64(cfg.MaxRegionSize))
					} else {
						estimatedChunkCount++
					}
				} else {
					estimatedChunkCount++
				}
			}
		}
	}
	metric.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated).Add(estimatedChunkCount)
	metric.ProcessedEngineCounter.WithLabelValues(metric.ChunkStateEstimated, metric.TableResultSuccess).
		Add(float64(estimatedEngineCnt))
	rc.tidbGlue.Record(glue.RecordEstimatedChunk, uint64(estimatedChunkCount))
	return nil
}

func (rc *Controller) saveStatusCheckpoint(tableName string, engineID int32, err error, statusIfSucceed checkpoints.CheckpointStatus) {
	merger := &checkpoints.StatusCheckpointMerger{Status: statusIfSucceed, EngineID: engineID}

	log.L().Debug("update checkpoint", zap.String("table", tableName), zap.Int32("engine_id", engineID),
		zap.Uint8("new_status", uint8(statusIfSucceed)), zap.Error(err))

	switch {
	case err == nil:
		break
	case !common.IsContextCanceledError(err):
		merger.SetInvalid()
		rc.errorSummaries.record(tableName, err, statusIfSucceed)
	default:
		return
	}

	if engineID == checkpoints.WholeTableEngineID {
		metric.RecordTableCount(statusIfSucceed.MetricName(), err)
	} else {
		metric.RecordEngineCount(statusIfSucceed.MetricName(), err)
	}

	rc.saveCpCh <- saveCp{tableName: tableName, merger: merger}
}

// listenCheckpointUpdates will combine several checkpoints together to reduce database load.
func (rc *Controller) listenCheckpointUpdates() {
	rc.checkpointsWg.Add(1)

	var lock sync.Mutex
	coalesed := make(map[string]*checkpoints.TableCheckpointDiff)

	hasCheckpoint := make(chan struct{}, 1)
	defer close(hasCheckpoint)

	go func() {
		for range hasCheckpoint {
			lock.Lock()
			cpd := coalesed
			coalesed = make(map[string]*checkpoints.TableCheckpointDiff)
			lock.Unlock()

			if len(cpd) > 0 {
				rc.checkpointsDB.Update(cpd)
				web.BroadcastCheckpointDiff(cpd)
			}
			rc.checkpointsWg.Done()
		}
	}()

	for scp := range rc.saveCpCh {
		lock.Lock()
		cpd, ok := coalesed[scp.tableName]
		if !ok {
			cpd = checkpoints.NewTableCheckpointDiff()
			coalesed[scp.tableName] = cpd
		}
		scp.merger.MergeInto(cpd)

		if len(hasCheckpoint) == 0 {
			rc.checkpointsWg.Add(1)
			hasCheckpoint <- struct{}{}
		}

		lock.Unlock()

		//nolint:scopelint // This would be either INLINED or ERASED, at compile time.
		failpoint.Inject("FailIfImportedChunk", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*checkpoints.ChunkCheckpointMerger); ok && merger.Checksum.SumKVS() >= uint64(val.(int)) {
				rc.checkpointsWg.Done()
				rc.checkpointsWg.Wait()
				panic("forcing failure due to FailIfImportedChunk")
			}
		})

		//nolint:scopelint // This would be either INLINED or ERASED, at compile time.
		failpoint.Inject("FailIfStatusBecomes", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*checkpoints.StatusCheckpointMerger); ok && merger.EngineID >= 0 && int(merger.Status) == val.(int) {
				rc.checkpointsWg.Done()
				rc.checkpointsWg.Wait()
				panic("forcing failure due to FailIfStatusBecomes")
			}
		})

		//nolint:scopelint // This would be either INLINED or ERASED, at compile time.
		failpoint.Inject("FailIfIndexEngineImported", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*checkpoints.StatusCheckpointMerger); ok &&
				merger.EngineID == checkpoints.WholeTableEngineID &&
				merger.Status == checkpoints.CheckpointStatusIndexImported && val.(int) > 0 {
				rc.checkpointsWg.Done()
				rc.checkpointsWg.Wait()
				panic("forcing failure due to FailIfIndexEngineImported")
			}
		})

		//nolint:scopelint // This would be either INLINED or ERASED, at compile time.
		failpoint.Inject("KillIfImportedChunk", func(val failpoint.Value) {
			if merger, ok := scp.merger.(*checkpoints.ChunkCheckpointMerger); ok && merger.Checksum.SumKVS() >= uint64(val.(int)) {
				if err := common.KillMySelf(); err != nil {
					log.L().Warn("KillMySelf() failed to kill itself", log.ShortError(err))
				}
			}
		})
	}
	rc.checkpointsWg.Done()
}

// buildRunPeriodicActionAndCancelFunc build the runPeriodicAction func and a cancel func
func (rc *Controller) buildRunPeriodicActionAndCancelFunc(ctx context.Context, stop <-chan struct{}) (func(), func(bool)) {
	cancelFuncs := make([]func(bool), 0)
	closeFuncs := make([]func(), 0)
	// a nil channel blocks forever.
	// if the cron duration is zero we use the nil channel to skip the action.
	var logProgressChan <-chan time.Time
	if rc.cfg.Cron.LogProgress.Duration > 0 {
		logProgressTicker := time.NewTicker(rc.cfg.Cron.LogProgress.Duration)
		closeFuncs = append(closeFuncs, func() {
			logProgressTicker.Stop()
		})
		logProgressChan = logProgressTicker.C
	}

	glueProgressTicker := time.NewTicker(3 * time.Second)
	closeFuncs = append(closeFuncs, func() {
		glueProgressTicker.Stop()
	})

	var switchModeChan <-chan time.Time
	// tidb backend don't need to switch tikv to import mode
	if rc.cfg.TikvImporter.Backend != config.BackendTiDB && rc.cfg.Cron.SwitchMode.Duration > 0 {
		switchModeTicker := time.NewTicker(rc.cfg.Cron.SwitchMode.Duration)
		cancelFuncs = append(cancelFuncs, func(bool) { switchModeTicker.Stop() })
		cancelFuncs = append(cancelFuncs, func(do bool) {
			if do {
				log.L().Info("switch to normal mode")
				if err := rc.switchToNormalMode(ctx); err != nil {
					log.L().Warn("switch tikv to normal mode failed", zap.Error(err))
				}
			}
		})
		switchModeChan = switchModeTicker.C
	}

	var checkQuotaChan <-chan time.Time
	// only local storage has disk quota concern.
	if rc.cfg.TikvImporter.Backend == config.BackendLocal && rc.cfg.Cron.CheckDiskQuota.Duration > 0 {
		checkQuotaTicker := time.NewTicker(rc.cfg.Cron.CheckDiskQuota.Duration)
		cancelFuncs = append(cancelFuncs, func(bool) { checkQuotaTicker.Stop() })
		checkQuotaChan = checkQuotaTicker.C
	}

	return func() {
			defer func() {
				for _, f := range closeFuncs {
					f()
				}
			}()
			// tidb backend don't need to switch tikv to import mode
			if rc.cfg.TikvImporter.Backend != config.BackendTiDB && rc.cfg.Cron.SwitchMode.Duration > 0 {
				rc.switchToImportMode(ctx)
			}
			start := time.Now()
			for {
				select {
				case <-ctx.Done():
					log.L().Warn("stopping periodic actions", log.ShortError(ctx.Err()))
					return
				case <-stop:
					log.L().Info("everything imported, stopping periodic actions")
					return

				case <-switchModeChan:
					// periodically switch to import mode, as requested by TiKV 3.0
					rc.switchToImportMode(ctx)

				case <-logProgressChan:
					// log the current progress periodically, so OPS will know that we're still working
					nanoseconds := float64(time.Since(start).Nanoseconds())
					// the estimated chunk is not accurate(likely under estimated), but the actual count is not accurate
					// before the last table start, so use the bigger of the two should be a workaround
					estimated := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated))
					pending := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
					if estimated < pending {
						estimated = pending
					}
					finished := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished))
					totalTables := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))
					completedTables := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStateCompleted, metric.TableResultSuccess))
					bytesRead := metric.ReadHistogramSum(metric.RowReadBytesHistogram)
					engineEstimated := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues(metric.ChunkStateEstimated, metric.TableResultSuccess))
					enginePending := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues(metric.ChunkStatePending, metric.TableResultSuccess))
					if engineEstimated < enginePending {
						engineEstimated = enginePending
					}
					engineFinished := metric.ReadCounter(metric.ProcessedEngineCounter.WithLabelValues(metric.TableStateImported, metric.TableResultSuccess))
					bytesWritten := metric.ReadCounter(metric.BytesCounter.WithLabelValues(metric.TableStateWritten))
					bytesImported := metric.ReadCounter(metric.BytesCounter.WithLabelValues(metric.TableStateImported))

					var state string
					var remaining zap.Field
					switch {
					case finished >= estimated:
						if engineFinished < engineEstimated {
							state = "importing"
						} else {
							state = "post-processing"
						}
					case finished > 0:
						state = "writing"
					default:
						state = "preparing"
					}

					// since we can't accurately estimate the extra time cost by import after all writing are finished,
					// so here we use estimatedWritingProgress * 0.8 + estimatedImportingProgress * 0.2 as the total
					// progress.
					remaining = zap.Skip()
					totalPercent := 0.0
					if finished > 0 {
						writePercent := math.Min(finished/estimated, 1.0)
						importPercent := 1.0
						if bytesWritten > 0 {
							totalBytes := bytesWritten / writePercent
							importPercent = math.Min(bytesImported/totalBytes, 1.0)
						}
						totalPercent = writePercent*0.8 + importPercent*0.2
						if totalPercent < 1.0 {
							remainNanoseconds := (1.0 - totalPercent) / totalPercent * nanoseconds
							remaining = zap.Duration("remaining", time.Duration(remainNanoseconds).Round(time.Second))
						}
					}

					formatPercent := func(finish, estimate float64) string {
						speed := ""
						if estimated > 0 {
							speed = fmt.Sprintf(" (%.1f%%)", finish/estimate*100)
						}
						return speed
					}

					// avoid output bytes speed if there are no unfinished chunks
					chunkSpeed := zap.Skip()
					if bytesRead > 0 {
						chunkSpeed = zap.Float64("speed(MiB/s)", bytesRead/(1048576e-9*nanoseconds))
					}

					// Note: a speed of 28 MiB/s roughly corresponds to 100 GiB/hour.
					log.L().Info("progress",
						zap.String("total", fmt.Sprintf("%.1f%%", totalPercent*100)),
						// zap.String("files", fmt.Sprintf("%.0f/%.0f (%.1f%%)", finished, estimated, finished/estimated*100)),
						zap.String("tables", fmt.Sprintf("%.0f/%.0f%s", completedTables, totalTables, formatPercent(completedTables, totalTables))),
						zap.String("chunks", fmt.Sprintf("%.0f/%.0f%s", finished, estimated, formatPercent(finished, estimated))),
						zap.String("engines", fmt.Sprintf("%.f/%.f%s", engineFinished, engineEstimated, formatPercent(engineFinished, engineEstimated))),
						chunkSpeed,
						zap.String("state", state),
						remaining,
					)

				case <-checkQuotaChan:
					// verify the total space occupied by sorted-kv-dir is below the quota,
					// otherwise we perform an emergency import.
					rc.enforceDiskQuota(ctx)

				case <-glueProgressTicker.C:
					finished := metric.ReadCounter(metric.ChunkCounter.WithLabelValues(metric.ChunkStateFinished))
					rc.tidbGlue.Record(glue.RecordFinishedChunk, uint64(finished))
				}
			}
		}, func(do bool) {
			log.L().Info("cancel periodic actions", zap.Bool("do", do))
			for _, f := range cancelFuncs {
				f(do)
			}
		}
}

var checksumManagerKey struct{}

func (rc *Controller) restoreTables(ctx context.Context) error {
	logTask := log.L().Begin(zap.InfoLevel, "restore all tables data")

	if err := rc.metaMgrBuilder.Init(ctx); err != nil {
		return err
	}

	// for local backend, we should disable some pd scheduler and change some settings, to
	// make split region and ingest sst more stable
	// because importer backend is mostly use for v3.x cluster which doesn't support these api,
	// so we also don't do this for import backend
	finishSchedulers := func() {}
	// if one lightning failed abnormally, and can't determine whether it needs to switch back,
	// we do not do switch back automatically
	cleanupFunc := func() {}
	switchBack := false
	if rc.cfg.TikvImporter.Backend == config.BackendLocal {
		// disable some pd schedulers
		pdController, err := pdutil.NewPdController(ctx, rc.cfg.TiDB.PdAddr,
			rc.tls.TLSConfig(), rc.tls.ToPDSecurityOption())
		if err != nil {
			return errors.Trace(err)
		}

		mgr := rc.metaMgrBuilder.TaskMetaMgr(pdController)
		if err = mgr.InitTask(ctx); err != nil {
			return err
		}

		logTask.Info("removing PD leader&region schedulers")

		restoreFn, err := mgr.CheckAndPausePdSchedulers(ctx)
		finishSchedulers = func() {
			if restoreFn != nil {
				// use context.Background to make sure this restore function can still be executed even if ctx is canceled
				restoreCtx := context.Background()
				needSwitchBack, err := mgr.CheckAndFinishRestore(restoreCtx)
				if err != nil {
					logTask.Warn("check restore pd schedulers failed", zap.Error(err))
					return
				}
				switchBack = needSwitchBack
				if needSwitchBack {
					if restoreE := restoreFn(restoreCtx); restoreE != nil {
						logTask.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
					}
					// clean up task metas
					if cleanupErr := mgr.Cleanup(restoreCtx); cleanupErr != nil {
						logTask.Warn("failed to clean task metas, you may need to restore them manually", zap.Error(cleanupErr))
					}
					// cleanup table meta and schema db if needed.
					cleanupFunc = func() {
						if e := mgr.CleanupAllMetas(restoreCtx); err != nil {
							logTask.Warn("failed to clean table task metas, you may need to restore them manually", zap.Error(e))
						}
					}
				}

				logTask.Info("add back PD leader&region schedulers")
			}

			pdController.Close()
		}

		if err != nil {
			return errors.Trace(err)
		}
	}
	defer func() {
		if switchBack {
			cleanupFunc()
		}
	}()

	type task struct {
		tr *TableRestore
		cp *checkpoints.TableCheckpoint
	}

	totalTables := 0
	for _, dbMeta := range rc.dbMetas {
		totalTables += len(dbMeta.Tables)
	}
	postProcessTaskChan := make(chan task, totalTables)

	var wg sync.WaitGroup
	var restoreErr common.OnceError

	stopPeriodicActions := make(chan struct{})

	periodicActions, cancelFunc := rc.buildRunPeriodicActionAndCancelFunc(ctx, stopPeriodicActions)
	go periodicActions()
	finishFuncCalled := false
	defer func() {
		if !finishFuncCalled {
			finishSchedulers()
			cancelFunc(switchBack)
			finishFuncCalled = true
		}
	}()

	defer close(stopPeriodicActions)

	taskCh := make(chan task, rc.cfg.App.IndexConcurrency)
	defer close(taskCh)

	manager, err := newChecksumManager(ctx, rc)
	if err != nil {
		return errors.Trace(err)
	}
	ctx2 := context.WithValue(ctx, &checksumManagerKey, manager)
	for i := 0; i < rc.cfg.App.IndexConcurrency; i++ {
		go func() {
			for task := range taskCh {
				tableLogTask := task.tr.logger.Begin(zap.InfoLevel, "restore table")
				web.BroadcastTableCheckpoint(task.tr.tableName, task.cp)
				needPostProcess, err := task.tr.restoreTable(ctx2, rc, task.cp)
				err = errors.Annotatef(err, "restore table %s failed", task.tr.tableName)
				tableLogTask.End(zap.ErrorLevel, err)
				web.BroadcastError(task.tr.tableName, err)
				metric.RecordTableCount("completed", err)
				restoreErr.Set(err)
				if needPostProcess {
					postProcessTaskChan <- task
				}
				wg.Done()
			}
		}()
	}

	// first collect all tables where the checkpoint is invalid
	allInvalidCheckpoints := make(map[string]checkpoints.CheckpointStatus)
	// collect all tables whose checkpoint's tableID can't match current tableID
	allDirtyCheckpoints := make(map[string]struct{})
	for _, dbMeta := range rc.dbMetas {
		dbInfo, ok := rc.dbInfos[dbMeta.Name]
		if !ok {
			return errors.Errorf("database %s not found in rc.dbInfos", dbMeta.Name)
		}
		for _, tableMeta := range dbMeta.Tables {
			tableInfo, ok := dbInfo.Tables[tableMeta.Name]
			if !ok {
				return errors.Errorf("table info %s.%s not found", dbMeta.Name, tableMeta.Name)
			}

			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			cp, err := rc.checkpointsDB.Get(ctx, tableName)
			if err != nil {
				return errors.Trace(err)
			}
			if cp.Status <= checkpoints.CheckpointStatusMaxInvalid {
				allInvalidCheckpoints[tableName] = cp.Status
			} else if cp.TableID > 0 && cp.TableID != tableInfo.ID {
				allDirtyCheckpoints[tableName] = struct{}{}
			}
		}
	}

	if len(allInvalidCheckpoints) != 0 {
		logger := log.L()
		logger.Error(
			"TiDB Lightning has failed last time. To prevent data loss, this run will stop now. Please resolve errors first",
			zap.Int("count", len(allInvalidCheckpoints)),
		)

		for tableName, status := range allInvalidCheckpoints {
			failedStep := status * 10
			var action strings.Builder
			action.WriteString("./tidb-lightning-ctl --checkpoint-error-")
			switch failedStep {
			case checkpoints.CheckpointStatusAlteredAutoInc, checkpoints.CheckpointStatusAnalyzed:
				action.WriteString("ignore")
			default:
				action.WriteString("destroy")
			}
			action.WriteString("='")
			action.WriteString(tableName)
			action.WriteString("' --config=...")

			logger.Info("-",
				zap.String("table", tableName),
				zap.Uint8("status", uint8(status)),
				zap.String("failedStep", failedStep.MetricName()),
				zap.Stringer("recommendedAction", &action),
			)
		}

		logger.Info("You may also run `./tidb-lightning-ctl --checkpoint-error-destroy=all --config=...` to start from scratch")
		logger.Info("For details of this failure, read the log file from the PREVIOUS run")

		return errors.New("TiDB Lightning has failed last time; please resolve these errors first")
	}
	if len(allDirtyCheckpoints) > 0 {
		logger := log.L()
		logger.Error(
			"TiDB Lightning has detected tables with illegal checkpoints. To prevent data mismatch, this run will stop now. Please remove these checkpoints first",
			zap.Int("count", len(allDirtyCheckpoints)),
		)

		for tableName := range allDirtyCheckpoints {
			logger.Info("-",
				zap.String("table", tableName),
				zap.String("recommendedAction", "./tidb-lightning-ctl --checkpoint-remove='"+tableName+"' --config=..."),
			)
		}

		logger.Info("You may also run `./tidb-lightning-ctl --checkpoint-remove=all --config=...` to start from scratch")

		return errors.New("TiDB Lightning has detected tables with illegal checkpoints; please remove these checkpoints first")
	}

	for _, dbMeta := range rc.dbMetas {
		dbInfo := rc.dbInfos[dbMeta.Name]
		for _, tableMeta := range dbMeta.Tables {
			tableInfo := dbInfo.Tables[tableMeta.Name]
			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			cp, err := rc.checkpointsDB.Get(ctx, tableName)
			if err != nil {
				return errors.Trace(err)
			}
			igCols, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(dbInfo.Name, tableInfo.Name, rc.cfg.Mydumper.CaseSensitive)
			if err != nil {
				return errors.Trace(err)
			}
			tr, err := NewTableRestore(tableName, tableMeta, dbInfo, tableInfo, cp, igCols.Columns)
			if err != nil {
				return errors.Trace(err)
			}

			wg.Add(1)
			select {
			case taskCh <- task{tr: tr, cp: cp}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	wg.Wait()
	// if context is done, should return directly
	select {
	case <-ctx.Done():
		err = restoreErr.Get()
		if err == nil {
			err = ctx.Err()
		}
		logTask.End(zap.ErrorLevel, err)
		return err
	default:
	}

	// stop periodic tasks for restore table such as pd schedulers and switch-mode tasks.
	// this can help make cluster switching back to normal state more quickly.
	// finishSchedulers()
	// cancelFunc(switchBack)
	// finishFuncCalled = true

	close(postProcessTaskChan)
	// otherwise, we should run all tasks in the post-process task chan
	for i := 0; i < rc.cfg.App.TableConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range postProcessTaskChan {
				metaMgr := rc.metaMgrBuilder.TableMetaMgr(task.tr)
				// force all the remain post-process tasks to be executed
				_, err = task.tr.postProcess(ctx2, rc, task.cp, true, metaMgr)
				restoreErr.Set(err)
			}
		}()
	}
	wg.Wait()

	err = restoreErr.Get()
	logTask.End(zap.ErrorLevel, err)
	return err
}

func (tr *TableRestore) restoreTable(
	ctx context.Context,
	rc *Controller,
	cp *checkpoints.TableCheckpoint,
) (bool, error) {
	// 1. Load the table info.

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	metaMgr := rc.metaMgrBuilder.TableMetaMgr(tr)
	// no need to do anything if the chunks are already populated
	if len(cp.Engines) > 0 {
		tr.logger.Info("reusing engines and files info from checkpoint",
			zap.Int("enginesCnt", len(cp.Engines)),
			zap.Int("filesCnt", cp.CountChunks()),
		)
	} else if cp.Status < checkpoints.CheckpointStatusAllWritten {
		versionStr, err := rc.tidbGlue.GetSQLExecutor().ObtainStringWithLog(
			ctx, "SELECT version()", "fetch tidb version", log.L())
		if err != nil {
			return false, errors.Trace(err)
		}

		tidbVersion, err := version.ExtractTiDBVersion(versionStr)
		if err != nil {
			return false, errors.Trace(err)
		}

		if err := tr.populateChunks(ctx, rc, cp); err != nil {
			return false, errors.Trace(err)
		}

		// fetch the max chunk row_id max value as the global max row_id
		rowIDMax := int64(0)
		for _, engine := range cp.Engines {
			if len(engine.Chunks) > 0 && engine.Chunks[len(engine.Chunks)-1].Chunk.RowIDMax > rowIDMax {
				rowIDMax = engine.Chunks[len(engine.Chunks)-1].Chunk.RowIDMax
			}
		}

		// "show table next_row_id" is only available after v4.0.0
		if tidbVersion.Major >= 4 && (rc.cfg.TikvImporter.Backend == config.BackendLocal || rc.cfg.TikvImporter.Backend == config.BackendImporter) {
			// first, insert a new-line into meta table
			if err = metaMgr.InitTableMeta(ctx); err != nil {
				return false, err
			}

			checksum, rowIDBase, err := metaMgr.AllocTableRowIDs(ctx, rowIDMax)
			if err != nil {
				return false, err
			}
			tr.RebaseChunkRowIDs(cp, rowIDBase)

			if checksum != nil {
				if cp.Checksum != *checksum {
					cp.Checksum = *checksum
					rc.saveCpCh <- saveCp{
						tableName: tr.tableName,
						merger: &checkpoints.TableChecksumMerger{
							Checksum: cp.Checksum,
						},
					}
				}
				tr.logger.Info("checksum before restore table", zap.Object("checksum", &cp.Checksum))
			}
		}
		if err := rc.checkpointsDB.InsertEngineCheckpoints(ctx, tr.tableName, cp.Engines); err != nil {
			return false, errors.Trace(err)
		}
		web.BroadcastTableCheckpoint(tr.tableName, cp)

		// rebase the allocator so it exceeds the number of rows.
		if tr.tableInfo.Core.PKIsHandle && tr.tableInfo.Core.ContainsAutoRandomBits() {
			cp.AllocBase = mathutil.MaxInt64(cp.AllocBase, tr.tableInfo.Core.AutoRandID)
			if err := tr.alloc.Get(autoid.AutoRandomType).Rebase(tr.tableInfo.ID, cp.AllocBase, false); err != nil {
				return false, err
			}
		} else {
			cp.AllocBase = mathutil.MaxInt64(cp.AllocBase, tr.tableInfo.Core.AutoIncID)
			if err := tr.alloc.Get(autoid.RowIDAllocType).Rebase(tr.tableInfo.ID, cp.AllocBase, false); err != nil {
				return false, err
			}
		}
		rc.saveCpCh <- saveCp{
			tableName: tr.tableName,
			merger: &checkpoints.RebaseCheckpointMerger{
				AllocBase: cp.AllocBase,
			},
		}
	}

	// 2. Restore engines (if still needed)
	err := tr.restoreEngines(ctx, rc, cp)
	if err != nil {
		return false, errors.Trace(err)
	}

	err = metaMgr.UpdateTableStatus(ctx, metaStatusRestoreFinished)
	if err != nil {
		return false, errors.Trace(err)
	}

	// 3. Post-process. With the last parameter set to false, we can allow delay analyze execute latter
	return tr.postProcess(ctx, rc, cp, false /* force-analyze */, metaMgr)
}

// estimate SST files compression threshold by total row file size
// with a higher compression threshold, the compression time increases, but the iteration time decreases.
// Try to limit the total SST files number under 500. But size compress 32GB SST files cost about 20min,
// we set the upper bound to 32GB to avoid too long compression time.
// factor is the non-clustered(1 for data engine and number of non-clustered index count for index engine).
func estimateCompactionThreshold(cp *checkpoints.TableCheckpoint, factor int64) int64 {
	totalRawFileSize := int64(0)
	var lastFile string
	for _, engineCp := range cp.Engines {
		for _, chunk := range engineCp.Chunks {
			if chunk.FileMeta.Path == lastFile {
				continue
			}
			size := chunk.FileMeta.FileSize
			if chunk.FileMeta.Type == mydump.SourceTypeParquet {
				// parquet file is compressed, thus estimates with a factor of 2
				size *= 2
			}
			totalRawFileSize += size
			lastFile = chunk.FileMeta.Path
		}
	}
	totalRawFileSize *= factor

	// try restrict the total file number within 512
	threshold := totalRawFileSize / 512
	threshold = utils.NextPowerOfTwo(threshold)
	if threshold < compactionLowerThreshold {
		// disable compaction if threshold is smaller than lower bound
		threshold = 0
	} else if threshold > compactionUpperThreshold {
		threshold = compactionUpperThreshold
	}

	return threshold
}

// do full compaction for the whole data.
func (rc *Controller) fullCompact(ctx context.Context) error {
	if !rc.cfg.PostRestore.Compact {
		log.L().Info("skip full compaction")
		return nil
	}

	// wait until any existing level-1 compact to complete first.
	task := log.L().Begin(zap.InfoLevel, "wait for completion of existing level 1 compaction")
	for !rc.compactState.CAS(compactStateIdle, compactStateDoing) {
		time.Sleep(100 * time.Millisecond)
	}
	task.End(zap.ErrorLevel, nil)

	return errors.Trace(rc.doCompact(ctx, FullLevelCompact))
}

func (rc *Controller) doCompact(ctx context.Context, level int32) error {
	tls := rc.tls.WithHost(rc.cfg.TiDB.PdAddr)
	return tikv.ForAllStores(
		ctx,
		tls,
		tikv.StoreStateDisconnected,
		func(c context.Context, store *tikv.Store) error {
			return tikv.Compact(c, tls, store.Address, level)
		},
	)
}

func (rc *Controller) switchToImportMode(ctx context.Context) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Import)
}

func (rc *Controller) switchToNormalMode(ctx context.Context) error {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Normal)
	return nil
}

func (rc *Controller) switchTiKVMode(ctx context.Context, mode sstpb.SwitchMode) {
	// It is fine if we miss some stores which did not switch to Import mode,
	// since we're running it periodically, so we exclude disconnected stores.
	// But it is essential all stores be switched back to Normal mode to allow
	// normal operation.
	var minState tikv.StoreState
	if mode == sstpb.SwitchMode_Import {
		minState = tikv.StoreStateOffline
	} else {
		minState = tikv.StoreStateDisconnected
	}
	tls := rc.tls.WithHost(rc.cfg.TiDB.PdAddr)
	// we ignore switch mode failure since it is not fatal.
	// no need log the error, it is done in kv.SwitchMode already.
	_ = tikv.ForAllStores(
		ctx,
		tls,
		minState,
		func(c context.Context, store *tikv.Store) error {
			return tikv.SwitchMode(c, tls, store.Address, mode)
		},
	)
}

func (rc *Controller) enforceDiskQuota(ctx context.Context) {
	if !rc.diskQuotaState.CAS(diskQuotaStateIdle, diskQuotaStateChecking) {
		// do not run multiple the disk quota check / import simultaneously.
		// (we execute the lock check in background to avoid blocking the cron thread)
		return
	}

	go func() {
		// locker is assigned when we detect the disk quota is exceeded.
		// before the disk quota is confirmed exceeded, we keep the diskQuotaLock
		// unlocked to avoid periodically interrupting the writer threads.
		var locker sync.Locker
		defer func() {
			rc.diskQuotaState.Store(diskQuotaStateIdle)
			if locker != nil {
				locker.Unlock()
			}
		}()

		isRetrying := false

		for {
			// sleep for a cycle if we are retrying because there is nothing new to import.
			if isRetrying {
				select {
				case <-ctx.Done():
					return
				case <-time.After(rc.cfg.Cron.CheckDiskQuota.Duration):
				}
			} else {
				isRetrying = true
			}

			quota := int64(rc.cfg.TikvImporter.DiskQuota)
			largeEngines, inProgressLargeEngines, totalDiskSize, totalMemSize := rc.backend.CheckDiskQuota(quota)
			metric.LocalStorageUsageBytesGauge.WithLabelValues("disk").Set(float64(totalDiskSize))
			metric.LocalStorageUsageBytesGauge.WithLabelValues("mem").Set(float64(totalMemSize))

			logger := log.With(
				zap.Int64("diskSize", totalDiskSize),
				zap.Int64("memSize", totalMemSize),
				zap.Int64("quota", quota),
				zap.Int("largeEnginesCount", len(largeEngines)),
				zap.Int("inProgressLargeEnginesCount", inProgressLargeEngines))

			if len(largeEngines) == 0 && inProgressLargeEngines == 0 {
				logger.Debug("disk quota respected")
				return
			}

			if locker == nil {
				// blocks all writers when we detected disk quota being exceeded.
				rc.diskQuotaLock.Lock()
				locker = rc.diskQuotaLock
			}

			logger.Warn("disk quota exceeded")
			if len(largeEngines) == 0 {
				logger.Warn("all large engines are already importing, keep blocking all writes")
				continue
			}

			// flush all engines so that checkpoints can be updated.
			if err := rc.backend.FlushAll(ctx); err != nil {
				logger.Error("flush engine for disk quota failed, check again later", log.ShortError(err))
				return
			}

			// at this point, all engines are synchronized on disk.
			// we then import every large engines one by one and complete.
			// if any engine failed to import, we just try again next time, since the data are still intact.
			rc.diskQuotaState.Store(diskQuotaStateImporting)
			task := logger.Begin(zap.WarnLevel, "importing large engines for disk quota")
			var importErr error
			for _, engine := range largeEngines {
				if err := rc.backend.UnsafeImportAndReset(ctx, engine); err != nil {
					importErr = multierr.Append(importErr, err)
				}
			}
			task.End(zap.ErrorLevel, importErr)
			return
		}
	}()
}

func (rc *Controller) setGlobalVariables(ctx context.Context) error {
	// set new collation flag base on tidb config
	enabled := ObtainNewCollationEnabled(ctx, rc.tidbGlue.GetSQLExecutor())
	// we should enable/disable new collation here since in server mode, tidb config
	// may be different in different tasks
	collate.SetNewCollationEnabledForTest(enabled)

	return nil
}

func (rc *Controller) waitCheckpointFinish() {
	// wait checkpoint process finish so that we can do cleanup safely
	close(rc.saveCpCh)
	rc.checkpointsWg.Wait()
}

func (rc *Controller) cleanCheckpoints(ctx context.Context) error {
	rc.waitCheckpointFinish()

	if !rc.cfg.Checkpoint.Enable {
		return nil
	}

	logger := log.With(
		zap.Bool("keepAfterSuccess", rc.cfg.Checkpoint.KeepAfterSuccess),
		zap.Int64("taskID", rc.cfg.TaskID),
	)

	task := logger.Begin(zap.InfoLevel, "clean checkpoints")
	var err error
	if rc.cfg.Checkpoint.KeepAfterSuccess {
		err = rc.checkpointsDB.MoveCheckpoints(ctx, rc.cfg.TaskID)
	} else {
		err = rc.checkpointsDB.RemoveCheckpoint(ctx, "all")
	}
	task.End(zap.ErrorLevel, err)
	return errors.Annotate(err, "clean checkpoints")
}

func (rc *Controller) isLocalBackend() bool {
	return rc.cfg.TikvImporter.Backend == config.BackendLocal
}

// preCheckRequirements checks
// 1. Cluster resource
// 2. Local node resource
// 3. Lightning configuration
// before restore tables start.
func (rc *Controller) preCheckRequirements(ctx context.Context) error {
	if !rc.cfg.App.CheckRequirements {
		log.L().Info("skip pre check due to user requirement")
		return nil
	}
	if err := rc.ClusterIsAvailable(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := rc.StoragePermission(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := rc.ClusterResource(ctx); err != nil {
		return errors.Trace(err)
	}

	if rc.isLocalBackend() {
		if err := rc.LocalResource(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// DataCheck checks the data schema which needs #rc.restoreSchema finished.
func (rc *Controller) DataCheck(ctx context.Context) error {
	if !rc.cfg.App.CheckRequirements {
		log.L().Info("skip data check due to user requirement")
		return nil
	}
	var err error
	err = rc.HasLargeCSV(rc.dbMetas)
	if err != nil {
		return errors.Trace(err)
	}
	checkPointCriticalMsgs := make([]string, 0, len(rc.dbMetas))
	schemaCriticalMsgs := make([]string, 0, len(rc.dbMetas))
	var msgs []string
	for _, dbInfo := range rc.dbMetas {
		for _, tableInfo := range dbInfo.Tables {
			// if hasCheckpoint is true, the table will start import from the checkpoint
			// so we can skip TableHasDataInCluster and SchemaIsValid check.
			noCheckpoint := true
			if rc.cfg.Checkpoint.Enable {
				if msgs, noCheckpoint, err = rc.CheckpointIsValid(ctx, tableInfo); err != nil {
					return errors.Trace(err)
				}
				if len(msgs) != 0 {
					checkPointCriticalMsgs = append(checkPointCriticalMsgs, msgs...)
				}
			}
			if noCheckpoint && rc.cfg.TikvImporter.Backend != config.BackendTiDB {
				if msgs, err = rc.SchemaIsValid(ctx, tableInfo); err != nil {
					return errors.Trace(err)
				}
				if len(msgs) != 0 {
					schemaCriticalMsgs = append(schemaCriticalMsgs, msgs...)
				}
			}
		}
	}
	if len(checkPointCriticalMsgs) != 0 {
		rc.checkTemplate.Collect(Critical, false, strings.Join(checkPointCriticalMsgs, "\n"))
	} else {
		rc.checkTemplate.Collect(Critical, true, "checkpoints are valid")
	}
	if len(schemaCriticalMsgs) != 0 {
		rc.checkTemplate.Collect(Critical, false, strings.Join(schemaCriticalMsgs, "\n"))
	} else {
		rc.checkTemplate.Collect(Critical, true, "table schemas are valid")
	}
	return nil
}

type chunkRestore struct {
	parser mydump.Parser
	index  int
	chunk  *checkpoints.ChunkCheckpoint
}

func newChunkRestore(
	ctx context.Context,
	index int,
	cfg *config.Config,
	chunk *checkpoints.ChunkCheckpoint,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
	tableInfo *checkpoints.TidbTableInfo,
) (*chunkRestore, error) {
	blockBufSize := int64(cfg.Mydumper.ReadBlockSize)

	var reader storage.ReadSeekCloser
	var err error
	if chunk.FileMeta.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, store, chunk.FileMeta.Path, chunk.FileMeta.FileSize)
	} else {
		reader, err = store.Open(ctx, chunk.FileMeta.Path)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	var parser mydump.Parser
	switch chunk.FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := cfg.Mydumper.CSV.Header && chunk.Chunk.Offset == 0
		parser = mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, blockBufSize, ioWorkers, hasHeader)
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(cfg.TiDB.SQLMode, reader, blockBufSize, ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, store, reader, chunk.FileMeta.Path)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", chunk.Key.Path, chunk.FileMeta.Type.String()))
	}

	if err = parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax); err != nil {
		return nil, errors.Trace(err)
	}
	if len(chunk.ColumnPermutation) > 0 {
		parser.SetColumns(getColumnNames(tableInfo.Core, chunk.ColumnPermutation))
	}

	return &chunkRestore{
		parser: parser,
		index:  index,
		chunk:  chunk,
	}, nil
}

func (cr *chunkRestore) close() {
	cr.parser.Close()
}

func getColumnNames(tableInfo *model.TableInfo, permutation []int) []string {
	colIndexes := make([]int, 0, len(permutation))
	for i := 0; i < len(permutation); i++ {
		colIndexes = append(colIndexes, -1)
	}
	colCnt := 0
	for i, p := range permutation {
		if p >= 0 {
			colIndexes[p] = i
			colCnt++
		}
	}

	names := make([]string, 0, colCnt)
	for _, idx := range colIndexes {
		// skip columns with index -1
		if idx >= 0 {
			// original fields contains _tidb_rowid field
			if idx == len(tableInfo.Columns) {
				names = append(names, model.ExtraHandleName.O)
			} else {
				names = append(names, tableInfo.Columns[idx].Name.O)
			}
		}
	}
	return names
}

var (
	maxKVQueueSize         = 32             // Cache at most this number of rows before blocking the encode loop
	minDeliverBytes uint64 = 96 * units.KiB // 96 KB (data + index). batch at least this amount of bytes to reduce number of messages
)

type deliveredKVs struct {
	kvs     kv.Row // if kvs is nil, this indicated we've got the last message.
	columns []string
	offset  int64
	rowID   int64
}

type deliverResult struct {
	totalDur time.Duration
	err      error
}

//nolint:nakedret // TODO: refactor
func (cr *chunkRestore) deliverLoop(
	ctx context.Context,
	kvsCh <-chan []deliveredKVs,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *backend.LocalEngineWriter,
	rc *Controller,
) (deliverTotalDur time.Duration, err error) {
	var channelClosed bool

	deliverLogger := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
		zap.String("task", "deliver"),
	)
	// Fetch enough KV pairs from the source.
	dataKVs := rc.backend.MakeEmptyRows()
	indexKVs := rc.backend.MakeEmptyRows()

	dataSynced := true
	for !channelClosed {
		var dataChecksum, indexChecksum verify.KVChecksum
		var columns []string
		var kvPacket []deliveredKVs
		// init these two field as checkpoint current value, so even if there are no kv pairs delivered,
		// chunk checkpoint should stay the same
		offset := cr.chunk.Chunk.Offset
		rowID := cr.chunk.Chunk.PrevRowIDMax

	populate:
		for dataChecksum.SumSize()+indexChecksum.SumSize() < minDeliverBytes {
			select {
			case kvPacket = <-kvsCh:
				if len(kvPacket) == 0 {
					channelClosed = true
					break populate
				}
				for _, p := range kvPacket {
					p.kvs.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
					columns = p.columns
					offset = p.offset
					rowID = p.rowID
				}
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}

		err = func() error {
			// We use `TryRLock` with sleep here to avoid blocking current goroutine during importing when disk-quota is
			// triggered, so that we can save chunkCheckpoint as soon as possible after `FlushEngine` is called.
			// This implementation may not be very elegant or even completely correct, but it is currently a relatively
			// simple and effective solution.
			for !rc.diskQuotaLock.TryRLock() {
				// try to update chunk checkpoint, this can help save checkpoint after importing when disk-quota is triggered
				if !dataSynced {
					dataSynced = cr.maybeSaveCheckpoint(rc, t, engineID, cr.chunk, dataEngine, indexEngine)
				}
				time.Sleep(time.Millisecond)
			}
			defer rc.diskQuotaLock.RUnlock()

			// Write KVs into the engine
			start := time.Now()

			if err = dataEngine.WriteRows(ctx, columns, dataKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					deliverLogger.Error("write to data engine failed", log.ShortError(err))
				}

				return errors.Trace(err)
			}
			if err = indexEngine.WriteRows(ctx, columns, indexKVs); err != nil {
				if !common.IsContextCanceledError(err) {
					deliverLogger.Error("write to index engine failed", log.ShortError(err))
				}
				return errors.Trace(err)
			}

			deliverDur := time.Since(start)
			deliverTotalDur += deliverDur
			metric.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
			metric.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumSize()))
			metric.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumSize()))
			metric.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumKVS()))
			metric.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumKVS()))
			return nil
		}()
		if err != nil {
			return
		}
		dataSynced = false

		dataKVs = dataKVs.Clear()
		indexKVs = indexKVs.Clear()

		// Update the table, and save a checkpoint.
		// (the write to the importer is effective immediately, thus update these here)
		// No need to apply a lock since this is the only thread updating `cr.chunk.**`.
		// In local mode, we should write these checkpoint after engine flushed.
		cr.chunk.Checksum.Add(&dataChecksum)
		cr.chunk.Checksum.Add(&indexChecksum)
		cr.chunk.Chunk.Offset = offset
		cr.chunk.Chunk.PrevRowIDMax = rowID

		if dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0 {
			// No need to save checkpoint if nothing was delivered.
			dataSynced = cr.maybeSaveCheckpoint(rc, t, engineID, cr.chunk, dataEngine, indexEngine)
		}
		failpoint.Inject("SlowDownWriteRows", func() {
			deliverLogger.Warn("Slowed down write rows")
		})
		failpoint.Inject("FailAfterWriteRows", nil)
		// TODO: for local backend, we may save checkpoint more frequently, e.g. after written
		// 10GB kv pairs to data engine, we can do a flush for both data & index engine, then we
		// can safely update current checkpoint.

		failpoint.Inject("LocalBackendSaveCheckpoint", func() {
			if !rc.isLocalBackend() && (dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0) {
				// No need to save checkpoint if nothing was delivered.
				saveCheckpoint(rc, t, engineID, cr.chunk)
			}
		})
	}

	return
}

func (cr *chunkRestore) maybeSaveCheckpoint(
	rc *Controller,
	t *TableRestore,
	engineID int32,
	chunk *checkpoints.ChunkCheckpoint,
	data, index *backend.LocalEngineWriter,
) bool {
	if data.IsSynced() && index.IsSynced() {
		saveCheckpoint(rc, t, engineID, chunk)
		return true
	}
	return false
}

func saveCheckpoint(rc *Controller, t *TableRestore, engineID int32, chunk *checkpoints.ChunkCheckpoint) {
	// We need to update the AllocBase every time we've finished a file.
	// The AllocBase is determined by the maximum of the "handle" (_tidb_rowid
	// or integer primary key), which can only be obtained by reading all data.

	var base int64
	if t.tableInfo.Core.PKIsHandle && t.tableInfo.Core.ContainsAutoRandomBits() {
		base = t.alloc.Get(autoid.AutoRandomType).Base() + 1
	} else {
		base = t.alloc.Get(autoid.RowIDAllocType).Base() + 1
	}
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &checkpoints.RebaseCheckpointMerger{
			AllocBase: base,
		},
	}
	rc.saveCpCh <- saveCp{
		tableName: t.tableName,
		merger: &checkpoints.ChunkCheckpointMerger{
			EngineID:          engineID,
			Key:               chunk.Key,
			Checksum:          chunk.Checksum,
			Pos:               chunk.Chunk.Offset,
			RowID:             chunk.Chunk.PrevRowIDMax,
			ColumnPermutation: chunk.ColumnPermutation,
		},
	}
}

//nolint:nakedret // TODO: refactor
func (cr *chunkRestore) encodeLoop(
	ctx context.Context,
	kvsCh chan<- []deliveredKVs,
	t *TableRestore,
	logger log.Logger,
	kvEncoder kv.Encoder,
	deliverCompleteCh <-chan deliverResult,
	rc *Controller,
) (readTotalDur time.Duration, encodeTotalDur time.Duration, err error) {
	send := func(kvs []deliveredKVs) error {
		select {
		case kvsCh <- kvs:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case deliverResult, ok := <-deliverCompleteCh:
			if deliverResult.err == nil && !ok {
				deliverResult.err = ctx.Err()
			}
			if deliverResult.err == nil {
				deliverResult.err = errors.New("unexpected premature fulfillment")
				logger.DPanic("unexpected: deliverCompleteCh prematurely fulfilled with no error", zap.Bool("chIsOpen", ok))
			}
			return errors.Trace(deliverResult.err)
		}
	}

	pauser, maxKvPairsCnt := rc.pauser, rc.cfg.TikvImporter.MaxKVPairs
	initializedColumns, reachEOF := false, false
	for !reachEOF {
		if err = pauser.Wait(ctx); err != nil {
			return
		}
		offset, _ := cr.parser.Pos()
		if offset >= cr.chunk.Chunk.EndOffset {
			break
		}

		var readDur, encodeDur time.Duration
		canDeliver := false
		kvPacket := make([]deliveredKVs, 0, maxKvPairsCnt)
		curOffset := offset
		var newOffset, rowID int64
		var kvSize uint64
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = cr.parser.ReadRow()
			columnNames := cr.parser.Columns()
			newOffset, rowID = cr.parser.Pos()

			switch errors.Cause(err) {
			case nil:
				if !initializedColumns {
					if len(cr.chunk.ColumnPermutation) == 0 {
						if err = t.initializeColumns(columnNames, cr.chunk); err != nil {
							return
						}
					}
					initializedColumns = true
				}
			case io.EOF:
				reachEOF = true
				break outLoop
			default:
				err = errors.Annotatef(err, "in file %s at offset %d", &cr.chunk.Key, newOffset)
				return
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			lastRow := cr.parser.LastRow()
			// sql -> kv
			kvs, encodeErr := kvEncoder.Encode(logger, lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation, curOffset)
			encodeDur += time.Since(encodeDurStart)
			cr.parser.RecycleRow(lastRow)
			if encodeErr != nil {
				err = errors.Annotatef(encodeErr, "in file %s at offset %d", &cr.chunk.Key, newOffset)
				return
			}
			kvPacket = append(kvPacket, deliveredKVs{kvs: kvs, columns: columnNames, offset: newOffset, rowID: rowID})
			kvSize += kvs.Size()
			failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
				kvSize += uint64(val.(int))
			})
			// pebble cannot allow > 4.0G kv in one batch.
			// we will meet pebble panic when import sql file and each kv has the size larger than 4G / maxKvPairsCnt.
			// so add this check.
			if kvSize >= minDeliverBytes || len(kvPacket) >= maxKvPairsCnt || newOffset == cr.chunk.Chunk.EndOffset {
				canDeliver = true
				kvSize = 0
			}
			curOffset = newOffset
		}
		encodeTotalDur += encodeDur
		metric.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
		readTotalDur += readDur
		metric.RowReadSecondsHistogram.Observe(readDur.Seconds())
		metric.RowReadBytesHistogram.Observe(float64(newOffset - offset))

		if len(kvPacket) != 0 {
			deliverKvStart := time.Now()
			if err = send(kvPacket); err != nil {
				return
			}
			metric.RowKVDeliverSecondsHistogram.Observe(time.Since(deliverKvStart).Seconds())
		}
	}

	err = send([]deliveredKVs{})
	return
}

func (cr *chunkRestore) restore(
	ctx context.Context,
	t *TableRestore,
	engineID int32,
	dataEngine, indexEngine *backend.LocalEngineWriter,
	rc *Controller,
) error {
	// Create the encoder.
	kvEncoder, err := rc.backend.NewEncoder(t.encTable, &kv.SessionOptions{
		SQLMode:   rc.cfg.TiDB.SQLMode,
		Timestamp: cr.chunk.Timestamp,
		SysVars:   rc.sysVars,
		// use chunk.PrevRowIDMax as the auto random seed, so it can stay the same value after recover from checkpoint.
		AutoRandomSeed: cr.chunk.Chunk.PrevRowIDMax,
	})
	if err != nil {
		return err
	}

	kvsCh := make(chan []deliveredKVs, maxKVQueueSize)
	deliverCompleteCh := make(chan deliverResult)

	defer func() {
		kvEncoder.Close()
		kvEncoder = nil
		close(kvsCh)
	}()

	go func() {
		defer close(deliverCompleteCh)
		dur, err := cr.deliverLoop(ctx, kvsCh, t, engineID, dataEngine, indexEngine, rc)
		select {
		case <-ctx.Done():
		case deliverCompleteCh <- deliverResult{dur, err}:
		}
	}()

	logTask := t.logger.With(
		zap.Int32("engineNumber", engineID),
		zap.Int("fileIndex", cr.index),
		zap.Stringer("path", &cr.chunk.Key),
	).Begin(zap.InfoLevel, "restore file")

	readTotalDur, encodeTotalDur, err := cr.encodeLoop(ctx, kvsCh, t, logTask.Logger, kvEncoder, deliverCompleteCh, rc)
	if err != nil {
		return err
	}

	select {
	case deliverResult, ok := <-deliverCompleteCh:
		if ok {
			logTask.End(zap.ErrorLevel, deliverResult.err,
				zap.Duration("readDur", readTotalDur),
				zap.Duration("encodeDur", encodeTotalDur),
				zap.Duration("deliverDur", deliverResult.totalDur),
				zap.Object("checksum", &cr.chunk.Checksum),
			)
			return errors.Trace(deliverResult.err)
		}
		// else, this must cause by ctx cancel
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
