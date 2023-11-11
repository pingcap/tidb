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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
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
	"github.com/pingcap/tidb/keyspace"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mathutil"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	"github.com/pingcap/tidb/util/set"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
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
	TaskMetaTableName  = "task_meta"
	TableMetaTableName = "table_meta"
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
		has_duplicates		BOOL NOT NULL DEFAULT 0,
		PRIMARY KEY (table_id, task_id)
	);`
	// CreateTaskMetaTable stores the pre-lightning metadata used by TiDB Lightning
	CreateTaskMetaTable = `CREATE TABLE IF NOT EXISTS %s (
		task_id BIGINT(20) UNSIGNED NOT NULL,
		pd_cfgs VARCHAR(2048) NOT NULL DEFAULT '',
		status  VARCHAR(32) NOT NULL,
		state   TINYINT(1) NOT NULL DEFAULT 0 COMMENT '0: normal, 1: exited before finish',
		source_bytes BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		cluster_avail BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		PRIMARY KEY (task_id)
	);`

	compactionLowerThreshold = 512 * units.MiB
	compactionUpperThreshold = 32 * units.GiB
)

var (
	minTiKVVersionForDuplicateResolution = *semver.New("5.2.0")
	maxTiKVVersionForDuplicateResolution = version.NextMajorVersion()
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
	waitCh    chan<- error
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
)

type Controller struct {
	taskCtx       context.Context
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
	ownStore          bool
	metaMgrBuilder    metaMgrBuilder
	errorMgr          *errormanager.ErrorManager
	taskMgr           taskMetaMgr

	diskQuotaLock  sync.RWMutex
	diskQuotaState atomic.Int32
	compactState   atomic.Int32
	status         *LightningStatus
	dupIndicator   *atomic.Bool

	preInfoGetter       PreRestoreInfoGetter
	precheckItemBuilder *PrecheckItemBuilder

	keyspaceName string
}

// LightningStatus provides the finished bytes and total bytes of the current task.
// It should keep the value after restart from checkpoint.
// When it is tidb backend, FinishedFileSize can be counted after chunk data is
// restored to tidb. When it is local backend it's counted after whole engine is
// imported.
// TotalFileSize may be an estimated value, so when the task is finished, it may
// not equal to FinishedFileSize.
type LightningStatus struct {
	backend          string
	FinishedFileSize atomic.Int64
	TotalFileSize    atomic.Int64
}

// ControllerParam contains many parameters for creating a Controller.
type ControllerParam struct {
	// databases that dumper created
	DBMetas []*mydump.MDDatabaseMeta
	// a pointer to status to report it to caller
	Status *LightningStatus
	// storage interface to read the dump data
	DumpFileStorage storage.ExternalStorage
	// true if DumpFileStorage is created by lightning. In some cases where lightning is a library, the framework may pass an DumpFileStorage
	OwnExtStorage bool
	// used by lightning server mode to pause tasks
	Pauser *common.Pauser
	// lightning via SQL will implement its glue, to let lightning use host TiDB's environment
	Glue glue.Glue
	// storage interface to write file checkpoints
	CheckpointStorage storage.ExternalStorage
	// when CheckpointStorage is not nil, save file checkpoint to it with this name
	CheckpointName string
	// DupIndicator can expose the duplicate detection result to the caller
	DupIndicator *atomic.Bool
	// Keyspace name
	KeyspaceName string
}

func NewRestoreController(
	ctx context.Context,
	cfg *config.Config,
	param *ControllerParam,
) (*Controller, error) {
	param.Pauser = DeliverPauser
	return NewRestoreControllerWithPauser(ctx, cfg, param)
}

func NewRestoreControllerWithPauser(
	ctx context.Context,
	cfg *config.Config,
	p *ControllerParam,
) (*Controller, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		return nil, err
	}

	var cpdb checkpoints.DB
	// if CheckpointStorage is set, we should use given ExternalStorage to create checkpoints.
	if p.CheckpointStorage != nil {
		cpdb, err = checkpoints.NewFileCheckpointsDBWithExstorageFileName(ctx, p.CheckpointStorage.URI(), p.CheckpointStorage, p.CheckpointName)
		if err != nil {
			return nil, common.ErrOpenCheckpoint.Wrap(err).GenWithStackByArgs()
		}
	} else {
		cpdb, err = p.Glue.OpenCheckpointsDB(ctx, cfg)
		if err != nil {
			if berrors.Is(err, common.ErrUnknownCheckpointDriver) {
				return nil, err
			}
			return nil, common.ErrOpenCheckpoint.Wrap(err).GenWithStackByArgs()
		}
	}

	taskCp, err := cpdb.TaskCheckpoint(ctx)
	if err != nil {
		return nil, common.ErrReadCheckpoint.Wrap(err).GenWithStack("get task checkpoint failed")
	}
	if err := verifyCheckpoint(cfg, taskCp); err != nil {
		return nil, errors.Trace(err)
	}
	// reuse task id to reuse task meta correctly.
	if taskCp != nil {
		cfg.TaskID = taskCp.TaskID
	}

	// TODO: support Lightning via SQL
	db, err := p.Glue.GetDB()
	if err != nil {
		return nil, errors.Trace(err)
	}
	errorMgr := errormanager.New(db, cfg, log.FromContext(ctx))
	if err := errorMgr.Init(ctx); err != nil {
		return nil, common.ErrInitErrManager.Wrap(err).GenWithStackByArgs()
	}

	var backend backend.Backend
	switch cfg.TikvImporter.Backend {
	case config.BackendTiDB:
		backend = tidb.NewTiDBBackend(ctx, db, cfg.TikvImporter.OnDuplicate, errorMgr)
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

		if cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone {
			if err := tikv.CheckTiKVVersion(ctx, tls, cfg.TiDB.PdAddr, minTiKVVersionForDuplicateResolution, maxTiKVVersionForDuplicateResolution); err != nil {
				if berrors.Is(err, berrors.ErrVersionMismatch) {
					log.FromContext(ctx).Warn("TiKV version doesn't support duplicate resolution. The resolution algorithm will fall back to 'none'", zap.Error(err))
					cfg.TikvImporter.DuplicateResolution = config.DupeResAlgNone
				} else {
					return nil, common.ErrCheckKVVersion.Wrap(err).GenWithStackByArgs()
				}
			}
		}

		backend, err = local.NewLocalBackend(ctx, tls, cfg, p.Glue, maxOpenFiles, errorMgr, p.KeyspaceName)
		if err != nil {
			return nil, common.NormalizeOrWrapErr(common.ErrUnknown, err)
		}
		err = verifyLocalFile(ctx, cpdb, cfg.TikvImporter.SortedKVDir)
		if err != nil {
			return nil, err
		}
	default:
		return nil, common.ErrUnknownBackend.GenWithStackByArgs(cfg.TikvImporter.Backend)
	}
	p.Status.backend = cfg.TikvImporter.Backend

	var metaBuilder metaMgrBuilder
	isSSTImport := cfg.TikvImporter.Backend == config.BackendLocal
	switch {
	case isSSTImport && cfg.TikvImporter.IncrementalImport:
		metaBuilder = &dbMetaMgrBuilder{
			db:           db,
			taskID:       cfg.TaskID,
			schema:       cfg.App.MetaSchemaName,
			needChecksum: cfg.PostRestore.Checksum != config.OpLevelOff,
		}
	case isSSTImport:
		metaBuilder = singleMgrBuilder{
			taskID: cfg.TaskID,
		}
	default:
		metaBuilder = noopMetaMgrBuilder{}
	}
	ioWorkers := worker.NewPool(ctx, cfg.App.IOConcurrency, "io")
	targetInfoGetter := &TargetInfoGetterImpl{
		cfg:          cfg,
		targetDBGlue: p.Glue,
		tls:          tls,
		backend:      backend,
	}
	preInfoGetter, err := NewPreRestoreInfoGetter(
		cfg,
		p.DBMetas,
		p.DumpFileStorage,
		targetInfoGetter,
		ioWorkers,
		backend,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	preCheckBuilder := NewPrecheckItemBuilder(
		cfg, p.DBMetas, preInfoGetter, cpdb,
	)

	rc := &Controller{
		taskCtx:       ctx,
		cfg:           cfg,
		dbMetas:       p.DBMetas,
		tableWorkers:  nil,
		indexWorkers:  nil,
		regionWorkers: worker.NewPool(ctx, cfg.App.RegionConcurrency, "region"),
		ioWorkers:     ioWorkers,
		checksumWorks: worker.NewPool(ctx, cfg.TiDB.ChecksumTableConcurrency, "checksum"),
		pauser:        p.Pauser,
		backend:       backend,
		tidbGlue:      p.Glue,
		sysVars:       defaultImportantVariables,
		tls:           tls,
		checkTemplate: NewSimpleTemplate(),

		errorSummaries:    makeErrorSummaries(log.FromContext(ctx)),
		checkpointsDB:     cpdb,
		saveCpCh:          make(chan saveCp),
		closedEngineLimit: worker.NewPool(ctx, cfg.App.TableConcurrency*2, "closed-engine"),

		store:          p.DumpFileStorage,
		ownStore:       p.OwnExtStorage,
		metaMgrBuilder: metaBuilder,
		errorMgr:       errorMgr,
		status:         p.Status,
		taskMgr:        nil,
		dupIndicator:   p.DupIndicator,

		preInfoGetter:       preInfoGetter,
		precheckItemBuilder: preCheckBuilder,

		keyspaceName: p.KeyspaceName,
	}

	return rc, nil
}

func (rc *Controller) Close() {
	rc.backend.Close()
	rc.tidbGlue.GetSQLExecutor().Close()
}

func (rc *Controller) Run(ctx context.Context) error {
	opts := []func(context.Context) error{
		rc.setGlobalVariables,
		rc.restoreSchema,
		rc.preCheckRequirements,
		rc.initCheckpoint,
		rc.restoreTables,
		rc.fullCompact,
		rc.cleanCheckpoints,
	}

	task := log.FromContext(ctx).Begin(zap.InfoLevel, "the whole procedure")

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
			break outside
		default:
			logger.Error("run failed")
			break outside // ps : not continue
		}
	}

	// if process is cancelled, should make sure checkpoints are written to db.
	if !finished {
		rc.waitCheckpointFinish()
	}

	task.End(zap.ErrorLevel, err)
	rc.errorMgr.LogErrorDetails()
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
	stmts    []string
}

type restoreSchemaWorker struct {
	ctx    context.Context
	quit   context.CancelFunc
	logger log.Logger
	jobCh  chan *schemaJob
	errCh  chan error
	wg     sync.WaitGroup
	glue   glue.Glue
	store  storage.ExternalStorage
}

func (worker *restoreSchemaWorker) addJob(sqlStr string, job *schemaJob) error {
	stmts, err := createIfNotExistsStmt(worker.glue.GetParser(), sqlStr, job.dbName, job.tblName)
	if err != nil {
		worker.logger.Warn("failed to rewrite statement, will use raw input instead",
			zap.String("db", job.dbName),
			zap.String("table", job.tblName),
			zap.String("statement", sqlStr),
			zap.Error(err))
		job.stmts = []string{sqlStr}
	} else {
		job.stmts = stmts
	}
	return worker.appendJob(job)
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
		sql := dbMeta.GetSchema(worker.ctx, worker.store)
		err = worker.addJob(sql, &schemaJob{
			dbName:   dbMeta.Name,
			tblName:  "",
			stmtType: schemaCreateDatabase,
		})
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
				return common.ErrSchemaNotExists.GenWithStackByArgs(dbMeta.Name, tblMeta.Name)
			}
			sql, err := tblMeta.GetSchema(worker.ctx, worker.store)
			if err != nil {
				return err
			}
			if sql != "" {
				err = worker.addJob(sql, &schemaJob{
					dbName:   dbMeta.Name,
					tblName:  tblMeta.Name,
					stmtType: schemaCreateTable,
				})
				if err != nil {
					return err
				}
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
				err = worker.addJob(sql, &schemaJob{
					dbName:   dbMeta.Name,
					tblName:  viewMeta.Name,
					stmtType: schemaCreateView,
				})
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
			logger := worker.logger.With(zap.String("db", job.dbName), zap.String("table", job.tblName))
			sqlWithRetry := common.SQLWithRetry{
				Logger: worker.logger,
				DB:     session,
			}
			for _, stmt := range job.stmts {
				task := logger.Begin(zap.DebugLevel, fmt.Sprintf("execute SQL: %s", stmt))
				err = sqlWithRetry.Exec(worker.ctx, "run create schema job", stmt)
				if err != nil {
					// try to imitate IF NOT EXISTS behavior for parsing errors
					exists := false
					switch job.stmtType {
					case schemaCreateDatabase:
						var err2 error
						exists, err2 = common.SchemaExists(worker.ctx, session, job.dbName)
						if err2 != nil {
							task.Error("failed to check database existence", zap.Error(err2))
						}
					case schemaCreateTable:
						exists, _ = common.TableExists(worker.ctx, session, job.dbName, job.tblName)
					}
					if exists {
						err = nil
					}
				}
				task.End(zap.ErrorLevel, err)

				if err != nil {
					err = common.ErrCreateSchema.Wrap(err).GenWithStackByArgs(common.UniqueTable(job.dbName, job.tblName), job.stmtType.String())
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
		return errors.Trace(worker.ctx.Err())
	case worker.jobCh <- job:
		return nil
	}
}

func (rc *Controller) restoreSchema(ctx context.Context) error {
	// create table with schema file
	// we can handle the duplicated created with createIfNotExist statement
	// and we will check the schema in TiDB is valid with the datafile in DataCheck later.
	logTask := log.FromContext(ctx).Begin(zap.InfoLevel, "restore all schema")
	concurrency := mathutil.Min(rc.cfg.App.RegionConcurrency, 8)
	childCtx, cancel := context.WithCancel(ctx)
	worker := restoreSchemaWorker{
		ctx:    childCtx,
		quit:   cancel,
		logger: log.FromContext(ctx),
		jobCh:  make(chan *schemaJob, concurrency),
		errCh:  make(chan error),
		glue:   rc.tidbGlue,
		store:  rc.store,
	}
	for i := 0; i < concurrency; i++ {
		go worker.doJob()
	}
	err := worker.makeJobs(rc.dbMetas, rc.preInfoGetter.FetchRemoteTableModels)
	logTask.End(zap.ErrorLevel, err)
	if err != nil {
		return err
	}

	dbInfos, err := rc.preInfoGetter.GetAllTableStructures(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// For local backend, we need DBInfo.ID to operate the global autoid allocator.
	if isLocalBackend(rc.cfg) {
		dbs, err := tikv.FetchRemoteDBModelsFromTLS(ctx, rc.tls)
		if err != nil {
			return errors.Trace(err)
		}
		dbIDs := make(map[string]int64)
		for _, db := range dbs {
			dbIDs[db.Name.L] = db.ID
		}
		for _, dbInfo := range dbInfos {
			dbInfo.ID = dbIDs[strings.ToLower(dbInfo.Name)]
		}
	}
	rc.dbInfos = dbInfos
	rc.sysVars = rc.preInfoGetter.GetTargetSysVariablesForImport(ctx)

	return nil
}

// initCheckpoint initializes all tables' checkpoint data
func (rc *Controller) initCheckpoint(ctx context.Context) error {
	// Load new checkpoints
	err := rc.checkpointsDB.Initialize(ctx, rc.cfg, rc.dbInfos)
	if err != nil {
		return common.ErrInitCheckpoint.Wrap(err).GenWithStackByArgs()
	}
	failpoint.Inject("InitializeCheckpointExit", func() {
		log.FromContext(ctx).Warn("exit triggered", zap.String("failpoint", "InitializeCheckpointExit"))
		os.Exit(0)
	})

	rc.checkpointsWg.Add(1) // checkpointsWg will be done in `rc.listenCheckpointUpdates`
	go rc.listenCheckpointUpdates(log.FromContext(ctx))

	// Estimate the number of chunks for progress reporting
	return rc.estimateChunkCountIntoMetrics(ctx)
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
		return common.ErrInvalidCheckpoint.GenWithStack("config 'tikv-importer.backend' value '%s' different from checkpoint value '%s', please %s", cfg.TikvImporter.Backend, taskCp.Backend, retryUsage)
	}

	if cfg.App.CheckRequirements {
		if build.ReleaseVersion != taskCp.LightningVer {
			var displayVer string
			if len(taskCp.LightningVer) != 0 {
				displayVer = fmt.Sprintf("at '%s'", taskCp.LightningVer)
			} else {
				displayVer = "before v4.0.6/v3.0.19"
			}
			return common.ErrInvalidCheckpoint.GenWithStack("lightning version is '%s', but checkpoint was created %s, please %s", build.ReleaseVersion, displayVer, retryUsage)
		}

		errorFmt := "config '%s' value '%s' different from checkpoint value '%s'. You may set 'check-requirements = false' to skip this check or " + retryUsage
		if cfg.Mydumper.SourceDir != taskCp.SourceDir {
			return common.ErrInvalidCheckpoint.GenWithStack(errorFmt, "mydumper.data-source-dir", cfg.Mydumper.SourceDir, taskCp.SourceDir)
		}

		if cfg.TikvImporter.Backend == config.BackendLocal && cfg.TikvImporter.SortedKVDir != taskCp.SortedKVDir {
			return common.ErrInvalidCheckpoint.GenWithStack(errorFmt, "mydumper.sorted-kv-dir", cfg.TikvImporter.SortedKVDir, taskCp.SortedKVDir)
		}

		if cfg.TiDB.Host != taskCp.TiDBHost {
			return common.ErrInvalidCheckpoint.GenWithStack(errorFmt, "tidb.host", cfg.TiDB.Host, taskCp.TiDBHost)
		}

		if cfg.TiDB.Port != taskCp.TiDBPort {
			return common.ErrInvalidCheckpoint.GenWithStack(errorFmt, "tidb.port", cfg.TiDB.Port, taskCp.TiDBPort)
		}

		if cfg.TiDB.PdAddr != taskCp.PdAddr {
			return common.ErrInvalidCheckpoint.GenWithStack(errorFmt, "tidb.pd-addr", cfg.TiDB.PdAddr, taskCp.PdAddr)
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
			file := local.Engine{UUID: eID}
			err := file.Exist(dir)
			if err != nil {
				log.FromContext(ctx).Error("can't find local file",
					zap.String("table name", tableName),
					zap.Int32("engine ID", engineID))
				if os.IsNotExist(err) {
					err = common.ErrCheckLocalFile.GenWithStackByArgs(tableName, dir)
				} else {
					err = common.ErrCheckLocalFile.Wrap(err).GenWithStackByArgs(tableName, dir)
				}
				return err
			}
		}
	}
	return nil
}

func (rc *Controller) estimateChunkCountIntoMetrics(ctx context.Context) error {
	estimatedChunkCount := 0.0
	estimatedEngineCnt := int64(0)
	batchSize := rc.cfg.Mydumper.BatchSize
	if batchSize <= 0 {
		// if rows in source files are not sorted by primary key(if primary is number or cluster index enabled),
		// the key range in each data engine may have overlap, thus a bigger engine size can somewhat alleviate it.
		batchSize = config.DefaultBatchSize
	}
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
					remainChunkCnt := float64(c.UnfinishedSize()) / float64(c.TotalSize())
					fileChunks[c.Key.Path] += remainChunkCnt
				}
			}
			// estimate engines count if engine cp is empty
			if len(dbCp.Engines) == 0 {
				estimatedEngineCnt += ((tableMeta.TotalSize + int64(batchSize) - 1) / int64(batchSize)) + 1
			}
			for _, fileMeta := range tableMeta.DataFiles {
				if cnt, ok := fileChunks[fileMeta.FileMeta.Path]; ok {
					estimatedChunkCount += cnt
					continue
				}
				if fileMeta.FileMeta.Type == mydump.SourceTypeCSV {
					cfg := rc.cfg.Mydumper
					if fileMeta.FileMeta.FileSize > int64(cfg.MaxRegionSize) && cfg.StrictFormat &&
						!cfg.CSV.Header && fileMeta.FileMeta.Compression == mydump.CompressionNone {
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
	if m, ok := metric.FromContext(ctx); ok {
		m.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated).Add(estimatedChunkCount)
		m.ProcessedEngineCounter.WithLabelValues(metric.ChunkStateEstimated, metric.TableResultSuccess).
			Add(float64(estimatedEngineCnt))
	}
	rc.tidbGlue.Record(glue.RecordEstimatedChunk, uint64(estimatedChunkCount))
	return nil
}

func firstErr(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func (rc *Controller) saveStatusCheckpoint(ctx context.Context, tableName string, engineID int32, err error, statusIfSucceed checkpoints.CheckpointStatus) error {
	merger := &checkpoints.StatusCheckpointMerger{Status: statusIfSucceed, EngineID: engineID}

	logger := log.FromContext(ctx).With(zap.String("table", tableName), zap.Int32("engine_id", engineID),
		zap.String("new_status", statusIfSucceed.MetricName()), zap.Error(err))
	logger.Debug("update checkpoint")

	switch {
	case err == nil:
	case utils.MessageIsRetryableStorageError(err.Error()), common.IsContextCanceledError(err):
		// recoverable error, should not be recorded in checkpoint
		// which will prevent lightning from automatically recovering
		return nil
	default:
		// unrecoverable error
		merger.SetInvalid()
		rc.errorSummaries.record(tableName, err, statusIfSucceed)
	}

	if m, ok := metric.FromContext(ctx); ok {
		if engineID == checkpoints.WholeTableEngineID {
			m.RecordTableCount(statusIfSucceed.MetricName(), err)
		} else {
			m.RecordEngineCount(statusIfSucceed.MetricName(), err)
		}
	}

	waitCh := make(chan error, 1)
	rc.saveCpCh <- saveCp{tableName: tableName, merger: merger, waitCh: waitCh}

	select {
	case saveCpErr := <-waitCh:
		if saveCpErr != nil {
			logger.Error("failed to save status checkpoint", log.ShortError(saveCpErr))
		}
		return saveCpErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// listenCheckpointUpdates will combine several checkpoints together to reduce database load.
func (rc *Controller) listenCheckpointUpdates(logger log.Logger) {
	var lock sync.Mutex
	coalesed := make(map[string]*checkpoints.TableCheckpointDiff)
	var waiters []chan<- error

	hasCheckpoint := make(chan struct{}, 1)
	defer close(hasCheckpoint)

	go func() {
		for range hasCheckpoint {
			lock.Lock()
			cpd := coalesed
			coalesed = make(map[string]*checkpoints.TableCheckpointDiff)
			ws := waiters
			waiters = nil
			lock.Unlock()

			//nolint:scopelint // This would be either INLINED or ERASED, at compile time.
			failpoint.Inject("SlowDownCheckpointUpdate", func() {})

			if len(cpd) > 0 {
				err := rc.checkpointsDB.Update(rc.taskCtx, cpd)
				for _, w := range ws {
					w <- common.NormalizeOrWrapErr(common.ErrUpdateCheckpoint, err)
				}
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
		if scp.waitCh != nil {
			waiters = append(waiters, scp.waitCh)
		}

		if len(hasCheckpoint) == 0 {
			rc.checkpointsWg.Add(1)
			hasCheckpoint <- struct{}{}
		}

		lock.Unlock()

		//nolint:scopelint // This would be either INLINED or ERASED, at compile time.
		failpoint.Inject("FailIfImportedChunk", func() {
			if merger, ok := scp.merger.(*checkpoints.ChunkCheckpointMerger); ok && merger.Pos >= merger.EndOffset {
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
		failpoint.Inject("KillIfImportedChunk", func() {
			if merger, ok := scp.merger.(*checkpoints.ChunkCheckpointMerger); ok && merger.Pos >= merger.EndOffset {
				rc.checkpointsWg.Done()
				rc.checkpointsWg.Wait()
				if err := common.KillMySelf(); err != nil {
					logger.Warn("KillMySelf() failed to kill itself", log.ShortError(err))
				}
				for scp := range rc.saveCpCh {
					if scp.waitCh != nil {
						scp.waitCh <- context.Canceled
					}
				}
				failpoint.Return()
			}
		})
	}
	// Don't put this statement in defer function at the beginning. failpoint function may call it manually.
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
				rc.switchToNormalMode(ctx)
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
			if rc.cfg.Cron.SwitchMode.Duration > 0 {
				rc.switchToImportMode(ctx)
			}
			start := time.Now()
			for {
				select {
				case <-ctx.Done():
					log.FromContext(ctx).Warn("stopping periodic actions", log.ShortError(ctx.Err()))
					return
				case <-stop:
					log.FromContext(ctx).Info("everything imported, stopping periodic actions")
					return

				case <-switchModeChan:
					// periodically switch to import mode, as requested by TiKV 3.0
					rc.switchToImportMode(ctx)

				case <-logProgressChan:
					metrics, ok := metric.FromContext(ctx)
					if !ok {
						log.FromContext(ctx).Warn("couldn't find metrics from context, skip log progress")
						continue
					}
					// log the current progress periodically, so OPS will know that we're still working
					nanoseconds := float64(time.Since(start).Nanoseconds())
					totalRestoreBytes := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.BytesStateTotalRestore))
					restoredBytes := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.BytesStateRestored))
					// the estimated chunk is not accurate(likely under estimated), but the actual count is not accurate
					// before the last table start, so use the bigger of the two should be a workaround
					estimated := metric.ReadCounter(metrics.ChunkCounter.WithLabelValues(metric.ChunkStateEstimated))
					pending := metric.ReadCounter(metrics.ChunkCounter.WithLabelValues(metric.ChunkStatePending))
					if estimated < pending {
						estimated = pending
					}
					finished := metric.ReadCounter(metrics.ChunkCounter.WithLabelValues(metric.ChunkStateFinished))
					totalTables := metric.ReadCounter(metrics.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))
					completedTables := metric.ReadCounter(metrics.TableCounter.WithLabelValues(metric.TableStateCompleted, metric.TableResultSuccess))
					bytesRead := metric.ReadHistogramSum(metrics.RowReadBytesHistogram)
					engineEstimated := metric.ReadCounter(metrics.ProcessedEngineCounter.WithLabelValues(metric.ChunkStateEstimated, metric.TableResultSuccess))
					enginePending := metric.ReadCounter(metrics.ProcessedEngineCounter.WithLabelValues(metric.ChunkStatePending, metric.TableResultSuccess))
					if engineEstimated < enginePending {
						engineEstimated = enginePending
					}
					engineFinished := metric.ReadCounter(metrics.ProcessedEngineCounter.WithLabelValues(metric.TableStateImported, metric.TableResultSuccess))
					bytesWritten := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.BytesStateRestoreWritten))
					bytesImported := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.BytesStateImported))

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

					// lightning restore is separated into restore engine and import engine, they are both parallelized
					// and pipelined between engines, so we can only weight the progress of those 2 phase to get the
					// total progress.
					//
					// for local & importer backend:
					// in most case import engine is faster since there's little computations, but inside one engine
					// restore and import is serialized, the progress of those two will not differ too much, and
					// import engine determines the end time of the whole restore, so we average them for now.
					// the result progress may fall behind the real progress if import is faster.
					//
					// for tidb backend, we do nothing during import engine, so we use restore engine progress as the
					// total progress.
					restoreBytesField := zap.Skip()
					importBytesField := zap.Skip()
					remaining = zap.Skip()
					totalPercent := 0.0
					if restoredBytes > 0 {
						restorePercent := math.Min(restoredBytes/totalRestoreBytes, 1.0)
						metrics.ProgressGauge.WithLabelValues(metric.ProgressPhaseRestore).Set(restorePercent)
						if rc.cfg.TikvImporter.Backend != config.BackendTiDB {
							var importPercent float64
							if bytesWritten > 0 {
								// estimate total import bytes from written bytes
								// when importPercent = 1, totalImportBytes = bytesWritten, but there's case
								// bytesImported may bigger or smaller than bytesWritten such as when deduplicate
								// we calculate progress using engines then use the bigger one in case bytesImported is
								// smaller.
								totalImportBytes := bytesWritten / restorePercent
								biggerPercent := math.Max(bytesImported/totalImportBytes, engineFinished/engineEstimated)
								importPercent = math.Min(biggerPercent, 1.0)
								importBytesField = zap.String("import-bytes", fmt.Sprintf("%s/%s(estimated)",
									units.BytesSize(bytesImported), units.BytesSize(totalImportBytes)))
							}
							metrics.ProgressGauge.WithLabelValues(metric.ProgressPhaseImport).Set(importPercent)
							totalPercent = (restorePercent + importPercent) / 2
						} else {
							totalPercent = restorePercent
						}
						if totalPercent < 1.0 {
							remainNanoseconds := (1.0 - totalPercent) / totalPercent * nanoseconds
							remaining = zap.Duration("remaining", time.Duration(remainNanoseconds).Round(time.Second))
						}
						restoreBytesField = zap.String("restore-bytes", fmt.Sprintf("%s/%s",
							units.BytesSize(restoredBytes), units.BytesSize(totalRestoreBytes)))
					}
					metrics.ProgressGauge.WithLabelValues(metric.ProgressPhaseTotal).Set(totalPercent)

					formatPercent := func(num, denom float64) string {
						if denom > 0 {
							return fmt.Sprintf(" (%.1f%%)", num/denom*100)
						}
						return ""
					}

					// avoid output bytes speed if there are no unfinished chunks
					encodeSpeedField := zap.Skip()
					if bytesRead > 0 {
						encodeSpeedField = zap.Float64("encode speed(MiB/s)", bytesRead/(1048576e-9*nanoseconds))
					}

					// Note: a speed of 28 MiB/s roughly corresponds to 100 GiB/hour.
					log.FromContext(ctx).Info("progress",
						zap.String("total", fmt.Sprintf("%.1f%%", totalPercent*100)),
						// zap.String("files", fmt.Sprintf("%.0f/%.0f (%.1f%%)", finished, estimated, finished/estimated*100)),
						zap.String("tables", fmt.Sprintf("%.0f/%.0f%s", completedTables, totalTables, formatPercent(completedTables, totalTables))),
						zap.String("chunks", fmt.Sprintf("%.0f/%.0f%s", finished, estimated, formatPercent(finished, estimated))),
						zap.String("engines", fmt.Sprintf("%.f/%.f%s", engineFinished, engineEstimated, formatPercent(engineFinished, engineEstimated))),
						restoreBytesField, importBytesField,
						encodeSpeedField,
						zap.String("state", state),
						remaining,
					)

				case <-checkQuotaChan:
					// verify the total space occupied by sorted-kv-dir is below the quota,
					// otherwise we perform an emergency import.
					rc.enforceDiskQuota(ctx)

				case <-glueProgressTicker.C:
					if m, ok := metric.FromContext(ctx); ok {
						finished := metric.ReadCounter(m.ChunkCounter.WithLabelValues(metric.ChunkStateFinished))
						rc.tidbGlue.Record(glue.RecordFinishedChunk, uint64(finished))
					}
				}
			}
		}, func(do bool) {
			log.FromContext(ctx).Info("cancel periodic actions", zap.Bool("do", do))
			for _, f := range cancelFuncs {
				f(do)
			}
		}
}

type checksumManagerKeyType struct{}

var checksumManagerKey checksumManagerKeyType

const (
	pauseGCTTLForDupeRes      = time.Hour
	pauseGCIntervalForDupeRes = time.Minute
)

func (rc *Controller) keepPauseGCForDupeRes(ctx context.Context) (<-chan struct{}, error) {
	tlsOpt := rc.tls.ToPDSecurityOption()
	pdCli, err := pd.NewClientWithContext(ctx, []string{rc.cfg.TiDB.PdAddr}, tlsOpt)
	if err != nil {
		return nil, errors.Trace(err)
	}

	serviceID := "lightning-duplicate-resolution-" + uuid.New().String()
	ttl := int64(pauseGCTTLForDupeRes / time.Second)

	var (
		safePoint uint64
		paused    bool
	)
	// Try to get the minimum safe point across all services as our GC safe point.
	for i := 0; i < 10; i++ {
		if i > 0 {
			time.Sleep(time.Second * 3)
		}
		minSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, serviceID, ttl, 1)
		if err != nil {
			pdCli.Close()
			return nil, errors.Trace(err)
		}
		newMinSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, serviceID, ttl, minSafePoint)
		if err != nil {
			pdCli.Close()
			return nil, errors.Trace(err)
		}
		if newMinSafePoint <= minSafePoint {
			safePoint = minSafePoint
			paused = true
			break
		}
		log.FromContext(ctx).Warn(
			"Failed to register GC safe point because the current minimum safe point is newer"+
				" than what we assume, will retry newMinSafePoint next time",
			zap.Uint64("minSafePoint", minSafePoint),
			zap.Uint64("newMinSafePoint", newMinSafePoint),
		)
	}
	if !paused {
		pdCli.Close()
		return nil, common.ErrPauseGC.GenWithStack("failed to pause GC for duplicate resolution after all retries")
	}

	exitCh := make(chan struct{})
	go func(safePoint uint64) {
		defer pdCli.Close()
		defer close(exitCh)
		ticker := time.NewTicker(pauseGCIntervalForDupeRes)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				minSafePoint, err := pdCli.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
				if err != nil {
					log.FromContext(ctx).Warn("Failed to register GC safe point", zap.Error(err))
					continue
				}
				if minSafePoint > safePoint {
					log.FromContext(ctx).Warn("The current minimum safe point is newer than what we hold, duplicate records are at"+
						"risk of being GC and not detectable",
						zap.Uint64("safePoint", safePoint),
						zap.Uint64("minSafePoint", minSafePoint),
					)
					safePoint = minSafePoint
				}
			case <-ctx.Done():
				stopCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
				if _, err := pdCli.UpdateServiceGCSafePoint(stopCtx, serviceID, 0, safePoint); err != nil {
					log.FromContext(ctx).Warn("Failed to reset safe point ttl to zero", zap.Error(err))
				}
				// just make compiler happy
				cancelFunc()
				return
			}
		}
	}(safePoint)
	return exitCh, nil
}

func (rc *Controller) restoreTables(ctx context.Context) (finalErr error) {
	// output error summary
	defer rc.outpuErrorSummary()

	if rc.cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone {
		subCtx, cancel := context.WithCancel(ctx)
		exitCh, err := rc.keepPauseGCForDupeRes(subCtx)
		if err != nil {
			cancel()
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-exitCh
		}()
	}

	logTask := log.FromContext(ctx).Begin(zap.InfoLevel, "restore all tables data")
	if rc.tableWorkers == nil {
		rc.tableWorkers = worker.NewPool(ctx, rc.cfg.App.TableConcurrency, "table")
	}
	if rc.indexWorkers == nil {
		rc.indexWorkers = worker.NewPool(ctx, rc.cfg.App.IndexConcurrency, "index")
	}

	// for local backend, we should disable some pd scheduler and change some settings, to
	// make split region and ingest sst more stable
	// because importer backend is mostly use for v3.x cluster which doesn't support these api,
	// so we also don't do this for import backend
	finishSchedulers := func() {
		if rc.taskMgr != nil {
			rc.taskMgr.Close()
		}
	}
	// if one lightning failed abnormally, and can't determine whether it needs to switch back,
	// we do not do switch back automatically
	switchBack := false
	cleanup := false
	postProgress := func() error { return nil }
	var kvStore tidbkv.Storage

	if isLocalBackend(rc.cfg) {
		var (
			restoreFn pdutil.UndoFunc
			err       error
		)

		if !rc.taskMgr.CanPauseSchedulerByKeyRange() {
			logTask.Info("removing PD leader&region schedulers")

			restoreFn, err = rc.taskMgr.CheckAndPausePdSchedulers(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}

		finishSchedulers = func() {
			taskFinished := finalErr == nil
			// use context.Background to make sure this restore function can still be executed even if ctx is canceled
			restoreCtx := context.Background()
			needSwitchBack, needCleanup, err := rc.taskMgr.CheckAndFinishRestore(restoreCtx, taskFinished)
			if err != nil {
				logTask.Warn("check restore pd schedulers failed", zap.Error(err))
				return
			}
			switchBack = needSwitchBack
			cleanup = needCleanup

			if needSwitchBack && restoreFn != nil {
				logTask.Info("add back PD leader&region schedulers")
				if restoreE := restoreFn(restoreCtx); restoreE != nil {
					logTask.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
				}
			}

			if rc.taskMgr != nil {
				rc.taskMgr.Close()
			}
		}

		// Disable GC because TiDB enables GC already.
		kvStore, err = driver.TiKVDriver{}.OpenWithOptions(
			fmt.Sprintf("tikv://%s?disableGC=true&keyspaceName=%s", rc.cfg.TiDB.PdAddr, rc.keyspaceName),
			driver.WithSecurity(rc.tls.ToTiKVSecurityConfig()),
		)
		if err != nil {
			return errors.Trace(err)
		}
		manager, err := newChecksumManager(ctx, rc, kvStore)
		if err != nil {
			return errors.Trace(err)
		}
		ctx = context.WithValue(ctx, &checksumManagerKey, manager)
	}

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

	defer close(stopPeriodicActions)

	defer func() {
		finishSchedulers()
		cancelFunc(switchBack)

		if err := postProgress(); err != nil {
			logTask.End(zap.ErrorLevel, err)
			finalErr = err
			return
		}
		logTask.End(zap.ErrorLevel, nil)
		// clean up task metas
		if cleanup {
			logTask.Info("cleanup task metas")
			if cleanupErr := rc.taskMgr.Cleanup(context.Background()); cleanupErr != nil {
				logTask.Warn("failed to clean task metas, you may need to restore them manually", zap.Error(cleanupErr))
			}
			// cleanup table meta and schema db if needed.
			if err := rc.taskMgr.CleanupAllMetas(context.Background()); err != nil {
				logTask.Warn("failed to clean table task metas, you may need to restore them manually", zap.Error(err))
			}
		}
		if kvStore != nil {
			if err := kvStore.Close(); err != nil {
				logTask.Warn("failed to close kv store", zap.Error(err))
			}
		}
	}()

	taskCh := make(chan task, rc.cfg.App.IndexConcurrency)
	defer close(taskCh)

	for i := 0; i < rc.cfg.App.IndexConcurrency; i++ {
		go func() {
			for task := range taskCh {
				tableLogTask := task.tr.logger.Begin(zap.InfoLevel, "restore table")
				web.BroadcastTableCheckpoint(task.tr.tableName, task.cp)

				needPostProcess, err := task.tr.restoreTable(ctx, rc, task.cp)

				err = common.NormalizeOrWrapErr(common.ErrRestoreTable, err, task.tr.tableName)
				tableLogTask.End(zap.ErrorLevel, err)
				web.BroadcastError(task.tr.tableName, err)
				if m, ok := metric.FromContext(ctx); ok {
					m.RecordTableCount(metric.TableStateCompleted, err)
				}
				restoreErr.Set(err)
				if needPostProcess {
					postProcessTaskChan <- task
				}
				wg.Done()
			}
		}()
	}

	var allTasks []task
	var totalDataSizeToRestore int64
	for _, dbMeta := range rc.dbMetas {
		dbInfo := rc.dbInfos[dbMeta.Name]
		for _, tableMeta := range dbMeta.Tables {
			tableInfo := dbInfo.Tables[tableMeta.Name]
			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			cp, err := rc.checkpointsDB.Get(ctx, tableName)
			if err != nil {
				return errors.Trace(err)
			}
			if cp.Status < checkpoints.CheckpointStatusAllWritten && len(tableMeta.DataFiles) == 0 {
				continue
			}
			igCols, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(dbInfo.Name, tableInfo.Name, rc.cfg.Mydumper.CaseSensitive)
			if err != nil {
				return errors.Trace(err)
			}
			tr, err := NewTableRestore(tableName, tableMeta, dbInfo, tableInfo, cp, igCols.ColumnsMap(), kvStore, log.FromContext(ctx))
			if err != nil {
				return errors.Trace(err)
			}

			allTasks = append(allTasks, task{tr: tr, cp: cp})

			if len(cp.Engines) == 0 {
				for _, fi := range tableMeta.DataFiles {
					totalDataSizeToRestore += fi.FileMeta.FileSize
				}
			} else {
				for _, eng := range cp.Engines {
					for _, chunk := range eng.Chunks {
						totalDataSizeToRestore += chunk.UnfinishedSize()
					}
				}
			}
		}
	}

	if m, ok := metric.FromContext(ctx); ok {
		m.BytesCounter.WithLabelValues(metric.BytesStateTotalRestore).Add(float64(totalDataSizeToRestore))
	}

	for i := range allTasks {
		wg.Add(1)
		select {
		case taskCh <- allTasks[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	wg.Wait()
	// if context is done, should return directly
	select {
	case <-ctx.Done():
		err := restoreErr.Get()
		if err == nil {
			err = ctx.Err()
		}
		logTask.End(zap.ErrorLevel, err)
		return err
	default:
	}

	postProgress = func() error {
		close(postProcessTaskChan)
		// otherwise, we should run all tasks in the post-process task chan
		for i := 0; i < rc.cfg.App.TableConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range postProcessTaskChan {
					metaMgr := rc.metaMgrBuilder.TableMetaMgr(task.tr)
					// force all the remain post-process tasks to be executed
					_, err2 := task.tr.postProcess(ctx, rc, task.cp, true, metaMgr)
					restoreErr.Set(err2)
				}
			}()
		}
		wg.Wait()
		return restoreErr.Get()
	}

	return nil
}

func addExtendDataForCheckpoint(
	ctx context.Context,
	cfg *config.Config,
	cp *checkpoints.TableCheckpoint,
) error {
	if len(cfg.Routes) == 0 {
		return nil
	}
	hasExtractor := false
	for _, route := range cfg.Routes {
		hasExtractor = hasExtractor || route.TableExtractor != nil || route.SchemaExtractor != nil || route.SourceExtractor != nil
		if hasExtractor {
			break
		}
	}
	if !hasExtractor {
		return nil
	}

	// Use default file router directly because fileRouter and router are not compatible
	fileRouter, err := mydump.NewDefaultFileRouter(log.FromContext(ctx))
	if err != nil {
		return err
	}
	var router *regexprrouter.RouteTable
	router, err = regexprrouter.NewRegExprRouter(cfg.Mydumper.CaseSensitive, cfg.Routes)
	if err != nil {
		return err
	}
	for _, engine := range cp.Engines {
		for _, chunk := range engine.Chunks {
			_, file := filepath.Split(chunk.FileMeta.Path)
			var res *mydump.RouteResult
			res, err = fileRouter.Route(file)
			if err != nil {
				return err
			}
			extendCols, extendData := router.FetchExtendColumn(res.Schema, res.Name, cfg.Mydumper.SourceID)
			chunk.FileMeta.ExtendData = mydump.ExtendColumnData{
				Columns: extendCols,
				Values:  extendData,
			}
		}
	}
	return nil
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
		err := addExtendDataForCheckpoint(ctx, rc.cfg, cp)
		if err != nil {
			return false, errors.Trace(err)
		}
	} else if cp.Status < checkpoints.CheckpointStatusAllWritten {
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
		db, _ := rc.tidbGlue.GetDB()
		versionStr, err := version.FetchVersion(ctx, db)
		if err != nil {
			return false, errors.Trace(err)
		}

		versionInfo := version.ParseServerInfo(versionStr)

		// "show table next_row_id" is only available after tidb v4.0.0
		if versionInfo.ServerVersion.Major >= 4 && isLocalBackend(rc.cfg) {
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
			cp.AllocBase = mathutil.Max(cp.AllocBase, tr.tableInfo.Core.AutoRandID)
			if err := tr.alloc.Get(autoid.AutoRandomType).Rebase(context.Background(), cp.AllocBase, false); err != nil {
				return false, err
			}
		} else {
			cp.AllocBase = mathutil.Max(cp.AllocBase, tr.tableInfo.Core.AutoIncID)
			if err := tr.alloc.Get(autoid.RowIDAllocType).Rebase(context.Background(), cp.AllocBase, false); err != nil {
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

func (rc *Controller) outpuErrorSummary() {
	if rc.errorMgr.HasError() {
		fmt.Println(rc.errorMgr.Output())
	}
}

// do full compaction for the whole data.
func (rc *Controller) fullCompact(ctx context.Context) error {
	if !rc.cfg.PostRestore.Compact {
		log.FromContext(ctx).Info("skip full compaction")
		return nil
	}

	// wait until any existing level-1 compact to complete first.
	task := log.FromContext(ctx).Begin(zap.InfoLevel, "wait for completion of existing level 1 compaction")
	for !rc.compactState.CompareAndSwap(compactStateIdle, compactStateDoing) {
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

func (rc *Controller) switchToNormalMode(ctx context.Context) {
	rc.switchTiKVMode(ctx, sstpb.SwitchMode_Normal)
}

func (rc *Controller) switchTiKVMode(ctx context.Context, mode sstpb.SwitchMode) {
	// // tidb backend don't need to switch tikv to import mode
	if isTiDBBackend(rc.cfg) {
		return
	}

	log.FromContext(ctx).Info("switch import mode", zap.Stringer("mode", mode))

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
	if !rc.diskQuotaState.CompareAndSwap(diskQuotaStateIdle, diskQuotaStateChecking) {
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
			if m, ok := metric.FromContext(ctx); ok {
				m.LocalStorageUsageBytesGauge.WithLabelValues("disk").Set(float64(totalDiskSize))
				m.LocalStorageUsageBytesGauge.WithLabelValues("mem").Set(float64(totalMemSize))
			}

			logger := log.FromContext(ctx).With(
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
				locker = &rc.diskQuotaLock
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
				// Use a larger split region size to avoid split the same region by many times.
				if err := rc.backend.UnsafeImportAndReset(ctx, engine, int64(config.SplitRegionSize)*int64(config.MaxSplitRegionSizeRatio), int64(config.SplitRegionKeys)*int64(config.MaxSplitRegionSizeRatio)); err != nil {
					importErr = multierr.Append(importErr, err)
				}
			}
			task.End(zap.ErrorLevel, importErr)
			return
		}
	}()
}

func (rc *Controller) setGlobalVariables(ctx context.Context) error {
	// skip for tidb backend to be compatible with MySQL
	if isTiDBBackend(rc.cfg) {
		return nil
	}
	// set new collation flag base on tidb config
	enabled, err := ObtainNewCollationEnabled(ctx, rc.tidbGlue.GetSQLExecutor())
	if err != nil {
		return err
	}
	// we should enable/disable new collation here since in server mode, tidb config
	// may be different in different tasks
	collate.SetNewCollationEnabledForTest(enabled)
	log.FromContext(ctx).Info("new_collation_enabled", zap.Bool("enabled", enabled))

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

	logger := log.FromContext(ctx).With(
		zap.Stringer("keepAfterSuccess", rc.cfg.Checkpoint.KeepAfterSuccess),
		zap.Int64("taskID", rc.cfg.TaskID),
	)

	task := logger.Begin(zap.InfoLevel, "clean checkpoints")
	var err error
	switch rc.cfg.Checkpoint.KeepAfterSuccess {
	case config.CheckpointRename:
		err = rc.checkpointsDB.MoveCheckpoints(ctx, rc.cfg.TaskID)
	case config.CheckpointRemove:
		err = rc.checkpointsDB.RemoveCheckpoint(ctx, "all")
	}
	task.End(zap.ErrorLevel, err)
	if err != nil {
		return common.ErrCleanCheckpoint.Wrap(err).GenWithStackByArgs()
	}
	return nil
}

func isLocalBackend(cfg *config.Config) bool {
	return cfg.TikvImporter.Backend == config.BackendLocal
}

func isTiDBBackend(cfg *config.Config) bool {
	return cfg.TikvImporter.Backend == config.BackendTiDB
}

// preCheckRequirements checks
// 1. Cluster resource
// 2. Local node resource
// 3. Cluster region
// 4. Lightning configuration
// before restore tables start.
func (rc *Controller) preCheckRequirements(ctx context.Context) error {
	if err := rc.DataCheck(ctx); err != nil {
		return errors.Trace(err)
	}

	if rc.cfg.App.CheckRequirements {
		if err := rc.ClusterIsAvailable(ctx); err != nil {
			return errors.Trace(err)
		}

		if rc.ownStore {
			if err := rc.StoragePermission(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if err := rc.metaMgrBuilder.Init(ctx); err != nil {
		return common.ErrInitMetaManager.Wrap(err).GenWithStackByArgs()
	}
	taskExist := false

	// We still need to sample source data even if this task has existed, because we need to judge whether the
	// source is in order as row key to decide how to sort local data.
	estimatedSizeResult, err := rc.preInfoGetter.EstimateSourceDataSize(ctx)
	if err != nil {
		return common.ErrCheckDataSource.Wrap(err).GenWithStackByArgs()
	}
	estimatedDataSizeWithIndex := estimatedSizeResult.SizeWithIndex

	// Do not import with too large concurrency because these data may be all unsorted.
	if estimatedSizeResult.HasUnsortedBigTables {
		if rc.cfg.App.TableConcurrency > rc.cfg.App.IndexConcurrency {
			rc.cfg.App.TableConcurrency = rc.cfg.App.IndexConcurrency
		}
	}
	if rc.status != nil {
		rc.status.TotalFileSize.Store(estimatedSizeResult.SizeWithoutIndex)
	}
	if isLocalBackend(rc.cfg) {
		pdController, err := pdutil.NewPdController(ctx, rc.cfg.TiDB.PdAddr,
			rc.tls.TLSConfig(), rc.tls.ToPDSecurityOption())
		if err != nil {
			return common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
		}

		// PdController will be closed when `taskMetaMgr` closes.
		rc.taskMgr = rc.metaMgrBuilder.TaskMetaMgr(pdController)
		taskExist, err = rc.taskMgr.CheckTaskExist(ctx)
		if err != nil {
			return common.ErrMetaMgrUnknown.Wrap(err).GenWithStackByArgs()
		}
		if !taskExist {
			if err = rc.taskMgr.InitTask(ctx, estimatedDataSizeWithIndex); err != nil {
				return common.ErrMetaMgrUnknown.Wrap(err).GenWithStackByArgs()
			}
		}
		if rc.cfg.App.CheckRequirements {
			needCheck := true
			if rc.cfg.Checkpoint.Enable {
				taskCheckpoints, err := rc.checkpointsDB.TaskCheckpoint(ctx)
				if err != nil {
					return common.ErrReadCheckpoint.Wrap(err).GenWithStack("get task checkpoint failed")
				}
				// If task checkpoint is initialized, it means check has been performed before.
				// We don't need and shouldn't check again, because lightning may have already imported some data.
				needCheck = taskCheckpoints == nil
			}
			if needCheck {
				err = rc.localResource(ctx)
				if err != nil {
					return common.ErrCheckLocalResource.Wrap(err).GenWithStackByArgs()
				}
				if err := rc.clusterResource(ctx); err != nil {
					if err1 := rc.taskMgr.CleanupTask(ctx); err1 != nil {
						log.FromContext(ctx).Warn("cleanup task failed", zap.Error(err1))
						return common.ErrMetaMgrUnknown.Wrap(err).GenWithStackByArgs()
					}
				}
				if err := rc.checkClusterRegion(ctx); err != nil {
					return common.ErrCheckClusterRegion.Wrap(err).GenWithStackByArgs()
				}
			}
			// even if checkpoint exists, we still need to make sure CDC/PiTR task is not running.
			if err := rc.checkCDCPiTR(ctx); err != nil {
				return common.ErrCheckCDCPiTR.Wrap(err).GenWithStackByArgs()
			}
		}
	}

	if rc.tidbGlue.OwnsSQLExecutor() && rc.cfg.App.CheckRequirements {
		fmt.Println(rc.checkTemplate.Output())
	}
	if !rc.checkTemplate.Success() {
		if !taskExist && rc.taskMgr != nil {
			err := rc.taskMgr.CleanupTask(ctx)
			if err != nil {
				log.FromContext(ctx).Warn("cleanup task failed", zap.Error(err))
			}
		}
		return common.ErrPreCheckFailed.GenWithStackByArgs(rc.checkTemplate.FailedMsg())
	}
	return nil
}

// DataCheck checks the data schema which needs #rc.restoreSchema finished.
func (rc *Controller) DataCheck(ctx context.Context) error {
	if rc.cfg.App.CheckRequirements {
		if err := rc.HasLargeCSV(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if err := rc.checkCheckpoints(ctx); err != nil {
		return errors.Trace(err)
	}

	if rc.cfg.App.CheckRequirements {
		if err := rc.checkSourceSchema(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if err := rc.checkTableEmpty(ctx); err != nil {
		return common.ErrCheckTableEmpty.Wrap(err).GenWithStackByArgs()
	}
	if err := rc.checkCSVHeader(ctx); err != nil {
		return common.ErrCheckCSVHeader.Wrap(err).GenWithStackByArgs()
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

	reader, err := openReader(ctx, chunk.FileMeta, store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var parser mydump.Parser
	switch chunk.FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := cfg.Mydumper.CSV.Header && chunk.Chunk.Offset == 0
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := mydump.NewCharsetConvertor(cfg.Mydumper.DataCharacterSet, cfg.Mydumper.DataInvalidCharReplace)
		if err != nil {
			return nil, err
		}
		parser, err = mydump.NewCSVParser(ctx, &cfg.Mydumper.CSV, reader, blockBufSize, ioWorkers, hasHeader, charsetConvertor)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(ctx, cfg.TiDB.SQLMode, reader, blockBufSize, ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, store, reader, chunk.FileMeta.Path)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", chunk.Key.Path, chunk.FileMeta.Type.String()))
	}

	if chunk.FileMeta.Compression == mydump.CompressionNone {
		if err = parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		if err = mydump.ReadUntil(parser, chunk.Chunk.Offset); err != nil {
			return nil, errors.Trace(err)
		}
		parser.SetRowID(chunk.Chunk.PrevRowIDMax)
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
	_ = cr.parser.Close()
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

	realOffset int64 // indicates file reader's current position, only used for compressed files
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
	hasMoreKVs := true
	var startRealOffset, currRealOffset int64 // save to 0 at first

	for hasMoreKVs {
		c := keyspace.CodecV1
		if t.kvStore != nil {
			c = t.kvStore.GetCodec()
		}
		var (
			dataChecksum  = verify.NewKVChecksumWithKeyspace(c)
			indexChecksum = verify.NewKVChecksumWithKeyspace(c)
		)
		var columns []string
		var kvPacket []deliveredKVs
		// init these two field as checkpoint current value, so even if there are no kv pairs delivered,
		// chunk checkpoint should stay the same
		startOffset := cr.chunk.Chunk.Offset
		currOffset := startOffset
		startRealOffset = cr.chunk.Chunk.RealOffset
		currRealOffset = startRealOffset
		rowID := cr.chunk.Chunk.PrevRowIDMax

	populate:
		for dataChecksum.SumSize()+indexChecksum.SumSize() < minDeliverBytes {
			select {
			case kvPacket = <-kvsCh:
				if len(kvPacket) == 0 {
					hasMoreKVs = false
					break populate
				}
				for _, p := range kvPacket {
					if p.kvs == nil {
						// This is the last message.
						currOffset = p.offset
						currRealOffset = p.realOffset
						hasMoreKVs = false
						break populate
					}
					p.kvs.ClassifyAndAppend(&dataKVs, dataChecksum, &indexKVs, indexChecksum)
					columns = p.columns
					currOffset = p.offset
					currRealOffset = p.realOffset
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

			if m, ok := metric.FromContext(ctx); ok {
				deliverDur := time.Since(start)
				deliverTotalDur += deliverDur
				m.BlockDeliverSecondsHistogram.Observe(deliverDur.Seconds())
				m.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumSize()))
				m.BlockDeliverBytesHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumSize()))
				m.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindData).Observe(float64(dataChecksum.SumKVS()))
				m.BlockDeliverKVPairsHistogram.WithLabelValues(metric.BlockDeliverKindIndex).Observe(float64(indexChecksum.SumKVS()))
			}
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
		// In local mode, we should write these checkpoints after engine flushed.
		lastOffset := cr.chunk.Chunk.Offset
		cr.chunk.Checksum.Add(dataChecksum)
		cr.chunk.Checksum.Add(indexChecksum)
		cr.chunk.Chunk.Offset = currOffset
		cr.chunk.Chunk.RealOffset = currRealOffset
		cr.chunk.Chunk.PrevRowIDMax = rowID

		if m, ok := metric.FromContext(ctx); ok {
			// value of currOffset comes from parser.pos which increase monotonically. the init value of parser.pos
			// comes from chunk.Chunk.Offset. so it shouldn't happen that currOffset - startOffset < 0.
			// but we met it one time, but cannot reproduce it now, we add this check to make code more robust
			// TODO: reproduce and find the root cause and fix it completely
			var lowOffset, highOffset int64
			if cr.chunk.FileMeta.Compression != mydump.CompressionNone {
				lowOffset, highOffset = startRealOffset, currRealOffset
			} else {
				lowOffset, highOffset = startOffset, currOffset
			}
			delta := highOffset - lowOffset
			if delta >= 0 {
				m.BytesCounter.WithLabelValues(metric.BytesStateRestored).Add(float64(delta))
				if rc.status != nil && rc.status.backend == config.BackendTiDB {
					rc.status.FinishedFileSize.Add(delta)
				}
			} else {
				deliverLogger.Warn("offset go back", zap.Int64("curr", highOffset),
					zap.Int64("start", lowOffset))
			}
		}

		if currOffset > lastOffset || dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0 {
			// No need to save checkpoint if nothing was delivered.
			dataSynced = cr.maybeSaveCheckpoint(rc, t, engineID, cr.chunk, dataEngine, indexEngine)
		}
		failpoint.Inject("SlowDownWriteRows", func() {
			deliverLogger.Warn("Slowed down write rows")
			finished := rc.status.FinishedFileSize.Load()
			total := rc.status.TotalFileSize.Load()
			deliverLogger.Warn("PrintStatus Failpoint",
				zap.Int64("finished", finished),
				zap.Int64("total", total))
		})
		failpoint.Inject("FailAfterWriteRows", nil)
		// TODO: for local backend, we may save checkpoint more frequently, e.g. after written
		// 10GB kv pairs to data engine, we can do a flush for both data & index engine, then we
		// can safely update current checkpoint.

		failpoint.Inject("LocalBackendSaveCheckpoint", func() {
			if !isLocalBackend(rc.cfg) && (dataChecksum.SumKVS() != 0 || indexChecksum.SumKVS() != 0) {
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
			EndOffset:         chunk.Chunk.EndOffset,
		},
	}
}

// filterColumns filter columns and extend columns.
// It accepts:
// - columnsNames, header in the data files;
// - extendData, extendData fetched through data file name, that is to say, table info;
// - ignoreColsMap, columns to be ignored when we import;
// - tableInfo, tableInfo of the target table;
// It returns:
// - filteredColumns, columns of the original data to import.
// - extendValueDatums, extended Data to import.
// The data we import will use filteredColumns as columns, use (parser.LastRow+extendValueDatums) as data
// ColumnPermutation will be modified to make sure the correspondence relationship is correct.
// if len(columnsNames) > 0, it means users has specified each field definition, we can just use users
func filterColumns(columnNames []string, extendData mydump.ExtendColumnData, ignoreColsMap map[string]struct{}, tableInfo *model.TableInfo) ([]string, []types.Datum) {
	extendCols, extendVals := extendData.Columns, extendData.Values
	extendColsSet := set.NewStringSet(extendCols...)
	filteredColumns := make([]string, 0, len(columnNames))
	if len(columnNames) > 0 {
		if len(ignoreColsMap) > 0 {
			for _, c := range columnNames {
				_, ok := ignoreColsMap[c]
				if !ok {
					filteredColumns = append(filteredColumns, c)
				}
			}
		} else {
			filteredColumns = columnNames
		}
	} else if len(ignoreColsMap) > 0 || len(extendCols) > 0 {
		// init column names by table schema
		// after filtered out some columns, we must explicitly set the columns for TiDB backend
		for _, col := range tableInfo.Columns {
			_, ok := ignoreColsMap[col.Name.L]
			// ignore all extend row values specified by users
			if !col.Hidden && !ok && !extendColsSet.Exist(col.Name.O) {
				filteredColumns = append(filteredColumns, col.Name.O)
			}
		}
	}
	extendValueDatums := make([]types.Datum, 0)
	filteredColumns = append(filteredColumns, extendCols...)
	for _, extendVal := range extendVals {
		extendValueDatums = append(extendValueDatums, types.NewStringDatum(extendVal))
	}
	return filteredColumns, extendValueDatums
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
	defer close(kvsCh)

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
	// filteredColumns is column names that excluded ignored columns
	// WARN: this might be not correct when different SQL statements contains different fields,
	// but since ColumnPermutation also depends on the hypothesis that the columns in one source file is the same
	// so this should be ok.
	var (
		filteredColumns []string
		extendVals      []types.Datum
	)
	ignoreColumns, err1 := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(t.dbInfo.Name, t.tableInfo.Core.Name.O, rc.cfg.Mydumper.CaseSensitive)
	if err1 != nil {
		err = err1
		return
	}
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
		var newOffset, rowID, realOffset int64
		var kvSize uint64
		var realOffsetErr error
	outLoop:
		for !canDeliver {
			readDurStart := time.Now()
			err = cr.parser.ReadRow()
			columnNames := cr.parser.Columns()
			newOffset, rowID = cr.parser.Pos()
			if cr.chunk.FileMeta.Compression != mydump.CompressionNone {
				realOffset, realOffsetErr = cr.parser.RealPos()
				if realOffsetErr != nil {
					logger.Warn("fail to get data engine RealPos, progress may not be accurate",
						log.ShortError(realOffsetErr), zap.String("file", cr.chunk.FileMeta.Path))
				}
			}

			switch errors.Cause(err) {
			case nil:
				if !initializedColumns {
					if len(cr.chunk.ColumnPermutation) == 0 {
						if err = t.initializeColumns(columnNames, cr.chunk); err != nil {
							return
						}
					}
					filteredColumns = columnNames
					ignoreColsMap := ignoreColumns.ColumnsMap()
					if len(ignoreColsMap) > 0 || len(cr.chunk.FileMeta.ExtendData.Columns) > 0 {
						filteredColumns, extendVals = filterColumns(columnNames, cr.chunk.FileMeta.ExtendData, ignoreColsMap, t.tableInfo.Core)
					}
					lastRow := cr.parser.LastRow()
					lastRowLen := len(lastRow.Row)
					extendColsMap := make(map[string]int)
					for i, c := range cr.chunk.FileMeta.ExtendData.Columns {
						extendColsMap[c] = lastRowLen + i
					}
					for i, col := range t.tableInfo.Core.Columns {
						if p, ok := extendColsMap[col.Name.O]; ok {
							cr.chunk.ColumnPermutation[i] = p
						}
					}
					initializedColumns = true
				}
			case io.EOF:
				reachEOF = true
				break outLoop
			default:
				err = common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(&cr.chunk.Key, newOffset)
				return
			}
			readDur += time.Since(readDurStart)
			encodeDurStart := time.Now()
			lastRow := cr.parser.LastRow()
			lastRow.Row = append(lastRow.Row, extendVals...)
			// sql -> kv
			kvs, encodeErr := kvEncoder.Encode(logger, lastRow.Row, lastRow.RowID, cr.chunk.ColumnPermutation, cr.chunk.Key.Path, curOffset)
			encodeDur += time.Since(encodeDurStart)

			hasIgnoredEncodeErr := false
			if encodeErr != nil {
				rowText := tidb.EncodeRowForRecord(ctx, t.encTable, rc.cfg.TiDB.SQLMode, lastRow.Row, cr.chunk.ColumnPermutation)
				encodeErr = rc.errorMgr.RecordTypeError(ctx, logger, t.tableName, cr.chunk.Key.Path, newOffset, rowText, encodeErr)
				if encodeErr != nil {
					err = common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(&cr.chunk.Key, newOffset)
				}
				hasIgnoredEncodeErr = true
			}
			cr.parser.RecycleRow(lastRow)
			curOffset = newOffset

			if err != nil {
				return
			}
			if hasIgnoredEncodeErr {
				continue
			}

			kvPacket = append(kvPacket, deliveredKVs{kvs: kvs, columns: filteredColumns, offset: newOffset,
				rowID: rowID, realOffset: realOffset})
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
		}
		encodeTotalDur += encodeDur
		readTotalDur += readDur
		if m, ok := metric.FromContext(ctx); ok {
			m.RowEncodeSecondsHistogram.Observe(encodeDur.Seconds())
			m.RowReadSecondsHistogram.Observe(readDur.Seconds())
			m.RowReadBytesHistogram.Observe(float64(newOffset - offset))
		}

		if len(kvPacket) != 0 {
			deliverKvStart := time.Now()
			if err = send(kvPacket); err != nil {
				return
			}
			if m, ok := metric.FromContext(ctx); ok {
				m.RowKVDeliverSecondsHistogram.Observe(time.Since(deliverKvStart).Seconds())
			}
		}
	}

	err = send([]deliveredKVs{{offset: cr.chunk.Chunk.EndOffset, realOffset: cr.chunk.FileMeta.FileSize}})
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
	kvEncoder, err := rc.backend.NewEncoder(ctx, t.encTable, &kv.SessionOptions{
		SQLMode:   rc.cfg.TiDB.SQLMode,
		Timestamp: cr.chunk.Timestamp,
		SysVars:   rc.sysVars,
		// use chunk.PrevRowIDMax as the auto random seed, so it can stay the same value after recover from checkpoint.
		AutoRandomSeed: cr.chunk.Chunk.PrevRowIDMax,
	})
	if err != nil {
		return err
	}
	defer kvEncoder.Close()

	kvsCh := make(chan []deliveredKVs, maxKVQueueSize)
	deliverCompleteCh := make(chan deliverResult)

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

	readTotalDur, encodeTotalDur, encodeErr := cr.encodeLoop(ctx, kvsCh, t, logTask.Logger, kvEncoder, deliverCompleteCh, rc)
	var deliverErr error
	select {
	case deliverResult, ok := <-deliverCompleteCh:
		if ok {
			logTask.End(zap.ErrorLevel, deliverResult.err,
				zap.Duration("readDur", readTotalDur),
				zap.Duration("encodeDur", encodeTotalDur),
				zap.Duration("deliverDur", deliverResult.totalDur),
				zap.Object("checksum", &cr.chunk.Checksum),
			)
			deliverErr = deliverResult.err
		} else {
			// else, this must cause by ctx cancel
			deliverErr = ctx.Err()
		}
	case <-ctx.Done():
		deliverErr = ctx.Err()
	}
	return errors.Trace(firstErr(encodeErr, deliverErr))
}

func openReader(ctx context.Context, fileMeta mydump.SourceFileMeta, store storage.ExternalStorage) (
	reader storage.ReadSeekCloser, err error) {
	switch {
	case fileMeta.Type == mydump.SourceTypeParquet:
		reader, err = mydump.OpenParquetReader(ctx, store, fileMeta.Path, fileMeta.FileSize)
	case fileMeta.Compression != mydump.CompressionNone:
		compressType, err2 := mydump.ToStorageCompressType(fileMeta.Compression)
		if err2 != nil {
			return nil, err2
		}
		reader, err = storage.WithCompression(store, compressType).Open(ctx, fileMeta.Path)
	default:
		reader, err = store.Open(ctx, fileMeta.Path)
	}
	return
}
