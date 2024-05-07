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

package importer

import (
	"context"
	"database/sql"
	"fmt"
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
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/lightning/pkg/web"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/keyspace"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/tikv"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/etcd"
	regexprrouter "github.com/pingcap/tidb/pkg/util/regexpr-router"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/prometheus/client_golang/prometheus"
	tikvconfig "github.com/tikv/client-go/v2/config"
	kvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/retry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// compact levels
const (
	FullLevelCompact = -1
	Level1Compact    = 1
)

const (
	compactStateIdle int32 = iota
	compactStateDoing
)

// task related table names and create table statements.
const (
	TaskMetaTableName  = "task_meta_v2"
	TableMetaTableName = "table_meta"
	// CreateTableMetadataTable stores the per-table sub jobs information used by TiDB Lightning
	CreateTableMetadataTable = `CREATE TABLE IF NOT EXISTS %s.%s (
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
	CreateTaskMetaTable = `CREATE TABLE IF NOT EXISTS %s.%s (
		task_id BIGINT(20) UNSIGNED NOT NULL,
		pd_cfgs VARCHAR(2048) NOT NULL DEFAULT '',
		status  VARCHAR(32) NOT NULL,
		state   TINYINT(1) NOT NULL DEFAULT 0 COMMENT '0: normal, 1: exited before finish',
		tikv_source_bytes BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		tiflash_source_bytes BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		tikv_avail BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		tiflash_avail BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
		PRIMARY KEY (task_id)
	);`
)

var (
	minTiKVVersionForConflictStrategy = *semver.New("5.2.0")
	maxTiKVVersionForConflictStrategy = version.NextMajorVersion()
)

// DeliverPauser is a shared pauser to pause progress to (*chunkProcessor).encodeLoop
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

// Controller controls the whole import process.
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
	engineMgr     backend.EngineManager
	backend       backend.Backend
	db            *sql.DB
	pdCli         pd.Client
	pdHTTPCli     pdhttp.Client

	sysVars       map[string]string
	tls           *common.TLS
	checkTemplate Template

	errorSummaries errorSummaries

	checkpointsDB checkpoints.DB
	saveCpCh      chan saveCp
	checkpointsWg sync.WaitGroup

	closedEngineLimit *worker.Pool
	addIndexLimit     *worker.Pool

	store          storage.ExternalStorage
	ownStore       bool
	metaMgrBuilder metaMgrBuilder
	errorMgr       *errormanager.ErrorManager
	taskMgr        taskMetaMgr

	diskQuotaLock  sync.RWMutex
	diskQuotaState atomic.Int32
	compactState   atomic.Int32
	status         *LightningStatus
	dupIndicator   *atomic.Bool

	preInfoGetter       PreImportInfoGetter
	precheckItemBuilder *PrecheckItemBuilder
	encBuilder          encode.EncodingBuilder
	tikvModeSwitcher    local.TiKVModeSwitcher

	keyspaceName      string
	resourceGroupName string
	taskType          string
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
	// DB is a connection pool to TiDB
	DB *sql.DB
	// storage interface to write file checkpoints
	CheckpointStorage storage.ExternalStorage
	// when CheckpointStorage is not nil, save file checkpoint to it with this name
	CheckpointName string
	// DupIndicator can expose the duplicate detection result to the caller
	DupIndicator *atomic.Bool
	// Keyspace name
	KeyspaceName string
	// ResourceGroup name for current TiDB user
	ResourceGroupName string
	// TaskType is the source component name use for background task control.
	TaskType string
}

// NewImportController creates a new Controller instance.
func NewImportController(
	ctx context.Context,
	cfg *config.Config,
	param *ControllerParam,
) (*Controller, error) {
	param.Pauser = DeliverPauser
	return NewImportControllerWithPauser(ctx, cfg, param)
}

// NewImportControllerWithPauser creates a new Controller instance with a pauser.
func NewImportControllerWithPauser(
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
		cpdb, err = checkpoints.OpenCheckpointsDB(ctx, cfg)
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

	db := p.DB
	errorMgr := errormanager.New(db, cfg, log.FromContext(ctx))
	if err := errorMgr.Init(ctx); err != nil {
		return nil, common.ErrInitErrManager.Wrap(err).GenWithStackByArgs()
	}

	var encodingBuilder encode.EncodingBuilder
	var backendObj backend.Backend
	var pdCli pd.Client
	var pdHTTPCli pdhttp.Client
	switch cfg.TikvImporter.Backend {
	case config.BackendTiDB:
		encodingBuilder = tidb.NewEncodingBuilder()
		backendObj = tidb.NewTiDBBackend(ctx, db, cfg, errorMgr)
	case config.BackendLocal:
		var rLimit local.RlimT
		rLimit, err = local.GetSystemRLimit()
		if err != nil {
			return nil, err
		}
		maxOpenFiles := int(rLimit / local.RlimT(cfg.App.TableConcurrency))
		// check overflow
		if maxOpenFiles < 0 {
			maxOpenFiles = math.MaxInt32
		}

		addrs := strings.Split(cfg.TiDB.PdAddr, ",")
		pdCli, err = pd.NewClientWithContext(ctx, addrs, tls.ToPDSecurityOption())
		if err != nil {
			return nil, errors.Trace(err)
		}
		pdHTTPCli = pdhttp.NewClientWithServiceDiscovery(
			"lightning",
			pdCli.GetServiceDiscovery(),
			pdhttp.WithTLSConfig(tls.TLSConfig()),
		).WithBackoffer(retry.InitialBackoffer(time.Second, time.Second, pdutil.PDRequestRetryTime*time.Second))

		if isLocalBackend(cfg) && cfg.Conflict.Strategy != config.NoneOnDup {
			if err := tikv.CheckTiKVVersion(ctx, pdHTTPCli, minTiKVVersionForConflictStrategy, maxTiKVVersionForConflictStrategy); err != nil {
				if !berrors.Is(err, berrors.ErrVersionMismatch) {
					return nil, common.ErrCheckKVVersion.Wrap(err).GenWithStackByArgs()
				}
				log.FromContext(ctx).Warn("TiKV version doesn't support conflict strategy. The resolution algorithm will fall back to 'none'", zap.Error(err))
				cfg.Conflict.Strategy = config.NoneOnDup
			}
		}

		initGlobalConfig(tls.ToTiKVSecurityConfig())

		encodingBuilder = local.NewEncodingBuilder(ctx)

		// get resource group name.
		exec := common.SQLWithRetry{
			DB:     db,
			Logger: log.FromContext(ctx),
		}
		if err := exec.QueryRow(ctx, "", "select current_resource_group();", &p.ResourceGroupName); err != nil {
			if common.IsFunctionNotExistErr(err, "current_resource_group") {
				log.FromContext(ctx).Warn("current_resource_group() not supported, ignore this error", zap.Error(err))
			}
		}

		taskType, err := common.GetExplicitRequestSourceTypeFromDB(ctx, db)
		if err != nil {
			return nil, errors.Annotatef(err, "get system variable '%s' failed", variable.TiDBExplicitRequestSourceType)
		}
		if taskType == "" {
			taskType = kvutil.ExplicitTypeLightning
		}
		p.TaskType = taskType

		isRaftKV2, err := common.IsRaftKV2(ctx, db)
		if err != nil {
			log.FromContext(ctx).Warn("check isRaftKV2 failed", zap.Error(err))
		}
		var raftKV2SwitchModeDuration time.Duration
		if isRaftKV2 {
			raftKV2SwitchModeDuration = cfg.Cron.SwitchMode.Duration
		}
		backendConfig := local.NewBackendConfig(cfg, maxOpenFiles, p.KeyspaceName, p.ResourceGroupName, p.TaskType, raftKV2SwitchModeDuration)
		backendObj, err = local.NewBackend(ctx, tls, backendConfig, pdCli.GetServiceDiscovery())
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
	case isSSTImport && cfg.TikvImporter.ParallelImport:
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

	var wrapper backend.TargetInfoGetter
	if cfg.TikvImporter.Backend == config.BackendLocal {
		wrapper = local.NewTargetInfoGetter(tls, db, pdHTTPCli)
	} else {
		wrapper = tidb.NewTargetInfoGetter(db)
	}
	ioWorkers := worker.NewPool(ctx, cfg.App.IOConcurrency, "io")
	targetInfoGetter := &TargetInfoGetterImpl{
		cfg:       cfg,
		db:        db,
		backend:   wrapper,
		pdHTTPCli: pdHTTPCli,
	}
	preInfoGetter, err := NewPreImportInfoGetter(
		cfg,
		p.DBMetas,
		p.DumpFileStorage,
		targetInfoGetter,
		ioWorkers,
		encodingBuilder,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	preCheckBuilder := NewPrecheckItemBuilder(
		cfg, p.DBMetas, preInfoGetter, cpdb, pdHTTPCli,
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
		engineMgr:     backend.MakeEngineManager(backendObj),
		backend:       backendObj,
		pdCli:         pdCli,
		pdHTTPCli:     pdHTTPCli,
		db:            db,
		sysVars:       common.DefaultImportantVariables,
		tls:           tls,
		checkTemplate: NewSimpleTemplate(),

		errorSummaries:    makeErrorSummaries(log.FromContext(ctx)),
		checkpointsDB:     cpdb,
		saveCpCh:          make(chan saveCp),
		closedEngineLimit: worker.NewPool(ctx, cfg.App.TableConcurrency*2, "closed-engine"),
		// Currently, TiDB add index acceration doesn't support multiple tables simultaneously.
		// So we use a single worker to ensure at most one table is adding index at the same time.
		addIndexLimit: worker.NewPool(ctx, 1, "add-index"),

		store:          p.DumpFileStorage,
		ownStore:       p.OwnExtStorage,
		metaMgrBuilder: metaBuilder,
		errorMgr:       errorMgr,
		status:         p.Status,
		taskMgr:        nil,
		dupIndicator:   p.DupIndicator,

		preInfoGetter:       preInfoGetter,
		precheckItemBuilder: preCheckBuilder,
		encBuilder:          encodingBuilder,
		tikvModeSwitcher:    local.NewTiKVModeSwitcher(tls.TLSConfig(), pdHTTPCli, log.FromContext(ctx).Logger),

		keyspaceName:      p.KeyspaceName,
		resourceGroupName: p.ResourceGroupName,
		taskType:          p.TaskType,
	}

	return rc, nil
}

// Close closes the controller.
func (rc *Controller) Close() {
	rc.backend.Close()
	_ = rc.db.Close()
	if rc.pdCli != nil {
		rc.pdCli.Close()
	}
}

// Run starts the restore task.
func (rc *Controller) Run(ctx context.Context) error {
	failpoint.Inject("beforeRun", func() {})

	opts := []func(context.Context) error{
		rc.setGlobalVariables,
		rc.restoreSchema,
		rc.preCheckRequirements,
		rc.initCheckpoint,
		rc.importTables,
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

func (rc *Controller) restoreSchema(ctx context.Context) error {
	// create table with schema file
	// we can handle the duplicated created with createIfNotExist statement
	// and we will check the schema in TiDB is valid with the datafile in DataCheck later.
	logger := log.FromContext(ctx)
	concurrency := min(rc.cfg.App.RegionConcurrency, 8)
	// sql.DB is a connection pool, we set it to concurrency + 1(for job generator)
	// to reuse connections, as we might call db.Conn/conn.Close many times.
	// there's no API to get sql.DB.MaxIdleConns, so we revert to its default which is 2
	rc.db.SetMaxIdleConns(concurrency + 1)
	defer rc.db.SetMaxIdleConns(2)
	schemaImp := mydump.NewSchemaImporter(logger, rc.cfg.TiDB.SQLMode, rc.db, rc.store, concurrency)
	err := schemaImp.Run(ctx, rc.dbMetas)
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
	if err := rc.loadDesiredTableInfos(ctx); err != nil {
		return err
	}

	rc.checkpointsWg.Add(1) // checkpointsWg will be done in `rc.listenCheckpointUpdates`
	go rc.listenCheckpointUpdates(log.FromContext(ctx))

	// Estimate the number of chunks for progress reporting
	return rc.estimateChunkCountIntoMetrics(ctx)
}

func (rc *Controller) loadDesiredTableInfos(ctx context.Context) error {
	for _, dbInfo := range rc.dbInfos {
		for _, tableInfo := range dbInfo.Tables {
			cp, err := rc.checkpointsDB.Get(ctx, common.UniqueTable(dbInfo.Name, tableInfo.Name))
			if err != nil {
				return err
			}
			// If checkpoint is disabled, cp.TableInfo will be nil.
			// In this case, we just use current tableInfo as desired tableInfo.
			if cp.TableInfo != nil {
				tableInfo.Desired = cp.TableInfo
			}
		}
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
			_, eID := backend.MakeUUID(tableName, int64(engineID))
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
	for _, dbMeta := range rc.dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			batchSize := mydump.CalculateBatchSize(float64(rc.cfg.Mydumper.BatchSize),
				tableMeta.IsRowOrdered, float64(tableMeta.TotalSize))
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
				if engineID == common.IndexEngineID {
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

	var switchModeChan <-chan time.Time
	// tidb backend don't need to switch tikv to import mode
	if isLocalBackend(rc.cfg) && rc.cfg.Cron.SwitchMode.Duration > 0 {
		switchModeTicker := time.NewTicker(rc.cfg.Cron.SwitchMode.Duration)
		cancelFuncs = append(cancelFuncs, func(bool) { switchModeTicker.Stop() })
		cancelFuncs = append(cancelFuncs, func(do bool) {
			if do {
				rc.tikvModeSwitcher.ToNormalMode(ctx)
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
			if rc.cfg.Cron.SwitchMode.Duration > 0 && isLocalBackend(rc.cfg) {
				rc.tikvModeSwitcher.ToImportMode(ctx)
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
					// TiKV will switch back to normal mode if we didn't call this again within 10 minutes
					rc.tikvModeSwitcher.ToImportMode(ctx)

				case <-logProgressChan:
					metrics, ok := metric.FromContext(ctx)
					if !ok {
						log.FromContext(ctx).Warn("couldn't find metrics from context, skip log progress")
						continue
					}
					// log the current progress periodically, so OPS will know that we're still working
					nanoseconds := float64(time.Since(start).Nanoseconds())
					totalRestoreBytes := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.StateTotalRestore))
					restoredBytes := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.StateRestored))
					totalRowsToRestore := metric.ReadAllCounters(metrics.RowsCounter.MetricVec, prometheus.Labels{"state": metric.StateTotalRestore})
					restoredRows := metric.ReadAllCounters(metrics.RowsCounter.MetricVec, prometheus.Labels{"state": metric.StateRestored})
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
					bytesWritten := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.StateRestoreWritten))
					bytesImported := metric.ReadCounter(metrics.BytesCounter.WithLabelValues(metric.StateImported))

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
					restoreRowsField := zap.Skip()
					remaining = zap.Skip()
					totalPercent := 0.0
					if restoredBytes > 0 || restoredRows > 0 {
						var restorePercent float64
						if totalRowsToRestore > 0 {
							restorePercent = math.Min(restoredRows/totalRowsToRestore, 1.0)
							restoreRowsField = zap.String("restore-rows", fmt.Sprintf("%.0f/%.0f",
								restoredRows, totalRowsToRestore))
						} else {
							restorePercent = math.Min(restoredBytes/totalRestoreBytes, 1.0)
							restoreRowsField = zap.String("restore-rows", fmt.Sprintf("%.0f/%.0f(estimated)",
								restoredRows, restoredRows/restorePercent))
						}
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
						restoreBytesField, restoreRowsField, importBytesField,
						encodeSpeedField,
						zap.String("state", state),
						remaining,
					)

				case <-checkQuotaChan:
					// verify the total space occupied by sorted-kv-dir is below the quota,
					// otherwise we perform an emergency import.
					rc.enforceDiskQuota(ctx)
				}
			}
		}, func(do bool) {
			log.FromContext(ctx).Info("cancel periodic actions", zap.Bool("do", do))
			for _, f := range cancelFuncs {
				f(do)
			}
		}
}

func (rc *Controller) buildTablesRanges() []tidbkv.KeyRange {
	var keyRanges []tidbkv.KeyRange
	for _, dbInfo := range rc.dbInfos {
		for _, tableInfo := range dbInfo.Tables {
			if ranges, err := distsql.BuildTableRanges(tableInfo.Core); err == nil {
				keyRanges = append(keyRanges, ranges...)
			}
		}
	}
	return keyRanges
}

type checksumManagerKeyType struct{}

var checksumManagerKey checksumManagerKeyType

const (
	pauseGCTTLForDupeRes      = time.Hour
	pauseGCIntervalForDupeRes = time.Minute
)

func (rc *Controller) keepPauseGCForDupeRes(ctx context.Context) (<-chan struct{}, error) {
	tlsOpt := rc.tls.ToPDSecurityOption()
	addrs := strings.Split(rc.cfg.TiDB.PdAddr, ",")
	pdCli, err := pd.NewClientWithContext(ctx, addrs, tlsOpt)
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

func (rc *Controller) importTables(ctx context.Context) (finalErr error) {
	// output error summary
	defer rc.outputErrorSummary()

	if isLocalBackend(rc.cfg) && rc.cfg.Conflict.Strategy != config.NoneOnDup {
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
	var etcdCli *clientv3.Client

	if isLocalBackend(rc.cfg) {
		var (
			restoreFn pdutil.UndoFunc
			err       error
		)

		if rc.cfg.TikvImporter.PausePDSchedulerScope == config.PausePDSchedulerScopeGlobal {
			logTask.Info("pause pd scheduler of global scope")

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
		urlsWithScheme := rc.pdCli.GetServiceDiscovery().GetServiceURLs()
		// remove URL scheme
		urlsWithoutScheme := make([]string, 0, len(urlsWithScheme))
		for _, u := range urlsWithScheme {
			u = strings.TrimPrefix(u, "http://")
			u = strings.TrimPrefix(u, "https://")
			urlsWithoutScheme = append(urlsWithoutScheme, u)
		}
		kvStore, err = driver.TiKVDriver{}.OpenWithOptions(
			fmt.Sprintf(
				"tikv://%s?disableGC=true&keyspaceName=%s",
				strings.Join(urlsWithoutScheme, ","), rc.keyspaceName,
			),
			driver.WithSecurity(rc.tls.ToTiKVSecurityConfig()),
		)
		if err != nil {
			return errors.Trace(err)
		}
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints:        urlsWithScheme,
			AutoSyncInterval: 30 * time.Second,
			TLS:              rc.tls.TLSConfig(),
		})
		if err != nil {
			return errors.Trace(err)
		}
		etcd.SetEtcdCliByNamespace(etcdCli, keyspace.MakeKeyspaceEtcdNamespace(kvStore.GetCodec()))

		manager, err := NewChecksumManager(ctx, rc, kvStore)
		if err != nil {
			return errors.Trace(err)
		}
		ctx = context.WithValue(ctx, &checksumManagerKey, manager)

		undo, err := rc.registerTaskToPD(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		defer undo()
	}

	type task struct {
		tr *TableImporter
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
		if etcdCli != nil {
			if err := etcdCli.Close(); err != nil {
				logTask.Warn("failed to close etcd client", zap.Error(err))
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

				needPostProcess, err := task.tr.importTable(ctx, rc, task.cp)
				if err != nil && !common.IsContextCanceledError(err) {
					task.tr.logger.Error("failed to import table", zap.Error(err))
				}

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
			tr, err := NewTableImporter(tableName, tableMeta, dbInfo, tableInfo, cp, igCols.ColumnsMap(), kvStore, etcdCli, log.FromContext(ctx))
			if err != nil {
				return errors.Trace(err)
			}

			allTasks = append(allTasks, task{tr: tr, cp: cp})

			if len(cp.Engines) == 0 {
				for i, fi := range tableMeta.DataFiles {
					totalDataSizeToRestore += fi.FileMeta.FileSize
					if fi.FileMeta.Type == mydump.SourceTypeParquet {
						numberRows, err := mydump.ReadParquetFileRowCountByFile(ctx, rc.store, fi.FileMeta)
						if err != nil {
							return errors.Trace(err)
						}
						if m, ok := metric.FromContext(ctx); ok {
							m.RowsCounter.WithLabelValues(metric.StateTotalRestore, tableName).Add(float64(numberRows))
						}
						fi.FileMeta.Rows = numberRows
						tableMeta.DataFiles[i] = fi
					}
				}
			} else {
				for _, eng := range cp.Engines {
					for _, chunk := range eng.Chunks {
						// for parquet files filesize is more accurate, we can calculate correct unfinished bytes unless
						//  we set up the reader, so we directly use filesize here
						if chunk.FileMeta.Type == mydump.SourceTypeParquet {
							totalDataSizeToRestore += chunk.FileMeta.FileSize
							if m, ok := metric.FromContext(ctx); ok {
								m.RowsCounter.WithLabelValues(metric.StateTotalRestore, tableName).Add(float64(chunk.UnfinishedSize()))
							}
						} else {
							totalDataSizeToRestore += chunk.UnfinishedSize()
						}
					}
				}
			}
		}
	}

	if m, ok := metric.FromContext(ctx); ok {
		m.BytesCounter.WithLabelValues(metric.StateTotalRestore).Add(float64(totalDataSizeToRestore))
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

func (rc *Controller) registerTaskToPD(ctx context.Context) (undo func(), _ error) {
	etcdCli, err := dialEtcdWithCfg(ctx, rc.cfg, rc.pdCli.GetServiceDiscovery().GetServiceURLs())
	if err != nil {
		return nil, errors.Trace(err)
	}

	register := utils.NewTaskRegister(etcdCli, utils.RegisterLightning, fmt.Sprintf("lightning-%s", uuid.New()))

	undo = func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := register.Close(closeCtx); err != nil {
			log.L().Warn("failed to unregister task", zap.Error(err))
		}
		if err := etcdCli.Close(); err != nil {
			log.L().Warn("failed to close etcd client", zap.Error(err))
		}
	}
	if err := register.RegisterTask(ctx); err != nil {
		undo()
		return nil, errors.Trace(err)
	}
	return undo, nil
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

func (rc *Controller) outputErrorSummary() {
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
	return tikv.ForAllStores(
		ctx,
		rc.pdHTTPCli,
		metapb.StoreState_Offline,
		func(c context.Context, store *pdhttp.MetaStore) error {
			return tikv.Compact(c, rc.tls, store.Address, level, rc.resourceGroupName)
		},
	)
}

func (rc *Controller) enforceDiskQuota(ctx context.Context) {
	if !rc.diskQuotaState.CompareAndSwap(diskQuotaStateIdle, diskQuotaStateChecking) {
		// do not run multiple the disk quota check / import simultaneously.
		// (we execute the lock check in background to avoid blocking the cron thread)
		return
	}

	localBackend := rc.backend.(*local.Backend)
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
			largeEngines, inProgressLargeEngines, totalDiskSize, totalMemSize := local.CheckDiskQuota(localBackend, quota)
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
			if err := rc.backend.FlushAllEngines(ctx); err != nil {
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
				if err := localBackend.UnsafeImportAndReset(ctx, engine, int64(config.SplitRegionSize)*int64(config.MaxSplitRegionSizeRatio), int64(config.SplitRegionKeys)*int64(config.MaxSplitRegionSizeRatio)); err != nil {
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
	enabled, err := ObtainNewCollationEnabled(ctx, rc.db)
	if err != nil {
		return err
	}
	// we should enable/disable new collation here since in server mode, tidb config
	// may be different in different tasks
	collate.SetNewCollationEnabledForTest(enabled)
	log.FromContext(ctx).Info(session.TidbNewCollationEnabled, zap.Bool("enabled", enabled))

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
	estimatedTiflashDataSize := estimatedSizeResult.TiFlashSize

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
		pdAddrs := rc.pdCli.GetServiceDiscovery().GetServiceURLs()
		pdController, err := pdutil.NewPdController(
			ctx, pdAddrs, rc.tls.TLSConfig(), rc.tls.ToPDSecurityOption(),
		)
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
			if err = rc.taskMgr.InitTask(ctx, estimatedDataSizeWithIndex, estimatedTiflashDataSize); err != nil {
				return common.ErrMetaMgrUnknown.Wrap(err).GenWithStackByArgs()
			}
		}
		if rc.cfg.TikvImporter.PausePDSchedulerScope == config.PausePDSchedulerScopeTable &&
			!rc.taskMgr.CanPauseSchedulerByKeyRange() {
			return errors.New("target cluster don't support pause-pd-scheduler-scope=table, the minimal version required is 6.1.0")
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

	if rc.cfg.App.CheckRequirements {
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

var (
	maxKVQueueSize         = 32             // Cache at most this number of rows before blocking the encode loop
	minDeliverBytes uint64 = 96 * units.KiB // 96 KB (data + index). batch at least this amount of bytes to reduce number of messages
)

type deliveredKVs struct {
	kvs     encode.Row // if kvs is nil, this indicated we've got the last message.
	columns []string
	offset  int64
	rowID   int64

	realOffset int64 // indicates file reader's current position, only used for compressed files
}

type deliverResult struct {
	totalDur time.Duration
	err      error
}

func saveCheckpoint(rc *Controller, t *TableImporter, engineID int32, chunk *checkpoints.ChunkCheckpoint) {
	// We need to update the AllocBase every time we've finished a file.
	// The AllocBase is determined by the maximum of the "handle" (_tidb_rowid
	// or integer primary key), which can only be obtained by reading all data.

	var base int64
	if t.tableInfo.Core.ContainsAutoRandomBits() {
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

// check store liveness of tikv client-go requires GlobalConfig to work correctly, so we need to init it,
// else tikv will report SSL error when tls is enabled.
// and the SSL error seems affects normal logic of newer TiKV version, and cause the error "tikv: region is unavailable"
// during checksum.
// todo: DM relay on lightning physical mode too, but client-go doesn't support passing TLS data as bytes,
func initGlobalConfig(secCfg tikvconfig.Security) {
	if secCfg.ClusterSSLCA != "" || secCfg.ClusterSSLCert != "" {
		conf := tidbconfig.GetGlobalConfig()
		conf.Security.ClusterSSLCA = secCfg.ClusterSSLCA
		conf.Security.ClusterSSLCert = secCfg.ClusterSSLCert
		conf.Security.ClusterSSLKey = secCfg.ClusterSSLKey
		tidbconfig.StoreGlobalConfig(conf)
	}
}
