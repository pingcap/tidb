// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/ingest"
	sess "github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/ddl/syncer"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/table"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/syncutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	// currentVersion is for all new DDL jobs.
	currentVersion = 1
	// DDLOwnerKey is the ddl owner path that is saved to etcd, and it's exported for testing.
	DDLOwnerKey = "/tidb/ddl/fg/owner"
	// addingDDLJobPrefix is the path prefix used to record the newly added DDL job, and it's saved to etcd.
	addingDDLJobPrefix = "/tidb/ddl/add_ddl_job_"
	ddlPrompt          = "ddl"

	shardRowIDBitsMax = 15

	batchAddingJobs = 10

	reorgWorkerCnt   = 10
	generalWorkerCnt = 1

	// checkFlagIndexInJobArgs is the recoverCheckFlag index used in RecoverTable/RecoverSchema job arg list.
	checkFlagIndexInJobArgs = 1
)

const (
	// The recoverCheckFlag is used to judge the gc work status when RecoverTable/RecoverSchema.
	recoverCheckFlagNone int64 = iota
	recoverCheckFlagEnableGC
	recoverCheckFlagDisableGC
)

// OnExist specifies what to do when a new object has a name collision.
type OnExist uint8

// AllocTableIDIf specifies whether to retain the old table ID.
// If this returns "false", then we would assume the table ID has been
// allocated before calling `CreateTableWithInfo` family.
type AllocTableIDIf func(*model.TableInfo) bool

// CreateTableWithInfoConfig is the configuration of `CreateTableWithInfo`.
type CreateTableWithInfoConfig struct {
	OnExist            OnExist
	ShouldAllocTableID AllocTableIDIf
}

// CreateTableWithInfoConfigurier is the "diff" which can be applied to the
// CreateTableWithInfoConfig, currently implementations are "OnExist" and "AllocTableIDIf".
type CreateTableWithInfoConfigurier interface {
	// Apply the change over the config.
	Apply(*CreateTableWithInfoConfig)
}

// GetCreateTableWithInfoConfig applies the series of configurier from default config
// and returns the final config.
func GetCreateTableWithInfoConfig(cs []CreateTableWithInfoConfigurier) CreateTableWithInfoConfig {
	config := CreateTableWithInfoConfig{}
	for _, c := range cs {
		c.Apply(&config)
	}
	if config.ShouldAllocTableID == nil {
		config.ShouldAllocTableID = func(*model.TableInfo) bool { return true }
	}
	return config
}

// Apply implements Configurier.
func (o OnExist) Apply(c *CreateTableWithInfoConfig) {
	c.OnExist = o
}

// Apply implements Configurier.
func (a AllocTableIDIf) Apply(c *CreateTableWithInfoConfig) {
	c.ShouldAllocTableID = a
}

const (
	// OnExistError throws an error on name collision.
	OnExistError OnExist = iota
	// OnExistIgnore skips creating the new object.
	OnExistIgnore
	// OnExistReplace replaces the old object by the new object. This is only
	// supported by VIEWs at the moment. For other object types, this is
	// equivalent to OnExistError.
	OnExistReplace

	jobRecordCapacity = 16
)

var (
	// EnableSplitTableRegion is a flag to decide whether to split a new region for
	// a newly created table. It takes effect only if the Storage supports split
	// region.
	EnableSplitTableRegion = uint32(0)
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error
	AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error
	DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error
	CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error
	CreateView(ctx sessionctx.Context, stmt *ast.CreateViewStmt) error
	DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error)
	RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error)
	RecoverSchema(ctx sessionctx.Context, recoverSchemaInfo *RecoverSchemaInfo) error
	DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error)
	CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error
	DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error
	AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error
	TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error
	LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error
	UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error
	CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error
	UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error
	RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error
	CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error
	DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error)
	AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error
	CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error
	DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error
	AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error
	AddResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) error
	AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) error
	DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) error
	FlashbackCluster(ctx sessionctx.Context, flashbackTS uint64) error

	// CreateSchemaWithInfo creates a database (schema) given its database info.
	//
	// WARNING: the DDL owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateSchemaWithInfo(
		ctx sessionctx.Context,
		info *model.DBInfo,
		onExist OnExist) error

	// CreateTableWithInfo creates a table, view or sequence given its table info.
	//
	// WARNING: the DDL owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateTableWithInfo(
		ctx sessionctx.Context,
		schema model.CIStr,
		info *model.TableInfo,
		cs ...CreateTableWithInfoConfigurier) error

	// BatchCreateTableWithInfo is like CreateTableWithInfo, but can handle multiple tables.
	BatchCreateTableWithInfo(ctx sessionctx.Context,
		schema model.CIStr,
		info []*model.TableInfo,
		cs ...CreateTableWithInfoConfigurier) error

	// CreatePlacementPolicyWithInfo creates a placement policy
	//
	// WARNING: the DDL owns the `policy` after calling this function, and will modify its fields
	// in-place. If you want to keep using `policy`, please call Clone() first.
	CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error

	// Start campaigns the owner and starts workers.
	// ctxPool is used for the worker's delRangeManager and creates sessions.
	Start(ctxPool *pools.ResourcePool) error
	// GetLease returns current schema lease time.
	GetLease() time.Duration
	// Stats returns the DDL statistics.
	Stats(vars *variable.SessionVars) (map[string]interface{}, error)
	// GetScope gets the status variables scope.
	GetScope(status string) variable.ScopeFlag
	// Stop stops DDL worker.
	Stop() error
	// RegisterStatsHandle registers statistics handle and its corresponding event channel for ddl.
	RegisterStatsHandle(*handle.Handle)
	// SchemaSyncer gets the schema syncer.
	SchemaSyncer() syncer.SchemaSyncer
	// StateSyncer gets the cluster state syncer.
	StateSyncer() syncer.StateSyncer
	// OwnerManager gets the owner manager.
	OwnerManager() owner.Manager
	// GetID gets the ddl ID.
	GetID() string
	// GetTableMaxHandle gets the max row ID of a normal table or a partition.
	GetTableMaxHandle(ctx *JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error)
	// SetBinlogClient sets the binlog client for DDL worker. It's exported for testing.
	SetBinlogClient(*pumpcli.PumpsClient)
	// GetHook gets the hook. It's exported for testing.
	GetHook() Callback
	// SetHook sets the hook.
	SetHook(h Callback)
	// GetInfoSchemaWithInterceptor gets the infoschema binding to d. It's exported for testing.
	GetInfoSchemaWithInterceptor(ctx sessionctx.Context) infoschema.InfoSchema
	// DoDDLJob does the DDL job, it's exported for test.
	DoDDLJob(ctx sessionctx.Context, job *model.Job) error
}

type limitJobTask struct {
	job      *model.Job
	err      chan error
	cacheErr error
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m          sync.RWMutex
	wg         tidbutil.WaitGroupWrapper // It's only used to deal with data race in restart_test.
	limitJobCh chan *limitJobTask

	*ddlCtx
	sessPool          *sess.Pool
	delRangeMgr       delRangeManager
	enableTiFlashPoll *atomicutil.Bool
	// used in the concurrency ddl.
	reorgWorkerPool      *workerPool
	generalDDLWorkerPool *workerPool
	// get notification if any DDL coming.
	ddlJobCh chan struct{}
}

// waitSchemaSyncedController is to control whether to waitSchemaSynced or not.
type waitSchemaSyncedController struct {
	mu  sync.RWMutex
	job map[int64]struct{}

	// true if this node is elected to the DDL owner, we should wait 2 * lease before it runs the first DDL job.
	once *atomicutil.Bool
}

func newWaitSchemaSyncedController() *waitSchemaSyncedController {
	return &waitSchemaSyncedController{
		job:  make(map[int64]struct{}, jobRecordCapacity),
		once: atomicutil.NewBool(true),
	}
}

func (w *waitSchemaSyncedController) registerSync(job *model.Job) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.job[job.ID] = struct{}{}
}

func (w *waitSchemaSyncedController) isSynced(job *model.Job) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.job[job.ID]
	return !ok
}

func (w *waitSchemaSyncedController) synced(job *model.Job) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.job, job.ID)
}

// ddlCtx is the context when we use worker to handle DDL jobs.
type ddlCtx struct {
	ctx          context.Context
	cancel       context.CancelFunc
	uuid         string
	store        kv.Storage
	ownerManager owner.Manager
	schemaSyncer syncer.SchemaSyncer
	stateSyncer  syncer.StateSyncer
	ddlJobDoneCh chan struct{}
	ddlEventCh   chan<- *util.Event
	lease        time.Duration        // lease is schema lease.
	binlogCli    *pumpcli.PumpsClient // binlogCli is used for Binlog.
	infoCache    *infoschema.InfoCache
	statsHandle  *handle.Handle
	tableLockCkr util.DeadTableLockChecker
	etcdCli      *clientv3.Client
	// backfillJobCh gets notification if any backfill jobs coming.
	backfillJobCh chan struct{}

	*waitSchemaSyncedController
	*schemaVersionManager
	// recording the running jobs.
	runningJobs struct {
		sync.RWMutex
		ids map[int64]struct{}
	}
	// It holds the running DDL jobs ID.
	runningJobIDs []string
	// reorgCtx is used for reorganization.
	reorgCtx reorgContexts
	// backfillCtx is used for backfill workers.
	backfillCtx struct {
		syncutil.RWMutex
		jobCtxMap map[int64]*JobContext
	}

	jobCtx struct {
		sync.RWMutex
		// jobCtxMap maps job ID to job's ctx.
		jobCtxMap map[int64]*JobContext
	}

	// hook may be modified.
	mu struct {
		sync.RWMutex
		hook        Callback
		interceptor Interceptor
	}

	ddlSeqNumMu struct {
		sync.Mutex
		seqNum uint64
	}
}

// schemaVersionManager is used to manage the schema version. To prevent the conflicts on this key between different DDL job,
// we use another transaction to update the schema version, so that we need to lock the schema version and unlock it until the job is committed.
type schemaVersionManager struct {
	schemaVersionMu sync.Mutex
	// lockOwner stores the job ID that is holding the lock.
	lockOwner atomicutil.Int64
}

func newSchemaVersionManager() *schemaVersionManager {
	return &schemaVersionManager{}
}

func (sv *schemaVersionManager) setSchemaVersion(job *model.Job, store kv.Storage) (schemaVersion int64, err error) {
	sv.lockSchemaVersion(job.ID)
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(ctx context.Context, txn kv.Transaction) error {
		var err error
		m := meta.NewMeta(txn)
		schemaVersion, err = m.GenSchemaVersion()
		return err
	})
	return schemaVersion, err
}

// lockSchemaVersion gets the lock to prevent the schema version from being updated.
func (sv *schemaVersionManager) lockSchemaVersion(jobID int64) {
	ownerID := sv.lockOwner.Load()
	// There may exist one job update schema version many times in multiple-schema-change, so we do not lock here again
	// if they are the same job.
	if ownerID != jobID {
		sv.schemaVersionMu.Lock()
		sv.lockOwner.Store(jobID)
	}
}

// unlockSchemaVersion releases the lock.
func (sv *schemaVersionManager) unlockSchemaVersion(jobID int64) {
	ownerID := sv.lockOwner.Load()
	if ownerID == jobID {
		sv.lockOwner.Store(0)
		sv.schemaVersionMu.Unlock()
	}
}

func (dc *ddlCtx) isOwner() bool {
	isOwner := dc.ownerManager.IsOwner()
	logutil.BgLogger().Debug("[ddl] check whether is the DDL owner", zap.Bool("isOwner", isOwner), zap.String("selfID", dc.uuid))
	if isOwner {
		metrics.DDLCounter.WithLabelValues(metrics.DDLOwner + "_" + mysql.TiDBReleaseVersion).Inc()
	}
	return isOwner
}

func (dc *ddlCtx) setDDLLabelForTopSQL(jobID int64, jobQuery string) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewJobContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.setDDLLabelForTopSQL(jobQuery)
}

func (dc *ddlCtx) setDDLSourceForDiagnosis(jobID int64, jobType model.ActionType) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewJobContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.setDDLLabelForDiagnosis(jobType)
}

func (dc *ddlCtx) getResourceGroupTaggerForTopSQL(jobID int64) tikvrpc.ResourceGroupTagger {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		return nil
	}
	return ctx.getResourceGroupTaggerForTopSQL()
}

func (dc *ddlCtx) removeJobCtx(job *model.Job) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	delete(dc.jobCtx.jobCtxMap, job.ID)
}

func (dc *ddlCtx) jobContext(jobID int64) *JobContext {
	dc.jobCtx.RLock()
	defer dc.jobCtx.RUnlock()
	if jobContext, exists := dc.jobCtx.jobCtxMap[jobID]; exists {
		return jobContext
	}
	return NewJobContext()
}

func (dc *ddlCtx) removeBackfillCtxJobCtx(jobID int64) {
	dc.backfillCtx.Lock()
	delete(dc.backfillCtx.jobCtxMap, jobID)
	dc.backfillCtx.Unlock()
}

func (dc *ddlCtx) backfillCtxJobIDs() []int64 {
	dc.backfillCtx.Lock()
	defer dc.backfillCtx.Unlock()

	runningJobIDs := make([]int64, 0, len(dc.backfillCtx.jobCtxMap))
	for id := range dc.backfillCtx.jobCtxMap {
		runningJobIDs = append(runningJobIDs, id)
	}
	return runningJobIDs
}

func (dc *ddlCtx) setBackfillCtxJobContext(jobID int64, jobQuery string, jobType model.ActionType) (*JobContext, bool) {
	dc.backfillCtx.Lock()
	defer dc.backfillCtx.Unlock()

	jobCtx, existent := dc.backfillCtx.jobCtxMap[jobID]
	if !existent {
		dc.setDDLLabelForTopSQL(jobID, jobQuery)
		dc.setDDLSourceForDiagnosis(jobID, jobType)
		jobCtx = dc.jobContext(jobID)
		dc.backfillCtx.jobCtxMap[jobID] = jobCtx
	}
	return jobCtx, existent
}

type reorgContexts struct {
	sync.RWMutex
	// reorgCtxMap maps job ID to reorg context.
	reorgCtxMap map[int64]*reorgCtx
}

func (dc *ddlCtx) getReorgCtx(jobID int64) *reorgCtx {
	dc.reorgCtx.RLock()
	defer dc.reorgCtx.RUnlock()
	return dc.reorgCtx.reorgCtxMap[jobID]
}

func (dc *ddlCtx) newReorgCtx(jobID int64, rowCount int64) *reorgCtx {
	dc.reorgCtx.Lock()
	defer dc.reorgCtx.Unlock()
	existedRC, ok := dc.reorgCtx.reorgCtxMap[jobID]
	if ok {
		existedRC.references.Add(1)
		return existedRC
	}
	rc := &reorgCtx{}
	rc.doneCh = make(chan error, 1)
	// initial reorgCtx
	rc.setRowCount(rowCount)
	rc.mu.warnings = make(map[errors.ErrorID]*terror.Error)
	rc.mu.warningsCount = make(map[errors.ErrorID]int64)
	rc.references.Add(1)
	dc.reorgCtx.reorgCtxMap[jobID] = rc
	return rc
}

func (dc *ddlCtx) removeReorgCtx(jobID int64) {
	dc.reorgCtx.Lock()
	defer dc.reorgCtx.Unlock()
	ctx, ok := dc.reorgCtx.reorgCtxMap[jobID]
	if ok {
		ctx.references.Sub(1)
		if ctx.references.Load() == 0 {
			delete(dc.reorgCtx.reorgCtxMap, jobID)
		}
	}
}

func (dc *ddlCtx) notifyReorgWorkerJobStateChange(job *model.Job) {
	rc := dc.getReorgCtx(job.ID)
	if rc == nil {
		return
	}
	rc.notifyJobState(job.State)
}

// EnableTiFlashPoll enables TiFlash poll loop aka PollTiFlashReplicaStatus.
func EnableTiFlashPoll(d interface{}) {
	if dd, ok := d.(*ddl); ok {
		dd.enableTiFlashPoll.Store(true)
	}
}

// DisableTiFlashPoll disables TiFlash poll loop aka PollTiFlashReplicaStatus.
func DisableTiFlashPoll(d interface{}) {
	if dd, ok := d.(*ddl); ok {
		dd.enableTiFlashPoll.Store(false)
	}
}

// IsTiFlashPollEnabled reveals enableTiFlashPoll
func (d *ddl) IsTiFlashPollEnabled() bool {
	return d.enableTiFlashPoll.Load()
}

// RegisterStatsHandle registers statistics handle and its corresponding even channel for ddl.
func (d *ddl) RegisterStatsHandle(h *handle.Handle) {
	d.ddlCtx.statsHandle = h
	d.ddlEventCh = h.DDLEventCh()
}

// asyncNotifyEvent will notify the ddl event to outside world, say statistic handle. When the channel is full, we may
// give up notify and log it.
func asyncNotifyEvent(d *ddlCtx, e *util.Event) {
	if d.ddlEventCh != nil {
		if d.lease == 0 {
			// If lease is 0, it's always used in test.
			select {
			case d.ddlEventCh <- e:
			default:
			}
			return
		}
		for i := 0; i < 10; i++ {
			select {
			case d.ddlEventCh <- e:
				return
			default:
				time.Sleep(time.Microsecond * 10)
			}
		}
		logutil.BgLogger().Warn("[ddl] fail to notify DDL event", zap.String("event", e.String()))
	}
}

// NewDDL creates a new DDL.
func NewDDL(ctx context.Context, options ...Option) DDL {
	return newDDL(ctx, options...)
}

func newDDL(ctx context.Context, options ...Option) *ddl {
	opt := &Options{
		Hook: &BaseCallback{},
	}
	for _, o := range options {
		o(opt)
	}

	id := uuid.New().String()
	var manager owner.Manager
	var schemaSyncer syncer.SchemaSyncer
	var stateSyncer syncer.StateSyncer
	var deadLockCkr util.DeadTableLockChecker
	if etcdCli := opt.EtcdCli; etcdCli == nil {
		// The etcdCli is nil if the store is localstore which is only used for testing.
		// So we use mockOwnerManager and MockSchemaSyncer.
		manager = owner.NewMockManager(ctx, id)
		schemaSyncer = NewMockSchemaSyncer()
		stateSyncer = NewMockStateSyncer()
	} else {
		manager = owner.NewOwnerManager(ctx, etcdCli, ddlPrompt, id, DDLOwnerKey)
		schemaSyncer = syncer.NewSchemaSyncer(etcdCli, id)
		stateSyncer = syncer.NewStateSyncer(etcdCli, util.ServerGlobalState)
		deadLockCkr = util.NewDeadTableLockChecker(etcdCli)
	}

	// TODO: make store and infoCache explicit arguments
	// these two should be ensured to exist
	if opt.Store == nil {
		panic("store should not be nil")
	}
	if opt.InfoCache == nil {
		panic("infoCache should not be nil")
	}

	ddlCtx := &ddlCtx{
		uuid:                       id,
		store:                      opt.Store,
		lease:                      opt.Lease,
		ddlJobDoneCh:               make(chan struct{}, 1),
		ownerManager:               manager,
		schemaSyncer:               schemaSyncer,
		stateSyncer:                stateSyncer,
		binlogCli:                  binloginfo.GetPumpsClient(),
		infoCache:                  opt.InfoCache,
		tableLockCkr:               deadLockCkr,
		etcdCli:                    opt.EtcdCli,
		schemaVersionManager:       newSchemaVersionManager(),
		waitSchemaSyncedController: newWaitSchemaSyncedController(),
		runningJobIDs:              make([]string, 0, jobRecordCapacity),
	}
	ddlCtx.reorgCtx.reorgCtxMap = make(map[int64]*reorgCtx)
	ddlCtx.jobCtx.jobCtxMap = make(map[int64]*JobContext)
	ddlCtx.mu.hook = opt.Hook
	ddlCtx.mu.interceptor = &BaseInterceptor{}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	ddlCtx.ctx, ddlCtx.cancel = context.WithCancel(ctx)
	ddlCtx.runningJobs.ids = make(map[int64]struct{})

	d := &ddl{
		ddlCtx:            ddlCtx,
		limitJobCh:        make(chan *limitJobTask, batchAddingJobs),
		enableTiFlashPoll: atomicutil.NewBool(true),
		ddlJobCh:          make(chan struct{}, 100),
	}

	scheduler.RegisterSchedulerConstructor("backfill",
		func(taskMeta []byte, step int64) (scheduler.Scheduler, error) {
			return NewBackfillSchedulerHandle(taskMeta, d)
		})

	dispatcher.RegisterTaskFlowHandle(BackfillTaskType, NewLitBackfillFlowHandle(d))
	scheduler.RegisterSubtaskExectorConstructor(BackfillTaskType, func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
		return &BackFillSubtaskExecutor{
			Task: minimalTask,
		}, nil
	})

	// Register functions for enable/disable ddl when changing system variable `tidb_enable_ddl`.
	variable.EnableDDL = d.EnableDDL
	variable.DisableDDL = d.DisableDDL
	variable.SwitchMDL = d.SwitchMDL

	return d
}

// Stop implements DDL.Stop interface.
func (d *ddl) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()
	logutil.BgLogger().Info("[ddl] stop DDL", zap.String("ID", d.uuid))
	return nil
}

func (d *ddl) newDeleteRangeManager(mock bool) delRangeManager {
	var delRangeMgr delRangeManager
	if !mock {
		delRangeMgr = newDelRangeManager(d.store, d.sessPool)
		logutil.BgLogger().Info("[ddl] start delRangeManager OK", zap.Bool("is a emulator", !d.store.SupportDeleteRange()))
	} else {
		delRangeMgr = newMockDelRangeManager()
	}

	delRangeMgr.start()
	return delRangeMgr
}

func (d *ddl) prepareWorkers4ConcurrencyDDL() {
	workerFactory := func(tp workerType) func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newWorker(d.ctx, tp, d.sessPool, d.delRangeMgr, d.ddlCtx)
			sessForJob, err := d.sessPool.Get()
			if err != nil {
				return nil, err
			}
			sessForJob.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
			wk.sess = sess.NewSession(sessForJob)
			metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDDL, wk.String())).Inc()
			return wk, nil
		}
	}
	// reorg worker count at least 1 at most 10.
	reorgCnt := mathutil.Min(mathutil.Max(runtime.GOMAXPROCS(0)/4, 1), reorgWorkerCnt)
	d.reorgWorkerPool = newDDLWorkerPool(pools.NewResourcePool(workerFactory(addIdxWorker), reorgCnt, reorgCnt, 0), reorg)
	d.generalDDLWorkerPool = newDDLWorkerPool(pools.NewResourcePool(workerFactory(generalWorker), generalWorkerCnt, generalWorkerCnt, 0), general)
	failpoint.Inject("NoDDLDispatchLoop", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return()
		}
	})
	d.wg.Run(d.startDispatchLoop)
}

// Start implements DDL.Start interface.
func (d *ddl) Start(ctxPool *pools.ResourcePool) error {
	logutil.BgLogger().Info("[ddl] start DDL", zap.String("ID", d.uuid), zap.Bool("runWorker", config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()))

	d.wg.Run(d.limitDDLJobs)
	d.sessPool = sess.NewSessionPool(ctxPool, d.store)
	d.ownerManager.SetBeOwnerHook(func() {
		var err error
		d.ddlSeqNumMu.seqNum, err = d.GetNextDDLSeqNum()
		if err != nil {
			logutil.BgLogger().Error("error when getting the ddl history count", zap.Error(err))
		}
	})

	d.delRangeMgr = d.newDeleteRangeManager(ctxPool == nil)

	if err := d.stateSyncer.Init(d.ctx); err != nil {
		logutil.BgLogger().Warn("[ddl] start DDL init state syncer failed", zap.Error(err))
		return errors.Trace(err)
	}

	d.prepareWorkers4ConcurrencyDDL()

	if config.TableLockEnabled() {
		d.wg.Add(1)
		go d.startCleanDeadTableLock()
	}

	// If tidb_enable_ddl is true, we need campaign owner and do DDL jobs. Besides, we also can do backfill jobs.
	// Otherwise, we needn't do that.
	if config.GetGlobalConfig().Instance.TiDBEnableDDL.Load() {
		if err := d.EnableDDL(); err != nil {
			return err
		}
	}

	variable.RegisterStatistics(d)

	metrics.DDLCounter.WithLabelValues(metrics.CreateDDLInstance).Inc()

	// Start some background routine to manage TiFlash replica.
	d.wg.Run(d.PollTiFlashRoutine)

	ingest.InitGlobalLightningEnv()

	return nil
}

// EnableDDL enable this node to execute ddl.
// Since ownerManager.CampaignOwner will start a new goroutine to run ownerManager.campaignLoop,
// we should make sure that before invoking EnableDDL(), ddl is DISABLE.
func (d *ddl) EnableDDL() error {
	err := d.ownerManager.CampaignOwner()
	return errors.Trace(err)
}

// DisableDDL disable this node to execute ddl.
// We should make sure that before invoking DisableDDL(), ddl is ENABLE.
func (d *ddl) DisableDDL() error {
	if d.ownerManager.IsOwner() {
		// If there is only one node, we should NOT disable ddl.
		serverInfo, err := infosync.GetAllServerInfo(d.ctx)
		if err != nil {
			logutil.BgLogger().Error("[ddl] error when GetAllServerInfo", zap.Error(err))
			return err
		}
		if len(serverInfo) <= 1 {
			return dbterror.ErrDDLSetting.GenWithStackByArgs("disabling", "can not disable ddl owner when it is the only one tidb instance")
		}
		// FIXME: if possible, when this node is the only node with DDL, ths setting of DisableDDL should fail.
	}

	// disable campaign by interrupting campaignLoop
	d.ownerManager.CampaignCancel()
	return nil
}

// GetNextDDLSeqNum return the next DDL seq num.
func (d *ddl) GetNextDDLSeqNum() (uint64, error) {
	var count uint64
	ctx := kv.WithInternalSourceType(d.ctx, kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		count, err = t.GetHistoryDDLCount()
		return err
	})
	return count, err
}

func (d *ddl) close() {
	if isChanClosed(d.ctx.Done()) {
		return
	}

	startTime := time.Now()
	d.cancel()
	d.wg.Wait()
	d.ownerManager.Cancel()
	d.schemaSyncer.Close()
	if d.reorgWorkerPool != nil {
		d.reorgWorkerPool.close()
	}
	if d.generalDDLWorkerPool != nil {
		d.generalDDLWorkerPool.close()
	}

	// d.delRangeMgr using sessions from d.sessPool.
	// Put it before d.sessPool.close to reduce the time spent by d.sessPool.close.
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}
	if d.sessPool != nil {
		d.sessPool.Close()
	}
	variable.UnregisterStatistics(d)

	logutil.BgLogger().Info("[ddl] DDL closed", zap.String("ID", d.uuid), zap.Duration("take time", time.Since(startTime)))
}

// GetLease implements DDL.GetLease interface.
func (d *ddl) GetLease() time.Duration {
	lease := d.lease
	return lease
}

// GetInfoSchemaWithInterceptor gets the infoschema binding to d. It's exported for testing.
// Please don't use this function, it is used by TestParallelDDLBeforeRunDDLJob to intercept the calling of d.infoHandle.Get(), use d.infoHandle.Get() instead.
// Otherwise, the TestParallelDDLBeforeRunDDLJob will hang up forever.
func (d *ddl) GetInfoSchemaWithInterceptor(ctx sessionctx.Context) infoschema.InfoSchema {
	is := d.infoCache.GetLatest()

	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mu.interceptor.OnGetInfoSchema(ctx, is)
}

func (d *ddl) genGlobalIDs(count int) ([]int64, error) {
	var ret []int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		failpoint.Inject("mockGenGlobalIDFail", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("gofail genGlobalIDs error"))
			}
		})

		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenGlobalIDs(count)
		return err
	})

	return ret, err
}

func (d *ddl) genPlacementPolicyID() (int64, error) {
	var ret int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenPlacementPolicyID()
		return err
	})

	return ret, err
}

// SchemaSyncer implements DDL.SchemaSyncer interface.
func (d *ddl) SchemaSyncer() syncer.SchemaSyncer {
	return d.schemaSyncer
}

// StateSyncer implements DDL.StateSyncer interface.
func (d *ddl) StateSyncer() syncer.StateSyncer {
	return d.stateSyncer
}

// OwnerManager implements DDL.OwnerManager interface.
func (d *ddl) OwnerManager() owner.Manager {
	return d.ownerManager
}

// GetID implements DDL.GetID interface.
func (d *ddl) GetID() string {
	return d.uuid
}

var (
	fastDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
	}
	normalDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	slowDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		1 * time.Second,
		3 * time.Second,
	}
)

func getIntervalFromPolicy(policy []time.Duration, i int) (time.Duration, bool) {
	plen := len(policy)
	if i < plen {
		return policy[i], true
	}
	return policy[plen-1], false
}

func getJobCheckInterval(job *model.Job, i int) (time.Duration, bool) {
	switch job.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey, model.ActionModifyColumn,
		model.ActionReorganizePartition:
		return getIntervalFromPolicy(slowDDLIntervalPolicy, i)
	case model.ActionCreateTable, model.ActionCreateSchema:
		return getIntervalFromPolicy(fastDDLIntervalPolicy, i)
	default:
		return getIntervalFromPolicy(normalDDLIntervalPolicy, i)
	}
}

func (dc *ddlCtx) asyncNotifyWorker(ch chan struct{}, etcdPath string, jobID int64, jobType string) {
	// If the workers don't run, we needn't notify workers.
	// TODO: It does not affect informing the backfill worker.
	if !config.GetGlobalConfig().Instance.TiDBEnableDDL.Load() {
		return
	}
	if dc.isOwner() {
		asyncNotify(ch)
	} else {
		dc.asyncNotifyByEtcd(etcdPath, jobID, jobType)
	}
}

func updateTickerInterval(ticker *time.Ticker, lease time.Duration, job *model.Job, i int) *time.Ticker {
	interval, changed := getJobCheckInterval(job, i)
	if !changed {
		return ticker
	}
	// For now we should stop old ticker and create a new ticker
	ticker.Stop()
	return time.NewTicker(chooseLeaseTime(lease, interval))
}

func recordLastDDLInfo(ctx sessionctx.Context, job *model.Job) {
	if job == nil {
		return
	}
	ctx.GetSessionVars().LastDDLInfo.Query = job.Query
	ctx.GetSessionVars().LastDDLInfo.SeqNum = job.SeqNum
}

func setDDLJobQuery(ctx sessionctx.Context, job *model.Job) {
	switch job.Type {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionUnlockTable:
		job.Query = ""
	default:
		job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	}
}

// DoDDLJob will return
// - nil: found in history DDL job and no job error
// - context.Cancel: job has been sent to worker, but not found in history DDL job before cancel
// - other: found in history DDL job and return that job error
func (d *ddl) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	if mci := ctx.GetSessionVars().StmtCtx.MultiSchemaInfo; mci != nil {
		// In multiple schema change, we don't run the job.
		// Instead, we merge all the jobs into one pending job.
		return appendToSubJobs(mci, job)
	}
	// Get a global job ID and put the DDL job in the queue.
	setDDLJobQuery(ctx, job)
	task := &limitJobTask{job, make(chan error), nil}
	d.limitJobCh <- task

	failpoint.Inject("mockParallelSameDDLJobTwice", func(val failpoint.Value) {
		if val.(bool) {
			<-task.err
			// The same job will be put to the DDL queue twice.
			job = job.Clone()
			task1 := &limitJobTask{job, make(chan error), nil}
			d.limitJobCh <- task1
			// The second job result is used for test.
			task = task1
		}
	})

	// worker should restart to continue handling tasks in limitJobCh, and send back through task.err
	err := <-task.err
	if err != nil {
		// The transaction of enqueuing job is failed.
		return errors.Trace(err)
	}

	sessVars := ctx.GetSessionVars()
	sessVars.StmtCtx.IsDDLJobInQueue = true

	// Notice worker that we push a new job and wait the job done.
	d.asyncNotifyWorker(d.ddlJobCh, addingDDLJobConcurrent, job.ID, job.Type.String())
	logutil.BgLogger().Info("[ddl] start DDL job", zap.String("job", job.String()), zap.String("query", job.Query))

	var historyJob *model.Job
	jobID := job.ID

	// Attach the context of the jobId to the calling session so that
	// KILL can cancel this DDL job.
	ctx.GetSessionVars().StmtCtx.DDLJobID = jobID

	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	initInterval, _ := getJobCheckInterval(job, 0)
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, initInterval))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(job.Type.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		recordLastDDLInfo(ctx, historyJob)
	}()
	i := 0
	for {
		failpoint.Inject("storeCloseInLoop", func(_ failpoint.Value) {
			_ = d.Stop()
		})

		select {
		case <-d.ddlJobDoneCh:
		case <-ticker.C:
			i++
			ticker = updateTickerInterval(ticker, 10*d.lease, job, i)
		case <-d.ctx.Done():
			logutil.BgLogger().Info("[ddl] DoDDLJob will quit because context done")
			return context.Canceled
		}

		// If the connection being killed, we need to CANCEL the DDL job.
		if atomic.LoadUint32(&sessVars.Killed) == 1 {
			if atomic.LoadInt32(&sessVars.ConnectionStatus) == variable.ConnStatusShutdown {
				logutil.BgLogger().Info("[ddl] DoDDLJob will quit because context done")
				return context.Canceled
			}
			if sessVars.StmtCtx.DDLJobID != 0 {
				se, err := d.sessPool.Get()
				if err != nil {
					logutil.BgLogger().Error("[ddl] get session failed, check again", zap.Error(err))
					continue
				}
				sessVars.StmtCtx.DDLJobID = 0 // Avoid repeat.
				errs, err := CancelJobsBySystem(se, []int64{jobID})
				d.sessPool.Put(se)
				if len(errs) > 0 {
					logutil.BgLogger().Warn("error canceling DDL job", zap.Error(errs[0]))
				}
				if err != nil {
					logutil.BgLogger().Warn("Kill command could not cancel DDL job", zap.Error(err))
					continue
				}
			}
		}

		se, err := d.sessPool.Get()
		if err != nil {
			logutil.BgLogger().Error("[ddl] get session failed, check again", zap.Error(err))
			continue
		}
		historyJob, err = GetHistoryJobByID(se, jobID)
		d.sessPool.Put(se)
		if err != nil {
			logutil.BgLogger().Error("[ddl] get history DDL job failed, check again", zap.Error(err))
			continue
		}
		if historyJob == nil {
			logutil.BgLogger().Debug("[ddl] DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		d.checkHistoryJobInTest(ctx, historyJob)

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			// Judge whether there are some warnings when executing DDL under the certain SQL mode.
			if historyJob.ReorgMeta != nil && len(historyJob.ReorgMeta.Warnings) != 0 {
				if len(historyJob.ReorgMeta.Warnings) != len(historyJob.ReorgMeta.WarningsCount) {
					logutil.BgLogger().Info("[ddl] DDL warnings doesn't match the warnings count", zap.Int64("jobID", jobID))
				} else {
					for key, warning := range historyJob.ReorgMeta.Warnings {
						keyCount := historyJob.ReorgMeta.WarningsCount[key]
						if keyCount == 1 {
							ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
						} else {
							newMsg := fmt.Sprintf("%d warnings with this error code, first warning: "+warning.GetMsg(), keyCount)
							newWarning := dbterror.ClassTypes.Synthesize(terror.ErrCode(warning.Code()), newMsg)
							ctx.GetSessionVars().StmtCtx.AppendWarning(newWarning)
						}
					}
				}
			}
			appendMultiChangeWarningsToOwnerCtx(ctx, historyJob)

			logutil.BgLogger().Info("[ddl] DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			logutil.BgLogger().Info("[ddl] DDL job is failed", zap.Int64("jobID", jobID))
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

func (d *ddl) callHookOnChanged(job *model.Job, err error) error {
	if job.State == model.JobStateNone {
		// We don't call the hook if the job haven't run yet.
		return err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	err = d.mu.hook.OnChanged(err)
	return errors.Trace(err)
}

// SetBinlogClient implements DDL.SetBinlogClient interface.
func (d *ddl) SetBinlogClient(binlogCli *pumpcli.PumpsClient) {
	d.binlogCli = binlogCli
}

// GetHook implements DDL.GetHook interface.
func (d *ddl) GetHook() Callback {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.mu.hook
}

// SetHook set the customized hook.
func (d *ddl) SetHook(h Callback) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.hook = h
}

func (d *ddl) startCleanDeadTableLock() {
	defer func() {
		tidbutil.Recover(metrics.LabelDDL, "startCleanDeadTableLock", nil, false)
		d.wg.Done()
	}()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !d.ownerManager.IsOwner() {
				continue
			}
			deadLockTables, err := d.tableLockCkr.GetDeadLockedTables(d.ctx, d.infoCache.GetLatest().AllSchemas())
			if err != nil {
				logutil.BgLogger().Info("[ddl] get dead table lock failed.", zap.Error(err))
				continue
			}
			for se, tables := range deadLockTables {
				err := d.CleanDeadTableLock(tables, se)
				if err != nil {
					logutil.BgLogger().Info("[ddl] clean dead table lock failed.", zap.Error(err))
				}
			}
		case <-d.ctx.Done():
			return
		}
	}
}

// SwitchMDL enables MDL or disable MDL.
func (d *ddl) SwitchMDL(enable bool) error {
	isEnableBefore := variable.EnableMDL.Load()
	if isEnableBefore == enable {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	// Check if there is any DDL running.
	// This check can not cover every corner cases, so users need to guarantee that there is no DDL running by themselves.
	sessCtx, err := d.sessPool.Get()
	if err != nil {
		return err
	}
	defer d.sessPool.Put(sessCtx)
	se := sess.NewSession(sessCtx)
	rows, err := se.Execute(ctx, "select 1 from mysql.tidb_ddl_job", "check job")
	if err != nil {
		return err
	}
	if len(rows) != 0 {
		return errors.New("please wait for all jobs done")
	}

	variable.EnableMDL.Store(enable)
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		oldEnable, _, err := m.GetMetadataLock()
		if err != nil {
			return err
		}
		if oldEnable != enable {
			err = m.SetMetadataLock(enable)
		}
		return err
	})
	if err != nil {
		logutil.BgLogger().Warn("[ddl] switch metadata lock feature", zap.Bool("enable", enable), zap.Error(err))
		return err
	}
	logutil.BgLogger().Info("[ddl] switch metadata lock feature", zap.Bool("enable", enable))
	return nil
}

// RecoverInfo contains information needed by DDL.RecoverTable.
type RecoverInfo struct {
	SchemaID      int64
	TableInfo     *model.TableInfo
	DropJobID     int64
	SnapshotTS    uint64
	AutoIDs       meta.AutoIDGroup
	OldSchemaName string
	OldTableName  string
}

// RecoverSchemaInfo contains information needed by DDL.RecoverSchema.
type RecoverSchemaInfo struct {
	*model.DBInfo
	RecoverTabsInfo []*RecoverInfo
	DropJobID       int64
	SnapshotTS      uint64
	OldSchemaName   model.CIStr
}

// delayForAsyncCommit sleeps `SafeWindow + AllowedClockDrift` before a DDL job finishes.
// It should be called before any DDL that could break data consistency.
// This provides a safe window for async commit and 1PC to commit with an old schema.
func delayForAsyncCommit() {
	if variable.EnableMDL.Load() {
		// If metadata lock is enabled. The transaction of DDL must begin after prewrite of the async commit transaction,
		// then the commit ts of DDL must be greater than the async commit transaction. In this case, the corresponding schema of the async commit transaction
		// is correct. But if metadata lock is disabled, we can't ensure that the corresponding schema of the async commit transaction isn't change.
		return
	}
	cfg := config.GetGlobalConfig().TiKVClient.AsyncCommit
	duration := cfg.SafeWindow + cfg.AllowedClockDrift
	logutil.BgLogger().Info("sleep before DDL finishes to make async commit and 1PC safe",
		zap.Duration("duration", duration))
	time.Sleep(duration)
}

var (
	// RunInGoTest is used to identify whether ddl in running in the test.
	RunInGoTest bool
)

// GetDropOrTruncateTableInfoFromJobsByStore implements GetDropOrTruncateTableInfoFromJobs
func GetDropOrTruncateTableInfoFromJobsByStore(jobs []*model.Job, gcSafePoint uint64, getTable func(uint64, int64, int64) (*model.TableInfo, error), fn func(*model.Job, *model.TableInfo) (bool, error)) (bool, error) {
	for _, job := range jobs {
		// Check GC safe point for getting snapshot infoSchema.
		err := gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return false, err
		}
		if job.Type != model.ActionDropTable && job.Type != model.ActionTruncateTable {
			continue
		}

		tbl, err := getTable(job.StartTS, job.SchemaID, job.TableID)
		if err != nil {
			if meta.ErrDBNotExists.Equal(err) {
				// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
				// then can't find the table from the snapshot info-schema. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropTable.
				continue
			}
			return false, err
		}
		if tbl == nil {
			// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
			// then can't find the table from the snapshot info-schema. Should just ignore error here,
			// see more in TestParallelDropSchemaAndDropTable.
			continue
		}
		finish, err := fn(job, tbl)
		if err != nil || finish {
			return finish, err
		}
	}
	return false, nil
}

// Info is for DDL information.
type Info struct {
	SchemaVer   int64
	ReorgHandle kv.Key       // It's only used for DDL information.
	Jobs        []*model.Job // It's the currently running jobs.
}

// GetDDLInfoWithNewTxn returns DDL information using a new txn.
func GetDDLInfoWithNewTxn(s sessionctx.Context) (*Info, error) {
	se := sess.NewSession(s)
	err := se.Begin()
	if err != nil {
		return nil, err
	}
	info, err := GetDDLInfo(s)
	se.Rollback()
	return info, err
}

// GetDDLInfo returns DDL information.
func GetDDLInfo(s sessionctx.Context) (*Info, error) {
	var err error
	info := &Info{}
	se := sess.NewSession(s)
	txn, err := se.Txn()
	if err != nil {
		return nil, errors.Trace(err)
	}
	t := meta.NewMeta(txn)
	info.Jobs = make([]*model.Job, 0, 2)
	var generalJob, reorgJob *model.Job
	generalJob, reorgJob, err = get2JobsFromTable(se)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if generalJob != nil {
		info.Jobs = append(info.Jobs, generalJob)
	}

	if reorgJob != nil {
		info.Jobs = append(info.Jobs, reorgJob)
	}

	info.SchemaVer, err = t.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if reorgJob == nil {
		return info, nil
	}

	_, info.ReorgHandle, _, _, err = newReorgHandler(se).GetDDLReorgHandle(reorgJob)
	if err != nil {
		if meta.ErrDDLReorgElementNotExist.Equal(err) {
			return info, nil
		}
		return nil, errors.Trace(err)
	}

	return info, nil
}

func get2JobsFromTable(sess *sess.Session) (*model.Job, *model.Job, error) {
	var generalJob, reorgJob *model.Job
	jobs, err := getJobsBySQL(sess, JobTable, "not reorg order by job_id limit 1")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if len(jobs) != 0 {
		generalJob = jobs[0]
	}
	jobs, err = getJobsBySQL(sess, JobTable, "reorg order by job_id limit 1")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if len(jobs) != 0 {
		reorgJob = jobs[0]
	}
	return generalJob, reorgJob, nil
}

// cancelRunningJob cancel a DDL job that is in the concurrent state.
func cancelRunningJob(sess *sess.Session, job *model.Job,
	byWho model.AdminCommandOperator) (err error) {
	// These states can't be cancelled.
	if job.IsDone() || job.IsSynced() {
		return dbterror.ErrCancelFinishedDDLJob.GenWithStackByArgs(job.ID)
	}

	// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
	if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
		return nil
	}

	if !job.IsRollbackable() {
		return dbterror.ErrCannotCancelDDLJob.GenWithStackByArgs(job.ID)
	}
	job.State = model.JobStateCancelling
	job.AdminOperator = byWho

	// Make sure RawArgs isn't overwritten.
	return json.Unmarshal(job.RawArgs, &job.Args)
}

// pauseRunningJob check and pause the running Job
func pauseRunningJob(sess *sess.Session, job *model.Job,
	byWho model.AdminCommandOperator) (err error) {
	if job.IsPausing() || job.IsPaused() {
		return dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
	}
	if !job.IsPausable() {
		errMsg := fmt.Sprintf("state [%s] or schema state [%s]", job.State.String(), job.SchemaState.String())
		err = dbterror.ErrCannotPauseDDLJob.GenWithStackByArgs(job.ID, errMsg)
		if err != nil {
			return err
		}
	}

	job.State = model.JobStatePausing
	job.AdminOperator = byWho

	if job.RawArgs == nil {
		return nil
	}

	return json.Unmarshal(job.RawArgs, &job.Args)
}

// resumePausedJob check and resume the Paused Job
func resumePausedJob(se *sess.Session, job *model.Job,
	byWho model.AdminCommandOperator) (err error) {
	if !job.IsResumable() {
		errMsg := fmt.Sprintf("job has not been paused, job state:%s, schema state:%s",
			job.State, job.SchemaState)
		return dbterror.ErrCannotResumeDDLJob.GenWithStackByArgs(job.ID, errMsg)
	}
	// The Paused job should only be resumed by who paused it
	if job.AdminOperator != byWho {
		errMsg := fmt.Sprintf("job has been paused by [%s], should not resumed by [%s]",
			job.AdminOperator.String(), byWho.String())
		return dbterror.ErrCannotResumeDDLJob.GenWithStackByArgs(job.ID, errMsg)
	}

	job.State = model.JobStateQueueing

	return json.Unmarshal(job.RawArgs, &job.Args)
}

// processJobs command on the Job according to the process
func processJobs(process func(*sess.Session, *model.Job, model.AdminCommandOperator) (err error),
	sessCtx sessionctx.Context,
	ids []int64,
	byWho model.AdminCommandOperator) (jobErrs []error, err error) {
	failpoint.Inject("mockFailedCommandOnConcurencyDDL", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock failed admin command on ddl jobs"))
		}
	})

	if len(ids) == 0 {
		return nil, nil
	}

	ns := sess.NewSession(sessCtx)
	// We should process (and try) all the jobs in one Transaction.
	for tryN := uint(0); tryN < 3; tryN += 1 {
		jobErrs = make([]error, len(ids))
		// Need to figure out which one could not be paused
		jobMap := make(map[int64]int, len(ids))
		idsStr := make([]string, 0, len(ids))
		for idx, id := range ids {
			jobMap[id] = idx
			idsStr = append(idsStr, strconv.FormatInt(id, 10))
		}

		err = ns.Begin()
		if err != nil {
			return nil, err
		}
		jobs, err := getJobsBySQL(ns, JobTable, fmt.Sprintf("job_id in (%s) order by job_id", strings.Join(idsStr, ", ")))
		if err != nil {
			ns.Rollback()
			return nil, err
		}

		for _, job := range jobs {
			i, ok := jobMap[job.ID]
			if !ok {
				logutil.BgLogger().Debug("Job ID from meta is not consistent with requested job id,",
					zap.Int64("fetched job ID", job.ID))
				jobErrs[i] = dbterror.ErrInvalidDDLJob.GenWithStackByArgs(job.ID)
				continue
			}
			delete(jobMap, job.ID)

			err = process(ns, job, byWho)
			if err != nil {
				jobErrs[i] = err
				continue
			}

			err = updateDDLJob2Table(ns, job, true)
			if err != nil {
				jobErrs[i] = err
				continue
			}
		}

		failpoint.Inject("mockCommitFailedOnDDLCommand", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(jobErrs, errors.New("mock commit failed on admin command on ddl jobs"))
			}
		})

		// There may be some conflict during the update, try it again
		if err = ns.Commit(); err != nil {
			continue
		}

		for id, idx := range jobMap {
			jobErrs[idx] = dbterror.ErrDDLJobNotFound.GenWithStackByArgs(id)
		}

		return jobErrs, nil
	}

	return jobErrs, err
}

// CancelJobs cancels the DDL jobs according to user command.
func CancelJobs(se sessionctx.Context, ids []int64) (errs []error, err error) {
	return processJobs(cancelRunningJob, se, ids, model.AdminCommandByEndUser)
}

// PauseJobs pause all the DDL jobs according to user command.
func PauseJobs(se sessionctx.Context, ids []int64) ([]error, error) {
	return processJobs(pauseRunningJob, se, ids, model.AdminCommandByEndUser)
}

// ResumeJobs resume all the DDL jobs according to user command.
func ResumeJobs(se sessionctx.Context, ids []int64) ([]error, error) {
	return processJobs(resumePausedJob, se, ids, model.AdminCommandByEndUser)
}

// CancelJobsBySystem cancels Jobs because of internal reasons.
func CancelJobsBySystem(se sessionctx.Context, ids []int64) (errs []error, err error) {
	return processJobs(cancelRunningJob, se, ids, model.AdminCommandBySystem)
}

// PauseJobsBySystem pauses Jobs because of internal reasons.
func PauseJobsBySystem(se sessionctx.Context, ids []int64) (errs []error, err error) {
	return processJobs(pauseRunningJob, se, ids, model.AdminCommandBySystem)
}

// ResumeJobsBySystem resumes Jobs that are paused by TiDB itself.
func ResumeJobsBySystem(se sessionctx.Context, ids []int64) (errs []error, err error) {
	return processJobs(resumePausedJob, se, ids, model.AdminCommandBySystem)
}

// pprocessAllJobs processes all the jobs in the job table, 100 jobs at a time in case of high memory usage.
func processAllJobs(process func(*sess.Session, *model.Job, model.AdminCommandOperator) (err error),
	se sessionctx.Context, byWho model.AdminCommandOperator) (map[int64]error, error) {
	var err error
	var jobErrs = make(map[int64]error)

	ns := sess.NewSession(se)
	err = ns.Begin()
	if err != nil {
		return nil, err
	}

	var jobID int64 = 0
	var jobIDMax int64 = 0
	var limit int = 100
	for {
		var jobs []*model.Job
		jobs, err = getJobsBySQL(ns, JobTable,
			fmt.Sprintf("job_id >= %s order by job_id asc limit %s",
				strconv.FormatInt(jobID, 10),
				strconv.FormatInt(int64(limit), 10)))
		if err != nil {
			ns.Rollback()
			return nil, err
		}

		for _, job := range jobs {
			err = process(ns, job, byWho)
			if err != nil {
				jobErrs[job.ID] = err
				continue
			}

			err = updateDDLJob2Table(ns, job, true)
			if err != nil {
				jobErrs[job.ID] = err
				continue
			}
		}

		// Just in case the job ID is not sequential
		if len(jobs) > 0 && jobs[len(jobs)-1].ID > jobIDMax {
			jobIDMax = jobs[len(jobs)-1].ID
		}

		// If rows returned is smaller than $limit, then there is no more records
		if len(jobs) < limit {
			break
		}

		jobID = jobIDMax + 1
	}

	err = ns.Commit()
	if err != nil {
		return nil, err
	}
	return jobErrs, nil
}

// PauseAllJobsBySystem pauses all running Jobs because of internal reasons.
func PauseAllJobsBySystem(se sessionctx.Context) (map[int64]error, error) {
	return processAllJobs(pauseRunningJob, se, model.AdminCommandBySystem)
}

// ResumeAllJobsBySystem resumes all paused Jobs because of internal reasons.
func ResumeAllJobsBySystem(se sessionctx.Context) (map[int64]error, error) {
	return processAllJobs(resumePausedJob, se, model.AdminCommandBySystem)
}

// GetAllDDLJobs get all DDL jobs and sorts jobs by job.ID.
func GetAllDDLJobs(se sessionctx.Context, t *meta.Meta) ([]*model.Job, error) {
	return getJobsBySQL(sess.NewSession(se), JobTable, "1 order by job_id")
}

// DefNumHistoryJobs is default value of the default number of history job
const DefNumHistoryJobs = 10

const batchNumHistoryJobs = 128

// GetLastNHistoryDDLJobs returns the DDL history jobs and an error.
// The maximum count of history jobs is num.
func GetLastNHistoryDDLJobs(t *meta.Meta, maxNumJobs int) ([]*model.Job, error) {
	iterator, err := GetLastHistoryDDLJobsIterator(t)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iterator.GetLastJobs(maxNumJobs, nil)
}

// IterHistoryDDLJobs iterates history DDL jobs until the `finishFn` return true or error.
func IterHistoryDDLJobs(txn kv.Transaction, finishFn func([]*model.Job) (bool, error)) error {
	txnMeta := meta.NewMeta(txn)
	iter, err := GetLastHistoryDDLJobsIterator(txnMeta)
	if err != nil {
		return err
	}
	cacheJobs := make([]*model.Job, 0, DefNumHistoryJobs)
	for {
		cacheJobs, err = iter.GetLastJobs(DefNumHistoryJobs, cacheJobs)
		if err != nil || len(cacheJobs) == 0 {
			return err
		}
		finish, err := finishFn(cacheJobs)
		if err != nil || finish {
			return err
		}
	}
}

// IterAllDDLJobs will iterates running DDL jobs first, return directly if `finishFn` return true or error,
// then iterates history DDL jobs until the `finishFn` return true or error.
func IterAllDDLJobs(ctx sessionctx.Context, txn kv.Transaction, finishFn func([]*model.Job) (bool, error)) error {
	jobs, err := GetAllDDLJobs(ctx, meta.NewMeta(txn))
	if err != nil {
		return err
	}

	finish, err := finishFn(jobs)
	if err != nil || finish {
		return err
	}
	return IterHistoryDDLJobs(txn, finishFn)
}

// GetLastHistoryDDLJobsIterator gets latest N history DDL jobs iterator.
func GetLastHistoryDDLJobsIterator(m *meta.Meta) (meta.LastJobIterator, error) {
	return m.GetLastHistoryDDLJobsIterator()
}

// GetAllHistoryDDLJobs get all the done DDL jobs.
func GetAllHistoryDDLJobs(m *meta.Meta) ([]*model.Job, error) {
	iterator, err := GetLastHistoryDDLJobsIterator(m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allJobs := make([]*model.Job, 0, batchNumHistoryJobs)
	for {
		jobs, err := iterator.GetLastJobs(batchNumHistoryJobs, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allJobs = append(allJobs, jobs...)
		if len(jobs) < batchNumHistoryJobs {
			break
		}
	}
	// sort job.
	slices.SortFunc(allJobs, func(i, j *model.Job) bool {
		return i.ID < j.ID
	})
	return allJobs, nil
}

// ScanHistoryDDLJobs get some of the done DDL jobs.
// When the DDL history is quite large, GetAllHistoryDDLJobs() API can't work well, because it makes the server OOM.
// The result is in descending order by job ID.
func ScanHistoryDDLJobs(m *meta.Meta, startJobID int64, limit int) ([]*model.Job, error) {
	var iter meta.LastJobIterator
	var err error
	if startJobID == 0 {
		iter, err = m.GetLastHistoryDDLJobsIterator()
	} else {
		if limit == 0 {
			return nil, errors.New("when 'start_job_id' is specified, it must work with a 'limit'")
		}
		iter, err = m.GetHistoryDDLJobsIterator(startJobID)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iter.GetLastJobs(limit, nil)
}

// GetHistoryJobByID return history DDL job by ID.
func GetHistoryJobByID(sess sessionctx.Context, id int64) (*model.Job, error) {
	err := sessiontxn.NewTxn(context.Background(), sess)
	if err != nil {
		return nil, err
	}
	defer func() {
		// we can ignore the commit error because this txn is readonly.
		_ = sess.CommitTxn(context.Background())
	}()
	txn, err := sess.Txn(true)
	if err != nil {
		return nil, err
	}
	t := meta.NewMeta(txn)
	job, err := t.GetHistoryDDLJob(id)
	return job, errors.Trace(err)
}

// AddHistoryDDLJob record the history job.
func AddHistoryDDLJob(sess *sess.Session, t *meta.Meta, job *model.Job, updateRawArgs bool) error {
	err := addHistoryDDLJob2Table(sess, job, updateRawArgs)
	if err != nil {
		logutil.BgLogger().Info("[ddl] failed to add DDL job to history table", zap.Error(err))
	}
	// we always add history DDL job to job list at this moment.
	return t.AddHistoryDDLJob(job, updateRawArgs)
}

// addHistoryDDLJob2Table adds DDL job to history table.
func addHistoryDDLJob2Table(sess *sess.Session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	_, err = sess.Execute(context.Background(),
		fmt.Sprintf("insert ignore into mysql.tidb_ddl_history(job_id, job_meta, db_name, table_name, schema_ids, table_ids, create_time) values (%d, %s, %s, %s, %s, %s, %v)",
			job.ID, wrapKey2String(b), strconv.Quote(job.SchemaName), strconv.Quote(job.TableName),
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)),
			strconv.Quote(strconv.FormatInt(job.TableID, 10)),
			strconv.Quote(model.TSConvert2Time(job.StartTS).String())),
		"insert_history")
	return errors.Trace(err)
}
