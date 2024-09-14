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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	pumpcli "github.com/pingcap/tidb/pkg/tidb-binlog/pump_client"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/intest"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// DDLOwnerKey is the ddl owner path that is saved to etcd, and it's exported for testing.
	DDLOwnerKey             = "/tidb/ddl/fg/owner"
	ddlSchemaVersionKeyLock = "/tidb/ddl/schema_version_lock"
	// addingDDLJobPrefix is the path prefix used to record the newly added DDL job, and it's saved to etcd.
	addingDDLJobPrefix = "/tidb/ddl/add_ddl_job_"
	ddlPrompt          = "ddl"

	batchAddingJobs = 100

	reorgWorkerCnt   = 10
	generalWorkerCnt = 10

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

// CreateTableConfig is the configuration of `CreateTableWithInfo`.
type CreateTableConfig struct {
	OnExist OnExist
	// IDAllocated indicates whether the job has allocated all IDs for tables affected
	// in the job, if true, DDL will not allocate IDs for them again, it's only used
	// by BR now. By reusing IDs BR can save a lot of works such as rewriting table
	// IDs in backed up KVs.
	IDAllocated bool
}

// CreateTableOption is the option for creating table.
type CreateTableOption func(*CreateTableConfig)

// GetCreateTableConfig applies the series of config options from default config
// and returns the final config.
func GetCreateTableConfig(cs []CreateTableOption) CreateTableConfig {
	cfg := CreateTableConfig{}
	for _, c := range cs {
		c(&cfg)
	}
	return cfg
}

// WithOnExist applies the OnExist option.
func WithOnExist(o OnExist) CreateTableOption {
	return func(cfg *CreateTableConfig) {
		cfg.OnExist = o
	}
}

// WithIDAllocated applies the IDAllocated option.
// WARNING!!!: if idAllocated == true, DDL will NOT allocate IDs by itself. That
// means if the caller can not promise ID is unique, then we got inconsistency.
// This option is only exposed to be used by BR.
func WithIDAllocated(idAllocated bool) CreateTableOption {
	return func(cfg *CreateTableConfig) {
		cfg.IDAllocated = idAllocated
	}
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
	jobOnceCapacity   = 1000
)

var (
	// EnableSplitTableRegion is a flag to decide whether to split a new region for
	// a newly created table. It takes effect only if the Storage supports split
	// region.
	EnableSplitTableRegion = uint32(0)
	// GetPumpClient is used to get the pump client.
	// It's exported for testing.
	GetPumpClient = binloginfo.GetPumpsClient
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	// Start campaigns the owner and starts workers.
	// ctxPool is used for the worker's delRangeManager and creates sessions.
	Start(ctxPool *pools.ResourcePool) error
	// Stats returns the DDL statistics.
	Stats(vars *variable.SessionVars) (map[string]any, error)
	// GetScope gets the status variables scope.
	GetScope(status string) variable.ScopeFlag
	// Stop stops DDL worker.
	Stop() error
	// RegisterStatsHandle registers statistics handle and its corresponding event channel for ddl.
	RegisterStatsHandle(*handle.Handle)
	// SchemaSyncer gets the schema syncer.
	SchemaSyncer() schemaver.Syncer
	// StateSyncer gets the cluster state syncer.
	StateSyncer() serverstate.Syncer
	// OwnerManager gets the owner manager.
	OwnerManager() owner.Manager
	// GetID gets the ddl ID.
	GetID() string
	// GetMinJobIDRefresher gets the MinJobIDRefresher, this api only works after Start.
	GetMinJobIDRefresher() *systable.MinJobIDRefresher
}

type jobSubmitResult struct {
	err   error
	jobID int64
	// merged indicates whether the job is merged into another job together with
	// other jobs. we only merge multiple create table jobs into one job when fast
	// create table is enabled.
	merged bool
}

// JobWrapper is used to wrap a job and some other information.
// exported for testing.
type JobWrapper struct {
	*model.Job
	// IDAllocated see config of same name in CreateTableConfig.
	// exported for test.
	IDAllocated bool
	JobArgs     model.JobArgs
	// job submission is run in async, we use this channel to notify the caller.
	// when fast create table enabled, we might combine multiple jobs into one, and
	// append the channel to this slice.
	ResultCh []chan jobSubmitResult
	cacheErr error
}

// NewJobWrapper creates a new JobWrapper.
// exported for testing.
func NewJobWrapper(job *model.Job, idAllocated bool) *JobWrapper {
	return &JobWrapper{
		Job:         job,
		IDAllocated: idAllocated,
		ResultCh:    []chan jobSubmitResult{make(chan jobSubmitResult)},
	}
}

// NewJobWrapperWithArgs creates a new JobWrapper with job args.
// TODO: merge with NewJobWrapper later.
func NewJobWrapperWithArgs(job *model.Job, args model.JobArgs, idAllocated bool) *JobWrapper {
	return &JobWrapper{
		Job:         job,
		IDAllocated: idAllocated,
		JobArgs:     args,
		ResultCh:    []chan jobSubmitResult{make(chan jobSubmitResult)},
	}
}

// NotifyResult notifies the job submit result.
func (t *JobWrapper) NotifyResult(err error) {
	merged := len(t.ResultCh) > 1
	for _, resultCh := range t.ResultCh {
		resultCh <- jobSubmitResult{
			err:    err,
			jobID:  t.ID,
			merged: merged,
		}
	}
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m  sync.RWMutex
	wg tidbutil.WaitGroupWrapper // It's only used to deal with data race in restart_test.

	*ddlCtx
	sessPool          *sess.Pool
	delRangeMgr       delRangeManager
	enableTiFlashPoll *atomicutil.Bool
	sysTblMgr         systable.Manager
	minJobIDRefresher *systable.MinJobIDRefresher

	executor     *executor
	jobSubmitter *JobSubmitter
}

// unSyncedJobTracker is to track whether changes of a DDL job are synced to all
// TiDB instances.
type unSyncedJobTracker struct {
	mu           sync.RWMutex
	unSyncedJobs map[int64]struct{}

	// Use to check if the DDL job is the first run on this owner.
	onceMap map[int64]struct{}
}

func newUnSyncedJobTracker() *unSyncedJobTracker {
	return &unSyncedJobTracker{
		unSyncedJobs: make(map[int64]struct{}, jobRecordCapacity),
		onceMap:      make(map[int64]struct{}, jobOnceCapacity),
	}
}

func (w *unSyncedJobTracker) addUnSynced(jobID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.unSyncedJobs[jobID] = struct{}{}
}

func (w *unSyncedJobTracker) isUnSynced(jobID int64) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.unSyncedJobs[jobID]
	return ok
}

func (w *unSyncedJobTracker) removeUnSynced(jobID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.unSyncedJobs, jobID)
}

// maybeAlreadyRunOnce returns true means that the job may be the first run on this owner.
// Returns false means that the job must not be the first run on this owner.
func (w *unSyncedJobTracker) maybeAlreadyRunOnce(id int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.onceMap[id]
	return ok
}

func (w *unSyncedJobTracker) setAlreadyRunOnce(id int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.onceMap) > jobOnceCapacity {
		// If the map is too large, we reset it. These jobs may need to check schema synced again, but it's ok.
		w.onceMap = make(map[int64]struct{}, jobRecordCapacity)
	}
	w.onceMap[id] = struct{}{}
}

// ddlCtx is the context when we use worker to handle DDL jobs.
type ddlCtx struct {
	ctx               context.Context
	cancel            context.CancelFunc
	uuid              string
	store             kv.Storage
	ownerManager      owner.Manager
	schemaVerSyncer   schemaver.Syncer
	serverStateSyncer serverstate.Syncer

	ddlEventCh   chan<- *notifier.SchemaChangeEvent
	lease        time.Duration        // lease is schema lease, default 45s, see config.Lease.
	binlogCli    *pumpcli.PumpsClient // binlogCli is used for Binlog.
	infoCache    *infoschema.InfoCache
	statsHandle  *handle.Handle
	tableLockCkr util.DeadTableLockChecker
	etcdCli      *clientv3.Client
	autoidCli    *autoid.ClientDiscover
	schemaLoader SchemaLoader

	// reorgCtx is used for reorganization.
	reorgCtx reorgContexts

	jobCtx struct {
		sync.RWMutex
		// jobCtxMap maps job ID to job's ctx.
		jobCtxMap map[int64]*ReorgContext
	}
}

// SchemaLoader is used to reload info schema, the only impl is domain currently.
type SchemaLoader interface {
	Reload() error
}

// schemaVersionManager is used to manage the schema version. info schema cache
// only load differences between its version and current version, so we must
// make sure increment version & set differ run in a single transaction. we are
// using memory lock to make sure this right now, as we are using optimistic
// transaction here, and there will be many write conflict if we allow those
// transactions run in parallel. we can change this to lock TiKV key inside the
// transaction later.
type schemaVersionManager struct {
	schemaVersionMu sync.Mutex
	// lockOwner stores the job ID that is holding the lock.
	lockOwner atomicutil.Int64
	store     kv.Storage
}

func newSchemaVersionManager(store kv.Storage) *schemaVersionManager {
	return &schemaVersionManager{
		store: store,
	}
}

func (sv *schemaVersionManager) setSchemaVersion(job *model.Job) (schemaVersion int64, err error) {
	err = sv.lockSchemaVersion(job.ID)
	if err != nil {
		return schemaVersion, errors.Trace(err)
	}
	// TODO we can merge this txn into job transaction to avoid schema version
	//  without differ.
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), sv.store, true, func(_ context.Context, txn kv.Transaction) error {
		var err error
		m := meta.NewMeta(txn)
		schemaVersion, err = m.GenSchemaVersion()
		return err
	})
	return schemaVersion, err
}

// lockSchemaVersion gets the lock to prevent the schema version from being updated.
func (sv *schemaVersionManager) lockSchemaVersion(jobID int64) error {
	ownerID := sv.lockOwner.Load()
	// There may exist one job update schema version many times in multiple-schema-change, so we do not lock here again
	// if they are the same job.
	if ownerID != jobID {
		sv.schemaVersionMu.Lock()
		sv.lockOwner.Store(jobID)
	}
	return nil
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
	logutil.DDLLogger().Debug("check whether is the DDL owner", zap.Bool("isOwner", isOwner), zap.String("selfID", dc.uuid))
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
		ctx = NewReorgContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.setDDLLabelForTopSQL(jobQuery)
}

func (dc *ddlCtx) setDDLSourceForDiagnosis(jobID int64, jobType model.ActionType) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewReorgContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.setDDLLabelForDiagnosis(jobType)
}

func (dc *ddlCtx) getResourceGroupTaggerForTopSQL(jobID int64) *kv.ResourceGroupTagBuilder {
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

func (dc *ddlCtx) jobContext(jobID int64, reorgMeta *model.DDLReorgMeta) *ReorgContext {
	dc.jobCtx.RLock()
	defer dc.jobCtx.RUnlock()
	var ctx *ReorgContext
	if jobContext, exists := dc.jobCtx.jobCtxMap[jobID]; exists {
		ctx = jobContext
	} else {
		ctx = NewReorgContext()
	}
	if reorgMeta != nil && len(ctx.resourceGroupName) == 0 {
		ctx.resourceGroupName = reorgMeta.ResourceGroupName
	}
	return ctx
}

type reorgContexts struct {
	sync.RWMutex
	// reorgCtxMap maps job ID to reorg context.
	reorgCtxMap map[int64]*reorgCtx
	beOwnerTS   int64
}

func (r *reorgContexts) getOwnerTS() int64 {
	r.RLock()
	defer r.RUnlock()
	return r.beOwnerTS
}

func (r *reorgContexts) setOwnerTS(ts int64) {
	r.Lock()
	r.beOwnerTS = ts
	r.Unlock()
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
	rc.doneCh = make(chan reorgFnResult, 1)
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
		logutil.DDLLogger().Warn("cannot find reorgCtx", zap.Int64("Job ID", job.ID))
		return
	}
	logutil.DDLLogger().Info("notify reorg worker the job's state",
		zap.Int64("Job ID", job.ID), zap.Stringer("Job State", job.State),
		zap.Stringer("Schema State", job.SchemaState))
	rc.notifyJobState(job.State)
}

// EnableTiFlashPoll enables TiFlash poll loop aka PollTiFlashReplicaStatus.
func EnableTiFlashPoll(d any) {
	if dd, ok := d.(*ddl); ok {
		dd.enableTiFlashPoll.Store(true)
	}
}

// DisableTiFlashPoll disables TiFlash poll loop aka PollTiFlashReplicaStatus.
func DisableTiFlashPoll(d any) {
	if dd, ok := d.(*ddl); ok {
		dd.enableTiFlashPoll.Store(false)
	}
}

// IsTiFlashPollEnabled reveals enableTiFlashPoll
func (d *ddl) IsTiFlashPollEnabled() bool {
	return d.enableTiFlashPoll.Load()
}

// RegisterStatsHandle registers statistics handle and its corresponding even channel for ddl.
// TODO this is called after ddl started, will cause panic if related DDL are executed
// in between.
func (d *ddl) RegisterStatsHandle(h *handle.Handle) {
	d.ddlCtx.statsHandle = h
	d.executor.statsHandle = h
	d.ddlEventCh = h.DDLEventCh()
}

// asyncNotifyEvent will notify the ddl event to outside world, say statistic handle. When the channel is full, we may
// give up notify and log it.
func asyncNotifyEvent(jobCtx *jobContext, e *notifier.SchemaChangeEvent, job *model.Job) {
	// skip notify for system databases, system databases are expected to change at
	// bootstrap and other nodes can also handle the changing in its bootstrap rather
	// than be notified.
	if tidbutil.IsMemOrSysDB(job.SchemaName) {
		return
	}

	ch := jobCtx.oldDDLCtx.ddlEventCh
	if ch != nil {
		for i := 0; i < 10; i++ {
			select {
			case ch <- e:
				return
			default:
				time.Sleep(time.Microsecond * 10)
			}
		}
		logutil.DDLLogger().Warn("fail to notify DDL event", zap.Stringer("event", e))
	}
}

// NewDDL creates a new DDL.
// TODO remove it, to simplify this PR we use this way.
func NewDDL(ctx context.Context, options ...Option) (DDL, Executor) {
	return newDDL(ctx, options...)
}

func newDDL(ctx context.Context, options ...Option) (*ddl, *executor) {
	opt := &Options{}
	for _, o := range options {
		o(opt)
	}

	id := uuid.New().String()
	var manager owner.Manager
	var schemaVerSyncer schemaver.Syncer
	var serverStateSyncer serverstate.Syncer
	var deadLockCkr util.DeadTableLockChecker
	if etcdCli := opt.EtcdCli; etcdCli == nil {
		// The etcdCli is nil if the store is localstore which is only used for testing.
		// So we use mockOwnerManager and memSyncer.
		manager = owner.NewMockManager(ctx, id, opt.Store, DDLOwnerKey)
		schemaVerSyncer = schemaver.NewMemSyncer()
		serverStateSyncer = serverstate.NewMemSyncer()
	} else {
		manager = owner.NewOwnerManager(ctx, etcdCli, ddlPrompt, id, DDLOwnerKey)
		schemaVerSyncer = schemaver.NewEtcdSyncer(etcdCli, id)
		serverStateSyncer = serverstate.NewEtcdSyncer(etcdCli, util.ServerGlobalState)
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
		uuid:              id,
		store:             opt.Store,
		lease:             opt.Lease,
		ownerManager:      manager,
		schemaVerSyncer:   schemaVerSyncer,
		serverStateSyncer: serverStateSyncer,
		binlogCli:         GetPumpClient(),
		infoCache:         opt.InfoCache,
		tableLockCkr:      deadLockCkr,
		etcdCli:           opt.EtcdCli,
		autoidCli:         opt.AutoIDClient,
		schemaLoader:      opt.SchemaLoader,
	}
	ddlCtx.reorgCtx.reorgCtxMap = make(map[int64]*reorgCtx)
	ddlCtx.jobCtx.jobCtxMap = make(map[int64]*ReorgContext)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	ddlCtx.ctx, ddlCtx.cancel = context.WithCancel(ctx)

	d := &ddl{
		ddlCtx:            ddlCtx,
		enableTiFlashPoll: atomicutil.NewBool(true),
	}

	taskexecutor.RegisterTaskType(proto.Backfill,
		func(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
			return newBackfillDistExecutor(ctx, id, task, taskTable, d)
		},
	)

	scheduler.RegisterSchedulerFactory(proto.Backfill,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			return newLitBackfillScheduler(ctx, d, task, param)
		})
	scheduler.RegisterSchedulerCleanUpFactory(proto.Backfill, newBackfillCleanUpS3)
	// Register functions for enable/disable ddl when changing system variable `tidb_enable_ddl`.
	variable.EnableDDL = d.EnableDDL
	variable.DisableDDL = d.DisableDDL
	variable.SwitchMDL = d.SwitchMDL

	ddlJobDoneChMap := generic.NewSyncMap[int64, chan struct{}](10)
	limitJobCh := make(chan *JobWrapper, batchAddingJobs)

	submitter := &JobSubmitter{
		ctx:               d.ctx,
		etcdCli:           d.etcdCli,
		ownerManager:      d.ownerManager,
		store:             d.store,
		serverStateSyncer: d.serverStateSyncer,
		ddlJobDoneChMap:   &ddlJobDoneChMap,

		limitJobCh:     limitJobCh,
		ddlJobNotifyCh: make(chan struct{}, 100),
	}
	d.jobSubmitter = submitter

	e := &executor{
		ctx:             d.ctx,
		uuid:            d.uuid,
		store:           d.store,
		autoidCli:       d.autoidCli,
		infoCache:       d.infoCache,
		limitJobCh:      limitJobCh,
		lease:           d.lease,
		ddlJobDoneChMap: &ddlJobDoneChMap,
	}
	d.executor = e

	return d, e
}

// Stop implements DDL.Stop interface.
func (d *ddl) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()
	logutil.DDLLogger().Info("stop DDL", zap.String("ID", d.uuid))
	return nil
}

func (d *ddl) newDeleteRangeManager(mock bool) delRangeManager {
	var delRangeMgr delRangeManager
	if !mock {
		delRangeMgr = newDelRangeManager(d.store, d.sessPool)
		logutil.DDLLogger().Info("start delRangeManager OK", zap.Bool("is a emulator", !d.store.SupportDeleteRange()))
	} else {
		delRangeMgr = newMockDelRangeManager()
	}

	delRangeMgr.start()
	return delRangeMgr
}

// Start implements DDL.Start interface.
func (d *ddl) Start(ctxPool *pools.ResourcePool) error {
	// if we are running in test, random choose a job version to run with.
	// TODO add a separate CI flow to run with different job version, so we can cover
	// more cases in a single run.
	if intest.InTest || config.GetGlobalConfig().Store == "unistore" {
		jobVer := model.JobVersion1
		// 50% percent to use JobVersion2 in test.
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		if rnd.Intn(2) == 0 {
			jobVer = model.JobVersion2
		}
		model.SetJobVerInUse(jobVer)
	}
	logutil.DDLLogger().Info("start DDL", zap.String("ID", d.uuid),
		zap.Bool("runWorker", config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()),
		zap.Stringer("jobVersion", model.GetJobVerInUse()))

	d.sessPool = sess.NewSessionPool(ctxPool)
	d.executor.sessPool, d.jobSubmitter.sessPool = d.sessPool, d.sessPool
	d.sysTblMgr = systable.NewManager(d.sessPool)
	d.minJobIDRefresher = systable.NewMinJobIDRefresher(d.sysTblMgr)
	d.jobSubmitter.sysTblMgr = d.sysTblMgr
	d.jobSubmitter.minJobIDRefresher = d.minJobIDRefresher
	d.wg.Run(func() {
		d.jobSubmitter.submitLoop()
	})
	d.wg.Run(func() {
		d.minJobIDRefresher.Start(d.ctx)
	})

	d.delRangeMgr = d.newDeleteRangeManager(ctxPool == nil)

	if err := d.serverStateSyncer.Init(d.ctx); err != nil {
		logutil.DDLLogger().Warn("start DDL init state syncer failed", zap.Error(err))
		return errors.Trace(err)
	}
	d.ownerManager.SetListener(&ownerListener{
		ddl:          d,
		jobSubmitter: d.jobSubmitter,
		ddlExecutor:  d.executor,
	})

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

	ingestDataDir, err := ingest.GenIngestTempDataDir()
	if err != nil {
		logutil.DDLIngestLogger().Warn(ingest.LitWarnEnvInitFail,
			zap.Error(err))
	} else {
		ok := ingest.InitGlobalLightningEnv(ingestDataDir)
		if ok {
			d.wg.Run(func() {
				d.CleanUpTempDirLoop(d.ctx, ingestDataDir)
			})
		}
	}

	return nil
}

func (d *ddl) CleanUpTempDirLoop(ctx context.Context, path string) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			se, err := d.sessPool.Get()
			if err != nil {
				logutil.DDLLogger().Warn("get session from pool failed", zap.Error(err))
				return
			}
			ingest.CleanUpTempDir(ctx, se, path)
			d.sessPool.Put(se)
		case <-d.ctx.Done():
			return
		}
	}
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
			logutil.DDLLogger().Error("error when GetAllServerInfo", zap.Error(err))
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

func (d *ddl) close() {
	if d.ctx.Err() != nil {
		return
	}

	startTime := time.Now()
	d.cancel()
	d.wg.Wait()
	d.ownerManager.Cancel()
	d.schemaVerSyncer.Close()

	// d.delRangeMgr using sessions from d.sessPool.
	// Put it before d.sessPool.close to reduce the time spent by d.sessPool.close.
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}
	if d.sessPool != nil {
		d.sessPool.Close()
	}
	variable.UnregisterStatistics(d)

	logutil.DDLLogger().Info("DDL closed", zap.String("ID", d.uuid), zap.Duration("take time", time.Since(startTime)))
}

// SchemaSyncer implements DDL.SchemaSyncer interface.
func (d *ddl) SchemaSyncer() schemaver.Syncer {
	return d.schemaVerSyncer
}

// StateSyncer implements DDL.StateSyncer interface.
func (d *ddl) StateSyncer() serverstate.Syncer {
	return d.serverStateSyncer
}

// OwnerManager implements DDL.OwnerManager interface.
func (d *ddl) OwnerManager() owner.Manager {
	return d.ownerManager
}

// GetID implements DDL.GetID interface.
func (d *ddl) GetID() string {
	return d.uuid
}

func (d *ddl) GetMinJobIDRefresher() *systable.MinJobIDRefresher {
	return d.minJobIDRefresher
}

func (d *ddl) startCleanDeadTableLock() {
	defer func() {
		d.wg.Done()
	}()

	defer tidbutil.Recover(metrics.LabelDDL, "startCleanDeadTableLock", nil, false)

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !d.ownerManager.IsOwner() {
				continue
			}
			deadLockTables, err := d.tableLockCkr.GetDeadLockedTables(d.ctx, d.infoCache.GetLatest())
			if err != nil {
				logutil.DDLLogger().Info("get dead table lock failed.", zap.Error(err))
				continue
			}
			for se, tables := range deadLockTables {
				err := d.cleanDeadTableLock(tables, se)
				if err != nil {
					logutil.DDLLogger().Info("clean dead table lock failed.", zap.Error(err))
				}
			}
		case <-d.ctx.Done():
			return
		}
	}
}

// cleanDeadTableLock uses to clean dead table locks.
func (d *ddl) cleanDeadTableLock(unlockTables []model.TableLockTpInfo, se model.SessionInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	arg := &LockTablesArg{
		UnlockTables: unlockTables,
		SessionInfo:  se,
	}
	job := &model.Job{
		SchemaID:   unlockTables[0].SchemaID,
		TableID:    unlockTables[0].TableID,
		Type:       model.ActionUnlockTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{arg},
	}

	ctx, err := d.sessPool.Get()
	if err != nil {
		return err
	}
	defer d.sessPool.Put(ctx)
	err = d.executor.DoDDLJob(ctx, job)
	return errors.Trace(err)
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
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), d.store, true, func(_ context.Context, txn kv.Transaction) error {
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
		logutil.DDLLogger().Warn("switch metadata lock feature", zap.Bool("enable", enable), zap.Error(err))
		return err
	}
	logutil.DDLLogger().Info("switch metadata lock feature", zap.Bool("enable", enable))
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
	// LoadTablesOnExecute is the new logic to avoid a large RecoverTabsInfo can't be
	// persisted. If it's true, DDL owner will recover RecoverTabsInfo instead of the
	// job submit node.
	LoadTablesOnExecute bool
	DropJobID           int64
	SnapshotTS          uint64
	OldSchemaName       pmodel.CIStr
}

// delayForAsyncCommit sleeps `SafeWindow + AllowedClockDrift` before a DDL job finishes.
// It should be called before any DDL that could break data consistency.
// This provides a safe window for async commit and 1PC to commit with an old schema.
func delayForAsyncCommit() {
	if variable.EnableMDL.Load() {
		// If metadata lock is enabled. The transaction of DDL must begin after
		// pre-write of the async commit transaction, then the commit ts of DDL
		// must be greater than the async commit transaction. In this case, the
		// corresponding schema of the async commit transaction is correct.
		// suppose we're adding index:
		// - schema state -> StateWriteOnly with version V
		// - some txn T started using async commit and version V,
		//   and T do pre-write before or after V+1
		// - schema state -> StateWriteReorganization with version V+1
		// - T commit finish, with TS
		// - 'wait schema synced' finish
		// - schema state -> Done with version V+2, commit-ts of this
		//   transaction must > TS, so it's safe for T to commit.
		return
	}
	cfg := config.GetGlobalConfig().TiKVClient.AsyncCommit
	duration := cfg.SafeWindow + cfg.AllowedClockDrift
	logutil.DDLLogger().Info("sleep before DDL finishes to make async commit and 1PC safe",
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
	err := se.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	info, err := GetDDLInfo(s)
	se.Rollback()
	return info, err
}

// GetDDLInfo returns DDL information and only uses for testing.
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
func cancelRunningJob(_ *sess.Session, job *model.Job,
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
	return nil
}

// pauseRunningJob check and pause the running Job
func pauseRunningJob(_ *sess.Session, job *model.Job,
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
	return nil
}

// resumePausedJob check and resume the Paused Job
func resumePausedJob(_ *sess.Session, job *model.Job,
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

	return nil
}

// processJobs command on the Job according to the process
func processJobs(
	process func(*sess.Session, *model.Job, model.AdminCommandOperator) (err error),
	sessCtx sessionctx.Context,
	ids []int64,
	byWho model.AdminCommandOperator,
) (jobErrs []error, err error) {
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
	for tryN := uint(0); tryN < 3; tryN++ {
		jobErrs = make([]error, len(ids))
		// Need to figure out which one could not be paused
		jobMap := make(map[int64]int, len(ids))
		idsStr := make([]string, 0, len(ids))
		for idx, id := range ids {
			jobMap[id] = idx
			idsStr = append(idsStr, strconv.FormatInt(id, 10))
		}

		err = ns.Begin(context.Background())
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
				logutil.DDLLogger().Debug("Job ID from meta is not consistent with requested job id,",
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

			err = updateDDLJob2Table(ns, job, false)
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
		if err = ns.Commit(context.Background()); err != nil {
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
	err = ns.Begin(context.Background())
	if err != nil {
		return nil, err
	}

	var jobID int64
	var jobIDMax int64
	var limit = 100
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

			err = updateDDLJob2Table(ns, job, false)
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

	err = ns.Commit(context.Background())
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
func GetAllDDLJobs(se sessionctx.Context) ([]*model.Job, error) {
	return getJobsBySQL(sess.NewSession(se), JobTable, "1 order by job_id")
}

// IterAllDDLJobs will iterates running DDL jobs first, return directly if `finishFn` return true or error,
// then iterates history DDL jobs until the `finishFn` return true or error.
func IterAllDDLJobs(ctx sessionctx.Context, txn kv.Transaction, finishFn func([]*model.Job) (bool, error)) error {
	jobs, err := GetAllDDLJobs(ctx)
	if err != nil {
		return err
	}

	finish, err := finishFn(jobs)
	if err != nil || finish {
		return err
	}
	return IterHistoryDDLJobs(txn, finishFn)
}
