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
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/ddl/testargsv1"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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
	// Prompt is the prompt for ddl owner manager.
	Prompt = "ddl"

	batchAddingJobs = 100

	reorgWorkerCnt   = 10
	generalWorkerCnt = 10
)

const (
	// The recoverCheckFlag is used to judge the gc work status when RecoverTable/RecoverSchema.
	recoverCheckFlagNone int64 = iota
	recoverCheckFlagEnableGC
	recoverCheckFlagDisableGC
)

var (
	jobV2FirstVer        = *semver.New("8.4.0")
	detectJobVerInterval = 10 * time.Second
)

// StartMode is an enum type for the start mode of the DDL.
type StartMode string

const (
	// Normal mode, cluster is in normal state.
	Normal StartMode = "normal"
	// Bootstrap mode, cluster is during bootstrap.
	Bootstrap StartMode = "bootstrap"
	// Upgrade mode, cluster is during upgrade, we will force current node to be
	// the DDL owner, to make sure all upgrade related DDLs are run on new version
	// TiDB instance.
	Upgrade StartMode = "upgrade"
	// BR mode, Start DDL from br, with this mode can skip loadSystemStore in next-gen and initLogBackup.
	BR StartMode = "br"
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
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	// Start campaigns the owner and starts workers.
	// ctxPool is used for the worker's delRangeManager and creates sessions.
	Start(startMode StartMode, ctxPool *pools.ResourcePool) error
	// Stats returns the DDL statistics.
	Stats(vars *variable.SessionVars) (map[string]any, error)
	// GetScope gets the status variables scope.
	GetScope(status string) vardef.ScopeFlag
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
		JobArgs:     &model.EmptyArgs{},
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

// FillArgsWithSubJobs fill args for job and its sub jobs
func (jobW *JobWrapper) FillArgsWithSubJobs() {
	if jobW.Type != model.ActionMultiSchemaChange {
		jobW.FillArgs(jobW.JobArgs)
	} else {
		for _, sub := range jobW.MultiSchemaInfo.SubJobs {
			sub.FillArgs(jobW.Version)
		}
	}
}

// NotifyResult notifies the job submit result.
func (jobW *JobWrapper) NotifyResult(err error) {
	merged := len(jobW.ResultCh) > 1
	for _, resultCh := range jobW.ResultCh {
		resultCh <- jobSubmitResult{
			err:    err,
			jobID:  jobW.ID,
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
	eventPublishStore notifier.Store

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
	lease        time.Duration // lease is schema lease, default 45s, see config.Lease.
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

func (sv *schemaVersionManager) setSchemaVersion(jobCtx *jobContext, job *model.Job) (schemaVersion int64, err error) {
	err = sv.lockSchemaVersion(jobCtx, job.ID)
	if err != nil {
		return schemaVersion, errors.Trace(err)
	}
	// TODO we can merge this txn into job transaction to avoid schema version
	//  without differ.
	start := time.Now()
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), sv.store, true, func(_ context.Context, txn kv.Transaction) error {
		var err error
		m := meta.NewMutator(txn)
		schemaVersion, err = m.GenSchemaVersion()
		return err
	})
	defer func() {
		metrics.DDLIncrSchemaVerOpHist.Observe(time.Since(start).Seconds())
	}()
	return schemaVersion, err
}

// lockSchemaVersion gets the lock to prevent the schema version from being updated.
func (sv *schemaVersionManager) lockSchemaVersion(jobCtx *jobContext, jobID int64) error {
	ownerID := sv.lockOwner.Load()
	// There may exist one job update schema version many times in multiple-schema-change, so we do not lock here again
	// if they are the same job.
	if ownerID != jobID {
		start := time.Now()
		sv.schemaVersionMu.Lock()
		defer func() {
			metrics.DDLLockSchemaVerOpHist.Observe(time.Since(start).Seconds())
		}()
		jobCtx.lockStartTime = time.Now()
		sv.lockOwner.Store(jobID)
	}
	return nil
}

// unlockSchemaVersion releases the lock.
func (sv *schemaVersionManager) unlockSchemaVersion(jobCtx *jobContext, jobID int64) {
	ownerID := sv.lockOwner.Load()
	if ownerID == jobID {
		sv.lockOwner.Store(0)
		sv.schemaVersionMu.Unlock()
		metrics.DDLLockVerDurationHist.Observe(time.Since(jobCtx.lockStartTime).Seconds())
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

const noSubJob int64 = -1 // noSubJob indicates the event is not a merged ddl.

// asyncNotifyEvent will notify the ddl event to outside world, say statistic handle. When the channel is full, we may
// give up notify and log it.
// subJobID is used to identify the sub job in a merged ddl, such as create tables, should pass noSubJob(-1) if not a merged ddl.
func asyncNotifyEvent(jobCtx *jobContext, e *notifier.SchemaChangeEvent, job *model.Job, subJobID int64, sctx *sess.Session) error {
	// skip notify for system databases, system databases are expected to change at
	// bootstrap and other nodes can also handle the changing in its bootstrap rather
	// than be notified.
	if metadef.IsMemOrSysDB(job.SchemaName) {
		return nil
	}

	// In test environments, we use a channel-based approach to handle DDL events.
	// This maintains compatibility with existing test cases that expect events to be delivered through channels.
	// In production, DDL events are handled by the notifier system instead.
	if intest.InTest {
		ch := jobCtx.oldDDLCtx.ddlEventCh
		if ch != nil {
		forLoop:
			// Try sending the event to the channel with a backoff strategy to avoid blocking indefinitely.
			// Since most unit tests don't consume events, we make a few attempts and then give up rather
			// than blocking the DDL job forever on a full channel.
			for range 3 {
				select {
				case ch <- e:
					break forLoop
				default:
					time.Sleep(time.Microsecond * 2)
				}
			}
			logutil.DDLLogger().Warn("fail to notify DDL event", zap.Stringer("event", e))
		}
	}

	intest.Assert(jobCtx.eventPublishStore != nil, "eventPublishStore should not be nil")
	failpoint.Inject("asyncNotifyEventError", func() {
		failpoint.Return(errors.New("mock publish event error"))
	})
	if subJobID == noSubJob && job.MultiSchemaInfo != nil {
		subJobID = int64(job.MultiSchemaInfo.Seq)
	}
	err := notifier.PubSchemeChangeToStore(
		jobCtx.stepCtx,
		sctx,
		job.ID,
		subJobID,
		e,
		jobCtx.eventPublishStore,
	)
	if err != nil {
		logutil.DDLLogger().Error("Error publish schema change event",
			zap.Int64("jobID", job.ID),
			zap.Int64("subJobID", subJobID),
			zap.String("event", e.String()), zap.Error(err))
		return err
	}
	return nil
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

	var id string
	var manager owner.Manager
	var schemaVerSyncer schemaver.Syncer
	var serverStateSyncer serverstate.Syncer
	var deadLockCkr util.DeadTableLockChecker
	if etcdCli := opt.EtcdCli; etcdCli == nil {
		id = uuid.New().String()
		// The etcdCli is nil if the store is localstore which is only used for testing.
		// So we use mockOwnerManager and memSyncer.
		manager = owner.NewMockManager(ctx, id, opt.Store, DDLOwnerKey)
		schemaVerSyncer = schemaver.NewMemSyncer()
		serverStateSyncer = serverstate.NewMemSyncer()
	} else {
		ownerMgr := getOwnerManager(opt.Store)
		id = ownerMgr.ID()
		manager = ownerMgr.OwnerManager()
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
		eventPublishStore: opt.EventPublishStore,
	}

	taskexecutor.RegisterTaskType(proto.Backfill,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			return newBackfillDistExecutor(ctx, task, param, d)
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
func (d *ddl) Start(startMode StartMode, ctxPool *pools.ResourcePool) error {
	if kerneltype.IsClassic() {
		d.detectAndUpdateJobVersion()
	}
	campaignOwner := config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()
	if startMode == Upgrade {
		if !campaignOwner {
			return errors.New("DDL must be enabled when upgrading")
		}

		logutil.DDLLogger().Info("DDL is in upgrade mode, force to be owner")
		if err := d.ownerManager.ForceToBeOwner(d.ctx); err != nil {
			return errors.Trace(err)
		}
	}
	logutil.DDLLogger().Info("start DDL", zap.String("ID", d.uuid),
		zap.Bool("runWorker", campaignOwner),
		zap.Stringer("jobVersion", model.GetJobVerInUse()),
		zap.String("startMode", string(startMode)),
	)

	d.executor.startMode = startMode
	failpoint.Inject("mockBRStartMode", func() {
		d.executor.startMode = BR
	})

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
	if campaignOwner {
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

