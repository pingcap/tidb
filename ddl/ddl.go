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
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/util"
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
	goutil "github.com/pingcap/tidb/util"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikvrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
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

	// PartitionCountLimit is limit of the number of partitions in a table.
	// Reference linking https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html.
	PartitionCountLimit = 8192
)

// OnExist specifies what to do when a new object has a name collision.
type OnExist uint8

const (
	// OnExistError throws an error on name collision.
	OnExistError OnExist = iota
	// OnExistIgnore skips creating the new object.
	OnExistIgnore
	// OnExistReplace replaces the old object by the new object. This is only
	// supported by VIEWs at the moment. For other object types, this is
	// equivalent to OnExistError.
	OnExistReplace
)

var (
	// EnableSplitTableRegion is a flag to decide whether to split a new region for
	// a newly created table. It takes effect only if the Storage supports split
	// region.
	EnableSplitTableRegion = uint32(0)
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx sessionctx.Context, schema model.CIStr, charsetInfo *ast.CharsetOpt, placementPolicyRef *model.PolicyRefInfo) error
	AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error
	DropSchema(ctx sessionctx.Context, schema model.CIStr) error
	CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error
	CreateView(ctx sessionctx.Context, stmt *ast.CreateViewStmt) error
	DropTable(ctx sessionctx.Context, tableIdent ast.Ident) (err error)
	RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error)
	DropView(ctx sessionctx.Context, tableIdent ast.Ident) (err error)
	CreateIndex(ctx sessionctx.Context, tableIdent ast.Ident, keyType ast.IndexKeyType, indexName model.CIStr,
		columnNames []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error
	DropIndex(ctx sessionctx.Context, tableIdent ast.Ident, indexName model.CIStr, ifExists bool) error
	AlterTable(ctx context.Context, sctx sessionctx.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx sessionctx.Context, oldTableIdent, newTableIdent ast.Ident, isAlterTable bool) error
	RenameTables(ctx sessionctx.Context, oldTableIdent, newTableIdent []ast.Ident, isAlterTable bool) error
	LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error
	UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error
	CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error
	UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error
	RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error
	CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error
	DropSequence(ctx sessionctx.Context, tableIdent ast.Ident, ifExists bool) (err error)
	AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error
	CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error
	DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error
	AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error

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
		onExist OnExist) error

	// BatchCreateTableWithInfo is like CreateTableWithInfo, but can handle multiple tables.
	BatchCreateTableWithInfo(ctx sessionctx.Context,
		schema model.CIStr,
		info []*model.TableInfo,
		onExist OnExist) error

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
	SchemaSyncer() util.SchemaSyncer
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
	// DoDDLJob does the DDL job, it's exported for test.
	DoDDLJob(ctx sessionctx.Context, job *model.Job) error
	// MigrateExistingDDLs move existing DDLs in queue to table.
	MigrateExistingDDLs() error
	// BackOffDDLs move existing DDLs in table to queue.
	BackOffDDLs() error
}

type limitJobTask struct {
	job *model.Job
	err chan error
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m          sync.RWMutex
	wg         tidbutil.WaitGroupWrapper // It's only used to deal with data race in restart_test.
	limitJobCh chan *limitJobTask

	*ddlCtx
	workers           map[workerType]*worker
	sessPool          *sessionPool
	delRangeMgr       delRangeManager
	enableTiFlashPoll *atomicutil.Bool
	// used in the concurrency ddl.
	reorgWorkerPool      *workerPool
	generalDDLWorkerPool *workerPool
	// get notification if any DDL coming.
	ddlJobCh chan struct{}
	// recording the running jobs.
	runningJobs struct {
		sync.RWMutex
		runningJobMap map[int64]struct{}
	}
	// used for build fetch DDL job SQL.
	runningOrBlockedIDs []string
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
		job:  make(map[int64]struct{}, 16),
		once: atomicutil.NewBool(true),
	}
}

func (w *waitSchemaSyncedController) needSync(job *model.Job) {
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
	schemaSyncer util.SchemaSyncer
	ddlJobDoneCh chan struct{}
	ddlEventCh   chan<- *util.Event
	lease        time.Duration        // lease is schema lease.
	binlogCli    *pumpcli.PumpsClient // binlogCli is used for Binlog.
	infoCache    *infoschema.InfoCache
	statsHandle  *handle.Handle
	tableLockCkr util.DeadTableLockChecker
	etcdCli      *clientv3.Client

	concurrentDDL *workerActivityManager
	queueDDL      *workerActivityManager

	*waitSchemaSyncedController
	*schemaVersionManager
	// reorgCtx is used for reorganization.
	reorgCtx struct {
		sync.RWMutex
		// reorgCtxMap maps job ID to reorg context.
		reorgCtxMap map[int64]*reorgCtx
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

type workerActivityManager struct {
	sync.Mutex
	sync.WaitGroup

	b bool
}

func newWorkerActivityManager() *workerActivityManager {
	w := &workerActivityManager{}
	w.Add(1)
	return w
}

func (w *workerActivityManager) active(b bool) {
	w.Lock()
	defer w.Unlock()
	if w.b != b {
		if b {
			w.Done()
		} else {
			w.Add(1)
		}
		w.b = b
	}
}

type schemaVersionManager struct {
	schemaVersionMu    sync.Mutex
	schemaVersionOwner atomicutil.Int64
}

func newSchemaVersionManager() *schemaVersionManager {
	return &schemaVersionManager{}
}

func (sv *schemaVersionManager) SetSchemaVersion(job *model.Job, store kv.Storage) (schemaVersion int64, err error) {
	sv.lockSchemaVersion(job)
	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		var err error
		m := meta.NewMeta(txn)
		schemaVersion, err = m.GenSchemaVersion()
		return err
	})
	if err != nil {
		return 0, err
	}
	return schemaVersion, nil
}

func (sv *schemaVersionManager) lockSchemaVersion(job *model.Job) {
	sv.schemaVersionMu.Lock()
	sv.schemaVersionOwner.Store(job.ID)
}

func (sv *schemaVersionManager) ResetSchemaVersion(job *model.Job) {
	if job == nil {
		return
	}
	ownerID := sv.schemaVersionOwner.Load()
	if ownerID == job.ID {
		sv.schemaVersionOwner.Store(0)
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

func (dc *ddlCtx) setDDLLabelForTopSQL(job *model.Job) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[job.ID]
	if !exists {
		ctx = NewJobContext()
		dc.jobCtx.jobCtxMap[job.ID] = ctx
	}
	ctx.setDDLLabelForTopSQL(job)
}

func (dc *ddlCtx) getResourceGroupTaggerForTopSQL(job *model.Job) tikvrpc.ResourceGroupTagger {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[job.ID]
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

func (dc *ddlCtx) jobContext(job *model.Job) *JobContext {
	dc.jobCtx.RLock()
	defer dc.jobCtx.RUnlock()
	if jobContext, exists := dc.jobCtx.jobCtxMap[job.ID]; exists {
		return jobContext
	}
	return NewJobContext()
}

func (dc *ddlCtx) getReorgCtx(job *model.Job) *reorgCtx {
	dc.reorgCtx.RLock()
	defer dc.reorgCtx.RUnlock()
	return dc.reorgCtx.reorgCtxMap[job.ID]
}

func (dc *ddlCtx) newReorgCtx(r *reorgInfo) *reorgCtx {
	rc := &reorgCtx{}
	rc.doneCh = make(chan error, 1)
	// initial reorgCtx
	rc.setRowCount(r.Job.GetRowCount())
	rc.setNextKey(r.StartKey)
	rc.setCurrentElement(r.currElement)
	rc.mu.warnings = make(map[errors.ErrorID]*terror.Error)
	rc.mu.warningsCount = make(map[errors.ErrorID]int64)
	dc.reorgCtx.Lock()
	defer dc.reorgCtx.Unlock()
	dc.reorgCtx.reorgCtxMap[r.Job.ID] = rc
	return rc
}

func (dc *ddlCtx) removeReorgCtx(job *model.Job) {
	dc.reorgCtx.Lock()
	defer dc.reorgCtx.Unlock()
	delete(dc.reorgCtx.reorgCtxMap, job.ID)
}

func (dc *ddlCtx) notifyReorgCancel(job *model.Job) {
	rc := dc.getReorgCtx(job)
	if rc == nil {
		return
	}
	rc.notifyReorgCancel()
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
	var syncer util.SchemaSyncer
	var deadLockCkr util.DeadTableLockChecker
	if etcdCli := opt.EtcdCli; etcdCli == nil {
		// The etcdCli is nil if the store is localstore which is only used for testing.
		// So we use mockOwnerManager and MockSchemaSyncer.
		manager = owner.NewMockManager(ctx, id)
		syncer = NewMockSchemaSyncer()
	} else {
		manager = owner.NewOwnerManager(ctx, etcdCli, ddlPrompt, id, DDLOwnerKey)
		syncer = util.NewSchemaSyncer(ctx, etcdCli, id, manager)
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
		schemaSyncer:               syncer,
		binlogCli:                  binloginfo.GetPumpsClient(),
		infoCache:                  opt.InfoCache,
		tableLockCkr:               deadLockCkr,
		etcdCli:                    opt.EtcdCli,
		schemaVersionManager:       newSchemaVersionManager(),
		waitSchemaSyncedController: newWaitSchemaSyncedController(),
	}
	ddlCtx.reorgCtx.reorgCtxMap = make(map[int64]*reorgCtx)
	ddlCtx.jobCtx.jobCtxMap = make(map[int64]*JobContext)
	ddlCtx.mu.hook = opt.Hook
	ddlCtx.mu.interceptor = &BaseInterceptor{}
	ddlCtx.ctx, ddlCtx.cancel = context.WithCancel(ctx)
	ddlCtx.concurrentDDL = newWorkerActivityManager()
	ddlCtx.queueDDL = newWorkerActivityManager()
	d := &ddl{
		ddlCtx:              ddlCtx,
		limitJobCh:          make(chan *limitJobTask, batchAddingJobs),
		enableTiFlashPoll:   atomicutil.NewBool(true),
		ddlJobCh:            make(chan struct{}, 100),
		runningOrBlockedIDs: make([]string, 0, 16),
	}
	d.runningJobs.runningJobMap = make(map[int64]struct{})
	d.runningOrBlockedIDs = append(d.runningOrBlockedIDs, "0")

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

func (d *ddl) readyForConcurrencyDDL() {
	workerFactor := func(tp workerType) func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newWorker(d.ctx, tp, d.sessPool, d.delRangeMgr, d.ddlCtx, true)
			sessForJob, err := d.sessPool.get()
			if err != nil {
				return nil, err
			}
			sessForJob.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
			wk.sess = newSession(sessForJob)
			metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDDL, wk.String())).Inc()
			return wk, nil
		}
	}
	d.reorgWorkerPool = newDDLWorkerPool(pools.NewResourcePool(workerFactor(addIdxWorker), batchAddingJobs, batchAddingJobs, 3*time.Minute), reorg)
	d.generalDDLWorkerPool = newDDLWorkerPool(pools.NewResourcePool(workerFactor(generalWorker), 1, 1, 0), general)
	d.wg.Run(d.startDispatchLoop)
}

func (d *ddl) readyForDDL() {
	d.workers = make(map[workerType]*worker, 2)
	d.workers[generalWorker] = newWorker(d.ctx, generalWorker, d.sessPool, d.delRangeMgr, d.ddlCtx, false)
	d.workers[addIdxWorker] = newWorker(d.ctx, addIdxWorker, d.sessPool, d.delRangeMgr, d.ddlCtx, false)
	for _, worker := range d.workers {
		worker.wg.Add(1)
		w := worker
		go w.start(d.ddlCtx)

		metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDDL, worker.String())).Inc()

		// When the start function is called, we will send a fake job to let worker
		// checks owner firstly and try to find whether a job exists and run.
		asyncNotify(worker.ddlJobCh)
	}
}

// Start implements DDL.Start interface.
func (d *ddl) Start(ctxPool *pools.ResourcePool) error {
	logutil.BgLogger().Info("[ddl] start DDL", zap.String("ID", d.uuid), zap.Bool("runWorker", RunWorker))

	d.wg.Run(d.limitDDLJobs)
	d.sessPool = newSessionPool(ctxPool, d.store)
	// If RunWorker is true, we need campaign owner and do DDL job.
	// Otherwise, we needn't do that.
	if RunWorker {
		err := d.ownerManager.CampaignOwner()
		if err != nil {
			return errors.Trace(err)
		}

		d.delRangeMgr = d.newDeleteRangeManager(ctxPool == nil)

		d.watchConcurrentDDLSwitch()
		d.readyForConcurrencyDDL()
		d.readyForDDL()

		d.ddlSeqNumMu.seqNum, err = d.GetHistoryDDLCount()
		if err != nil {
			return err
		}
		go d.schemaSyncer.StartCleanWork()
		if config.TableLockEnabled() {
			d.wg.Add(1)
			go d.startCleanDeadTableLock()
		}
		metrics.DDLCounter.WithLabelValues(metrics.StartCleanWork).Inc()
	}

	variable.RegisterStatistics(d)

	metrics.DDLCounter.WithLabelValues(metrics.CreateDDLInstance).Inc()

	// Start some background routine to manage TiFlash replica.
	d.wg.Run(d.PollTiFlashRoutine)

	return nil
}

// GetHistoryDDLCount the count of done ddl jobs.
func (d *ddl) GetHistoryDDLCount() (count uint64, err error) {
	if variable.EnableConcurrentDDL.Load() {
		ctx, err := d.sessPool.get()
		if err != nil {
			return 0, errors.Trace(err)
		}
		defer d.sessPool.put(ctx)
		rows, err := newSession(ctx).execute(context.Background(), "select count(1) from mysql.tidb_ddl_history", "get_history_ddl_cnt")
		if err != nil {
			return 0, err
		}
		return rows[0].GetUint64(0), nil
	}

	err = kv.RunInNewTxn(d.ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		count, err = t.GetHistoryDDLCount()
		return err
	})
	return
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
	d.reorgWorkerPool.close()
	d.generalDDLWorkerPool.close()

	for _, worker := range d.workers {
		worker.Close()
	}
	// d.delRangeMgr using sessions from d.sessPool.
	// Put it before d.sessPool.close to reduce the time spent by d.sessPool.close.
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}
	d.sessPool.close()
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
	err := kv.RunInNewTxn(context.Background(), d.store, true, func(ctx context.Context, txn kv.Transaction) error {
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
	err := kv.RunInNewTxn(context.Background(), d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenPlacementPolicyID()
		return err
	})

	return ret, err
}

// SchemaSyncer implements DDL.SchemaSyncer interface.
func (d *ddl) SchemaSyncer() util.SchemaSyncer {
	return d.schemaSyncer
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
	case model.ActionAddIndex, model.ActionAddPrimaryKey, model.ActionModifyColumn:
		return getIntervalFromPolicy(slowDDLIntervalPolicy, i)
	case model.ActionCreateTable, model.ActionCreateSchema:
		return getIntervalFromPolicy(fastDDLIntervalPolicy, i)
	default:
		return getIntervalFromPolicy(normalDDLIntervalPolicy, i)
	}
}

func (d *ddl) asyncNotifyWorker(job *model.Job) {
	// If the workers don't run, we needn't notify workers.
	if !RunWorker {
		return
	}
	if variable.EnableConcurrentDDL.Load() {
		key := ""
		if job.MayNeedReorg() {
			key = addingDDLJobReorg
		} else {
			key = addingDDLJobGeneral
		}
		if d.ownerManager.IsOwner() {
			asyncNotify(d.ddlJobCh)
		} else {
			d.asyncNotifyByEtcd(key, job)
		}
	} else {
		var worker *worker
		if job.MayNeedReorg() {
			worker = d.workers[addIdxWorker]
		} else {
			worker = d.workers[generalWorker]
		}
		if d.ownerManager.IsOwner() {
			asyncNotify(worker.ddlJobCh)
		} else {
			d.asyncNotifyByEtcd(worker.addingDDLJobKey, job)
		}
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
	// Get a global job ID and put the DDL job in the queue.
	setDDLJobQuery(ctx, job)
	task := &limitJobTask{job, make(chan error)}
	d.limitJobCh <- task
	// worker should restart to continue handling tasks in limitJobCh, and send back through task.err
	err := <-task.err
	if err != nil {
		// The transaction of enqueuing job is failed.
		return errors.Trace(err)
	}

	ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true

	// Notice worker that we push a new job and wait the job done.
	d.asyncNotifyWorker(job)
	logutil.BgLogger().Info("[ddl] start DDL job", zap.String("job", job.String()), zap.String("query", job.Query))

	var historyJob *model.Job
	jobID := job.ID
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
	sess, err := d.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer d.sessPool.put(sess)
	for {
		failpoint.Inject("storeCloseInLoop", func(_ failpoint.Value) {
			go func() {
				_ = d.Stop()
			}()
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
		historyJob, err = GetHistoryJobByID(sess, jobID)
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
						for j := int64(0); j < historyJob.ReorgMeta.WarningsCount[key]; j++ {
							ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
						}
					}
				}
			}

			if historyJob.MultiSchemaInfo != nil && len(historyJob.MultiSchemaInfo.Warnings) != 0 {
				for _, warning := range historyJob.MultiSchemaInfo.Warnings {
					ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
				}
			}

			logutil.BgLogger().Info("[ddl] DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			logutil.BgLogger().Info("[ddl] DDL job is failed", zap.Int64("jobID", jobID))
			return errors.Trace(historyJob.Error)
		}
		// Only for JobStateCancelled job which is adding columns or drop columns or drop indexes.
		if historyJob.IsCancelled() && (historyJob.Type == model.ActionAddColumns || historyJob.Type == model.ActionDropColumns || historyJob.Type == model.ActionDropIndexes) {
			if historyJob.MultiSchemaInfo != nil && len(historyJob.MultiSchemaInfo.Warnings) != 0 {
				for _, warning := range historyJob.MultiSchemaInfo.Warnings {
					ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
				}
			}
			logutil.BgLogger().Info("[ddl] DDL job is cancelled", zap.Int64("jobID", jobID))
			return nil
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
		goutil.Recover(metrics.LabelDDL, "startCleanDeadTableLock", nil, false)
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

func (d *ddl) setWorkerActivity() {
	b := variable.EnableConcurrentDDL.Load()
	d.queueDDL.active(!b)
	d.concurrentDDL.active(b)
}

func (d *ddl) watchConcurrentDDLSwitch() {
	d.wg.Run(func() {
		original := variable.EnableConcurrentDDL.Load()
		d.setWorkerActivity()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-d.ctx.Done():
				// make them active, let worker quit.
				d.queueDDL.active(true)
				d.concurrentDDL.active(true)
				return
			case <-ticker.C:
				v := variable.EnableConcurrentDDL.Load()
				if original != v {
					var err error
					if v {
						err = d.MigrateExistingDDLs()
					} else {
						err = d.BackOffDDLs()
					}
					if err != nil {
						// set back if meet error.
						logutil.BgLogger().Info("migrate DDL fail")
						variable.EnableConcurrentDDL.Store(original)
						continue
					}
					original = v
					d.setWorkerActivity()
					logutil.BgLogger().Info("migrate DDL success", zap.Bool("EnableConcurrentDDL", variable.EnableConcurrentDDL.Load()))
				}
			}
		}
	})
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

// delayForAsyncCommit sleeps `SafeWindow + AllowedClockDrift` before a DDL job finishes.
// It should be called before any DDL that could break data consistency.
// This provides a safe window for async commit and 1PC to commit with an old schema.
func delayForAsyncCommit() {
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

func init() {
	if flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil {
		RunInGoTest = true
	}
}

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

// GetDDLInfoFromSession get DDL info from brand-new session.
func GetDDLInfoFromSession(sess sessionctx.Context) (*Info, error) {
	s := newSession(sess)
	err := s.begin()
	if err != nil {
		return nil, err
	}
	info, err := GetDDLInfo(sess)
	_ = s.commit()
	return info, err
}

// GetDDLInfo returns DDL information for new ddl framework.
func GetDDLInfo(s sessionctx.Context) (*Info, error) {
	info := &Info{}
	info.Jobs = make([]*model.Job, 0, 2)
	sess := newSession(s)
	txn, err := sess.txn()
	if err != nil {
		return nil, errors.Trace(err)
	}
	t := meta.NewMeta(txn)
	var reorgJob *model.Job
	enable := variable.EnableConcurrentDDL.Load()
	if enable {
		generalJobs, err := getJobsBySQL(sess, "tidb_ddl_job", "not reorg order by job_id limit 1")
		if err != nil {
			return nil, errors.Trace(err)
		}

		if len(generalJobs) != 0 {
			info.Jobs = append(info.Jobs, generalJobs[0])
		}
		reorgJobs, err := getJobsBySQL(sess, "tidb_ddl_job", "reorg order by job_id limit 1")
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(reorgJobs) != 0 {
			info.Jobs = append(info.Jobs, reorgJobs[0])
			reorgJob = reorgJobs[0]
		}
	} else {
		generalJob, err := t.GetDDLJobByIdx(0)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if generalJob != nil {
			info.Jobs = append(info.Jobs, generalJob)
		}
		reorgJob, err = t.GetDDLJobByIdx(0, meta.AddIndexJobListKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if reorgJob != nil {
			info.Jobs = append(info.Jobs, reorgJob)
		}
	}

	info.SchemaVer, err = t.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if reorgJob == nil {
		return info, nil
	}

	_, info.ReorgHandle, _, _, err = newReorgHandler(t, sess, enable).GetDDLReorgHandle(reorgJob)
	if err != nil {
		if meta.ErrDDLReorgElementNotExist.Equal(err) {
			return info, nil
		}
		return nil, errors.Trace(err)
	}

	return info, nil
}

// CancelJobs cancels the DDL jobs.
func CancelJobs(txn kv.Transaction, ids []int64) ([]error, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	errs := make([]error, len(ids))
	t := meta.NewMeta(txn)
	generalJobs, err := getDDLJobsInQueue(t, meta.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDDLJobsInQueue(t, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)
	jobsMap := make(map[int64]int)
	for i, id := range ids {
		jobsMap[id] = i
	}
	for j, job := range jobs {
		i, ok := jobsMap[job.ID]
		if !ok {
			logutil.BgLogger().Debug("the job that needs to be canceled isn't equal to current job",
				zap.Int64("need to canceled job ID", job.ID),
				zap.Int64("current job ID", job.ID))
			continue
		}
		delete(jobsMap, job.ID)
		// These states can't be cancelled.
		if job.IsDone() || job.IsSynced() {
			errs[i] = dbterror.ErrCancelFinishedDDLJob.GenWithStackByArgs(job.ID)
			continue
		}
		// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
		if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
			continue
		}
		if !job.IsRollbackable() {
			errs[i] = dbterror.ErrCannotCancelDDLJob.GenWithStackByArgs(job.ID)
			continue
		}

		job.State = model.JobStateCancelling
		// Make sure RawArgs isn't overwritten.
		err := json.Unmarshal(job.RawArgs, &job.Args)
		if err != nil {
			errs[i] = errors.Trace(err)
			continue
		}
		if j >= len(generalJobs) {
			offset := int64(j - len(generalJobs))
			err = t.UpdateDDLJob(offset, job, true, meta.AddIndexJobListKey)
		} else {
			err = t.UpdateDDLJob(int64(j), job, true)
		}
		if err != nil {
			errs[i] = errors.Trace(err)
		}
	}
	for id, i := range jobsMap {
		errs[i] = dbterror.ErrDDLJobNotFound.GenWithStackByArgs(id)
	}
	return errs, nil
}

// CancelConcurrencyJobs cancels the DDL jobs that are in the concurrent state.
func CancelConcurrencyJobs(se sessionctx.Context, ids []int64) ([]error, error) {
	failpoint.Inject("mockCancelConcurencyDDL", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock commit error"))
		}
	})
	if len(ids) == 0 {
		return nil, nil
	}
	var getJobSQL string
	var jobSet = make(map[int64]int) // jobID -> error index

	sess := newSession(se)
	err := sess.begin()
	if err != nil {
		return nil, err
	}
	if len(ids) == 1 {
		getJobSQL = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = %d order by job_id", ids[0])
		jobSet[ids[0]] = 0
	} else {
		var idsStr []string
		for idx, id := range ids {
			jobSet[id] = idx
			idsStr = append(idsStr, strconv.FormatInt(id, 10))
		}
		getJobSQL = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id in (%s) order by job_id", strings.Join(idsStr, ", "))
	}
	rows, err := sess.execute(context.Background(), getJobSQL, "cancel")
	if err != nil {
		sess.rollback()
		return nil, err
	}

	errs := make([]error, len(ids))
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := &model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			sess.rollback()
			return nil, err
		}
		i, ok := jobSet[job.ID]
		if !ok {
			continue
		}
		delete(jobSet, job.ID)
		if job.IsDone() || job.IsSynced() {
			errs[i] = dbterror.ErrCancelFinishedDDLJob.GenWithStackByArgs(job.ID)
			continue
		}
		// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
		if job.IsCancelled() {
			errs[i] = dbterror.ErrCancelledDDLJob.GenWithStackByArgs(job.ID)
			continue
		}
		if job.IsRollingback() || job.IsRollbackDone() {
			continue
		}
		if !job.IsRollbackable() {
			errs[i] = dbterror.ErrCannotCancelDDLJob.GenWithStackByArgs(job.ID)
			continue
		}
		job.State = model.JobStateCancelling
		// Make sure RawArgs isn't overwritten.
		err := json.Unmarshal(job.RawArgs, &job.Args)
		if err != nil {
			errs[i] = errors.Trace(err)
			continue
		}
		err = updateConcurrencyDDLJob(sess, job, true)
		if err != nil {
			errs[i] = errors.Trace(err)
		}
	}
	err = sess.commit()
	if err != nil {
		return nil, err
	}
	for id, idx := range jobSet {
		errs[idx] = dbterror.ErrDDLJobNotFound.GenWithStackByArgs(id)
	}
	return errs, nil
}

// GetHistoryDDLJobs returns the DDL history jobs and an error.
// The maximum count of history jobs is num.
func GetHistoryDDLJobs(sess sessionctx.Context, txn kv.Transaction, maxNumJobs int) ([]*model.Job, error) {
	t := meta.NewMeta(txn)
	return GetLastNHistoryDDLJobs(newSession(sess), t, maxNumJobs)
}

func getDDLJobsInQueue(t *meta.Meta, jobListKey meta.JobListKeyType) ([]*model.Job, error) {
	cnt, err := t.DDLJobQueueLen(jobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, cnt)
	for i := range jobs {
		jobs[i], err = t.GetDDLJobByIdx(int64(i), jobListKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return jobs, nil
}

// GetAllDDLJobs get all DDL jobs and sorts jobs by job.ID.
func GetAllDDLJobs(sess sessionctx.Context, t *meta.Meta) ([]*model.Job, error) {
	if variable.EnableConcurrentDDL.Load() {
		return getJobsBySQL(newSession(sess), "tidb_ddl_job", "1 order by job_id")
	}

	return getDDLJobs(t)
}

// getDDLJobs get all DDL jobs and sorts jobs by job.ID.
func getDDLJobs(t *meta.Meta) ([]*model.Job, error) {
	generalJobs, err := getDDLJobsInQueue(t, meta.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDDLJobsInQueue(t, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)
	sort.Sort(jobArray(jobs))
	return jobs, nil
}

type jobArray []*model.Job

func (v jobArray) Len() int {
	return len(v)
}

func (v jobArray) Less(i, j int) bool {
	return v[i].ID < v[j].ID
}

func (v jobArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

// MaxHistoryJobs is exported for testing.
const MaxHistoryJobs = 10

// DefNumHistoryJobs is default value of the default number of history job
const DefNumHistoryJobs = 10

// IterHistoryDDLJobs iterates history DDL jobs until the `finishFn` return true or error.
func IterHistoryDDLJobs(sess sessionctx.Context, txn kv.Transaction, finishFn func([]*model.Job) (bool, error)) error {
	txnMeta := meta.NewMeta(txn)
	iter, err := GetLastHistoryDDLJobsIterator(sess, txnMeta)
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
	return IterHistoryDDLJobs(ctx, txn, finishFn)
}

// GetLastNHistoryDDLJobs get last n history ddl jobs.
func GetLastNHistoryDDLJobs(sess *session, m *meta.Meta, maxNumJobs int) ([]*model.Job, error) {
	if variable.EnableConcurrentDDL.Load() {
		return getJobsBySQL(sess, "tidb_ddl_history", "1 order by job_id desc limit "+strconv.Itoa(maxNumJobs))
	}
	jobs, err := m.GetLastNHistoryDDLJobs(maxNumJobs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

// GetLastHistoryDDLJobsIterator gets latest N history ddl jobs iterator.
func GetLastHistoryDDLJobsIterator(sess sessionctx.Context, m *meta.Meta) (meta.LastJobIterator, error) {
	if variable.EnableConcurrentDDL.Load() {
		return &ConcurrentDDLLastJobIterator{
			sess:   newSession(sess),
			offset: 0,
		}, nil
	}
	return m.GetLastHistoryDDLJobsIterator()
}

// ConcurrentDDLLastJobIterator is the iterator for gets latest history.
type ConcurrentDDLLastJobIterator struct {
	sess   *session
	offset uint64
}

// GetLastJobs iter latest num history ddl jobs.
func (c *ConcurrentDDLLastJobIterator) GetLastJobs(num int, _ []*model.Job) ([]*model.Job, error) {
	jobs, err := getJobsBySQL(c.sess, "tidb_ddl_history", fmt.Sprintf("1 order by job_id desc limit %d, %d", c.offset, num))
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.offset += uint64(len(jobs))
	return jobs, err
}

type session struct {
	s sessionctx.Context
}

func newSession(s sessionctx.Context) *session {
	return &session{s: s}
}

func (s *session) begin() error {
	err := sessiontxn.NewTxn(context.Background(), s.s)
	if err != nil {
		return err
	}
	s.s.GetSessionVars().SetInTxn(true)
	return nil
}

func (s *session) commit() error {
	s.s.GetSessionVars().SetInTxn(false)
	s.s.StmtCommit()
	return s.s.CommitTxn(context.Background())
}

func (s *session) txn() (kv.Transaction, error) {
	return s.s.Txn(true)
}

func (s *session) rollback() {
	s.s.StmtRollback()
	s.s.RollbackTxn(context.Background())
}

func (s *session) reset() {
	s.s.StmtRollback()
}

func (s *session) execute(ctx context.Context, query string, label string) ([]chunk.Row, error) {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DDLJobTableDuration.WithLabelValues(label + "-" + metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	rs, err := s.s.(sqlexec.SQLExecutor).ExecuteInternal(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return nil, errors.Trace(err)
	}
	if err = rs.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func (s *session) session() sessionctx.Context {
	return s.s
}

// GetAllHistoryDDLJobs get all the done DDL jobs.
func GetAllHistoryDDLJobs(sess sessionctx.Context, m *meta.Meta) ([]*model.Job, error) {
	if variable.EnableConcurrentDDL.Load() {
		return getJobsBySQL(newSession(sess), "tidb_ddl_history", "1")
	}

	return m.GetAllHistoryDDLJobs()
}

// GetHistoryJobByID return history DDL job by ID.
func GetHistoryJobByID(sess sessionctx.Context, id int64) (*model.Job, error) {
	if variable.EnableConcurrentDDL.Load() {
		jobs, err := getJobsBySQL(newSession(sess), "tidb_ddl_history", fmt.Sprintf("job_id = %d", id))
		if err != nil || len(jobs) == 0 {
			return nil, errors.Trace(err)
		}
		return jobs[0], nil
	}
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

// AddHistoryDDLJobForTest used for test.
func AddHistoryDDLJobForTest(sess sessionctx.Context, t *meta.Meta, job *model.Job, updateRawArgs bool) error {
	return AddHistoryDDLJob(newSession(sess), t, job, updateRawArgs, variable.EnableConcurrentDDL.Load())
}

// AddHistoryDDLJob adds DDL job to history table.
func AddHistoryDDLJob(sess *session, t *meta.Meta, job *model.Job, updateRawArgs bool, enableConcurrentDDL bool) error {
	if enableConcurrentDDL {
		return addHistoryDDLJob(sess, job, updateRawArgs)
	}

	return t.AddHistoryDDLJob(job, updateRawArgs)
}

func addHistoryDDLJob(sess *session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	_, err = sess.execute(context.Background(), fmt.Sprintf("insert into mysql.tidb_ddl_history(job_id, job_meta) values (%d, 0x%x)", job.ID, b), "insert_history")
	return errors.Trace(err)
}
