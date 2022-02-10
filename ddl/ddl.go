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
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/table"
	goutil "github.com/pingcap/tidb/util"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
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
	AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error
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
	GetTableMaxHandle(startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error)
	// SetBinlogClient sets the binlog client for DDL worker. It's exported for testing.
	SetBinlogClient(*pumpcli.PumpsClient)
	// GetHook gets the hook. It's exported for testing.
	GetHook() Callback
	// SetHook sets the hook.
	SetHook(h Callback)
}

type limitJobTask struct {
	job *model.Job
	err chan error
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m          sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         tidbutil.WaitGroupWrapper // It's only used to deal with data race in restart_test.
	limitJobCh chan *limitJobTask

	*ddlCtx
	workers           map[workerType]*worker
	sessPool          *sessionPool
	delRangeMgr       delRangeManager
	enableTiFlashPoll *atomicutil.Bool
}

// ddlCtx is the context when we use worker to handle DDL jobs.
type ddlCtx struct {
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

	// hook may be modified.
	mu struct {
		sync.RWMutex
		hook        Callback
		interceptor Interceptor
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
		uuid:         id,
		store:        opt.Store,
		lease:        opt.Lease,
		ddlJobDoneCh: make(chan struct{}, 1),
		ownerManager: manager,
		schemaSyncer: syncer,
		binlogCli:    binloginfo.GetPumpsClient(),
		infoCache:    opt.InfoCache,
		tableLockCkr: deadLockCkr,
		etcdCli:      opt.EtcdCli,
	}
	ddlCtx.mu.hook = opt.Hook
	ddlCtx.mu.interceptor = &BaseInterceptor{}
	d := &ddl{
		ctx:               ctx,
		ddlCtx:            ddlCtx,
		limitJobCh:        make(chan *limitJobTask, batchAddingJobs),
		enableTiFlashPoll: atomicutil.NewBool(true),
	}

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

// Start implements DDL.Start interface.
func (d *ddl) Start(ctxPool *pools.ResourcePool) error {
	logutil.BgLogger().Info("[ddl] start DDL", zap.String("ID", d.uuid), zap.Bool("runWorker", RunWorker))
	d.ctx, d.cancel = context.WithCancel(d.ctx)

	d.wg.Add(1)
	go d.limitDDLJobs()

	// If RunWorker is true, we need campaign owner and do DDL job.
	// Otherwise, we needn't do that.
	if RunWorker {
		err := d.ownerManager.CampaignOwner()
		if err != nil {
			return errors.Trace(err)
		}

		d.workers = make(map[workerType]*worker, 2)
		d.sessPool = newSessionPool(ctxPool)
		d.delRangeMgr = d.newDeleteRangeManager(ctxPool == nil)
		d.workers[generalWorker] = newWorker(d.ctx, generalWorker, d.sessPool, d.delRangeMgr)
		d.workers[addIdxWorker] = newWorker(d.ctx, addIdxWorker, d.sessPool, d.delRangeMgr)
		for _, worker := range d.workers {
			worker.wg.Add(1)
			w := worker
			go w.start(d.ddlCtx)

			metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDDL, worker.String())).Inc()

			// When the start function is called, we will send a fake job to let worker
			// checks owner firstly and try to find whether a job exists and run.
			asyncNotify(worker.ddlJobCh)
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

func (d *ddl) close() {
	if isChanClosed(d.ctx.Done()) {
		return
	}

	startTime := time.Now()
	d.cancel()
	d.wg.Wait()
	d.ownerManager.Cancel()
	d.schemaSyncer.Close()

	for _, worker := range d.workers {
		worker.close()
	}
	// d.delRangeMgr using sessions from d.sessPool.
	// Put it before d.sessPool.close to reduce the time spent by d.sessPool.close.
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}
	if d.sessPool != nil {
		d.sessPool.close()
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

// mayNeedReorg indicates that this job may need to reorganize the data.
func mayNeedReorg(job *model.Job) bool {
	switch job.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		return true
	case model.ActionModifyColumn:
		if len(job.CtxVars) > 0 {
			needReorg, ok := job.CtxVars[0].(bool)
			return ok && needReorg
		}
		return false
	default:
		return false
	}
}

func (d *ddl) asyncNotifyWorker(job *model.Job) {
	// If the workers don't run, we needn't notify workers.
	if !RunWorker {
		return
	}
	var worker *worker
	if mayNeedReorg(job) {
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

func updateTickerInterval(ticker *time.Ticker, lease time.Duration, job *model.Job, i int) *time.Ticker {
	interval, changed := getJobCheckInterval(job, i)
	if !changed {
		return ticker
	}
	// For now we should stop old ticker and create a new ticker
	ticker.Stop()
	return time.NewTicker(chooseLeaseTime(lease, interval))
}

// doDDLJob will return
// - nil: found in history DDL job and no job error
// - context.Cancel: job has been sent to worker, but not found in history DDL job before cancel
// - other: found in history DDL job and return that job error
func (d *ddl) doDDLJob(ctx sessionctx.Context, job *model.Job) error {
	// Get a global job ID and put the DDL job in the queue.
	job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
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
			logutil.BgLogger().Info("[ddl] doDDLJob will quit because context done")
			return context.Canceled
		}

		historyJob, err = d.getHistoryDDLJob(jobID)
		if err != nil {
			logutil.BgLogger().Error("[ddl] get history DDL job failed, check again", zap.Error(err))
			continue
		}
		if historyJob == nil {
			logutil.BgLogger().Debug("[ddl] DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

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

func (d *ddl) callHookOnChanged(err error) error {
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
