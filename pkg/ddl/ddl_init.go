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
	"time"

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
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/intest"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

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
