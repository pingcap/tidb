// Copyright 2022 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	// addingDDLJobNotifyKey is the key in etcd to notify DDL scheduler that there
	// is a new DDL job.
	addingDDLJobNotifyKey       = "/tidb/ddl/add_ddl_job_general"
	dispatchLoopWaitingDuration = 1 * time.Second
	schedulerLoopRetryInterval  = time.Second
)

func init() {
	// In test the wait duration can be reduced to make test case run faster
	if intest.InTest {
		dispatchLoopWaitingDuration = 50 * time.Millisecond
	}
}

type jobType int

func (t jobType) String() string {
	switch t {
	case jobTypeGeneral:
		return "general"
	case jobTypeReorg:
		return "reorg"
	}
	return "unknown job type: " + strconv.Itoa(int(t))
}

const (
	jobTypeGeneral jobType = iota
	jobTypeReorg
)

type ownerListener struct {
	ddl          *ddl
	jobSubmitter *JobSubmitter
	ddlExecutor  *executor

	scheduler *jobScheduler
}

var _ owner.Listener = (*ownerListener)(nil)

func (l *ownerListener) OnBecomeOwner() {
	ctx, cancelFunc := context.WithCancel(l.ddl.ddlCtx.ctx)
	sysTblMgr := systable.NewManager(l.ddl.sessPool)
	l.scheduler = &jobScheduler{
		schCtx:            ctx,
		cancel:            cancelFunc,
		runningJobs:       newRunningJobs(),
		sysTblMgr:         sysTblMgr,
		schemaLoader:      l.ddl.schemaLoader,
		minJobIDRefresher: l.ddl.minJobIDRefresher,
		unSyncedTracker:   newUnSyncedJobTracker(),
		schemaVerMgr:      newSchemaVersionManager(l.ddl.store),
		schemaVerSyncer:   l.ddl.schemaVerSyncer,

		ddlCtx:         l.ddl.ddlCtx,
		ddlJobNotifyCh: l.jobSubmitter.ddlJobNotifyCh,
		sessPool:       l.ddl.sessPool,
		delRangeMgr:    l.ddl.delRangeMgr,

		ddlJobDoneChMap: l.ddlExecutor.ddlJobDoneChMap,
	}
	l.ddl.reorgCtx.setOwnerTS(time.Now().Unix())
	l.scheduler.start()
}

func (l *ownerListener) OnRetireOwner() {
	if l.scheduler == nil {
		return
	}
	l.scheduler.close()
}

// jobScheduler is used to schedule the DDL jobs, it's only run on the DDL owner.
type jobScheduler struct {
	// *ddlCtx already have context named as "ctx", so we use "schCtx" here to avoid confusion.
	schCtx            context.Context
	cancel            context.CancelFunc
	wg                tidbutil.WaitGroupWrapper
	runningJobs       *runningJobs
	sysTblMgr         systable.Manager
	schemaLoader      SchemaLoader
	minJobIDRefresher *systable.MinJobIDRefresher
	unSyncedTracker   *unSyncedJobTracker
	schemaVerMgr      *schemaVersionManager
	schemaVerSyncer   schemaver.Syncer

	// those fields are created or initialized on start
	reorgWorkerPool      *workerPool
	generalDDLWorkerPool *workerPool
	seqAllocator         atomic.Uint64

	// those fields are shared with 'ddl' instance
	// TODO ddlCtx is too large for here, we should remove dependency on it.
	*ddlCtx
	ddlJobNotifyCh chan struct{}
	sessPool       *sess.Pool
	delRangeMgr    delRangeManager

	// shared with ddl executor and job submitter.
	ddlJobDoneChMap *generic.SyncMap[int64, chan struct{}]
}

func (s *jobScheduler) start() {
	workerFactory := func(tp workerType) func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newWorker(s.schCtx, tp, s.sessPool, s.delRangeMgr, s.ddlCtx)
			sessForJob, err := s.sessPool.Get()
			if err != nil {
				return nil, err
			}
			wk.seqAllocator = &s.seqAllocator
			sessForJob.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
			wk.sess = sess.NewSession(sessForJob)
			metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDDL, wk.String())).Inc()
			return wk, nil
		}
	}
	// reorg worker count at least 1 at most 10.
	reorgCnt := min(max(runtime.GOMAXPROCS(0)/4, 1), reorgWorkerCnt)
	s.reorgWorkerPool = newDDLWorkerPool(pools.NewResourcePool(workerFactory(addIdxWorker), reorgCnt, reorgCnt, 0), jobTypeReorg)
	s.generalDDLWorkerPool = newDDLWorkerPool(pools.NewResourcePool(workerFactory(generalWorker), generalWorkerCnt, generalWorkerCnt, 0), jobTypeGeneral)
	s.wg.RunWithLog(s.scheduleLoop)
	s.wg.RunWithLog(func() {
		s.schemaVerSyncer.SyncJobSchemaVerLoop(s.schCtx)
	})
}

func (s *jobScheduler) close() {
	s.cancel()
	s.wg.Wait()
	if s.reorgWorkerPool != nil {
		s.reorgWorkerPool.close()
	}
	if s.generalDDLWorkerPool != nil {
		s.generalDDLWorkerPool.close()
	}
	failpoint.InjectCall("afterSchedulerClose")
}

func hasSysDB(job *model.Job) bool {
	for _, info := range job.GetInvolvingSchemaInfo() {
		if tidbutil.IsSysDB(info.Database) {
			return true
		}
	}
	return false
}

func (s *jobScheduler) processJobDuringUpgrade(sess *sess.Session, job *model.Job) (isRunnable bool, err error) {
	if s.serverStateSyncer.IsUpgradingState() {
		if job.IsPaused() {
			return false, nil
		}
		// We need to turn the 'pausing' job to be 'paused' in ddl worker,
		// and stop the reorganization workers
		if job.IsPausing() || hasSysDB(job) {
			return true, nil
		}
		var errs []error
		// During binary upgrade, pause all running DDL jobs
		errs, err = PauseJobsBySystem(sess.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			err = errs[0]
		}

		if err != nil {
			isCannotPauseDDLJobErr := dbterror.ErrCannotPauseDDLJob.Equal(err)
			logutil.DDLUpgradingLogger().Warn("pause the job failed", zap.Stringer("job", job),
				zap.Bool("isRunnable", isCannotPauseDDLJobErr), zap.Error(err))
			if isCannotPauseDDLJobErr {
				return true, nil
			}
		} else {
			logutil.DDLUpgradingLogger().Warn("pause the job successfully", zap.Stringer("job", job))
		}

		return false, nil
	}

	if job.IsPausedBySystem() {
		var errs []error
		errs, err = ResumeJobsBySystem(sess.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			logutil.DDLUpgradingLogger().Warn("normal cluster state, resume the job failed", zap.Stringer("job", job), zap.Error(errs[0]))
			return false, errs[0]
		}
		if err != nil {
			logutil.DDLUpgradingLogger().Warn("normal cluster state, resume the job failed", zap.Stringer("job", job), zap.Error(err))
			return false, err
		}
		logutil.DDLUpgradingLogger().Warn("normal cluster state, resume the job successfully", zap.Stringer("job", job))
		return false, errors.Errorf("system paused job:%d need to be resumed", job.ID)
	}

	if job.IsPaused() {
		return false, nil
	}

	return true, nil
}

func (s *jobScheduler) scheduleLoop() {
	const retryInterval = 3 * time.Second
	for {
		err := s.schedule()
		if err == context.Canceled {
			logutil.DDLLogger().Info("scheduleLoop quit due to context canceled")
			return
		}
		logutil.DDLLogger().Warn("scheduleLoop failed, retrying",
			zap.Error(err))

		select {
		case <-s.schCtx.Done():
			logutil.DDLLogger().Info("scheduleLoop quit due to context done")
			return
		case <-time.After(retryInterval):
		}
	}
}

func (s *jobScheduler) schedule() error {
	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sessCtx)
	se := sess.NewSession(sessCtx)
	var notifyDDLJobByEtcdCh clientv3.WatchChan
	if s.etcdCli != nil {
		notifyDDLJobByEtcdCh = s.etcdCli.Watch(s.schCtx, addingDDLJobNotifyKey)
	}
	if err := s.checkAndUpdateClusterState(true); err != nil {
		return errors.Trace(err)
	}
	ticker := time.NewTicker(dispatchLoopWaitingDuration)
	defer ticker.Stop()
	s.mustReloadSchemas()

	for {
		if err := s.schCtx.Err(); err != nil {
			return err
		}
		failpoint.Inject("ownerResignAfterDispatchLoopCheck", func() {
			if ingest.ResignOwnerForTest.Load() {
				err2 := s.ownerManager.ResignOwner(context.Background())
				if err2 != nil {
					logutil.DDLLogger().Info("resign meet error", zap.Error(err2))
				}
				ingest.ResignOwnerForTest.Store(false)
			}
		})
		select {
		case <-s.ddlJobNotifyCh:
		case <-ticker.C:
		case _, ok := <-notifyDDLJobByEtcdCh:
			if !ok {
				logutil.DDLLogger().Warn("start worker watch channel closed", zap.String("watch key", addingDDLJobNotifyKey))
				notifyDDLJobByEtcdCh = s.etcdCli.Watch(s.schCtx, addingDDLJobNotifyKey)
				time.Sleep(time.Second)
				continue
			}
		case <-s.schCtx.Done():
			return s.schCtx.Err()
		}
		if err := s.checkAndUpdateClusterState(false); err != nil {
			continue
		}
		failpoint.InjectCall("beforeLoadAndDeliverJobs")
		if err := s.loadAndDeliverJobs(se); err != nil {
			logutil.SampleLogger().Warn("load and deliver jobs failed", zap.Error(err))
		}
	}
}

// TODO make it run in a separate routine.
func (s *jobScheduler) checkAndUpdateClusterState(needUpdate bool) error {
	select {
	case _, ok := <-s.serverStateSyncer.WatchChan():
		if !ok {
			// TODO serverStateSyncer should only be started when we are the owner, and use
			// the context of scheduler, will refactor it later.
			s.serverStateSyncer.Rewatch(s.ddlCtx.ctx)
		}
	default:
		if !needUpdate {
			return nil
		}
	}

	oldState := s.serverStateSyncer.IsUpgradingState()
	stateInfo, err := s.serverStateSyncer.GetGlobalState(s.schCtx)
	if err != nil {
		logutil.DDLLogger().Warn("get global state failed", zap.Error(err))
		return errors.Trace(err)
	}
	logutil.DDLLogger().Info("get global state and global state change",
		zap.Bool("oldState", oldState), zap.Bool("currState", s.serverStateSyncer.IsUpgradingState()))

	ownerOp := owner.OpNone
	if stateInfo.State == serverstate.StateUpgrading {
		ownerOp = owner.OpSyncUpgradingState
	}
	err = s.ownerManager.SetOwnerOpValue(s.schCtx, ownerOp)
	if err != nil {
		logutil.DDLLogger().Warn("the owner sets global state to owner operator value failed", zap.Error(err))
		return errors.Trace(err)
	}
	logutil.DDLLogger().Info("the owner sets owner operator value", zap.Stringer("ownerOp", ownerOp))
	return nil
}

func (s *jobScheduler) loadAndDeliverJobs(se *sess.Session) error {
	if s.generalDDLWorkerPool.available() == 0 && s.reorgWorkerPool.available() == 0 {
		return nil
	}

	defer s.runningJobs.resetAllPending()

	const getJobSQL = `select reorg, job_meta from mysql.tidb_ddl_job where job_id >= %d %s order by job_id`
	var whereClause string
	if ids := s.runningJobs.allIDs(); len(ids) > 0 {
		whereClause = fmt.Sprintf("and job_id not in (%s)", ids)
	}
	sql := fmt.Sprintf(getJobSQL, s.minJobIDRefresher.GetCurrMinJobID(), whereClause)
	rows, err := se.Execute(context.Background(), sql, "load_ddl_jobs")
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		reorgJob := row.GetInt64(0) == 1
		targetPool := s.generalDDLWorkerPool
		if reorgJob {
			targetPool = s.reorgWorkerPool
		}
		jobBinary := row.GetBytes(1)

		job := model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return errors.Trace(err)
		}
		intest.Assert(job.Version > 0, "job version should be greater than 0")

		involving := job.GetInvolvingSchemaInfo()
		if targetPool.available() == 0 {
			s.runningJobs.addPending(involving)
			continue
		}

		isRunnable, err := s.processJobDuringUpgrade(se, &job)
		if err != nil {
			return errors.Trace(err)
		}
		if !isRunnable {
			s.runningJobs.addPending(involving)
			continue
		}

		if !s.runningJobs.checkRunnable(job.ID, involving) {
			s.runningJobs.addPending(involving)
			continue
		}

		wk, err := targetPool.get()
		if err != nil {
			return errors.Trace(err)
		}
		intest.Assert(wk != nil, "worker should not be nil")
		if wk == nil {
			// should not happen, we have checked available() before, and we are
			// the only routine consumes worker.
			logutil.DDLLogger().Info("no worker available now", zap.Stringer("type", targetPool.tp()))
			s.runningJobs.addPending(involving)
			continue
		}

		s.deliveryJob(wk, targetPool, &job)

		if s.generalDDLWorkerPool.available() == 0 && s.reorgWorkerPool.available() == 0 {
			break
		}
	}
	return nil
}

// mustReloadSchemas is used to reload schema when we become the DDL owner, in case
// the schema version is outdated before we become the owner.
// It will keep reloading schema until either success or context done.
// Domain also have a similar method 'mustReload', but its methods don't accept context.
func (s *jobScheduler) mustReloadSchemas() {
	for {
		err := s.schemaLoader.Reload()
		if err == nil {
			return
		}
		logutil.DDLLogger().Warn("reload schema failed, will retry later", zap.Error(err))
		select {
		case <-s.schCtx.Done():
			return
		case <-time.After(schedulerLoopRetryInterval):
		}
	}
}

// deliveryJob deliver the job to the worker to run it asynchronously.
// the worker will run the job until it's finished, paused or another owner takes
// over and finished it.
func (s *jobScheduler) deliveryJob(wk *worker, pool *workerPool, job *model.Job) {
	failpoint.InjectCall("beforeDeliveryJob", job)
	injectFailPointForGetJob(job)
	jobID, involvedSchemaInfos := job.ID, job.GetInvolvingSchemaInfo()
	s.runningJobs.addRunning(jobID, involvedSchemaInfos)
	metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
	jobCtx := s.getJobRunCtx(job.ID)
	s.wg.Run(func() {
		defer func() {
			r := recover()
			if r != nil {
				logutil.DDLLogger().Error("panic in deliveryJob", zap.Any("recover", r), zap.Stack("stack"))
			}
			failpoint.InjectCall("afterDeliveryJob", job)
			// Because there is a gap between `allIDs()` and `checkRunnable()`,
			// we append unfinished job to pending atomically to prevent `getJob()`
			// chosing another runnable job that involves the same schema object.
			moveRunningJobsToPending := r != nil || (job != nil && !job.IsFinished())
			s.runningJobs.finishOrPendJob(jobID, involvedSchemaInfos, moveRunningJobsToPending)
			asyncNotify(s.ddlJobNotifyCh)
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
			pool.put(wk)
		}()
		for {
			err := s.transitOneJobStepAndWaitSync(wk, jobCtx, job)
			if err != nil {
				logutil.DDLLogger().Info("run job failed", zap.Error(err), zap.Stringer("job", job))
			} else if job.InFinalState() {
				return
			}
			// we have to refresh the job, to handle cases like job cancel or pause
			// or the job is finished by another owner.
			// TODO for JobStateRollbackDone we have to query 1 additional time when the
			// job is already moved to history.
			failpoint.InjectCall("beforeRefreshJob", job)
			for {
				job, err = s.sysTblMgr.GetJobByID(s.schCtx, jobID)
				failpoint.InjectCall("mockGetJobByIDFail", &err)
				if err == nil {
					break
				}

				if err == systable.ErrNotFound {
					logutil.DDLLogger().Info("job not found, might already finished",
						zap.Int64("job_id", jobID))
					return
				}
				logutil.DDLLogger().Error("get job failed", zap.Int64("job_id", jobID), zap.Error(err))
				select {
				case <-s.schCtx.Done():
					return
				case <-time.After(500 * time.Millisecond):
					continue
				}
			}
		}
	})
}

func (s *jobScheduler) getJobRunCtx(jobID int64) *jobContext {
	ch, _ := s.ddlJobDoneChMap.Load(jobID)
	return &jobContext{
		ctx:                  s.schCtx,
		unSyncedJobTracker:   s.unSyncedTracker,
		schemaVersionManager: s.schemaVerMgr,
		infoCache:            s.infoCache,
		autoidCli:            s.autoidCli,
		store:                s.store,
		schemaVerSyncer:      s.schemaVerSyncer,

		notifyCh: ch,

		oldDDLCtx: s.ddlCtx,
	}
}

// transitOneJobStepAndWaitSync runs one step of the DDL job, persist it and
// waits for other TiDB node to synchronize.
func (s *jobScheduler) transitOneJobStepAndWaitSync(wk *worker, jobCtx *jobContext, job *model.Job) error {
	failpoint.InjectCall("beforeRunOneJobStep")
	ownerID := s.ownerManager.ID()
	// suppose we failed to sync version last time, we need to check and sync it
	// before run to maintain the 2-version invariant.
	// if owner not change, we need try to sync when it's un-synced.
	// if owner changed, we need to try sync it if the job is not started by
	// current owner.
	if jobCtx.isUnSynced(job.ID) || (job.Started() && !jobCtx.maybeAlreadyRunOnce(job.ID)) {
		if variable.EnableMDL.Load() {
			version, err := s.sysTblMgr.GetMDLVer(s.schCtx, job.ID)
			if err == nil {
				err = waitVersionSynced(jobCtx, job, version)
				if err != nil {
					return err
				}
				s.cleanMDLInfo(job, ownerID)
			} else if err != systable.ErrNotFound {
				wk.jobLogger(job).Warn("check MDL info failed", zap.Error(err))
				return err
			}
		} else {
			err := waitVersionSyncedWithoutMDL(jobCtx, job)
			if err != nil {
				time.Sleep(time.Second)
				return err
			}
		}
		jobCtx.setAlreadyRunOnce(job.ID)
	}

	schemaVer, err := wk.transitOneJobStep(jobCtx, job)
	if err != nil {
		tidblogutil.Logger(wk.logCtx).Info("handle ddl job failed", zap.Error(err), zap.Stringer("job", job))
		return err
	}
	failpoint.Inject("mockDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
		if val.(bool) {
			if mockDDLErrOnce == 0 {
				mockDDLErrOnce = schemaVer
				failpoint.Return(errors.New("mock down before update global version"))
			}
		}
	})

	failpoint.InjectCall("beforeWaitSchemaChanged", job, schemaVer)
	// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
	// If the job is done or still running or rolling back, we will wait 2 * lease time or util MDL synced to guarantee other servers to update
	// the newest schema.
	if err = updateGlobalVersionAndWaitSynced(jobCtx, schemaVer, job); err != nil {
		return err
	}
	s.cleanMDLInfo(job, ownerID)
	jobCtx.removeUnSynced(job.ID)

	failpoint.InjectCall("onJobUpdated", job)
	return nil
}

// cleanMDLInfo cleans metadata lock info.
func (s *jobScheduler) cleanMDLInfo(job *model.Job, ownerID string) {
	if !variable.EnableMDL.Load() {
		return
	}
	var sql string
	if tidbutil.IsSysDB(strings.ToLower(job.SchemaName)) {
		// DDLs that modify system tables could only happen in upgrade process,
		// we should not reference 'owner_id'. Otherwise, there is a circular blocking problem.
		sql = fmt.Sprintf("delete from mysql.tidb_mdl_info where job_id = %d", job.ID)
	} else {
		sql = fmt.Sprintf("delete from mysql.tidb_mdl_info where job_id = %d and owner_id = '%s'", job.ID, ownerID)
	}
	sctx, _ := s.sessPool.Get()
	defer s.sessPool.Put(sctx)
	se := sess.NewSession(sctx)
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := se.Execute(s.schCtx, sql, "delete-mdl-info")
	if err != nil {
		logutil.DDLLogger().Warn("unexpected error when clean mdl info", zap.Int64("job ID", job.ID), zap.Error(err))
		return
	}
	// TODO we need clean it when version of JobStateRollbackDone is synced also.
	if job.State == model.JobStateSynced && s.etcdCli != nil {
		path := fmt.Sprintf("%s/%d/", util.DDLAllSchemaVersionsByJob, job.ID)
		_, err = s.etcdCli.Delete(s.schCtx, path, clientv3.WithPrefix())
		if err != nil {
			logutil.DDLLogger().Warn("delete versions failed", zap.Int64("job ID", job.ID), zap.Error(err))
		}
	}
}

func (d *ddl) getTableByTxn(r autoid.Requirement, schemaID, tableID int64) (*model.DBInfo, table.Table, error) {
	var tbl table.Table
	var dbInfo *model.DBInfo
	err := kv.RunInNewTxn(d.ctx, r.Store(), false, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		dbInfo, err1 = t.GetDatabase(schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		tblInfo, err1 := getTableInfo(t, tableID, schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		tbl, err1 = getTable(r, schemaID, tblInfo)
		return errors.Trace(err1)
	})
	return dbInfo, tbl, err
}

const (
	addDDLJobSQL    = "insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values"
	updateDDLJobSQL = "update mysql.tidb_ddl_job set job_meta = %s where job_id = %d"
)

func insertDDLJobs2Table(ctx context.Context, se *sess.Session, jobWs ...*JobWrapper) error {
	failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	if len(jobWs) == 0 {
		return nil
	}
	var sql bytes.Buffer
	sql.WriteString(addDDLJobSQL)
	for i, jobW := range jobWs {
		// TODO remove this check when all job type pass args in this way.
		if jobW.JobArgs != nil {
			jobW.FillArgs(jobW.JobArgs)
		}
		injectModifyJobArgFailPoint(jobWs)
		b, err := jobW.Encode(true)
		if err != nil {
			return err
		}
		if i != 0 {
			sql.WriteString(",")
		}
		fmt.Fprintf(&sql, "(%d, %t, %s, %s, %s, %d, %t)", jobW.ID, jobW.MayNeedReorg(),
			strconv.Quote(job2SchemaIDs(jobW)), strconv.Quote(job2TableIDs(jobW)),
			util.WrapKey2String(b), jobW.Type, jobW.Started())
	}
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := se.Execute(ctx, sql.String(), "insert_job")
	logutil.DDLLogger().Debug("add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
	return errors.Trace(err)
}

func makeStringForIDs(ids []int64) string {
	set := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}

	s := make([]string, 0, len(set))
	for id := range set {
		s = append(s, strconv.FormatInt(id, 10))
	}
	slices.Sort(s)
	return strings.Join(s, ",")
}

func job2SchemaIDs(jobW *JobWrapper) string {
	switch jobW.Type {
	case model.ActionRenameTables:
		var ids []int64
		arg := jobW.JobArgs.(*model.RenameTablesArgs)
		ids = make([]int64, 0, len(arg.RenameTableInfos)*2)
		for _, info := range arg.RenameTableInfos {
			ids = append(ids, info.OldSchemaID, info.NewSchemaID)
		}
		return makeStringForIDs(ids)
	case model.ActionRenameTable:
		oldSchemaID := jobW.JobArgs.(*model.RenameTableArgs).OldSchemaID
		ids := []int64{oldSchemaID, jobW.SchemaID}
		return makeStringForIDs(ids)
	case model.ActionExchangeTablePartition:
		ids := jobW.CtxVars[0].([]int64)
		return makeStringForIDs(ids)
	default:
		return strconv.FormatInt(jobW.SchemaID, 10)
	}
}

func job2TableIDs(jobW *JobWrapper) string {
	switch jobW.Type {
	case model.ActionRenameTables:
		var ids []int64
		arg := jobW.JobArgs.(*model.RenameTablesArgs)
		ids = make([]int64, 0, len(arg.RenameTableInfos))
		for _, info := range arg.RenameTableInfos {
			ids = append(ids, info.TableID)
		}
		return makeStringForIDs(ids)
	case model.ActionExchangeTablePartition:
		ids := jobW.CtxVars[1].([]int64)
		return makeStringForIDs(ids)
	case model.ActionTruncateTable:
		newTableID := jobW.JobArgs.(*model.TruncateTableArgs).NewTableID
		return strconv.FormatInt(jobW.TableID, 10) + "," + strconv.FormatInt(newTableID, 10)
	default:
		return strconv.FormatInt(jobW.TableID, 10)
	}
}

func updateDDLJob2Table(se *sess.Session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(updateDDLJobSQL, util.WrapKey2String(b), job.ID)
	_, err = se.Execute(context.Background(), sql, "update_job")
	return errors.Trace(err)
}

// getDDLReorgHandle gets DDL reorg handle.
func getDDLReorgHandle(se *sess.Session, job *model.Job) (element *meta.Element,
	startKey, endKey kv.Key, physicalTableID int64, err error) {
	sql := fmt.Sprintf("select ele_id, ele_type, start_key, end_key, physical_id, reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := se.Execute(ctx, sql, "get_handle")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, nil, 0, meta.ErrDDLReorgElementNotExist
	}
	id := rows[0].GetInt64(0)
	tp := rows[0].GetBytes(1)
	element = &meta.Element{
		ID:      id,
		TypeKey: tp,
	}
	startKey = rows[0].GetBytes(2)
	endKey = rows[0].GetBytes(3)
	physicalTableID = rows[0].GetInt64(4)
	return
}

func getImportedKeyFromCheckpoint(se *sess.Session, job *model.Job) (imported kv.Key, physicalTableID int64, err error) {
	sql := fmt.Sprintf("select reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := se.Execute(ctx, sql, "get_handle")
	if err != nil {
		return nil, 0, err
	}
	if len(rows) == 0 {
		return nil, 0, meta.ErrDDLReorgElementNotExist
	}
	if !rows[0].IsNull(0) {
		rawReorgMeta := rows[0].GetBytes(0)
		var reorgMeta ingest.JobReorgMeta
		err = json.Unmarshal(rawReorgMeta, &reorgMeta)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		if cp := reorgMeta.Checkpoint; cp != nil {
			logutil.DDLIngestLogger().Info("resume physical table ID from checkpoint",
				zap.Int64("jobID", job.ID),
				zap.String("global sync key", hex.EncodeToString(cp.GlobalSyncKey)),
				zap.Int64("checkpoint physical ID", cp.PhysicalID))
			return cp.GlobalSyncKey, cp.PhysicalID, nil
		}
	}
	return
}

// updateDDLReorgHandle update startKey, endKey physicalTableID and element of the handle.
// Caller should wrap this in a separate transaction, to avoid conflicts.
func updateDDLReorgHandle(se *sess.Session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s, end_key = %s, physical_id = %d where job_id = %d",
		element.ID, util.WrapKey2String(element.TypeKey), util.WrapKey2String(startKey), util.WrapKey2String(endKey), physicalTableID, jobID)
	_, err := se.Execute(context.Background(), sql, "update_handle")
	return err
}

// initDDLReorgHandle initializes the handle for ddl reorg.
func initDDLReorgHandle(s *sess.Session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	rawReorgMeta, err := json.Marshal(ingest.JobReorgMeta{
		Checkpoint: &ingest.ReorgCheckpoint{
			PhysicalID: physicalTableID,
			Version:    ingest.JobCheckpointVersionCurrent,
		}})
	if err != nil {
		return errors.Trace(err)
	}
	del := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", jobID)
	ins := fmt.Sprintf("insert into mysql.tidb_ddl_reorg(job_id, ele_id, ele_type, start_key, end_key, physical_id, reorg_meta) values (%d, %d, %s, %s, %s, %d, %s)",
		jobID, element.ID, util.WrapKey2String(element.TypeKey), util.WrapKey2String(startKey), util.WrapKey2String(endKey), physicalTableID, util.WrapKey2String(rawReorgMeta))
	return s.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), del, "init_handle")
		if err != nil {
			logutil.DDLLogger().Info("initDDLReorgHandle failed to delete", zap.Int64("jobID", jobID), zap.Error(err))
		}
		_, err = se.Execute(context.Background(), ins, "init_handle")
		return err
	})
}

// deleteDDLReorgHandle deletes the handle for ddl reorg.
func removeDDLReorgHandle(se *sess.Session, job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// removeReorgElement removes the element from ddl reorg, it is the same with removeDDLReorgHandle, only used in failpoint
func removeReorgElement(se *sess.Session, job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// cleanDDLReorgHandles removes handles that are no longer needed.
func cleanDDLReorgHandles(se *sess.Session, job *model.Job) error {
	sql := "delete from mysql.tidb_ddl_reorg where job_id = " + strconv.FormatInt(job.ID, 10)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "clean_handle")
		return err
	})
}

func getJobsBySQL(se *sess.Session, tbl, condition string) ([]*model.Job, error) {
	rows, err := se.Execute(context.Background(), fmt.Sprintf("select job_meta from mysql.%s where %s", tbl, condition), "get_job")
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, 0, 16)
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := model.Job{}
		err := job.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}
