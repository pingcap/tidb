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
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/syncer"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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
	localWorkerWaitingDuration  = 10 * time.Millisecond
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
	case jobTypeLocal:
		return "local"
	}
	return "unknown job type: " + strconv.Itoa(int(t))
}

const (
	jobTypeGeneral jobType = iota
	jobTypeReorg
	jobTypeLocal
)

type ownerListener struct {
	ddl       *ddl
	scheduler *jobScheduler
}

var _ owner.Listener = (*ownerListener)(nil)

func (l *ownerListener) OnBecomeOwner() {
	ctx, cancelFunc := context.WithCancel(l.ddl.ddlCtx.ctx)
	l.scheduler = &jobScheduler{
		schCtx:      ctx,
		cancel:      cancelFunc,
		runningJobs: newRunningJobs(),

		ddlCtx:         l.ddl.ddlCtx,
		ddlJobNotifyCh: l.ddl.ddlJobNotifyCh,
		sessPool:       l.ddl.sessPool,
		delRangeMgr:    l.ddl.delRangeMgr,
	}
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
	schCtx      context.Context
	cancel      context.CancelFunc
	wg          tidbutil.WaitGroupWrapper
	runningJobs *runningJobs

	// those fields are created on start
	reorgWorkerPool      *workerPool
	generalDDLWorkerPool *workerPool

	// those fields are shared with 'ddl' instance
	// TODO ddlCtx is too large for here, we should remove dependency on it.
	*ddlCtx
	ddlJobNotifyCh chan struct{}
	sessPool       *sess.Pool
	delRangeMgr    delRangeManager
}

func (s *jobScheduler) start() {
	var err error
	s.ddlCtx.ddlSeqNumMu.Lock()
	defer s.ddlCtx.ddlSeqNumMu.Unlock()
	s.ddlCtx.ddlSeqNumMu.seqNum, err = s.GetNextDDLSeqNum()
	if err != nil {
		logutil.DDLLogger().Error("error when getting the ddl history count", zap.Error(err))
	}

	workerFactory := func(tp workerType) func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newWorker(s.schCtx, tp, s.sessPool, s.delRangeMgr, s.ddlCtx)
			sessForJob, err := s.sessPool.Get()
			if err != nil {
				return nil, err
			}
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
	s.wg.RunWithLog(s.startDispatchLoop)
	s.wg.RunWithLog(func() {
		s.schemaSyncer.SyncJobSchemaVerLoop(s.schCtx)
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
}

func (s *jobScheduler) getJob(se *sess.Session, tp jobType, filter func(*model.Job) (bool, error)) (*model.Job, error) {
	not := "not"
	label := "get_job_general"
	if tp == jobTypeReorg {
		not = ""
		label = "get_job_reorg"
	}
	const getJobSQL = `select job_meta, processing from mysql.tidb_ddl_job where job_id in
		(select min(job_id) from mysql.tidb_ddl_job group by schema_ids, table_ids, processing)
		and %s reorg %s order by processing desc, job_id`
	var excludedJobIDs string
	if ids := s.runningJobs.allIDs(); len(ids) > 0 {
		excludedJobIDs = fmt.Sprintf("and job_id not in (%s)", ids)
	}
	sql := fmt.Sprintf(getJobSQL, not, excludedJobIDs)
	rows, err := se.Execute(context.Background(), sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		isJobProcessing := row.GetInt64(1) == 1

		job := model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}

		isRunnable, err := s.processJobDuringUpgrade(se, &job)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !isRunnable {
			continue
		}

		// The job has already been picked up, just return to continue it.
		if isJobProcessing {
			return &job, nil
		}

		b, err := filter(&job)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if b {
			if err = s.markJobProcessing(se, &job); err != nil {
				logutil.DDLLogger().Warn(
					"[ddl] handle ddl job failed: mark job is processing meet error",
					zap.Error(err),
					zap.Stringer("job", &job))
				return nil, errors.Trace(err)
			}
			return &job, nil
		}
	}
	return nil, nil
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
	if s.stateSyncer.IsUpgradingState() {
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

func (s *jobScheduler) getGeneralJob(sess *sess.Session) (*model.Job, error) {
	return s.getJob(sess, jobTypeGeneral, func(job *model.Job) (bool, error) {
		if !s.runningJobs.checkRunnable(job) {
			return false, nil
		}
		if job.Type == model.ActionDropSchema {
			// Check if there is any reorg job on this schema.
			sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing limit 1", strconv.Quote(strconv.FormatInt(job.SchemaID, 10)))
			rows, err := sess.Execute(s.schCtx, sql, "check conflict jobs")
			return len(rows) == 0, err
		}
		// Check if there is any running job works on the same table.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job t1, (select table_ids from mysql.tidb_ddl_job where job_id = %d) t2 where "+
			"(processing and CONCAT(',', t2.table_ids, ',') REGEXP CONCAT(',(', REPLACE(t1.table_ids, ',', '|'), '),') != 0)"+
			"or (type = %d and processing)", job.ID, model.ActionFlashbackCluster)
		rows, err := sess.Execute(s.schCtx, sql, "check conflict jobs")
		return len(rows) == 0, err
	})
}

func (s *jobScheduler) getReorgJob(sess *sess.Session) (*model.Job, error) {
	return s.getJob(sess, jobTypeReorg, func(job *model.Job) (bool, error) {
		if !s.runningJobs.checkRunnable(job) {
			return false, nil
		}
		// Check if there is any block ddl running, like drop schema and flashback cluster.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where "+
			"(CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and type = %d and processing) "+
			"or (CONCAT(',', table_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing) "+
			"or (type = %d and processing) limit 1",
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), model.ActionDropSchema, strconv.Quote(strconv.FormatInt(job.TableID, 10)), model.ActionFlashbackCluster)
		rows, err := sess.Execute(s.schCtx, sql, "check conflict jobs")
		return len(rows) == 0, err
	})
}

// startLocalWorkerLoop starts the local worker loop to run the DDL job of v2.
func (d *ddl) startLocalWorkerLoop() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case task, ok := <-d.localJobCh:
			if !ok {
				return
			}
			d.delivery2LocalWorker(d.localWorkerPool, task)
		}
	}
}

func (s *jobScheduler) startDispatchLoop() {
	sessCtx, err := s.sessPool.Get()
	if err != nil {
		logutil.DDLLogger().Fatal("dispatch loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	defer s.sessPool.Put(sessCtx)
	se := sess.NewSession(sessCtx)
	var notifyDDLJobByEtcdCh clientv3.WatchChan
	if s.etcdCli != nil {
		notifyDDLJobByEtcdCh = s.etcdCli.Watch(s.schCtx, addingDDLJobNotifyKey)
	}
	if err := s.checkAndUpdateClusterState(true); err != nil {
		logutil.DDLLogger().Fatal("dispatch loop get cluster state failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	ticker := time.NewTicker(dispatchLoopWaitingDuration)
	defer ticker.Stop()
	// TODO move waitSchemaSyncedController out of ddlCtx.
	s.clearOnceMap()
	for {
		if s.schCtx.Err() != nil {
			return
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
			return
		}
		if err := s.checkAndUpdateClusterState(false); err != nil {
			continue
		}
		s.loadDDLJobAndRun(se, s.generalDDLWorkerPool, s.getGeneralJob)
		s.loadDDLJobAndRun(se, s.reorgWorkerPool, s.getReorgJob)
	}
}

// TODO make it run in a separate routine.
func (s *jobScheduler) checkAndUpdateClusterState(needUpdate bool) error {
	select {
	case _, ok := <-s.stateSyncer.WatchChan():
		if !ok {
			// TODO stateSyncer should only be started when we are the owner, and use
			// the context of scheduler, will refactor it later.
			s.stateSyncer.Rewatch(s.ddlCtx.ctx)
		}
	default:
		if !needUpdate {
			return nil
		}
	}

	oldState := s.stateSyncer.IsUpgradingState()
	stateInfo, err := s.stateSyncer.GetGlobalState(s.schCtx)
	if err != nil {
		logutil.DDLLogger().Warn("get global state failed", zap.Error(err))
		return errors.Trace(err)
	}
	logutil.DDLLogger().Info("get global state and global state change",
		zap.Bool("oldState", oldState), zap.Bool("currState", s.stateSyncer.IsUpgradingState()))

	ownerOp := owner.OpNone
	if stateInfo.State == syncer.StateUpgrading {
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

func (s *jobScheduler) loadDDLJobAndRun(se *sess.Session, pool *workerPool, getJob func(*sess.Session) (*model.Job, error)) {
	wk, err := pool.get()
	if err != nil || wk == nil {
		logutil.DDLLogger().Debug(fmt.Sprintf("[ddl] no %v worker available now", pool.tp()), zap.Error(err))
		return
	}

	s.mu.RLock()
	s.mu.hook.OnGetJobBefore(pool.tp().String())
	s.mu.RUnlock()

	startTime := time.Now()
	job, err := getJob(se)
	if job == nil || err != nil {
		if err != nil {
			wk.jobLogger(job).Warn("get job met error", zap.Duration("take time", time.Since(startTime)), zap.Error(err))
		}
		pool.put(wk)
		return
	}
	s.mu.RLock()
	s.mu.hook.OnGetJobAfter(pool.tp().String(), job)
	s.mu.RUnlock()

	s.delivery2Worker(wk, pool, job)
}

// delivery2LocalWorker runs the DDL job of v2 in local.
// send the result to the error channels in the task.
// delivery2Localworker owns the worker, need to put it back to the pool in this function.
func (d *ddl) delivery2LocalWorker(pool *workerPool, task *limitJobTask) {
	job := task.job
	wk, err := pool.get()
	if err != nil {
		task.NotifyError(err)
		return
	}
	for wk == nil {
		select {
		case <-d.ctx.Done():
			return
		case <-time.After(localWorkerWaitingDuration):
		}
		wk, err = pool.get()
		if err != nil {
			task.NotifyError(err)
			return
		}
	}
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
		defer func() {
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
		}()

		err := wk.HandleLocalDDLJob(d.ddlCtx, job)
		pool.put(wk)
		if err != nil {
			logutil.DDLLogger().Info("handle ddl job failed", zap.Error(err), zap.Stringer("job", job))
		}
		task.NotifyError(err)
	})
}

// delivery2Worker owns the worker, need to put it back to the pool in this function.
func (s *jobScheduler) delivery2Worker(wk *worker, pool *workerPool, job *model.Job) {
	injectFailPointForGetJob(job)
	s.runningJobs.add(job)
	s.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
		defer func() {
			failpoint.InjectCall("afterDelivery2Worker", job)
			s.runningJobs.remove(job)
			asyncNotify(s.ddlJobNotifyCh)
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
			if wk.ctx.Err() != nil && ingest.LitBackCtxMgr != nil {
				// if ctx cancelled, i.e. owner changed, we need to Unregister the backend
				// as litBackendCtx is holding this very 'ctx', and it cannot reuse now.
				// TODO make LitBackCtxMgr a local value of the job scheduler, it makes
				// it much harder to test multiple owners in 1 unit test.
				ingest.LitBackCtxMgr.Unregister(job.ID)
			}
		}()
		ownerID := s.ownerManager.ID()
		// check if this ddl job is synced to all servers.
		if !job.NotStarted() && (!s.isSynced(job) || !s.maybeAlreadyRunOnce(job.ID)) {
			if variable.EnableMDL.Load() {
				exist, version, err := checkMDLInfo(job.ID, s.sessPool)
				if err != nil {
					wk.jobLogger(job).Warn("check MDL info failed", zap.Error(err))
					// Release the worker resource.
					pool.put(wk)
					return
				} else if exist {
					// Release the worker resource.
					pool.put(wk)
					err = waitSchemaSyncedForMDL(wk.ctx, s.ddlCtx, job, version)
					if err != nil {
						return
					}
					s.setAlreadyRunOnce(job.ID)
					cleanMDLInfo(s.sessPool, job, s.etcdCli, ownerID, job.State == model.JobStateSynced)
					// Don't have a worker now.
					return
				}
			} else {
				err := waitSchemaSynced(wk.ctx, s.ddlCtx, job, 2*s.lease)
				if err != nil {
					time.Sleep(time.Second)
					// Release the worker resource.
					pool.put(wk)
					return
				}
				s.setAlreadyRunOnce(job.ID)
			}
		}

		schemaVer, err := wk.HandleDDLJobTable(s.ddlCtx, job)
		logCtx := wk.logCtx
		pool.put(wk)
		if err != nil {
			tidblogutil.Logger(logCtx).Info("handle ddl job failed", zap.Error(err), zap.Stringer("job", job))
		} else {
			failpoint.Inject("mockDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
				if val.(bool) {
					if mockDDLErrOnce == 0 {
						mockDDLErrOnce = schemaVer
						failpoint.Return()
					}
				}
			})

			// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
			// If the job is done or still running or rolling back, we will wait 2 * lease time or util MDL synced to guarantee other servers to update
			// the newest schema.
			err := waitSchemaChanged(wk.ctx, s.ddlCtx, s.lease*2, schemaVer, job)
			if err != nil {
				return
			}
			cleanMDLInfo(s.sessPool, job, s.etcdCli, ownerID, job.State == model.JobStateSynced)
			s.synced(job)

			if RunInGoTest {
				// s.mu.hook is initialed from domain / test callback, which will force the owner host update schema diff synchronously.
				s.mu.RLock()
				s.mu.hook.OnSchemaStateChanged(schemaVer)
				s.mu.RUnlock()
			}

			s.mu.RLock()
			s.mu.hook.OnJobUpdated(job)
			s.mu.RUnlock()
		}
	})
}

func (*jobScheduler) markJobProcessing(se *sess.Session, job *model.Job) error {
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := se.Execute(context.Background(), fmt.Sprintf(
		"update mysql.tidb_ddl_job set processing = 1 where job_id = %d", job.ID),
		"mark_job_processing")
	return errors.Trace(err)
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

func insertDDLJobs2Table(se *sess.Session, updateRawArgs bool, jobs ...*model.Job) error {
	failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	if len(jobs) == 0 {
		return nil
	}
	var sql bytes.Buffer
	sql.WriteString(addDDLJobSQL)
	for i, job := range jobs {
		b, err := job.Encode(updateRawArgs)
		if err != nil {
			return err
		}
		if i != 0 {
			sql.WriteString(",")
		}
		fmt.Fprintf(&sql, "(%d, %t, %s, %s, %s, %d, %t)", job.ID, job.MayNeedReorg(), strconv.Quote(job2SchemaIDs(job)), strconv.Quote(job2TableIDs(job)), util.WrapKey2String(b), job.Type, !job.NotStarted())
	}
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := se.Execute(ctx, sql.String(), "insert_job")
	logutil.DDLLogger().Debug("add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
	return errors.Trace(err)
}

func job2SchemaIDs(job *model.Job) string {
	return job2UniqueIDs(job, true)
}

func job2TableIDs(job *model.Job) string {
	return job2UniqueIDs(job, false)
}

func job2UniqueIDs(job *model.Job, schema bool) string {
	switch job.Type {
	case model.ActionExchangeTablePartition, model.ActionRenameTables, model.ActionRenameTable:
		var ids []int64
		if schema {
			ids = job.CtxVars[0].([]int64)
		} else {
			ids = job.CtxVars[1].([]int64)
		}
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
	case model.ActionTruncateTable:
		if schema {
			return strconv.FormatInt(job.SchemaID, 10)
		}
		return strconv.FormatInt(job.TableID, 10) + "," + strconv.FormatInt(job.Args[0].(int64), 10)
	}
	if schema {
		return strconv.FormatInt(job.SchemaID, 10)
	}
	return strconv.FormatInt(job.TableID, 10)
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sess.Execute(context.Background(), sql, "delete_job")
	return errors.Trace(err)
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

func getCheckpointReorgHandle(se *sess.Session, job *model.Job) (startKey, endKey kv.Key, physicalTableID int64, err error) {
	startKey, endKey = kv.Key{}, kv.Key{}
	sql := fmt.Sprintf("select reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := se.Execute(ctx, sql, "get_handle")
	if err != nil {
		return nil, nil, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, 0, meta.ErrDDLReorgElementNotExist
	}
	if !rows[0].IsNull(0) {
		rawReorgMeta := rows[0].GetBytes(0)
		var reorgMeta ingest.JobReorgMeta
		err = json.Unmarshal(rawReorgMeta, &reorgMeta)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		if cp := reorgMeta.Checkpoint; cp != nil {
			logutil.DDLIngestLogger().Info("resume physical table ID from checkpoint",
				zap.Int64("jobID", job.ID),
				zap.String("start", hex.EncodeToString(cp.StartKey)),
				zap.String("end", hex.EncodeToString(cp.EndKey)),
				zap.Int64("checkpoint physical ID", cp.PhysicalID))
			physicalTableID = cp.PhysicalID
			if len(cp.StartKey) > 0 {
				startKey = cp.StartKey
			}
			if len(cp.EndKey) > 0 {
				endKey = cp.EndKey
				endKey = adjustEndKeyAcrossVersion(job, endKey)
			}
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
			StartKey:   startKey,
			EndKey:     endKey,
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

func filterProcessingJobIDs(se *sess.Session, jobIDs []int64) ([]int64, error) {
	if len(jobIDs) == 0 {
		return nil, nil
	}

	var sb strings.Builder
	for i, id := range jobIDs {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strconv.FormatInt(id, 10))
	}
	sql := fmt.Sprintf(
		"SELECT job_id FROM mysql.tidb_ddl_job WHERE job_id IN (%s) AND processing",
		sb.String())
	rows, err := se.Execute(context.Background(), sql, "filter_processing_job_ids")
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make([]int64, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.GetInt64(0))
	}
	return ret, nil
}
