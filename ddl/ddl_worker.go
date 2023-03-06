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

package ddl

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"github.com/pingcap/tidb/util/topsql"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/tikv/client-go/v2/tikvrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	// ddlWorkerID is used for generating the next DDL worker ID.
	ddlWorkerID = atomicutil.NewInt32(0)
	// backfillContextID is used for generating the next backfill context ID.
	backfillContextID = atomicutil.NewInt32(0)
	// WaitTimeWhenErrorOccurred is waiting interval when processing DDL jobs encounter errors.
	WaitTimeWhenErrorOccurred = int64(1 * time.Second)

	mockDDLErrOnce = int64(0)
	// TestNotifyBeginTxnCh is used for if the txn is beginning in runInTxn.
	TestNotifyBeginTxnCh = make(chan struct{})
)

// GetWaitTimeWhenErrorOccurred return waiting interval when processing DDL jobs encounter errors.
func GetWaitTimeWhenErrorOccurred() time.Duration {
	return time.Duration(atomic.LoadInt64(&WaitTimeWhenErrorOccurred))
}

// SetWaitTimeWhenErrorOccurred update waiting interval when processing DDL jobs encounter errors.
func SetWaitTimeWhenErrorOccurred(dur time.Duration) {
	atomic.StoreInt64(&WaitTimeWhenErrorOccurred, int64(dur))
}

type workerType byte

const (
	// generalWorker is the worker who handles all DDL statements except “add index”.
	generalWorker workerType = 0
	// addIdxWorker is the worker who handles the operation of adding indexes.
	addIdxWorker workerType = 1
	// waitDependencyJobInterval is the interval when the dependency job doesn't be done.
	waitDependencyJobInterval = 200 * time.Millisecond
	// noneDependencyJob means a job has no dependency-job.
	noneDependencyJob = 0
)

// worker is used for handling DDL jobs.
// Now we have two kinds of workers.
type worker struct {
	id              int32
	tp              workerType
	addingDDLJobKey string
	ddlJobCh        chan struct{}
	ctx             context.Context
	wg              sync.WaitGroup

	sessPool        *sessionPool // sessPool is used to new sessions to execute SQL in ddl package.
	sess            *session     // sess is used and only used in running DDL job.
	delRangeManager delRangeManager
	logCtx          context.Context
	lockSeqNum      bool

	*ddlCtx
}

// JobContext is the ddl job execution context.
type JobContext struct {
	// below fields are cache for top sql
	ddlJobCtx          context.Context
	cacheSQL           string
	cacheNormalizedSQL string
	cacheDigest        *parser.Digest
	tp                 string
}

// NewJobContext returns a new ddl job context.
func NewJobContext() *JobContext {
	return &JobContext{
		ddlJobCtx:          context.Background(),
		cacheSQL:           "",
		cacheNormalizedSQL: "",
		cacheDigest:        nil,
		tp:                 "",
	}
}

func newWorker(ctx context.Context, tp workerType, sessPool *sessionPool, delRangeMgr delRangeManager, dCtx *ddlCtx) *worker {
	worker := &worker{
		id:              ddlWorkerID.Add(1),
		tp:              tp,
		ddlJobCh:        make(chan struct{}, 1),
		ctx:             ctx,
		ddlCtx:          dCtx,
		sessPool:        sessPool,
		delRangeManager: delRangeMgr,
	}
	worker.addingDDLJobKey = addingDDLJobPrefix + worker.typeStr()
	worker.logCtx = logutil.WithKeyValue(context.Background(), "worker", worker.String())
	return worker
}

func (w *worker) typeStr() string {
	var str string
	switch w.tp {
	case generalWorker:
		str = "general"
	case addIdxWorker:
		str = "add index"
	default:
		str = "unknown"
	}
	return str
}

func (w *worker) String() string {
	return fmt.Sprintf("worker %d, tp %s", w.id, w.typeStr())
}

func (w *worker) Close() {
	startTime := time.Now()
	if w.sess != nil {
		w.sessPool.put(w.sess.session())
	}
	w.wg.Wait()
	logutil.Logger(w.logCtx).Info("[ddl] DDL worker closed", zap.Duration("take time", time.Since(startTime)))
}

func (d *ddlCtx) asyncNotifyByEtcd(etcdPath string, jobID int64, jobType string) {
	if d.etcdCli == nil {
		return
	}

	jobIDStr := strconv.FormatInt(jobID, 10)
	timeStart := time.Now()
	err := util.PutKVToEtcd(d.ctx, d.etcdCli, 1, etcdPath, jobIDStr)
	if err != nil {
		logutil.BgLogger().Info("[ddl] notify handling DDL job failed",
			zap.String("etcdPath", etcdPath), zap.Int64("jobID", jobID), zap.String("type", jobType), zap.Error(err))
	}
	metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerNotifyDDLJob, jobType, metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *ddl) limitDDLJobs() {
	defer tidbutil.Recover(metrics.LabelDDL, "limitDDLJobs", nil, true)

	tasks := make([]*limitJobTask, 0, batchAddingJobs)
	for {
		select {
		case task := <-d.limitJobCh:
			tasks = tasks[:0]
			jobLen := len(d.limitJobCh)
			tasks = append(tasks, task)
			for i := 0; i < jobLen; i++ {
				tasks = append(tasks, <-d.limitJobCh)
			}
			d.addBatchDDLJobs(tasks)
		case <-d.ctx.Done():
			return
		}
	}
}

// addBatchDDLJobs gets global job IDs and puts the DDL jobs in the DDL queue.
func (d *ddl) addBatchDDLJobs(tasks []*limitJobTask) {
	startTime := time.Now()
	var err error
	// DDLForce2Queue is a flag to tell DDL worker to always push the job to the DDL queue.
	toTable := !variable.DDLForce2Queue.Load()
	if toTable {
		err = d.addBatchDDLJobs2Table(tasks)
	} else {
		err = d.addBatchDDLJobs2Queue(tasks)
	}
	var jobs string
	for _, task := range tasks {
		task.err <- err
		jobs += task.job.String() + "; "
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, task.job.Type.String(),
			metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}
	if err != nil {
		logutil.BgLogger().Warn("[ddl] add DDL jobs failed", zap.String("jobs", jobs), zap.Error(err))
	} else {
		logutil.BgLogger().Info("[ddl] add DDL jobs", zap.Int("batch count", len(tasks)), zap.String("jobs", jobs), zap.Bool("table", toTable))
	}
}

// buildJobDependence sets the curjob's dependency-ID.
// The dependency-job's ID must less than the current job's ID, and we need the largest one in the list.
func buildJobDependence(t *meta.Meta, curJob *model.Job) error {
	// Jobs in the same queue are ordered. If we want to find a job's dependency-job, we need to look for
	// it from the other queue. So if the job is "ActionAddIndex" job, we need find its dependency-job from DefaultJobList.
	jobListKey := meta.DefaultJobListKey
	if !curJob.MayNeedReorg() {
		jobListKey = meta.AddIndexJobListKey
	}
	jobs, err := t.GetAllDDLJobsInQueue(jobListKey)
	if err != nil {
		return errors.Trace(err)
	}

	for _, job := range jobs {
		if curJob.ID < job.ID {
			continue
		}
		isDependent, err := curJob.IsDependentOn(job)
		if err != nil {
			return errors.Trace(err)
		}
		if isDependent {
			logutil.BgLogger().Info("[ddl] current DDL job depends on other job", zap.String("currentJob", curJob.String()), zap.String("dependentJob", job.String()))
			curJob.DependencyID = job.ID
			break
		}
	}
	return nil
}

func (d *ddl) addBatchDDLJobs2Queue(tasks []*limitJobTask) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	return kv.RunInNewTxn(ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ids, err := t.GenGlobalIDs(len(tasks))
		if err != nil {
			return errors.Trace(err)
		}

		jobs, err := t.GetAllDDLJobsInQueue(meta.DefaultJobListKey)
		if err != nil {
			return errors.Trace(err)
		}
		for _, job := range jobs {
			if job.Type == model.ActionFlashbackCluster {
				return errors.Errorf("Can't add ddl job, have flashback cluster job")
			}
		}

		for i, task := range tasks {
			job := task.job
			job.Version = currentVersion
			job.StartTS = txn.StartTS()
			job.ID = ids[i]
			setJobStateToQueueing(job)
			if err = buildJobDependence(t, job); err != nil {
				return errors.Trace(err)
			}
			jobListKey := meta.DefaultJobListKey
			if job.MayNeedReorg() {
				jobListKey = meta.AddIndexJobListKey
			}
			injectModifyJobArgFailPoint(job)
			if err = t.EnQueueDDLJob(job, jobListKey); err != nil {
				return errors.Trace(err)
			}
		}
		failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
			}
		})
		return nil
	})
}

func injectModifyJobArgFailPoint(job *model.Job) {
	failpoint.Inject("MockModifyJobArg", func(val failpoint.Value) {
		if val.(bool) {
			// Corrupt the DDL job argument.
			if job.Type == model.ActionMultiSchemaChange {
				if len(job.MultiSchemaInfo.SubJobs) > 0 && len(job.MultiSchemaInfo.SubJobs[0].Args) > 0 {
					job.MultiSchemaInfo.SubJobs[0].Args[0] = 1
				}
			} else if len(job.Args) > 0 {
				job.Args[0] = 1
			}
		}
	})
}

func setJobStateToQueueing(job *model.Job) {
	if job.Type == model.ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			sub.State = model.JobStateQueueing
		}
	}
	job.State = model.JobStateQueueing
}

// addBatchDDLJobs2Table gets global job IDs and puts the DDL jobs in the DDL job table.
func (d *ddl) addBatchDDLJobs2Table(tasks []*limitJobTask) error {
	var ids []int64
	var err error

	sess, err := d.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer d.sessPool.put(sess)
	job, err := getJobsBySQL(newSession(sess), JobTable, fmt.Sprintf("type = %d", model.ActionFlashbackCluster))
	if err != nil {
		return errors.Trace(err)
	}
	if len(job) != 0 {
		return errors.Errorf("Can't add ddl job, have flashback cluster job")
	}

	startTS := uint64(0)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ids, err = t.GenGlobalIDs(len(tasks))
		if err != nil {
			return errors.Trace(err)
		}
		startTS = txn.StartTS()
		return nil
	})
	if err == nil {
		jobTasks := make([]*model.Job, len(tasks))
		for i, task := range tasks {
			job := task.job
			job.Version = currentVersion
			job.StartTS = startTS
			job.ID = ids[i]
			setJobStateToQueueing(job)
			jobTasks[i] = job
			injectModifyJobArgFailPoint(job)
		}

		sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
		err = insertDDLJobs2Table(newSession(sess), true, jobTasks...)
	}
	return errors.Trace(err)
}

func injectFailPointForGetJob(job *model.Job) {
	if job == nil {
		return
	}
	failpoint.Inject("mockModifyJobSchemaId", func(val failpoint.Value) {
		job.SchemaID = int64(val.(int))
	})
	failpoint.Inject("MockModifyJobTableId", func(val failpoint.Value) {
		job.TableID = int64(val.(int))
	})
}

// handleUpdateJobError handles the too large DDL job.
func (w *worker) handleUpdateJobError(t *meta.Meta, job *model.Job, err error) error {
	if err == nil {
		return nil
	}
	if kv.ErrEntryTooLarge.Equal(err) {
		logutil.Logger(w.logCtx).Warn("[ddl] update DDL job failed", zap.String("job", job.String()), zap.Error(err))
		// Reduce this txn entry size.
		job.BinlogInfo.Clean()
		job.Error = toTError(err)
		job.ErrorCount++
		job.SchemaState = model.StateNone
		job.State = model.JobStateCancelled
		err = w.finishDDLJob(t, job)
	}
	return errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
// Every time we enter another state except final state, we must call this function.
func (w *worker) updateDDLJob(job *model.Job, meetErr bool) error {
	failpoint.Inject("mockErrEntrySizeTooLarge", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(kv.ErrEntryTooLarge)
		}
	})
	updateRawArgs := needUpdateRawArgs(job, meetErr)
	if !updateRawArgs {
		logutil.Logger(w.logCtx).Info("[ddl] meet something wrong before update DDL job, shouldn't update raw args",
			zap.String("job", job.String()))
	}
	return errors.Trace(updateDDLJob2Table(w.sess, job, updateRawArgs))
}

// registerMDLInfo registers metadata lock info.
func (w *worker) registerMDLInfo(job *model.Job, ver int64) error {
	if !variable.EnableMDL.Load() {
		return nil
	}
	if ver == 0 {
		return nil
	}
	rows, err := w.sess.execute(context.Background(), fmt.Sprintf("select table_ids from mysql.tidb_ddl_job where job_id = %d", job.ID), "register-mdl-info")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.Errorf("can't find ddl job %d", job.ID)
	}
	ids := rows[0].GetString(0)
	sql := fmt.Sprintf("replace into mysql.tidb_mdl_info (job_id, version, table_ids) values (%d, %d, '%s')", job.ID, ver, ids)
	_, err = w.sess.execute(context.Background(), sql, "register-mdl-info")
	return err
}

// cleanMDLInfo cleans metadata lock info.
func cleanMDLInfo(pool *sessionPool, jobID int64, ec *clientv3.Client) {
	if !variable.EnableMDL.Load() {
		return
	}
	sql := fmt.Sprintf("delete from mysql.tidb_mdl_info where job_id = %d", jobID)
	sctx, _ := pool.get()
	defer pool.put(sctx)
	sess := newSession(sctx)
	sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := sess.execute(context.Background(), sql, "delete-mdl-info")
	if err != nil {
		logutil.BgLogger().Warn("unexpected error when clean mdl info", zap.Error(err))
	}
	if ec != nil {
		path := fmt.Sprintf("%s/%d/", util.DDLAllSchemaVersionsByJob, jobID)
		_, err = ec.Delete(context.Background(), path, clientv3.WithPrefix())
		if err != nil {
			logutil.BgLogger().Warn("[ddl] delete versions failed", zap.Any("job id", jobID), zap.Error(err))
		}
	}
}

// checkMDLInfo checks if metadata lock info exists. It means the schema is locked by some TiDBs if exists.
func checkMDLInfo(jobID int64, pool *sessionPool) (bool, int64, error) {
	sql := fmt.Sprintf("select version from mysql.tidb_mdl_info where job_id = %d", jobID)
	sctx, _ := pool.get()
	defer pool.put(sctx)
	sess := newSession(sctx)
	rows, err := sess.execute(context.Background(), sql, "check-mdl-info")
	if err != nil {
		return false, 0, err
	}
	if len(rows) == 0 {
		return false, 0, nil
	}
	ver := rows[0].GetInt64(0)
	return true, ver, nil
}

func needUpdateRawArgs(job *model.Job, meetErr bool) bool {
	// If there is an error when running job and the RawArgs hasn't been decoded by DecodeArgs,
	// we shouldn't replace RawArgs with the marshaling Args.
	if meetErr && job.RawArgs != nil && job.Args == nil {
		// However, for multi-schema change, the args of the parent job is always nil.
		// Since Job.Encode() can handle the sub-jobs properly, we can safely update the raw args.
		return job.MultiSchemaInfo != nil
	}
	return true
}

func (w *worker) deleteRange(ctx context.Context, job *model.Job) error {
	var err error
	if job.Version <= currentVersion {
		err = w.delRangeManager.addDelRangeJob(ctx, job)
	} else {
		err = dbterror.ErrInvalidDDLJobVersion.GenWithStackByArgs(job.Version, currentVersion)
	}
	return errors.Trace(err)
}

func jobNeedGC(job *model.Job) bool {
	if !job.IsCancelled() {
		if job.Warning != nil && dbterror.ErrCantDropFieldOrKey.Equal(job.Warning) {
			// For the field/key not exists warnings, there is no need to
			// delete the ranges.
			return false
		}
		switch job.Type {
		case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable, model.ActionDropIndex, model.ActionDropPrimaryKey,
			model.ActionDropTablePartition, model.ActionTruncateTablePartition, model.ActionDropColumn, model.ActionModifyColumn,
			model.ActionAddIndex, model.ActionAddPrimaryKey,
			model.ActionReorganizePartition:
			return true
		case model.ActionMultiSchemaChange:
			for _, sub := range job.MultiSchemaInfo.SubJobs {
				proxyJob := sub.ToProxyJob(job)
				needGC := jobNeedGC(&proxyJob)
				if needGC {
					return true
				}
			}
			return false
		}
	}
	return false
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (w *worker) finishDDLJob(t *meta.Meta, job *model.Job) (err error) {
	startTime := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerFinishDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	if jobNeedGC(job) {
		err = w.deleteRange(w.ctx, job)
		if err != nil {
			return errors.Trace(err)
		}
	}

	switch job.Type {
	case model.ActionRecoverTable:
		err = finishRecoverTable(w, job)
	case model.ActionFlashbackCluster:
		err = finishFlashbackCluster(w, job)
	case model.ActionRecoverSchema:
		err = finishRecoverSchema(w, job)
	case model.ActionCreateTables:
		if job.IsCancelled() {
			// it may be too large that it can not be added to the history queue, too
			// delete its arguments
			job.Args = nil
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	err = w.deleteDDLJob(job)
	if err != nil {
		return errors.Trace(err)
	}

	job.BinlogInfo.FinishedTS = t.StartTS
	logutil.Logger(w.logCtx).Info("[ddl] finish DDL job", zap.String("job", job.String()))
	updateRawArgs := true
	if job.Type == model.ActionAddPrimaryKey && !job.IsCancelled() {
		// ActionAddPrimaryKey needs to check the warnings information in job.Args.
		// Notice: warnings is used to support non-strict mode.
		updateRawArgs = false
	}
	w.writeDDLSeqNum(job)
	w.removeJobCtx(job)
	err = AddHistoryDDLJob(w.sess, t, job, updateRawArgs)
	return errors.Trace(err)
}

func (w *worker) writeDDLSeqNum(job *model.Job) {
	w.ddlSeqNumMu.Lock()
	w.ddlSeqNumMu.seqNum++
	w.lockSeqNum = true
	job.SeqNum = w.ddlSeqNumMu.seqNum
}

func finishRecoverTable(w *worker, job *model.Job) error {
	var (
		recoverInfo           *RecoverInfo
		recoverTableCheckFlag int64
	)
	err := job.DecodeArgs(&recoverInfo, &recoverTableCheckFlag)
	if err != nil {
		return errors.Trace(err)
	}
	if recoverTableCheckFlag == recoverCheckFlagEnableGC {
		err = enableGC(w)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func finishRecoverSchema(w *worker, job *model.Job) error {
	var (
		recoverSchemaInfo      *RecoverSchemaInfo
		recoverSchemaCheckFlag int64
	)
	err := job.DecodeArgs(&recoverSchemaInfo, &recoverSchemaCheckFlag)
	if err != nil {
		return errors.Trace(err)
	}
	if recoverSchemaCheckFlag == recoverCheckFlagEnableGC {
		err = enableGC(w)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func isDependencyJobDone(t *meta.Meta, job *model.Job) (bool, error) {
	if job.DependencyID == noneDependencyJob {
		return true, nil
	}

	historyJob, err := t.GetHistoryDDLJob(job.DependencyID)
	if err != nil {
		return false, errors.Trace(err)
	}
	if historyJob == nil {
		return false, nil
	}
	logutil.BgLogger().Info("[ddl] current DDL job dependent job is finished", zap.String("currentJob", job.String()), zap.Int64("dependentJobID", job.DependencyID))
	job.DependencyID = noneDependencyJob
	return true, nil
}

func (w *JobContext) setDDLLabelForTopSQL(jobQuery string) {
	if !topsqlstate.TopSQLEnabled() || jobQuery == "" {
		return
	}

	if jobQuery != w.cacheSQL || w.cacheDigest == nil {
		w.cacheNormalizedSQL, w.cacheDigest = parser.NormalizeDigest(jobQuery)
		w.cacheSQL = jobQuery
		w.ddlJobCtx = topsql.AttachAndRegisterSQLInfo(context.Background(), w.cacheNormalizedSQL, w.cacheDigest, false)
	} else {
		topsql.AttachAndRegisterSQLInfo(w.ddlJobCtx, w.cacheNormalizedSQL, w.cacheDigest, false)
	}
}

func (w *worker) unlockSeqNum(err error) {
	if w.lockSeqNum {
		if err != nil {
			// if meet error, we should reset seqNum.
			w.ddlSeqNumMu.seqNum--
		}
		w.lockSeqNum = false
		w.ddlSeqNumMu.Unlock()
	}
}

// DDLBackfillers contains the DDL need backfill step.
var DDLBackfillers = map[model.ActionType]string{
	model.ActionAddIndex:            "add_index",
	model.ActionModifyColumn:        "modify_column",
	model.ActionDropIndex:           "drop_index",
	model.ActionReorganizePartition: "reorganize_partition",
}

func getDDLRequestSource(jobType model.ActionType) string {
	if tp, ok := DDLBackfillers[jobType]; ok {
		return kv.InternalTxnBackfillDDLPrefix + tp
	}
	return kv.InternalTxnDDL
}

func (w *JobContext) setDDLLabelForDiagnosis(jobType model.ActionType) {
	if w.tp != "" {
		return
	}
	w.tp = getDDLRequestSource(jobType)
	w.ddlJobCtx = kv.WithInternalSourceType(w.ddlJobCtx, w.ddlJobSourceType())
}

func (w *worker) HandleJobDone(d *ddlCtx, job *model.Job, t *meta.Meta) error {
	err := w.finishDDLJob(t, job)
	if err != nil {
		w.sess.rollback()
		return err
	}

	err = w.sess.commit()
	if err != nil {
		return err
	}
	CleanupDDLReorgHandles(job, w.sess)
	asyncNotify(d.ddlJobDoneCh)
	return nil
}

func (w *worker) HandleDDLJobTable(d *ddlCtx, job *model.Job) (int64, error) {
	var (
		err       error
		schemaVer int64
		runJobErr error
	)
	defer func() {
		w.unlockSeqNum(err)
	}()

	err = w.sess.begin()
	if err != nil {
		return 0, err
	}
	if d.waiting.Load() {
		w.sess.rollback()
		return 0, nil
	}
	failpoint.Inject("mockRunJobTime", func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) // #nosec G404
		}
	})
	txn, err := w.sess.txn()
	if err != nil {
		w.sess.rollback()
		return 0, err
	}
	// Only general DDLs are allowed to be executed when TiKV is disk full.
	if w.tp == addIdxWorker && job.IsRunning() {
		txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_NotAllowedOnFull)
	}
	w.setDDLLabelForTopSQL(job.ID, job.Query)
	w.setDDLSourceForDiagnosis(job.ID, job.Type)
	jobContext := w.jobContext(job.ID)
	if tagger := w.getResourceGroupTaggerForTopSQL(job.ID); tagger != nil {
		txn.SetOption(kv.ResourceGroupTagger, tagger)
	}
	t := meta.NewMeta(txn)
	if job.IsDone() || job.IsRollbackDone() {
		if job.IsDone() {
			job.State = model.JobStateSynced
		}
		err = w.HandleJobDone(d, job, t)
		return 0, err
	}

	d.mu.RLock()
	d.mu.hook.OnJobRunBefore(job)
	d.mu.RUnlock()

	// set request source type to DDL type
	txn.SetOption(kv.RequestSourceType, jobContext.ddlJobSourceType())

	// If running job meets error, we will save this error in job Error
	// and retry later if the job is not cancelled.
	schemaVer, runJobErr = w.runDDLJob(d, t, job)

	d.mu.RLock()
	d.mu.hook.OnJobRunAfter(job)
	d.mu.RUnlock()

	if job.IsCancelled() {
		defer d.unlockSchemaVersion(job.ID)
		w.sess.reset()
		err = w.HandleJobDone(d, job, t)
		return 0, err
	}

	if runJobErr != nil && !job.IsRollingback() && !job.IsRollbackDone() {
		// If the running job meets an error
		// and the job state is rolling back, it means that we have already handled this error.
		// Some DDL jobs (such as adding indexes) may need to update the table info and the schema version,
		// then shouldn't discard the KV modification.
		// And the job state is rollback done, it means the job was already finished, also shouldn't discard too.
		// Otherwise, we should discard the KV modification when running job.
		w.sess.reset()
		// If error happens after updateSchemaVersion(), then the schemaVer is updated.
		// Result in the retry duration is up to 2 * lease.
		schemaVer = 0
	}

	err = w.registerMDLInfo(job, schemaVer)
	if err != nil {
		w.sess.rollback()
		d.unlockSchemaVersion(job.ID)
		return 0, err
	}
	err = w.updateDDLJob(job, runJobErr != nil)
	if err = w.handleUpdateJobError(t, job, err); err != nil {
		w.sess.rollback()
		d.unlockSchemaVersion(job.ID)
		return 0, err
	}
	writeBinlog(d.binlogCli, txn, job)
	// reset the SQL digest to make topsql work right.
	w.sess.GetSessionVars().StmtCtx.ResetSQLDigest(job.Query)
	err = w.sess.commit()
	d.unlockSchemaVersion(job.ID)
	if err != nil {
		return 0, err
	}
	w.registerSync(job)

	if runJobErr != nil {
		// wait a while to retry again. If we don't wait here, DDL will retry this job immediately,
		// which may act like a deadlock.
		logutil.Logger(w.logCtx).Info("[ddl] run DDL job failed, sleeps a while then retries it.",
			zap.Duration("waitTime", GetWaitTimeWhenErrorOccurred()), zap.Error(runJobErr))
		time.Sleep(GetWaitTimeWhenErrorOccurred())
	}

	return schemaVer, nil
}

func (w *JobContext) getResourceGroupTaggerForTopSQL() tikvrpc.ResourceGroupTagger {
	if !topsqlstate.TopSQLEnabled() || w.cacheDigest == nil {
		return nil
	}

	digest := w.cacheDigest
	tagger := func(req *tikvrpc.Request) {
		req.ResourceGroupTag = resourcegrouptag.EncodeResourceGroupTag(digest, nil,
			resourcegrouptag.GetResourceGroupLabelByKey(resourcegrouptag.GetFirstKeyFromRequest(req)))
	}
	return tagger
}

func (w *JobContext) ddlJobSourceType() string {
	return w.tp
}

func skipWriteBinlog(job *model.Job) bool {
	switch job.Type {
	// ActionUpdateTiFlashReplicaStatus is a TiDB internal DDL,
	// it's used to update table's TiFlash replica available status.
	case model.ActionUpdateTiFlashReplicaStatus:
		return true
	// Don't sync 'alter table cache|nocache' to other tools.
	// It's internal to the current cluster.
	case model.ActionAlterCacheTable, model.ActionAlterNoCacheTable:
		return true
	}

	return false
}

func writeBinlog(binlogCli *pumpcli.PumpsClient, txn kv.Transaction, job *model.Job) {
	if job.IsDone() || job.IsRollbackDone() ||
		// When this column is in the "delete only" and "delete reorg" states, the binlog of "drop column" has not been written yet,
		// but the column has been removed from the binlog of the write operation.
		// So we add this binlog to enable downstream components to handle DML correctly in this schema state.
		(job.Type == model.ActionDropColumn && job.SchemaState == model.StateDeleteOnly) {
		if skipWriteBinlog(job) {
			return
		}
		binloginfo.SetDDLBinlog(binlogCli, txn, job.ID, int32(job.SchemaState), job.Query)
	}
}

// waitDependencyJobFinished waits for the dependency-job to be finished.
// If the dependency job isn't finished yet, we'd better wait a moment.
func (w *worker) waitDependencyJobFinished(job *model.Job, cnt *int) {
	if job.DependencyID != noneDependencyJob {
		intervalCnt := int(3 * time.Second / waitDependencyJobInterval)
		if *cnt%intervalCnt == 0 {
			logutil.Logger(w.logCtx).Info("[ddl] DDL job need to wait dependent job, sleeps a while, then retries it.",
				zap.Int64("jobID", job.ID),
				zap.Int64("dependentJobID", job.DependencyID),
				zap.Duration("waitTime", waitDependencyJobInterval))
		}
		time.Sleep(waitDependencyJobInterval)
		*cnt++
	} else {
		*cnt = 0
	}
}

func chooseLeaseTime(t, max time.Duration) time.Duration {
	if t == 0 || t > max {
		return max
	}
	return t
}

// countForPanic records the error count for DDL job.
func (w *worker) countForPanic(job *model.Job) {
	// If run DDL job panic, just cancel the DDL jobs.
	if job.State == model.JobStateRollingback {
		job.State = model.JobStateCancelled
	} else {
		job.State = model.JobStateCancelling
	}
	job.ErrorCount++

	// Load global DDL variables.
	if err1 := loadDDLVars(w); err1 != nil {
		logutil.Logger(w.logCtx).Error("[ddl] load DDL global variable failed", zap.Error(err1))
	}
	errorCount := variable.GetDDLErrorCountLimit()

	if job.ErrorCount > errorCount {
		msg := fmt.Sprintf("panic in handling DDL logic and error count beyond the limitation %d, cancelled", errorCount)
		logutil.Logger(w.logCtx).Warn(msg)
		job.Error = toTError(errors.New(msg))
		job.State = model.JobStateCancelled
	}
}

// countForError records the error count for DDL job.
func (w *worker) countForError(err error, job *model.Job) error {
	job.Error = toTError(err)
	job.ErrorCount++

	// If job is cancelled, we shouldn't return an error and shouldn't load DDL variables.
	if job.State == model.JobStateCancelled {
		logutil.Logger(w.logCtx).Info("[ddl] DDL job is cancelled normally", zap.Error(err))
		return nil
	}
	logutil.Logger(w.logCtx).Error("[ddl] run DDL job error", zap.Error(err))

	// Load global DDL variables.
	if err1 := loadDDLVars(w); err1 != nil {
		logutil.Logger(w.logCtx).Error("[ddl] load DDL global variable failed", zap.Error(err1))
	}
	// Check error limit to avoid falling into an infinite loop.
	if job.ErrorCount > variable.GetDDLErrorCountLimit() && job.State == model.JobStateRunning && job.IsRollbackable() {
		logutil.Logger(w.logCtx).Warn("[ddl] DDL job error count exceed the limit, cancelling it now", zap.Int64("jobID", job.ID), zap.Int64("errorCountLimit", variable.GetDDLErrorCountLimit()))
		job.State = model.JobStateCancelling
	}
	return err
}

// runDDLJob runs a DDL job. It returns the current schema version in this transaction and the error.
func (w *worker) runDDLJob(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	defer tidbutil.Recover(metrics.LabelDDLWorker, fmt.Sprintf("%s runDDLJob", w),
		func() {
			w.countForPanic(job)
		}, false)

	// Mock for run ddl job panic.
	failpoint.Inject("mockPanicInRunDDLJob", func(val failpoint.Value) {})

	if job.Type != model.ActionMultiSchemaChange {
		logutil.Logger(w.logCtx).Info("[ddl] run DDL job", zap.String("job", job.String()))
	}
	timeStart := time.Now()
	if job.RealStartTS == 0 {
		job.RealStartTS = t.StartTS
	}
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerRunDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()
	if job.IsFinished() {
		logutil.Logger(w.logCtx).Debug("[ddl] finish DDL job", zap.String("job", job.String()))
		return
	}
	// The cause of this job state is that the job is cancelled by client.
	if job.IsCancelling() {
		logutil.Logger(w.logCtx).Debug("[ddl] cancel DDL job", zap.String("job", job.String()))
		return convertJob2RollbackJob(w, d, t, job)
	}

	if !job.IsRollingback() && !job.IsCancelling() {
		job.State = model.JobStateRunning
	}

	// For every type, `schema/table` modification and `job` modification are conducted
	// in the one kv transaction. The `schema/table` modification can be always discarded
	// by kv reset when meets a unhandled error, but the `job` modification can't.
	// So make sure job state and args change is after all other checks or make sure these
	// change has no effect when retrying it.
	switch job.Type {
	case model.ActionCreateSchema:
		ver, err = onCreateSchema(d, t, job)
	case model.ActionModifySchemaCharsetAndCollate:
		ver, err = onModifySchemaCharsetAndCollate(d, t, job)
	case model.ActionDropSchema:
		ver, err = onDropSchema(d, t, job)
	case model.ActionRecoverSchema:
		ver, err = w.onRecoverSchema(d, t, job)
	case model.ActionModifySchemaDefaultPlacement:
		ver, err = onModifySchemaDefaultPlacement(d, t, job)
	case model.ActionCreateTable:
		ver, err = onCreateTable(d, t, job)
	case model.ActionCreateTables:
		ver, err = onCreateTables(d, t, job)
	case model.ActionRepairTable:
		ver, err = onRepairTable(d, t, job)
	case model.ActionCreateView:
		ver, err = onCreateView(d, t, job)
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		ver, err = onDropTableOrView(d, t, job)
	case model.ActionDropTablePartition:
		ver, err = w.onDropTablePartition(d, t, job)
	case model.ActionTruncateTablePartition:
		ver, err = onTruncateTablePartition(d, t, job)
	case model.ActionExchangeTablePartition:
		ver, err = w.onExchangeTablePartition(d, t, job)
	case model.ActionAddColumn:
		ver, err = onAddColumn(d, t, job)
	case model.ActionDropColumn:
		ver, err = onDropColumn(d, t, job)
	case model.ActionModifyColumn:
		ver, err = w.onModifyColumn(d, t, job)
	case model.ActionSetDefaultValue:
		ver, err = onSetDefaultValue(d, t, job)
	case model.ActionAddIndex:
		ver, err = w.onCreateIndex(d, t, job, false)
	case model.ActionAddPrimaryKey:
		ver, err = w.onCreateIndex(d, t, job, true)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		ver, err = onDropIndex(d, t, job)
	case model.ActionRenameIndex:
		ver, err = onRenameIndex(d, t, job)
	case model.ActionAddForeignKey:
		ver, err = w.onCreateForeignKey(d, t, job)
	case model.ActionDropForeignKey:
		ver, err = onDropForeignKey(d, t, job)
	case model.ActionTruncateTable:
		ver, err = onTruncateTable(d, t, job)
	case model.ActionRebaseAutoID:
		ver, err = onRebaseAutoIncrementIDType(d, t, job)
	case model.ActionRebaseAutoRandomBase:
		ver, err = onRebaseAutoRandomType(d, t, job)
	case model.ActionRenameTable:
		ver, err = onRenameTable(d, t, job)
	case model.ActionShardRowID:
		ver, err = w.onShardRowID(d, t, job)
	case model.ActionModifyTableComment:
		ver, err = onModifyTableComment(d, t, job)
	case model.ActionModifyTableAutoIdCache:
		ver, err = onModifyTableAutoIDCache(d, t, job)
	case model.ActionAddTablePartition:
		ver, err = w.onAddTablePartition(d, t, job)
	case model.ActionModifyTableCharsetAndCollate:
		ver, err = onModifyTableCharsetAndCollate(d, t, job)
	case model.ActionRecoverTable:
		ver, err = w.onRecoverTable(d, t, job)
	case model.ActionLockTable:
		ver, err = onLockTables(d, t, job)
	case model.ActionUnlockTable:
		ver, err = onUnlockTables(d, t, job)
	case model.ActionSetTiFlashReplica:
		ver, err = w.onSetTableFlashReplica(d, t, job)
	case model.ActionUpdateTiFlashReplicaStatus:
		ver, err = onUpdateFlashReplicaStatus(d, t, job)
	case model.ActionCreateSequence:
		ver, err = onCreateSequence(d, t, job)
	case model.ActionAlterIndexVisibility:
		ver, err = onAlterIndexVisibility(d, t, job)
	case model.ActionAlterSequence:
		ver, err = onAlterSequence(d, t, job)
	case model.ActionRenameTables:
		ver, err = onRenameTables(d, t, job)
	case model.ActionAlterTableAttributes:
		ver, err = onAlterTableAttributes(d, t, job)
	case model.ActionAlterTablePartitionAttributes:
		ver, err = onAlterTablePartitionAttributes(d, t, job)
	case model.ActionCreatePlacementPolicy:
		ver, err = onCreatePlacementPolicy(d, t, job)
	case model.ActionDropPlacementPolicy:
		ver, err = onDropPlacementPolicy(d, t, job)
	case model.ActionAlterPlacementPolicy:
		ver, err = onAlterPlacementPolicy(d, t, job)
	case model.ActionAlterTablePartitionPlacement:
		ver, err = onAlterTablePartitionPlacement(d, t, job)
	case model.ActionAlterTablePlacement:
		ver, err = onAlterTablePlacement(d, t, job)
	case model.ActionCreateResourceGroup:
		ver, err = onCreateResourceGroup(d, t, job)
	case model.ActionAlterResourceGroup:
		ver, err = onAlterResourceGroup(d, t, job)
	case model.ActionDropResourceGroup:
		ver, err = onDropResourceGroup(d, t, job)
	case model.ActionAlterCacheTable:
		ver, err = onAlterCacheTable(d, t, job)
	case model.ActionAlterNoCacheTable:
		ver, err = onAlterNoCacheTable(d, t, job)
	case model.ActionFlashbackCluster:
		ver, err = w.onFlashbackCluster(d, t, job)
	case model.ActionMultiSchemaChange:
		ver, err = onMultiSchemaChange(w, d, t, job)
	case model.ActionReorganizePartition:
		ver, err = w.onReorganizePartition(d, t, job)
	case model.ActionAlterTTLInfo:
		ver, err = onTTLInfoChange(d, t, job)
	case model.ActionAlterTTLRemove:
		ver, err = onTTLInfoRemove(d, t, job)
	default:
		// Invalid job, cancel it.
		job.State = model.JobStateCancelled
		err = dbterror.ErrInvalidDDLJob.GenWithStack("invalid ddl job type: %v", job.Type)
	}

	// Save errors in job if any, so that others can know errors happened.
	if err != nil {
		err = w.countForError(err, job)
	}
	return
}

func loadDDLVars(w *worker) error {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)
	return util.LoadDDLVars(ctx)
}

func toTError(err error) *terror.Error {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	if ok {
		return tErr
	}

	// TODO: Add the error code.
	return dbterror.ClassDDL.Synthesize(terror.CodeUnknown, err.Error())
}

// waitSchemaChanged waits for the completion of updating all servers' schema. In order to make sure that happens,
// we wait at most 2 * lease time(sessionTTL, 90 seconds).
func waitSchemaChanged(d *ddlCtx, waitTime time.Duration, latestSchemaVersion int64, job *model.Job) {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return
	}
	if waitTime == 0 {
		return
	}

	timeStart := time.Now()
	var err error
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerWaitSchemaChanged, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	if latestSchemaVersion == 0 {
		logutil.Logger(d.ctx).Info("[ddl] schema version doesn't change")
		return
	}

	err = d.schemaSyncer.OwnerUpdateGlobalVersion(d.ctx, latestSchemaVersion)
	if err != nil {
		logutil.Logger(d.ctx).Info("[ddl] update latest schema version failed", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			// If err is context.DeadlineExceeded, it means waitTime(2 * lease) is elapsed. So all the schemas are synced by ticker.
			// There is no need to use etcd to sync. The function returns directly.
			return
		}
	}

	// OwnerCheckAllVersions returns only when all TiDB schemas are synced(exclude the isolated TiDB).
	err = d.schemaSyncer.OwnerCheckAllVersions(d.ctx, job.ID, latestSchemaVersion)
	if err != nil {
		logutil.Logger(d.ctx).Info("[ddl] wait latest schema version encounter error", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		return
	}
	logutil.Logger(d.ctx).Info("[ddl] wait latest schema version changed(get the metadata lock if tidb_enable_metadata_lock is true)",
		zap.Int64("ver", latestSchemaVersion),
		zap.Duration("take time", time.Since(timeStart)),
		zap.String("job", job.String()))
}

// waitSchemaSyncedForMDL likes waitSchemaSynced, but it waits for getting the metadata lock of the latest version of this DDL.
func waitSchemaSyncedForMDL(d *ddlCtx, job *model.Job, latestSchemaVersion int64) error {
	failpoint.Inject("checkDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
		if val.(bool) {
			if mockDDLErrOnce > 0 && mockDDLErrOnce != latestSchemaVersion {
				panic("check down before update global version failed")
			} else {
				mockDDLErrOnce = -1
			}
		}
	})

	timeStart := time.Now()
	// OwnerCheckAllVersions returns only when all TiDB schemas are synced(exclude the isolated TiDB).
	err := d.schemaSyncer.OwnerCheckAllVersions(d.ctx, job.ID, latestSchemaVersion)
	if err != nil {
		logutil.Logger(d.ctx).Info("[ddl] wait latest schema version encounter error", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		return err
	}
	logutil.Logger(d.ctx).Info("[ddl] wait latest schema version changed(get the metadata lock if tidb_enable_metadata_lock is true)",
		zap.Int64("ver", latestSchemaVersion),
		zap.Duration("take time", time.Since(timeStart)),
		zap.String("job", job.String()))
	return nil
}

// waitSchemaSynced handles the following situation:
// If the job enters a new state, and the worker crashs when it's in the process of waiting for 2 * lease time,
// Then the worker restarts quickly, we may run the job immediately again,
// but in this case we don't wait enough 2 * lease time to let other servers update the schema.
// So here we get the latest schema version to make sure all servers' schema version update to the latest schema version
// in a cluster, or to wait for 2 * lease time.
func waitSchemaSynced(d *ddlCtx, job *model.Job, waitTime time.Duration) error {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return nil
	}

	ver, _ := d.store.CurrentVersion(kv.GlobalTxnScope)
	snapshot := d.store.GetSnapshot(ver)
	m := meta.NewSnapshotMeta(snapshot)
	latestSchemaVersion, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		logutil.Logger(d.ctx).Warn("[ddl] get global version failed", zap.Error(err))
		return err
	}

	failpoint.Inject("checkDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
		if val.(bool) {
			if mockDDLErrOnce > 0 && mockDDLErrOnce != latestSchemaVersion {
				panic("check down before update global version failed")
			} else {
				mockDDLErrOnce = -1
			}
		}
	})

	waitSchemaChanged(d, waitTime, latestSchemaVersion, job)
	return nil
}

func buildPlacementAffects(oldIDs []int64, newIDs []int64) []*model.AffectedOption {
	if len(oldIDs) == 0 {
		return nil
	}

	affects := make([]*model.AffectedOption, len(oldIDs))
	for i := 0; i < len(oldIDs); i++ {
		affects[i] = &model.AffectedOption{
			OldTableID: oldIDs[i],
			TableID:    newIDs[i],
		}
	}
	return affects
}

// updateSchemaVersion increments the schema version by 1 and sets SchemaDiff.
func updateSchemaVersion(d *ddlCtx, t *meta.Meta, job *model.Job, multiInfos ...schemaIDAndTableInfo) (int64, error) {
	schemaVersion, err := d.setSchemaVersion(job, d.store)
	if err != nil {
		return 0, errors.Trace(err)
	}
	diff := &model.SchemaDiff{
		Version:  schemaVersion,
		Type:     job.Type,
		SchemaID: job.SchemaID,
	}
	switch job.Type {
	case model.ActionCreateTables:
		var tableInfos []*model.TableInfo
		err = job.DecodeArgs(&tableInfos)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.AffectedOpts = make([]*model.AffectedOption, len(tableInfos))
		for i := range tableInfos {
			diff.AffectedOpts[i] = &model.AffectedOption{
				SchemaID:    job.SchemaID,
				OldSchemaID: job.SchemaID,
				TableID:     tableInfos[i].ID,
				OldTableID:  tableInfos[i].ID,
			}
		}
	case model.ActionTruncateTable:
		// Truncate table has two table ID, should be handled differently.
		err = job.DecodeArgs(&diff.TableID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.OldTableID = job.TableID

		// affects are used to update placement rule cache
		if len(job.CtxVars) > 0 {
			oldIDs := job.CtxVars[0].([]int64)
			newIDs := job.CtxVars[1].([]int64)
			diff.AffectedOpts = buildPlacementAffects(oldIDs, newIDs)
		}
	case model.ActionCreateView:
		tbInfo := &model.TableInfo{}
		var orReplace bool
		var oldTbInfoID int64
		if err := job.DecodeArgs(tbInfo, &orReplace, &oldTbInfoID); err != nil {
			return 0, errors.Trace(err)
		}
		// When the statement is "create or replace view " and we need to drop the old view,
		// it has two table IDs and should be handled differently.
		if oldTbInfoID > 0 && orReplace {
			diff.OldTableID = oldTbInfoID
		}
		diff.TableID = tbInfo.ID
	case model.ActionRenameTable:
		err = job.DecodeArgs(&diff.OldSchemaID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.TableID = job.TableID
	case model.ActionRenameTables:
		var (
			oldSchemaIDs, newSchemaIDs, tableIDs []int64
			tableNames, oldSchemaNames           []*model.CIStr
		)
		err = job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs, &tableNames, &tableIDs, &oldSchemaNames)
		if err != nil {
			return 0, errors.Trace(err)
		}
		affects := make([]*model.AffectedOption, len(newSchemaIDs))
		for i, newSchemaID := range newSchemaIDs {
			affects[i] = &model.AffectedOption{
				SchemaID:    newSchemaID,
				TableID:     tableIDs[i],
				OldTableID:  tableIDs[i],
				OldSchemaID: oldSchemaIDs[i],
			}
		}
		diff.TableID = tableIDs[0]
		diff.SchemaID = newSchemaIDs[0]
		diff.OldSchemaID = oldSchemaIDs[0]
		diff.AffectedOpts = affects
	case model.ActionExchangeTablePartition:
		var (
			ptSchemaID     int64
			ptTableID      int64
			partName       string
			withValidation bool
		)
		err = job.DecodeArgs(&diff.TableID, &ptSchemaID, &ptTableID, &partName, &withValidation)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.OldTableID = job.TableID
		affects := make([]*model.AffectedOption, 1)
		affects[0] = &model.AffectedOption{
			SchemaID:   ptSchemaID,
			TableID:    ptTableID,
			OldTableID: ptTableID,
		}
		diff.AffectedOpts = affects
	case model.ActionTruncateTablePartition:
		diff.TableID = job.TableID
		if len(job.CtxVars) > 0 {
			oldIDs := job.CtxVars[0].([]int64)
			newIDs := job.CtxVars[1].([]int64)
			diff.AffectedOpts = buildPlacementAffects(oldIDs, newIDs)
		}
	case model.ActionDropTablePartition, model.ActionRecoverTable, model.ActionDropTable:
		// affects are used to update placement rule cache
		diff.TableID = job.TableID
		if len(job.CtxVars) > 0 {
			if oldIDs, ok := job.CtxVars[0].([]int64); ok {
				diff.AffectedOpts = buildPlacementAffects(oldIDs, oldIDs)
			}
		}
	case model.ActionReorganizePartition:
		diff.TableID = job.TableID
		if len(job.CtxVars) > 0 {
			if droppedIDs, ok := job.CtxVars[0].([]int64); ok {
				if addedIDs, ok := job.CtxVars[1].([]int64); ok {
					// to use AffectedOpts we need both new and old to have the same length
					maxParts := mathutil.Max[int](len(droppedIDs), len(addedIDs))
					// Also initialize them to 0!
					oldIDs := make([]int64, maxParts)
					copy(oldIDs, droppedIDs)
					newIDs := make([]int64, maxParts)
					copy(newIDs, addedIDs)
					diff.AffectedOpts = buildPlacementAffects(oldIDs, newIDs)
				}
			}
		}
	case model.ActionCreateTable:
		diff.TableID = job.TableID
		if len(job.Args) > 0 {
			tbInfo, _ := job.Args[0].(*model.TableInfo)
			// When create table with foreign key, we actually has two schema status change:
			// 1. none -> write-only
			// 2. write-only -> public
			// In the second status change write-only -> public, infoschema loader should apply drop old table first, then
			// apply create new table. So need to set diff.OldTableID here to make sure it.
			if tbInfo != nil && tbInfo.State == model.StatePublic && len(tbInfo.ForeignKeys) > 0 {
				diff.OldTableID = job.TableID
			}
		}
	case model.ActionRecoverSchema:
		var (
			recoverSchemaInfo      *RecoverSchemaInfo
			recoverSchemaCheckFlag int64
		)
		err = job.DecodeArgs(&recoverSchemaInfo, &recoverSchemaCheckFlag)
		if err != nil {
			return 0, errors.Trace(err)
		}
		// Reserved recoverSchemaCheckFlag value for gc work judgment.
		job.Args[checkFlagIndexInJobArgs] = recoverSchemaCheckFlag
		recoverTabsInfo := recoverSchemaInfo.RecoverTabsInfo
		diff.AffectedOpts = make([]*model.AffectedOption, len(recoverTabsInfo))
		for i := range recoverTabsInfo {
			diff.AffectedOpts[i] = &model.AffectedOption{
				SchemaID:    job.SchemaID,
				OldSchemaID: job.SchemaID,
				TableID:     recoverTabsInfo[i].TableInfo.ID,
				OldTableID:  recoverTabsInfo[i].TableInfo.ID,
			}
		}
	case model.ActionFlashbackCluster:
		diff.TableID = -1
		if job.SchemaState == model.StatePublic {
			diff.RegenerateSchemaMap = true
		}
	default:
		diff.TableID = job.TableID
	}
	if len(multiInfos) > 0 {
		existsMap := make(map[int64]struct{})
		existsMap[diff.TableID] = struct{}{}
		for _, affect := range diff.AffectedOpts {
			existsMap[affect.TableID] = struct{}{}
		}
		for _, info := range multiInfos {
			_, exist := existsMap[info.tblInfo.ID]
			if exist {
				continue
			}
			existsMap[info.tblInfo.ID] = struct{}{}
			diff.AffectedOpts = append(diff.AffectedOpts, &model.AffectedOption{
				SchemaID:    info.schemaID,
				OldSchemaID: info.schemaID,
				TableID:     info.tblInfo.ID,
				OldTableID:  info.tblInfo.ID,
			})
		}
	}
	err = t.SetSchemaDiff(diff)
	return schemaVersion, errors.Trace(err)
}

func isChanClosed(quitCh <-chan struct{}) bool {
	select {
	case <-quitCh:
		return true
	default:
		return false
	}
}
