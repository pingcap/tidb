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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	// RunWorker indicates if this TiDB server starts DDL worker and can run DDL job.
	RunWorker = true
	// ddlWorkerID is used for generating the next DDL worker ID.
	ddlWorkerID = int32(0)
	// WaitTimeWhenErrorOccurred is waiting interval when processing DDL jobs encounter errors.
	WaitTimeWhenErrorOccurred = int64(1 * time.Second)
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
	id       int32
	tp       workerType
	ddlJobCh chan struct{}
	ctx      context.Context
	wg       sync.WaitGroup

	sessPool        *sessionPool // sessPool is used to new sessions to execute SQL in ddl package.
	reorgCtx        *reorgCtx    // reorgCtx is used for reorganization.
	delRangeManager delRangeManager
	logCtx          context.Context
}

func newWorker(ctx context.Context, tp workerType, sessPool *sessionPool, delRangeMgr delRangeManager) *worker {
	worker := &worker{
		id:              atomic.AddInt32(&ddlWorkerID, 1),
		tp:              tp,
		ddlJobCh:        make(chan struct{}, 1),
		ctx:             ctx,
		reorgCtx:        &reorgCtx{notifyCancelReorgJob: 0},
		sessPool:        sessPool,
		delRangeManager: delRangeMgr,
	}

	worker.logCtx = logutil.WithKeyValue(context.Background(), "worker", worker.String())
	return worker
}

func (w *worker) typeStr() string {
	var str string
	switch w.tp {
	case generalWorker:
		str = "general"
	case addIdxWorker:
		str = model.AddIndexStr
	default:
		str = "unknow"
	}
	return str
}

func (w *worker) String() string {
	return fmt.Sprintf("worker %d, tp %s", w.id, w.typeStr())
}

func (w *worker) close() {
	startTime := time.Now()
	w.wg.Wait()
	logutil.Logger(w.logCtx).Info("[ddl] DDL worker closed", zap.Duration("take time", time.Since(startTime)))
}

// start is used for async online schema changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schema change job.
func (w *worker) start(d *ddlCtx) {
	logutil.Logger(w.logCtx).Info("[ddl] start DDL worker")
	defer w.wg.Done()
	defer tidbutil.Recover(
		metrics.LabelDDLWorker,
		fmt.Sprintf("DDL ID %s, %s start", d.uuid, w),
		nil, true,
	)

	// We use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time. If lease is 0, we will use default 1s.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s as the max value.
	checkTime := chooseLeaseTime(2*d.lease, 1*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			logutil.Logger(w.logCtx).Debug("[ddl] wait to check DDL status again", zap.Duration("interval", checkTime))
		case <-w.ddlJobCh:
		case <-w.ctx.Done():
			return
		}

		err := w.handleDDLJobQueue(d)
		if err != nil {
			logutil.Logger(w.logCtx).Error("[ddl] handle DDL job failed", zap.Error(err))
		}
	}
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// buildJobDependence sets the curjob's dependency-ID.
// The dependency-job's ID must less than the current job's ID, and we need the largest one in the list.
func buildJobDependence(t *meta.Meta, curJob *model.Job) error {
	// Jobs in the same queue are ordered. If we want to find a job's dependency-job, we need to look for
	// it from the other queue. So if the job is "ActionAddIndex" job, we need find its dependency-job from DefaultJobList.
	var jobs []*model.Job
	var err error
	switch curJob.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		jobs, err = t.GetAllDDLJobsInQueue(meta.DefaultJobListKey)
	default:
		jobs, err = t.GetAllDDLJobsInQueue(meta.AddIndexJobListKey)
	}
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

func (d *ddl) limitDDLJobs() {
	defer d.wg.Done()
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
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ids, err := t.GenGlobalIDs(len(tasks))
		if err != nil {
			return errors.Trace(err)
		}
		for i, task := range tasks {
			job := task.job
			job.Version = currentVersion
			job.StartTS = txn.StartTS()
			job.ID = ids[i]
			if err = buildJobDependence(t, job); err != nil {
				return errors.Trace(err)
			}

			if job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey {
				jobKey := meta.AddIndexJobListKey
				err = t.EnQueueDDLJob(job, jobKey)
			} else {
				err = t.EnQueueDDLJob(job)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	var jobs string
	for _, task := range tasks {
		task.err <- err
		jobs += task.job.String() + "; "
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, task.job.Type.String(),
			metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}
	logutil.BgLogger().Info("[ddl] add DDL jobs", zap.Int("batch count", len(tasks)), zap.String("jobs", jobs))
}

// getHistoryDDLJob gets a DDL job with job's ID from history queue.
func (d *ddl) getHistoryDDLJob(id int64) (*model.Job, error) {
	var job *model.Job

	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

// getFirstDDLJob gets the first DDL job form DDL queue.
func (w *worker) getFirstDDLJob(t *meta.Meta) (*model.Job, error) {
	job, err := t.GetDDLJobByIdx(0)
	return job, errors.Trace(err)
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
func (w *worker) updateDDLJob(t *meta.Meta, job *model.Job, meetErr bool) error {
	failpoint.Inject("mockErrEntrySizeTooLarge", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(kv.ErrEntryTooLarge)
		}
	})
	updateRawArgs := true
	// If there is an error when running job and the RawArgs hasn't been decoded by DecodeArgs,
	// so we shouldn't replace RawArgs with the marshaling Args.
	if meetErr && (job.RawArgs != nil && job.Args == nil) {
		logutil.Logger(w.logCtx).Info("[ddl] meet something wrong before update DDL job, shouldn't update raw args",
			zap.String("job", job.String()))
		updateRawArgs = false
	}
	return errors.Trace(t.UpdateDDLJob(0, job, updateRawArgs))
}

func (w *worker) deleteRange(job *model.Job) error {
	var err error
	if job.Version <= currentVersion {
		err = w.delRangeManager.addDelRangeJob(job)
	} else {
		err = errInvalidDDLJobVersion.GenWithStackByArgs(job.Version, currentVersion)
	}
	return errors.Trace(err)
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (w *worker) finishDDLJob(t *meta.Meta, job *model.Job) (err error) {
	startTime := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerFinishDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	if !job.IsCancelled() {
		switch job.Type {
		case model.ActionAddIndex, model.ActionAddPrimaryKey:
			if job.State != model.JobStateRollbackDone {
				break
			}

			// After rolling back an AddIndex operation, we need to use delete-range to delete the half-done index data.
			err = w.deleteRange(job)
		case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable, model.ActionDropIndex, model.ActionDropPrimaryKey,
			model.ActionDropTablePartition, model.ActionTruncateTablePartition, model.ActionDropColumn, model.ActionDropColumns:
			err = w.deleteRange(job)
		}
	}
	switch job.Type {
	case model.ActionRecoverTable:
		err = finishRecoverTable(w, t, job)
	}
	if err != nil {
		return errors.Trace(err)
	}

	_, err = t.DeQueueDDLJob()
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
	err = t.AddHistoryDDLJob(job, updateRawArgs)
	return errors.Trace(err)
}

func finishRecoverTable(w *worker, t *meta.Meta, job *model.Job) error {
	tbInfo := &model.TableInfo{}
	var autoIncID, autoRandID, dropJobID, recoverTableCheckFlag int64
	var snapshotTS uint64
	err := job.DecodeArgs(tbInfo, &autoIncID, &dropJobID, &snapshotTS, &recoverTableCheckFlag, &autoRandID)
	if err != nil {
		return errors.Trace(err)
	}
	if recoverTableCheckFlag == recoverTableCheckFlagEnableGC {
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

func newMetaWithQueueTp(txn kv.Transaction, tp string) *meta.Meta {
	if tp == model.AddIndexStr || tp == model.AddPrimaryKeyStr {
		return meta.NewMeta(txn, meta.AddIndexJobListKey)
	}
	return meta.NewMeta(txn)
}

// handleDDLJobQueue handles DDL jobs in DDL Job queue.
func (w *worker) handleDDLJobQueue(d *ddlCtx) error {
	once := true
	waitDependencyJobCnt := 0
	for {
		if isChanClosed(w.ctx.Done()) {
			return nil
		}

		var (
			job       *model.Job
			schemaVer int64
			runJobErr error
		)
		waitTime := 2 * d.lease
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			// We are not owner, return and retry checking later.
			if !d.isOwner() {
				return nil
			}

			var err error
			t := newMetaWithQueueTp(txn, w.typeStr())
			// We become the owner. Get the first job and run it.
			job, err = w.getFirstDDLJob(t)
			if job == nil || err != nil {
				return errors.Trace(err)
			}
			if isDone, err1 := isDependencyJobDone(t, job); err1 != nil || !isDone {
				return errors.Trace(err1)
			}

			if once {
				w.waitSchemaSynced(d, job, waitTime)
				once = false
				return nil
			}

			if job.IsDone() || job.IsRollbackDone() {
				if !job.IsRollbackDone() {
					job.State = model.JobStateSynced
				}
				err = w.finishDDLJob(t, job)
				return errors.Trace(err)
			}

			d.mu.RLock()
			d.mu.hook.OnJobRunBefore(job)
			d.mu.RUnlock()

			// If running job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			schemaVer, runJobErr = w.runDDLJob(d, t, job)
			if job.IsCancelled() {
				txn.Reset()
				err = w.finishDDLJob(t, job)
				return errors.Trace(err)
			}
			if runJobErr != nil && !job.IsRollingback() && !job.IsRollbackDone() {
				// If the running job meets an error
				// and the job state is rolling back, it means that we have already handled this error.
				// Some DDL jobs (such as adding indexes) may need to update the table info and the schema version,
				// then shouldn't discard the KV modification.
				// And the job state is rollback done, it means the job was already finished, also shouldn't discard too.
				// Otherwise, we should discard the KV modification when running job.
				txn.Reset()
				// If error happens after updateSchemaVersion(), then the schemaVer is updated.
				// Result in the retry duration is up to 2 * lease.
				schemaVer = 0
			}
			err = w.updateDDLJob(t, job, runJobErr != nil)
			if err = w.handleUpdateJobError(t, job, err); err != nil {
				return errors.Trace(err)
			}
			writeBinlog(d.binlogCli, txn, job)
			return nil
		})

		if runJobErr != nil {
			// wait a while to retry again. If we don't wait here, DDL will retry this job immediately,
			// which may act like a deadlock.
			logutil.Logger(w.logCtx).Info("[ddl] run DDL job failed, sleeps a while then retries it.",
				zap.Duration("waitTime", GetWaitTimeWhenErrorOccurred()), zap.Error(runJobErr))
			time.Sleep(GetWaitTimeWhenErrorOccurred())
		}

		if err != nil {
			return errors.Trace(err)
		} else if job == nil {
			// No job now, return and retry getting later.
			return nil
		}
		w.waitDependencyJobFinished(job, &waitDependencyJobCnt)

		// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// If the job is done or still running or rolling back, we will wait 2 * lease time to guarantee other servers to update
		// the newest schema.
		ctx, cancel := context.WithTimeout(w.ctx, waitTime)
		w.waitSchemaChanged(ctx, d, waitTime, schemaVer, job)
		cancel()

		d.mu.RLock()
		d.mu.hook.OnJobUpdated(job)
		d.mu.RUnlock()

		if job.IsSynced() || job.IsCancelled() {
			asyncNotify(d.ddlJobDoneCh)
		}
	}
}

func skipWriteBinlog(job *model.Job) bool {
	switch job.Type {
	// ActionUpdateTiFlashReplicaStatus is a TiDB internal DDL,
	// it's used to update table's TiFlash replica available status.
	case model.ActionUpdateTiFlashReplicaStatus:
		return true
	// It is done without modifying table info, bin log is not needed
	case model.ActionAlterTableAlterPartition:
		return true
	}

	return false
}

func writeBinlog(binlogCli *pumpcli.PumpsClient, txn kv.Transaction, job *model.Job) {
	if job.IsDone() || job.IsRollbackDone() ||
		// When this column is in the "delete only" and "delete reorg" states, the binlog of "drop column" has not been written yet,
		// but the column has been removed from the binlog of the write operation.
		// So we add this binlog to enable downstream components to handle DML correctly in this schema state.
		((job.Type == model.ActionDropColumn || job.Type == model.ActionDropColumns) && job.SchemaState == model.StateDeleteOnly) {
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

// runDDLJob runs a DDL job. It returns the current schema version in this transaction and the error.
func (w *worker) runDDLJob(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	defer tidbutil.Recover(metrics.LabelDDLWorker, fmt.Sprintf("%s runDDLJob", w),
		func() {
			// If run DDL job panic, just cancel the DDL jobs.
			job.State = model.JobStateCancelling
		}, false)

	// Mock for run ddl job panic.
	failpoint.Inject("mockPanicInRunDDLJob", func(val failpoint.Value) {})

	logutil.Logger(w.logCtx).Info("[ddl] run DDL job", zap.String("job", job.String()))
	timeStart := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerRunDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()
	if job.IsFinished() {
		return
	}
	// The cause of this job state is that the job is cancelled by client.
	if job.IsCancelling() {
		return convertJob2RollbackJob(w, d, t, job)
	}

	if !job.IsRollingback() && !job.IsCancelling() {
		job.State = model.JobStateRunning
	}

	switch job.Type {
	case model.ActionCreateSchema:
		ver, err = onCreateSchema(d, t, job)
	case model.ActionModifySchemaCharsetAndCollate:
		ver, err = onModifySchemaCharsetAndCollate(t, job)
	case model.ActionDropSchema:
		ver, err = onDropSchema(t, job)
	case model.ActionCreateTable:
		ver, err = onCreateTable(d, t, job)
	case model.ActionRepairTable:
		ver, err = onRepairTable(d, t, job)
	case model.ActionCreateView:
		ver, err = onCreateView(d, t, job)
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		ver, err = onDropTableOrView(t, job)
	case model.ActionDropTablePartition:
		ver, err = onDropTablePartition(t, job)
	case model.ActionTruncateTablePartition:
		ver, err = onTruncateTablePartition(d, t, job)
	case model.ActionExchangeTablePartition:
		ver, err = w.onExchangeTablePartition(d, t, job)
	case model.ActionAddColumn:
		ver, err = onAddColumn(d, t, job)
	case model.ActionAddColumns:
		ver, err = onAddColumns(d, t, job)
	case model.ActionDropColumn:
		ver, err = onDropColumn(t, job)
	case model.ActionDropColumns:
		ver, err = onDropColumns(t, job)
	case model.ActionModifyColumn:
		ver, err = w.onModifyColumn(t, job)
	case model.ActionSetDefaultValue:
		ver, err = onSetDefaultValue(t, job)
	case model.ActionAddIndex:
		ver, err = w.onCreateIndex(d, t, job, false)
	case model.ActionAddPrimaryKey:
		ver, err = w.onCreateIndex(d, t, job, true)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		ver, err = onDropIndex(t, job)
	case model.ActionRenameIndex:
		ver, err = onRenameIndex(t, job)
	case model.ActionAddForeignKey:
		ver, err = onCreateForeignKey(t, job)
	case model.ActionDropForeignKey:
		ver, err = onDropForeignKey(t, job)
	case model.ActionTruncateTable:
		ver, err = onTruncateTable(d, t, job)
	case model.ActionRebaseAutoID:
		ver, err = onRebaseRowIDType(d.store, t, job)
	case model.ActionRebaseAutoRandomBase:
		ver, err = onRebaseAutoRandomType(d.store, t, job)
	case model.ActionRenameTable:
		ver, err = onRenameTable(d, t, job)
	case model.ActionShardRowID:
		ver, err = w.onShardRowID(d, t, job)
	case model.ActionModifyTableComment:
		ver, err = onModifyTableComment(t, job)
	case model.ActionModifyTableAutoIdCache:
		ver, err = onModifyTableAutoIDCache(t, job)
	case model.ActionAddTablePartition:
		ver, err = onAddTablePartition(d, t, job)
	case model.ActionModifyTableCharsetAndCollate:
		ver, err = onModifyTableCharsetAndCollate(t, job)
	case model.ActionRecoverTable:
		ver, err = w.onRecoverTable(d, t, job)
	case model.ActionLockTable:
		ver, err = onLockTables(t, job)
	case model.ActionUnlockTable:
		ver, err = onUnlockTables(t, job)
	case model.ActionSetTiFlashReplica:
		ver, err = w.onSetTableFlashReplica(t, job)
	case model.ActionUpdateTiFlashReplicaStatus:
		ver, err = onUpdateFlashReplicaStatus(t, job)
	case model.ActionCreateSequence:
		ver, err = onCreateSequence(d, t, job)
	case model.ActionAlterIndexVisibility:
		ver, err = onAlterIndexVisibility(t, job)
	case model.ActionAlterTableAlterPartition:
		ver, err = onAlterTablePartition(t, job)
	default:
		// Invalid job, cancel it.
		job.State = model.JobStateCancelled
		err = errInvalidDDLJob.GenWithStack("invalid ddl job type: %v", job.Type)
	}

	// Save errors in job, so that others can know errors happened.
	if err != nil {
		job.Error = toTError(err)
		job.ErrorCount++

		// If job is cancelled, we shouldn't return an error and shouldn't load DDL variables.
		if job.State == model.JobStateCancelled {
			logutil.Logger(w.logCtx).Info("[ddl] DDL job is cancelled normally", zap.Error(err))
			return ver, nil
		}
		logutil.Logger(w.logCtx).Error("[ddl] run DDL job error", zap.Error(err))

		// Load global ddl variables.
		if err1 := loadDDLVars(w); err1 != nil {
			logutil.Logger(w.logCtx).Error("[ddl] load DDL global variable failed", zap.Error(err1))
		}
		// Check error limit to avoid falling into an infinite loop.
		if job.ErrorCount > variable.GetDDLErrorCountLimit() && job.State == model.JobStateRunning && admin.IsJobRollbackable(job) {
			logutil.Logger(w.logCtx).Warn("[ddl] DDL job error count exceed the limit, cancelling it now", zap.Int64("jobID", job.ID), zap.Int64("errorCountLimit", variable.GetDDLErrorCountLimit()))
			job.State = model.JobStateCancelling
		}
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
	return terror.ClassDDL.Synthesize(terror.CodeUnknown, err.Error())
}

// waitSchemaChanged waits for the completion of updating all servers' schema. In order to make sure that happens,
// we wait 2 * lease time.
func (w *worker) waitSchemaChanged(ctx context.Context, d *ddlCtx, waitTime time.Duration, latestSchemaVersion int64, job *model.Job) {
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
		logutil.Logger(w.logCtx).Info("[ddl] schema version doesn't change")
		return
	}

	err = d.schemaSyncer.OwnerUpdateGlobalVersion(ctx, latestSchemaVersion)
	if err != nil {
		logutil.Logger(w.logCtx).Info("[ddl] update latest schema version failed", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			// If err is context.DeadlineExceeded, it means waitTime(2 * lease) is elapsed. So all the schemas are synced by ticker.
			// There is no need to use etcd to sync. The function returns directly.
			return
		}
	}

	// OwnerCheckAllVersions returns only when context is timeout(2 * lease) or all TiDB schemas are synced.
	err = d.schemaSyncer.OwnerCheckAllVersions(ctx, latestSchemaVersion)
	if err != nil {
		logutil.Logger(w.logCtx).Info("[ddl] wait latest schema version to deadline", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			return
		}
		d.schemaSyncer.NotifyCleanExpiredPaths()
		// Wait until timeout.
		select {
		case <-ctx.Done():
			return
		}
	}
	logutil.Logger(w.logCtx).Info("[ddl] wait latest schema version changed",
		zap.Int64("ver", latestSchemaVersion),
		zap.Duration("take time", time.Since(timeStart)),
		zap.String("job", job.String()))
}

// waitSchemaSynced handles the following situation:
// If the job enters a new state, and the worker crashs when it's in the process of waiting for 2 * lease time,
// Then the worker restarts quickly, we may run the job immediately again,
// but in this case we don't wait enough 2 * lease time to let other servers update the schema.
// So here we get the latest schema version to make sure all servers' schema version update to the latest schema version
// in a cluster, or to wait for 2 * lease time.
func (w *worker) waitSchemaSynced(d *ddlCtx, job *model.Job, waitTime time.Duration) {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return
	}
	ctx, cancelFunc := context.WithTimeout(w.ctx, waitTime)
	defer cancelFunc()

	latestSchemaVersion, err := d.schemaSyncer.MustGetGlobalVersion(ctx)
	if err != nil {
		logutil.Logger(w.logCtx).Warn("[ddl] get global version failed", zap.Error(err))
		return
	}
	w.waitSchemaChanged(ctx, d, waitTime, latestSchemaVersion, job)
}

// updateSchemaVersion increments the schema version by 1 and sets SchemaDiff.
func updateSchemaVersion(t *meta.Meta, job *model.Job) (int64, error) {
	schemaVersion, err := t.GenSchemaVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	diff := &model.SchemaDiff{
		Version:  schemaVersion,
		Type:     job.Type,
		SchemaID: job.SchemaID,
	}
	switch job.Type {
	case model.ActionTruncateTable:
		// Truncate table has two table ID, should be handled differently.
		err = job.DecodeArgs(&diff.TableID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.OldTableID = job.TableID
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
	case model.ActionExchangeTablePartition:
		var (
			ptSchemaID int64
			ptTableID  int64
		)
		err = job.DecodeArgs(&diff.TableID, &ptSchemaID, &ptTableID)
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
	default:
		diff.TableID = job.TableID
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
