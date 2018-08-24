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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var (
	// RunWorker indicates if this TiDB server starts DDL worker and can run DDL job.
	RunWorker = true
	// ddlWorkerID is used for generating the next DDL worker ID.
	ddlWorkerID = int32(0)
)

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
	quitCh   chan struct{}
	wg       sync.WaitGroup

	reorgCtx        *reorgCtx // reorgCtx is used for reorganization.
	delRangeManager delRangeManager
}

func newWorker(tp workerType, store kv.Storage, ctxPool *pools.ResourcePool) *worker {
	worker := &worker{
		id:       atomic.AddInt32(&ddlWorkerID, 1),
		tp:       tp,
		ddlJobCh: make(chan struct{}, 1),
		quitCh:   make(chan struct{}),
		reorgCtx: &reorgCtx{notifyCancelReorgJob: 0},
	}

	if ctxPool != nil {
		worker.delRangeManager = newDelRangeManager(store, ctxPool)
		log.Infof("[ddl] start delRangeManager OK, with emulator: %t", !store.SupportDeleteRange())
	} else {
		worker.delRangeManager = newMockDelRangeManager()
	}
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
	close(w.quitCh)
	w.delRangeManager.clear()
	w.wg.Wait()
	log.Infof("[ddl-%s] close DDL worker", w)
}

// start is used for async online schema changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schema change job.
func (w *worker) start(d *ddlCtx) {
	log.Infof("[ddl-%s] start DDL worker", w)
	defer w.wg.Done()

	w.delRangeManager.start()

	// We use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time. If lease is 0, we will use default 1s.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s as the max value.
	checkTime := chooseLeaseTime(2*d.lease, 1*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()
	defer func() {
		r := recover()
		if r != nil {
			buf := util.GetStack()
			log.Errorf("[ddl-%s] ddl %s, %v %s", w, d.uuid, r, buf)
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
		}
	}()

	for {
		select {
		case <-ticker.C:
			log.Debugf("[ddl-%s] wait %s to check DDL status again", w, checkTime)
		case <-w.ddlJobCh:
		case <-w.quitCh:
			return
		}

		err := w.handleDDLJobQueue(d)
		if err != nil {
			log.Errorf("[ddl-%s] handle DDL job err %v", w, errors.ErrorStack(err))
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
	case model.ActionAddIndex:
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
			log.Infof("[ddl] current DDL job %v depends on job %v", curJob, job)
			curJob.DependencyID = job.ID
			break
		}
	}
	return nil
}

// addDDLJob gets a global job ID and puts the DDL job in the DDL queue.
func (d *ddl) addDDLJob(ctx sessionctx.Context, job *model.Job) error {
	startTime := time.Now()
	job.Version = currentVersion
	job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := newMetaWithQueueTp(txn, job.Type.String())
		var err error
		job.ID, err = t.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}
		job.StartTS = txn.StartTS()
		if err = buildJobDependence(t, job); err != nil {
			return errors.Trace(err)
		}
		err = t.EnQueueDDLJob(job)

		return errors.Trace(err)
	})
	metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
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
		log.Warnf("[ddl-%s] update DDL job %v failed %v", w, job, errors.ErrorStack(err))
		// Reduce this txn entry size.
		job.BinlogInfo.Clean()
		job.Error = toTError(err)
		job.SchemaState = model.StateNone
		job.State = model.JobStateCancelled
		err = w.finishDDLJob(t, job)
	}
	return errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
// Every time we enter another state except final state, we must call this function.
func (w *worker) updateDDLJob(t *meta.Meta, job *model.Job, meetErr bool) error {
	updateRawArgs := true
	// If there is an error when running job and the RawArgs hasn't been decoded by DecodeArgs,
	// so we shouldn't replace RawArgs with the marshaling Args.
	if meetErr && (job.RawArgs != nil && job.Args == nil) {
		log.Infof("[ddl-%s] update DDL Job %s shouldn't update raw args", w, job)
		updateRawArgs = false
	}
	return errors.Trace(t.UpdateDDLJob(0, job, updateRawArgs))
}

func (w *worker) deleteRange(job *model.Job) error {
	var err error
	if job.Version <= currentVersion {
		err = w.delRangeManager.addDelRangeJob(job)
	} else {
		err = errInvalidJobVersion.GenByArgs(job.Version, currentVersion)
	}
	return errors.Trace(err)
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (w *worker) finishDDLJob(t *meta.Meta, job *model.Job) (err error) {
	startTime := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerFinishDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	switch job.Type {
	case model.ActionAddIndex:
		if job.State != model.JobStateRollbackDone {
			break
		}
		// After rolling back an AddIndex operation, we need to use delete-range to delete the half-done index data.
		err = w.deleteRange(job)
	case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable, model.ActionDropIndex, model.ActionDropTablePartition:
		err = w.deleteRange(job)
	}
	if err != nil {
		return errors.Trace(err)
	}

	_, err = t.DeQueueDDLJob()
	if err != nil {
		return errors.Trace(err)
	}

	job.BinlogInfo.FinishedTS = t.StartTS
	log.Infof("[ddl-%s] finish DDL job %v", w, job)
	err = t.AddHistoryDDLJob(job)
	return errors.Trace(err)
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
	log.Infof("[ddl] current DDL job %v dependent job ID %d is finished", job, job.DependencyID)
	job.DependencyID = noneDependencyJob
	return true, nil
}

func newMetaWithQueueTp(txn kv.Transaction, tp string) *meta.Meta {
	if tp == model.AddIndexStr {
		return meta.NewMeta(txn, meta.AddIndexJobListKey)
	}
	return meta.NewMeta(txn)
}

// handleDDLJobQueue handles DDL jobs in DDL Job queue.
func (w *worker) handleDDLJobQueue(d *ddlCtx) error {
	once := true
	waitDependencyJobCnt := 0
	for {
		if isChanClosed(w.quitCh) {
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
				binloginfo.SetDDLBinlog(d.binlogCli, txn, job.ID, job.Query)
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
				err = w.finishDDLJob(t, job)
				return errors.Trace(err)
			}
			err = w.updateDDLJob(t, job, runJobErr != nil)
			return errors.Trace(w.handleUpdateJobError(t, job, err))
		})

		if runJobErr != nil {
			// wait a while to retry again. If we don't wait here, DDL will retry this job immediately,
			// which may act like a deadlock.
			log.Infof("[ddl-%s] run DDL job error, sleeps a while:%v then retries it.", w, WaitTimeWhenErrorOccured)
			metrics.DDLJobErrCounter.Inc()
			time.Sleep(WaitTimeWhenErrorOccured)
		}

		if err != nil {
			return errors.Trace(err)
		} else if job == nil {
			// No job now, return and retry getting later.
			return nil
		}
		w.waitDependencyJobFinished(job, &waitDependencyJobCnt)

		d.mu.RLock()
		d.mu.hook.OnJobUpdated(job)
		d.mu.RUnlock()

		// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// If the job is done or still running or rolling back, we will wait 2 * lease time to guarantee other servers to update
		// the newest schema.
		w.waitSchemaChanged(nil, d, waitTime, schemaVer, job)
		if job.IsSynced() {
			asyncNotify(d.ddlJobDoneCh)
		}
	}
}

// waitDependencyJobFinished waits for the dependency-job to be finished.
// If the dependency job isn't finished yet, we'd better wait a moment.
func (w *worker) waitDependencyJobFinished(job *model.Job, cnt *int) {
	if job.DependencyID != noneDependencyJob {
		intervalCnt := int(3 * time.Second / waitDependencyJobInterval)
		if *cnt%intervalCnt == 0 {
			log.Infof("[ddl-%s] job ID:%d needs to wait dependency job %d, sleeps a while:%v then retries it.", w, job.ID, job.DependencyID, waitDependencyJobInterval)
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
	log.Infof("[ddl-%s] run DDL job %s", w, job)
	if job.IsFinished() {
		return
	}
	// The cause of this job state is that the job is cancelled by client.
	if job.IsCancelling() {
		// If the value of SnapshotVer isn't zero, it means the work is backfilling the indexes.
		if job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
			log.Infof("[ddl-%s] run the cancelling DDL job %s", w, job)
			w.reorgCtx.notifyReorgCancel()
		} else {
			job.State = model.JobStateCancelled
			job.Error = errCancelledDDLJob
			job.ErrorCount++
			return
		}
	}

	if !job.IsRollingback() && !job.IsCancelling() {
		job.State = model.JobStateRunning
	}

	switch job.Type {
	case model.ActionCreateSchema:
		ver, err = onCreateSchema(t, job)
	case model.ActionDropSchema:
		ver, err = onDropSchema(t, job)
	case model.ActionCreateTable:
		ver, err = onCreateTable(d, t, job)
	case model.ActionDropTable:
		ver, err = onDropTable(t, job)
	case model.ActionDropTablePartition:
		ver, err = onDropTablePartition(t, job)
	case model.ActionAddColumn:
		ver, err = onAddColumn(d, t, job)
	case model.ActionDropColumn:
		ver, err = onDropColumn(t, job)
	case model.ActionModifyColumn:
		ver, err = onModifyColumn(t, job)
	case model.ActionSetDefaultValue:
		ver, err = onSetDefaultValue(t, job)
	case model.ActionAddIndex:
		ver, err = w.onCreateIndex(d, t, job)
	case model.ActionDropIndex:
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
		ver, err = onRebaseAutoID(d.store, t, job)
	case model.ActionRenameTable:
		ver, err = onRenameTable(t, job)
	case model.ActionShardRowID:
		ver, err = onShardRowID(t, job)
	case model.ActionModifyTableComment:
		ver, err = onModifyTableComment(t, job)
	case model.ActionAddTablePartition:
		ver, err = onAddTablePartition(t, job)
	default:
		// Invalid job, cancel it.
		job.State = model.JobStateCancelled
		err = errInvalidDDLJob.Gen("invalid ddl job %v", job)
	}

	// Save errors in job, so that others can know errors happened.
	if err != nil {
		// If job is not cancelled, we should log this error.
		if job.State != model.JobStateCancelled {
			log.Errorf("[ddl-%s] run DDL job err %v", w, errors.ErrorStack(err))
		} else {
			log.Infof("[ddl-%s] the DDL job is normal to cancel because %v", w, errors.ErrorStack(err))
		}

		job.Error = toTError(err)
		job.ErrorCount++
	}
	return
}

func toTError(err error) *terror.Error {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	if ok {
		return tErr
	}

	// TODO: Add the error code.
	return terror.ClassDDL.New(terror.CodeUnknown, err.Error())
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
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerWaitSchemaChanged, metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	if latestSchemaVersion == 0 {
		log.Infof("[ddl-%s] schema version doesn't change", w)
		return
	}

	if ctx == nil {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(context.Background(), waitTime)
		defer cancelFunc()
	}
	err = d.schemaSyncer.OwnerUpdateGlobalVersion(ctx, latestSchemaVersion)
	if err != nil {
		log.Infof("[ddl-%s] update latest schema version %d failed %v", w, latestSchemaVersion, err)
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			// If err is context.DeadlineExceeded, it means waitTime(2 * lease) is elapsed. So all the schemas are synced by ticker.
			// There is no need to use etcd to sync. The function returns directly.
			return
		}
	}

	// OwnerCheckAllVersions returns only when context is timeout(2 * lease) or all TiDB schemas are synced.
	err = d.schemaSyncer.OwnerCheckAllVersions(ctx, latestSchemaVersion)
	if err != nil {
		log.Infof("[ddl-%s] wait latest schema version %d to deadline %v", w, latestSchemaVersion, err)
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			return
		}
		select {
		case <-ctx.Done():
			return
		}
	}
	log.Infof("[ddl-%s] wait latest schema version %d changed, take time %v, job %s", w, latestSchemaVersion, time.Since(timeStart), job)
	return
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
	// TODO: Make ctx exits when the d is close.
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitTime)
	defer cancelFunc()

	startTime := time.Now()
	latestSchemaVersion, err := d.schemaSyncer.MustGetGlobalVersion(ctx)
	if err != nil {
		log.Warnf("[ddl-%s] handle exception take time %v", w, time.Since(startTime))
		return
	}
	w.waitSchemaChanged(ctx, d, waitTime, latestSchemaVersion, job)
	log.Infof("[ddl-%s] handle exception take time %v", w, time.Since(startTime))
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
	if job.Type == model.ActionTruncateTable {
		// Truncate table has two table ID, should be handled differently.
		err = job.DecodeArgs(&diff.TableID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.OldTableID = job.TableID
	} else if job.Type == model.ActionRenameTable {
		err = job.DecodeArgs(&diff.OldSchemaID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.TableID = job.TableID
	} else {
		diff.TableID = job.TableID
	}
	err = t.SetSchemaDiff(diff)
	return schemaVersion, errors.Trace(err)
}

func isChanClosed(quitCh chan struct{}) bool {
	select {
	case <-quitCh:
		return true
	default:
		return false
	}
}
