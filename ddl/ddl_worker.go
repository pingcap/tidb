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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/terror"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

// RunWorker indicates if this TiDB server starts DDL worker and can run DDL job.
var RunWorker = true

// onDDLWorker is for async online schema changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schema change job.
func (d *ddl) onDDLWorker() {
	defer d.wait.Done()

	// We use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time. If lease is 0, we will use default 1s.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s as the max value.
	checkTime := chooseLeaseTime(2*d.lease, 1*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Debugf("[ddl] wait %s to check DDL status again", checkTime)
		case <-d.ddlJobCh:
		case <-d.quitCh:
			return
		}

		err := d.handleDDLJobQueue()
		if err != nil {
			log.Errorf("[ddl] handle ddl job err %v", errors.ErrorStack(err))
		}
	}
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func cleanNotify(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
}

func (d *ddl) isOwner() bool {
	isOwner := d.ownerManager.IsOwner()
	log.Debugf("[ddl] it's the job owner %v, self id %s", isOwner, d.uuid)
	return isOwner
}

// addDDLJob gets a global job ID and puts the DDL job in the DDL queue.
func (d *ddl) addDDLJob(ctx context.Context, job *model.Job) error {
	job.Version = currentVersion
	job.Query, _ = ctx.Value(context.QueryString).(string)
	return kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		job.ID, err = t.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}
		job.StartTS = txn.StartTS()
		err = t.EnQueueDDLJob(job)
		return errors.Trace(err)
	})
}

// getFirstDDLJob gets the first DDL job form DDL queue.
func (d *ddl) getFirstDDLJob(t *meta.Meta) (*model.Job, error) {
	job, err := t.GetDDLJob(0)
	return job, errors.Trace(err)
}

// handleUpdateJobError handles the too large DDL job.
func (d *ddl) handleUpdateJobError(t *meta.Meta, job *model.Job, err error) error {
	if err == nil {
		return nil
	}
	if kv.ErrEntryTooLarge.Equal(err) {
		log.Warnf("[ddl] update DDL job %v failed %v", job, errors.ErrorStack(err))
		// Reduce this txn entry size.
		job.BinlogInfo.Clean()
		job.Error = toTError(err)
		job.SchemaState = model.StateNone
		job.State = model.JobStateCancelled
		err = d.finishDDLJob(t, job)
	}
	return errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
// Every time we enter another state except final state, we must call this function.
func (d *ddl) updateDDLJob(t *meta.Meta, job *model.Job, meetErr bool) error {
	updateRawArgs := true
	// If there is an error when running job and the RawArgs hasn't been decoded by DecodeArgs,
	// so we shouldn't replace RawArgs with the marshaling Args.
	if meetErr && (job.RawArgs != nil && job.Args == nil) {
		log.Infof("[ddl] update DDL Job %s shouldn't update raw args", job)
		updateRawArgs = false
	}
	return errors.Trace(t.UpdateDDLJob(0, job, updateRawArgs))
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (d *ddl) finishDDLJob(t *meta.Meta, job *model.Job) (err error) {
	switch job.Type {
	case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable, model.ActionDropIndex:
		if job.Version <= currentVersion {
			err = d.delRangeManager.addDelRangeJob(job)
		} else {
			err = errInvalidJobVersion.GenByArgs(job.Version, currentVersion)
		}
		if err != nil {
			return errors.Trace(err)
		}
	}

	_, err = t.DeQueueDDLJob()
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[ddl] finish DDL job %v", job)
	err = t.AddHistoryDDLJob(job)
	return errors.Trace(err)
}

// getHistoryDDLJob gets a DDL job with job's ID form history queue.
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

func (d *ddl) handleDDLJobQueue() error {
	once := true
	for {
		if d.isClosed() {
			return nil
		}

		waitTime := 2 * d.lease
		var job *model.Job
		var schemaVer int64
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			// We are not owner, return and retry checking later.
			if !d.isOwner() {
				return nil
			}

			var err error
			t := meta.NewMeta(txn)
			// We become the owner. Get the first job and run it.
			job, err = d.getFirstDDLJob(t)
			if job == nil || err != nil {
				return errors.Trace(err)
			}

			if once {
				d.waitSchemaSynced(job, waitTime)
				once = false
				return nil
			}

			if job.IsDone() {
				binloginfo.SetDDLBinlog(d.workerVars.BinlogClient, txn, job.ID, job.Query)
				job.State = model.JobStateSynced
				err = d.finishDDLJob(t, job)
				return errors.Trace(err)
			}

			d.hookMu.Lock()
			d.hook.OnJobRunBefore(job)
			d.hookMu.Unlock()

			// If running job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			schemaVer, err = d.runDDLJob(t, job)
			if job.IsCancelled() {
				err = d.finishDDLJob(t, job)
				return errors.Trace(err)
			}
			err = d.updateDDLJob(t, job, err != nil)
			return errors.Trace(d.handleUpdateJobError(t, job, err))
		})
		if err != nil {
			return errors.Trace(err)
		} else if job == nil {
			// No job now, return and retry getting later.
			return nil
		}

		d.hookMu.Lock()
		d.hook.OnJobUpdated(job)
		d.hookMu.Unlock()

		// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// If the job is done or still running, we will wait 2 * lease time to guarantee other servers to update
		// the newest schema.
		if job.State == model.JobStateRunning || job.State == model.JobStateDone {
			d.waitSchemaChanged(nil, waitTime, schemaVer)
		}
		if job.IsSynced() {
			asyncNotify(d.ddlJobDoneCh)
		}
	}
}

func chooseLeaseTime(t, max time.Duration) time.Duration {
	if t == 0 || t > max {
		return max
	}
	return t
}

// runDDLJob runs a DDL job. It returns the current schema version in this transaction and the error.
func (d *ddl) runDDLJob(t *meta.Meta, job *model.Job) (ver int64, err error) {
	log.Infof("[ddl] run DDL job %s", job)
	if job.IsFinished() {
		return
	}
	// The cause of this job state is that the job is cancelled by client.
	if job.IsCancelling() {
		// If the value of SnapshotVer isn't zero, it means the work is backfilling the indexes.
		if job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
			log.Infof("[ddl] run the cancelling DDL job %s", job)
			asyncNotify(d.reorgCtx.notifyCancelReorgJob)
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
		ver, err = d.onCreateSchema(t, job)
	case model.ActionDropSchema:
		ver, err = d.onDropSchema(t, job)
	case model.ActionCreateTable:
		ver, err = d.onCreateTable(t, job)
	case model.ActionDropTable:
		ver, err = d.onDropTable(t, job)
	case model.ActionAddColumn:
		ver, err = d.onAddColumn(t, job)
	case model.ActionDropColumn:
		ver, err = d.onDropColumn(t, job)
	case model.ActionModifyColumn:
		ver, err = d.onModifyColumn(t, job)
	case model.ActionAddIndex:
		ver, err = d.onCreateIndex(t, job)
	case model.ActionDropIndex:
		ver, err = d.onDropIndex(t, job)
	case model.ActionAddForeignKey:
		ver, err = d.onCreateForeignKey(t, job)
	case model.ActionDropForeignKey:
		ver, err = d.onDropForeignKey(t, job)
	case model.ActionTruncateTable:
		ver, err = d.onTruncateTable(t, job)
	case model.ActionRebaseAutoID:
		ver, err = d.onRebaseAutoID(t, job)
	case model.ActionRenameTable:
		ver, err = d.onRenameTable(t, job)
	case model.ActionSetDefaultValue:
		ver, err = d.onSetDefaultValue(t, job)
	case model.ActionShardRowID:
		ver, err = d.onShardRowID(t, job)
	default:
		// Invalid job, cancel it.
		job.State = model.JobStateCancelled
		err = errInvalidDDLJob.Gen("invalid ddl job %v", job)
	}

	// Save errors in job, so that others can know errors happened.
	if err != nil {
		// If job is not cancelled, we should log this error.
		if job.State != model.JobStateCancelled {
			log.Errorf("[ddl] run DDL job err %v", errors.ErrorStack(err))
		} else {
			log.Infof("[ddl] the DDL job is normal to cancel because %v", errors.ErrorStack(err))
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
func (d *ddl) waitSchemaChanged(ctx goctx.Context, waitTime time.Duration, latestSchemaVersion int64) {
	if waitTime == 0 {
		return
	}

	timeStart := time.Now()
	// TODO: Do we need to wait for a while?
	if latestSchemaVersion == 0 {
		log.Infof("[ddl] schema version doesn't change")
		return
	}

	if ctx == nil {
		var cancelFunc goctx.CancelFunc
		ctx, cancelFunc = goctx.WithTimeout(goctx.Background(), waitTime)
		defer cancelFunc()
	}
	err := d.schemaSyncer.OwnerUpdateGlobalVersion(ctx, latestSchemaVersion)
	if err != nil {
		log.Infof("[ddl] update latest schema version %d failed %v", latestSchemaVersion, err)
		if terror.ErrorEqual(err, goctx.DeadlineExceeded) {
			return
		}
	}

	err = d.schemaSyncer.OwnerCheckAllVersions(ctx, latestSchemaVersion)
	if err != nil {
		log.Infof("[ddl] wait latest schema version %d to deadline %v", latestSchemaVersion, err)
		if terror.ErrorEqual(err, goctx.DeadlineExceeded) {
			return
		}
		select {
		case <-ctx.Done():
			return
		}
	}
	log.Infof("[ddl] wait latest schema version %v changed, take time %v", latestSchemaVersion, time.Since(timeStart))
	return
}

// waitSchemaSynced handles the following situation:
// If the job enters a new state, and the worker crashs when it's in the process of waiting for 2 * lease time,
// Then the worker restarts quickly, we may run the job immediately again,
// but in this case we don't wait enough 2 * lease time to let other servers update the schema.
// So here we get the latest schema version to make sure all servers' schema version update to the latest schema version
// in a cluster, or to wait for 2 * lease time.
func (d *ddl) waitSchemaSynced(job *model.Job, waitTime time.Duration) {
	if !job.IsRunning() && !job.IsDone() {
		return
	}
	// TODO: Make ctx exits when the d is close.
	ctx, cancelFunc := goctx.WithTimeout(goctx.Background(), waitTime)
	defer cancelFunc()

	startTime := time.Now()
	latestSchemaVersion, err := d.schemaSyncer.MustGetGlobalVersion(ctx)
	if err != nil {
		log.Warnf("[ddl] handle exception take time %v", time.Since(startTime))
		return
	}
	d.waitSchemaChanged(ctx, waitTime, latestSchemaVersion)
	log.Infof("[ddl] the handle exception take time %v", time.Since(startTime))
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
