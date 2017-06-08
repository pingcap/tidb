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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/terror"
	goctx "golang.org/x/net/context"
)

// RunWorker indicates if this TiDB server starts DDL worker and can run DDL job.
var RunWorker = true

// onDDLWorker is for async online schema changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schema change job.
func (d *ddl) onDDLWorker() {
	defer d.wait.Done()
	if !RunWorker {
		return
	}

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

func (d *ddl) isOwner(flag JobType) bool {
	if flag == ddlJobFlag {
		isOwner := d.ownerManager.IsOwner()
		log.Debugf("[ddl] it's the %s job owner %v, self id %s", flag, isOwner, d.uuid)
		return isOwner
	}
	isOwner := d.ownerManager.IsBgOwner()
	log.Debugf("[ddl] it's the %s job owner %v, self id %s", flag, isOwner, d.uuid)
	return isOwner
}

func (d *ddl) getJobOwner(t *meta.Meta, flag JobType) (*model.Owner, error) {
	var owner *model.Owner
	var err error

	switch flag {
	case ddlJobFlag:
		owner, err = t.GetDDLJobOwner()
	case bgJobFlag:
		owner, err = t.GetBgJobOwner()
	default:
		err = errInvalidJobFlag
	}

	return owner, errors.Trace(err)
}

// addDDLJob gets a global job ID and puts the DDL job in the DDL queue.
func (d *ddl) addDDLJob(ctx context.Context, job *model.Job) error {
	job.Query, _ = ctx.Value(context.QueryString).(string)
	return kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)

		var err error
		job.ID, err = t.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}

		err = t.EnQueueDDLJob(job)
		return errors.Trace(err)
	})
}

// getFirstDDLJob gets the first DDL job form DDL queue.
func (d *ddl) getFirstDDLJob(t *meta.Meta) (*model.Job, error) {
	job, err := t.GetDDLJob(0)
	return job, errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
// Every time we enter another state except final state, we must call this function.
func (d *ddl) updateDDLJob(t *meta.Meta, job *model.Job, updateTS uint64) error {
	job.LastUpdateTS = int64(updateTS)
	err := t.UpdateDDLJob(0, job)
	return errors.Trace(err)
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (d *ddl) finishDDLJob(t *meta.Meta, job *model.Job) error {
	log.Infof("[ddl] finish DDL job %v", job)
	// Job is finished, notice and run the next job.
	_, err := t.DeQueueDDLJob()
	if err != nil {
		return errors.Trace(err)
	}
	switch job.Type {
	case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable:
		if err = d.prepareBgJob(t, job); err != nil {
			return errors.Trace(err)
		}
	}

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

// JobType is job type, including ddl/background.
type JobType int

const (
	ddlJobFlag = iota + 1
	bgJobFlag
)

func (j JobType) String() string {
	switch j {
	case ddlJobFlag:
		return "ddl"
	case bgJobFlag:
		return "background"
	}

	return "unknown"
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
			if !d.isOwner(ddlJobFlag) {
				return nil
			}

			var err error
			t := meta.NewMeta(txn)
			// We become the owner. Get the first job and run it.
			job, err = d.getFirstDDLJob(t)
			if job == nil || err != nil {
				return errors.Trace(err)
			}

			if job.IsRunning() {
				// If we enter a new state, crash when waiting 2 * lease time, and restart quickly,
				// we may run the job immediately again, but we don't wait enough 2 * lease time to
				// let other servers update the schema.
				// So here we must check the elapsed time from last update, if < 2 * lease, we must
				// wait again.
				// TODO: Check all versions to handle this.
				elapsed := time.Duration(int64(txn.StartTS()) - job.LastUpdateTS)
				if once && elapsed > 0 && elapsed < waitTime {
					log.Warnf("[ddl] the elapsed time from last update is %s < %s, wait again", elapsed, waitTime)
					waitTime -= elapsed
					time.Sleep(time.Millisecond)
					return nil
				}
			}
			once = false

			d.hookMu.Lock()
			d.hook.OnJobRunBefore(job)
			d.hookMu.Unlock()

			// If running job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			schemaVer = d.runDDLJob(t, job)
			if job.IsFinished() {
				binloginfo.SetDDLBinlog(txn, job.ID, job.Query)
				err = d.finishDDLJob(t, job)
			} else {
				err = d.updateDDLJob(t, job, txn.StartTS())
			}
			return errors.Trace(err)
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
		if job.State == model.JobRunning || job.State == model.JobDone {
			d.waitSchemaChanged(waitTime, schemaVer)
		}
		if job.IsFinished() {
			d.startBgJob(job.Type)
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

// runDDLJob runs a DDL job. It returns the current schema version in this transaction.
func (d *ddl) runDDLJob(t *meta.Meta, job *model.Job) (ver int64) {
	log.Infof("[ddl] run DDL job %s", job)
	if job.IsFinished() {
		return
	}

	if job.State != model.JobRollback {
		job.State = model.JobRunning
	}

	var err error
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
	case model.ActionRenameTable:
		ver, err = d.onRenameTable(t, job)
	case model.ActionSetDefaultValue:
		ver, err = d.onSetDefaultValue(t, job)
	default:
		// Invalid job, cancel it.
		job.State = model.JobCancelled
		err = errInvalidDDLJob.Gen("invalid ddl job %v", job)
	}

	// Save errors in job, so that others can know errors happened.
	if err != nil {
		// If job is not cancelled, we should log this error.
		if job.State != model.JobCancelled {
			log.Errorf("[ddl] run ddl job err %v", errors.ErrorStack(err))
		} else {
			log.Infof("[ddl] the job is normal to cancel because %v", errors.ErrorStack(err))
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
func (d *ddl) waitSchemaChanged(waitTime time.Duration, latestSchemaVersion int64) {
	if waitTime == 0 {
		return
	}

	// TODO: Do we need to wait for a while?
	if latestSchemaVersion == 0 {
		log.Infof("[ddl] schema version doesn't change")
		return
	}
	// TODO: Make ctx exits when the d is close.
	ctx, cancelFunc := goctx.WithTimeout(goctx.Background(), waitTime)
	defer cancelFunc()
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
	return
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
