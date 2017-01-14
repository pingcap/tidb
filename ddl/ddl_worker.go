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
)

// onDDLWorker is for async online schema changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schema change job.
func (d *ddl) onDDLWorker() {
	defer d.wait.Done()

	// We use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time. If lease is 0, we will use default 10s.
	checkTime := chooseLeaseTime(2*d.lease, 10*time.Second)

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

const maxOwnerTimeout = int64(20 * time.Minute)

// We define minBgOwnerTimeout and minDDLOwnerTimeout as variable,
// because we need to change them in test.
var (
	minBgOwnerTimeout  = int64(20 * time.Second)
	minDDLOwnerTimeout = int64(4 * time.Second)
)

func (d *ddl) getCheckOwnerTimeout(flag JobType) int64 {
	// we must wait 2 * lease time to guarantee other servers update the schema,
	// the owner will update its owner status every 2 * lease time, so here we use
	// 4 * lease to check its timeout.
	timeout := int64(4 * d.lease)
	if timeout > maxOwnerTimeout {
		return maxOwnerTimeout
	}

	// The value of lease may be less than 1 second, so the operation of
	// checking owner is frequent and it isn't necessary.
	// So if timeout is less than 4 second, we will use default minDDLOwnerTimeout.
	if flag == ddlJobFlag && timeout < minDDLOwnerTimeout {
		return minDDLOwnerTimeout
	}
	if flag == bgJobFlag && timeout < minBgOwnerTimeout {
		// Background job is serial processing, so we can extend the owner timeout to make sure
		// a batch of rows will be processed before timeout.
		// If timeout is less than maxBgOwnerTimeout, we will use default minBgOwnerTimeout.
		return minBgOwnerTimeout
	}
	return timeout
}

func (d *ddl) checkOwner(t *meta.Meta, flag JobType) (*model.Owner, error) {
	owner, err := d.getJobOwner(t, flag)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if owner == nil {
		owner = &model.Owner{}
		// try to set onwer
		owner.OwnerID = d.uuid
	}

	now := time.Now().UnixNano()
	maxTimeout := d.getCheckOwnerTimeout(flag)
	sub := now - owner.LastUpdateTS
	if owner.OwnerID == d.uuid || sub > maxTimeout {
		owner.OwnerID = d.uuid
		owner.LastUpdateTS = now
		// update status.
		switch flag {
		case ddlJobFlag:
			err = t.SetDDLJobOwner(owner)
		case bgJobFlag:
			err = t.SetBgJobOwner(owner)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Debugf("[ddl] become %s job owner, owner is %s sub %vs", flag, owner, sub/1e9)
	}

	if owner.OwnerID != d.uuid {
		log.Debugf("[ddl] not %s job owner, self id %s owner is %s", flag, d.uuid, owner.OwnerID)
		return nil, errors.Trace(errNotOwner)
	}

	return owner, nil
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
func (d *ddl) updateDDLJob(t *meta.Meta, job *model.Job) error {
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
	for {
		if d.isClosed() {
			return nil
		}

		waitTime := 2 * d.lease
		var job *model.Job
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			owner, err := d.checkOwner(t, ddlJobFlag)
			if terror.ErrorEqual(err, errNotOwner) {
				// We are not owner, return and retry checking later.
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}

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
				elapsed := time.Duration(time.Now().UnixNano() - job.LastUpdateTS)
				if elapsed > 0 && elapsed < waitTime {
					log.Warnf("[ddl] the elapsed time from last update is %s < %s, wait again", elapsed, waitTime)
					waitTime -= elapsed
					return nil
				}
			}

			d.hookMu.Lock()
			d.hook.OnJobRunBefore(job)
			d.hookMu.Unlock()

			// If running job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			d.runDDLJob(t, job)
			if job.IsFinished() {
				binloginfo.SetDDLBinlog(txn, job.ID, job.Query)
				err = d.finishDDLJob(t, job)
			} else {
				err = d.updateDDLJob(t, job)
			}
			if err != nil {
				return errors.Trace(err)
			}

			// Running job may cost some time, so here we must update owner status to
			// prevent other become the owner.
			owner.LastUpdateTS = time.Now().UnixNano()
			err = t.SetDDLJobOwner(owner)
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
			switch job.Type {
			case model.ActionCreateSchema, model.ActionDropSchema, model.ActionCreateTable,
				model.ActionTruncateTable, model.ActionDropTable:
				// Do not need to wait for those DDL, because those DDL do not need to modify data,
				// So there is no data inconsistent issue.
			default:
				d.waitSchemaChanged(waitTime)
			}
		}
		if job.IsFinished() {
			d.startBgJob(job.Type)
			asyncNotify(d.ddlJobDoneCh)
		}
	}
}

func chooseLeaseTime(n1 time.Duration, n2 time.Duration) time.Duration {
	if n1 > 0 {
		return n1
	}

	return n2
}

// runDDLJob runs a DDL job.
func (d *ddl) runDDLJob(t *meta.Meta, job *model.Job) {
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
		err = d.onCreateSchema(t, job)
	case model.ActionDropSchema:
		err = d.onDropSchema(t, job)
	case model.ActionCreateTable:
		err = d.onCreateTable(t, job)
	case model.ActionDropTable:
		err = d.onDropTable(t, job)
	case model.ActionAddColumn:
		err = d.onAddColumn(t, job)
	case model.ActionDropColumn:
		err = d.onDropColumn(t, job)
	case model.ActionModifyColumn:
		err = d.onModifyColumn(t, job)
	case model.ActionAddIndex:
		err = d.onCreateIndex(t, job)
	case model.ActionDropIndex:
		err = d.onDropIndex(t, job)
	case model.ActionAddForeignKey:
		err = d.onCreateForeignKey(t, job)
	case model.ActionDropForeignKey:
		err = d.onDropForeignKey(t, job)
	case model.ActionTruncateTable:
		err = d.onTruncateTable(t, job)
	case model.ActionRenameTable:
		err = d.onRenameTable(t, job)
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

// For every lease, we will re-update the whole schema, so we will wait 2 * lease time
// to guarantee that all servers have already updated schema.
func (d *ddl) waitSchemaChanged(waitTime time.Duration) {
	if waitTime == 0 {
		return
	}

	select {
	case <-time.After(waitTime):
	case <-d.quitCh:
	}
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
