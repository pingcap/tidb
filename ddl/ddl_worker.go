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
	"github.com/pingcap/tipb/go-binlog"
)

func (d *ddl) doDDLJob(ctx context.Context, job *model.Job) error {
	// for every DDL, we must commit current transaction.
	if err := ctx.CommitTxn(); err != nil {
		return errors.Trace(err)
	}
	var startTS uint64
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		job.ID, err = t.GenGlobalID()
		startTS = txn.StartTS()
		return errors.Trace(err)
	})
	if err != nil {
		return errors.Trace(err)
	}
	ddlQuery, _ := ctx.Value(context.QueryString).(string)
	job.Query = ddlQuery

	// Create a new job and queue it.
	err = kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err1 := t.EnQueueDDLJob(job)
		return errors.Trace(err1)
	})
	if err != nil {
		return errors.Trace(err)
	}

	// notice worker that we push a new job and wait the job done.
	asyncNotify(d.ddlJobCh)

	log.Warnf("[ddl] start DDL job %v", job)

	var historyJob *model.Job
	jobID := job.ID
	// for a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// for every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, 10*time.Second))
	startTime := time.Now()
	jobsGauge.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		jobsGauge.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String()).Desc()
		retLabel := handleJobSucc
		if err != nil {
			retLabel = handleJobFailed
		}
		handleJobHistogram.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String(),
			retLabel).Observe(time.Since(startTime).Seconds())
	}()
	for {
		select {
		case <-d.ddlJobDoneCh:
		case <-ticker.C:
		}

		historyJob, err = d.getHistoryDDLJob(jobID)
		if err != nil {
			log.Errorf("[ddl] get history DDL job err %v, check again", err)
			continue
		} else if historyJob == nil {
			log.Warnf("[ddl] DDL job %d is not in history, maybe not run", jobID)
			continue
		}

		// if a job is a history table, the state must be JobDone or JobCancel.
		if historyJob.State == model.JobDone {
			return nil
		}

		return errors.Trace(historyJob.Error)
	}
}

func (d *ddl) writePreDDLBinlog(job *model.Job, startTS uint64) error {
	if binloginfo.PumpClient == nil {
		return nil
	}
	bin := &binlog.Binlog{
		Tp:       binlog.BinlogType_PreDDL,
		DdlJobId: job.ID,
		StartTs:  int64(startTS),
		DdlQuery: []byte(job.Query),
	}
	err := binloginfo.WriteBinlog(bin)
	return errors.Trace(err)
}

func (d *ddl) writePostDDLBinlog(jobID int64, startTS, commitTS uint64) {
	if binloginfo.PumpClient == nil {
		return
	}
	bin := &binlog.Binlog{
		Tp:       binlog.BinlogType_PostDDL,
		DdlJobId: jobID,
		StartTs:  int64(startTS),
		CommitTs: int64(commitTS),
	}
	err := binloginfo.WriteBinlog(bin)
	if err != nil {
		log.Errorf("failed to write PostDDL binlog %v", err)
	}
	return
}

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

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

const maxBgOwnerTimeout = int64(10 * time.Minute)

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
	// we must wait 2 * lease time to guarantee other servers update the schema,
	// the owner will update its owner status every 2 * lease time, so here we use
	// 4 * lease to check its timeout.
	maxTimeout := int64(4 * d.lease)
	if flag == bgJobFlag {
		// Background job is serial processing, so we can extend the owner timeout to make sure
		// a batch of rows will be processed before timeout. So here we use 20 * lease to check its timeout.
		maxTimeout = int64(20 * d.lease)
		// If 20 * lease is greater than maxBgOwnerTimeout, we will use default maxBgOwnerTimeout.
		if maxTimeout > maxBgOwnerTimeout {
			maxTimeout = maxBgOwnerTimeout
		}
	}
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

func (d *ddl) getFirstDDLJob(t *meta.Meta) (*model.Job, error) {
	job, err := t.GetDDLJob(0)
	return job, errors.Trace(err)
}

// every time we enter another state except final state, we must call this function.
func (d *ddl) updateDDLJob(t *meta.Meta, job *model.Job) error {
	err := t.UpdateDDLJob(0, job)
	return errors.Trace(err)
}

func (d *ddl) finishDDLJob(t *meta.Meta, job *model.Job) error {
	log.Warnf("[ddl] finish DDL job %v", job)
	// done, notice and run next job.
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
		var binlogStartTS uint64
		var job *model.Job
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			owner, err := d.checkOwner(t, ddlJobFlag)
			if terror.ErrorEqual(err, errNotOwner) {
				// we are not owner, return and retry checking later.
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}

			// become the owner
			// get the first job and run
			job, err = d.getFirstDDLJob(t)
			if job == nil || err != nil {
				return errors.Trace(err)
			}

			if job.IsRunning() {
				// if we enter a new state, crash when waiting 2 * lease time, and restart quickly,
				// we may run the job immediately again, but we don't wait enough 2 * lease time to
				// let other servers update the schema.
				// so here we must check the elapsed time from last update, if < 2 * lease, we must
				// wait again.
				elapsed := time.Duration(time.Now().UnixNano() - job.LastUpdateTS)
				if elapsed > 0 && elapsed < waitTime {
					log.Warnf("[ddl] the elapsed time from last update is %s < %s, wait again", elapsed, waitTime)
					waitTime -= elapsed
					return nil
				}
			}

			log.Warnf("[ddl] run DDL job %v", job)

			d.hookMu.Lock()
			d.hook.OnJobRunBefore(job)
			d.hookMu.Unlock()

			// if run job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			d.runDDLJob(t, job)

			if job.IsFinished() {
				err = d.writePreDDLBinlogIfNeeded(txn, job, &binlogStartTS)
				if err != nil {
					return errors.Trace(err)
				}
				err = d.finishDDLJob(t, job)
			} else {
				err = d.updateDDLJob(t, job)
			}
			if err != nil {
				return errors.Trace(err)
			}

			// running job may cost some time, so here we must update owner status to
			// prevent other become the owner.
			owner.LastUpdateTS = time.Now().UnixNano()
			err = t.SetDDLJobOwner(owner)

			return errors.Trace(err)
		})
		if err != nil {
			return errors.Trace(err)
		} else if job == nil {
			// no job now, return and retry get later.
			return nil
		}
		if binlogStartTS != 0 {
			commitTS, err1 := d.store.CurrentVersion()
			if err1 == nil {
				d.writePostDDLBinlog(job.ID, binlogStartTS, commitTS.Ver)
			}
		}

		d.hookMu.Lock()
		d.hook.OnJobUpdated(job)
		d.hookMu.Unlock()

		// here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// if the job is done or still running, we will wait 2 * lease time to guarantee other servers to update
		// the newest schema.
		if job.State == model.JobRunning || job.State == model.JobDone {
			d.waitSchemaChanged(waitTime)
		}

		if job.IsFinished() {
			d.startBgJob(job.Type)
			asyncNotify(d.ddlJobDoneCh)
		}
	}
}

// writePreDDLBinlog writes preDDL binlog if job is done and the binlog has not been write before.
func (d *ddl) writePreDDLBinlogIfNeeded(txn kv.Transaction, job *model.Job, binlogStartTS *uint64) error {
	if job.IsDone() {
		// Avoid write multiple times.
		if *binlogStartTS == 0 {
			startTS := txn.StartTS()
			err := d.writePreDDLBinlog(job, startTS)
			if err != nil {
				return errors.Trace(err)
			}
			*binlogStartTS = startTS
		}
	}
	return nil
}

func chooseLeaseTime(n1 time.Duration, n2 time.Duration) time.Duration {
	if n1 > 0 {
		return n1
	}

	return n2
}

// onDDLWorker is for async online schema change, it will try to become the owner first,
// then wait or pull the job queue to handle a schema change job.
func (d *ddl) onDDLWorker() {
	defer d.wait.Done()

	// we use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time, if lease is 0, we will use default 10s.
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

func (d *ddl) runDDLJob(t *meta.Meta, job *model.Job) {
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
	default:
		// invalid job, cancel it.
		job.State = model.JobCancelled
		err = errInvalidDDLJob.Gen("invalid ddl job %v", job)
	}

	// saves error in job, so that others can know error happens.
	if err != nil {
		// if job is not cancelled, we should log this error.
		if job.State != model.JobCancelled {
			log.Errorf("[ddl] run ddl job err %v", errors.ErrorStack(err))
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

	// TODO: add the error code
	return terror.ClassDDL.New(terror.CodeUnknown, err.Error())
}

// for every lease seconds, we will re-update the whole schema, so we will wait 2 * lease time
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
	} else {
		diff.TableID = job.TableID
	}
	err = t.SetSchemaDiff(schemaVersion, diff)
	return schemaVersion, errors.Trace(err)
}
