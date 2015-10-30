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
	"github.com/pingcap/tidb/util/errors2"
)

func (d *ddl) startJob(ctx context.Context, job *model.Job) error {
	// for every DDL, we must commit current transaction.
	if err := ctx.FinishTxn(false); err != nil {
		return errors.Trace(err)
	}

	// Create a new job and queue it.
	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		job.ID, err = t.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}

		err = t.EnQueueDDLJob(job)
		return errors.Trace(err)
	})

	if err != nil {
		return errors.Trace(err)
	}

	// notice worker that we push a new job and wait the job done.
	asyncNotify(d.jobCh)

	log.Warnf("start DDL job %v", job)

	jobID := job.ID

	var historyJob *model.Job

	// for a job from start to end, the state of it will be none -> delete only -> write only -> reorgnization -> public
	// for every state change, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, 10*time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-d.jobDoneCh:
		case <-ticker.C:
		}

		historyJob, err = d.getHistoryJob(jobID)
		if err != nil {
			log.Errorf("get history job err %v, check again", err)
			continue
		} else if historyJob == nil {
			log.Warnf("job %d is not in history, maybe not run", jobID)
			continue
		}

		// if a job is a history table, the state must be JobDone or JobCancel.
		if historyJob.State == model.JobDone {
			return nil
		}

		return errors.Errorf(historyJob.Error)
	}
}

func (d *ddl) getHistoryJob(id int64) (*model.Job, error) {
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

func (d *ddl) checkOwner(t *meta.Meta) (*model.Owner, error) {
	owner, err := t.GetDDLOwner()
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
	if owner.OwnerID == d.uuid || now-owner.LastUpdateTS > maxTimeout {
		owner.OwnerID = d.uuid
		owner.LastUpdateTS = now
		// update status.
		if err = t.SetDDLOwner(owner); err != nil {
			return nil, errors.Trace(err)
		}
		log.Debugf("become owner %s", owner.OwnerID)
	}

	if owner.OwnerID != d.uuid {
		log.Debugf("not owner, owner is %s", owner.OwnerID)
		return nil, errors.Trace(ErrNotOwner)
	}

	return owner, nil
}

func (d *ddl) getFirstJob(t *meta.Meta) (*model.Job, error) {
	job, err := t.GetDDLJob(0)
	return job, errors.Trace(err)
}

// every time we enter another state except final state, we must call this function.
func (d *ddl) updateJob(t *meta.Meta, job *model.Job) error {
	err := t.UpdateDDLJob(0, job)
	return errors.Trace(err)
}

func (d *ddl) finishJob(t *meta.Meta, job *model.Job) error {
	log.Warnf("finish DDL job %v", job)
	// done, notice and run next job.
	_, err := t.DeQueueDDLJob()
	if err != nil {
		return errors.Trace(err)
	}

	err = t.AddHistoryDDLJob(job)
	return errors.Trace(err)
}

// ErrNotOwner means we are not owner and can't handle DDL jobs.
var ErrNotOwner = errors.New("DDL: not owner")

func (d *ddl) handleJobQueue() error {
	for {
		if d.isClosed() {
			return nil
		}

		var job *model.Job
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			owner, err := d.checkOwner(t)
			if errors2.ErrorEqual(err, ErrNotOwner) {
				// we are not owner, return and retry checking later.
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}

			// become the owner
			// get the first job and run
			job, err = d.getFirstJob(t)
			if job == nil || err != nil {
				return errors.Trace(err)
			}

			log.Warnf("run DDL job %v", job)
			err = d.runJob(t, job)
			if err != nil {
				return errors.Trace(err)
			}

			if job.State == model.JobDone || job.State == model.JobCancelled {
				err = d.finishJob(t, job)
				if err == nil {
					asyncNotify(d.jobDoneCh)
				}
			} else {
				err = d.updateJob(t, job)
			}

			if err != nil {
				return errors.Trace(err)
			}

			// running job may cost some time, so here we must update owner status to
			// prevent other become the owner.
			owner.LastUpdateTS = time.Now().UnixNano()
			if err = t.SetDDLOwner(owner); err != nil {
				return errors.Trace(err)
			}
			return errors.Trace(err)
		})

		if err != nil {
			return errors.Trace(err)
		} else if job == nil {
			// no job now, return and retry get later.
			return nil
		}

		// here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// if the job is done or still running, we will wait 2 * lease time to guarantee other servers to update
		// the newest schema.
		if job.State == model.JobRunning || job.State == model.JobDone {
			d.waitSchemaChanged()
		}
	}
}

func chooseLeaseTime(n1 time.Duration, n2 time.Duration) time.Duration {
	if n1 > 0 {
		return n1
	}

	return n2
}

// onWorker is for async online schema change, it will try to become the owner first,
// then wait or pull the job queue to handle a schema change job.
func (d *ddl) onWorker() {
	defer d.wait.Done()

	// we use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time, if lease is 0, we will use default 10s.
	checkTime := chooseLeaseTime(2*d.lease, 10*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Debugf("wait %s to check DDL status again", checkTime)
		case <-d.jobCh:
		case <-d.quitCh:
			return
		}

		err := d.handleJobQueue()
		if err != nil {
			log.Errorf("handle job err %v", errors.ErrorStack(err))
		}
	}
}

func (d *ddl) runJob(t *meta.Meta, job *model.Job) error {
	if job.State == model.JobDone || job.State == model.JobCancelled {
		return nil
	}

	job.State = model.JobRunning

	var err error
	switch job.Type {
	case model.ActionCreateSchema:
		err = d.onSchemaCreate(t, job)
	case model.ActionDropSchema:
		err = d.onSchemaDrop(t, job)
	case model.ActionCreateTable:
		err = d.onTableCreate(t, job)
	case model.ActionDropTable:
		err = d.onTableDrop(t, job)
	case model.ActionAddColumn:
	case model.ActionDropColumn:
	case model.ActionAddIndex:
		err = d.onIndexCreate(t, job)
	case model.ActionDropIndex:
		err = d.onIndexDrop(t, job)
	case model.ActionAddConstraint:
	case model.ActionDropConstraint:
	default:
		// invalid job, cancel it.
		job.State = model.JobCancelled
		err = errors.Errorf("invalid job %v", job)
	}

	// if err and inner doesn't cancel job, return err.
	if err != nil {
		if job.State != model.JobCancelled {
			return errors.Trace(err)
		}

		job.Error = err.Error()
	}

	return nil
}

// for every lease seconds, we will re-update the whole schema, so we will wait 2 * lease time
// to guarantee that all servers have already updated schema.
func (d *ddl) waitSchemaChanged() {
	if d.lease > 0 {
		select {
		case <-time.After(d.lease * 2):
		case <-d.quitCh:
		}
	}
}
