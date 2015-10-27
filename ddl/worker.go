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
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

func (d *ddl) startJob(ctx context.Context, job *model.Job) error {
	// for every DDL, we must commit current transaction.
	if err := ctx.FinishTxn(false); err != nil {
		return errors.Trace(err)
	}

	// Create a new job and queue it.
	err := d.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
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

	jobID := job.ID
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.jobDoneCh:
		case <-ticker.C:
		}

		job, err = d.getHistoryJob(jobID)
		if err != nil {
			log.Errorf("get history job err %v, check again", err)
			continue
		} else if job == nil {
			log.Warnf("job %d is not in history, maybe not run", jobID)
			continue
		}

		// if a job is a history table, the state must be JobDone or JobCancel.
		if job.State == model.JobDone {
			return nil
		}

		return errors.Errorf(job.Error)
	}
}

func (d *ddl) getHistoryJob(id int64) (*model.Job, error) {
	var job *model.Job

	err := d.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
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

func (d *ddl) verifyOwner(t *meta.TMeta) error {
	owner, err := t.GetDDLOwner()
	if err != nil {
		return errors.Trace(err)
	}

	if owner == nil {
		owner = &model.Owner{}
		// try to set onwer
		owner.OwnerID = d.uuid
	}

	now := time.Now().Unix()
	// we must wait 2 * lease time to guarantee other servers update the schema,
	// the owner will update its owner status every 2 * lease time, so here we use
	// 4 * lease to check its timeout.
	maxTimeout := int64(4 * d.lease)
	if owner.OwnerID == d.uuid || now-owner.LastUpdateTS > maxTimeout {
		owner.OwnerID = d.uuid
		owner.LastUpdateTS = now

		// update or try to set itself as owner.
		if err = t.SetDDLOwner(owner); err != nil {
			return errors.Trace(err)
		}
	}

	if owner.OwnerID != d.uuid {
		return errors.Trace(ErrNotOwner)
	}

	return nil
}

func (d *ddl) getFirstJob(t *meta.TMeta) (*model.Job, error) {
	job, err := t.GetDDLJob(0)
	return job, errors.Trace(err)
}

// every time we enter another state except final state, we must call this function.
func (d *ddl) updateJob(t *meta.TMeta, job *model.Job) error {
	err := t.UpdateDDLJob(0, job)
	return errors.Trace(err)
}

func (d *ddl) finishJob(t *meta.TMeta, job *model.Job) error {
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
		var job *model.Job
		err := d.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
			err := d.verifyOwner(t)
			if err != nil {
				return errors.Trace(err)
			}

			// become the owner
			// get the first job and run
			job, err = d.getFirstJob(t)
			if job == nil || err != nil {
				return errors.Trace(err)
			}

			log.Warnf("run job %v", job)
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

// onWorker is for async online schema change, it will try to become the owner first,
// then wait or pull the job queue to handle a schema change job.
func (d *ddl) onWorker() {
	checkTime := 10 * time.Second
	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Warnf("wait %s to check status again", checkTime)
		case <-d.jobCh:
		}

		err := d.handleJobQueue()
		if err != nil {
			log.Errorf("handle job err %v", errors.ErrorStack(err))
		}
	}
}

func (d *ddl) runJob(t *meta.TMeta, job *model.Job) error {
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
	case model.ActionDropIndex:
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
		time.Sleep(time.Duration(d.lease) * time.Second * 2)
	}
}
