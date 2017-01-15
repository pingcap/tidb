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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

// handleBgJobQueue handles the background job queue.
func (d *ddl) handleBgJobQueue() error {
	if d.isClosed() {
		return nil
	}

	job := &model.Job{}
	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		owner, err := d.checkOwner(t, bgJobFlag)
		if err != nil {
			return errors.Trace(filterError(err, errNotOwner))
		}

		// Get the first background job and run it.
		job, err = d.getFirstBgJob(t)
		if err != nil {
			return errors.Trace(err)
		}
		if job == nil {
			return nil
		}

		d.runBgJob(t, job)
		if job.IsFinished() {
			err = d.finishBgJob(t, job)
		} else {
			err = d.updateBgJob(t, job)
		}
		if err != nil {
			return errors.Trace(err)
		}

		owner.LastUpdateTS = time.Now().UnixNano()
		err = t.SetBgJobOwner(owner)

		return errors.Trace(err)
	})
	if err != nil {
		return errors.Trace(err)
	}

	d.hookMu.Lock()
	d.hook.OnBgJobUpdated(job)
	d.hookMu.Unlock()

	return nil
}

// runBgJob runs a background job.
func (d *ddl) runBgJob(t *meta.Meta, job *model.Job) {
	job.State = model.JobRunning

	var err error
	switch job.Type {
	case model.ActionDropSchema:
		err = d.delReorgSchema(t, job)
	case model.ActionDropTable, model.ActionTruncateTable:
		err = d.delReorgTable(t, job)
	default:
		job.State = model.JobCancelled
		err = errInvalidBgJob
	}

	if err != nil {
		if job.State != model.JobCancelled {
			log.Errorf("[ddl] run background job err %v", errors.ErrorStack(err))
		}
		job.Error = toTError(err)
		job.ErrorCount++
	}
}

// prepareBgJob prepares a background job.
func (d *ddl) prepareBgJob(t *meta.Meta, ddlJob *model.Job) error {
	job := &model.Job{
		ID:       ddlJob.ID,
		SchemaID: ddlJob.SchemaID,
		TableID:  ddlJob.TableID,
		Type:     ddlJob.Type,
		Args:     ddlJob.Args,
	}

	err := t.EnQueueBgJob(job)
	return errors.Trace(err)
}

// startBgJob starts a background job.
func (d *ddl) startBgJob(tp model.ActionType) {
	switch tp {
	case model.ActionDropSchema, model.ActionDropTable, model.ActionTruncateTable:
		asyncNotify(d.bgJobCh)
	}
}

// getFirstBgJob gets the first background job.
func (d *ddl) getFirstBgJob(t *meta.Meta) (*model.Job, error) {
	job, err := t.GetBgJob(0)
	return job, errors.Trace(err)
}

// updateBgJob updates a background job.
func (d *ddl) updateBgJob(t *meta.Meta, job *model.Job) error {
	err := t.UpdateBgJob(0, job)
	return errors.Trace(err)
}

// finishBgJob finishs a background job.
func (d *ddl) finishBgJob(t *meta.Meta, job *model.Job) error {
	log.Infof("[ddl] finish background job %v", job)
	if _, err := t.DeQueueBgJob(); err != nil {
		return errors.Trace(err)
	}

	err := t.AddHistoryBgJob(job)

	return errors.Trace(err)
}

func (d *ddl) onBackgroundWorker() {
	defer d.wait.Done()

	// We use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time. If lease is 0, we will use default 10s.
	checkTime := chooseLeaseTime(2*d.lease, 10*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Debugf("[ddl] wait %s to check background job status again", checkTime)
		case <-d.bgJobCh:
		case <-d.quitCh:
			return
		}

		err := d.handleBgJobQueue()
		if err != nil {
			log.Errorf("[ddl] handle background job err %v", errors.ErrorStack(err))
		}
	}
}
