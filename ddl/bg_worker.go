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
	"github.com/pingcap/tidb/terror"
)

// handleBgJobQueue handles background job queue.
func (d *ddl) handleBgJobQueue() error {
	if d.isClosed() {
		return nil
	}

	task := &model.Job{}
	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		owner, err := d.checkOwner(t, bgJobFlag)
		if terror.ErrorEqual(err, ErrNotOwner) {
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// get the first background job and run
		task, err = d.getFirstBgJob(t)
		if err != nil {
			return errors.Trace(err)
		}
		if task == nil {
			return nil
		}

		d.runBgJob(t, task)
		if task.IsFinished() {
			err = d.finishBgJob(t, task)
		} else {
			err = d.updateBgJob(t, task)
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

	return nil
}

// runBgJob runs background job.
func (d *ddl) runBgJob(t *meta.Meta, task *model.Job) {
	task.State = model.JobRunning

	var err error
	switch task.Type {
	case model.ActionDropSchema:
		err = d.delReorgSchema(t, task)
	case model.ActionDropTable:
		err = d.delReorgTable(t, task)
	default:
		task.State = model.JobCancelled
		err = errors.Errorf("invalid background job %v", task)
	}

	if err != nil {
		if task.State != model.JobCancelled {
			log.Errorf("run background job err %v", errors.ErrorStack(err))
		}

		task.Error = err.Error()
		task.ErrorCount++
	}
}

// prepareBgJob prepares background job.
func (d *ddl) prepareBgJob(job *model.Job) error {
	task := &model.Job{
		ID:       job.ID,
		SchemaID: job.SchemaID,
		TableID:  job.TableID,
		Type:     job.Type,
		Args:     job.Args,
	}

	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err1 := t.EnQueueBgJob(task)

		return errors.Trace(err1)
	})

	return errors.Trace(err)
}

// startBgJob starts background job.
func (d *ddl) startBgJob(tp model.ActionType) {
	switch tp {
	case model.ActionDropSchema, model.ActionDropTable:
		asyncNotify(d.bgJobCh)
	}
}

// getFirstBgJob gets the first background job.
func (d *ddl) getFirstBgJob(t *meta.Meta) (*model.Job, error) {
	task, err := t.GetBgJob(0)
	return task, errors.Trace(err)
}

// updateBgJob updates background job.
func (d *ddl) updateBgJob(t *meta.Meta, task *model.Job) error {
	err := t.UpdateBgJob(0, task)
	return errors.Trace(err)
}

// finishBgJob finishs background job.
func (d *ddl) finishBgJob(t *meta.Meta, task *model.Job) error {
	log.Warnf("[ddl] finish background job %v", task)
	if _, err := t.DeQueueBgJob(); err != nil {
		return errors.Trace(err)
	}

	err := t.AddHistoryBgJob(task)

	return errors.Trace(err)
}

func (d *ddl) onBackgroundWorker() {
	defer d.wait.Done()

	// ensure that have ddl job convert to background job.
	checkTime := chooseLeaseTime(8*d.lease, 10*time.Second)

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
