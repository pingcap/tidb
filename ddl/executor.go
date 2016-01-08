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

func (d *ddl) handleTaskQueue() error {
	for {
		if d.isClosed() {
			return nil
		}

		task := &model.Job{}
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			owner, err := d.checkOwner(t, ddlTaskFlag)
			if terror.ErrorEqual(err, ErrNotOwner) {
				return nil
			}
			if err != nil {
				return errors.Trace(err)
			}

			// get the first task and run
			task, err = d.getFirstTask(t)
			if err != nil {
				return errors.Trace(err)
			}
			if task == nil {
				return nil
			}

			d.runTask(t, task)
			if task.IsFinished() {
				err = d.finishTask(t, task)
			} else {
				err = d.updateTask(t, task)
			}
			if err != nil {
				return errors.Trace(err)
			}

			owner.LastUpdateTS = time.Now().UnixNano()
			err = t.SetDDLTaskOwner(owner)

			return errors.Trace(err)
		})

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) runTask(t *meta.Meta, task *model.Job) {
	task.State = model.JobRunning

	var err error
	switch task.Type {
	case model.ActionDropSchema:
		err = d.delReorgSchema(t, task)
	case model.ActionDropTable:
		err = d.delReorgTable(t, task)
		//	case model.ActionDropColumn:
		//		err = d.delReorgColumn(t, task)
		//	case model.ActionDropIndex:
		//		err = d.delReorgIndex(t, task)
	default:
		task.State = model.JobCancelled
		err = errors.Errorf("invalid task %v", task)
	}

	if err != nil {
		if task.State != model.JobCancelled {
			log.Errorf("run task err %v", errors.ErrorStack(err))
		}

		task.Error = err.Error()
		task.ErrorCount++
	}
}

func (d *ddl) prepareTask(job *model.Job) error {
	task := &model.Job{
		ID:       job.ID,
		SchemaID: job.SchemaID,
		TableID:  job.TableID,
		Type:     job.Type,
	}
	copy(task.Args, job.Args)

	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err1 := t.EnQueueDDLTask(task)

		return errors.Trace(err1)
	})

	return errors.Trace(err)
}

func (d *ddl) startTask(tp model.ActionType) {
	switch tp {
	case model.ActionDropSchema, model.ActionDropTable,
		model.ActionDropColumn, model.ActionDropIndex:
		asyncNotify(d.taskCh)
	}
}

func (d *ddl) getFirstTask(t *meta.Meta) (*model.Job, error) {
	task, err := t.GetDDLTask(0)
	return task, errors.Trace(err)
}

func (d *ddl) updateTask(t *meta.Meta, task *model.Job) error {
	err := t.UpdateDDLTask(0, task)
	return errors.Trace(err)
}

func (d *ddl) finishTask(t *meta.Meta, task *model.Job) error {
	log.Warnf("[ddl] finish DDL task %v", task)
	if _, err := t.DeQueueDDLTask(); err != nil {
		return errors.Trace(err)
	}

	err := t.AddHistoryDDLTask(task)

	return errors.Trace(err)
}

func (d *ddl) onExecute() {
	defer d.wait.Done()

	// ensure that have ddl job convert to ddl task
	checkTime := chooseLeaseTime(8*d.lease, 10*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Debugf("[ddl] wait %s to check DDL task status again", checkTime)
		case <-d.taskCh:
		case <-d.quitCh:
			return
		}

		err := d.handleTaskQueue()
		if err != nil {
			log.Errorf("[ddl] handle task err %v", errors.ErrorStack(err))
		}
	}
}
