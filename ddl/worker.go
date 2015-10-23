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
	"encoding/json"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/meta"
)

// ActionType is the type for DDL action.
type ActionType byte

// List DDL actions.
const (
	ActionNone ActionType = iota
	ActionCreateSchema
	ActionDropSchema
	ActionCreateTable
	ActionDropTable
	ActionAddColumn
	ActionDropColumn
	ActionAddIndex
	ActionDropIndex
	ActionAddConstraint
	ActionDropConstraint
)

// Job is for a DDL operation.
type Job struct {
	ID   int64      `json:"id"`
	Type ActionType `json:"type"`
	// SchemaID is 0 if creating schema.
	SchemaID int64 `json:"schema_id"`
	// TableID is 0 if creating table.
	TableID int64         `json:"table_id"`
	Args    []interface{} `json:"args"`
	State   JobState      `json:"state"`
	Error   string        `json:"err"`
}

// JobState is for job.
type JobState byte

// List job states.
const (
	JobNone JobState = iota
	JobRunning
	JobDone
	JobCancelled
)

// String implements fmt.Stringer interface.
func (s JobState) String() string {
	switch s {
	case JobRunning:
		return "running"
	case JobDone:
		return "done"
	case JobCancelled:
		return "cancelled"
	default:
		return "none"
	}
}

// Owner is for DDL Owner.
type Owner struct {
	OwnerID      string `json:"owner_id"`
	LastUpdateTs int64  `json:"last_update_ts"`
}

func (d *ddl) startJob(ctx context.Context, job *Job) error {
	// for every DDL, we must commit current transaction.
	if err := ctx.FinishTxn(false); err != nil {
		return errors.Trace(err)
	}

	// alloc a global job id.
	// add this job to job queue.
	err := d.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		var err error
		job.ID, err = t.GenDDLJobID()
		if err != nil {
			return errors.Trace(err)
		}

		var b []byte
		b, err = json.Marshal(job)
		if err != nil {
			return errors.Trace(err)
		}

		err = t.PushDDLJob(b)

		return errors.Trace(err)
	})

	if err != nil {
		return errors.Trace(err)
	}

	// notice worker that we push a new job and wait the job done.
	asyncNotice(d.jobCh)

	// check
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
		if job.State == JobDone {
			return nil
		}

		return errors.Errorf("job is %s, err :%v", job.State, job.Error)
	}
}

func (d *ddl) getHistoryJob(id int64) (*Job, error) {
	var (
		job   Job
		found = false
	)
	err := d.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		b, err1 := t.GetHistoryDDLJob(id)
		if err1 != nil {
			return errors.Trace(err1)
		}

		err1 = json.Unmarshal(b, &job)
		if err1 != nil {
			return errors.Trace(err1)
		}

		found = true

		return nil
	})

	if err != nil || !found {
		return nil, errors.Trace(err)
	}

	return &job, nil
}

func asyncNotice(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *ddl) checkOwner() (*Owner, error) {
	var o Owner
	err := d.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		value, err := t.GetDDLOwner()
		if err != nil {
			return errors.Trace(err)
		}

		if value == nil {
			// try to set onwer
			o.OwnerID = d.uuid
		} else {
			err = json.Unmarshal(value, &o)
			if err != nil {
				return errors.Trace(err)
			}
		}

		now := time.Now().Unix()
		maxTimeout := int64(4 * d.lease)
		if o.OwnerID == d.uuid || now-o.LastUpdateTs > maxTimeout {
			o.OwnerID = d.uuid
			o.LastUpdateTs = now

			value, err = json.Marshal(o)
			if err != nil {
				return errors.Trace(err)
			}

			// update or try to set itself as owner.
			err = t.SetDDLOwner(value)
		}

		return errors.Trace(err)
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	return &o, nil
}

func (d *ddl) updateJob(t *meta.TMeta, j *Job) error {
	if d.owner == nil {
		return errors.Errorf("not owner, can't run job")
	}

	b, err := json.Marshal(j)
	if err != nil {
		return errors.Trace(err)
	}

	err = t.UpdateDDLJob(0, b)
	if err != nil {
		return errors.Trace(err)
	}

	d.owner.LastUpdateTs = time.Now().Unix()
	b, err = json.Marshal(d.owner)
	if err != nil {
		return errors.Trace(err)
	}

	err = t.SetDDLOwner(b)

	return errors.Trace(err)
}

func (d *ddl) getFirstJob() (*Job, error) {
	var (
		job   Job
		found = false
	)

	err := d.meta.RunInNewTxn(true, func(t *meta.TMeta) error {
		b, err := t.GetDDLJob(0)
		if err != nil {
			return errors.Trace(err)
		}

		if b == nil {
			return nil
		}

		err = json.Unmarshal(b, &job)
		if err != nil {
			return errors.Trace(err)
		}
		found = true
		return nil
	})

	if err != nil || !found {
		return nil, errors.Trace(err)
	}

	return &job, nil
}

func (d *ddl) finishJob(job *Job) error {
	// done, notice and run next job.
	err := d.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		var err1 error
		_, err1 = t.PopDDLJob()
		if err1 != nil {
			return errors.Trace(err1)
		}

		// add job to job history
		var b []byte
		b, err1 = json.Marshal(job)
		if err1 != nil {
			return errors.Trace(err1)
		}

		err1 = t.AddHistoryDDLJob(job.ID, b)

		return errors.Trace(err1)
	})

	return errors.Trace(err)
}

func (d *ddl) handleJobQueue() error {
	for {
		owner, err := d.checkOwner()
		if err != nil {
			return errors.Errorf("check ddl owner err %v", err)
		}

		if owner.OwnerID != d.uuid {
			d.owner = nil
			// not the owner
			return nil
		}

		d.owner = owner

		// become the owner
		// get the first job and run
		var job *Job
		job, err = d.getFirstJob()
		if job == nil || err != nil {
			return errors.Trace(err)
		}

		err = d.runJob(job)
		if err != nil {
			return errors.Errorf("run job %v failed, err %v", job, err)
		}
		// done, notice and run next job.
		err = d.finishJob(job)
		if err != nil {
			return errors.Trace(err)
		}

		asyncNotice(d.jobDoneCh)
	}
}

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
			log.Errorf("handle job err %v", err)
		}
	}
}

func (d *ddl) runJob(job *Job) error {
	if job.State == JobDone || job.State == JobCancelled {
		return nil
	}

	job.State = JobRunning

	var err error
	switch job.Type {
	case ActionCreateSchema:
	case ActionDropSchema:
	case ActionCreateTable:
	case ActionDropTable:
	case ActionAddColumn:
	case ActionDropColumn:
	case ActionAddIndex:
	case ActionDropIndex:
	case ActionAddConstraint:
	case ActionDropConstraint:
	default:
		return errors.Errorf("invalid job %v", job)
	}

	// if err and inner doesn't cancel job, return err.
	if err != nil {
		if job.State != JobCancelled {
			return errors.Trace(err)
		}

		job.Error = err.Error()
	}

	if err == nil {
		job.State = JobDone
	}

	return nil
}
