// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttlworker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const updateJobCurrentStatusTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_status = '%s' WHERE table_id = %d AND current_job_status = '%s' AND current_job_id = '%s'"
const finishJobTemplate = "UPDATE mysql.tidb_ttl_table_status SET last_job_id = current_job_id, last_job_start_time = current_job_start_time, last_job_finish_time = '%s', last_job_ttl_expire = current_job_ttl_expire, last_job_summary = '%s', current_job_id = NULL, current_job_owner_id = NULL, current_job_owner_hb_time = NULL, current_job_start_time = NULL, current_job_ttl_expire = NULL, current_job_state = NULL, current_job_status = NULL, current_job_status_update_time = NULL WHERE table_id = %d AND current_job_id = '%s'"
const updateJobStateTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_state = '%s' WHERE table_id = %d AND current_job_id = '%s' AND current_job_owner_id = '%s'"

func updateJobCurrentStatusSQL(tableID int64, oldStatus cache.JobStatus, newStatus cache.JobStatus, jobID string) string {
	return fmt.Sprintf(updateJobCurrentStatusTemplate, newStatus, tableID, oldStatus, jobID)
}

func finishJobSQL(tableID int64, finishTime time.Time, summary string, jobID string) string {
	return fmt.Sprintf(finishJobTemplate, finishTime.Format(timeFormat), summary, tableID, jobID)
}

func updateJobState(tableID int64, currentJobID string, currentJobState string, currentJobOwnerID string) string {
	return fmt.Sprintf(updateJobStateTemplate, currentJobState, tableID, currentJobID, currentJobOwnerID)
}

type ttlJob struct {
	id      string
	ownerID string

	ctx    context.Context
	cancel func()

	createTime time.Time

	tbl *cache.PhysicalTable

	tasks                   []*ttlScanTask
	taskIter                int
	finishedScanTaskCounter int
	scanTaskErr             error

	// status is the only field which should be protected by a mutex, as `Cancel` may be called at any time, and will
	// change the status
	statusMutex sync.Mutex
	status      cache.JobStatus

	statistics *ttlStatistics
}

// changeStatus updates the state of this job
func (job *ttlJob) changeStatus(ctx context.Context, se session.Session, status cache.JobStatus) error {
	job.statusMutex.Lock()
	oldStatus := job.status
	job.status = status
	job.statusMutex.Unlock()

	_, err := se.ExecuteSQL(ctx, updateJobCurrentStatusSQL(job.tbl.ID, oldStatus, status, job.id))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (job *ttlJob) updateState(ctx context.Context, se session.Session) error {
	_, err := se.ExecuteSQL(ctx, updateJobState(job.tbl.ID, job.id, job.statistics.String(), job.ownerID))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// peekScanTask returns the next scan task, but doesn't promote the iterator
func (job *ttlJob) peekScanTask() (*ttlScanTask, error) {
	return job.tasks[job.taskIter], nil
}

// nextScanTask promotes the iterator
func (job *ttlJob) nextScanTask() {
	job.taskIter += 1
}

// finish turns current job into last job, and update the error message and statistics summary
func (job *ttlJob) finish(se session.Session, now time.Time) {
	summary := job.statistics.String()
	if job.scanTaskErr != nil {
		summary = fmt.Sprintf("Scan Error: %s, Statistics: %s", job.scanTaskErr.Error(), summary)
	}
	// at this time, the job.ctx may have been canceled (to cancel this job)
	// even when it's canceled, we'll need to update the states, so use another context
	_, err := se.ExecuteSQL(context.TODO(), finishJobSQL(job.tbl.ID, now, summary, job.id))
	if err != nil {
		logutil.Logger(job.ctx).Error("fail to finish a ttl job", zap.Error(err), zap.Int64("tableID", job.tbl.ID), zap.String("jobID", job.id))
	}
}

// AllSpawned returns whether all scan tasks have been dumped out
// **This function will be called concurrently, in many workers' goroutine**
func (job *ttlJob) AllSpawned() bool {
	return job.taskIter == len(job.tasks) && len(job.tasks) != 0
}

// Timeout will return whether the job has timeout, if it is, it will be killed
func (job *ttlJob) Timeout(ctx context.Context, se session.Session, now time.Time) bool {
	if !job.createTime.Add(ttlJobTimeout).Before(now) {
		return false
	}

	err := job.changeStatus(ctx, se, cache.JobStatusTimeout)
	if err != nil {
		logutil.BgLogger().Info("fail to update status of ttl job", zap.String("jobID", job.id), zap.Error(err))
	}

	return true
}

// Finished returns whether the job is finished
func (job *ttlJob) Finished() bool {
	job.statusMutex.Lock()
	defer job.statusMutex.Unlock()
	// in three condition, a job is considered finished:
	// 1. It's cancelled manually
	// 2. All scan tasks have been finished, and all selected rows succeed or in error state
	// 3. The job is created one hour ago. It's a timeout.
	return job.status == cache.JobStatusCancelled || (job.AllSpawned() && job.finishedScanTaskCounter == len(job.tasks) && job.statistics.TotalRows.Load() == job.statistics.ErrorRows.Load()+job.statistics.SuccessRows.Load())
}

// Cancel cancels the job context
func (job *ttlJob) Cancel(ctx context.Context, se session.Session) error {
	if job.cancel != nil {
		job.cancel()
	}
	// TODO: wait until all tasks have been finished
	return job.changeStatus(ctx, se, cache.JobStatusCancelled)
}

func findJobWithTableID(jobs []*ttlJob, id int64) *ttlJob {
	for _, j := range jobs {
		if j.tbl.ID == id {
			return j
		}
	}

	return nil
}
