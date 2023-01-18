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
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const updateJobCurrentStatusTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_status = %? WHERE table_id = %? AND current_job_status = %? AND current_job_id = %?"
const finishJobTemplate = `UPDATE mysql.tidb_ttl_table_status
	SET last_job_id = current_job_id,
		last_job_start_time = current_job_start_time,
		last_job_finish_time = %?,
		last_job_ttl_expire = current_job_ttl_expire,
		last_job_summary = %?,
		current_job_id = NULL,
		current_job_owner_id = NULL,
		current_job_owner_hb_time = NULL,
		current_job_start_time = NULL,
		current_job_ttl_expire = NULL,
		current_job_state = NULL,
		current_job_status = NULL,
		current_job_status_update_time = NULL
	WHERE table_id = %? AND current_job_id = %?`
const updateJobStateTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_state = %? WHERE table_id = %? AND current_job_id = %? AND current_job_owner_id = %?"
const removeTaskForJobTemplate = "DELETE FROM mysql.tidb_ttl_task WHERE job_id = %?"
const addJobHistoryTemplate = `INSERT INTO
    mysql.tidb_ttl_job_history (
        job_id,
        table_id,
        parent_table_id,
        table_schema,
        table_name,
        partition_name,
        create_time,
        finish_time,
        ttl_expire,
        summary_text,
        expired_rows,
        deleted_rows,
        error_delete_rows,
        status
    )
VALUES
    (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`

func updateJobCurrentStatusSQL(tableID int64, oldStatus cache.JobStatus, newStatus cache.JobStatus, jobID string) (string, []interface{}) {
	return updateJobCurrentStatusTemplate, []interface{}{string(newStatus), tableID, string(oldStatus), jobID}
}

func finishJobSQL(tableID int64, finishTime time.Time, summary string, jobID string) (string, []interface{}) {
	return finishJobTemplate, []interface{}{finishTime.Format(timeFormat), summary, tableID, jobID}
}

func updateJobState(tableID int64, currentJobID string, currentJobState string, currentJobOwnerID string) (string, []interface{}) {
	return updateJobStateTemplate, []interface{}{currentJobState, tableID, currentJobID, currentJobOwnerID}
}

func removeTaskForJob(jobID string) (string, []interface{}) {
	return removeTaskForJobTemplate, []interface{}{jobID}
}

func addJobHistorySQL(job *ttlJob, finishTime time.Time, summaryText string) (string, []interface{}) {
	status := cache.JobStatusFinished
	if job.status == cache.JobStatusTimeout || job.status == cache.JobStatusCancelled {
		status = job.status
	}

	var partitionName interface{}
	if job.tbl.Partition.O != "" {
		partitionName = job.tbl.Partition.O
	}

	return addJobHistoryTemplate, []interface{}{
		job.id,
		job.tbl.ID,
		job.tbl.TableInfo.ID,
		job.tbl.Schema.O,
		job.tbl.Name.O,
		partitionName,
		job.createTime.Format(timeFormat),
		finishTime.Format(timeFormat),
		job.ttlExpireTime.Format(timeFormat),
		summaryText,
		job.statistics.TotalRows.Load(),
		job.statistics.SuccessRows.Load(),
		job.statistics.ErrorRows.Load(),
		string(status),
	}
}

type ttlJob struct {
	id      string
	ownerID string

	ctx    context.Context
	cancel func()

	createTime    time.Time
	ttlExpireTime time.Time

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

	sql, args := updateJobCurrentStatusSQL(job.tbl.ID, oldStatus, status, job.id)
	_, err := se.ExecuteSQL(ctx, sql, args...)
	if err != nil {
		return errors.Wrapf(err, "execute sql: %s", sql)
	}

	return nil
}

func (job *ttlJob) updateState(ctx context.Context, se session.Session) error {
	summary, err := job.summary()
	if err != nil {
		logutil.Logger(job.ctx).Warn("fail to generate summary for ttl job", zap.Error(err))
	}
	sql, args := updateJobState(job.tbl.ID, job.id, summary, job.ownerID)
	_, err = se.ExecuteSQL(ctx, sql, args...)
	if err != nil {
		return errors.Wrapf(err, "execute sql: %s", sql)
	}

	return nil
}

// peekScanTask returns the next scan task, but doesn't promote the iterator
func (job *ttlJob) peekScanTask() *ttlScanTask {
	return job.tasks[job.taskIter]
}

// nextScanTask promotes the iterator
func (job *ttlJob) nextScanTask() {
	job.taskIter += 1
}

// finish turns current job into last job, and update the error message and statistics summary
func (job *ttlJob) finish(se session.Session, now time.Time) {
	summary, err := job.summary()
	if err != nil {
		logutil.Logger(job.ctx).Warn("fail to generate summary for ttl job", zap.Error(err))
	}

	// at this time, the job.ctx may have been canceled (to cancel this job)
	// even when it's canceled, we'll need to update the states, so use another context
	err = se.RunInTxn(context.TODO(), func() error {
		sql, args := finishJobSQL(job.tbl.ID, now, summary, job.id)
		_, err = se.ExecuteSQL(context.TODO(), sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		sql, args = removeTaskForJob(job.id)
		_, err = se.ExecuteSQL(context.TODO(), sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		sql, args = addJobHistorySQL(job, now, summary)
		_, err = se.ExecuteSQL(context.TODO(), sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		return nil
	})

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

type ttlSummary struct {
	TotalRows   uint64 `json:"total_rows"`
	SuccessRows uint64 `json:"success_rows"`
	ErrorRows   uint64 `json:"error_rows"`

	TotalScanTask     int `json:"total_scan_task"`
	ScheduledScanTask int `json:"scheduled_scan_task"`
	FinishedScanTask  int `json:"finished_scan_task"`

	ScanTaskErr string `json:"scan_task_err,omitempty"`
}

func (job *ttlJob) summary() (string, error) {
	summary := &ttlSummary{
		TotalRows:   job.statistics.TotalRows.Load(),
		SuccessRows: job.statistics.SuccessRows.Load(),
		ErrorRows:   job.statistics.ErrorRows.Load(),

		TotalScanTask:     len(job.tasks),
		ScheduledScanTask: job.taskIter,
		FinishedScanTask:  job.finishedScanTaskCounter,
	}

	if job.scanTaskErr != nil {
		summary.ScanTaskErr = job.scanTaskErr.Error()
	}

	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return "", err
	}

	return string(hack.String(summaryJSON)), nil
}
