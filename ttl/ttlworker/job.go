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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
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

func removeTaskForJob(jobID string) (string, []interface{}) {
	return removeTaskForJobTemplate, []interface{}{jobID}
}

func addJobHistorySQL(job *ttlJob, finishTime time.Time, summary *TTLSummary) (string, []interface{}) {
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
		summary.SummaryText,
		summary.TotalRows,
		summary.SuccessRows,
		summary.ErrorRows,
		string(status),
	}
}

type ttlJob struct {
	id      string
	ownerID string

	createTime    time.Time
	ttlExpireTime time.Time

	tbl *cache.PhysicalTable

	// status is the only field which should be protected by a mutex, as `Cancel` may be called at any time, and will
	// change the status
	statusMutex sync.Mutex
	status      cache.JobStatus
}

// finish turns current job into last job, and update the error message and statistics summary
func (job *ttlJob) finish(se session.Session, now time.Time, summary *TTLSummary) {
	// at this time, the job.ctx may have been canceled (to cancel this job)
	// even when it's canceled, we'll need to update the states, so use another context
	err := se.RunInTxn(context.TODO(), func() error {
		sql, args := finishJobSQL(job.tbl.ID, now, summary.SummaryText, job.id)
		_, err := se.ExecuteSQL(context.TODO(), sql, args...)
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
	}, session.TxnModeOptimistic)

	if err != nil {
		logutil.BgLogger().Error("fail to finish a ttl job", zap.Error(err), zap.Int64("tableID", job.tbl.ID), zap.String("jobID", job.id))
	}
}
