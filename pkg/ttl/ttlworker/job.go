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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/intest"
)

const finishJobTemplateBody = `UPDATE %s
	SET last_job_id = current_job_id,
		last_job_start_time = current_job_start_time,
		last_job_finish_time = %%?,
		%s
		last_job_summary = %%?,
		current_job_id = NULL,
		current_job_owner_id = NULL,
		current_job_owner_addr = NULL,
		current_job_owner_hb_time = NULL,
		current_job_start_time = NULL,
		current_job_ttl_expire = NULL,
		current_job_state = NULL,
		current_job_status = NULL,
		current_job_status_update_time = NULL
	WHERE table_id = %%? AND current_job_id = %%?`

var (
	finishJobTemplateSoftdelete = fmt.Sprintf(finishJobTemplateBody, "mysql.tidb_softdelete_table_status", "")
	finishJobTemplateTTL        = fmt.Sprintf(finishJobTemplateBody, "mysql.tidb_ttl_table_status", "last_job_ttl_expire = current_job_ttl_expire,")
)

const removeTaskForJobTemplate = "DELETE FROM mysql.tidb_ttl_task WHERE job_id = %?"
const createJobHistoryRowTemplate = `INSERT INTO
    mysql.tidb_ttl_job_history (
        job_id,
        job_type,
        table_id,
        parent_table_id,
        table_schema,
        table_name,
        partition_name,
        create_time,
        finish_time,
        ttl_expire,
        status
    )
VALUES
    (%?, %?, %?, %?, %?, %?, %?, %?, FROM_UNIXTIME(1), %?, %?)`
const finishJobHistoryTemplate = `UPDATE mysql.tidb_ttl_job_history
	SET finish_time = %?,
	    summary_text = %?,
	    expired_rows = %?,
	    deleted_rows = %?,
	    error_delete_rows = %?,
	    status = %?
	WHERE job_id = %?`

func finishJobSQL(jobType session.TTLJobType, tableID int64, finishTime time.Time, summary string, jobID string) (string, []any) {
	sql := finishJobTemplateTTL
	if jobType == session.TTLJobTypeSoftDelete {
		sql = finishJobTemplateSoftdelete
	}
	return sql, []any{finishTime.Format(timeFormat), summary, tableID, jobID}
}

func removeTaskForJob(jobID string) (string, []any) {
	return removeTaskForJobTemplate, []any{jobID}
}

func createJobHistorySQL(jobID string, jobType session.TTLJobType, tbl *cache.PhysicalTable, expire time.Time, now time.Time) (string, []any) {
	var partitionName any
	if tbl.Partition.O != "" {
		partitionName = tbl.Partition.O
	}

	return createJobHistoryRowTemplate, []any{
		jobID,
		jobType,
		tbl.ID,
		tbl.TableInfo.ID,
		tbl.Schema.O,
		tbl.Name.O,
		partitionName,
		now.Format(timeFormat),
		expire.Format(timeFormat),
		string(cache.JobStatusRunning),
	}
}

func finishJobHistorySQL(jobID string, finishTime time.Time, summary *TTLSummary) (string, []any) {
	return finishJobHistoryTemplate, []any{
		finishTime.Format(timeFormat),
		summary.SummaryText,
		summary.TotalRows,
		summary.SuccessRows,
		summary.ErrorRows,
		string(cache.JobStatusFinished),
		jobID,
	}
}

type ttlJob struct {
	id      string
	jobType session.TTLJobType
	ownerID string

	createTime    time.Time
	ttlExpireTime time.Time

	// assignTime is the time when the job is assigned to the current manager.
	// The `assignTime` may be greater than `createTime` if the job is reassigned to another manager.
	assignTime time.Time

	tableID int64

	// status is the only field which should be protected by a mutex, as `Cancel` may be called at any time, and will
	// change the status
	status cache.JobStatus
}

// finish turns current job into last job, and update the error message and statistics summary
func (job *ttlJob) finish(se session.Session, now time.Time, summary *TTLSummary) error {
	intest.Assert(se.GetSessionVars().Location().String() == now.Location().String())

	// at this time, the job.ctx may have been canceled (to cancel this job)
	// even when it's canceled, we'll need to update the states, so use another context
	err := se.RunInTxn(context.TODO(), func() error {
		sql, args := finishJobSQL(job.jobType, job.tableID, now, summary.SummaryText, job.id)
		_, err := se.ExecuteSQL(context.TODO(), sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		sql, args = removeTaskForJob(job.id)
		_, err = se.ExecuteSQL(context.TODO(), sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		sql, args = finishJobHistorySQL(job.id, now, summary)
		_, err = se.ExecuteSQL(context.TODO(), sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		failpoint.InjectCall("ttl-finish", &err)
		return err
	}, session.TxnModePessimistic)

	return err
}
