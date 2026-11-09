// Copyright 2026 PingCAP, Inc.
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

//! Go `pkg/ttl/ttlworker/job.go`.

use tidb_datatype::Datum;
use tidb_timer::go_time::GoTime;
use tidb_ttl::cache::table::PhysicalTable;
use tidb_ttl::cache::ttlstatus::JobStatus;
use tidb_ttl::session::{SessionError, TxnMode};

use crate::job_manager::TtlSummary;
use crate::{str_arg, wrap_execute_sql, Result, Session};

/// Go `finishJobTemplate`.
pub const FINISH_JOB_TEMPLATE: &str = "UPDATE mysql.tidb_ttl_table_status
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
	WHERE table_id = %? AND current_job_id = %?";

/// Go `removeTaskForJobTemplate`.
pub const REMOVE_TASK_FOR_JOB_TEMPLATE: &str = "DELETE FROM mysql.tidb_ttl_task WHERE job_id = %?";

/// Go `createJobHistoryRowTemplate`.
pub const CREATE_JOB_HISTORY_ROW_TEMPLATE: &str = "INSERT INTO
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
        status
    )
VALUES
    (%?, %?, %?, %?, %?, %?, %?, FROM_UNIXTIME(1), %?, %?)";

/// Go `finishJobHistoryTemplate`.
pub const FINISH_JOB_HISTORY_TEMPLATE: &str = "UPDATE mysql.tidb_ttl_job_history
	SET finish_time = %?,
	    summary_text = %?,
	    expired_rows = %?,
	    deleted_rows = %?,
	    error_delete_rows = %?,
	    status = %?
	WHERE job_id = %?";

/// Go `finishJobSQL`.
pub fn finish_job_sql(table_id: i64, finish_time: &GoTime, summary: &str, job_id: &str) -> (&'static str, Vec<Datum>) {
    (
        FINISH_JOB_TEMPLATE,
        vec![
            str_arg(&finish_time.format_date_time()),
            str_arg(summary),
            Datum::new_int(table_id),
            str_arg(job_id),
        ],
    )
}

/// Go `removeTaskForJob`.
pub fn remove_task_for_job(job_id: &str) -> (&'static str, Vec<Datum>) {
    (REMOVE_TASK_FOR_JOB_TEMPLATE, vec![str_arg(job_id)])
}

/// Go `createJobHistorySQL`.
pub fn create_job_history_sql(
    job_id: &str,
    tbl: &PhysicalTable,
    expire: &GoTime,
    now: &GoTime,
) -> (&'static str, Vec<Datum>) {
    let partition_name = if tbl.partition.original().is_empty() {
        Datum::Null
    } else {
        str_arg(tbl.partition.original())
    };
    (
        CREATE_JOB_HISTORY_ROW_TEMPLATE,
        vec![
            str_arg(job_id),
            Datum::new_int(tbl.id),
            Datum::new_int(tbl.table_info.read().id),
            str_arg(tbl.schema.original()),
            str_arg(&tbl.name_original()),
            partition_name,
            str_arg(&now.format_date_time()),
            str_arg(&expire.format_date_time()),
            str_arg(JobStatus::RUNNING),
        ],
    )
}

/// Go `finishJobHistorySQL`.
pub fn finish_job_history_sql(job_id: &str, finish_time: &GoTime, summary: &TtlSummary) -> (&'static str, Vec<Datum>) {
    (
        FINISH_JOB_HISTORY_TEMPLATE,
        vec![
            str_arg(&finish_time.format_date_time()),
            str_arg(&summary.summary_text),
            Datum::UInt(summary.total_rows),
            Datum::UInt(summary.success_rows),
            Datum::UInt(summary.error_rows),
            str_arg(JobStatus::FINISHED),
            str_arg(job_id),
        ],
    )
}

/// Go `ttlJob`.
#[derive(Debug, Clone)]
pub struct TtlJob {
    /// Go `id`.
    pub id: String,
    /// Go `ownerID`.
    pub owner_id: String,
    /// Go `createTime`.
    pub create_time: GoTime,
    /// Go `ttlExpireTime`.
    pub ttl_expire_time: GoTime,
    /// Go `assignTime`: when the job was assigned to the current manager;
    /// later than `create_time` when the job was reassigned.
    pub assign_time: GoTime,
    /// Go `tableID`.
    pub table_id: i64,
    /// Go `status`.
    pub status: JobStatus,
}

impl TtlJob {
    /// Go `finish`: turns the current job into the last job, and records its
    /// summary.
    pub fn finish(&self, se: &Session, now: &GoTime, summary: &TtlSummary) -> Result<()> {
        debug_assert_eq!(se.get_session_vars().location(), *now.location());
        // at this time, the job.ctx may have been canceled (to cancel this job)
        // even when it's canceled, we'll need to update the states, so use another context
        se.run_in_txn(&tidb_gocontext::Context::background(), 
            &mut || {
                let execute = |(sql, args): (&str, Vec<Datum>)| {
                    se.execute_sql(&tidb_gocontext::Context::background(), sql, &args)
                        .map(drop)
                        .map_err(|err| SessionError(wrap_execute_sql(err, sql)))
                };
                execute(finish_job_sql(self.table_id, now, &summary.summary_text, &self.id))?;
                execute(remove_task_for_job(&self.id))?;
                execute(finish_job_history_sql(&self.id, now, summary))
            },
            TxnMode::PESSIMISTIC,
            None,
        )
        .map_err(|err| err.to_string())
    }
}
