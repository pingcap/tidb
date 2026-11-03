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

//! Go `pkg/ddl/ddl.go`'s job commands: `processJobs` over
//! `cancelRunningJob`, `util.PauseRunningJob` and `resumePausedJob`, which
//! ADMIN CANCEL / PAUSE / RESUME DDL JOBS run (`CancelJobs`, `PauseJobs`,
//! `ResumeJobs`). The command rewrites each job's row in
//! `mysql.tidb_ddl_job`; the owner's next step of the job acts on the state.

use tidb_dbterror as dbterror;
use tidb_ddl_build::session_pool::StepSqlSession;
use tidb_model::{AdminCommandOperator, Job, JobState};
use tidb_terror::{MysqlError, TerrorError};

/// One job's command error (Go's `jobErrs[i]`), printed as Go's `%v`.
#[derive(Debug)]
pub enum JobCommandError {
    /// The command's refusal or Go's not-found error.
    Terror(TerrorError),
    /// The job row's update failed.
    Sql(MysqlError),
}

impl std::fmt::Display for JobCommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Terror(error) => error.fmt(formatter),
            Self::Sql(error) => error.fmt(formatter),
        }
    }
}

/// A job command (Go `func(*model.Job, model.AdminCommandOperator) error`).
pub type JobCommand = fn(&mut Job, AdminCommandOperator) -> Result<(), TerrorError>;

/// Go `cancelRunningJob`.
///
/// # Errors
///
/// `ErrCancelFinishedDDLJob` for a finished job, `ErrCannotCancelDDLJob`
/// for one past its rollbackable states.
pub fn cancel_running_job(job: &mut Job, by_who: AdminCommandOperator) -> Result<(), TerrorError> {
    // These states can't be cancelled.
    if job.is_done() || job.is_synced() {
        return Err(
            dbterror::ERR_CANCEL_FINISHED_DDL_JOB.generate_with_stack_by_args(&[job.id.into()])
        );
    }
    // A rolling-back job is already cleaning up after a cancel.
    if job.is_cancelled() || job.is_rollingback() || job.is_rollback_done() {
        return Ok(());
    }
    if !job.is_rollbackable() {
        return Err(
            dbterror::ERR_CANNOT_CANCEL_DDL_JOB.generate_with_stack_by_args(&[job.id.into()])
        );
    }
    job.state = JobState::CANCELLING;
    job.admin_operator = by_who;
    Ok(())
}

/// Go `util.PauseRunningJob`.
///
/// # Errors
///
/// `ErrPausedDDLJob` for a pausing or paused job, `ErrCannotPauseDDLJob`
/// for one that cannot pause in its state.
pub fn pause_running_job(job: &mut Job, by_who: AdminCommandOperator) -> Result<(), TerrorError> {
    if job.is_pausing() || job.is_paused() {
        return Err(dbterror::ERR_PAUSED_DDL_JOB.generate_with_stack_by_args(&[job.id.into()]));
    }
    if !job.is_pausable() {
        let message = format!(
            "state [{}] or schema state [{}]",
            job.state, job.schema_state
        );
        return Err(dbterror::ERR_CANNOT_PAUSE_DDL_JOB
            .generate_with_stack_by_args(&[job.id.into(), message.into()]));
    }
    job.state = JobState::PAUSING;
    job.admin_operator = by_who;
    Ok(())
}

/// Go `resumePausedJob`.
///
/// # Errors
///
/// `ErrCannotResumeDDLJob` for a job that is not paused, or was paused by
/// another operator.
pub fn resume_paused_job(job: &mut Job, by_who: AdminCommandOperator) -> Result<(), TerrorError> {
    if !job.is_resumable() {
        let message = format!(
            "job has not been paused, job state:{}, schema state:{}",
            job.state, job.schema_state
        );
        return Err(dbterror::ERR_CANNOT_RESUME_DDL_JOB
            .generate_with_stack_by_args(&[job.id.into(), message.into()]));
    }
    // Only the operator who paused the job resumes it, except a system
    // pause whose reason lets an end user recover.
    if job.admin_operator != by_who && !can_end_user_resume_system_paused_job(job, by_who) {
        let message = format!(
            "job has been paused by [{}], should not resumed by [{}]",
            job.admin_operator, by_who
        );
        return Err(dbterror::ERR_CANNOT_RESUME_DDL_JOB
            .generate_with_stack_by_args(&[job.id.into(), message.into()]));
    }
    let resume_from_kv_disk_full_by_end_user =
        by_who == AdminCommandOperator::BY_END_USER && job.is_paused_by_system_for_kv_disk_full();
    job.state = JobState::QUEUEING;
    job.clear_pause_reason();
    job.error = None;
    if resume_from_kv_disk_full_by_end_user {
        job.set_resume_reason(tidb_model::job::JOB_RESUME_REASON_KV_DISK_FULL);
    } else {
        job.clear_resume_reason();
    }
    Ok(())
}

/// Go `canEndUserResumeSystemPausedJob`.
fn can_end_user_resume_system_paused_job(job: &Job, by_who: AdminCommandOperator) -> bool {
    by_who == AdminCommandOperator::BY_END_USER && job.is_paused_by_system_for_kv_disk_full()
}

/// Go `processJobs`: `process` on every listed job and its row rewritten,
/// all in one transaction of `sess`, tried up to three times when the
/// commit conflicts. One entry per id, `None` where the command succeeded.
///
/// # Errors
///
/// The transaction's begin or read error, or the last commit's.
pub fn process_jobs(
    process: JobCommand,
    sess: &mut dyn StepSqlSession,
    ids: &[i64],
    by_who: AdminCommandOperator,
) -> Result<Vec<Option<JobCommandError>>, MysqlError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut last_error = None;
    for _ in 0..3 {
        let mut job_errs: Vec<Option<JobCommandError>> = ids.iter().map(|_| None).collect();
        // Which ids are still to be found.
        let mut job_map: std::collections::HashMap<i64, usize> =
            ids.iter().enumerate().map(|(idx, id)| (*id, idx)).collect();
        let ids_str: Vec<String> = ids.iter().map(i64::to_string).collect();

        // Go `ns.Begin`: `sessiontxn.NewTxn` enters an optimistic
        // transaction.
        sess.begin()?;
        let jobs = match get_jobs_by_sql(
            sess,
            &format!("job_id in ({}) order by job_id", ids_str.join(", ")),
        ) {
            Ok(jobs) => jobs,
            Err(error) => {
                sess.rollback();
                return Err(error);
            }
        };
        for mut job in jobs {
            let Some(i) = job_map.remove(&job.id) else {
                // Go reads index 0 of a missing map entry.
                job_errs[0] = Some(JobCommandError::Terror(
                    dbterror::ERR_INVALID_DDL_JOB.generate_with_stack_by_args(&[job.id.into()]),
                ));
                continue;
            };
            if let Err(error) = process(&mut job, by_who) {
                job_errs[i] = Some(JobCommandError::Terror(error));
                continue;
            }
            if let Err(error) = update_ddl_job_2_table(sess, &mut job, false) {
                job_errs[i] = Some(JobCommandError::Sql(error));
            }
        }
        // A conflicting update retries the whole command.
        if let Err(error) = sess.commit() {
            sess.rollback();
            last_error = Some(error);
            continue;
        }
        for (id, idx) in job_map {
            job_errs[idx] = Some(JobCommandError::Terror(
                dbterror::ERR_DDL_JOB_NOT_FOUND.generate_with_stack_by_args(&[id.into()]),
            ));
        }
        return Ok(job_errs);
    }
    Err(last_error.expect("three failed attempts"))
}

/// Go `PauseJobsBySystem` for one job, its error included (Go's
/// `errs[0]` or `err`).
///
/// # Errors
///
/// The job's command error or the transaction's.
pub fn pause_job_by_system(
    sess: &mut dyn StepSqlSession,
    job_id: i64,
) -> Result<(), JobCommandError> {
    one_job(process_jobs(pause_running_job, sess, &[job_id], AdminCommandOperator::BY_SYSTEM))
}

/// Go `ResumeJobsBySystem` for one job.
///
/// # Errors
///
/// The job's command error or the transaction's.
pub fn resume_job_by_system(
    sess: &mut dyn StepSqlSession,
    job_id: i64,
) -> Result<(), JobCommandError> {
    one_job(process_jobs(resume_paused_job, sess, &[job_id], AdminCommandOperator::BY_SYSTEM))
}

fn one_job(
    result: Result<Vec<Option<JobCommandError>>, MysqlError>,
) -> Result<(), JobCommandError> {
    match result {
        Ok(mut errs) => match errs.pop().flatten() {
            Some(error) => Err(error),
            None => Ok(()),
        },
        Err(error) => Err(JobCommandError::Sql(error)),
    }
}

/// Go `util.HasSysDB`: whether the job involves a system-related database.
#[must_use]
pub fn has_sys_db(job: &Job) -> bool {
    job.get_involving_schema_info()
        .snapshot()
        .iter()
        .any(|info| tidb_metadef::db::is_system_related_db(&info.database.to_utf8_lossy_go()))
}

/// Go `getJobsBySQL`: the jobs of `mysql.tidb_ddl_job` matching
/// `condition`.
fn get_jobs_by_sql(sess: &mut dyn StepSqlSession, condition: &str) -> Result<Vec<Job>, MysqlError> {
    let rows = sess.execute(
        &format!("select job_meta from mysql.tidb_ddl_job where {condition}"),
        &[],
    )?;
    rows.into_iter()
        .map(|row| {
            let mut job = Job::default();
            let bytes = row.first().map_or(&[][..], tidb_datatype::Datum::go_bytes);
            job.decode(bytes)
                .map_err(|error| MysqlError::unknown(error.to_string()))?;
            Ok(job)
        })
        .collect()
}

/// Go `updateDDLJob2Table` on `sess`.
fn update_ddl_job_2_table(
    sess: &mut dyn StepSqlSession,
    job: &mut Job,
    update_raw_args: bool,
) -> Result<(), MysqlError> {
    let encoded = job
        .encode(update_raw_args)
        .map_err(|error| MysqlError::unknown(error.to_string()))?;
    sess.execute(
        &format!(
            "update mysql.tidb_ddl_job set job_meta = {} where job_id = {}",
            crate::cluster_ddl::wrap_key_2_string(&encoded),
            job.id
        ),
        &[],
    )?;
    Ok(())
}
