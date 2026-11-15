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

//! Go `pkg/executor/operate_ddl_jobs.go`: `AlterDDLJobExec`, which rewrites
//! a queued or running job's reorg configuration in `mysql.tidb_ddl_job`.

use tidb_ddl_build::session_pool::StepSqlSession;
use tidb_model::{AdminCommandOperator, Job};
use tidb_planner::common_plans::{
    AlterDdlJobOpt, ALTER_DDL_JOB_BATCH_SIZE, ALTER_DDL_JOB_MAX_WRITE_SPEED, ALTER_DDL_JOB_THREAD,
};
use tidb_terror::MysqlError;

/// Go `alterDDLJobMaxRetryCnt`.
const ALTER_DDL_JOB_MAX_RETRY_CNT: usize = 3;

/// Go `getJobMetaFromTable`'s answer for a job `mysql.tidb_ddl_job` does
/// not hold.
#[must_use]
pub fn ddl_job_not_running(job_id: i64) -> MysqlError {
    MysqlError::unknown(format!("ddl job {job_id} is not running"))
}

/// Go `getJobMetaFromTable`.
fn get_job_meta_from_table(se: &mut dyn StepSqlSession, job_id: i64) -> Result<Job, MysqlError> {
    let sql = format!("select job_meta from mysql.tidb_ddl_job where job_id = {job_id}");
    let rows = se.execute(&sql, &[])?;
    let Some(row) = rows.first() else {
        return Err(ddl_job_not_running(job_id));
    };
    let mut job = Job::default();
    job.decode(row.first().map_or(&[][..], tidb_datatype::Datum::go_bytes))
        .map_err(|error| MysqlError::unknown(error.to_string()))?;
    Ok(job)
}

/// Go `updateJobMeta2Table`.
fn update_job_meta_2_table(se: &mut dyn StepSqlSession, job: &mut Job) -> Result<(), MysqlError> {
    let b = job.encode(false).map_err(|error| MysqlError::unknown(error.to_string()))?;
    let sql = format!(
        "update mysql.tidb_ddl_job set job_meta = {} where job_id = {}",
        tidb_ddl_jobsubmit::wrap_key_2_string(&b),
        job.id
    );
    se.execute(&sql, &[])?;
    Ok(())
}

/// Go `AlterDDLJobExec.processAlterDDLJobConfig`: alter the job's configs in
/// one transaction of `ns`, retried up to [`ALTER_DDL_JOB_MAX_RETRY_CNT`]
/// times.
///
/// # Errors
///
/// The unsupported-job errors at once; otherwise the last attempt's error
/// (`ddl job N is not running` when the job is not in `mysql.tidb_ddl_job`).
pub fn process_alter_ddl_job_config(
    ns: &mut dyn StepSqlSession,
    job_id: i64,
    alter_opts: &[AlterDdlJobOpt],
) -> Result<(), MysqlError> {
    let mut err = None;
    for _ in 0..ALTER_DDL_JOB_MAX_RETRY_CNT {
        if let Err(error) = ns.begin() {
            err = Some(error);
            continue;
        }
        let mut job = match get_job_meta_from_table(ns, job_id) {
            Ok(job) => job,
            Err(error) => {
                err = Some(error);
                continue;
            }
        };
        if !job.is_alterable() {
            return Err(MysqlError::unknown(format!(
                "unsupported DDL operation: {}. Supported DDL operations are: ADD INDEX, MODIFY COLUMN, and ALTER TABLE REORGANIZE PARTITION",
                job.type_
            )));
        }
        if tidb_config_kerneltype::is_next_gen()
            && job.type_ == tidb_model::ActionType::ACTION_ADD_INDEX
        {
            return Err(MysqlError::from_terror(
                &tidb_variable::variable_errors::ERR_NOT_SUPPORTED_IN_NEXT_GEN
                    .gen_with_stack_by_args(&["Altering ADD INDEX job".into()]),
            ));
        }
        if let Err(error) = update_reorg_meta(alter_opts, &mut job, AdminCommandOperator::BY_END_USER) {
            err = Some(error);
            continue;
        }
        if let Err(error) = update_job_meta_2_table(ns, &mut job) {
            err = Some(error);
            continue;
        }
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/mockAlterDDLJobCommitFailed")
            .is_some_and(|val| val.bool())
        {
            ns.rollback();
            return Err(MysqlError::unknown("mock commit failed on admin alter ddl jobs"));
        }
        if let Err(error) = ns.commit() {
            ns.rollback();
            err = Some(error);
            continue;
        }
        return Ok(());
    }
    Err(err.expect("every attempt failed"))
}

/// Go `AlterDDLJobExec.updateReorgMeta`.
fn update_reorg_meta(
    alter_opts: &[AlterDdlJobOpt],
    job: &mut Job,
    by_who: AdminCommandOperator,
) -> Result<(), MysqlError> {
    for opt in alter_opts {
        let Some(value) = opt.value.as_ref() else {
            continue;
        };
        let tidb_expr::expression::Expression::Constant(cons) = value else {
            unreachable!("the planner rewrites every option to a constant");
        };
        match opt.name.as_str() {
            ALTER_DDL_JOB_THREAD => {
                reorg_meta(job).set_concurrency(cons.value.get_int64());
                job.admin_operator = by_who;
            }
            ALTER_DDL_JOB_BATCH_SIZE => {
                reorg_meta(job).set_batch_size(cons.value.get_int64());
                job.admin_operator = by_who;
            }
            ALTER_DDL_JOB_MAX_WRITE_SPEED => {
                let speed = tidb_planner::plan_builder::stmt_build::get_max_write_speed_from_expression(opt)
                    .map_err(MysqlError::unknown)?;
                reorg_meta(job).set_max_write_speed(speed);
                job.admin_operator = by_who;
            }
            name => {
                return Err(MysqlError::unknown(format!(
                    "unsupported admin alter ddl jobs config: {name}"
                )));
            }
        }
    }
    Ok(())
}

/// Go `job.ReorgMeta`, which every alterable job carries.
fn reorg_meta(job: &Job) -> std::sync::RwLockReadGuard<'_, tidb_model::DDLReorgMeta> {
    job.reorg_meta.as_ref().expect("an alterable job has reorg meta").read()
}
