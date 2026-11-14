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

//! Go `ddl_history.go`: the cluster's running and historical DDL jobs.

/// The cluster's DDL jobs, Go's `mysql.tidb_ddl_job` and meta
/// `DDLJobHistory`, where a front end keeps them in a shared store rather
/// than in its own catalog.
pub trait DdlJobHistoryStore: Send + Sync {
    /// Go `IterAllDDLJobs`' order: the running jobs (`GetAllDDLJobs`, by job
    /// ID), then the meta history newest first
    /// (`GetLastHistoryDDLJobsIterator`).
    fn all_ddl_jobs(&self) -> Result<(Vec<tidb_model::Job>, Vec<tidb_model::Job>), String>;

    /// Go `ScanHistoryDDLJobs` over a fresh transaction's meta: the history
    /// newest first -- from `start_job_id` down when it is set -- up to
    /// `limit` jobs.
    fn scan_history_ddl_jobs(&self, start_job_id: i64, limit: i32) -> Result<Vec<tidb_model::Job>, String>;

    /// Go `GetDDLInfoWithNewTxn`: in a new transaction, the first general
    /// and the first reorganization job of `mysql.tidb_ddl_job`
    /// (`get2JobsFromTable`) and the schema version with a non-empty diff.
    fn get_ddl_info(&self) -> Result<DdlInfo, String>;
}

/// Go `get2JobsFromTable` over `mysql.tidb_ddl_job`'s rows in job order:
/// the first job that needs no reorganization (`not reorg order by job_id
/// limit 1`), then the first that may (`reorg order by job_id limit 1`).
#[must_use]
pub fn get_2_jobs_from_table(jobs: &[tidb_model::Job]) -> Vec<tidb_model::Job> {
    let general = jobs.iter().find(|job| !job.may_need_reorg());
    let reorg = jobs.iter().find(|job| job.may_need_reorg());
    general.into_iter().chain(reorg).cloned().collect()
}

/// Go `getJobsBySQL`: the jobs of `mysql.tidb_ddl_job` matching
/// `condition`, selected on `se`.
///
/// # Errors
///
/// The statement's error, or a job that does not decode.
pub fn get_jobs_by_sql(
    se: &tidb_ddl_systable::DdlSession,
    condition: &str,
) -> Result<Vec<tidb_model::Job>, String> {
    let rows = se
        .execute(
            &tidb_sqlexec::BackgroundContext,
            &format!("select job_meta from mysql.tidb_ddl_job where {condition}"),
            "get_job",
            &[],
        )
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    rows.iter()
        .map(|row| {
            let mut job = tidb_model::Job::default();
            job.decode(row[0].go_bytes())
                .map_err(|error| error.to_string())?;
            Ok(job)
        })
        .collect()
}

/// Runs `read` on a session of the DDL's pool (Go `sess.NewSession` over a
/// pooled context). An unavailable pool is an error, as with Go Pool.Get.
pub fn with_pool_session<T>(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    read: impl FnOnce(&tidb_ddl_systable::DdlSession) -> Result<T, String>,
) -> Result<T, String> {
    let pool = sess_pool.sess_pool().ok_or_else(|| "the DDL session pool has no sessions".to_owned())?;
    pool.get().map_err(|error| error.to_string()).and_then(|sess_ctx| {
        let result = read(&tidb_ddl_session::Session::new(std::sync::Arc::clone(&sess_ctx)));
        pool.put(sess_ctx);
        result
    })
}

/// Go `ddl.Info`, as `ADMIN SHOW DDL` reads it.
#[derive(Debug, Default)]
pub struct DdlInfo {
    /// Go `Info.SchemaVer`.
    pub schema_ver: i64,
    /// Go `Info.Jobs`: the general job, then the reorganization job.
    pub jobs: Vec<tidb_model::Job>,
}

/// Go `DefNumGetDDLHistoryJobs`.
pub const DEF_NUM_GET_DDL_HISTORY_JOBS: i32 = 2048;

/// Go `ScanHistoryDDLJobs`: without a start, the last `limit` (default
/// [`DEF_NUM_GET_DDL_HISTORY_JOBS`]) history jobs; with one, a limit is
/// required and the jobs run down from `start_job_id`.
pub fn scan_history_ddl_jobs<T: tidb_meta::transaction::MetaTransaction>(
    m: &tidb_meta::transaction::Mutator<T>,
    start_job_id: i64,
    mut limit: i32,
) -> Result<Vec<tidb_model::Job>, String> {
    let mut iterator = if start_job_id == 0 {
        if limit == 0 {
            limit = DEF_NUM_GET_DDL_HISTORY_JOBS;
        }
        m.get_last_history_ddl_jobs_iterator::<tidb_model::Job>()
    } else {
        if limit == 0 {
            return Err("when 'start_job_id' is specified, it must work with a 'limit'".to_owned());
        }
        m.get_history_ddl_jobs_iterator::<tidb_model::Job>(start_job_id)
    }
    .map_err(|error| error.to_string())?;
    let mut jobs = Vec::new();
    iterator.get_last_jobs(limit, &mut jobs).map_err(|error| error.to_string())?;
    Ok(jobs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `get2JobsFromTable`: the first general job and the first job that
    /// may reorganize, each the lowest job ID of its kind, general first.
    #[test]
    fn get_2_jobs_from_table_takes_the_first_of_each_kind() {
        use tidb_model::ActionType;
        let job = |id, type_| {
            let mut job = tidb_model::Job::default();
            job.id = id;
            job.type_ = type_;
            job
        };
        let jobs = [
            job(3, ActionType::ACTION_ADD_INDEX),
            job(4, ActionType::ACTION_CREATE_TABLE),
            job(5, ActionType::ACTION_ADD_PRIMARY_KEY),
            job(6, ActionType::ACTION_DROP_TABLE),
        ];
        let ids: Vec<i64> = get_2_jobs_from_table(&jobs).iter().map(|job| job.id).collect();
        assert_eq!(ids, [4, 3]);
        let ids: Vec<i64> = get_2_jobs_from_table(&jobs[1..2]).iter().map(|job| job.id).collect();
        assert_eq!(ids, [4]);
        assert!(get_2_jobs_from_table(&[]).is_empty());
    }

    /// Go `ScanHistoryDDLJobs`: without a start, the newest `limit` jobs
    /// (2048 by default); from a start, a limit is required and the scan
    /// runs down from it inclusively.
    #[test]
    fn scan_history_ddl_jobs_follows_gos_start_and_limit_rules() {
        let meta = tidb_meta::transaction::Mutator::new(tidb_meta::transaction::MemoryTransaction::default());
        for id in 1..=5 {
            let mut job = tidb_model::Job::default();
            job.id = id;
            meta.add_history_ddl_job(&mut job, false).unwrap();
        }
        let ids = |jobs: Vec<tidb_model::Job>| jobs.into_iter().map(|job| job.id).collect::<Vec<_>>();
        assert_eq!(ids(scan_history_ddl_jobs(&meta, 0, 0).unwrap()), [5, 4, 3, 2, 1]);
        assert_eq!(ids(scan_history_ddl_jobs(&meta, 0, 2).unwrap()), [5, 4]);
        assert_eq!(ids(scan_history_ddl_jobs(&meta, 3, 2).unwrap()), [3, 2]);
        assert_eq!(
            scan_history_ddl_jobs(&meta, 3, 0).unwrap_err(),
            "when 'start_job_id' is specified, it must work with a 'limit'"
        );
    }
}
