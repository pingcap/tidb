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
