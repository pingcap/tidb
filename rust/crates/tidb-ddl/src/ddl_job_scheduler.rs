// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pinned Go `pkg/ddl` scheduler support consumed by the generated mock
//! package and the owner-side scheduler.

use std::collections::HashSet;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::RwLock;
use std::time::Duration;

const JOB_RECORD_CAPACITY: usize = 16;

/// Go `getJobsBySQL`: the jobs of `mysql.tidb_ddl_job` matching
/// `condition`, selected on `se`.
///
/// # Errors
///
/// The statement's error, or a job that does not decode.
pub fn get_jobs_by_sql(
    se: &tidb_ddl_session::DdlSession,
    condition: &str,
) -> Result<Vec<tidb_model::Job>, tidb_terror::MysqlError> {
    let rows = tidb_ddl_build::session_pool::execute(
        se,
        &format!("select job_meta from mysql.tidb_ddl_job where {condition}"),
        "get_job",
        &[],
    )?;
    rows.iter()
        .map(|row| {
            let mut job = tidb_model::Job::default();
            let bytes = row.first().map_or(&[][..], tidb_datatype::Datum::go_bytes);
            job.decode(bytes)
                .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
            Ok(job)
        })
        .collect()
}

/// Go `updateDDLJob2Table`: rewrites the job's row of `mysql.tidb_ddl_job`
/// through `se`.
///
/// # Errors
///
/// The job's encoding or the statement's error.
pub fn update_ddl_job_2_table(
    se: &tidb_ddl_session::DdlSession,
    job: &mut tidb_model::Job,
    update_raw_args: bool,
) -> Result<(), tidb_terror::MysqlError> {
    let encoded = job
        .encode(update_raw_args)
        .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
    let sql = format!(
        "update mysql.tidb_ddl_job set job_meta = {} where job_id = {}",
        crate::cluster_ddl::wrap_key_2_string(&encoded),
        job.id
    );
    tidb_ddl_build::session_pool::execute(se, &sql, "update_job", &[])?;
    Ok(())
}
const JOB_ONCE_CAPACITY: usize = 1_000;

/// Go `SchemaLoader`.
pub trait SchemaLoader: Send + Sync {
    /// Reloads the information schema.
    fn reload(&self) -> Result<(), String>;
}

/// Go `jobScheduler.mustReloadSchemas`.
pub fn must_reload_schemas(
    loader: &dyn SchemaLoader,
    cancelled: &Receiver<()>,
    retry_interval: Duration,
) {
    loop {
        match loader.reload() {
            Ok(()) => return,
            Err(error) => tidb_ddl_logutil::ddl_logger().warn(
                "reload schema failed, will retry later",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
            ),
        }
        match cancelled.recv_timeout(retry_interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

#[derive(Default)]
struct TrackerState {
    unsynced_jobs: HashSet<i64>,
    once: HashSet<i64>,
}

/// Go `unSyncedJobTracker`.
#[derive(Default)]
pub struct UnSyncedJobTracker {
    state: RwLock<TrackerState>,
}

impl UnSyncedJobTracker {
    /// Go `newUnSyncedJobTracker`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(TrackerState {
                unsynced_jobs: HashSet::with_capacity(JOB_RECORD_CAPACITY),
                once: HashSet::with_capacity(JOB_ONCE_CAPACITY),
            }),
        }
    }

    /// Go `addUnSynced`.
    pub fn add_unsynced(&self, job_id: i64) {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unsynced_jobs
            .insert(job_id);
    }

    /// Go `isUnSynced`.
    #[must_use]
    pub fn is_unsynced(&self, job_id: i64) -> bool {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unsynced_jobs
            .contains(&job_id)
    }

    /// Go `removeUnSynced`.
    pub fn remove_unsynced(&self, job_id: i64) {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unsynced_jobs
            .remove(&job_id);
    }

    /// Go `maybeAlreadyRunOnce`.
    #[must_use]
    pub fn maybe_already_run_once(&self, job_id: i64) -> bool {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .once
            .contains(&job_id)
    }

    /// Go `setAlreadyRunOnce`, including its strictly-greater-than capacity
    /// reset condition.
    pub fn set_already_run_once(&self, job_id: i64) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.once.len() > JOB_ONCE_CAPACITY {
            state.once = HashSet::with_capacity(JOB_RECORD_CAPACITY);
        }
        state.once.insert(job_id);
    }
}
