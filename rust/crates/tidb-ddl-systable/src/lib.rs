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

//! Go `pkg/ddl/systable`: reads of the active DDL and MDL system tables on
//! the DDL's session pool, plus the monotonic minimum-job-ID refresher.

pub mod ddl_job_table;

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::Datum;
use tidb_model::{GoShared, Job, JobW};
use tidb_goruntime::go_runtime::GoSharedSlice;

use tidb_infoschema::cluster_catalog::MetaSnapshot;
use tidb_infoschema::SessionExtendedInfoSchema;
use crate::ddl_job_table::{DdlJobTable, DdlJobTableError};
use tidb_syssession::mysql_system_tables::{
    scan_system_table_prefixed, SystemRow, SystemTableError, SystemTableView,
};

/// Go `session.Session` over a session of the DDL's pool.
pub type DdlSession = tidb_ddl_session::Session<dyn tidb_ddl_session::SessionContext>;

/// Go `systable.Manager`.
pub trait Manager: Send + Sync {
    /// Go `GetJobByID`.
    fn get_job_by_id(&self, job_id: i64) -> Result<JobW, SystemTableManagerError>;
    /// Go `GetJobBytesByIDWithSe`.
    fn get_job_bytes_by_id_with_se(
        &self,
        se: &DdlSession,
        job_id: i64,
    ) -> Result<Vec<u8>, SystemTableManagerError>;
    /// Go `GetMDLVer`.
    fn get_mdl_ver(&self, job_id: i64) -> Result<i64, SystemTableManagerError>;
    /// Go `GetMinJobID`.
    fn get_min_job_id(&self, previous_min_job_id: i64) -> Result<i64, SystemTableManagerError>;
    /// Go `HasFlashbackClusterJob`.
    fn has_flashback_cluster_job(&self, min_job_id: i64) -> Result<bool, SystemTableManagerError>;
}

const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Pinned Go `systable.ErrNotFound` and storage/decode failures.
#[derive(Debug)]
pub enum SystemTableManagerError {
    /// The requested job or MDL row does not exist.
    NotFound,
    /// The active DDL table could not be read.
    JobTable(DdlJobTableError),
    /// The MDL table could not be read.
    MdlTable(SystemTableError),
    /// Stored job metadata could not be decoded.
    Job(serde_json::Error),
    /// The pool's session or its statement failed.
    Sql(String),
}

impl fmt::Display for SystemTableManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("not found"),
            Self::JobTable(error) => write!(formatter, "{error}"),
            Self::MdlTable(error) => write!(formatter, "{error}"),
            Self::Job(error) => write!(formatter, "{error}"),
            Self::Sql(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for SystemTableManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotFound | Self::Sql(_) => None,
            Self::JobTable(error) => Some(error),
            Self::MdlTable(error) => Some(error),
            Self::Job(error) => Some(error),
        }
    }
}

impl From<DdlJobTableError> for SystemTableManagerError {
    fn from(error: DdlJobTableError) -> Self {
        Self::JobTable(error)
    }
}

impl From<SystemTableError> for SystemTableManagerError {
    fn from(error: SystemTableError) -> Self {
        Self::MdlTable(error)
    }
}

impl From<serde_json::Error> for SystemTableManagerError {
    fn from(error: serde_json::Error) -> Self {
        Self::Job(error)
    }
}

/// The system-table reads of a DDL with no session pool (a store-level
/// harness): the manager's statements as record reads of one snapshot.
#[derive(Clone, Debug)]
pub struct SnapshotSystemTables {
    catalog: SessionExtendedInfoSchema,
}

impl SnapshotSystemTables {
    /// Table lookup remains method-local just like Go's SQL execution, so
    /// one absent system table cannot break unrelated reads.
    #[must_use]
    pub fn new(catalog: &SessionExtendedInfoSchema) -> Self {
        Self {
            catalog: catalog.clone(),
        }
    }

    fn jobs(&self) -> Result<DdlJobTable, SystemTableManagerError> {
        DdlJobTable::locate(&self.catalog).map_err(Into::into)
    }

    fn mdl(&self) -> Result<SystemTableView, SystemTableManagerError> {
        SystemTableView::locate(&self.catalog, "tidb_mdl_info", &["job_id", "version"])
            .map_err(Into::into)
    }

    /// Go `GetJobBytesByIDWithSe`.
    pub fn get_job_bytes_by_id_with_se<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        job_id: i64,
    ) -> Result<Vec<u8>, SystemTableManagerError> {
        self.jobs()?
            .job_bytes_by_id(snapshot, job_id)?
            .ok_or(SystemTableManagerError::NotFound)
    }

    /// Go `GetJobByID`, retaining the exact bytes in `JobW`.
    pub fn get_job_by_id<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        job_id: i64,
    ) -> Result<JobW, SystemTableManagerError> {
        let bytes = self.get_job_bytes_by_id_with_se(snapshot, job_id)?;
        let mut job = Job::default();
        job.decode(&bytes)?;
        Ok(JobW::new(
            Some(GoShared::new(job)),
            GoSharedSlice::from_vec(bytes),
        ))
    }

    /// Go `GetMDLVer`.
    pub fn get_mdl_ver<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        job_id: i64,
    ) -> Result<i64, SystemTableManagerError> {
        let mdl = self.mdl()?;
        for (key, value) in scan_system_table_prefixed(snapshot, &mdl, &[Datum::Int(job_id)])? {
            let row = SystemRow::parse(&mdl, &key, &value)?;
            if row.i64("job_id")? == Some(job_id) {
                return row.i64("version")?.ok_or(SystemTableManagerError::NotFound);
            }
        }
        Err(SystemTableManagerError::NotFound)
    }

    /// Go `GetMinJobID`.
    pub fn get_min_job_id<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        previous_min_job_id: i64,
    ) -> Result<i64, SystemTableManagerError> {
        Ok(self
            .jobs()?
            .min_job_id(snapshot, previous_min_job_id)?
            .unwrap_or_default())
    }

    /// Go `HasFlashbackClusterJob`.
    pub fn has_flashback_cluster_job<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        min_job_id: i64,
    ) -> Result<bool, SystemTableManagerError> {
        self.jobs()?
            .has_flashback_cluster_job(snapshot, min_job_id)
            .map_err(Into::into)
    }
}

/// Go `manager`: each read is one statement on a session of the DDL's pool.
pub struct SystemTableManager {
    se_pool: Arc<tidb_ddl_session::DynPool>,
}

impl SystemTableManager {
    /// Go `NewManager`.
    #[must_use]
    pub fn new(pool: Arc<tidb_ddl_session::DynPool>) -> Self {
        Self { se_pool: pool }
    }

    /// Go `withNewSession`.
    fn with_new_session<T>(
        &self,
        callback: impl FnOnce(&DdlSession) -> Result<T, SystemTableManagerError>,
    ) -> Result<T, SystemTableManagerError> {
        let se = self
            .se_pool
            .get()
            .map_err(|error| SystemTableManagerError::Sql(error.to_string()))?;
        let result = callback(&DdlSession::new(Arc::clone(&se)));
        self.se_pool.put(se);
        result
    }
}

/// Go `se.Execute(ctx, sql, label)`: the statement's rows.
fn execute(
    se: &DdlSession,
    sql: &str,
    label: &str,
) -> Result<Vec<Vec<Datum>>, SystemTableManagerError> {
    se.execute(&tidb_sqlexec::BackgroundContext, sql, label, &[])
        .map(Option::unwrap_or_default)
        .map_err(|error| SystemTableManagerError::Sql(error.to_string()))
}

impl Manager for SystemTableManager {
    fn get_job_by_id(&self, job_id: i64) -> Result<JobW, SystemTableManagerError> {
        self.with_new_session(|se| {
            let bytes = self.get_job_bytes_by_id_with_se(se, job_id)?;
            let mut job = Job::default();
            job.decode(&bytes)?;
            Ok(JobW::new(
                Some(GoShared::new(job)),
                GoSharedSlice::from_vec(bytes),
            ))
        })
    }

    fn get_job_bytes_by_id_with_se(
        &self,
        se: &DdlSession,
        job_id: i64,
    ) -> Result<Vec<u8>, SystemTableManagerError> {
        let sql = format!("select job_meta from mysql.tidb_ddl_job where job_id = {job_id}");
        let rows = execute(se, &sql, "get-job-by-id")?;
        match rows.first() {
            Some(row) => Ok(row[0].go_bytes().to_vec()),
            None => Err(SystemTableManagerError::NotFound),
        }
    }

    fn get_mdl_ver(&self, job_id: i64) -> Result<i64, SystemTableManagerError> {
        self.with_new_session(|se| {
            let sql = format!("select version from mysql.tidb_mdl_info where job_id = {job_id}");
            let rows = execute(se, &sql, "check-mdl-info")?;
            match rows.first() {
                Some(row) => Ok(row[0].get_int64()),
                None => Err(SystemTableManagerError::NotFound),
            }
        })
    }

    fn get_min_job_id(&self, previous_min_job_id: i64) -> Result<i64, SystemTableManagerError> {
        self.with_new_session(|se| {
            let sql = format!(
                "select min(job_id) from mysql.tidb_ddl_job where job_id >= {previous_min_job_id}"
            );
            let rows = execute(se, &sql, "get-min-job-id")?;
            Ok(rows.first().map_or(0, |row| row[0].get_int64()))
        })
    }

    fn has_flashback_cluster_job(&self, min_job_id: i64) -> Result<bool, SystemTableManagerError> {
        self.with_new_session(|se| {
            let sql = format!(
                "select count(1) from mysql.tidb_ddl_job where job_id >= {min_job_id} and type = {}",
                tidb_model::ActionType::ACTION_FLASHBACK_CLUSTER.0
            );
            let rows = execute(se, &sql, "has-flashback-cluster-job")?;
            Ok(rows.first().is_some_and(|row| row[0].get_int64() > 0))
        })
    }
}

/// Pinned Go `MinJobIDRefresher`.
#[derive(Debug, Default)]
pub struct MinJobIdRefresher {
    current_min_job_id: AtomicI64,
}

impl MinJobIdRefresher {
    /// Go `NewMinJobIDRefresher`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            current_min_job_id: AtomicI64::new(0),
        }
    }

    /// Go `GetCurrMinJobID`.
    #[must_use]
    pub fn get_curr_min_job_id(&self) -> i64 {
        self.current_min_job_id.load(Ordering::SeqCst)
    }

    /// One Go `refresh` call. Read failures leave the cached lower bound
    /// unchanged, and an empty table's zero never moves it backwards.
    pub fn refresh<E: fmt::Display>(&self, get_min_job_id: impl FnOnce(i64) -> Result<i64, E>) {
        let current = self.get_curr_min_job_id();
        match get_min_job_id(current) {
            Ok(next) => self
                .current_min_job_id
                .store(current.max(next), Ordering::SeqCst),
            Err(error) => tidb_ddl_logutil::ddl_logger().info(
                "get min job ID failed",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(error.to_string()),
                )],
            ),
        }
    }

    /// Go `Start`: refresh immediately, then every ten seconds until the
    /// supplied cancellation channel closes or receives a value.
    pub fn start<E: fmt::Display>(
        &self,
        cancelled: &Receiver<()>,
        mut get_min_job_id: impl FnMut(i64) -> Result<i64, E>,
    ) {
        loop {
            self.refresh(&mut get_min_job_id);
            match cancelled.recv_timeout(REFRESH_INTERVAL) {
                Err(RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    #[test]
    fn refresher_never_moves_backwards_like_go() {
        let refresher = MinJobIdRefresher::new();
        refresher.refresh(|current| -> Result<_, &'static str> {
            assert_eq!(current, 0);
            Ok(1)
        });
        assert_eq!(refresher.get_curr_min_job_id(), 1);
        refresher.refresh(|current| -> Result<_, &'static str> {
            assert_eq!(current, 1);
            Ok(100)
        });
        assert_eq!(refresher.get_curr_min_job_id(), 100);
        refresher.refresh(|current| -> Result<_, &'static str> {
            assert_eq!(current, 100);
            Ok(0)
        });
        assert_eq!(refresher.get_curr_min_job_id(), 100);
    }

    #[test]
    fn start_refreshes_before_observing_cancellation_like_go() {
        let refresher = MinJobIdRefresher::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let (cancel, cancelled) = std::sync::mpsc::channel();
        cancel.send(()).unwrap();
        refresher.start(&cancelled, {
            let calls = Arc::clone(&calls);
            move |current| -> Result<_, &'static str> {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(current, 0);
                Ok(7)
            }
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(refresher.get_curr_min_job_id(), 7);
    }
}
