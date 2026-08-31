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

//! Pinned Go `pkg/ddl/systable`: reads of the active DDL and MDL system
//! tables plus the monotonic minimum-job-ID refresher.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use tidb_datatype::Datum;
use tidb_model::{GoShared, GoSharedSlice, Job, JobW};

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::ddl_job_table::{DdlJobTable, DdlJobTableError};
use crate::mysql_system_tables::{
    scan_system_table_prefixed, SystemRow, SystemTableError, SystemTableView,
};

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
}

impl fmt::Display for SystemTableManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("not found"),
            Self::JobTable(error) => write!(formatter, "{error}"),
            Self::MdlTable(error) => write!(formatter, "{error}"),
            Self::Job(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for SystemTableManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotFound => None,
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

/// Pinned Go `systable.Manager` over one loaded system-table catalog.
#[derive(Clone, Debug)]
pub struct SystemTableManager {
    catalog: ClusterCatalog,
}

impl SystemTableManager {
    /// Go `NewManager`. Table lookup remains method-local just like Go's SQL
    /// execution, so one absent system table cannot break unrelated reads.
    #[must_use]
    pub fn new(catalog: &ClusterCatalog) -> Self {
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
    pub fn get_job_bytes_by_id<S: MetaSnapshot>(
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
        let bytes = self.get_job_bytes_by_id(snapshot, job_id)?;
        let mut job = Job::default();
        job.decode(&bytes)?;
        Ok(JobW::new(
            Some(GoShared::new(job)),
            GoSharedSlice::from_vec(bytes),
        ))
    }

    /// Go `GetMDLVer`.
    pub fn get_mdl_version<S: MetaSnapshot>(
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
    pub fn current_min_job_id(&self) -> i64 {
        self.current_min_job_id.load(Ordering::SeqCst)
    }

    /// One Go `refresh` call. Read failures leave the cached lower bound
    /// unchanged, and an empty table's zero never moves it backwards.
    pub fn refresh<E: fmt::Display>(&self, get_min_job_id: impl FnOnce(i64) -> Result<i64, E>) {
        let current = self.current_min_job_id();
        match get_min_job_id(current) {
            Ok(next) => self
                .current_min_job_id
                .store(current.max(next), Ordering::SeqCst),
            Err(error) => eprintln!(
                "{{\"level\":\"info\",\"message\":\"get min job ID failed\",\"error\":{}}}",
                serde_json::to_string(&error.to_string())
                    .unwrap_or_else(|_| "\"unprintable\"".to_owned())
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
        assert_eq!(refresher.current_min_job_id(), 1);
        refresher.refresh(|current| -> Result<_, &'static str> {
            assert_eq!(current, 1);
            Ok(100)
        });
        assert_eq!(refresher.current_min_job_id(), 100);
        refresher.refresh(|current| -> Result<_, &'static str> {
            assert_eq!(current, 100);
            Ok(0)
        });
        assert_eq!(refresher.current_min_job_id(), 100);
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
        assert_eq!(refresher.current_min_job_id(), 7);
    }
}
