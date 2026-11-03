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

//! The internal sessions the domain's workers run SQL on: Go
//! `Domain.sysSessionPool`'s resources, which the session package creates
//! (`createSessionFunc`) and the domain only sees as `sessionctx.Context`.

use std::sync::Arc;

use tidb_datatype::Datum;

/// One pooled internal session, as the domain's workers use it: the stats
/// handle's session contexts plus Go `ExecuteInternal`.
pub trait DomainSysSession:
    tidb_stats_handle_util::StatsSessionContext
    + tidb_stats_handle_autoanalyze_exec::AutoAnalyzeSessionContext
{
    /// The first column of the first row of an internal query, if any.
    fn scalar(&self, sql: &str) -> Result<Option<Datum>, String>;

    /// Go `ExecuteInternal` of a write: the rows it affected.
    fn execute_internal_write(&self, sql: &str) -> Result<u64, String>;

    /// Go `ExecuteInternal` of a query, its rows drained.
    fn query_internal(&self, sql: &str) -> Result<Vec<Vec<Datum>>, String>;

    /// Go `ddlHandlerImpl.HandleDDLEvent`'s statistics writes for one schema
    /// change, in this session's transaction.
    fn handle_stats_ddl_event(
        &self,
        event: &tidb_ddl_notifier::SchemaChangeEvent,
    ) -> Result<(), String>;

    /// The session package's own view of this session, which only it
    /// downcasts.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Go `Domain.AdvancedSysSessionPool()`.
pub type DomainSysSessionPool = tidb_syssession::AdvancedSessionPool<dyn DomainSysSession>;

/// A statement error of a statistics session: the statement's own error,
/// printed as its message (what callers that only read the text see).
#[derive(Debug)]
pub struct StatsSessionStatementError(pub tidb_store_driver_error::sql_query_error::SqlQueryError);

impl std::fmt::Display for StatsSessionStatementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

impl std::error::Error for StatsSessionStatementError {}

/// A statistics session's failure as its MySQL error: a statement's own, or
/// an unknown error naming any other failure.
pub fn stats_session_sql_error(
    error: tidb_sqlexec::SqlExecError,
) -> tidb_store_driver_error::sql_query_error::SqlQueryError {
    match error.downcast::<StatsSessionStatementError>() {
        Ok(error) => error.0,
        Err(error) => tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(error.to_string()),
    }
}

/// Go `statsHandle.SPool()` for the statistics handle's saves: the domain's
/// system sessions (`None` once the domain is closed).
pub struct StatsPool<'a> {
    pub session_pool: Option<&'a std::sync::Arc<DomainSysSessionPool>>,
}

impl tidb_stats_handle_storage::stats_read_writer::StatsSessionPool for StatsPool<'_> {
    fn call_with_sctx(
        &self,
        f: &mut dyn FnMut(&dyn tidb_stats_handle_util::StatsSessionContext) -> Result<(), tidb_sqlexec::SqlExecError>,
        flags: &[i32],
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        let pool = self.session_pool.ok_or_else(|| {
            tidb_store_driver_error::sql_query_error::SqlQueryError::unknown("the node's domain is closed")
        })?;
        tidb_stats_handle_util::call_with_sctx(pool.as_ref(), |sctx| f(sctx), flags)
            .map_err(stats_session_sql_error)
    }
}

pub fn stats_session_error(message: impl Into<String>) -> tidb_sqlexec::SqlExecError {
    Box::new(std::io::Error::other(message.into()))
}

pub fn system_process_trackers(
    processes: tidb_sessmgr::ProcessRegistry,
) -> (tidb_sqlexec::TrackSysProc, tidb_sqlexec::UntrackSysProc) {
    let tracking = processes.clone();
    let track: tidb_sqlexec::TrackSysProc = Arc::new(move |id, process| {
        let process = (&*process as &dyn std::any::Any)
            .downcast_ref::<tidb_sessmgr::SystemProcess>()
            .ok_or_else(|| stats_session_error("invalid system process owner"))?;
        tracking
            .track_system_process(id, process.clone())
            .map_err(stats_session_error)
    });
    let untrack: tidb_sqlexec::UntrackSysProc =
        Arc::new(move |id| processes.untrack_system_process(id));
    (track, untrack)
}
