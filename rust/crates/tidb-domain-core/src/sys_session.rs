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

pub fn stats_session_error(message: impl Into<String>) -> tidb_sqlexec::SqlExecError {
    Box::new(std::io::Error::other(message.into()))
}

pub fn system_process_trackers(
    processes: tidb_executor::process::ProcessRegistry,
) -> (tidb_sqlexec::TrackSysProc, tidb_sqlexec::UntrackSysProc) {
    let tracking = processes.clone();
    let track: tidb_sqlexec::TrackSysProc = Arc::new(move |id, process| {
        let process = (&*process as &dyn std::any::Any)
            .downcast_ref::<tidb_executor::process::SystemProcess>()
            .ok_or_else(|| stats_session_error("invalid system process owner"))?;
        tracking
            .track_system_process(id, process.clone())
            .map_err(stats_session_error)
    });
    let untrack: tidb_sqlexec::UntrackSysProc =
        Arc::new(move |id| processes.untrack_system_process(id));
    (track, untrack)
}
