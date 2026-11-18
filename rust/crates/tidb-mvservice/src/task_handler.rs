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

//! Go master `pkg/mvservice/task_handler.go`.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_timer::go_time::GoTime;

use crate::service::{MViewTask, MlogPurgeTask, RefreshAlertTask};

/// An MV service error: Go's `error`, keeping the two context errors the
/// service tells apart (`errors.Is(err, context.Canceled)` /
/// `context.DeadlineExceeded`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvsError {
    /// Go `context.Canceled`.
    Canceled,
    /// Go `context.DeadlineExceeded`.
    DeadlineExceeded,
    /// A statement's MySQL error (Go compares these with `terror.Equal`).
    Sql {
        /// The MySQL error number.
        code: u16,
        /// The error text.
        message: String,
    },
    /// Any other error, by its message.
    Other(String),
}

impl std::fmt::Display for MvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Canceled => f.write_str("context canceled"),
            Self::DeadlineExceeded => f.write_str("context deadline exceeded"),
            Self::Sql { message, .. } | Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for MvsError {}

impl From<String> for MvsError {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

/// Go `ErrMVRefreshHandlerNotRegistered`.
pub const ERR_MV_REFRESH_HANDLER_NOT_REGISTERED: &str = "mv refresh handler is not registered";
/// Go `ErrMVLogPurgeHandlerNotRegistered`.
pub const ERR_MV_LOG_PURGE_HANDLER_NOT_REGISTERED: &str = "mvlog purge handler is not registered";
/// Go `errMVTaskCanceledManually`.
pub const ERR_MV_TASK_CANCELED_MANUALLY: &str = "materialized view task canceled manually";

/// Go `errMVTaskCanceledManually` as an error value.
#[must_use]
pub fn err_mv_task_canceled_manually() -> MvsError {
    MvsError::Other(ERR_MV_TASK_CANCELED_MANUALLY.to_owned())
}

/// Go `isMVTaskCanceledManually`.
#[must_use]
pub fn is_mv_task_canceled_manually(err: &MvsError) -> bool {
    err.to_string().contains(ERR_MV_TASK_CANCELED_MANUALLY)
}

/// Go `mlogAccumulationTask`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MlogAccumulationTask {
    /// Go `schemaName`.
    pub schema_name: String,
    /// Go `mlogName`.
    pub mlog_name: String,
    /// Go `alertRows`.
    pub alert_rows: u64,
}

/// Go `mlogAnalyzeTask`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MlogAnalyzeTask {
    /// Go `schemaName`.
    pub schema_name: String,
    /// Go `mlogName`.
    pub mlog_name: String,
    /// Go `reason`.
    pub reason: String,
    /// Go `ratio`.
    pub ratio: f64,
}

/// Go `basic.SessionPool` as the service hands it to its task handler: the
/// handler knows the concrete pool.
pub trait SessionPool: Send + Sync {
    /// The concrete pool.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Go `MVTaskHandler`: every task operation the service needs. A zero
/// [`GoTime`] answer means "no further scheduling", as Go's zero time.
#[allow(clippy::missing_errors_doc)]
pub trait MVTaskHandler: Send + Sync {
    /// Go `RefreshMV`.
    fn refresh_mv(&self, ctx: &Context, pool: &dyn SessionPool, mview_id: i64) -> Result<GoTime, MvsError>;
    /// Go `PurgeMVLog`.
    fn purge_mv_log(&self, ctx: &Context, pool: &dyn SessionPool, mlog_id: i64) -> Result<GoTime, MvsError>;
    /// Go `TryBackoffRefreshManualCancel`.
    fn try_backoff_refresh_manual_cancel(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
        mview_id: i64,
        next_refresh: &GoTime,
    ) -> (bool, GoTime, Option<MvsError>);
    /// Go `TryBackoffPurgeManualCancel`.
    fn try_backoff_purge_manual_cancel(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
        mlog_id: i64,
        next_purge: &GoTime,
    ) -> (bool, GoTime, Option<MvsError>);
    /// Go `SyncMVRefreshAlertStates`.
    fn sync_mv_refresh_alert_states(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
        updated_at: &GoTime,
        states: &[RefreshAlertTask],
    ) -> Result<(), MvsError>;
    /// Go `CleanupStaleMVRefreshAlerts`.
    fn cleanup_stale_mv_refresh_alerts(&self, ctx: &Context, pool: &dyn SessionPool) -> Result<(), MvsError>;
    /// Go `LoadAllTiDBMVLogPurge`.
    fn load_all_tidb_mv_log_purge(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
    ) -> Result<HashMap<i64, Arc<MlogPurgeTask>>, MvsError>;
    /// Go `LoadAllTiDBMVLogAccumulationTasks`.
    fn load_all_tidb_mv_log_accumulation_tasks(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
    ) -> Result<HashMap<i64, MlogAccumulationTask>, MvsError>;
    /// Go `LoadTiDBMVLogAccumulationRowCounts`.
    fn load_tidb_mv_log_accumulation_row_counts(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
        tasks: &HashMap<i64, MlogAccumulationTask>,
    ) -> Result<HashMap<i64, u64>, MvsError>;
    /// Go `LoadAllTiDBMVLogAnalyzeTasks`.
    fn load_all_tidb_mv_log_analyze_tasks(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
    ) -> Result<HashMap<i64, MlogAnalyzeTask>, MvsError>;
    /// Go `AnalyzeMVLog`.
    fn analyze_mv_log(&self, ctx: &Context, pool: &dyn SessionPool, mlog_id: i64) -> Result<(), MvsError>;
    /// Go `LoadAllTiDBMVRefresh`.
    fn load_all_tidb_mv_refresh(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
    ) -> Result<HashMap<i64, Arc<MViewTask>>, MvsError>;
    /// Go `GetCurrentTSO`.
    fn get_current_tso(&self, ctx: &Context, pool: &dyn SessionPool) -> Result<u64, MvsError>;
    /// Go `PurgeMVHistoryBeforeTSO`.
    fn purge_mv_history_before_tso(
        &self,
        ctx: &Context,
        pool: &dyn SessionPool,
        current_tso: u64,
        mview_refresh_retention: i64,
        mlog_purge_retention: i64,
    ) -> Result<(), MvsError>;
}
