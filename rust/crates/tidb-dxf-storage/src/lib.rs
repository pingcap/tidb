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


//! Go `pkg/dxf/framework/storage`: the distributed execution framework's
//! `TaskManager`, the only code that reads or writes the framework's tables
//! (`mysql.tidb_global_task`, `mysql.tidb_background_subtask`, their
//! histories and `mysql.dist_framework_meta`). Every method runs Go's SQL text
//! on a session from the framework's session pool.

mod converter;
mod history;
mod nodes;
mod subtask_state;
mod subtask_table;
mod task_state;
mod task_table;

pub use converter::{row2_sub_task, row2_task, task_id_to_key};
pub use history::{
    classify_task_error, validate_history_task_page_size, HistoryTaskPage, HistoryTaskSummary, TaskCleanupInfo,
    DEFAULT_HISTORY_TASK_PAGE_SIZE, MAX_HISTORY_TASK_PAGE_SIZE, MIN_HISTORY_TASK_PAGE_SIZE,
};
pub use nodes::{get_dxf_cpu_count, get_node_resource, set_node_resource};
pub use subtask_table::SubtaskError;
pub use task_state::{is_cancelled_err, TASK_CANCEL_MESSAGE};
pub use task_table::{ActiveTaskSummary, TaskExecInfo, TEST_CHANNEL};

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::{BackgroundContext, ExecutionContext};

/// Go `defaultSubtaskKeepDays`.
pub const DEFAULT_SUBTASK_KEEP_DAYS: i64 = 14;

/// Go `basicTaskColumns`.
pub const BASIC_TASK_COLUMNS: &str = "t.id, t.task_key, t.type, t.state, t.step, t.priority, t.concurrency, t.create_time, t.target_scope, t.max_node_count, t.extra_params, t.keyspace";
/// Go `TaskColumns`.
pub const TASK_COLUMNS: &str = "t.id, t.task_key, t.type, t.state, t.step, t.priority, t.concurrency, t.create_time, t.target_scope, t.max_node_count, t.extra_params, t.keyspace, t.start_time, t.state_update_time, t.meta, t.dispatcher_id, t.error, t.modify_params";
/// Go `InsertTaskColumns`.
pub const INSERT_TASK_COLUMNS: &str = "task_key, type, state, priority, concurrency, step, meta, create_time, target_scope, max_node_count, extra_params, keyspace";
/// Go `basicSubtaskColumns`.
pub const BASIC_SUBTASK_COLUMNS: &str = "id, step, task_key, type, exec_id, state, concurrency, create_time, ordinal, start_time";
/// Go `SubtaskColumns`.
pub const SUBTASK_COLUMNS: &str = "id, step, task_key, type, exec_id, state, concurrency, create_time, ordinal, start_time, state_update_time, meta, summary";
/// Go `InsertSubtaskColumns`.
pub const INSERT_SUBTASK_COLUMNS: &str = "step, task_key, exec_id, meta, state, type, concurrency, ordinal, create_time, checkpoint, summary";

/// Go `maxSubtaskBatchSize` (16 MiB).
pub const MAX_SUBTASK_BATCH_SIZE: usize = 16 * 1024 * 1024;

/// The package's errors: Go's sentinel errors, and the SQL layer's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Go `ErrUnstableSubtasks`.
    UnstableSubtasks,
    /// Go `ErrTaskNotFound`.
    TaskNotFound,
    /// Go `ErrTaskAlreadyExists`.
    TaskAlreadyExists,
    /// Go `ErrTaskStateNotAllow`.
    TaskStateNotAllow,
    /// Go `ErrTaskChanged`.
    TaskChanged,
    /// Go `ErrSubtaskNotFound`.
    SubtaskNotFound,
    /// An error the session or the SQL layer returned, or Go's
    /// `errors.Errorf` text.
    Other(String),
    /// Go `errors.Annotate(cause, message)`: the message in front of its
    /// cause, which Go's `errors.Is` still finds.
    Annotated(Box<StorageError>, String),
    /// A `*terror.Error` the session or the SQL layer returned: Go returns
    /// the terror itself, which keeps its `[class:code]` identity.
    Terror(tidb_terror::TerrorError),
}

impl StorageError {
    /// Go `errors.Annotate` / `errors.Annotatef`.
    #[must_use]
    pub fn annotate(cause: Self, message: impl Into<String>) -> Self {
        Self::Annotated(Box::new(cause), message.into())
    }

    /// Go `errors.Is(err, target)` over the annotations.
    #[must_use]
    pub fn is(&self, target: &Self) -> bool {
        match self {
            Self::Annotated(cause, _) => self == target || cause.is(target),
            _ => self == target,
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Annotated(cause, message) => return write!(f, "{message}: {cause}"),
            Self::Terror(t_err) => return write!(f, "{t_err}"),
            _ => {}
        }
        f.write_str(match self {
            Self::UnstableSubtasks => "unstable subtasks",
            Self::TaskNotFound => "task not found",
            Self::TaskAlreadyExists => "task already exists",
            Self::TaskStateNotAllow => "task state not allow to do the operation",
            Self::TaskChanged => "task changed by other operation",
            Self::SubtaskNotFound => "subtask not found",
            Self::Other(message) | Self::Annotated(_, message) => message,
            Self::Terror(_) => unreachable!("written above"),
        })
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Annotated(cause, _) => Some(cause.as_ref()),
            Self::Terror(t_err) => t_err.source(),
            _ => None,
        }
    }
}

impl From<tidb_sqlexec::SqlExecError> for StorageError {
    fn from(error: tidb_sqlexec::SqlExecError) -> Self {
        let error = match error.downcast::<Self>() {
            Ok(error) => return *error,
            Err(error) => error,
        };
        match error.downcast::<tidb_store_driver_error::sql_query_error::SqlQueryError>() {
            Ok(query_error) => match query_error.identity {
                // Go `err.Error()` of a class without a description: `[code]message`.
                Some(identity) if identity.class().description().is_empty() => {
                    Self::Terror(tidb_terror::TerrorError::compatible(identity.code(), query_error.message))
                }
                Some(identity) => Self::Terror(tidb_terror::TerrorError::synthesize(
                    identity.class(),
                    identity.code(),
                    query_error.message,
                )),
                None => Self::Other(query_error.message),
            },
            Err(error) => Self::Other(error.to_string()),
        }
    }
}

impl StorageError {
    /// The error as Go holds it: a terror the SQL layer returned is the
    /// terror itself, every other error is this one.
    #[must_use]
    pub fn as_go_error(&self) -> &(dyn std::error::Error + 'static) {
        match self {
            Self::Terror(t_err) => t_err,
            _ => self,
        }
    }
}

/// The package's result.
pub type Result<T> = std::result::Result<T, StorageError>;

/// The `sessionctx.Context` operations this package uses, beyond those
/// `syssession` declares.
pub trait SessionContext: tidb_syssession::SessionContext {
    /// Go `CommitTxn`.
    ///
    /// # Errors
    /// The commit failed.
    fn commit_txn(&self, context: &dyn ExecutionContext) -> Result<()>;
    /// Go `GetSessionVars().TxnEntrySizeLimit`.
    fn txn_entry_size_limit(&self) -> u64;
    /// Sets Go `GetSessionVars().TxnEntrySizeLimit`.
    fn set_txn_entry_size_limit(&self, limit: u64);
    /// Go `vardef.TxnEntrySizeLimit.Load()`, the global value.
    fn global_txn_entry_size_limit(&self) -> u64;
    /// Go `GetSessionVars().StmtCtx.AffectedRows()`: the rows the last
    /// statement changed.
    fn affected_rows(&self) -> u64;
    /// Go `GetSessionVars().MemQuotaQuery`.
    fn mem_quota_query(&self) -> i64;
    /// Go `GetSessionVars().SetSystemVar(name, value)`.
    ///
    /// # Errors
    /// The variable rejects the value.
    fn set_system_var(&self, name: &str, value: &str) -> Result<()>;
    /// Go `GetStore().GetKeyspace()`: the keyspace of the session's store.
    fn store_keyspace(&self) -> String;
    /// Go `GetSQLServer()`.
    fn sql_server(&self) -> Arc<dyn tidb_domain_sqlsvrapi::Server<dyn SessionContext>>;
}

/// Go `TaskManager`: the manager of tasks and subtasks.
pub struct TaskManager {
    se_pool: Arc<dyn tidb_syssession::Pool<dyn SessionContext>>,
}

/// Go `taskManagerInstance`.
static TASK_MANAGER_INSTANCE: std::sync::RwLock<Option<Arc<TaskManager>>> = std::sync::RwLock::new(None);

/// Go `dxfSvcTaskMgr`: only used on nextgen, and only initialized in a user
/// keyspace, where it points to the SYSTEM keyspace.
static DXF_SVC_TASK_MGR: std::sync::RwLock<Option<Arc<TaskManager>>> = std::sync::RwLock::new(None);

/// Go `GetTaskManager`: the task manager over this instance's storage.
/// Not only DXF uses it: add-index and import-into reach internal sessions
/// through it too. On nextgen the DXF service runs only in the SYSTEM
/// keyspace; [`get_dxf_svc_task_mgr`] reaches it.
///
/// # Errors
/// No task manager is set.
pub fn get_task_manager() -> Result<Arc<TaskManager>> {
    TASK_MANAGER_INSTANCE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .ok_or_else(|| StorageError::Other("task manager is not initialized".to_owned()))
}

/// Go `SetTaskManager`.
pub fn set_task_manager(is: Arc<TaskManager>) {
    *TASK_MANAGER_INSTANCE.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(is);
}

/// Go `GetDXFSvcTaskMgr`: the task manager that reaches the DXF service.
///
/// # Errors
/// The needed task manager is not set.
pub fn get_dxf_svc_task_mgr() -> Result<Arc<TaskManager>> {
    if tidb_config_kerneltype::is_next_gen()
        && tidb_config::config_tree::config::get_global_keyspace_name() != tidb_keyspace::SYSTEM
    {
        return DXF_SVC_TASK_MGR
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| StorageError::Other("DXF service task manager is not initialized".to_owned()));
    }
    get_task_manager()
}

/// Go `SetDXFSvcTaskMgr`.
pub fn set_dxf_svc_task_mgr(mgr: Arc<TaskManager>) {
    *DXF_SVC_TASK_MGR.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(mgr);
}

impl TaskManager {
    /// Go `NewTaskManager`.
    #[must_use]
    pub fn new(se_pool: Arc<dyn tidb_syssession::Pool<dyn SessionContext>>) -> Self {
        Self { se_pool }
    }

    /// Go `WithNewSession`: runs `f` with a pooled session whose
    /// `TxnEntrySizeLimit` is the global one for the call (a global-sort
    /// subtask meta can name every file the step generated).
    ///
    /// # Errors
    /// The pool or `f` failed.
    pub fn with_new_session(&self, f: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
        let mut outcome: Result<()> = Ok(());
        let pooled = self.se_pool.with_session(&mut |session| {
            session
                .with_session_context(|se| {
                    let limit_bak = se.txn_entry_size_limit();
                    se.set_txn_entry_size_limit(se.global_txn_entry_size_limit());
                    outcome = f(se);
                    se.set_txn_entry_size_limit(limit_bak);
                    Ok::<(), tidb_syssession::SysSessionError>(())
                })
                .map_err(|error| Box::new(error) as tidb_sqlexec::SqlExecError)
        });
        outcome?;
        pooled.map_err(StorageError::from)
    }

    /// Go `WithNewTxn`: `begin` on the SQL path, then the session's commit,
    /// or its rollback when `f` fails.
    ///
    /// # Errors
    /// The begin, `f` or the commit failed.
    pub fn with_new_txn(&self, f: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
        let context = tikv_client::util::with_internal_source_type(
            &tikv_client::trace::TraceContext::new(),
            tidb_txnkv::INTERNAL_DIST_TASK,
        );
        self.with_new_session(&mut |se| {
            exec_sql(&context, se, "begin", &[])?;
            match f(se) {
                Ok(()) => se.commit_txn(&context),
                Err(error) => {
                    se.rollback_txn(&context);
                    Err(error)
                }
            }
        })
    }

    /// Go `ExecuteSQLWithNewSession`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn execute_sql_with_new_session(&self, sql: &str, arguments: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>> {
        let mut rows = Vec::new();
        self.with_new_session(&mut |se| {
            rows = exec_sql(&BackgroundContext, se, sql, arguments)?;
            Ok(())
        })?;
        Ok(rows)
    }
}

/// Go `sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sql, args...)`.
///
/// # Errors
/// The statement failed.
pub fn exec_sql(
    context: &dyn ExecutionContext,
    se: &dyn SessionContext,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>> {
    tidb_sqlexec::execute_sql(context, se.sql_executor().as_ref(), sql, arguments).map_err(StorageError::from)
}
