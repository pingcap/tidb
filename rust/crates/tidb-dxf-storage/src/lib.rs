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

pub use converter::{row2_sub_task, row2_task, task_id_to_key};

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
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnstableSubtasks => "unstable subtasks",
            Self::TaskNotFound => "task not found",
            Self::TaskAlreadyExists => "task already exists",
            Self::TaskStateNotAllow => "task state not allow to do the operation",
            Self::TaskChanged => "task changed by other operation",
            Self::SubtaskNotFound => "subtask not found",
            Self::Other(message) => message,
        })
    }
}

impl std::error::Error for StorageError {}

impl From<tidb_sqlexec::SqlExecError> for StorageError {
    fn from(error: tidb_sqlexec::SqlExecError) -> Self {
        match error.downcast::<Self>() {
            Ok(error) => *error,
            Err(error) => Self::Other(error.to_string()),
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
}

/// Go `TaskManager`: the manager of tasks and subtasks.
pub struct TaskManager {
    se_pool: Arc<dyn tidb_syssession::Pool<dyn SessionContext>>,
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
        // Go `clitutil.WithInternalSourceType(ctx, kv.InternalDistTask)`.
        let context = BackgroundContext;
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
