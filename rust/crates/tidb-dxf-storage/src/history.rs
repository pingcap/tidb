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

//! Go `history.go`: moving finished tasks and subtasks to the history
//! tables, and listing the task history.

use std::error::Error;

use chrono::{DateTime, FixedOffset};
use tidb_dxf::{Task, TaskBase, TaskState, TaskType, TASK_STATE_FAILED, TASK_STATE_REVERTED};
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::BackgroundContext;

use crate::converter::{get_int64, get_local_time, is_null, row2_task_basic, row2_task_error, zero_time};
use crate::task_table::{sql_args, Arg};
use crate::{
    exec_sql, is_cancelled_err, task_id_to_key, Result, SessionContext, StorageError, TaskManager, BASIC_TASK_COLUMNS,
    DEFAULT_SUBTASK_KEEP_DAYS,
};

/// Go `DefaultHistoryTaskPageSize`: the default page size for history task
/// listing.
pub const DEFAULT_HISTORY_TASK_PAGE_SIZE: isize = 20;
/// Go `MinHistoryTaskPageSize`.
pub const MIN_HISTORY_TASK_PAGE_SIZE: isize = 1;
/// Go `MaxHistoryTaskPageSize`.
pub const MAX_HISTORY_TASK_PAGE_SIZE: isize = 200;
/// Go `taskErrorCategoryCancelled`.
const TASK_ERROR_CATEGORY_CANCELLED: &str = "cancelled";
/// Go `taskErrorCategoryDataError`.
const TASK_ERROR_CATEGORY_DATA_ERROR: &str = "data-error";

/// Go `historyTaskSummaryColumns`.
fn history_task_summary_columns() -> String {
    [BASIC_TASK_COLUMNS, ", t.error, t.start_time, t.state_update_time, t.end_time"].concat()
}

/// Go `HistoryTaskSummary`: the summary fields of one history task.
#[derive(Debug, Clone)]
pub struct HistoryTaskSummary {
    /// Go's embedded `*proto.TaskBase`.
    pub task_base: TaskBase,
    /// Go `ErrorCode`.
    pub error_code: String,
    /// Go `ErrorCategory`.
    pub error_category: String,
    /// Go `StartTime`.
    pub start_time: DateTime<FixedOffset>,
    /// Go `StateUpdateTime`.
    pub state_update_time: DateTime<FixedOffset>,
    /// Go `EndTime`.
    pub end_time: DateTime<FixedOffset>,
}

/// Go `HistoryTaskPage`: the paged result of history task listing.
#[derive(Debug, Clone, Default)]
pub struct HistoryTaskPage {
    /// Go `Items`.
    pub items: Vec<HistoryTaskSummary>,
    /// Go `HasMore`.
    pub has_more: bool,
    /// Go `NextPageToken`.
    pub next_page_token: i64,
    /// Go `ApproxTotalCount`.
    pub approx_total_count: i64,
}

/// Go `TaskCleanupInfo`: the task metadata needed to clean up external
/// files.
#[derive(Debug, Clone)]
pub struct TaskCleanupInfo {
    /// Go `ID`.
    pub id: i64,
    /// Go `Type`.
    pub tp: TaskType,
    /// Go `State`.
    pub state: TaskState,
    /// Go `EndTime`.
    pub end_time: Option<DateTime<FixedOffset>>,
}

/// Go `ValidateHistoryTaskPageSize`.
///
/// # Errors
/// The page size is outside Go's bounds.
pub fn validate_history_task_page_size(page_size: isize) -> Result<()> {
    if !(MIN_HISTORY_TASK_PAGE_SIZE..=MAX_HISTORY_TASK_PAGE_SIZE).contains(&page_size) {
        return Err(StorageError::Other(format!(
            "page size should be within [{MIN_HISTORY_TASK_PAGE_SIZE}, {MAX_HISTORY_TASK_PAGE_SIZE}]"
        )));
    }
    Ok(())
}

impl TaskManager {
    /// Go `TransferSubtasks2HistoryWithSession`: moves the task's subtasks
    /// to `mysql.tidb_background_subtask_history`.
    ///
    /// # Errors
    /// A statement failed.
    pub fn transfer_subtasks2_history_with_session(se: &dyn SessionContext, task_id: i64) -> Result<()> {
        let args = [Arg::Str(task_id_to_key(task_id))];
        exec_sql(&BackgroundContext, se, "insert into mysql.tidb_background_subtask_history select * from mysql.tidb_background_subtask where task_key = %?", &sql_args(&args))?;
        // delete taskID subtask
        exec_sql(
            &BackgroundContext,
            se,
            "delete from mysql.tidb_background_subtask where task_key = %?",
            &sql_args(&args),
        )?;
        Ok(())
    }

    /// Go `TransferTasks2History`: moves the tasks and their subtasks to the
    /// history tables, in one transaction.
    ///
    /// # Errors
    /// The session or a statement failed.
    pub fn transfer_tasks2_history(&self, tasks: &[Task]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }
        let mut task_id_strs = Vec::with_capacity(tasks.len());
        let mut task_key_strs = Vec::with_capacity(tasks.len());
        let mut update_meta_args = Vec::with_capacity(tasks.len() * 2);
        let mut update_meta_sql = String::from("
		update mysql.tidb_global_task
		set meta = case id");
        for task in tasks {
            task_id_strs.push(format!("{}", task.base.id));
            task_key_strs.push(format!("'{}'", task.base.id));
            update_meta_sql.push_str(" when %? then %?");
            update_meta_args.extend([Arg::Int(task.base.id), Arg::Bytes(task.meta.clone())]);
        }
        let task_id_list = task_id_strs.join(", ");
        let task_key_list = task_key_strs.join(", ");
        update_meta_sql.push_str(&["
			end, state_update_time = CURRENT_TIMESTAMP()
		where id in(", &task_id_list, ")"].concat());
        self.with_new_txn(&mut |se| {
            // sensitive data in meta might be redacted, need update first.
            exec_sql(&BackgroundContext, se, &update_meta_sql, &sql_args(&update_meta_args))?;
            exec_sql(&BackgroundContext, se, &["
			insert into mysql.tidb_global_task_history
			select * from mysql.tidb_global_task
			where id in(", &task_id_list, ")"].concat(), &[])?;
            exec_sql(&BackgroundContext, se, &["
			delete from mysql.tidb_global_task
			where id in(", &task_id_list, ")"].concat(), &[])?;
            exec_sql(&BackgroundContext, se, &["
			insert into mysql.tidb_background_subtask_history
			select * from mysql.tidb_background_subtask
			where task_key in(", &task_key_list, ")"].concat(), &[])?;
            exec_sql(&BackgroundContext, se, &["
			delete from mysql.tidb_background_subtask
			where task_key in(", &task_key_list, ")"].concat(), &[])?;
            Ok(())
        })
    }

    /// Go `ListHistoryTasks`: history tasks with keyset pagination and an
    /// optional keyspace filter.
    ///
    /// # Errors
    /// The page size is out of bounds, or the session or a statement failed.
    pub fn list_history_tasks(&self, page_size: isize, page_token: i64, keyspace: &str) -> Result<HistoryTaskPage> {
        validate_history_task_page_size(page_size)?;

        let mut where_parts = Vec::with_capacity(2);
        let mut count_args = Vec::with_capacity(1);
        if !keyspace.is_empty() {
            where_parts.push("t.keyspace = %?");
            count_args.push(SqlArg::String(keyspace.as_bytes()));
        }
        let mut data_args = count_args.clone();
        if page_token > 0 {
            where_parts.push("t.id < %?");
            data_args.push(SqlArg::Signed(page_token));
        }
        let where_sql = if where_parts.is_empty() { String::new() } else { format!(" where {}", where_parts.join(" and ")) };
        data_args.push(SqlArg::Signed(page_size as i64 + 1));

        let mut rows = self.execute_sql_with_new_session(
            &["select ", &history_task_summary_columns(), " from mysql.tidb_global_task_history t", &where_sql, " order by t.id desc limit %?"].concat(),
            &data_args,
        )?;
        // Intentionally keep this as a separate best-effort read.
        // Under concurrent task transfers, Items and ApproxTotalCount may observe slightly
        // different snapshots, which is acceptable for this observability API.
        // Pagination correctness relies on page rows + HasMore/NextPageToken from the page query.
        let count_rows = self.execute_sql_with_new_session(
            &["select count(1) from mysql.tidb_global_task_history", if keyspace.is_empty() { "" } else { " where keyspace = %?" }].concat(),
            &count_args,
        )?;

        let page_size = page_size as usize;
        let has_more = rows.len() > page_size;
        if has_more {
            rows.truncate(page_size);
        }
        let mut page = HistoryTaskPage {
            items: rows.iter().map(|row| row2_history_task_summary(row)).collect(),
            has_more,
            next_page_token: 0,
            approx_total_count: count_rows.first().map_or(0, |row| get_int64(row, 0)),
        };
        if has_more {
            page.next_page_token = page.items.last().map_or(0, |item| item.task_base.id);
        }
        Ok(page)
    }

    /// Go `GCSubtasks`: deletes the history subtasks older than the keep
    /// days.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn gc_subtasks(&self) -> Result<()> {
        let subtask_history_keep_seconds = DEFAULT_SUBTASK_KEEP_DAYS * 24 * 60 * 60;
        self.execute_sql_with_new_session(
            &format!(
                "DELETE FROM mysql.tidb_background_subtask_history WHERE state_update_time < UNIX_TIMESTAMP() - {subtask_history_keep_seconds} ;"
            ),
            &[],
        )?;
        Ok(())
    }
}

/// Go `row2HistoryTaskSummary`.
fn row2_history_task_summary(r: &[tidb_datatype::Datum]) -> HistoryTaskSummary {
    let mut item = HistoryTaskSummary {
        task_base: row2_task_basic(r),
        error_code: String::new(),
        error_category: String::new(),
        start_time: zero_time(),
        state_update_time: zero_time(),
        end_time: zero_time(),
    };
    if let Some(task_err) = row2_task_error(r, 12) {
        let task_err: &(dyn Error + 'static) = &*task_err;
        item.error_code = task_error_code(task_err);
        item.error_category = classify_task_error(&item.task_base.state, Some(task_err));
    }
    if !is_null(r, 13) {
        item.start_time = get_local_time(r, 13);
    }
    if !is_null(r, 14) {
        item.state_update_time = get_local_time(r, 14);
    }
    if !is_null(r, 15) {
        item.end_time = get_local_time(r, 15);
    }
    item
}

/// Go `ClassifyTaskError`: a coarse category of a terminal task error. The
/// history API exposes it, so it never contains the raw error text.
#[must_use]
pub fn classify_task_error(state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> String {
    let Some(task_err) = task_err else {
        return String::new();
    };
    if *state == TASK_STATE_FAILED {
        TASK_STATE_FAILED.to_string()
    } else if *state == TASK_STATE_REVERTED {
        if is_cancelled_err(Some(task_err)) {
            return TASK_ERROR_CATEGORY_CANCELLED.to_owned();
        }
        if is_data_error(task_err) {
            return TASK_ERROR_CATEGORY_DATA_ERROR.to_owned();
        }
        TASK_STATE_FAILED.to_string()
    } else {
        String::new()
    }
}

/// Go `isDataError`.
fn is_data_error(task_err: &(dyn Error + 'static)) -> bool {
    let err_msg = task_err.to_string();
    // Keep these checks string-based to avoid depending on Lightning error definitions.
    // We can replace this when those error definitions are split out of the Lightning
    // package. DXF error serialization keeps only the outer error code, so nested data
    // errors must be identified by their canonical messages.
    //
    // import-into examples:
    // [Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk:
    // encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0
    // at offset 0: Value conversion failed for column 'event_id'. Expected type:
    // bigint, received value: ?. Reason: [types:1292]Truncated incorrect DOUBLE value: '?'.
    //
    // add-index examples:
    // [kv:1062]Duplicate entry '1' for key 't.idx'
    let is_import_data_err = err_msg.contains("ErrEncodeKV")
        && (err_msg.contains("Value conversion failed for column")
            || (err_msg.contains("Check constraint '") && err_msg.contains("' is violated"))
            || err_msg.contains("Table has no partition for value"));
    let is_import_conflict_err = (err_msg.contains("[executor:8167]") && err_msg.contains("Duplicate key conflict found"))
        || (err_msg.contains("ErrFoundDataConflictRecords") && err_msg.contains("found data conflict records"))
        || (err_msg.contains("ErrFoundIndexConflictRecords") && err_msg.contains("found index conflict records"));
    let is_uk_dup_entry_err = err_msg.contains("[kv:1062]") && err_msg.contains("Duplicate entry");
    is_import_data_err || is_import_conflict_err || is_uk_dup_entry_err
}

/// Go `taskErrorCode`: the RFC code of the `*errors.Error` in the chain,
/// empty when there is none.
fn task_error_code(task_err: &(dyn Error + 'static)) -> String {
    let mut cause = Some(task_err);
    while let Some(error) = cause {
        if let Some(normalized_err) = error.downcast_ref::<tidb_terror::TerrorError>() {
            let code = normalized_err.id();
            // errors.Normalize without RFCCodeText or MySQLErrorCode leaves a zero-value
            // numeric code, which RFCCode returns as "0".
            return if code == "0" { String::new() } else { code.into_owned() };
        }
        cause = error.source();
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use tidb_dxf::{TASK_STATE_FAILED, TASK_STATE_REVERTED, TASK_STATE_RUNNING, TASK_STATE_SUCCEED};

    use super::{classify_task_error, task_error_code};
    use crate::converter::TaskError;

    fn err(message: &str) -> TaskError {
        TaskError(message.to_owned())
    }

    /// Go `TestClassifyTaskError`.
    #[test]
    fn test_classify_task_error() {
        let data_err = err("[Lightning:Restore:ErrEncodeKV]Value conversion failed for column 'a'");

        assert!(classify_task_error(&TASK_STATE_SUCCEED, None).is_empty());
        assert!(classify_task_error(&TASK_STATE_FAILED, None).is_empty());
        assert_eq!(classify_task_error(&TASK_STATE_FAILED, Some(&err("fatal"))), TASK_STATE_FAILED.to_string());
        assert_eq!(classify_task_error(&TASK_STATE_REVERTED, Some(&err("cancelled by user"))), "cancelled");
        assert_eq!(classify_task_error(&TASK_STATE_REVERTED, Some(&data_err)), "data-error");
        assert_eq!(classify_task_error(&TASK_STATE_REVERTED, Some(&err("fatal"))), TASK_STATE_FAILED.to_string());
        assert!(classify_task_error(&TASK_STATE_RUNNING, Some(&err("fatal"))).is_empty());
    }

    /// Go `taskErrorCode` over the errors `row2TaskError` reads back: a
    /// stored terror keeps its RFC code; a plain stored error's code "0" and
    /// a non-JSON error give none.
    #[test]
    fn task_error_code_reads_the_stored_rfc_code() {
        let stored = |json: &str| serde_json::from_str::<tidb_terror::TerrorError>(json).unwrap();
        assert_eq!(
            task_error_code(&stored(r#"{"class":21,"code":2,"message":"m","rfccode":"global:2"}"#)),
            "global:2"
        );
        assert_eq!(task_error_code(&stored(r#"{"class":0,"code":0,"message":"m","rfccode":""}"#)), "");
        assert_eq!(task_error_code(&err("not json")), "");
    }
}
