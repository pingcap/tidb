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

//! SEED of Go `pkg/ttl/cache/task.go`: the `mysql.tidb_ttl_task` statements and
//! the row decoder behind them.
//!
//! Everything except two pieces comes across byte-for-byte — the four SQL
//! statements, their argument lists, `TaskStatus`, `TTLTask`, `TTLTaskState`
//! and every NULL/default branch of `RowToTTLTask`. The two exceptions are
//! named at their definition sites: [`insert_into_ttl_task`] cannot run
//! `codec.EncodeKey` over the scan-range datums, and [`TTLTaskState`] cannot be
//! decoded from its JSON column; both need dependencies this crate may not add
//! (see the package header).

use tidb_datatype::Time;

use super::Result;
use crate::session::ResultRow;

/// Go's unexported `selectFromTTLTask`.
pub const SELECT_FROM_TTL_TASK: &str = "SELECT LOW_PRIORITY
\tjob_id,
\ttable_id,
\tscan_id,
\tscan_range_start,
\tscan_range_end,
\texpire_time,
\towner_id,
\towner_addr,
\towner_hb_time,
\tstatus,
\tstatus_update_time,
\tstate,
\tcreated_time FROM mysql.tidb_ttl_task";

/// Go's unexported `insertIntoTTLTask`.
pub const INSERT_INTO_TTL_TASK: &str = "INSERT LOW_PRIORITY INTO mysql.tidb_ttl_task SET
\tjob_id = %?,
\ttable_id = %?,
\tscan_id = %?,
\tscan_range_start = %?,
\tscan_range_end = %?,
\texpire_time = %?,
\tcreated_time = %?";

/// One element of Go's `[]any` statement-argument slice.
///
/// Go passes `any` and lets the executor's `%?` expansion sort out the type.
/// The variants below are exactly the types these four builders produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlArg {
    /// A Go `string` argument.
    Str(String),
    /// A Go `int64` argument.
    Int(i64),
    /// A Go `[]byte` argument.
    Bytes(Vec<u8>),
    /// A Go `time.Time` argument, carried as the datetime it renders to.
    ///
    /// `// boundary:` Go's `time.Time`. No Go-instant transcreation is
    /// reachable from this crate (see the package header), so the caller
    /// supplies the value the executor would bind.
    Time(Time),
}

/// Go `SelectFromTTLTaskWithJobID`.
#[must_use]
pub fn select_from_ttl_task_with_job_id(job_id: &str) -> (String, Vec<SqlArg>) {
    (
        format!("{SELECT_FROM_TTL_TASK} WHERE job_id = %?"),
        vec![SqlArg::Str(job_id.to_owned())],
    )
}

/// Go `SelectFromTTLTaskWithID`.
#[must_use]
pub fn select_from_ttl_task_with_id(job_id: &str, scan_id: i64) -> (String, Vec<SqlArg>) {
    (
        format!("{SELECT_FROM_TTL_TASK} WHERE job_id = %? AND scan_id = %?"),
        vec![SqlArg::Str(job_id.to_owned()), SqlArg::Int(scan_id)],
    )
}

/// Go `PeekWaitingTTLTask`.
///
/// `// boundary:` Go takes `hbExpire time.Time` and binds
/// `hbExpire.Format(time.DateTime)`. With no reachable Go-instant type the
/// already-rendered `"2006-01-02 15:04:05"` literal is the parameter, which is
/// exactly the value Go binds.
#[must_use]
pub fn peek_waiting_ttl_task(hb_expire_date_time: &str) -> (String, Vec<SqlArg>) {
    (
        format!(
            "{SELECT_FROM_TTL_TASK} WHERE status = 'waiting' \
             OR (owner_hb_time < %? AND status = 'running') ORDER BY created_time ASC"
        ),
        vec![SqlArg::Str(hb_expire_date_time.to_owned())],
    )
}

/// Go `InsertIntoTTLTask`.
///
/// `// boundary:` Go first runs `codec.EncodeKey(loc, []byte{}, scanRange...)`
/// over both bound datum slices. That memcomparable encoder lives in
/// `tidb-codec`, which this crate may not depend on (see the package header),
/// so the encoded bounds are parameters here and the `loc` argument — whose
/// only use is that encoding — has nothing left to do.
#[must_use]
pub fn insert_into_ttl_task(
    job_id: &str,
    table_id: i64,
    scan_id: i32,
    encoded_range_start: Vec<u8>,
    encoded_range_end: Vec<u8>,
    expire_time: Time,
    created_time: Time,
) -> (&'static str, Vec<SqlArg>) {
    (
        INSERT_INTO_TTL_TASK,
        vec![
            SqlArg::Str(job_id.to_owned()),
            SqlArg::Int(table_id),
            SqlArg::Int(i64::from(scan_id)),
            SqlArg::Bytes(encoded_range_start),
            SqlArg::Bytes(encoded_range_end),
            SqlArg::Time(expire_time),
            SqlArg::Time(created_time),
        ],
    )
}

/// Go `TaskStatus`: the current status of a task.
///
/// Go's `type TaskStatus string` accepts whatever the column holds, so the
/// string is preserved rather than folded into a closed enum.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskStatus(pub String);

impl TaskStatus {
    /// Go `TaskStatusWaiting`: the task hasn't started.
    pub const WAITING: &'static str = "waiting";
    /// Go `TaskStatusRunning`: this task is running.
    pub const RUNNING: &'static str = "running";
    /// Go `TaskStatusFinished`: this task has finished.
    pub const FINISHED: &'static str = "finished";
}

/// Go `TTLTaskState`: the internal states of the ttl task.
///
/// `// boundary:` Go reaches this struct only through
/// `json.Unmarshal([]byte(stateStr), state)`. `serde_json` is not on this
/// crate's dependency list and no edge may be added (see the package header),
/// so [`TTLTask::state`] carries the raw column text and this struct is the
/// shape a decoder must fill.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TTLTaskState {
    /// Go `TotalRows`, JSON `total_rows`.
    pub total_rows: u64,
    /// Go `SuccessRows`, JSON `success_rows`.
    pub success_rows: u64,
    /// Go `ErrorRows`, JSON `error_rows`.
    pub error_rows: u64,
    /// Go `ScanTaskErr`, JSON `scan_task_err`.
    pub scan_task_err: String,
    /// Go `PreviousOwner`, JSON `prev_owner,omitempty`.
    ///
    /// When non-empty, this task is resigned from another owner.
    pub previous_owner: String,
}

/// Go `TTLTask`: a row recorded in `mysql.tidb_ttl_task`.
///
/// Every `time.Time` field is the MySQL datetime the column holds. Go follows
/// each `row.GetTime(i)` with `.GoTime(timeZone)`; that conversion is the
/// boundary named on [`row_to_ttl_task`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TTLTask {
    /// Go `JobID`.
    pub job_id: String,
    /// Go `TableID`.
    pub table_id: i64,
    /// Go `ScanID`.
    pub scan_id: i64,
    /// Go `ScanRangeStart`, still memcomparable-encoded.
    ///
    /// `// boundary:` Go decodes it with `codec.Decode`; that decoder is not
    /// reachable here (see the package header), so the raw column bytes stand.
    pub scan_range_start: Option<Vec<u8>>,
    /// Go `ScanRangeEnd`, still memcomparable-encoded. Same boundary as above.
    pub scan_range_end: Option<Vec<u8>>,
    /// Go `ExpireTime`.
    pub expire_time: Option<Time>,
    /// Go `OwnerID`.
    pub owner_id: String,
    /// Go `OwnerAddr`.
    pub owner_addr: String,
    /// Go `OwnerHBTime`.
    pub owner_hb_time: Option<Time>,
    /// Go `Status`.
    pub status: TaskStatus,
    /// Go `StatusUpdateTime`.
    pub status_update_time: Option<Time>,
    /// Go `State`, as the raw JSON text of the column.
    pub state: Option<String>,
    /// Go `CreatedTime`.
    pub created_time: Option<Time>,
}

/// Go `RowToTTLTask`.
///
/// `// boundary:` Go's `timeZone *time.Location` parameter exists only to run
/// `row.GetTime(i).GoTime(timeZone)` on the five datetime columns. That
/// conversion needs a Go-instant type this crate cannot reach (see the package
/// header), so the MySQL datetimes are carried through unconverted and the
/// parameter, along with the error path `GoTime` can raise, has no counterpart.
/// Every other branch — the two NULL-guarded range columns, the empty-status
/// default, and the per-column NULL checks — comes across exactly.
pub fn row_to_ttl_task<R: ResultRow>(row: &R) -> Result<TTLTask> {
    let mut task = TTLTask {
        job_id: row.get_string(0),
        table_id: row.get_int64(1),
        scan_id: row.get_int64(2),
        ..TTLTask::default()
    };

    if !row.is_null(3) {
        let buf = row.get_bytes(3);
        // it's still posibble to be empty even this column is not NULL
        if !buf.is_empty() {
            task.scan_range_start = Some(buf);
        }
    }
    if !row.is_null(4) {
        let buf = row.get_bytes(4);
        // it's still posibble to be empty even this column is not NULL
        if !buf.is_empty() {
            task.scan_range_end = Some(buf);
        }
    }

    task.expire_time = Some(row.get_time(5));

    if !row.is_null(6) {
        task.owner_id = row.get_string(6);
    }
    if !row.is_null(7) {
        task.owner_addr = row.get_string(7);
    }
    if !row.is_null(8) {
        task.owner_hb_time = Some(row.get_time(8));
    }
    if !row.is_null(9) {
        let mut status = row.get_string(9);
        if status.is_empty() {
            status = TaskStatus::WAITING.to_owned();
        }
        task.status = TaskStatus(status);
    }
    if !row.is_null(10) {
        task.status_update_time = Some(row.get_time(10));
    }
    if !row.is_null(11) {
        // Go decodes the text into a `*TTLTaskState` here and propagates the
        // decode error; with the decoder behind a boundary the raw text is what
        // this port can carry, and validation moves to whoever decodes it.
        task.state = Some(row.get_string(11));
    }

    task.created_time = Some(row.get_time(12));

    Ok(task)
}
