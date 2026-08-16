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

//! SEED of Go `pkg/ttl/cache/ttlstatus.go`: the `mysql.tidb_ttl_table_status`
//! statement, its row decoder, and the cache built from it.
//!
//! `JobStatus`, the `SELECT`, `SelectFromTTLTableStatusWithID`, `TableStatus`
//! and every branch of `RowToTableStatus` come across. What does not is named
//! at its definition site: [`TableStatusCache::update`] needs a live
//! [`Session`](crate::session::Session) to run the query, and the `GoTime`
//! conversion each datetime column goes through has no reachable counterpart
//! (see the package header).

use std::collections::HashMap;
use std::time::Duration;

use tidb_datatype::Time;

use super::base::BaseCache;
use super::Result;
use crate::session::{ResultRow, Session};

/// Go `JobStatus`: the current status of a job.
///
/// Go's `type JobStatus string` accepts whatever the column holds, so the
/// string is preserved rather than folded into a closed enum.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobStatus(pub String);

impl JobStatus {
    /// Go `JobStatusWaiting`: the job hasn't started.
    pub const WAITING: &'static str = "waiting";
    /// Go `JobStatusRunning`: this job is running.
    pub const RUNNING: &'static str = "running";
    /// Go `JobStatusCancelling`: being canceled, but not canceled yet.
    pub const CANCELLING: &'static str = "cancelling";
    /// Go `JobStatusCancelled`: canceled successfully.
    pub const CANCELLED: &'static str = "cancelled";
    /// Go `JobStatusTimeout`: this job has timeout.
    pub const TIMEOUT: &'static str = "timeout";
    /// Go `JobStatusFinished`: job has been finished.
    pub const FINISHED: &'static str = "finished";
}

/// Go's unexported `selectFromTTLTableStatus`.
pub const SELECT_FROM_TTL_TABLE_STATUS: &str = "SELECT LOW_PRIORITY table_id,parent_table_id,\
table_statistics,last_job_id,last_job_start_time,last_job_finish_time,last_job_ttl_expire,\
last_job_summary,current_job_id,current_job_owner_id,current_job_owner_addr,\
current_job_owner_hb_time,current_job_start_time,current_job_ttl_expire,current_job_state,\
current_job_status,current_job_status_update_time FROM mysql.tidb_ttl_table_status";

/// Go `SelectFromTTLTableStatusWithID`.
#[must_use]
pub fn select_from_ttl_table_status_with_id(table_id: i64) -> (String, Vec<i64>) {
    (
        format!("{SELECT_FROM_TTL_TABLE_STATUS} WHERE table_id = %?"),
        vec![table_id],
    )
}

/// Go `TableStatus`: the row shape of `mysql.tidb_ttl_table_status`.
///
/// Every `time.Time` field is the MySQL datetime the column holds; see the
/// boundary named on [`row_to_table_status`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableStatus {
    /// Go `TableID`.
    pub table_id: i64,
    /// Go `ParentTableID`.
    pub parent_table_id: i64,
    /// Go `TableStatistics`.
    pub table_statistics: String,
    /// Go `LastJobID`.
    pub last_job_id: String,
    /// Go `LastJobStartTime`.
    pub last_job_start_time: Option<Time>,
    /// Go `LastJobFinishTime`.
    pub last_job_finish_time: Option<Time>,
    /// Go `LastJobTTLExpire`.
    pub last_job_ttl_expire: Option<Time>,
    /// Go `LastJobSummary`.
    pub last_job_summary: String,
    /// Go `CurrentJobID`.
    pub current_job_id: String,
    /// Go `CurrentJobOwnerID`.
    pub current_job_owner_id: String,
    /// Go `CurrentJobOwnerAddr`.
    pub current_job_owner_addr: String,
    /// Go `CurrentJobOwnerHBTime`.
    pub current_job_owner_hb_time: Option<Time>,
    /// Go `CurrentJobStartTime`.
    pub current_job_start_time: Option<Time>,
    /// Go `CurrentJobTTLExpire`.
    pub current_job_ttl_expire: Option<Time>,
    /// Go `CurrentJobState`.
    pub current_job_state: String,
    /// Go `CurrentJobStatus`.
    pub current_job_status: JobStatus,
    /// Go `CurrentJobStatusUpdateTime`.
    pub current_job_status_update_time: Option<Time>,
}

/// Go `RowToTableStatus`.
///
/// `// boundary:` Go's `timeZone *time.Location` parameter exists only to run
/// `row.GetTime(i).GoTime(timeZone)` on the seven datetime columns. That needs
/// a Go-instant type this crate cannot reach (see the package header), so the
/// MySQL datetimes are carried through unconverted and the error path `GoTime`
/// can raise has no counterpart. All seventeen column reads, their NULL guards
/// and the empty-status default come across exactly.
pub fn row_to_table_status<R: ResultRow>(row: &R) -> Result<TableStatus> {
    let mut status = TableStatus {
        table_id: row.get_int64(0),
        ..TableStatus::default()
    };

    if !row.is_null(1) {
        status.parent_table_id = row.get_int64(1);
    }
    if !row.is_null(2) {
        status.table_statistics = row.get_string(2);
    }
    if !row.is_null(3) {
        status.last_job_id = row.get_string(3);
    }
    if !row.is_null(4) {
        status.last_job_start_time = Some(row.get_time(4));
    }
    if !row.is_null(5) {
        status.last_job_finish_time = Some(row.get_time(5));
    }
    if !row.is_null(6) {
        status.last_job_ttl_expire = Some(row.get_time(6));
    }
    if !row.is_null(7) {
        status.last_job_summary = row.get_string(7);
    }
    if !row.is_null(8) {
        status.current_job_id = row.get_string(8);
    }
    if !row.is_null(9) {
        status.current_job_owner_id = row.get_string(9);
    }
    if !row.is_null(10) {
        status.current_job_owner_addr = row.get_string(10);
    }
    if !row.is_null(11) {
        status.current_job_owner_hb_time = Some(row.get_time(11));
    }
    if !row.is_null(12) {
        status.current_job_start_time = Some(row.get_time(12));
    }
    if !row.is_null(13) {
        status.current_job_ttl_expire = Some(row.get_time(13));
    }
    if !row.is_null(14) {
        status.current_job_state = row.get_string(14);
    }
    if !row.is_null(15) {
        let mut job_status = row.get_string(15);
        if job_status.is_empty() {
            job_status = JobStatus::WAITING.to_owned();
        }
        status.current_job_status = JobStatus(job_status);
    }
    if !row.is_null(16) {
        status.current_job_status_update_time = Some(row.get_time(16));
    }

    Ok(status)
}

/// Go `TableStatusCache`: a map from physical table id to the table status.
#[derive(Debug, Clone)]
pub struct TableStatusCache {
    base: BaseCache,
    /// Go `Tables`.
    pub tables: HashMap<i64, TableStatus>,
}

impl TableStatusCache {
    /// Go `NewTableStatusCache`.
    #[must_use]
    pub fn new(update_interval: Duration) -> Self {
        Self {
            base: BaseCache::new(update_interval),
            tables: HashMap::new(),
        }
    }

    /// Go's embedded `baseCache.ShouldUpdate`.
    #[must_use]
    pub fn should_update(&self) -> bool {
        self.base.should_update()
    }

    /// Go's embedded `baseCache.SetInterval`.
    pub fn set_interval(&mut self, interval: Duration) {
        self.base.set_interval(interval);
    }

    /// Go's embedded `baseCache.GetInterval`.
    #[must_use]
    pub fn get_interval(&self) -> Duration {
        self.base.get_interval()
    }

    /// Go `(*TableStatusCache).Update`.
    ///
    /// Go passes the session location into `RowToTableStatus`; that argument's
    /// only use is the `GoTime` conversion named on [`row_to_table_status`], so
    /// nothing here consumes it.
    pub fn update<S: Session>(&mut self, se: &S) -> Result<()> {
        let rows = se
            .execute_sql(SELECT_FROM_TTL_TABLE_STATUS, &[])
            .map_err(|err| super::error(err.to_string()))?;

        let mut new_tables = HashMap::with_capacity(rows.len());
        for row in &rows {
            let status = row_to_table_status(row)?;
            new_tables.insert(status.table_id, status);
        }
        self.tables = new_tables;
        self.base.mark_updated();
        Ok(())
    }
}
