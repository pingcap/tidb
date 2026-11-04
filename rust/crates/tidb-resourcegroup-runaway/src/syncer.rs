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

//! Go `syncer.go`: the readers of `mysql.tidb_runaway_watch` and
//! `mysql.tidb_runaway_watch_done` that keep every node's watch list in step.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tidb_datatype::Datum;
use tidb_proto::resource_manager as rmpb;

use crate::manager::MAX_WATCH_RECORD_CHANNEL_SIZE;
use crate::record::{exec_rc_restricted_sql, null_time, Param, QuarantineRecord};
use crate::{InfoCache, SessionPool};

/// Label values for syncer metrics.
const LBL_SYNC: &str = "sync";
const LBL_WATCH: &str = "watch";
const LBL_WATCH_DONE: &str = "watch_done";

/// Go `watchSyncInterval`: the interval to sync the watch record.
pub(crate) const WATCH_SYNC_INTERVAL: Duration = Duration::from_secs(1);
/// Go `watchSyncOverlap`: how far back `CheckPoint` rewinds from the captured
/// `UpperBound` after a non-empty scan, so the next scan re-inspects the tail
/// of the previous window (commit/read-TS delay and clock skew); `AddWatch`
/// de-duplicates across the overlap in memory.
pub(crate) const WATCH_SYNC_OVERLAP: Duration = Duration::from_secs(3);
/// Go `watchSyncBatchLimit`: the rows one scan returns at most.
const WATCH_SYNC_BATCH_LIMIT: usize = MAX_WATCH_RECORD_CHANNEL_SIZE * 2;
const RUNAWAY_WATCH_TABLE_NAME: &str = "tidb_runaway_watch";
const RUNAWAY_WATCH_DONE_TABLE_NAME: &str = "tidb_runaway_watch_done";
/// Go `systemSchemaCIStr`.
pub(crate) const SYSTEM_SCHEMA: &str = "mysql";
pub(crate) const RUNAWAY_WATCH_FULL_TABLE_NAME: &str = "mysql.tidb_runaway_watch";
pub(crate) const RUNAWAY_WATCH_DONE_FULL_TABLE_NAME: &str = "mysql.tidb_runaway_watch_done";

// Column layout of `mysql.tidb_runaway_watch`, in DDL order. `WATCH_COL_RULE`
// is the column literally named `rule`; it persists `exceed_cause`.
const WATCH_COL_ID: usize = 0;
const WATCH_COL_RESOURCE_GROUP_NAME: usize = 1;
const WATCH_COL_START_TIME: usize = 2;
const WATCH_COL_END_TIME: usize = 3;
const WATCH_COL_WATCH: usize = 4;
const WATCH_COL_WATCH_TEXT: usize = 5;
const WATCH_COL_SOURCE: usize = 6;
const WATCH_COL_ACTION: usize = 7;
const WATCH_COL_SWITCH_GROUP_NAME: usize = 8;
const WATCH_COL_RULE: usize = 9;

// Column layout of `mysql.tidb_runaway_watch_done`: its own `id`, then
// `record_id`, the watch columns and `done_time`.
const WATCH_DONE_COL_RECORD_ID: usize = 1;
const WATCH_DONE_COL_RESOURCE_GROUP_NAME: usize = 2;
const WATCH_DONE_COL_START_TIME: usize = 3;
const WATCH_DONE_COL_END_TIME: usize = 4;
const WATCH_DONE_COL_WATCH: usize = 5;
const WATCH_DONE_COL_WATCH_TEXT: usize = 6;
const WATCH_DONE_COL_SOURCE: usize = 7;
const WATCH_DONE_COL_ACTION: usize = 8;
const WATCH_DONE_COL_SWITCH_GROUP_NAME: usize = 9;
const WATCH_DONE_COL_RULE: usize = 10;
const WATCH_DONE_COL_DONE_TIME: usize = 11;

/// Go `quarantineColumns`: where each record field sits in one table's row.
#[derive(Clone, Copy, Debug)]
pub(crate) struct QuarantineColumns {
    id: usize,
    resource_group_name: usize,
    start_time: usize,
    end_time: usize,
    watch: usize,
    watch_text: usize,
    source: usize,
    action: usize,
    switch_group_name: usize,
    exceed_cause: usize,
}

/// Go `watchRecordColumns`.
const WATCH_RECORD_COLUMNS: QuarantineColumns = QuarantineColumns {
    id: WATCH_COL_ID,
    resource_group_name: WATCH_COL_RESOURCE_GROUP_NAME,
    start_time: WATCH_COL_START_TIME,
    end_time: WATCH_COL_END_TIME,
    watch: WATCH_COL_WATCH,
    watch_text: WATCH_COL_WATCH_TEXT,
    source: WATCH_COL_SOURCE,
    action: WATCH_COL_ACTION,
    switch_group_name: WATCH_COL_SWITCH_GROUP_NAME,
    exceed_cause: WATCH_COL_RULE,
};

/// Go `watchDoneRecordColumns`.
const WATCH_DONE_RECORD_COLUMNS: QuarantineColumns = QuarantineColumns {
    id: WATCH_DONE_COL_RECORD_ID,
    resource_group_name: WATCH_DONE_COL_RESOURCE_GROUP_NAME,
    start_time: WATCH_DONE_COL_START_TIME,
    end_time: WATCH_DONE_COL_END_TIME,
    watch: WATCH_DONE_COL_WATCH,
    watch_text: WATCH_DONE_COL_WATCH_TEXT,
    source: WATCH_DONE_COL_SOURCE,
    action: WATCH_DONE_COL_ACTION,
    switch_group_name: WATCH_DONE_COL_SWITCH_GROUP_NAME,
    exceed_cause: WATCH_DONE_COL_RULE,
};

/// Go `syncer`'s state under its `mu`.
pub(crate) struct SyncState {
    pub(crate) new_watch_reader: SystemTableReader,
    pub(crate) deletion_watch_reader: SystemTableReader,
    pub(crate) last_sync_time: Option<Instant>,
}

/// Go `syncer`.
pub(crate) struct Syncer {
    sys_session_pool: Arc<dyn SessionPool>,
    info_cache: Arc<dyn InfoCache>,
    /// Go `mu` and what it guards.
    pub(crate) mu: Mutex<SyncState>,
}

impl Syncer {
    /// Go `newSyncer`.
    pub(crate) fn new(sys_session_pool: Arc<dyn SessionPool>, info_cache: Arc<dyn InfoCache>) -> Self {
        Self {
            sys_session_pool,
            info_cache,
            mu: Mutex::new(SyncState {
                new_watch_reader: SystemTableReader::new(
                    RUNAWAY_WATCH_FULL_TABLE_NAME,
                    "start_time",
                    WATCH_COL_START_TIME,
                    WATCH_RECORD_COLUMNS,
                ),
                deletion_watch_reader: SystemTableReader::new(
                    RUNAWAY_WATCH_DONE_FULL_TABLE_NAME,
                    "done_time",
                    WATCH_DONE_COL_DONE_TIME,
                    WATCH_DONE_RECORD_COLUMNS,
                ),
                last_sync_time: None,
            }),
        }
    }

    pub(crate) fn sync_interval_observe(seconds: f64) {
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_SYNCER_INTERVAL_SECONDS.with_label_values(&[LBL_SYNC]).observe(seconds);
    }

    pub(crate) fn sync_duration_observe(seconds: f64) {
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_SYNCER_DURATION_SECONDS.with_label_values(&[LBL_SYNC]).observe(seconds);
    }

    pub(crate) fn sync_counter_inc(ok: bool) {
        let result = if ok { tidb_metrics::server::labels::OK } else { tidb_metrics::server::labels::ERROR };
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_SYNCER_TOTAL.with_label_values(&[result, LBL_SYNC]).inc();
    }

    pub(crate) fn checkpoint_gauges_set(state: &SyncState) {
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_SYNCER_CHECKPOINT
            .with_label_values(&[LBL_WATCH])
            .set(checkpoint_gauge_value(state.new_watch_reader.check_point));
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_SYNCER_CHECKPOINT
            .with_label_values(&[LBL_WATCH_DONE])
            .set(checkpoint_gauge_value(state.deletion_watch_reader.check_point));
    }

    /// Go `checkWatchTableExist`.
    pub(crate) fn check_watch_table_exist(&self) -> bool {
        self.info_cache.table_exists(SYSTEM_SCHEMA, RUNAWAY_WATCH_TABLE_NAME)
    }

    /// Go `checkWatchDoneTableExist`.
    pub(crate) fn check_watch_done_table_exist(&self) -> bool {
        self.info_cache.table_exists(SYSTEM_SCHEMA, RUNAWAY_WATCH_DONE_TABLE_NAME)
    }

    /// Go `getWatchRecordByID`; the caller holds `mu`.
    pub(crate) fn get_watch_record_by_id(&self, id: i64) -> Result<Vec<QuarantineRecord>, String> {
        let sql = format!("select * from {RUNAWAY_WATCH_FULL_TABLE_NAME} where id = %?");
        self.read_quarantine_records(WATCH_RECORD_COLUMNS, &sql, &[Param::Int(id)])
    }

    /// Go `getWatchRecordByGroup`; the caller holds `mu`.
    pub(crate) fn get_watch_record_by_group(&self, group_name: &str) -> Result<Vec<QuarantineRecord>, String> {
        let sql = format!("select * from {RUNAWAY_WATCH_FULL_TABLE_NAME} where resource_group_name = %?");
        self.read_quarantine_records(WATCH_RECORD_COLUMNS, &sql, &[Param::Str(group_name.to_owned())])
    }

    /// Go `scanNewRecordsInRange`: one time-windowed scan over the reader's
    /// table in `[CheckPoint, UpperBound)`, advancing `CheckPoint`. A full
    /// batch continues from its last row's key time (or, when every row
    /// shares one timestamp, falls back to the partial-batch advance); a
    /// partial batch rewinds `UpperBound` by the overlap.
    pub(crate) fn scan_new_records_in_range(
        &self,
        reader: &mut SystemTableReader,
    ) -> Result<Vec<QuarantineRecord>, String> {
        // Capture UpperBound before reading so the checkpoint advances from it.
        reader.upper_bound = Utc::now();
        let (sql, params) = reader.gen_select_stmt();
        let rows = exec_rc_restricted_sql(self.sys_session_pool.as_ref(), &sql, &params)?;
        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            let Some(record) = decode_quarantine_record(row, reader.record_columns) else {
                continue;
            };
            if let Some(time) = row.get(reader.key_col_idx).and_then(go_time_utc) {
                reader.last_scan_key_time = time;
            }
            records.push(record);
        }
        let overlap = chrono::Duration::from_std(WATCH_SYNC_OVERLAP).expect("overlap fits");
        if records.len() >= WATCH_SYNC_BATCH_LIMIT {
            reader.check_point = if reader.last_scan_key_time > reader.check_point {
                reader.last_scan_key_time
            } else {
                reader.upper_bound - overlap
            };
        } else if !records.is_empty() {
            reader.check_point = reader.upper_bound - overlap;
        }
        Ok(records)
    }

    /// Go `readQuarantineRecords`: a point query, which leaves the scan
    /// state alone.
    fn read_quarantine_records(
        &self,
        columns: QuarantineColumns,
        sql: &str,
        params: &[Param],
    ) -> Result<Vec<QuarantineRecord>, String> {
        let rows = exec_rc_restricted_sql(self.sys_session_pool.as_ref(), sql, params)?;
        Ok(rows.iter().filter_map(|row| decode_quarantine_record(row, columns)).collect())
    }
}

/// Go `checkpointGaugeValue`: a checkpoint in Unix milliseconds, the zero
/// time as 0.
fn checkpoint_gauge_value(checkpoint: DateTime<Utc>) -> f64 {
    if checkpoint == null_time() {
        return 0.0;
    }
    checkpoint.timestamp_millis() as f64
}

/// Go `r.GetTime(i).GoTime(time.UTC)`.
fn go_time_utc(datum: &Datum) -> Option<DateTime<Utc>> {
    match datum {
        Datum::Time(time) => time.go_time(&Utc).ok(),
        _ => None,
    }
}

fn datum_i64(datum: Option<&Datum>) -> i64 {
    match datum {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

fn datum_string(datum: Option<&Datum>) -> String {
    match datum {
        Some(Datum::Null) | None => String::new(),
        Some(datum) => datum.sql_string().unwrap_or_default(),
    }
}

/// Go `decodeQuarantineRecord`: `None` when `start_time` or a non-NULL
/// `end_time` fails to convert.
fn decode_quarantine_record(row: &[Datum], cols: QuarantineColumns) -> Option<QuarantineRecord> {
    let start_time = go_time_utc(row.get(cols.start_time)?)?;
    let end_time = match row.get(cols.end_time) {
        Some(Datum::Null) | None => null_time(),
        Some(datum) => go_time_utc(datum)?,
    };
    Some(QuarantineRecord {
        id: datum_i64(row.get(cols.id)),
        resource_group_name: datum_string(row.get(cols.resource_group_name)),
        start_time,
        end_time,
        watch: rmpb::RunawayWatchType::try_from(datum_i64(row.get(cols.watch)) as i32)
            .unwrap_or(rmpb::RunawayWatchType::NoneWatch),
        watch_text: datum_string(row.get(cols.watch_text)),
        source: datum_string(row.get(cols.source)),
        action: rmpb::RunawayAction::try_from(datum_i64(row.get(cols.action)) as i32)
            .unwrap_or(rmpb::RunawayAction::NoneAction),
        switch_group_name: datum_string(row.get(cols.switch_group_name)),
        exceed_cause: datum_string(row.get(cols.exceed_cause)),
    })
}

/// Go `systemTableReader`: reads `tidb_runaway_watch` or
/// `tidb_runaway_watch_done` over the half-open `[CheckPoint, UpperBound)`
/// window.
pub(crate) struct SystemTableReader {
    /// The key column's index in `SELECT *`, for pagination.
    pub(crate) key_col_idx: usize,
    pub(crate) record_columns: QuarantineColumns,
    pub(crate) check_point: DateTime<Utc>,
    pub(crate) upper_bound: DateTime<Utc>,
    select_window_sql: String,
    /// The key time of the last valid row the most recent scan produced.
    last_scan_key_time: DateTime<Utc>,
}

impl SystemTableReader {
    /// Go `newSystemTableReader`.
    fn new(table_name: &'static str, key_col: &'static str, key_col_idx: usize, cols: QuarantineColumns) -> Self {
        Self {
            key_col_idx,
            record_columns: cols,
            check_point: null_time(),
            upper_bound: null_time(),
            select_window_sql: format!(
                "select * from {table_name} where {key_col} >= %? and {key_col} < %? order by {key_col} limit %?"
            ),
            last_scan_key_time: null_time(),
        }
    }

    /// Go `genSelectStmt`.
    fn gen_select_stmt(&self) -> (String, Vec<Param>) {
        (
            self.select_window_sql.clone(),
            vec![
                Param::Time(self.check_point),
                Param::Time(self.upper_bound),
                Param::Int(WATCH_SYNC_BATCH_LIMIT as i64),
            ],
        )
    }
}
