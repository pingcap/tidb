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

//! Go `record.go`: the runaway and quarantine records, the statements that
//! write them, and the manual watch add/remove paths.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tidb_datatype::Datum;
use tidb_proto::resource_manager as rmpb;
use tidb_sqlescape::SqlArg;

use crate::manager::Manager;
use crate::syncer::{RUNAWAY_WATCH_DONE_FULL_TABLE_NAME, RUNAWAY_WATCH_FULL_TABLE_NAME, SYSTEM_SCHEMA};
use crate::{SessionPool, SqlSession};

const MAX_ID_RETRIES: u64 = 3;

/// Go `NullTime`: the zero `time.Time`, `0001-01-01 00:00:00 UTC`.
pub fn null_time() -> DateTime<Utc> {
    DateTime::<Utc>::from_naive_utc_and_offset(
        chrono::NaiveDate::from_ymd_opt(1, 1, 1).expect("year 1").and_hms_opt(0, 0, 0).expect("midnight"),
        Utc,
    )
}

/// One `%?` parameter, owned: what Go passes as `[]any`.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    /// Go `nil`.
    Null,
    /// Any signed integer, including Go's named `rmpb` enums.
    Int(i64),
    /// Any unsigned integer.
    UInt(u64),
    /// A string.
    Str(String),
    /// A `time.Time`; [`null_time`] is Go's zero time.
    Time(DateTime<Utc>),
}

impl Param {
    /// The escaper's view of this parameter.
    pub fn as_arg(&self) -> SqlArg<'_> {
        match self {
            Self::Null => SqlArg::Null,
            Self::Int(value) => SqlArg::Signed(*value),
            Self::UInt(value) => SqlArg::Unsigned(*value),
            Self::Str(value) => SqlArg::String(value.as_bytes()),
            Self::Time(value) if *value == null_time() => SqlArg::Time(None),
            Self::Time(value) => SqlArg::Time(Some(value.naive_utc())),
        }
    }
}

/// The escaper's view of a parameter list.
pub fn args(params: &[Param]) -> Vec<SqlArg<'_>> {
    params.iter().map(Param::as_arg).collect()
}

/// Go `Record`: one row of `mysql.tidb_runaway_queries`.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub resource_group_name: String,
    pub start_time: DateTime<Utc>,
    pub match_type: String,
    pub action: String,
    pub sample_text: String,
    pub sql_digest: String,
    pub plan_digest: String,
    pub source: String,
    pub exceed_cause: String,
    /// Go `Repeats`: how many times the record repeated before it was
    /// flushed; only `resource_group_name`, `sql_digest`, `plan_digest` and
    /// `match_type` are compared. Default 1.
    pub repeats: i64,
}

/// Go `recordKey`: the composite key of a record in `tidb_runaway_queries`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecordKey {
    pub resource_group_name: String,
    pub sql_digest: String,
    pub plan_digest: String,
    pub match_type: String,
}

/// Go `genRunawayQueriesStmt`.
pub fn gen_runaway_queries_stmt<'a>(records: impl IntoIterator<Item = &'a Record>) -> (String, Vec<Param>) {
    let mut builder = String::from(
        "INSERT INTO mysql.tidb_runaway_queries (resource_group_name, start_time, match_type, action, sample_sql, sql_digest, plan_digest, tidb_server, rule, repeats) VALUES ",
    );
    let mut params = Vec::new();
    for (index, r) in records.into_iter().enumerate() {
        if index > 0 {
            builder.push(',');
        }
        builder.push_str("(%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)");
        params.extend([
            Param::Str(r.resource_group_name.clone()),
            Param::Time(r.start_time),
            Param::Str(r.match_type.clone()),
            Param::Str(r.action.clone()),
            Param::Str(r.sample_text.clone()),
            Param::Str(r.sql_digest.clone()),
            Param::Str(r.plan_digest.clone()),
            Param::Str(r.source.clone()),
            Param::Str(r.exceed_cause.clone()),
            Param::Int(r.repeats),
        ]);
    }
    (builder, params)
}

/// Go `QuarantineRecord`: one row of `mysql.tidb_runaway_watch`.
#[derive(Clone, Debug, PartialEq)]
pub struct QuarantineRecord {
    pub id: i64,
    pub resource_group_name: String,
    /// In UTC.
    pub start_time: DateTime<Utc>,
    /// In UTC; [`null_time`] is unlimited.
    pub end_time: DateTime<Utc>,
    pub watch: rmpb::RunawayWatchType,
    pub watch_text: String,
    pub source: String,
    pub exceed_cause: String,
    pub action: rmpb::RunawayAction,
    pub switch_group_name: String,
}

impl Default for QuarantineRecord {
    fn default() -> Self {
        Self {
            id: 0,
            resource_group_name: String::new(),
            start_time: null_time(),
            end_time: null_time(),
            watch: rmpb::RunawayWatchType::NoneWatch,
            watch_text: String::new(),
            source: String::new(),
            exceed_cause: String::new(),
            action: rmpb::RunawayAction::NoneAction,
            switch_group_name: String::new(),
        }
    }
}

impl QuarantineRecord {
    /// Go `getRecordKey`: the key in the watch list.
    pub fn get_record_key(&self) -> String {
        format!("{}/{}", self.resource_group_name, self.watch_text)
    }

    /// Go `getSwitchGroupName`.
    pub fn get_switch_group_name(&self) -> &str {
        if self.action == rmpb::RunawayAction::SwitchGroup {
            &self.switch_group_name
        } else {
            ""
        }
    }

    /// Go `GetExceedCause`.
    pub fn get_exceed_cause(&self) -> &str {
        &self.exceed_cause
    }

    /// Go `GetActionString`.
    pub fn get_action_string(&self) -> String {
        if self.action == rmpb::RunawayAction::SwitchGroup {
            return format!("{}({})", self.action.as_str_name(), self.switch_group_name);
        }
        self.action.as_str_name().to_owned()
    }

    fn end_time_param(&self) -> Param {
        if self.end_time == null_time() {
            Param::Null
        } else {
            Param::Time(self.end_time)
        }
    }

    /// The nine watch columns after `id`, in DDL order.
    fn watch_params(&self) -> [Param; 9] {
        [
            Param::Str(self.resource_group_name.clone()),
            Param::Time(self.start_time),
            self.end_time_param(),
            Param::Int(self.watch as i64),
            Param::Str(self.watch_text.clone()),
            Param::Str(self.source.clone()),
            Param::Int(self.action as i64),
            Param::Str(self.get_switch_group_name().to_owned()),
            Param::Str(self.exceed_cause.clone()),
        ]
    }

    /// Go `genInsertionStmt`.
    pub fn gen_insertion_stmt(&self) -> (String, Vec<Param>) {
        let mut builder = String::new();
        write_insert(&mut builder, RUNAWAY_WATCH_FULL_TABLE_NAME);
        builder.push_str("(null, %?, %?, %?, %?, %?, %?, %?, %?, %?)");
        (builder, self.watch_params().to_vec())
    }

    /// Go `genInsertionDoneStmt`.
    pub fn gen_insertion_done_stmt(&self) -> (String, Vec<Param>) {
        let mut builder = String::new();
        write_insert(&mut builder, RUNAWAY_WATCH_DONE_FULL_TABLE_NAME);
        builder.push_str("(null, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)");
        let mut params = vec![Param::Int(self.id)];
        params.extend(self.watch_params());
        params.push(Param::Time(Utc::now()));
        (builder, params)
    }

    /// Go `genDeletionStmt`.
    pub fn gen_deletion_stmt(&self) -> (String, Vec<Param>) {
        (format!("delete from {RUNAWAY_WATCH_FULL_TABLE_NAME} where id = %?"), vec![Param::Int(self.id)])
    }
}

fn write_insert(builder: &mut String, table_name: &str) {
    builder.push_str("insert into ");
    builder.push_str(table_name);
    builder.push_str(" VALUES ");
}

/// Go `genBatchInsertWatchStmt`.
pub fn gen_batch_insert_watch_stmt<'a>(
    records: impl IntoIterator<Item = &'a QuarantineRecord>,
) -> (String, Vec<Param>) {
    let mut builder = String::new();
    write_insert(&mut builder, RUNAWAY_WATCH_FULL_TABLE_NAME);
    let mut params = Vec::new();
    for (index, r) in records.into_iter().enumerate() {
        if index > 0 {
            builder.push(',');
        }
        builder.push_str("(null, %?, %?, %?, %?, %?, %?, %?, %?, %?)");
        params.extend(r.watch_params());
    }
    (builder, params)
}

/// Go `genBatchDeleteWatchByIDStmt`.
pub fn gen_batch_delete_watch_by_id_stmt(ids: impl IntoIterator<Item = i64>) -> (String, Vec<Param>) {
    let mut builder = format!("delete from {RUNAWAY_WATCH_FULL_TABLE_NAME} where id in (");
    let mut params = Vec::new();
    for (index, id) in ids.into_iter().enumerate() {
        if index > 0 {
            builder.push(',');
        }
        builder.push_str("%?");
        params.push(Param::Int(id));
    }
    builder.push(')');
    (builder, params)
}

impl Manager {
    /// Go `deleteExpiredRows`: the DDL owner deletes `tidb_runaway_queries`
    /// rows older than `expired_duration`, in TTL-style scan-then-delete
    /// batches.
    pub(crate) fn delete_expired_rows(&self, expired_duration: Duration) {
        const TABLE_NAME: &str = "tidb_runaway_queries";
        const COL_NAME: &str = "start_time";
        if !self.ddl.is_owner() {
            return;
        }
        let batch_size = crate::manager::RUNAWAY_RECORD_GC_SELECT_BATCH_SIZE;
        let delete_size = crate::manager::RUNAWAY_RECORD_GC_BATCH_SIZE;
        let expired_time = Utc::now() - chrono::Duration::from_std(expired_duration).unwrap_or(chrono::Duration::MAX);
        let log_error = |message: &str, error: String| {
            tidb_logutil::bg_logger().error(
                message,
                &[
                    tidb_log::Field::new("table", tidb_log::Value::Str(TABLE_NAME.to_owned())),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                ],
            );
        };
        let Some(tb_info) = self.info_cache.table_by_name(SYSTEM_SCHEMA, TABLE_NAME) else {
            // Go: a missing table (`ErrTableNotExists`) or no schema returns.
            return;
        };
        let col = tb_info.read().find_public_column_by_name(COL_NAME).map(|col| col.read().clone());
        let Some(col) = col else {
            tidb_logutil::bg_logger().error(
                "time column is not public in table",
                &[
                    tidb_log::Field::new("table", tidb_log::Value::Str(TABLE_NAME.to_owned())),
                    tidb_log::Field::new("column", tidb_log::Value::Str(COL_NAME.to_owned())),
                ],
            );
            return;
        };
        let tb = match tidb_ttl::cache::table::new_base_physical_table(
            tidb_ast::CiString::new(SYSTEM_SCHEMA),
            &tb_info,
            tidb_ast::CiString::new(""),
            Some(col),
        ) {
            Ok(tb) => tb,
            Err(error) => return log_error("delete system table failed", error.to_string()),
        };
        let expire_unix = expired_time.timestamp();
        let mut generator = match tidb_ttl::sql_builder::ScanQueryGenerator::new(&tb, expire_unix, &[], &[]) {
            Ok(generator) => generator,
            Err(error) => return log_error("delete system table failed", error.to_string()),
        };
        let mut left_rows: Vec<Vec<Datum>> = Vec::new();
        loop {
            let sql = match generator.next_sql(&left_rows, batch_size as i64) {
                Ok(sql) => sql,
                Err(error) => return log_error("delete system table failed", error.to_string()),
            };
            // to remove
            if sql.is_empty() {
                return;
            }
            let rows = match exec_rc_restricted_sql(self.sys_session_pool.as_ref(), &sql, &[]) {
                Ok(rows) => rows,
                Err(error) => return log_error("delete system table failed", error),
            };
            if rows.is_empty() {
                return;
            }
            tidb_logutil::bg_logger().info(
                "start to delete the expired rows",
                &[
                    tidb_log::Field::new("rows", tidb_log::Value::I64(rows.len() as i64)),
                    tidb_log::Field::new("batch-size", tidb_log::Value::I64(batch_size as i64)),
                    tidb_log::Field::new("delete-size", tidb_log::Value::I64(delete_size as i64)),
                ],
            );
            left_rows = rows;
            for batch in left_rows.chunks(delete_size) {
                let sql = match tidb_ttl::sql_builder::build_delete_sql(&tb, batch, expire_unix) {
                    Ok(sql) => sql,
                    Err(error) => {
                        tidb_logutil::bg_logger().error(
                            "build delete SQL failed when deleting system table",
                            &[
                                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                                tidb_log::Field::new(
                                    "table",
                                    tidb_log::Value::Str(format!("{SYSTEM_SCHEMA}.{TABLE_NAME}")),
                                ),
                            ],
                        );
                        return;
                    }
                };
                if let Err(error) = exec_rc_restricted_sql(self.sys_session_pool.as_ref(), &sql, &[]) {
                    tidb_logutil::bg_logger().error(
                        "delete SQL failed when deleting system table",
                        &[
                            tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                            tidb_log::Field::new("SQL", tidb_log::Value::Str(sql)),
                        ],
                    );
                }
            }
            tidb_logutil::bg_logger().info(
                "deleted expired rows",
                &[
                    tidb_log::Field::new("rows", tidb_log::Value::I64(left_rows.len() as i64)),
                    tidb_log::Field::new("batch-size", tidb_log::Value::I64(batch_size as i64)),
                    tidb_log::Field::new("delete-size", tidb_log::Value::I64(delete_size as i64)),
                ],
            );
        }
    }

    /// Go `AddRunawayWatch`: the manual watch item's insert, in its own
    /// transaction, answering the new row's ID.
    ///
    /// # Errors
    /// Whatever the statements report.
    pub fn add_runaway_watch(&self, record: &QuarantineRecord) -> Result<u64, String> {
        let mut id = 0;
        on_session(self.sys_session_pool.as_ref(), |session| {
            in_transaction(session, |session| {
                let (sql, params) = record.gen_insertion_stmt();
                session.execute_internal(&sql, &args(&params))?;
                let mut error = String::new();
                for retry in 0..MAX_ID_RETRIES {
                    if retry > 0 {
                        if self.exit_wait(Duration::from_millis(retry * 100)) {
                            return Err(error);
                        }
                        tidb_logutil::bg_logger().warn(
                            "failed to get last insert id when adding runaway watch",
                            &[tidb_log::Field::new("error", tidb_log::Value::Str(error.clone()))],
                        );
                    }
                    match session.execute_internal("SELECT LAST_INSERT_ID();", &[]) {
                        Ok(rows) if rows.len() == 1 => {
                            id = datum_u64(rows[0].first());
                            return Ok(());
                        }
                        Ok(rows) => error = format!("unexpected result length: {}", rows.len()),
                        Err(err) => error = err,
                    }
                }
                Err(format!(
                    "An error: {error} occurred while getting the ID of the newly added watch record. Try querying information_schema.runaway_watches later"
                ))
            })
        })?;
        Ok(id)
    }

    /// Go `RemoveRunawayWatch`.
    ///
    /// # Errors
    /// No watch with the ID, or whatever the statements report.
    pub fn remove_runaway_watch(&self, record_id: i64) -> Result<(), String> {
        let _guard = self.runaway_syncer.mu.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let records = self.runaway_syncer.get_watch_record_by_id(record_id)?;
        if records.len() != 1 {
            return Err("no runaway watch with the specific ID".to_owned());
        }
        handle_runaway_watch_done(self.sys_session_pool.as_ref(), &records[0])
    }

    /// Go `RemoveRunawayResourceGroupWatch`.
    ///
    /// # Errors
    /// Whatever reading or moving the records reports.
    pub fn remove_runaway_resource_group_watch(&self, group_name: &str) -> Result<(), String> {
        let _guard = self.runaway_syncer.mu.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let records = self
            .runaway_syncer
            .get_watch_record_by_group(group_name)
            .map_err(|error| format!("get watch records by resource group failed: {error}"))?;
        for record in &records {
            handle_runaway_watch_done(self.sys_session_pool.as_ref(), record)
                .map_err(|error| format!("remove watch for resource group {group_name} failed: {error}"))?;
        }
        Ok(())
    }
}

/// Go's `BEGIN` ... `COMMIT` / `ROLLBACK` around `body` on one session.
fn in_transaction(
    session: &dyn SqlSession,
    body: impl FnOnce(&dyn SqlSession) -> Result<(), String>,
) -> Result<(), String> {
    session.execute_internal("BEGIN", &[])?;
    match body(session) {
        Ok(()) => session.execute_internal("COMMIT", &[]).map(drop),
        Err(error) => {
            if let Err(rollback) = session.execute_internal("ROLLBACK", &[]) {
                tidb_logutil::bg_logger().error(
                    "encountered error",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(rollback))],
                );
            }
            Err(error)
        }
    }
}

/// Go `handleRunawayWatchDone`: the record moves from the watch table to the
/// done table in one transaction.
///
/// # Errors
/// Whatever the statements report.
pub fn handle_runaway_watch_done(pool: &dyn SessionPool, record: &QuarantineRecord) -> Result<(), String> {
    on_session(pool, |session| {
        in_transaction(session, |session| {
            let (sql, params) = record.gen_insertion_done_stmt();
            session.execute_internal(&sql, &args(&params))?;
            let (sql, params) = record.gen_deletion_stmt();
            session.execute_internal(&sql, &args(&params)).map(drop)
        })
    })
}

/// Go `sysSessionPool.Get()` ... `Put(se)` around `body`; a failed `Get` is
/// annotated `get session failed`.
pub(crate) fn on_session<T>(
    pool: &dyn SessionPool,
    body: impl FnOnce(&dyn SqlSession) -> Result<T, String>,
) -> Result<T, String> {
    let mut body = Some(body);
    let mut outcome = None;
    pool.with_session(&mut |session| {
        if let Some(body) = body.take() {
            outcome = Some(body(session));
        }
    })
    .map_err(|error| format!("get session failed: {error}"))?;
    outcome.unwrap_or_else(|| Err("get session failed".to_owned()))
}

/// Go `ExecRCRestrictedSQL`: a restricted statement of the resource-control
/// internal source on a pooled session.
///
/// # Errors
/// Whatever the statement reports.
pub fn exec_rc_restricted_sql(pool: &dyn SessionPool, sql: &str, params: &[Param]) -> Result<Vec<Vec<Datum>>, String> {
    on_session(pool, |session| session.exec_restricted_sql(sql, &args(params)))
}

/// Go `chunk.Row.GetUint64(0)` of `LAST_INSERT_ID()`.
fn datum_u64(datum: Option<&Datum>) -> u64 {
    match datum {
        Some(Datum::UInt(value)) => *value,
        Some(Datum::Int(value)) => *value as u64,
        _ => 0,
    }
}
