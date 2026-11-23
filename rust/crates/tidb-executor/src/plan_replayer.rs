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

//! Go `pkg/executor/plan_replayer.go`: `PlanReplayerExec` (`PLAN REPLAYER
//! DUMP EXPLAIN [ANALYZE]` over one statement, a list of statements or a
//! client's file, `PLAN REPLAYER CAPTURE [REMOVE]`, `PLAN REPLAYER LOAD`) and
//! the `PlanReplayerDumpInfo` that waits on the session for the client's
//! file. The archive itself is `tidb_domain::plan_replayer`'s
//! `DumpPlanReplayerInfo`, which the session runs as Go's `sctx`.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_domain::plan_replayer as replayer;
use tidb_driver_error::DriverError;

use crate::StmtOutput;

/// Go `PlanReplayerDumpInfo`: a dump between `PlanReplayerExec.Next` and
/// its statements' dump.
pub struct PlanReplayerDumpInfo {
    /// Go `Analyze`.
    pub analyze: bool,
    /// Go `File`.
    pub file: Option<Box<dyn tidb_replayer::WriteCloser + Send>>,
    /// Go `FileName`.
    pub file_name: String,
    /// Go `StartTS`.
    pub start_ts: u64,
}

impl PlanReplayerDumpInfo {
    /// Go's deferred `e.DumpInfo.File.Close()` of a failed statement.
    pub fn close_file(&mut self) {
        if let Some(mut file) = self.file.take() {
            let _ = file.close();
        }
    }

    fn into_closed(mut self) {
        self.close_file();
    }
}

/// Go `PlanReplayerExec.createFile`.
fn create_file() -> Result<(Box<dyn tidb_replayer::WriteCloser + Send>, String), DriverError> {
    let unknown = |message: String| DriverError::Mysql(tidb_terror::MysqlError::unknown(message));
    let storage = tidb_planner_extstore::get_global_ext_storage().map_err(unknown)?;
    let (file, file_name) = tidb_replayer::generate_plan_replayer_file(storage.as_ref(), false, false)
        .map_err(|error| unknown(error.to_string()))?;
    Ok((Box::new(file), file_name))
}

/// Go `buildPlanReplayer`'s output schema.
fn item_value_columns() -> Vec<(String, FieldType)> {
    vec![
        ("Item".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
        ("Value".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
    ]
}


/// The session reach Go's `PlanReplayerExec` takes from its `sctx`.
pub trait PlanReplayerSession {
    /// Go `GetTxnManager(sctx).GetStmtReadTS()`.
    fn statement_read_ts(&self) -> u64;
    /// Go's `PlanReplayerLoadVarKey` session value: a load's file path.
    fn plan_replayer_load_info(&mut self) -> &mut Option<String>;
    /// Go's `PlanReplayerDumpVarKey` session value.
    fn plan_replayer_dump_info(&mut self) -> &mut Option<PlanReplayerDumpInfo>;
    /// The session variables a dump task records.
    fn plan_replayer_session_vars(&self) -> replayer::PlanReplayerSessionVars;
    /// Go `domain.DumpPlanReplayerInfo(ctx, sctx, task)` with this session
    /// as `sctx`, writing the archive to `zf`.
    ///
    /// # Errors
    ///
    /// The dump's failure.
    fn dump_plan_replayer_info(
        &mut self,
        task: &mut replayer::PlanReplayerDumpTask,
        zf: &mut dyn tidb_replayer::WriteCloser,
    ) -> Result<(), replayer::PlanReplayerError>;
    /// Go `SessionVars.LastPlanReplayerToken`.
    fn set_last_plan_replayer_token(&mut self, token: String);
    /// Go `ExecRestrictedSQL` on the session's restricted executor.
    ///
    /// # Errors
    ///
    /// The statement's failure.
    fn exec_restricted_sql(&mut self, source: &str, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError>;
    /// Go `GetPlanReplayerHandle().CollectPlanReplayerTask()`.
    ///
    /// # Errors
    ///
    /// The collector's failure.
    fn collect_plan_replayer_task(&self) -> Result<(), DriverError>;
    /// Go `GetRestrictedSQLExecutor().ParseWithParams(sql)`: the parse
    /// error's text when `sql` does not parse.
    ///
    /// # Errors
    ///
    /// The parse error's text.
    fn parse_plan_replayer_sql(&mut self, sql: &str) -> Result<(), String>;
}

/// Go `PlanReplayerExec` and `PlanReplayerLoadExec` over a session.
pub trait PlanReplayerExec: PlanReplayerSession {
    /// Go `PlanReplayerExec.Next`.
    fn plan_replayer_stmt(
        &mut self,
        stmt: &tidb_ast::PlanReplayerStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        match stmt {
            tidb_ast::PlanReplayerStmt::Capture { remove, sql_digest, plan_digest } => {
                if *remove {
                    self.remove_capture_task(sql_digest, plan_digest)?;
                } else {
                    self.register_capture_task(sql_digest, plan_digest)?;
                }
                Ok(Some(StmtOutput::Rows { columns: item_value_columns(), rows: Vec::new() }))
            }
            // Go `PlanReplayerLoadExec.Next`: the load waits on the session
            // (Go's `PlanReplayerLoadVarKey`) for the client's file.
            tidb_ast::PlanReplayerStmt::Load(path) => {
                if path.is_empty() {
                    return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                        "plan replayer: file path is empty",
                    )));
                }
                if self.plan_replayer_load_info().take().is_some() {
                    return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                        "plan replayer: previous plan replayer load option isn't closed normally, please try again",
                    )));
                }
                *self.plan_replayer_load_info() = Some(path.clone());
                Ok(None)
            }
            tidb_ast::PlanReplayerStmt::Dump { historical_stats, analyze, target } => {
                // Go `buildPlanReplayer` and `executorBuilder.buildPlanReplayer`.
                if historical_stats.is_some() {
                    return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                        "WITH STATS AS OF TIMESTAMP is no longer supported because the historical statistics feature has been removed; remove the clause to dump the current statistics",
                    )));
                }
                let exec_stmts = match target.as_ref() {
                    // Go `execStmt.Text()`: the statement's own source text.
                    tidb_ast::PlanReplayerTarget::Statement(statement) => {
                        let text = String::from_utf8_lossy(statement.text()).into_owned();
                        Some(vec![if text.is_empty() { statement.restore() } else { text }])
                    }
                    tidb_ast::PlanReplayerTarget::Statements(statements) => Some(statements.clone()),
                    // A file's statements arrive with the client's file; Go's
                    // builder ignores the slow-query selector.
                    tidb_ast::PlanReplayerTarget::File(_) | tidb_ast::PlanReplayerTarget::SlowQuery { .. } => None,
                };

                // Go `PlanReplayerExec.Next`.
                let (file, file_name) = create_file()?;
                // Go `GetTxnManager(sctx).GetStmtReadTS()`: the statement's read
                // snapshot.
                let start_ts = self.statement_read_ts();
                let mut dump_info = PlanReplayerDumpInfo {
                    analyze: *analyze,
                    file: Some(file),
                    file_name,
                    start_ts,
                };
                if let tidb_ast::PlanReplayerTarget::File(_) = target.as_ref() {
                    // As we can only read file from the connection (which
                    // requests the path), the dump info waits on the session
                    // for the client's file.
                    self.prepare_plan_replayer_dump(dump_info)?;
                    return Ok(None);
                }
                let Some(exec_stmts) = exec_stmts else {
                    dump_info.close_file();
                    return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown("plan replayer: sql is empty")));
                };
                let token = self.plan_replayer_dump(&mut dump_info, exec_stmts)?;
                Ok(Some(StmtOutput::Rows {
                    columns: item_value_columns(),
                    rows: vec![vec![
                        Datum::String(tidb_datatype::StringDatum::new("File token", tidb_charset::Collation::Utf8Mb4Bin)),
                        Datum::String(tidb_datatype::StringDatum::new(token.as_str(), tidb_charset::Collation::Utf8Mb4Bin)),
                    ]],
                }))
            }
        }
    }

    /// Go `PlanReplayerExec.prepare`: the dump waits on the session (Go's
    /// `PlanReplayerDumpVarKey`) until the connection reads the client's file.
    fn prepare_plan_replayer_dump(&mut self, dump_info: PlanReplayerDumpInfo) -> Result<(), DriverError> {
        if let Some(mut previous) = self.plan_replayer_dump_info().take() {
            previous.close_file();
            dump_info.into_closed();
            return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                "plan replayer: previous plan replayer dump option isn't closed normally, please try again",
            )));
        }
        *self.plan_replayer_dump_info() = Some(dump_info);
        Ok(())
    }

    /// Go `handlePlanReplayerLoad`'s info: the load's file path, taken once
    /// the connection read the client's file.
    fn take_plan_replayer_load_info(&mut self) -> Result<String, DriverError> {
        self.plan_replayer_load_info()
            .take()
            .ok_or_else(|| DriverError::Mysql(tidb_terror::MysqlError::unknown("plan replayer load: info is empty")))
    }

    /// Go `clientConn.handlePlanReplayerDump` once the connection read the
    /// client's file: an empty file dumps nothing.
    fn handle_plan_replayer_dump(&mut self, data: &[u8]) -> Result<(), DriverError> {
        let Some(mut dump_info) = self.plan_replayer_dump_info().take() else {
            return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                "plan replayer dump: executor is empty",
            )));
        };
        if data.is_empty() {
            return Ok(());
        }
        self.dump_sqls_from_file(&mut dump_info, data)
    }

    /// Go `PlanReplayerDumpInfo.DumpSQLsFromFile` dumps plan replayer results
    /// for sqls from file.
    fn dump_sqls_from_file(&mut self, dump_info: &mut PlanReplayerDumpInfo, b: &[u8]) -> Result<(), DriverError> {
        let text = String::from_utf8_lossy(b).into_owned();
        let mut exec_stmts = Vec::new();
        for sql in text.split(';') {
            let s = sql.trim_matches('\n');
            if s.is_empty() {
                continue;
            }
            // Go `GetRestrictedSQLExecutor().ParseWithParams`.
            if let Err(error) = self.parse_plan_replayer_sql(s) {
                return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(format!(
                    "parse sql error, sql:{s}, err:{}",
                    error
                ))));
            }
            exec_stmts.push(s.to_owned());
        }
        self.plan_replayer_dump(dump_info, exec_stmts).map(drop)
    }

    /// Go `PlanReplayerDumpInfo.dump`: the archive is written to the global
    /// storage's replayer directory, the status records inserted, and the
    /// token (the file name, or a presigned URL) kept as
    /// `LastPlanReplayerToken`.
    fn plan_replayer_dump(
        &mut self,
        dump_info: &mut PlanReplayerDumpInfo,
        exec_stmts: Vec<String>,
    ) -> Result<String, DriverError> {
        let unknown = |message: String| DriverError::Mysql(tidb_terror::MysqlError::unknown(message));
        let mut task = replayer::PlanReplayerDumpTask {
            file_name: Some(dump_info.file_name.clone()),
            start_ts: dump_info.start_ts,
            analyze: dump_info.analyze,
            session_vars: self.plan_replayer_session_vars(),
            exec_stmts,
            ..replayer::PlanReplayerDumpTask::default()
        };
        let mut file = dump_info.file.take().expect("the dump's file is open");
        self.dump_plan_replayer_info(&mut task, file.as_mut()).map_err(|error| unknown(error.to_string()))?;
        let token = task.presigned_url.filter(|url| !url.is_empty()).unwrap_or_else(|| dump_info.file_name.clone());
        self.set_last_plan_replayer_token(token.clone());
        Ok(token)
    }

    /// Go `PlanReplayerExec.registerCaptureTask`.
    fn register_capture_task(&mut self, sql_digest: &str, plan_digest: &str) -> Result<(), DriverError> {
        let exists = self.exec_restricted_sql(tidb_txnkv::INTERNAL_TXN_STATS_FOREGROUND_PRIORITY, &replayer::check_plan_replayer_task_exists_sql(sql_digest, plan_digest))?;
        if !exists.is_empty() {
            return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                "plan replayer capture task already exists",
            )));
        }
        self.exec_restricted_sql(tidb_txnkv::INTERNAL_TXN_STATS_FOREGROUND_PRIORITY, &format!(
            "insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('{sql_digest}','{plan_digest}')"
        ))?;
        self.collect_plan_replayer_task()
    }

    /// Go `PlanReplayerExec.removeCaptureTask`.
    fn remove_capture_task(&mut self, sql_digest: &str, plan_digest: &str) -> Result<(), DriverError> {
        self.exec_restricted_sql(tidb_txnkv::INTERNAL_TXN_STATS_FOREGROUND_PRIORITY, &format!(
            "delete from mysql.plan_replayer_task where sql_digest = '{sql_digest}' and plan_digest = '{plan_digest}'"
        ))?;
        self.collect_plan_replayer_task()
    }
}

impl<T: PlanReplayerSession + ?Sized> PlanReplayerExec for T {}
