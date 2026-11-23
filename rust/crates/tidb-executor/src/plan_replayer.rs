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
use std::collections::BTreeSet;

use tidb_driver_error::DriverError;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

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

/// Go `domain.PlanReplayerSchemaMetaFile`.
const PLAN_REPLAYER_SCHEMA_META_FILE: &str = "schema_meta.txt";
/// Go `domain.PlanReplayerTiFlashReplicasFile`.
const PLAN_REPLAYER_TIFLASH_REPLICAS_FILE: &str = "table_tiflash_replica.txt";
/// Go `domain.PlanReplayerVariablesFile`.
const PLAN_REPLAYER_VARIABLES_FILE: &str = "variables.toml";
/// Go `domain.PlanReplayerSessionBindingFile`.
const PLAN_REPLAYER_SESSION_BINDING_FILE: &str = "session_bindings.sql";
/// Go `domain.PlanReplayerGlobalBindingFile`.
const PLAN_REPLAYER_GLOBAL_BINDING_FILE: &str = "global_bindings.sql";

fn unknown(message: impl Into<String>) -> SqlQueryError {
    SqlQueryError::unknown(message)
}


/// The session state Go's `PlanReplayerLoadInfo.Update` reaches through `sctx`.
pub trait PlanReplayerLoadSession {
    /// Go `sctx.GetSessionVars()`.
    fn load_session_vars(&self) -> &tidb_variable::SessionVars;
    /// Go `sctx.GetSessionVars()`, changed.
    fn load_session_vars_mut(&mut self) -> &mut tidb_variable::SessionVars;
    /// Go `StmtCtx.AppendWarning(errors.New(message))`.
    fn append_load_warning(&mut self, message: String);
    /// Go `GlobalVarsAccessor.SetGlobalSysVar`.
    ///
    /// # Errors
    ///
    /// The variable's refusal.
    fn set_load_global_sys_var(&mut self, name: &str, value: String) -> Result<(), SqlQueryError>;
    /// Go `ctx.GetSQLExecutor().Execute(ctx, sql)`: one statement on this
    /// session, as the connection runs one.
    ///
    /// # Errors
    ///
    /// The statement's failure.
    fn execute_nested(&mut self, sql: &str) -> Result<(), SqlQueryError>;
    /// Go `loadStats`' `statsHandle.LoadStatsFromJSON`.
    ///
    /// # Errors
    ///
    /// The load's failure.
    fn load_stats_json(&mut self, json: &tidb_stats_util::JsonTable) -> Result<(), SqlQueryError>;
}

/// Go `PlanReplayerLoadInfo.Update` and `loadPlanReplayerForExplainExplore`
/// over a session.
pub trait PlanReplayerLoad: PlanReplayerLoadSession {
    /// Go `loadPlanReplayerForExplainExplore`: the replayer file's
    /// environment loaded into this session, and its target SQL.
    fn load_plan_replayer_for_explain_explore(&mut self, path: &str) -> Result<String, SqlQueryError> {
        if path.trim().is_empty() {
            return Err(unknown("plan replayer: file path is empty"));
        }
        let data = std::fs::read(path).map_err(|error| {
            unknown(format!("open {path}: {}", crate::select_into::go_errno_text(&error)))
        })?;
        let target_sql = extract_plan_replayer_target_sql(&data)?;
        self.plan_replayer_load_update(&data)?;
        Ok(target_sql)
    }

    /// Go `PlanReplayerLoadInfo.Update` updates the data of the
    /// corresponding table.
    fn plan_replayer_load_update(&mut self, data: &[u8]) -> Result<(), SqlQueryError> {
        let z = tidb_replayer::zip::read_zip(data).map_err(|error| unknown(error.to_string()))?;

        // load variable
        self.load_variables(&z)?;
        // Explicitly disable auto-analyze after restore so imported stats stay
        // stable. Users can re-enable it manually when they no longer need a
        // frozen replay env.
        self.disable_auto_analyze_for_plan_replayer_load()?;
        // build schema and table first
        let database_sets = self.create_table(&z)?;

        // set tiflash replica if exists
        self.load_set_tiflash_replica(&z);

        // build view next
        for (name, contents) in &z {
            let path: Vec<&str> = name.split('/').collect();
            if path.len() == 2 && path[0] == "view" {
                self.create_schema_and_items(contents)?;
            }
        }

        // load stats
        for (name, contents) in &z {
            let path: Vec<&str> = name.split('/').collect();
            if path.len() == 2 && path[0] == "stats" {
                self.load_stats(name, contents)?;
            }
        }

        self.load_all_bindings(&z, &database_sets);

        // Notify users that PLAN REPLAYER LOAD disables auto-analyze to keep
        // restored stats stable.
        self.append_load_warning(format!(
                "`PLAN REPLAYER LOAD` sets @@global.{}=OFF to keep restored statistics stable; re-enable it manually if needed",
                tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE
            ));
        Ok(())
    }

    /// Go `loadVariables`.
    fn load_variables(&mut self, z: &[(String, Vec<u8>)]) -> Result<(), SqlQueryError> {
        let mut un_load_vars = Vec::new();
        for (name, contents) in z {
            if name == PLAN_REPLAYER_VARIABLES_FILE {
                un_load_vars = tidb_util_config::load_config_for_plan_replayer_load(
                    self.load_session_vars_mut(),
                    contents.as_slice(),
                )
                .map_err(|error| unknown(error.to_string()))?;
            }
        }
        if !un_load_vars.is_empty() {
            self.append_load_warning(format!("variables set failed:{}", un_load_vars.join(",")));
        }
        Ok(())
    }

    /// Go `disableAutoAnalyzeForPlanReplayerLoad`: plan replayer loads
    /// recorded statistics, which an auto-analyze right after the restore
    /// would overwrite.
    fn disable_auto_analyze_for_plan_replayer_load(&mut self) -> Result<(), SqlQueryError> {
        self.set_load_global_sys_var(tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE, "OFF".to_owned())
    }

    /// Go `PlanReplayerLoadInfo.createTable`: the dumped schemas, with
    /// foreign key checks off and placement ignored, since their order is not
    /// guaranteed; answers the dump's databases.
    fn create_table(&mut self, z: &[(String, Vec<u8>)]) -> Result<BTreeSet<String>, SqlQueryError> {
        let read = |vars: &tidb_variable::vars::SessionVars, name: &str| vars.get_system(name).unwrap_or_default();
        let origin_foreign_key_checks = read(self.load_session_vars(), tidb_vardef::FOREIGN_KEY_CHECKS);
        let origin_placement_mode = read(self.load_session_vars(), tidb_vardef::tidb_vars::TIDB_PLACEMENT_MODE);
        let set = |vars: &mut tidb_variable::vars::SessionVars, name: &str, value: String| {
            let _ = vars.set_system(name, value);
        };
        set(self.load_session_vars_mut(), tidb_vardef::FOREIGN_KEY_CHECKS, "OFF".to_owned());
        set(self.load_session_vars_mut(), tidb_vardef::tidb_vars::TIDB_PLACEMENT_MODE, "IGNORE".to_owned());
        let outcome = (|| {
            let mut database_sets = BTreeSet::new();
            for (name, contents) in z {
                if *name == format!("schema/{PLAN_REPLAYER_SCHEMA_META_FILE}") {
                    let text = String::from_utf8_lossy(contents);
                    for row in text.split('\n') {
                        for m in row.split(';') {
                            if m.is_empty() {
                                continue;
                            }
                            let s = m.split('.').next().unwrap_or_default();
                            database_sets.insert(s.to_owned());
                        }
                    }
                    continue;
                }
                let path: Vec<&str> = name.split('/').collect();
                if path.len() == 2 && path[0] == "schema" {
                    self.create_schema_and_items(contents)?;
                }
            }
            Ok(database_sets)
        })();
        set(self.load_session_vars_mut(), tidb_vardef::FOREIGN_KEY_CHECKS, origin_foreign_key_checks);
        set(self.load_session_vars_mut(), tidb_vardef::tidb_vars::TIDB_PLACEMENT_MODE, origin_placement_mode);
        outcome
    }

    /// Go `createSchemaAndItems` creates schema and tables or views.
    fn create_schema_and_items(&mut self, contents: &[u8]) -> Result<(), SqlQueryError> {
        let origin_text = String::from_utf8_lossy(contents).into_owned();
        let stmts = tidb_parser::parse_multi_with_sql_mode_and_config(
            &origin_text,
            tidb_sessionctx::scanner_sql_mode(self.load_session_vars().sql_mode()),
            self.load_session_vars().build_parser_config(),
        )
        .map_err(|error| {
            crate::ddl_exec::map_error(match error.terror {
                Some(proto) => tidb_driver_error::DriverError::from(proto.gen_with_stack(error.message)),
                None => tidb_driver_error::DriverError::from(tidb_util::misc::syntax_error(
                    error.compatibility_message(&origin_text),
                )),
            })
        })?;
        if stmts.is_empty() {
            return Err(unknown("plan replayer: empty schema file"));
        }
        for (i, stmt) in stmts.iter().enumerate() {
            let sql_text = String::from_utf8_lossy(stmt.text()).into_owned();
            if sql_text.trim().is_empty() {
                continue;
            }
            let outcome = self.execute_nested(&sql_text);
            if i == 0 {
                // create database if not exists
                continue;
            }
            if let Err(error) = outcome {
                if error.code == tidb_errno::errcode::ErrTableExists {
                    continue;
                }
                return Err(error);
            }
        }
        Ok(())
    }


    /// Go `loadSetTiFlashReplica`; every failure is skipped.
    fn load_set_tiflash_replica(&mut self, z: &[(String, Vec<u8>)]) {
        for (name, contents) in z {
            if name != PLAN_REPLAYER_TIFLASH_REPLICAS_FILE {
                continue;
            }
            let text = String::from_utf8_lossy(contents).into_owned();
            for row in text.split('\n') {
                if row.is_empty() {
                    continue;
                }
                let r: Vec<&str> = row.split('\t').collect();
                if r.len() < 3 {
                    continue;
                }
                let (db_name, table_name, replica_count) = (r[0], r[1], r[2].trim());
                let sql = format!("alter table {db_name}.{table_name} set tiflash replica {replica_count}");
                if let Err(error) = self.execute_nested(&sql) {
                    // Without TiFlash stores, use one hypothetical replica so
                    // the optimizer can still reproduce TiFlash access paths.
                    if error.error_string().contains("total tiflash server count: 0") {
                        let sql = format!("alter table {db_name}.{table_name} set hypo tiflash replica 1");
                        let _ = self.execute_nested(&sql);
                    }
                }
            }
        }
    }

    /// Go `loadStats`: a file that does not decode is a warning, a dump of
    /// a table without statistics is a no-op.
    fn load_stats(&mut self, name: &str, contents: &[u8]) -> Result<(), SqlQueryError> {
        let json = match tidb_stats_handle_storage::json::parse_stats_json(&String::from_utf8_lossy(contents)) {
            Ok(json) => json,
            Err(error) => {
                self.append_load_warning(format!("fail to unmarshal stats JSON for file {name}\n{error}"));
                return Ok(());
            }
        };
        // Keep plan replayer consistent with LOAD STATS: a dumped stats file
        // can be null when the source table has no stats.
        if json.table_name.is_empty() && json.version == 0 {
            return Ok(());
        }
        self.load_stats_json(&json)
    }

    /// Go `loadAllBindings`.
    fn load_all_bindings(&mut self, z: &[(String, Vec<u8>)], database_sets: &BTreeSet<String>) {
        for (name, contents) in z {
            if name == PLAN_REPLAYER_SESSION_BINDING_FILE {
                self.load_bindings(contents, database_sets, true);
            } else if name == PLAN_REPLAYER_GLOBAL_BINDING_FILE {
                self.load_bindings(contents, database_sets, false);
            }
        }
    }

    /// Go `loadBindings`: the bindings recreated `USING` their bind SQL,
    /// since the original SQL is normalized and cannot run.
    fn load_bindings(&mut self, contents: &[u8], database_sets: &BTreeSet<String>, is_session: bool) {
        let text = String::from_utf8_lossy(contents).into_owned();
        if text.is_empty() {
            return;
        }
        for binding in text.split('\n') {
            let cols: Vec<&str> = binding.split('\t').collect();
            if cols.len() < 3 {
                continue;
            }
            let binding_sql = cols[1];
            let default_db = cols[2];
            // Go reads `cols[3]` unguarded, which a three-column row panics on.
            let enabled = cols[3];
            // An empty defaultDB is a universal binding, which doesn't need
            // to check databaseSets.
            if !default_db.is_empty() && !database_sets.contains(default_db) {
                continue;
            }
            if enabled == "enabled" {
                let sql = format!(
                    "CREATE {} BINDING USING {binding_sql}",
                    if is_session { "SESSION" } else { "GLOBAL" }
                );
                if let Err(error) = self.execute_nested(&sql) {
                    tidb_logutil::bg_logger().warn(
                        "load bindings failed",
                        &[
                            tidb_logutil::zap_error(error.error_string()),
                            tidb_log::Field::new("sql", tidb_log::Value::Str(sql)),
                        ],
                    );
                }
            }
        }
    }
}

impl<T: PlanReplayerLoadSession + ?Sized> PlanReplayerLoad for T {}

/// Go `extractPlanReplayerTargetSQL`: the archive's `sql/sql0.sql`, trimmed.
fn extract_plan_replayer_target_sql(data: &[u8]) -> Result<String, SqlQueryError> {
    let z = tidb_replayer::zip::read_zip(data).map_err(|error| unknown(error.to_string()))?;
    for (name, contents) in &z {
        if name != "sql/sql0.sql" {
            continue;
        }
        let target_sql = String::from_utf8_lossy(contents).trim().to_owned();
        if target_sql.is_empty() {
            return Err(unknown("plan replayer: target SQL is empty"));
        }
        return Ok(target_sql);
    }
    Err(unknown("plan replayer: target SQL file sql/sql0.sql not found"))
}
