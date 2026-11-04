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

//! Go `PlanReplayerExec` (`pkg/executor/plan_replayer.go`): `PLAN REPLAYER
//! DUMP EXPLAIN [ANALYZE]` over one statement or a list of statements, and
//! `PLAN REPLAYER CAPTURE [REMOVE]`. The dump assembly is
//! `tidb_domain::plan_replayer::dump_plan_replayer_info`; this file gives it
//! the session's seams and the archive in the global external storage.

use std::cell::RefCell;
use std::collections::BTreeMap;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_domain::plan_replayer as replayer;
use tidb_driver_error::DriverError;
use tidb_executor::StmtOutput;
use tidb_infoschema::SessionExtendedInfoSchema;

use crate::Session;

/// The dumping session as Go's `sctx.GetSQLExecutor()`.
struct SessionSql<'a>(RefCell<&'a mut Session>);

impl replayer::PlanReplayerSqlExecutor for SessionSql<'_> {
    fn execute(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>, replayer::PlanReplayerError> {
        let error = |error: DriverError| replayer::PlanReplayerError::Other(error.to_mysql_error().message);
        let stmt = tidb_parser::parse(sql)
            .map_err(|parse| replayer::PlanReplayerError::Other(parse.compatibility_message(sql)))?;
        let output = self.0.borrow_mut().execute_statement_owned(stmt, sql).map_err(error)?;
        let StmtOutput::Rows { rows, .. } = output else {
            return Ok(Vec::new());
        };
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|datum| match datum {
                        Datum::Null => Ok(None),
                        // Go `Datum.ToString`.
                        datum => datum
                            .sql_string()
                            .map(Some)
                            .map_err(|error| replayer::PlanReplayerError::Other(error.to_string())),
                    })
                    .collect()
            })
            .collect()
    }

    fn set_in_plan_replayer(&self, in_plan_replayer: bool) {
        self.0.borrow_mut().vars.in_plan_replayer = in_plan_replayer;
    }
}

/// The statement's info schema as the table-name extractor's source and the
/// TiFlash replica source (Go `GetDomain(sctx).InfoSchema()`).
struct CatalogSource(SessionExtendedInfoSchema);

impl CatalogSource {
    fn table(&self, db: &str, table: &str) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        self.0.table_info_by_name(db, table).map(|(_, table)| table)
    }
}

impl replayer::ExtractSchemaSource for CatalogSource {
    fn table_exists(&self, db: &str, table: &str) -> bool {
        self.table(db, table).is_some()
    }

    fn is_view(&self, db: &str, table: &str) -> bool {
        self.table(db, table).is_some_and(|table| table.read().is_view())
    }

    fn view_select_stmt(&self, db: &str, table: &str) -> Option<String> {
        let table = self.table(db, table)?;
        let table = table.read();
        table.view.as_ref().map(|view| view.read().select_stmt.clone())
    }

    fn foreign_keys(&self, db: &str, table: &str) -> Vec<(String, String)> {
        let Some(table) = self.table(db, table) else {
            return Vec::new();
        };
        let table = table.read();
        table
            .foreign_keys
            .iter_deref()
            .map(|fk| {
                let fk = fk.read();
                (fk.ref_schema.lowercase().to_owned(), fk.ref_table.lowercase().to_owned())
            })
            .collect()
    }
}

impl replayer::TiFlashReplicaSource for CatalogSource {
    fn tiflash_replica_count(&self, db: &str, table: &str) -> Option<u64> {
        let table = self.table(db, table)?;
        let table = table.read();
        table
            .tiflash_replica
            .as_ref()
            .map(|replica| replica.read().count)
            .filter(|count| *count > 0)
    }
}

struct ViewParser;

impl replayer::ExtractViewParser for ViewParser {
    fn parse(&self, sql: &str) -> Result<tidb_ast::Stmt, replayer::PlanReplayerError> {
        tidb_parser::parse(sql).map_err(|error| replayer::PlanReplayerError::Other(error.compatibility_message(sql)))
    }
}

/// The domain's statistics for the dump.
struct StatsSource(Option<Box<dyn tidb_executor::stats_exec::StatsStatementService>>);

impl replayer::StatsDumpSource for StatsSource {
    fn stats_mem_sections(
        &self,
        db: &str,
        table: &str,
    ) -> Result<Option<(Vec<String>, Vec<String>)>, replayer::PlanReplayerError> {
        match &self.0 {
            Some(service) => service
                .plan_replayer_stats_mem(db, table)
                .map_err(|error| replayer::PlanReplayerError::Other(error.to_mysql_error().message)),
            None => Ok(None),
        }
    }

    fn stats_json_for_table(
        &self,
        db: &str,
        table: &str,
        history_stats_ts: u64,
    ) -> Result<(String, Vec<String>), replayer::PlanReplayerError> {
        let error = |error: DriverError| replayer::PlanReplayerError::Other(error.to_mysql_error().message);
        let service = self
            .0
            .as_ref()
            .ok_or_else(|| replayer::PlanReplayerError::Other("this node has no statistics to dump".to_owned()))?;
        let (json, fallbacks) = service.plan_replayer_stats_json(db, table, history_stats_ts).map_err(error)?;
        // Go `json.Marshal(jsonTbl)`.
        let text = serde_json::to_string(&json).map_err(|e| replayer::PlanReplayerError::Other(e.to_string()))?;
        Ok((text, fallbacks))
    }
}

/// The session's variables, read before the dump runs its statements.
struct VariableValues {
    values: BTreeMap<String, Result<String, String>>,
    noop: std::collections::BTreeSet<String>,
    hidden: std::collections::BTreeSet<String>,
    enable_noop_variables: bool,
}

impl replayer::VariablesDumpSource for VariableValues {
    fn variable_names(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }
    fn is_noop(&self, name: &str) -> bool {
        self.noop.contains(name)
    }
    fn hidden_for_sem(&self, name: &str) -> bool {
        self.hidden.contains(name)
    }
    fn session_or_global_value(&self, name: &str) -> Result<String, String> {
        self.values.get(name).cloned().unwrap_or_else(|| Err(format!("unknown system variable '{name}'")))
    }
    fn enable_noop_variables(&self) -> bool {
        self.enable_noop_variables
    }
}

/// Go `getPresignedURL` over the local global storage.
struct LocalPresign(Option<std::sync::Arc<tidb_planner_extstore::LocalStorage>>);

impl replayer::PresignSource for LocalPresign {
    fn has_global_storage(&self) -> bool {
        self.0.is_some()
    }
    fn presign_file(&self, path: &str, _expire: std::time::Duration) -> Result<String, String> {
        self.0
            .as_ref()
            .map(|storage| storage.presign_file(path))
            .ok_or_else(|| "no global external storage".to_owned())
    }
}

/// The archive the dump writes, buffered until Go's deferred close.
struct ArchiveWriter(tidb_replayer::zip::ZipWriter<Vec<u8>>);

impl replayer::DumpArchiveWriter for ArchiveWriter {
    fn create_file(&mut self, name: &str, contents: &[u8]) -> Result<(), replayer::PlanReplayerError> {
        self.0
            .create_file(name, contents)
            .map_err(|error| replayer::PlanReplayerError::Other(error.to_string()))
    }
}

/// Go `buildPlanReplayer`'s output schema.
fn item_value_columns() -> Vec<(String, FieldType)> {
    vec![
        ("Item".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
        ("Value".to_owned(), FieldType::new(FieldTypeCode::Varchar)),
    ]
}

impl Session {
    /// Go `PlanReplayerExec.Next`.
    pub(crate) fn plan_replayer_stmt(
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
            tidb_ast::PlanReplayerStmt::Load(_) => {
                Err(DriverError::unsupported("PLAN REPLAYER LOAD is not supported"))
            }
            tidb_ast::PlanReplayerStmt::Dump { historical_stats, analyze, target } => {
                if historical_stats.is_some() {
                    return Err(DriverError::unsupported("PLAN REPLAYER DUMP WITH STATS AS OF is not supported"));
                }
                let exec_stmts = match target.as_ref() {
                    // Go `execStmt.Text()`: the statement's own source text.
                    tidb_ast::PlanReplayerTarget::Statement(statement) => {
                        let text = String::from_utf8_lossy(statement.text()).into_owned();
                        vec![if text.is_empty() { statement.restore() } else { text }]
                    }
                    tidb_ast::PlanReplayerTarget::Statements(statements) => statements.clone(),
                    _ => {
                        return Err(DriverError::unsupported(
                            "PLAN REPLAYER DUMP of a file or the slow log is not supported",
                        ))
                    }
                };
                if exec_stmts.is_empty() {
                    return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown("plan replayer: sql is empty")));
                }
                let token = self.dump_plan_replayer(*analyze, &exec_stmts)?;
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

    /// Go `PlanReplayerExec.createFile` + `PlanReplayerDumpInfo.dump`: the
    /// archive is written to the global storage's replayer directory, the
    /// status records inserted, and the token (the file name, or a
    /// presigned URL) kept as `LastPlanReplayerToken`.
    fn dump_plan_replayer(&mut self, analyze: bool, exec_stmts: &[String]) -> Result<String, DriverError> {
        let unknown = |message: String| DriverError::Mysql(tidb_terror::MysqlError::unknown(message));
        let storage = tidb_planner_extstore::get_global_ext_storage().map_err(unknown)?;
        let (mut file, file_name) =
            tidb_replayer::generate_plan_replayer_file(storage.as_ref(), false, false, false)
                .map_err(|error| unknown(error.to_string()))?;
        let catalog = self.with_catalog_mut(|catalog| Ok(catalog.clone()))?;
        let variables = self.plan_replayer_variables();
        let stats = StatsSource(self.domain_services().map(|services| (services.stats)(self)));
        let cur_db = self.current_database().to_owned();
        // Go `GetTxnManager(sctx).GetStmtReadTS()`: the statement's read
        // snapshot.
        let start_ts = self.statement_snapshot.as_ref().map_or(0, |(ts, _)| *ts);
        let mut task = replayer::PlanReplayerDumpTask {
            file_name: Some(file_name.clone()),
            start_ts,
            analyze,
            ..replayer::PlanReplayerDumpTask::default()
        };
        let catalog = CatalogSource(catalog);
        let presign = LocalPresign(Some(std::sync::Arc::clone(&storage)));
        let mut archive = ArchiveWriter(tidb_replayer::zip::ZipWriter::new(Vec::new()));
        let outcome = {
            let sql = SessionSql(RefCell::new(&mut *self));
            let seams = replayer::DumpSeams {
                sql: &sql,
                extract_schema: &catalog,
                view_parser: &ViewParser,
                stats: &stats,
                mem_stats: &stats,
                tiflash: &catalog,
                variables: &variables,
                presign: &presign,
                cur_db,
                enable_historical_stats_for_capture: false,
            };
            replayer::dump_plan_replayer_info(&seams, &mut task, exec_stmts, &mut archive)
        };
        // Go's deferred `zw.Close` / `zf.Close`, then the status records.
        let mut close_error = String::new();
        let bytes = match archive.0.close() {
            Ok(()) => archive.0.into_inner(),
            Err(error) => {
                close_error = error.to_string();
                Vec::new()
            }
        };
        let write = tidb_replayer::WriteCloser::write(&mut file, &bytes)
            .and_then(|_| tidb_replayer::WriteCloser::close(&mut file));
        if let Err(error) = write {
            close_error = if close_error.is_empty() { error.to_string() } else { format!("{close_error},{error}") };
        }
        let mut records = outcome.records;
        let error_message = match (&outcome.error, close_error.is_empty()) {
            (Some(error), true) => Some(error.to_string()),
            (Some(error), false) => Some(format!("{error},{close_error}")),
            (None, false) => Some(close_error),
            (None, true) => None,
        };
        if let Some(message) = &error_message {
            for record in &mut records {
                record.failed_reason = message.clone();
            }
        }
        self.insert_plan_replayer_status(&records);
        if let Some(error) = outcome.error {
            return Err(unknown(error.to_string()));
        }
        let token = task.presigned_url.filter(|url| !url.is_empty()).unwrap_or(file_name);
        self.vars.last_plan_replayer_token = token.clone();
        Ok(token)
    }

    /// Go `dumpVariables`' reads: every system variable's session-or-global
    /// value.
    fn plan_replayer_variables(&self) -> VariableValues {
        let vars = &self.vars;
        let mut values = BTreeMap::new();
        let mut noop = std::collections::BTreeSet::new();
        let mut hidden = std::collections::BTreeSet::new();
        for definition in tidb_variable::sysvar::SYS_VARS {
            let name = definition.name.to_owned();
            if definition.is_noop() {
                noop.insert(name.clone());
            }
            if tidb_executor::sessionctx::sys_var_hidden_for_sem(self, definition.name) {
                hidden.insert(name.clone());
            }
            values.insert(name, vars.get_system(definition.name).map_err(|error| error.to_string()));
        }
        VariableValues { values, noop, hidden, enable_noop_variables: vars.enable_noop_variables() }
    }

    /// Go `insertPlanReplayerStatus`: one row per record into
    /// `mysql.plan_replayer_status`, failures logged.
    fn insert_plan_replayer_status(&mut self, records: &[replayer::PlanReplayerStatusRecord]) {
        let instance = self.plan_replayer_instance();
        for record in records {
            let sql = if record.failed_reason.is_empty() {
                format!(
                    "insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, token, instance) values ({}, {}, {}, {}, {})",
                    quote(&record.sql_digest),
                    quote(&record.plan_digest),
                    quote(&record.origin_sql),
                    quote(&record.token),
                    quote(&instance)
                )
            } else {
                format!(
                    "insert into mysql.plan_replayer_status (sql_digest, plan_digest, origin_sql, fail_reason, instance) values ({}, {}, {}, {}, {})",
                    quote(&record.sql_digest),
                    quote(&record.plan_digest),
                    quote(&record.origin_sql),
                    quote(&record.failed_reason),
                    quote(&instance)
                )
            };
            if let Err(error) = self.exec_restricted_sql(&sql) {
                tidb_logutil::bg_logger().warn(
                    "insert mysql.plan_replayer_status record failed",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                );
            }
        }
    }

    /// Go's `instance` column: `infosync.GetServerInfo()`'s IP and port.
    fn plan_replayer_instance(&self) -> String {
        match &self.server_info_syncer {
            Some(syncer) => {
                let info = syncer.get_local_server_info();
                replayer::join_host_port(&info.static_info.ip, info.static_info.port as u64)
            }
            None => String::new(),
        }
    }

    /// Go `PlanReplayerExec.registerCaptureTask`.
    fn register_capture_task(&mut self, sql_digest: &str, plan_digest: &str) -> Result<(), DriverError> {
        let exists = self.exec_restricted_sql(&replayer::check_plan_replayer_task_exists_sql(sql_digest, plan_digest))?;
        if !exists.is_empty() {
            return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                "plan replayer capture task already exists",
            )));
        }
        self.exec_restricted_sql(&format!(
            "insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('{sql_digest}','{plan_digest}')"
        ))?;
        Ok(())
    }

    /// Go `PlanReplayerExec.removeCaptureTask`.
    fn remove_capture_task(&mut self, sql_digest: &str, plan_digest: &str) -> Result<(), DriverError> {
        self.exec_restricted_sql(&format!(
            "delete from mysql.plan_replayer_task where sql_digest = '{sql_digest}' and plan_digest = '{plan_digest}'"
        ))?;
        Ok(())
    }
}

/// Go `%?` of a string parameter.
fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for character in value.chars() {
        match character {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\0' => out.push_str("\\0"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\u{1a}' => out.push_str("\\Z"),
            character => out.push(character),
        }
    }
    out.push('\'');
    out
}
