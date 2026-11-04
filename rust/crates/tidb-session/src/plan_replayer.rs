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

/// Go `domain.planReplayerHandle` over the domain's internal sessions.
type PlanReplayerHandle =
    replayer::PlanReplayerHandle<tidb_domain_core::domain::plan_replayer_owner::PooledSql>;

/// Go `planReplayerSessionFinishedTaskKeyLen`.
const PLAN_REPLAYER_SESSION_FINISHED_TASK_KEY_LEN: usize = 128;

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

/// Go `variable.GetSysVars()`'s flags and `SysVarHiddenForSem` for the
/// dumping session, read before the dump runs its statements.
struct SysVarRegistry {
    names: Vec<String>,
    noop: std::collections::BTreeSet<String>,
    hidden: std::collections::BTreeSet<String>,
    enable_noop_variables: bool,
}

impl replayer::VariablesDumpSource for SysVarRegistry {
    fn variable_names(&self) -> Vec<String> {
        self.names.clone()
    }
    fn is_noop(&self, name: &str) -> bool {
        self.noop.contains(name)
    }
    fn hidden_for_sem(&self, name: &str) -> bool {
        self.hidden.contains(name)
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
        // Go `GetTxnManager(sctx).GetStmtReadTS()`: the statement's read
        // snapshot.
        let start_ts = self.statement_snapshot.as_ref().map_or(0, |(ts, _)| *ts);
        let mut task = replayer::PlanReplayerDumpTask {
            file_name: Some(file_name.clone()),
            start_ts,
            analyze,
            session_vars: self.plan_replayer_session_vars(),
            exec_stmts: exec_stmts.to_vec(),
            ..replayer::PlanReplayerDumpTask::default()
        };
        self.dump_plan_replayer_info(&mut task, &mut file).map_err(|error| unknown(error.to_string()))?;
        let token = task.presigned_url.filter(|url| !url.is_empty()).unwrap_or(file_name);
        self.vars.last_plan_replayer_token = token.clone();
        Ok(token)
    }

    /// Go `DumpPlanReplayerInfo(ctx, sctx, task)` with this session as
    /// `sctx`: the archive goes to `zf`, Go's deferred close of the zip
    /// writer and of `zf` runs, and the status records are inserted.
    pub(crate) fn dump_plan_replayer_info(
        &mut self,
        task: &mut replayer::PlanReplayerDumpTask,
        zf: &mut dyn tidb_replayer::WriteCloser,
    ) -> Result<(), replayer::PlanReplayerError> {
        let catalog = CatalogSource(
            self.with_catalog_mut(|catalog| Ok(catalog.clone()))
                .map_err(|error| replayer::PlanReplayerError::Other(error.to_mysql_error().message))?,
        );
        let stats = StatsSource(self.domain_services().map(|services| (services.stats)(self)));
        let presign = LocalPresign(tidb_planner_extstore::get_global_ext_storage().ok());
        // Go `vardef.EnableHistoricalStatsForCapture.Load()`.
        let enable_historical_stats_for_capture = self
            .vars
            .get_global(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS_FOR_CAPTURE)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value));
        let variables = self.plan_replayer_sys_var_registry();
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
                presign: &presign,
                variables: &variables,
                enable_historical_stats_for_capture,
            };
            replayer::dump_plan_replayer_info(&seams, task, &mut archive)
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
        let write = zf.write(&bytes).and_then(|_| zf.close());
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
        outcome.error.map_or(Ok(()), Err)
    }

    /// Go `*variable.SessionVars` as the dump reads it: the current database
    /// and every system variable's session-or-global value.
    fn plan_replayer_session_vars(&self) -> replayer::PlanReplayerSessionVars {
        let values = tidb_variable::sysvar::SYS_VARS
            .iter()
            .map(|definition| {
                (definition.name.to_owned(), self.vars.get_system(definition.name).map_err(|error| error.to_string()))
            })
            .collect();
        replayer::PlanReplayerSessionVars { current_db: self.current_database().to_owned(), values }
    }

    /// Go `dumpVariables`' registry walk for this (the dumping) session.
    fn plan_replayer_sys_var_registry(&self) -> SysVarRegistry {
        let mut registry = SysVarRegistry {
            names: Vec::new(),
            noop: std::collections::BTreeSet::new(),
            hidden: std::collections::BTreeSet::new(),
            enable_noop_variables: self.vars.enable_noop_variables(),
        };
        for definition in tidb_variable::sysvar::SYS_VARS {
            registry.names.push(definition.name.to_owned());
            if definition.is_noop() {
                registry.noop.insert(definition.name.to_owned());
            }
            if tidb_executor::sessionctx::sys_var_hidden_for_sem(self, definition.name) {
                registry.hidden.insert(definition.name.to_owned());
            }
        }
        registry
    }

    /// Go `ExecStmt.checkPlanReplayerCapture`, the first step of
    /// `FinishExecuteStmt`: a matching capture task, or the continuous
    /// capture, sends this statement to the dump workers.
    pub(crate) fn check_plan_replayer_capture(
        &mut self,
        observation: &tidb_executor::exec_stmt_observation::StatementObservation,
        plan: &tidb_stmtctx::ProcessPlanInfo,
        txn_ts: u64,
    ) {
        // Go also skips the statistics request sources; this node's
        // statistics statements all run on restricted sessions.
        let on = |name: &str| {
            self.vars.get_system(name).is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
        };
        let continues = on(tidb_vardef::tidb_vars::TIDB_ENABLE_PLAN_REPLAYER_CONTINUOUS_CAPTURE);
        // Go `IsPlanReplayerCaptureEnabled`.
        if self.vars.restricted_sql || !(on(tidb_vardef::tidb_vars::TIDB_ENABLE_PLAN_REPLAYER_CAPTURE) || continues) {
            return;
        }
        let Some(handle) = self.plan_replayer_handle() else {
            return;
        };
        let key = tidb_replayer::PlanReplayerTaskKey {
            sql_digest: observation.digest.as_str().to_owned(),
            plan_digest: plan.plan_digest.clone(),
        };
        if continues {
            // Go `checkPlanReplayerContinuesCapture`.
            if !observation.plan_replayer_continues_capture_valid
                || self.vars.plan_replayer_finished_task_key.contains(&key)
            {
                return;
            }
            self.send_plan_replayer_dump_task(&handle, key.clone(), observation, plan, txn_ts, true);
            // Go `AddPlanReplayerFinishedTaskKey`.
            if self.vars.plan_replayer_finished_task_key.len() >= PLAN_REPLAYER_SESSION_FINISHED_TASK_KEY_LEN {
                self.vars.plan_replayer_finished_task_key.clear();
            }
            self.vars.plan_replayer_finished_task_key.insert(key);
            return;
        }
        // Go `checkPlanReplayerCaptureTask`.
        let tasks = handle.collector.get_tasks();
        if tasks.is_empty() || key.sql_digest.is_empty() || key.plan_digest.is_empty() {
            return;
        }
        if tasks
            .iter()
            .any(|task| task.sql_digest == key.sql_digest && (task.plan_digest == "*" || task.plan_digest == key.plan_digest))
        {
            self.send_plan_replayer_dump_task(&handle, key, observation, plan, txn_ts, false);
        }
    }

    /// Go `sendPlanReplayerDumpTask`.
    fn send_plan_replayer_dump_task(
        &self,
        handle: &PlanReplayerHandle,
        key: tidb_replayer::PlanReplayerTaskKey,
        observation: &tidb_executor::exec_stmt_observation::StatementObservation,
        plan: &tidb_stmtctx::ProcessPlanInfo,
        start_ts: u64,
        is_continues_capture: bool,
    ) {
        let session_bindings = self
            .session_bindings
            .get_all_session_bindings()
            .into_iter()
            .map(|binding| {
                vec![
                    binding.original_sql.clone(),
                    binding.bind_sql.clone(),
                    binding.db.clone(),
                    binding.status.to_owned(),
                    binding.create_time.clone(),
                    binding.update_time.clone(),
                    binding.charset.clone(),
                    binding.collation.clone(),
                    binding.source.to_owned(),
                ]
            })
            .collect();
        handle.send_task(replayer::PlanReplayerDumpTask {
            key,
            start_ts,
            session_bindings,
            session_vars: self.plan_replayer_session_vars(),
            // Go `[]ast.StmtNode{stmtNode}`, an EXECUTE's being the prepared
            // statement: the statement's original text either way.
            exec_stmts: vec![observation.text.clone()],
            encoded_plan: plan.encoded_plan.clone(),
            analyze: false,
            is_capture: true,
            is_continues_capture,
            ..replayer::PlanReplayerDumpTask::default()
        });
    }

    /// Go `domain.GetDomain(sctx).GetPlanReplayerHandle()`.
    fn plan_replayer_handle(&self) -> Option<std::sync::Arc<PlanReplayerHandle>> {
        self.domain_services()?.domain.upgrade()?.plan_replayer_handle()
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
        self.collect_plan_replayer_task()
    }

    /// Go `GetPlanReplayerHandle().CollectPlanReplayerTask()` after a
    /// capture task is added or removed.
    fn collect_plan_replayer_task(&self) -> Result<(), DriverError> {
        match self.plan_replayer_handle() {
            Some(handle) => handle
                .collector
                .collect_plan_replayer_task()
                .map_err(|error| DriverError::Mysql(tidb_terror::MysqlError::unknown(error.to_string()))),
            None => Ok(()),
        }
    }

    /// Go `PlanReplayerExec.removeCaptureTask`.
    fn remove_capture_task(&mut self, sql_digest: &str, plan_digest: &str) -> Result<(), DriverError> {
        self.exec_restricted_sql(&format!(
            "delete from mysql.plan_replayer_task where sql_digest = '{sql_digest}' and plan_digest = '{plan_digest}'"
        ))?;
        self.collect_plan_replayer_task()
    }
}

/// Go `planReplayerTaskDumpWorker`'s dumping half: the archive Go's
/// `GeneratePlanReplayerFile` creates waits here (Go `task.Zf`) until
/// `DumpPlanReplayerInfo` runs on a worker session.
pub struct PlanReplayerWorkerDumper {
    open_session: Box<dyn Fn() -> Result<Session, String> + Send + Sync>,
    files: std::sync::Mutex<std::collections::HashMap<String, Box<dyn tidb_replayer::WriteCloser + Send>>>,
}

impl PlanReplayerWorkerDumper {
    /// A dumper whose worker sessions `open_session` opens.
    pub fn new(open_session: impl Fn() -> Result<Session, String> + Send + Sync + 'static) -> Self {
        Self { open_session: Box::new(open_session), files: std::sync::Mutex::default() }
    }

    fn take_file(&self, file_name: &str) -> Option<Box<dyn tidb_replayer::WriteCloser + Send>> {
        self.files.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(file_name)
    }
}

impl replayer::PlanReplayerDumper for PlanReplayerWorkerDumper {
    fn generate_plan_replayer_file(
        &self,
        is_capture: bool,
        is_continues_capture: bool,
        enable_historical_stats_for_capture: bool,
    ) -> Result<String, replayer::PlanReplayerError> {
        let storage = tidb_planner_extstore::get_global_ext_storage().map_err(replayer::PlanReplayerError::Other)?;
        let (file, file_name) = tidb_replayer::generate_plan_replayer_file(
            storage.as_ref(),
            is_capture,
            is_continues_capture,
            enable_historical_stats_for_capture,
        )
        .map_err(|error| replayer::PlanReplayerError::Other(error.to_string()))?;
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(file_name.clone(), Box::new(file));
        Ok(file_name)
    }

    fn dump_plan_replayer_info(&self, task: &replayer::PlanReplayerDumpTask) -> Result<(), replayer::PlanReplayerError> {
        let file_name = task.file_name.clone().unwrap_or_default();
        let mut zf = self
            .take_file(&file_name)
            .ok_or_else(|| replayer::PlanReplayerError::Other(format!("plan replayer: no file {file_name}")))?;
        let mut session = (self.open_session)().map_err(replayer::PlanReplayerError::Other)?;
        let mut task = task.clone();
        session.dump_plan_replayer_info(&mut task, zf.as_mut())
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
