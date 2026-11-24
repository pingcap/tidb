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

use tidb_datatype::Datum;
use tidb_domain::plan_replayer as replayer;
use tidb_driver_error::DriverError;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::StmtOutput;
use tidb_infoschema::SessionExtendedInfoSchema;

use crate::Session;

/// Go `domain.planReplayerHandle` over the domain's internal sessions.
type PlanReplayerHandle =
    replayer::PlanReplayerHandle<tidb_domain_core::domain::plan_replayer_owner::PooledSql>;

/// Go `planReplayerSessionFinishedTaskKeyLen`.

/// The dumping session as Go's `sctx`: its `GetSQLExecutor()` and session
/// variables.
struct SessionSql<'a>(std::sync::Mutex<&'a mut Session>);

impl SessionSql<'_> {
    /// One statement of the session; a query's rows become its record set.
    fn record_set(
        &self,
        sql: &str,
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        let stmt = tidb_parser::parse(sql).map_err(|parse| parse.compatibility_message(sql))?;
        let output = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .execute_statement_owned(stmt, sql)
            .map_err(|error| error.to_mysql_error().message)?;
        let StmtOutput::Rows { columns, rows } = output else {
            return Ok(None);
        };
        let fields = columns
            .into_iter()
            .map(|(name, field_type)| {
                tidb_model::GoShared::new(tidb_resolve::ResultField {
                    column: Some(tidb_model::GoShared::new(tidb_model::ColumnInfo {
                        name: tidb_ast::CiString::new(name.clone()),
                        field_type,
                        ..tidb_model::ColumnInfo::default()
                    })),
                    column_as_name: tidb_ast::CiString::new(name),
                    ..tidb_resolve::ResultField::default()
                })
            })
            .collect();
        Ok(Some(Box::new(tidb_sqlexec::SimpleRecordSet::new(fields, rows, 1024))))
    }
}

impl replayer::PlanReplayerSessionContext for SessionSql<'_> {
    fn get_sql_executor(&self) -> &dyn tidb_sqlexec::SqlExecutor {
        self
    }

    fn get_restricted_sql_executor(&self) -> &dyn tidb_sqlexec::RestrictedSqlExecutor {
        self
    }

    fn set_in_plan_replayer(&self, in_plan_replayer: bool) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .vars
            .in_plan_replayer = in_plan_replayer;
    }
}

/// Go `infosync.GetServerInfo()` for the plan replayer's `instance` column.
struct SessionServerInfo(Option<std::sync::Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>>);

impl replayer::ServerInfoSource for SessionServerInfo {
    fn get_server_info(&self) -> Result<replayer::ServerInfo, replayer::PlanReplayerError> {
        let syncer = self.0.as_ref().ok_or_else(|| replayer::PlanReplayerError::Other("the server info syncer is not set up".to_owned()))?;
        let info = syncer.get_local_server_info();
        Ok(replayer::ServerInfo { ip: info.static_info.ip.clone(), port: info.static_info.port as u64 })
    }
}

/// Go `sctx.GetRestrictedSQLExecutor()` of the dumping session: on this
/// session under `ExecOptionUseCurSession`, on a fresh internal session of
/// its node otherwise (Go `withRestrictedSQLExecutor`).
impl tidb_sqlexec::RestrictedSqlExecutor for SessionSql<'_> {
    fn parse_with_params(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<tidb_ast::Stmt, tidb_sqlexec::SqlExecError> {
        let sql = tidb_sqlexec::sql_with_params(sql, arguments)?;
        tidb_parser::parse(&sql).map_err(|parse| parse.compatibility_message(&sql).into())
    }

    fn exec_restricted_stmt(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
        options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>), tidb_sqlexec::SqlExecError> {
        let sql = String::from_utf8(statement.original_text().to_vec())?;
        self.exec_restricted_sql(context, options, &sql, &[])
    }

    fn exec_restricted_sql(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        options: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>), tidb_sqlexec::SqlExecError> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .execute_restricted_sql(context, options, sql, arguments)
    }
}

/// Go `sctx.GetSQLExecutor()` of the dumping session.
impl tidb_sqlexec::SqlExecutor for SessionSql<'_> {
    fn execute(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        Ok(self.record_set(sql)?.into_iter().collect())
    }

    fn execute_internal(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        self.record_set(&tidb_sqlexec::sql_with_params(sql, arguments)?)
    }

    fn execute_stmt(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        self.record_set(&String::from_utf8(statement.original_text().to_vec())?)
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

    fn stats_json_for_table(&self, db: &str, table: &str) -> Result<String, replayer::PlanReplayerError> {
        let error = |error: DriverError| replayer::PlanReplayerError::Other(error.to_mysql_error().message);
        let service = self
            .0
            .as_ref()
            .ok_or_else(|| replayer::PlanReplayerError::Other("this node has no statistics to dump".to_owned()))?;
        let json = service.plan_replayer_stats_json(db, table).map_err(error)?;
        // Go `json.Marshal(jsonTbl)`.
        serde_json::to_string(&json).map_err(|e| replayer::PlanReplayerError::Other(e.to_string()))
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
struct LocalPresign(Option<tidb_planner_extstore::ExtStorage>);

impl replayer::PresignSource for LocalPresign {
    fn has_global_storage(&self) -> bool {
        self.0.is_some()
    }
    fn presign_file(&self, path: &str, expire: std::time::Duration) -> Result<String, String> {
        self.0
            .as_ref()
            .ok_or_else(|| "no global external storage".to_owned())?
            .presign_file(&tidb_gocontext::Context::background(), path, expire)
            .map_err(|error| error.to_string())
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

impl Session {
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
        let variables = self.plan_replayer_sys_var_registry();
        let mut archive = ArchiveWriter(tidb_replayer::zip::ZipWriter::new(Vec::new()));
        let outcome = {
            let sql = SessionSql(std::sync::Mutex::new(&mut *self));
            let seams = replayer::DumpSeams {
                sql: &sql,
                extract_schema: &catalog,
                view_parser: &ViewParser,
                stats: &stats,
                mem_stats: &stats,
                tiflash: &catalog,
                presign: &presign,
                variables: &variables,
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
        let values = tidb_variable::sysvar::sys_vars()
            .into_iter()
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
        for definition in tidb_variable::sysvar::sys_vars() {
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

    /// Go `domain.GetDomain(sctx).GetPlanReplayerHandle()`.
    fn plan_replayer_handle(&self) -> Option<std::sync::Arc<PlanReplayerHandle>> {
        self.domain_services()?.domain.upgrade()?.plan_replayer_handle()
    }

    /// Go `insertPlanReplayerStatus` on the dumping session's restricted
    /// executor, the `instance` column from `infosync.GetServerInfo()`.
    fn insert_plan_replayer_status(&mut self, records: &[replayer::PlanReplayerStatusRecord]) {
        let info = SessionServerInfo(self.server_info_syncer.clone());
        let exec = SessionSql(std::sync::Mutex::new(self));
        replayer::insert_plan_replayer_status(&exec, &info, records);
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
    ) -> Result<String, replayer::PlanReplayerError> {
        let storage = tidb_planner_extstore::get_global_ext_storage().map_err(replayer::PlanReplayerError::Other)?;
        let (file, file_name) = tidb_replayer::generate_plan_replayer_file(
            storage.as_ref(),
            is_capture,
            is_continues_capture,
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

/// Go `SetupExtractHandle`'s worker session (`createSessions(store, 1)`):
/// a system session the extract worker runs each task on. It is opened per
/// task, as the plan-replayer dump workers' sessions are, so the domain
/// holds no session of its own.
pub struct ExtractWorkerSession {
    open_session: Box<dyn Fn() -> Result<Session, String> + Send + Sync>,
}

impl ExtractWorkerSession {
    /// A worker whose session `open_session` opens.
    pub fn new(open_session: impl Fn() -> Result<Session, String> + Send + Sync + 'static) -> Self {
        Self { open_session: Box::new(open_session) }
    }
}

impl tidb_domain::extract::ExtractSessionContext for ExtractWorkerSession {
    fn with_seams(
        &mut self,
        run: &mut dyn FnMut(&tidb_domain::extract::ExtractSeams<'_>) -> Result<String, replayer::PlanReplayerError>,
    ) -> Result<String, replayer::PlanReplayerError> {
        let mut session = (self.open_session)().map_err(replayer::PlanReplayerError::Other)?;
        let catalog = CatalogSource(
            session
                .with_catalog_mut(|catalog| Ok(catalog.clone()))
                .map_err(|error| replayer::PlanReplayerError::Other(error.to_mysql_error().message))?,
        );
        let stats = StatsSource(session.domain_services().map(|services| (services.stats)(&session)));
        let variables = session.plan_replayer_sys_var_registry();
        let session_vars = session.plan_replayer_session_vars();
        let sql = SessionSql(std::sync::Mutex::new(&mut session));
        let seams = tidb_domain::extract::ExtractSeams {
            sql: &sql,
            extract_schema: &catalog,
            view_parser: &ViewParser,
            stats: &stats,
            tiflash: &catalog,
            variables: &variables,
            session_vars,
        };
        run(&seams)
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

/// Go `PlanReplayerExec`'s `sctx`.
impl tidb_executor::plan_replayer::PlanReplayerSession for Session {
    fn statement_read_ts(&self) -> u64 {
        self.statement_snapshot.as_ref().map_or(0, |(ts, _)| *ts)
    }

    fn plan_replayer_load_info(&mut self) -> &mut Option<String> {
        &mut self.plan_replayer_load_info
    }

    fn plan_replayer_dump_info(
        &mut self,
    ) -> &mut Option<tidb_executor::plan_replayer::PlanReplayerDumpInfo> {
        &mut self.plan_replayer_dump_info
    }

    fn plan_replayer_session_vars(&self) -> replayer::PlanReplayerSessionVars {
        Session::plan_replayer_session_vars(self)
    }

    fn dump_plan_replayer_info(
        &mut self,
        task: &mut replayer::PlanReplayerDumpTask,
        zf: &mut dyn tidb_replayer::WriteCloser,
    ) -> Result<(), replayer::PlanReplayerError> {
        Session::dump_plan_replayer_info(self, task, zf)
    }

    fn set_last_plan_replayer_token(&mut self, token: String) {
        self.vars.last_plan_replayer_token = token;
    }

    fn exec_restricted_sql(&mut self, source: &str, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        Session::exec_restricted_sql(self, source, sql)
    }

    fn collect_plan_replayer_task(&self) -> Result<(), DriverError> {
        Session::collect_plan_replayer_task(self)
    }

    fn parse_plan_replayer_sql(&mut self, sql: &str) -> Result<(), String> {
        ResetContextOfStmt::parse_statement(self, sql).map(drop).map_err(|error| error.error_string())
    }
}

impl tidb_executor::exec_stmt_observation::PlanReplayerCapture for Session {
    fn plan_replayer_capture_tasks(&self) -> Option<Vec<tidb_replayer::PlanReplayerTaskKey>> {
        self.plan_replayer_handle().map(|handle| handle.collector.get_tasks())
    }

    fn send_plan_replayer_task(&self, task: replayer::PlanReplayerDumpTask) {
        if let Some(handle) = self.plan_replayer_handle() {
            handle.send_task(task);
        }
    }

    fn session_binding_records(&self) -> Vec<Vec<String>> {
        self.session_bindings
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
            .collect()
    }
}
