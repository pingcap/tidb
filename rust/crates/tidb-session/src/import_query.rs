// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! The session half of Go `pkg/executor/importer/query_source.go`
//! (`ParseImportQuery`, `CaptureQuerySource`, `RestoreStates`'s session) and
//! Go `pkg/executor/import_into_query.go` (`RunImportQuery`): the SELECT of
//! IMPORT INTO ... FROM SELECT captured on the submitter and run on a DXF
//! worker (`ImportStepQuery`).

use tidb_driver_error::DriverError;
use tidb_executor::importer::chunk_process::QueryChunk;
use tidb_executor::importer::query_source::{self, QuerySource, QuerySourceTableKind, SourceTable};
use tidb_executor::stmt_ctx_build::ResetContextOfStmt as _;
use tidb_gocontext::Context;
use tidb_terror::MysqlError;

use crate::Session;

fn unknown(error: impl std::fmt::Display) -> DriverError {
    DriverError::from(MysqlError::unknown(error.to_string()))
}

impl tidb_executor::importer::query_source::QueryStatesSession for Session {
    fn status(&self) -> u32 {
        if self.is_autocommit() { 2 } else { 0 }
    }

    fn decode_states(&mut self, states: &tidb_sessionstates::SessionStates) -> Result<(), String> {
        Session::decode_states(self, states.clone()).map_err(|e| e.to_string())
    }
}

impl Session {
    /// Go `importer.ParseImportQuery`: parses an IMPORT INTO ... FROM SELECT
    /// statement with the session's parser config and SQL mode, and returns
    /// the statement and its SELECT.
    pub(crate) fn parse_import_query(
        &self,
        sql: &str,
    ) -> Result<(tidb_ast::ImportIntoStmt, tidb_ast::QueryStmt), DriverError> {
        let charset = self.vars.get_system("character_set_connection").unwrap_or_else(|_| "utf8mb4".to_owned());
        let collation = self.vars.get_system("collation_connection").unwrap_or_else(|_| "utf8mb4_bin".to_owned());
        let node = tidb_parser::parse_with_sql_mode_config_and_connection(
            sql,
            tidb_sessionctx::scanner_sql_mode(self.vars.sql_mode()),
            self.vars.build_parser_config(),
            &charset,
            &collation,
        )
        .map_err(|error| unknown(error.compatibility_message(sql)))?;
        let tidb_ast::Stmt::Dml(dml) = node else {
            return Err(unknown("import query requires a SELECT"));
        };
        let tidb_ast::DmlStmt::ImportInto(stmt) = dml.into_inner() else {
            return Err(unknown("import query requires a SELECT"));
        };
        let tidb_ast::ImportSource::Select { query, .. } = &stmt.source else {
            return Err(unknown("import query requires a SELECT"));
        };
        let query = (**query).clone();
        Ok((*stmt, query))
    }

    /// Go `importer.CaptureQuerySource`: captures the SELECT of the IMPORT
    /// INTO statement `sql` that this session is running. It runs after the
    /// statement is planned and before the session runs another statement,
    /// since it takes the source tables the planner recorded in the
    /// statement's related tables; `stmt_ctx` is the statement's context.
    pub(crate) fn capture_query_source(
        &self,
        sql: &str,
        stmt_ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<QuerySource, DriverError> {
        let (_, node) = self.parse_import_query(sql)?;
        if stmt_ctx.is_staleness() || self.txn_is_stale_read() {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NOT_SUPPORTED_YET
                    .gen_with_stack_by_args(&["stale reads in IMPORT INTO FROM SELECT".into()]),
            ));
        }
        // The planner does not resolve the sequences of sequence functions.
        if query_source::has_sequence_func(&node) {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NOT_SUPPORTED_YET
                    .gen_with_stack_by_args(&["sequences in IMPORT INTO FROM SELECT".into()]),
            ));
        }
        let session_states = self.store_query_states(stmt_ctx)?;
        let mut q = QuerySource {
            encode_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64),
            session_states: Some(session_states),
            schema_version: self.txn_info_schema_version()?,
            in_mview_maintenance: self.vars.in_mview_maintenance,
            ..QuerySource::default()
        };
        // Like the plan cache, take the tables that the planner resolved for
        // the statement, including those in the views it expands.
        let ids: Vec<i64> = self
            .stmt_related_table_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        // Go `TxnManager.GetTxnInfoSchema()`: the transaction's schema with
        // the session's local temporary tables.
        q.tables = self.with_planning_catalog(|is| ids.iter().map(|&id| query_source_table(is, id)).collect())?;
        q.read_ts = stmt_ctx
            .txn_store()
            .map(|store| store.start_ts())
            .ok_or_else(|| unknown("the import statement has no transaction to read"))?;
        Ok(q)
    }

    /// Go `executor.PlanImportQueryForTest`: plans the SELECT of the IMPORT
    /// INTO statement `sql` like the statement does, so that
    /// [`Self::capture_query_source`] can capture it; answers the
    /// statement's context.
    pub fn plan_import_query_for_test(&mut self, sql: &str) -> Result<tidb_sessionctx::StmtContext, DriverError> {
        let (import_stmt, node) = self.parse_import_query(sql)?;
        // Preprocess the IMPORT INTO statement as it is, then plan its SELECT.
        tidb_planner::preprocess::check_param_marker(&import_stmt).map_err(DriverError::from)?;
        // Each statement starts with no related tables.
        self.stmt_related_table_ids.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
        let stmt = tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::ImportInto(Box::new(import_stmt))));
        // Go's preprocessor (`staleReadProcessor.OnSelectTable`) marks a
        // statement whose SELECT reads a table AS OF a time stale.
        let ctx = self.statement_context_for_stmt(&stmt, false).with_staleness(has_stale_read(&node));
        let current_db = self.current_database().to_owned();
        self.with_planning_catalog(|catalog| {
            tidb_executor::driver::plan_import_query(&node, catalog, &current_db, &ctx, self)
        })?;
        Ok(ctx)
    }

    /// Go `storeQueryStates`: the states of the submitting session for the
    /// worker.
    fn store_query_states(
        &self,
        stmt_ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<tidb_sessionstates::SessionStates, DriverError> {
        let mut states = self.encode_session_states()?;
        let timestamp = self.vars.get_system("timestamp").map_err(crate::variables::var_error)?;
        states.system_vars.insert("timestamp".to_owned(), timestamp);
        states.resource_group_name = stmt_ctx.resource_group_name().to_owned();
        Ok(states)
    }

    /// Go `executor.RunImportQuery`: runs the SELECT of the IMPORT INTO
    /// statement `import_sql` at `q.ReadTS`, sending its result chunks to
    /// `output`. The session takes the submitter's session states, so the
    /// caller destroys it.
    pub fn run_import_query(
        &mut self,
        ctx: &Context,
        q: &QuerySource,
        import_sql: &str,
        mem_quota: i64,
        output: &crossbeam_channel::Sender<QueryChunk>,
    ) -> Result<(), DriverError> {
        let (import_stmt, node) = self.prepare_import_query(q, import_sql)?;
        let result = self.run_prepared_import_query(ctx, &import_stmt, &node, mem_quota, output);
        // The stale transaction ends with the query, so the session is no
        // longer in a transaction.
        self.discard_stale_statement_transaction();
        result
    }

    /// Go `prepareImportQuery`: prepares the session to run the SELECT of
    /// an import query at `q.ReadTS` over the info schema that holds only
    /// the source tables at `q.ReadTS`.
    fn prepare_import_query(
        &mut self,
        q: &QuerySource,
        import_sql: &str,
    ) -> Result<(tidb_ast::Stmt, tidb_ast::QueryStmt), DriverError> {
        q.restore_states(self).map_err(unknown)?;
        if tidb_config_kerneltype::is_next_gen() {
            // Optimization can execute uncorrelated subqueries and cache
            // their DistSQL context, so select the execution group before
            // compilation starts.
            self.vars.resource_group = tidb_model::DEFAULT_RESOURCE_GROUP_NAME.to_owned();
        }
        // Without a statistics provider, this worker uses static partition
        // pruning.
        self.vars.store_partition_prune_mode("static".to_owned());
        self.vars.in_mview_maintenance = q.in_mview_maintenance;
        self.vars.restricted_sql = true;
        self.vars.request_source_type = tidb_txnkv::INTERNAL_DIST_TASK.to_owned();
        let domain = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .ok_or_else(|| DriverError::unsupported("the session has no domain to read the import query schema from"))?;
        let parent = self.lock_catalog()?.clone();
        let is = crate::domain::cluster_session_node::build_import_query_info_schema(&domain, q, &parent)?;
        let (import_stmt, node) = self.parse_import_query(import_sql)?;
        // Go `Compile`'s preprocessor over the SELECT.
        tidb_planner::preprocess::check_param_marker(&node).map_err(DriverError::from)?;
        // The statement reads the minimal info schema through the stale
        // provider (Go `EnterNewTxnWithReplaceProvider` with a
        // `StalenessTxnContextProvider` over `is`), and `SnapshotTS` /
        // `SnapshotInfoschema` are the query's.
        self.vars.restore_snapshot_ts(q.read_ts);
        self.snapshot_schema = Some((q.read_ts, is));
        self.open_stale_transaction(q.read_ts, false)?;
        Ok((tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::ImportInto(Box::new(import_stmt)))), node))
    }

    /// Go `buildImportQueryExecutor` and the rest of `RunImportQuery`.
    fn run_prepared_import_query(
        &mut self,
        ctx: &Context,
        import_stmt: &tidb_ast::Stmt,
        node: &tidb_ast::QueryStmt,
        mem_quota: i64,
        output: &crossbeam_channel::Sender<QueryChunk>,
    ) -> Result<(), DriverError> {
        // Reset with the IMPORT INTO statement, so the SELECT is evaluated
        // under the same statement flags as classic IMPORT INTO.
        let mut stmt_ctx = self.statement_context_for_stmt(import_stmt, false);
        if tidb_config_kerneltype::is_next_gen() {
            // Go resets `StmtCtx.ResourceGroupName` after compilation, which
            // may apply a RESOURCE_GROUP hint from the captured SELECT: the
            // query runs in the default group either way.
            stmt_ctx = stmt_ctx.with_resource_group_name(tidb_model::DEFAULT_RESOURCE_GROUP_NAME);
        }
        let current_db = self.current_database().to_owned();
        let mut physical = self.with_planning_catalog(|catalog| {
            tidb_executor::driver::plan_import_query(node, catalog, &current_db, &stmt_ctx, self)
        })?;
        // Apply the quota after compilation, which may apply a
        // SET_VAR(tidb_mem_quota_query) hint.
        self.vars.mem_quota_query = mem_quota;
        stmt_ctx.statement_memory().session_tracker().set_bytes_limit(mem_quota);
        // Go passes the physical plan, whose `SCtx()` is this session.
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", self);
        let sent = self.with_planning_catalog(|catalog| {
            let mut source = tidb_executor::ImportSelectSource::open(&mut physical, catalog, &stmt_ctx)?;
            send_query_chunks(ctx, &mut source, output)
        });
        self.drain_eval_warnings(&stmt_ctx);
        sent
    }
}

/// Whether a table of `node` is read `AS OF` a time (Go
/// `staleReadProcessor.OnSelectTable`'s stale read).
fn has_stale_read(node: &tidb_ast::QueryStmt) -> bool {
    struct Finder {
        found: bool,
    }
    impl tidb_ast::Visitor for Finder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.downcast_ref::<tidb_ast::TableRef>().is_some_and(|table| table.as_of.is_some()) {
                self.found = true;
            }
            self.found
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            !self.found
        }
    }
    let mut node = node.clone();
    let mut finder = Finder { found: false };
    tidb_ast::Visitable::accept(&mut node, &mut finder);
    finder.found
}

/// Go `is.TableByID` and `checkQuerySourceTable` for a related table.
fn query_source_table(is: &tidb_infoschema::SessionExtendedInfoSchema, id: i64) -> Result<SourceTable, DriverError> {
    let (db_name, info) = if let Some((db, table)) = is.table_by_id(id) {
        (Some(db.to_owned()), table.meta().read().clone())
    } else if let Some(info) = is.table_info_by_id(id) {
        (is.schema_name_by_table_id(id), info.read().clone())
    } else {
        return Err(unknown(format!("source table {id} not found in the info schema")));
    };
    let kind = QuerySourceTableKind {
        temporary: info.temp_table_type != tidb_model::TempTableType::NONE,
        virtual_table: db_name.is_some_and(|db| tidb_metadef::is_mem_db(&tidb_hack::go_to_lower(&db))),
        sequence: info.is_sequence(),
    };
    query_source::check_query_source_table(&kind).map_err(DriverError::from)?;
    Ok(SourceTable { db_id: info.db_id, id: info.id, revision: info.revision })
}

/// Go `sendQueryChunks`: sends the rows of `source` to `output` in chunks,
/// which the encoders own after receiving them.
fn send_query_chunks(
    ctx: &Context,
    source: &mut tidb_executor::ImportSelectSource,
    output: &crossbeam_channel::Sender<QueryChunk>,
) -> Result<(), DriverError> {
    let mut row_id = 0i64;
    while let Some(rows) = source.next_chunk()? {
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len() as i64;
        let mut chunk = QueryChunk { rows, row_id_offset: row_id };
        loop {
            if let Some(e) = ctx.err() {
                return Err(unknown(e));
            }
            match output.send_timeout(chunk, std::time::Duration::from_millis(50)) {
                Ok(()) => break,
                Err(crossbeam_channel::SendTimeoutError::Timeout(back)) => chunk = back,
                Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => return Err(unknown("context canceled")),
            }
        }
        row_id += count;
    }
    Ok(())
}

/// Registers Go `executor.RunImportQuery` on a DXF worker's system session
/// as this instance's import query runner, next to `importinto`'s task type
/// registration.
pub fn register_import_query_runner() {
    tidb_dxf_importinto::set_import_query_runner(crate::domain::cluster_session_node::run_import_query_on_worker);
}
