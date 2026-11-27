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

//! Statement dispatch: the arms that send one parsed statement to the executor
//! entry point that answers it.
//!
//! This is the body of Go's `session.ExecuteStmt` after the statement-boundary
//! bookkeeping [`Session::run_with_columns`] does around it. The order of the
//! doors matters and is Go's: the schema/account arm runs first (it carries
//! DDL's implicit commit), then transaction control and `SET`, then the parsed
//! dispatch over query/DML/DDL.

use tidb_executor::ddl_exec::TemporaryTableDdlExec;
use tidb_executor::mem_table_retrieval::StmtMemTableRetrieval as _;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::compiler::PrepareExec;
use tidb_planner::optimize::PlanCacheContext as _;
use tidb_executor::compiler::PlanCacheableChecker;
use tidb_ast::{DmlStmt, SessionStmt, Stmt};
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_driver_error::DriverError;

pub(crate) use tidb_executor::compiler::explain::variant_name;
use crate::record_set::{PendingExecution, PendingQuery, QueryTransactionEnd};
use crate::{
    statement_kind_of, Session, StatementKind, StmtOutput, WarningLevel,
};

/// The `SELECT <exprs>` whose execution is Go `buildDo`'s Projection over a
/// one-row TableDual.
fn do_projection_query(exprs: &[tidb_ast::Expr]) -> Stmt {
    let mut fields = tidb_ast::SelectFieldList::default();
    for expr in exprs {
        fields.push(tidb_ast::SelectField::Expr {
            expr: expr.clone(),
            alias: None,
        });
    }
    Stmt::Query(tidb_ast::NodeBox::new(tidb_ast::QueryStmt::Select(
        Box::new(tidb_ast::SelectStmt {
            kind: Default::default(),
            is_in_braces: false,
            with_before_braces: false,
            with: None,
            hints: Vec::new(),
            priority: Default::default(),
            sql_small_result: false,
            sql_big_result: false,
            sql_buffer_result: false,
            sql_no_cache: false,
            straight_join: false,
            calc_found_rows: false,
            distinct: false,
            all: false,
            fields,
            values: Vec::new(),
            from: None,
            where_clause: None,
            group_by: Vec::new(),
            rollup: false,
            having: None,
            windows: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            lock: None,
            into_outfile: None,
            query_block_offset: 0,
            as_view_schema: false,
        }),
    )))
}

fn sem_table_option(option: &tidb_ast::TableOption) -> tidb_sem_v2::TableOptionType {
    match option {
        tidb_ast::TableOption::Ttl { .. } => tidb_sem_v2::TableOptionType::Ttl,
        tidb_ast::TableOption::TtlEnable(_) => tidb_sem_v2::TableOptionType::TtlEnable,
        tidb_ast::TableOption::TtlJobInterval(_) => {
            tidb_sem_v2::TableOptionType::TtlJobInterval
        }
        _ => tidb_sem_v2::TableOptionType::Other,
    }
}

fn sem_stmt_kind(stmt: &Stmt) -> tidb_sem_v2::StmtKind {
    use tidb_sem_v2::{AlterTableSpec, AlterTableType, StmtKind};

    match stmt {
        Stmt::Query(query) => match query.as_ref() {
            tidb_ast::QueryStmt::Select(select) => StmtKind::Select {
                select_into: select.into_outfile.is_some(),
            },
            tidb_ast::QueryStmt::SetOpr(_) => StmtKind::Other,
        },
        Stmt::Dml(dml) => match dml.as_ref() {
            tidb_ast::DmlStmt::ImportInto(import) => match &import.source {
                tidb_ast::ImportSource::File { path, .. } => StmtKind::ImportInto {
                    from_select: false,
                    path: path.clone(),
                },
                tidb_ast::ImportSource::Select { .. } => StmtKind::ImportInto {
                    from_select: true,
                    path: String::new(),
                },
            },
            tidb_ast::DmlStmt::LoadData(load) => StmtKind::LoadData {
                file_loc_client: load.local,
                path: load.path.clone(),
            },
            _ => StmtKind::Other,
        },
        Stmt::Ddl(ddl) => match ddl.as_ref() {
            tidb_ast::DdlStmt::CreateTable(create) => StmtKind::CreateTable {
                options: create.table_options.iter().map(sem_table_option).collect(),
            },
            tidb_ast::DdlStmt::AlterTable(alter) => StmtKind::AlterTable {
                specs: alter
                    .actions
                    .iter()
                    .map(|action| match action {
                        tidb_ast::AlterTableAction::RemoveTtl(_) => AlterTableSpec {
                            tp: AlterTableType::RemoveTtl,
                            options: Vec::new(),
                        },
                        tidb_ast::AlterTableAction::SetTableOptions { options } => AlterTableSpec {
                            tp: AlterTableType::Option,
                            options: options.iter().map(sem_table_option).collect(),
                        },
                        tidb_ast::AlterTableAction::SetAttributes(_) => AlterTableSpec {
                            tp: AlterTableType::Attributes,
                            options: Vec::new(),
                        },
                        tidb_ast::AlterTableAction::Partition(
                            tidb_ast::AlterPartitionAction::SetAttributes { .. },
                        ) => AlterTableSpec {
                            tp: AlterTableType::PartitionAttributes,
                            options: Vec::new(),
                        },
                        _ => AlterTableSpec {
                            tp: AlterTableType::Other,
                            options: Vec::new(),
                        },
                    })
                    .collect(),
            },
            _ => StmtKind::Other,
        },
        Stmt::Admin(_) | Stmt::Session(_) => StmtKind::Other,
    }
}

fn sem_stmt_view(stmt: &Stmt) -> tidb_sem_v2::StmtView {
    tidb_sem_v2::StmtView {
        sem_command: stmt.sem_command().to_owned(),
        kind: sem_stmt_kind(stmt),
    }
}

/// Whether a query contains a locking clause. Go's `StmtCtx.IsReadOnly` is
/// false for every locking read, including a lock attached to a nested query
/// or set-operation wrapper, so those statements must populate the MDL map
/// even under autocommit.
pub(crate) fn query_has_lock(stmt: &Stmt) -> bool {
    struct LockFinder {
        found: bool,
    }
    impl tidb_ast::Visitor for LockFinder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<tidb_ast::SelectLock>() {
                self.found = true;
            }
            self.found
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut statement = stmt.clone();
    let mut finder = LockFinder { found: false };
    tidb_ast::Visitable::accept(&mut statement, &mut finder);
    finder.found
}

/// A planner-owned SELECT tree offered to the ordinary statement executor.
/// `used` records whether the schema still matched while the catalog was
/// locked; a moved schema falls through to ordinary physical planning.
struct RetainedSelectPlan<'a> {
    physical: &'a mut tidb_planner_core_operator_physicalop::PhysicalPlan,
    schema_version: u64,
    used: &'a mut bool,
}

// The statement compiles this thread is inside, innermost last: whether each
// compiles an EXECUTE, and whether it is the prepared statement an EXECUTE
// runs (which Go plans inside the EXECUTE's compile).
thread_local! {
    static COMPILES: std::cell::RefCell<Vec<(bool, bool)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// One statement compile on this thread; see [`COMPILES`].
struct CompileDepth {
    nested: bool,
}

impl CompileDepth {
    fn enter(stmt: &Stmt) -> Self {
        let is_execute = matches!(
            stmt,
            Stmt::Session(session) if matches!(&**session, SessionStmt::Execute { .. })
        );
        let nested = COMPILES.with(|compiles| {
            let mut compiles = compiles.borrow_mut();
            let nested = compiles.last().is_some_and(|(execute, _)| *execute);
            compiles.push((is_execute, nested));
            nested
        });
        Self { nested }
    }

    /// How many statement compiles this thread is inside.
    fn depth() -> usize {
        COMPILES.with(|compiles| compiles.borrow().len())
    }

    /// Whether the compile running now is a prepared statement's inside an
    /// EXECUTE's.
    fn is_nested() -> bool {
        COMPILES.with(|compiles| compiles.borrow().last().is_some_and(|(_, nested)| *nested))
    }
}

impl Drop for CompileDepth {
    fn drop(&mut self) {
        COMPILES.with(|compiles| compiles.borrow_mut().pop());
    }
}

/// Go `ExecStmt` for an INSERT / UPDATE / DELETE the funnel compiled
/// ([`Session::compile_dml`]): the statement as compiled (its variables bound,
/// its binding applied), the database it resolved names in, its final plan,
/// the statement's hints, and where its warnings began. A pessimistic retry
/// executes the same plan again (Go `handlePessimisticLockError` re-runs
/// `buildExecutor` over `ExecStmt.Plan`).
pub(crate) struct CompiledDml {
    stmt: Stmt,
    current_db: String,
    plan: CompiledDmlPlan,
    stmt_hints: tidb_hint::StmtHints,
    warnings_start: usize,
}

/// A locking SELECT the funnel compiled ([`Session::compile_locking_query`]):
/// the statement, its database, its final plan, the statement's hints, and
/// where its warnings began. Go `ExecStmt` for a `SELECT ... FOR UPDATE`,
/// whose pessimistic retry opens the same plan again.
pub(crate) struct CompiledQuery {
    stmt: Stmt,
    current_db: String,
    physical: tidb_planner_core_operator_physicalop::PhysicalPlan,
    stmt_hints: tidb_hint::StmtHints,
    warnings_start: usize,
}

/// The statements whose compiled form outlives an execution for a
/// pessimistic retry (Go `ExecStmt`).
pub(crate) enum ExecStmt {
    Dml(CompiledDml),
    Query(CompiledQuery),
}

enum CompiledDmlPlan {
    Insert(tidb_executor::driver::CompiledInsert),
    Update(tidb_planner_core_operator_physicalop::PhysicalPlan),
    Delete(tidb_planner_core_operator_physicalop::PhysicalPlan),
}

/// An INSERT, UPDATE or DELETE: the statements the funnel compiles to a
/// [`CompiledDml`].
fn compiles_to_dml_plan(stmt: &Stmt) -> bool {
    matches!(stmt, Stmt::Dml(dml) if matches!(
        dml.as_ref(),
        DmlStmt::Insert(_) | DmlStmt::Update(_) | DmlStmt::Delete(_)
    ))
}

fn cached_dml_plan<'a>(
    plan: Option<&'a mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    operator: &str,
) -> Result<Option<&'a mut tidb_planner_core_operator_physicalop::PhysicalPlan>, DriverError> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(dml) = &*plan else {
        return Err(DriverError::unsupported(
            "prepared DML execution received a non-DML physical root",
        ));
    };
    if !dml.go_operator.eq_ignore_ascii_case(operator) {
        return Err(DriverError::unsupported(format!(
            "prepared {operator} execution received a {} physical root",
            dml.go_operator
        )));
    }
    Ok(Some(plan))
}
/// Go `tryLockMDLAndUpdateSchemaIfNecessary` for a table the transaction has
/// not bound yet: the table is read from the latest schema; a public index
/// the pinned image lacks is demoted to write-reorganization and a public
/// column whose ID changed refuses the statement, unless the transaction is
/// pessimistic read-committed; the transaction's image then reads that table
/// (`UpdateTableInfo`). Returns the table and the latest schema version to
/// bind it at.
fn update_schema_if_necessary(
    catalog: &mut tidb_infoschema::SessionExtendedInfoSchema,
    latest: &dyn crate::LatestInfoSchema,
    db: &str,
    table: &str,
    pessimistic_read_consistency: bool,
) -> Result<(i64, i64, bool), DriverError> {
    let latest_is = latest.latest();
    let domain_schema_ver = latest_is.schema_meta_version();
    let key = tidb_infoschema::CatalogTableKey::new(db, table);
    let Some(entry) = latest_is.table_handle_by_key(&key) else {
        return Err(DriverError::Mysql(tidb_terror::MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .fast_gen_by_args(&[tidb_hack::GoToLower::go_to_lower(&db).into(), table.into()]),
        )));
    };
    let (tidb_infoschema::TableEntry::Kv(new_table), Some(tidb_infoschema::TableEntry::Kv(old_table))) =
        (entry.as_ref(), catalog.get_in(db, table))
    else {
        return Ok((
            catalog.related_table_id_for_mdl(db, table).flatten().unwrap_or_default(),
            domain_schema_ver,
            false,
        ));
    };
    let new_info = new_table.meta();
    let old_info = old_table.meta();
    let (new_id, new_revision) = { let info = new_info.read(); (info.id, info.revision) };
    let (old_id, old_revision) = { let info = old_info.read(); (info.id, info.revision) };
    if new_id == old_id && new_revision == old_revision {
        // The same table: Go's overlay would hold what the image holds.
        return Ok((new_id, domain_schema_ver, false));
    }
    let mut entry = entry;
    if new_revision != old_revision && !pessimistic_read_consistency {
        let old = old_info.read();
        let mut copy: Option<tidb_model::TableInfo> = None;
        {
            let new = new_info.read();
            for (i, index) in new.indices.iter_deref().enumerate() {
                let index = index.read();
                if index.state != tidb_model::SchemaState::PUBLIC {
                    continue;
                }
                let found = old
                    .indices
                    .iter_deref()
                    .find(|old_index| old_index.read().name.lowercase() == index.name.lowercase())
                    .map(|old_index| old_index.read().id);
                if found != Some(index.id) {
                    let copy = copy.get_or_insert_with(|| new.clone_like_go());
                    if let Some(demoted) = copy.indices.iter_deref().nth(i) {
                        demoted.write().state = tidb_model::SchemaState::WRITE_REORGANIZATION;
                    }
                }
            }
            for column in new.columns.iter_deref() {
                let column = column.read();
                if column.state != tidb_model::SchemaState::PUBLIC {
                    continue;
                }
                let old_column = old
                    .columns
                    .iter_deref()
                    .find(|old_column| old_column.read().name.lowercase() == column.name.lowercase());
                if old_column.is_some_and(|old_column| old_column.read().id != column.id) {
                    tidb_log::info(
                        "public column changed",
                        &[
                            tidb_log::Field::new("column", tidb_log::Value::Str(column.name.lowercase().to_owned())),
                            tidb_log::Field::new("new id", tidb_log::Value::I64(column.id)),
                        ],
                    );
                    return Err(DriverError::from(
                        tidb_domain::domain_errors::ERR_INFO_SCHEMA_CHANGED
                            .gen_with_stack(format!("public column {} has changed", column.name.original())),
                    ));
                }
            }
        }
        if let Some(copy) = copy {
            let db_id = latest_is.database_id(db).unwrap_or_default();
            let rebuilt = latest
                .table_from_meta(&copy, db_id)
                .map_err(DriverError::unsupported)?;
            entry = std::sync::Arc::new(tidb_infoschema::TableEntry::Kv(std::sync::Arc::new(rebuilt)));
        }
    }
    catalog.update_table_info(db, table, entry);
    Ok((new_id, domain_schema_ver, true))
}

/// Go preprocessor's `stmtTp == TypeSelect` when it reaches the statement's
/// tables, which is when `staleReadProcessor.OnSelectTable` runs: a SELECT,
/// or an EXPLAIN of one (`ExplainStmt` sets no statement type of its own).
fn reads_as_select(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Query(_) => true,
        Stmt::Admin(admin) => match admin.as_ref() {
            tidb_ast::AdminStmt::Explain(explain) => explain.statement().is_some_and(reads_as_select),
            _ => false,
        },
        _ => false,
    }
}

impl Session {
    /// The checks every compiled statement passes before its executor: the
    /// `tidb_snapshot` write refusal, the implicit commit Go's `DDLExec`,
    /// `GrantExec`, `RevokeExec` and `SimpleExec.autoNewTxn` make. (The staleness refusal precedes
    /// `Compile`, and the DDL preprocess runs inside it.)
    pub(crate) fn statement_preamble(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        self.check_txn_aborted(stmt)?;
        self.statement_is_staleness = reads_as_select(stmt) && self.is_staleness_txn();
        // Go `ExecStmt.Exec` verifies the statement provider's read timestamp.
        if self.statement_is_staleness {
            if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/assertStaleTSO") {
                let ts = self.historical_read_ts().expect("staleness statement read timestamp");
                assert_eq!((ts >> 18) / 1000, value.int() as u64, "different tso");
            }
        }
        // Go `executor.newLockCtx`: process start time plus the statement's
        // timeout (`getMaxExecutionDeadline`, Go `ExecStmt.getMaxExecutionTime`).
        let dml_ms = self.vars.dml_max_execution_time();
        let max_ms = match stmt {
            Stmt::Query(_) => {
                if self.stmt_hints.has_max_execution_time { self.stmt_hints.max_execution_time }
                else { self.vars.max_execution_time() }
            }
            Stmt::Dml(dml) if dml_ms > 0 => {
                let exempt = tidb_executor::stmt_ctx_build::ResetContextOfStmt::dml_timeout_exempt(self);
                let batch = match dml.as_ref() {
                    DmlStmt::Insert(_) => exempt.batch_insert,
                    DmlStmt::Delete(_) => exempt.batch_delete,
                    _ => false,
                };
                if exempt.all || batch { 0 } else { dml_ms }
            }
            _ => 0,
        };
        let mut kv_context = self.statement_kv_context.get();
        kv_context.max_execution_deadline = (max_ms > 0).then(|| {
            let elapsed = self.statement_observation.as_ref().map_or(std::time::Duration::ZERO, |obs| obs.started.elapsed());
            std::time::SystemTime::now() - elapsed + std::time::Duration::from_millis(max_ms)
        });
        self.statement_kv_context.publish(kv_context);
        tidb_executor::adapter::check_no_delay_snapshot_writes(self, stmt)?;
        if matches!(stmt, Stmt::Ddl(_)) && !self.ddl_preserves_transaction(stmt) {
            // Go commits the open transaction before ordinary DDL
            // (`session.ExecuteStmt`, which calls `sessiontxn`'s
            // `OnStmtStart` -> `checkBeforeNewTxn` for a DDL node). LOCAL
            // CREATE and local-only DROP are exceptions: `DDLExec.Next`
            // returns through the temporary-table owner before
            // `NewTxnInStmt`, retaining the user's transaction. For ordinary
            // DDL, everything staged before it is already durable when it
            // starts. Captured from TiDB: after
            // `INSERT; BEGIN; INSERT; TRUNCATE TABLE d; ROLLBACK` the table
            // is EMPTY -- the ROLLBACK takes nothing back, because the
            // TRUNCATE committed the insert that preceded it -- and the same
            // `ALTER TABLE ... AUTO_INCREMENT` sequence leaves the in-
            // transaction row stored.
            //
            // The session leaves the transaction here; the store session
            // commits it when the statement finishes
            // (`commit_if_session_left_transaction`, Go `autoCommitAfterStmt`).
            self.leave_txn_ctx_for_commit();
        }
        // Go `GrantExec.Next` and `RevokeExec.Next`, and `SimpleExec`'s
        // `autoNewTxn`, commit the open transaction before touching the
        // privilege tables, like DDL.
        if Self::simple_exec_new_txn_in_stmt(stmt) {
            self.leave_txn_ctx_for_commit();
        }
        Ok(())
    }

    /// Whether Go's executor for a `Simple` plan commits the open
    /// transaction with `NewTxnInStmt` before its work: `GrantExec` and
    /// `RevokeExec` (`grant.go:144`, `revoke.go:71`), and the statements
    /// `SimpleExec.autoNewTxn` lists (`simple.go:3564`): CREATE / ALTER /
    /// DROP / RENAME USER (CREATE and DROP ROLE are the same statements),
    /// GRANT / REVOKE ROLE and FLUSH. SET PASSWORD and SET DEFAULT ROLE are
    /// not among them.
    pub(crate) fn simple_exec_new_txn_in_stmt(stmt: &Stmt) -> bool {
        matches!(stmt, Stmt::Admin(admin) if matches!(
            &**admin,
            tidb_ast::AdminStmt::Grant(_)
                | tidb_ast::AdminStmt::Revoke(_)
                | tidb_ast::AdminStmt::GrantRole(_)
                | tidb_ast::AdminStmt::RevokeRole(_)
                | tidb_ast::AdminStmt::Flush(_)
                | tidb_ast::AdminStmt::CreateUser { .. }
                | tidb_ast::AdminStmt::CreateRole { .. }
                | tidb_ast::AdminStmt::AlterUser(_)
                | tidb_ast::AdminStmt::DropUser { .. }
                | tidb_ast::AdminStmt::RenameUser { .. }
        ))
    }

    /// Answers the memory-table scans the statement's executors asked for
    /// (`StmtContext::mem_table_retrieval`): Go's `MemTableReaderExec` calls
    /// its retriever when it is read; the retrievers here need this session,
    /// so the executor states what it needs and the session retrieves it.
    /// Returns how many scans were answered.
    pub(crate) fn retrieve_memory_tables(
        &mut self,
        ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<usize, DriverError> {
        let reads = ctx.mem_table_retrieval().take_pending();
        for read in &reads {
            // A scan an installed retriever serves reads its own rows when it
            // is first read.
            if ctx.mem_table_retrieval().retriever_for(read).is_some() {
                continue;
            }
            if let Some(retriever) = self.mem_table_retriever_for(read) {
                ctx.mem_table_retrieval().install(retriever);
                continue;
            }
            // No retriever builds this table: Go's `memtableRetriever.retrieve`
            // has no case for it either, so it reads as empty.
            ctx.mem_table_retrieval().supply(read, Vec::new());
        }
        Ok(reads.len())
    }

    /// The retriever a scan reads its rows through, for the tables that need
    /// only the statement's schema and a snapshot of this session
    /// ([`tidb_executor::mem_table_retrieval::MemTableRetriever`]).
    fn mem_table_retriever_for(
        &self,
        read: &tidb_executor::mem_table_retrieval::MemTableRead,
    ) -> Option<std::sync::Arc<dyn tidb_executor::mem_table_retrieval::MemTableRetriever>> {
        let retriever = crate::query_watch::RunawayWatchesRetriever(
            self.domain_services().and_then(|services| services.domain.upgrade()).and_then(|domain| domain.runaway_manager()),
        );
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = tidb_executor::memtable_retrievers::InformationSchemaRetriever::new(
            self.schema_visibility(),
            self.table_storage_stats.clone(),
        );
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = tidb_executor::memtable_reader::InstanceStateRetriever::new(self.viewer_state());
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = tidb_executor::memtable_retrievers::CatalogTablesRetriever::new(
            self.schema_visibility(),
            self.sem_hides_cluster_metadata(),
        );
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = tidb_executor::account_state::AccountRetriever::new(self.account_state());
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = crate::perfschema_retriever::PerfSchemaRetriever::new(self);
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = tidb_executor::account_state::VariablesRetriever::new(self.variables_state());
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = tidb_executor::cluster_state::ClusterTablesRetriever::new(self.viewer_state());
        tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read)
            .then(|| std::sync::Arc::new(retriever) as _)
    }

    /// Runs `attempt` over the session's catalog, retrieving the memory
    /// tables its plan reads and running it again when it asks for them
    /// (`DriverError::MemTableRetrievalNeeded`). The ask comes while the
    /// statement is planned (an uncorrelated subquery runs then, Go
    /// `EvalSubqueryFirstRow`) or before its executors are built, so nothing
    /// the attempt did needs undoing but its warnings: those are dropped and
    /// raised again by the rerun, and the retrievers' own go after them, in
    /// the order Go raises them (planning, then the read).
    pub(crate) fn with_catalog_retrieving<T>(
        &mut self,
        ctx: &tidb_sessionctx::StmtContext,
        mut attempt: impl FnMut(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.retrieving_memory_tables(ctx, |session| session.with_catalog_mut(&mut attempt))
    }

    /// [`Self::with_catalog_retrieving`] over any way of running the
    /// statement, such as a staged catalog for a write.
    pub(crate) fn retrieving_memory_tables<T>(
        &mut self,
        ctx: &tidb_sessionctx::StmtContext,
        mut run: impl FnMut(&mut Self) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        let warnings_start = ctx.warning_count();
        let mut retriever_warnings = Vec::new();
        loop {
            let result = run(self);
            if !matches!(result, Err(DriverError::MemTableRetrievalNeeded)) {
                ctx.extend_warnings(retriever_warnings);
                return result;
            }
            ctx.truncate_warnings(warnings_start);
            if self.retrieve_memory_tables(ctx)? == 0 {
                return result;
            }
            retriever_warnings.extend(ctx.split_off_warnings(warnings_start));
        }
    }

    /// Go's receiving cluster table reader. It borrows live process/history
    /// owners without registering a synthetic connection or redispatching.
    pub fn local_cluster_table_rows(
        &mut self,
        table_name: &str,
        registry: Option<&tidb_sessmgr::ProcessRegistry>,
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        self.local_cluster_table_rows_for(table_name, registry, zone, None, None)
    }

    /// Receiving scan requirements use original column names, including predicates.
    /// None retains full-row callers; a present empty set needs no expensive fields.
    pub fn local_cluster_table_rows_for(
        &mut self,
        table_name: &str,
        registry: Option<&tidb_sessmgr::ProcessRegistry>,
        zone: &tidb_datatype::SessionTimeZone,
        required: Option<&std::collections::HashSet<String>>,
        extractor: Option<&tidb_planner::memtable_predicate_extractor::MemTableExtractor>,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let ctx = self.statement_context(false);
        let viewer = self.viewer_state();
        let rows = self.with_catalog_mut(|catalog| {
            viewer.local_cluster_table_rows_for(
                &ctx, catalog, table_name, registry, zone, required, extractor,
            )
        });
        self.drain_eval_warnings(&ctx);
        rows
    }

    /// `@@tidb_distsql_scan_concurrency`, the statement-summary history
    /// reader's worker count.
    pub(crate) fn dist_sql_scan_concurrency(&self) -> usize {
        self.vars
            .get_system(tidb_vardef::tidb_vars::TIDB_DIST_SQL_SCAN_CONCURRENCY)
            .expect("registered DistSQL scan concurrency")
            .parse::<usize>()
            .expect("validated positive scan concurrency")
    }

    /// The catalog `information_schema.TABLES` / `PARTITIONS` read their size
    /// columns from: the statement's own schema image, with each table's
    /// sizes estimated from Go `TableRowStatsCache` through the node's
    /// provider, which serves
    /// its cache when a refresh fails; the in-process store answers from its
    /// own table statistics.
    ///
    /// Go's `setDataFromTables` / `setDataFromPartitions` walk the
    /// statement's InfoSchema and refresh the cache for exactly those
    /// physical IDs (`updateStatsCacheIfNeed(sctx, tbls)`), so a table the
    /// statement's snapshot holds is sized even when a later DDL dropped it.
    pub(crate) fn table_storage_statistics_catalog(
        &mut self,
    ) -> Result<tidb_infoschema::SessionExtendedInfoSchema, DriverError> {
        let scratch = self.with_catalog_mut(|catalog| Ok(catalog.clone()))?;
        tidb_executor::memtable_retrievers::table_storage_statistics_catalog_of(
            scratch,
            &self.stats_handle,
            self.table_storage_stats.as_deref(),
        )
    }

    pub fn execute_statement(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        let stmt = self.parse_at_statement_boundary(sql)?;
        self.execute_parsed_statement(sql, stmt, None)
    }

    /// [`Self::execute_statement`] over the statement a front end already
    /// parsed from `sql` (Go `ExecuteStmt` takes the `ast.StmtNode`
    /// `ParseSQL` produced; nothing parses the text again).
    pub(crate) fn execute_statement_owned(
        &mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        self.begin_text_statement_boundary(&stmt);
        self.execute_parsed_statement(sql, stmt, None)
    }

    /// Executes a statement tree already parsed and bound by the prepared
    /// protocol path.  The surrounding session lifecycle is still applied by
    /// `run_with_columns_using`; this seam only avoids reparsing the SQL text.
    pub(crate) fn execute_statement_parsed(
        &mut self,
        stmt: Stmt,
        sql: &str,
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        self.execute_prepared_ast(sql, stmt, privilege_requests)
    }

    /// `privilege_requests` are the ones PREPARE derived for this statement
    /// (Go `PlanCacheStmt.VisitInfos`, checked by `checkPreparedPriv`).
    pub(crate) fn execute_prepared_ast(
        &mut self,
        sql: &str,
        stmt: Stmt,
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        match self.prepare_bound_execution(sql, stmt, privilege_requests) {
            Ok(pending) => pending.collect(self),
            Err(error) => {
                // A statement that fails at plan time produced no record set,
                // so the record-set drain never runs: the fold's diagnostics
                // must still reach the statement's warning list beside the
                // error (go's FORMAT('x', 'y') keeps both coercion warnings
                // beside the 1582, and the error itself stays out of the
                // buffer on the read door).
                for warning in tidb_context::fold_warnings::take_fold_warnings() {
                    self.append_sql_warning(warning);
                }
                Err(error)
            }
        }
    }

    /// Go `PlanCacheTracker.warnSkipPlanCache` for a prepared execution:
    /// the statement's unqualified reason (the first one recorded) and the
    /// `skip prepared plan-cache: <reason>` warning.
    fn warn_skip_prepared_plan_cache(&mut self, reason: &str) {
        {
            let mut unqualified = self
                .statement_plan_cache_unqualified
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if unqualified.is_empty() {
                reason.clone_into(&mut unqualified);
            }
        }
        self.append_warning(
            tidb_context::WarningLevel::Warning,
            1105,
            format!("skip prepared plan-cache: {reason}"),
        );
    }

    pub(crate) fn prepare_bound_execution(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
    ) -> Result<PendingExecution, DriverError> {
        let parameters = tidb_executor::bound_parameter_values(&mut stmt)?;
        self.begin_prepared_statement_boundary(&stmt, parameters);
        if let Some(reason) = self.pending_plan_cache_refusal.take() {
            self.warn_skip_prepared_plan_cache(reason);
        }
        // The plan-cache refusal warnings land AFTER the boundary reset so
        // they survive it: go records the refusals on every EXECUTE (oracle
        // m21: PREPARE/EXECUTE of a DDL carry the not-a-SELECT skip row and
        // EXECUTE of `SELECT ? + 1` carries the TableDual skip row).
        let cacheable = {
            let catalog = self.lock_catalog()?;
            self.prepared_statement_cacheable(&mut stmt, &catalog)
        };
        if let Err(reason) = cacheable {
            self.warn_skip_prepared_plan_cache(&reason);
        } else if tidb_executor::compiler::prepared::plans_over_table_dual(&stmt) {
            self.warn_skip_prepared_plan_cache("get a TableDual plan");
        }
        self.prepare_parsed_statement_with_optional_physical_plan(
            sql,
            stmt,
            Some(privilege_requests),
            None,
            None,
        )
    }

    /// Opens a bound PREPARE tree for a front end that owns session teardown.
    pub fn open_bound_record_set_for(
        &mut self,
        stmt: Stmt,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::OpenedStatement, DriverError> {
        self.begin_statement_execution(prepared.sql())?;
        let result =
            self.prepare_bound_execution(prepared.sql(), stmt, prepared.privilege_requests());
        self.return_opened_record_set(result)
    }

    /// Executes a prepared SELECT through the ordinary statement and
    /// executor funnel, offering the retained physical tree at the same seam
    /// where a fresh plan is handed to the executor builder.
    /// Runs a cached prepared SELECT for the definition PREPARE retained;
    /// its privilege requests are the ones derived then (Go
    /// `checkPreparedPriv` on `PlanCacheStmt.VisitInfos`).
    pub fn execute_prepared_select_for(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_prepared_select(execution, prepared.sql(), prepared.privilege_requests())
    }

    pub(crate) fn execute_prepared_select(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        sql: &str,
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        self.execute_prepared_select_internal(execution, sql, privilege_requests, false)
            .map(|(output, _)| output)
    }

    pub(crate) fn execute_prepared_select_internal(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        sql: &str,
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
        capture_authority: bool,
    ) -> Result<(StmtOutput, Option<crate::ResultMaterializationAuthority>), DriverError> {
        self.run_with_columns_using(sql, capture_authority, |session| {
            session
                .prepare_cached_select_execution(execution, sql, privilege_requests)?
                .collect(session)
        })
    }

    /// Opens the retained prepared plan without collecting its query results.
    pub fn execute_prepared_record_set_for<'a>(
        &'a mut self,
        execution: &tidb_executor::PlanCacheExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::StatementExecution<'a>, DriverError> {
        Ok(self
            .open_prepared_record_set_for(execution, prepared)?
            .attach(self))
    }

    /// Opens a cached SELECT for a front end that owns session teardown.
    pub fn open_prepared_record_set_for(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::OpenedStatement, DriverError> {
        self.begin_statement_execution(prepared.sql())?;
        self.observe_statement_node(prepared.statement(), true);
        let result = self.open_cached_prepared_select(execution, prepared)?;
        let result = match result {
            Some(result) => Ok(result),
            None => self.prepare_cached_select_execution(
                execution,
                prepared.sql(),
                prepared.privilege_requests(),
            ),
        };
        self.return_opened_record_set(result)
    }

    /// Opens a cache-rebuilt SELECT without re-running the ordinary
    /// preprocess/planner funnel. Go's plan-cache hit returns the rebuilt
    /// physical tree directly to executor construction; the old Rust path fed
    /// that tree back through prepare_parsed_statement, which made every
    /// prepared SELECT pay the full compile cost again.
    fn open_cached_prepared_select(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<Option<PendingExecution>, DriverError> {
        let started = std::time::Instant::now();
        let cache_hit = execution.cache_hit();
        let schema_version = execution.schema_version();
        let table_names = execution.plan().table_names().to_vec();
        let parameters = execution.parameters();
        let plan_digest = execution.plan_digest_cell();
        self.set_statement_arbitration_key(prepared.sql());
        self.active_resource_group.clone_from(&self.vars.resource_group);
        self.publish_statement_resource_group();
        self.begin_cached_prepared_query_boundary();
        for warning in execution.take_planning_warnings() {
            self.append_sql_warning(warning);
        }

        let opened = execution.with_plan(|statement, physical| {
            self.apply_set_var_hints(statement)?;
            self.activate_statement_resource_group(statement);
            self.check_table_privilege_requests(prepared.privilege_requests())?;
            self.vars.statement_insert_id = 0;
            self.statement_kind = StatementKind::Select;
            // Go `GetPlanFromPlanCache` step 3: a table changed since the
            // transaction began makes the cached plan stale, which replans.
            if self.in_transaction() && self.record_mdl_related_table_names(&table_names)? {
                return Ok(None);
            }
            let current_db = self.vars.current_db.clone();
            let stmt_ctx = self
                .statement_context_for_stmt(statement, false)
                .with_prepared_params(parameters.clone())
                .with_cached_plan_digest(plan_digest);
            self.with_catalog_retrieving(&stmt_ctx, |catalog| {
                if schema_version != catalog.schema_version() {
                    return Ok(None);
                }
                let Stmt::Query(query) = statement else {
                    return Err(DriverError::unsupported(
                        "prepared SELECT cache entry is not a query",
                    ));
                };
                let open_query = if execution.plan().contains_sequence_functions() {
                    tidb_executor::driver::open_query_meta_stmt_with_physical
                } else {
                    tidb_executor::driver::open_query_meta_stmt_with_physical_without_sequence_validation
                };
                open_query(
                    query,
                    Some(&mut *physical),
                    catalog,
                    &current_db,
                    &stmt_ctx,
                )
                .map(|record_set| Some((record_set, stmt_ctx.clone())))
            })
        });
        let Some(opened) = opened else {
            return Err(DriverError::unsupported(
                "prepared SELECT plan generation changed before executor construction",
            ));
        };
        let Some((record_set, stmt_ctx)) = opened? else {
            return Ok(None);
        };

        tidb_metrics::session::observe_compile_duration(
            self.observed_compile_duration()
                .unwrap_or_else(|| started.elapsed())
                .as_secs_f64(),
            false,
        );
        self.vars.found_in_plan_cache = cache_hit;
        let query = PendingQuery::new(record_set, stmt_ctx);
        Ok(Some(PendingExecution::Query(query)))
    }

    pub(crate) fn prepare_cached_select_execution(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        sql: &str,
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
    ) -> Result<PendingExecution, DriverError> {
        let mut used = false;
        let output = execution
            .with_plan(|statement, physical| {
                self.begin_prepared_statement_boundary(statement, Some(execution.parameters()));
                for warning in execution.take_planning_warnings() {
                    self.append_sql_warning(warning);
                }
                self.prepare_parsed_statement_with_optional_physical_plan(
                    sql,
                    statement.clone(),
                    Some(privilege_requests),
                    Some(RetainedSelectPlan {
                        physical,
                        schema_version: execution.schema_version(),
                        used: &mut used,
                    }),
                    None,
                )
            })
            .ok_or_else(|| {
                DriverError::unsupported(
                    "prepared SELECT plan generation changed before executor construction",
                )
            })??;
        self.vars.found_in_plan_cache = execution.cache_hit() && used;
        Ok(output)
    }

    /// Go `ExecuteStmt` when the plan-cache miss's `generateNewPlan` fails:
    /// the EXECUTE's one statement context keeps the warnings the optimizer
    /// left on it, and the optimizer's error is the statement's.
    pub(crate) fn fail_generate_new_plan(
        &mut self,
        statement: &Stmt,
        parameters: &[tidb_datatype::Datum],
        failure: tidb_executor::compiler::GenerateNewPlanError,
    ) -> DriverError {
        self.begin_prepared_statement_boundary(statement, Some(parameters.into()));
        for warning in failure.warnings {
            self.append_sql_warning(warning);
        }
        failure.error
    }

    /// [`Self::fail_generate_new_plan`] for a binary EXECUTE that opens a
    /// record set, whose statement lifecycle opens here.
    pub fn open_failed_generate_new_plan_for(
        &mut self,
        statement: &Stmt,
        parameters: &[tidb_datatype::Datum],
        failure: tidb_executor::compiler::GenerateNewPlanError,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::OpenedStatement, DriverError> {
        self.begin_statement_execution(prepared.sql())?;
        let error = self.fail_generate_new_plan(statement, parameters, failure);
        self.return_opened_record_set(Err(error))
    }

    /// [`Self::fail_generate_new_plan`] for a binary EXECUTE of a write,
    /// whose statement lifecycle opens here.
    pub fn run_failed_generate_new_plan_for(
        &mut self,
        statement: &Stmt,
        parameters: &[tidb_datatype::Datum],
        failure: tidb_executor::compiler::GenerateNewPlanError,
        prepared: &crate::PreparedAst,
    ) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_using(prepared.sql(), false, |session| {
            Err(session.fail_generate_new_plan(statement, parameters, failure))
        })
        .map(|(output, _)| output)
    }

    /// Runs a cached prepared DML for the definition PREPARE retained (see
    /// [`Self::execute_prepared_select_for`]): a binary EXECUTE, whose
    /// statement lifecycle opens here (Go `ExecutePreparedStmt` reaching
    /// `ExecuteStmt`'s `ResetContextOfStmt`).
    pub fn execute_cached_prepared_dml_for(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_using(prepared.sql(), false, |session| {
            session.execute_cached_prepared_dml(
                execution,
                prepared.sql(),
                prepared.privilege_requests(),
            )
        })
        .map(|(output, _)| output)
    }

    /// Executes a bound prepared DML statement through the ordinary statement
    /// funnel, then publishes whether Go's complete cache key was reused.
    /// Cache hits and misses therefore share privilege checks, metadata locks,
    /// resource-group selection, statement context, and the DML executor.
    /// It runs inside the current statement's lifecycle: Go plans and runs
    /// an EXECUTE's prepared statement under the EXECUTE's one `StmtCtx`.
    pub(crate) fn execute_cached_prepared_dml(
        &mut self,
        execution: &tidb_executor::PlanCacheExecution,
        sql: &str,
        privilege_requests: &[tidb_executor::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        // go's stats meta bumps the target table's row count on every INSERT
        // (`stats_meta.count`), which `I_S.TABLES`' TABLE_ROWS reads back.
        let current_db = self.vars.current_db.clone();
        let mut stats_update: Option<(String, String, i64)> = None;
        let output = execution
            .with_plan(|statement, physical| {
                self.begin_prepared_statement_boundary(statement, Some(execution.parameters()));
                for warning in execution.take_planning_warnings() {
                    self.append_sql_warning(warning);
                }
                let output = self.execute_parsed_statement_with_dml_plan(
                    sql,
                    statement.clone(),
                    Some(privilege_requests),
                    physical,
                )?;
                if let (StmtOutput::Affected(count), tidb_ast::Stmt::Dml(dml)) =
                    (&output, statement)
                {
                    if let tidb_ast::DmlStmt::Insert(insert) = dml.as_ref() {
                        let name = insert.table.last().cloned();
                        let database = insert
                            .table
                            .len()
                            .ge(&2)
                            .then(|| insert.table[0].clone())
                            .unwrap_or_else(|| current_db.clone());
                        if let Some(name) = name {
                            stats_update = Some((database, name, *count as i64));
                        }
                    }
                }
                Ok::<_, DriverError>(output)
            })
            .ok_or_else(|| {
                DriverError::unsupported(
                    "cached DML plan generation changed before executor construction",
                )
            })??;
        if let Some((database, name, delta)) = stats_update {
            let _ = self.with_catalog_mut(|catalog| {
                catalog.adjust_table_row_count(&database, &name, delta);
                Ok::<_, DriverError>(())
            });
        }
        self.vars.found_in_plan_cache = execution.cache_hit();
        Ok(output)
    }

    /// Records one cached point read's table on the transaction's metadata-
    /// lock map when it runs INSIDE an explicit transaction. The ordinary
    /// funnel does this from its single preprocess walk; the cached arm skips
    /// that walk (which is its whole point), and a read under autocommit
    /// records nothing anyway (`IsAutoCommitTxn && IsReadOnly`). Skipping the
    /// RECORD here would let a DDL slip past a live transaction's reader.

    /// Go `preprocess.go:2243-2270`: as a statement's tables are bound, each
    /// is recorded on the transaction as `table id -> the LATEST domain
    /// schema version` at first use, and the metadata-lock gate blocks a DDL
    /// job while some live map holds one of its tables below the job's
    /// version (`RemoveLockDDLJobs`). The preprocessor locks each name the
    /// statement writes (`tableByName`), recorded here; the planner locks
    /// every table it builds a data source for, a view's body included
    /// (`buildDataSource`), through [`Session::mdl_related_table_recorder`]. A
    /// name that resolves to nothing reports `record_unresolved` and the gate
    /// falls back to blocking conservatively.
    ///
    /// Go's exemption is kept verbatim: a READ-ONLY statement under
    /// autocommit records nothing (`IsAutoCommitTxn && IsReadOnly` returns
    /// before the store), so the hot point-read path pays nothing here
    /// either.
    fn record_mdl_related_tables(&mut self, stmt: &Stmt, names: &[(String, String)]) -> Result<(), DriverError> {
        // Go's `IsAutoCommitTxn && StmtCtx.IsReadOnly` exemption does not
        // apply to locking reads. `SELECT ... FOR UPDATE/FOR SHARE` enters
        // the transaction's related-table map even when autocommit is on.
        let read_only = matches!(stmt, Stmt::Query(_)) && !query_has_lock(stmt);
        self.mdl_statement_locks = !(read_only && !self.in_transaction() && self.is_autocommit());
        if self.mdl_statement_locks {
            // Go `handleTableName` binds PURGE MATERIALIZED VIEW LOG's
            // target like any table name the statement writes.
            let mut names = names.to_vec();
            if let Stmt::Admin(admin) = stmt {
                if let tidb_ast::AdminStmt::PurgeMaterializedViewLog(purge) = admin.as_ref() {
                    let (db, table) = match purge.table.as_slice() {
                        [table] => (String::new(), table.clone()),
                        [db, table] => (db.clone(), table.clone()),
                        _ => (String::new(), String::new()),
                    };
                    names.push((tidb_hack::go_to_lower(&db), tidb_hack::go_to_lower(&table)));
                }
            }
            self.record_mdl_named_tables(&names)?;
            if !read_only {
                self.record_mdl_implicit_mlogs(&names);
            }
        }
        Ok(())
    }

    /// [`Self::record_mdl_related_tables`] for a statement whose single table
    /// name the cached plan already carries. A cached point read is a
    /// SELECT: under autocommit it records nothing (the read-only rule
    /// above), and inside an explicit transaction it records exactly that
    /// one table.
    /// Go `GetPlanFromPlanCache` step 3 for a prepared statement run inside a
    /// transaction: each of its tables passes
    /// `tryLockMDLAndUpdateSchemaIfNecessary` before the cached plan is looked
    /// up, so a table changed since the transaction began is the one the
    /// lookup's `RelateVersion` reads.
    pub fn update_prepared_tables_for_plan_cache(&mut self, template: &Stmt) -> Result<(), DriverError> {
        if !self.in_transaction() {
            return Ok(());
        }
        // The statement's first table record: the previous statement's
        // tables are not this one's (the funnel clears them again later).
        if let Ok(mut locked) = self.mdl_statement_tables.lock() {
            locked.clear();
        }
        let mut stmt = template.clone();
        let names = tidb_planner::preprocess::preprocess(
            &mut stmt,
            &*self,
        )
        .map_err(tidb_executor::driver::planner_error_to_driver)?
        .table_names;
        self.record_mdl_related_tables(&stmt, &names)
    }

    /// Returns whether a table changed since the transaction began was
    /// swapped in: Go's `GetPlanFromPlanCache` step 3 then finds the cached
    /// plan's `RelateVersion` stale (`schemaNotMatch`) and replans.
    fn record_mdl_related_table_names(&mut self, names: &[(String, String)]) -> Result<bool, DriverError> {
        if let Ok(mut locked) = self.mdl_statement_tables.lock() {
            locked.clear();
        }
        self.mdl_statement_locks = self.in_transaction();
        if self.mdl_statement_locks {
            return self.record_mdl_named_tables(names);
        }
        Ok(false)
    }

    /// The preprocessor's half: each name the statement writes, as its own
    /// stored object.
    fn record_mdl_named_tables(&mut self, names: &[(String, String)]) -> Result<bool, DriverError> {
        let Some(sink) = self.mdl_related_tables.clone() else {
            return Ok(false);
        };
        let version = self.cluster_schema_version_now();
        let current_db = self.current_database().to_owned();
        // Go `tryLockMDLAndUpdateSchemaIfNecessary`'s early returns: no MDL,
        // a `tidb_snapshot` schema, a staleness transaction.
        let latest_info_schema = self
            .latest_info_schema
            .clone()
            .filter(|_| tidb_vardef::is_mdl_enabled(false))
            .filter(|_| self.snapshot_schema.is_none() && !self.is_staleness_txn());
        let pessimistic_read_consistency = self.statement_is_pessimistic() && self.is_read_committed();
        let mut catalog = match self.lock_catalog() {
            Ok(catalog) => catalog,
            Err(_) => {
                sink.record_unresolved();
                return Ok(false);
            }
        };
        let mut updated = false;
        for (db, table) in names {
            let db = if db.is_empty() { &current_db } else { db };
            match catalog.related_table_id_for_mdl(db, table) {
                Some(Some(table_id)) => {
                    if !self
                        .mdl_statement_tables
                        .lock()
                        .is_ok_and(|mut locked| locked.insert(table_id))
                    {
                        continue;
                    }
                    let (table_id, version) = match &latest_info_schema {
                        Some(latest) if sink.related_table(table_id).is_none() => {
                            let (id, version, changed) = update_schema_if_necessary(
                                &mut catalog,
                                latest.as_ref(),
                                db,
                                table,
                                pessimistic_read_consistency,
                            )?;
                            updated |= changed;
                            (id, version)
                        }
                        _ => (table_id, version),
                    };
                    sink.record_table(table_id, version);
                    if let Some(process) = &self.process {
                        process
                            .registry()
                            .transaction_related_table(process.id(), table_id);
                    }
                }
                Some(None) => {}
                None => sink.record_unresolved(),
            }
        }
        Ok(updated)
    }

    /// Go master `tryLockMDLAndUpdateSchemaIfNecessary`'s second half: "Writes
    /// also depend on the implicit MLog schema" -- a public base table's log
    /// joins the map too.
    fn record_mdl_implicit_mlogs(&mut self, names: &[(String, String)]) {
        let (Some(sink), Some(latest)) = (self.mdl_related_tables.clone(), self.latest_info_schema.clone()) else {
            return;
        };
        if !tidb_vardef::is_mdl_enabled(false) || self.snapshot_schema.is_some() || self.is_staleness_txn() {
            return;
        }
        let version = self.cluster_schema_version_now();
        let current_db = self.current_database().to_owned();
        for (db, table) in names {
            let db = if db.is_empty() { &current_db } else { db };
            let Some((_, info)) = latest.latest().table_info_by_name(db, table) else {
                continue;
            };
            let mlog_id = {
                let info = info.read();
                if info.state != tidb_model::SchemaState::PUBLIC {
                    continue;
                }
                info.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id)
            };
            if mlog_id == 0 || !self.mdl_statement_tables.lock().is_ok_and(|mut locked| locked.insert(mlog_id)) {
                continue;
            }
            sink.record_table(mlog_id, version);
            if let Some(process) = &self.process {
                process.registry().transaction_related_table(process.id(), mlog_id);
            }
        }
    }

    /// Runs a statement whose table refs carry `AS OF TIMESTAMP`, against
    /// the store's state as of that timestamp. `Ok(None)` means the
    /// statement carries no as-of clause and takes its ordinary path.
    ///
    /// Go's rules, from `preprocess.go` and `staleread/util.go`: every
    /// as-of expression in one statement must name the same instant (Go
    /// errors on a mix; this port refuses it the same way), the resolved
    /// snapshot serves every read, and the statement's transaction is the
    /// stale one -- `@@tidb_current_ts` is the as-of timestamp for its
    /// duration, and `LastTxnInfo` records the start-only shape a read-only
    /// transaction leaves (`setLastTxnInfoBeforeTxnEnd`).
    fn execute_as_of_statement(
        &mut self,
        stmt: &Stmt,
    ) -> Result<Option<PendingExecution>, DriverError> {
        /// Each table's `AS OF`, in the order Go's preprocessor visits them.
        struct StripAsOf {
            taken: Vec<Option<tidb_ast::Expr>>,
        }
        impl tidb_ast::Visitor for StripAsOf {
            fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                    self.taken.push(table_ref.as_of.take().map(|expr| *expr));
                }
                false
            }
            fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
                true
            }
        }
        let mut stripped = stmt.clone();
        let mut visitor = StripAsOf { taken: Vec::new() };
        tidb_ast::Visitable::accept(&mut stripped, &mut visitor);
        if visitor.taken.iter().all(Option::is_none) {
            return Ok(None);
        }
        // Go `staleReadProcessor.OnSelectTable`, table by table: a table
        // without `AS OF` is timestamp 0. The first table evaluates the
        // processor (stale or, `setAsNonStaleRead`, not) and fixes the
        // statement's timestamp; every later table must name the same one.
        let mut evaluated: Option<u64> = None;
        let mut resolved: Option<u64> = None;
        for expr in &visitor.taken {
            let ts = match expr {
                Some(expr) => self.calculate_as_of_ts_expr(expr)?,
                None => 0,
            };
            if ts > 0 && resolved.is_none() {
                resolved = Some(ts);
            }
            match evaluated {
                // Go refuses `AS OF` under `@@tx_read_ts` at this table,
                // before any later table is compared (below).
                None if ts > 0 && self.vars.txn_read_ts() != 0 => break,
                None => evaluated = Some(ts),
                Some(previous) if previous == ts => {}
                Some(_) => {
                    // Go refuses a statement naming two different instants.
                    return Err(DriverError::from(
                        tidb_plannererrors::ERR_AS_OF.gen_with_stack_by_args(&[
                            ("can not set different time in the as of".to_owned()).into(),
                        ]),
                    ));
                }
            }
        }
        let ts = resolved.expect("at least one as-of expression");
        if self.vars.txn_read_ts() != 0 {
            if let Some(provider) = &self.snapshot_schema_provider {
                provider(ts, self.current_resource_group(), false)?;
            }
            self.vars.use_txn_read_ts();
            self.cleanup_txn_read_ts();
            return Err(DriverError::from(
                tidb_plannererrors::ERR_AS_OF.gen_with_stack_by_args(&[
                    "can't use select as of while already set transaction as of".into(),
                ]),
            ));
        }
        let output = self.run_statement_as_of(ts, stripped, true);
        Some(output).transpose()
    }

    fn run_snapshot_statement_in_transaction(
        &mut self,
        ts: u64,
        statement: Stmt,
    ) -> Result<PendingExecution, DriverError> {
        let provider = self.historical_read_provider.as_ref().ok_or_else(|| {
            DriverError::unsupported("snapshot reads require the historical storage provider")
        })?;
        let schema = self
            .snapshot_schema
            .as_ref()
            .filter(|(selected, _)| *selected == ts)
            .map(|(_, catalog)| catalog);
        let read = provider(ts, self.current_resource_group(), schema)?;
        self.statement_snapshot = Some((ts, read));
        match self.execute_parsed_statement_inner("", statement, None, None, None) {
            Ok(PendingExecution::Query(mut query)) => {
                query.transaction_end = QueryTransactionEnd::SnapshotRead;
                Ok(PendingExecution::Query(query))
            }
            outcome => {
                self.statement_snapshot = None;
                outcome
            }
        }
    }

    /// Executes one already-stripped statement against the snapshot at `ts`,
    /// through the same transaction overlay every in-transaction statement
    /// uses -- so the read path is the ordinary one, only the catalog is
    /// historical.
    fn run_statement_as_of(
        &mut self,
        ts: u64,
        stripped: Stmt,
        is_staleness: bool,
    ) -> Result<PendingExecution, DriverError> {
        self.open_stale_transaction(ts, is_staleness)?;
        match self.execute_parsed_statement_inner("", stripped, None, None, None) {
            Ok(PendingExecution::Query(mut query)) => {
                query.transaction_end = if self.is_autocommit() || self.vars.snapshot_ts() != 0 {
                    QueryTransactionEnd::StaleRead
                } else {
                    QueryTransactionEnd::None
                };
                Ok(PendingExecution::Query(query))
            }
            outcome => {
                self.discard_stale_statement_transaction();
                outcome
            }
        }
    }

    fn execute_parsed_statement(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: Option<&[tidb_executor::table_privilege::TablePrivilegeRequest]>,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_parsed_statement_with_optional_physical_plan(sql, stmt, prepared, None, None)
    }

    fn execute_parsed_statement_with_dml_plan(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: Option<&[tidb_executor::table_privilege::TablePrivilegeRequest]>,
        physical: &mut tidb_planner_core_operator_physicalop::PhysicalPlan,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_parsed_statement_with_optional_physical_plan(
            sql,
            stmt,
            prepared,
            None,
            Some(physical),
        )
    }

    fn execute_parsed_statement_with_optional_physical_plan(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: Option<&[tidb_executor::table_privilege::TablePrivilegeRequest]>,
        select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    ) -> Result<StmtOutput, DriverError> {
        self.prepare_parsed_statement_with_optional_physical_plan(
            sql,
            stmt,
            prepared,
            select_plan,
            dml_plan,
        )?
        .collect(self)
    }

    /// Opens a parsed statement for a front end that retains its own snapshot.
    pub fn open_record_set_parsed(
        &mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<crate::OpenedStatement, DriverError> {
        self.begin_statement_execution(sql)?;
        self.begin_text_statement_boundary(&stmt);
        // Go `Compile`'s `CountStmtNode`, once the query is planned.
        let count = self.stmt_node_count(&stmt);
        let result =
            self.prepare_parsed_statement_with_optional_physical_plan(sql, stmt, None, None, None);
        if let (Ok(_), Some(count)) = (&result, count) {
            count.record();
        }
        self.return_opened_record_set(result)
    }

    fn prepare_parsed_statement_with_optional_physical_plan(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: Option<&[tidb_executor::table_privilege::TablePrivilegeRequest]>,
        select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    ) -> Result<PendingExecution, DriverError> {
        // Go charges `SessionExecuteCompileDuration` for every statement whose
        // compile succeeds (`session.go:2624`); a failed compile returns
        // before the observation. This door is where preprocess, privilege,
        // and planning happen, so it is the compile boundary.
        let started = std::time::Instant::now();
        self.start_observation_compile();
        // Go `executor.Compile`; the executor hands over to `session.runStmt`
        // where planning ends and execution begins, and a statement that
        // reaches no such point hands over here. The statement an EXECUTE
        // runs comes back through this door inside the EXECUTE's compile:
        // Go plans it inside the EXECUTE's `planner.Optimize`, so it opens
        // no compile of its own.
        let compile = CompileDepth::enter(&stmt);
        if !compile.nested && tidb_tracing::current_span().is_some() {
            tidb_tracing::begin_thread_region("executor.Compile", Some(sql.to_owned()));
        }
        let result =
            self.prepare_parsed_statement_compile_phase(sql, stmt, prepared, select_plan, dml_plan);
        if !compile.nested {
            tidb_executor::driver::end_held_optimize_region();
            if result.is_ok() {
                tidb_executor::driver::begin_run_stmt_region();
            } else {
                tidb_tracing::end_thread_region("executor.Compile");
            }
        }
        if !compile.nested && result.is_ok() {
            self.successful_compiles += 1;
        }
        drop(compile);
        if result.is_err() && !self.in_transaction() {
            self.cleanup_txn_read_ts();
        }
        if result.is_ok() {
            tidb_metrics::session::observe_compile_duration(
                self.observed_compile_duration()
                    .unwrap_or_else(|| started.elapsed())
                    .as_secs_f64(),
                false,
            );
        }
        result
    }

    /// Go `PlanBuilder.checkSEMStmt`, the first step of `Build` for every
    /// statement: SQL the SEM v2 policy restricts needs
    /// `RESTRICTED_SQL_ADMIN`, ahead of any privilege check.
    pub(crate) fn check_sem_stmt(&self, sql: &str, stmt: &Stmt) -> Result<(), DriverError> {
        if tidb_sem_v2::is_enabled()
            && tidb_sem_v2::is_restricted_sql(&sem_stmt_view(stmt))
            && !self.has_dynamic_privilege("RESTRICTED_SQL_ADMIN", false)
        {
            let statement = if stmt.text().is_empty() {
                sql.to_owned()
            } else {
                String::from_utf8_lossy(stmt.text()).into_owned()
            };
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NOT_SUPPORTED_WITH_SEM
                    .gen_with_stack_by_args(&[statement.into()]),
            ));
        }
        Ok(())
    }

    fn prepare_parsed_statement_compile_phase(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        prepared: Option<&[tidb_executor::table_privilege::TablePrivilegeRequest]>,
        select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    ) -> Result<PendingExecution, DriverError> {
        // A statement locks nothing until its preprocessing decides it does
        // (`record_mdl_related_tables`).
        self.mdl_statement_locks = false;
        if let Ok(mut locked) = self.mdl_statement_tables.lock() {
            locked.clear();
        }
        // Go `ResetContextOfStmt`'s new `StmtCtx`: no related tables yet.
        self.stmt_related_table_ids.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
        self.check_sem_stmt(sql, &stmt)?;
        for warning in tidb_planner::optimize::filter_sem_restricted_hints(&mut stmt) {
            self.append_warning(WarningLevel::Warning, 1105, warning);
        }
        // Go's `Preprocess` walks the AST once per statement and answers
        // every table-shaped question from that pass (`preprocess.go`); the
        // three consumers below share this one walk instead of each cloning
        // and re-walking the statement.
        // Go preprocesses the statement it compiles: an EXECUTE, not the
        // prepared statement it runs.
        let region = (!CompileDepth::is_nested())
            .then(|| tidb_tracing::start_thread_region("planner.Preprocess"));
        // A statement the node compiled ([`Self::compile_statement`]) was
        // walked there: its preprocessed statement and the walk's answers
        // continue here, as Go's one `Compile` carries them to `Optimize`.
        let mut scan = match self.take_precompiled_preprocess(&stmt) {
            Some((walked, scan)) => {
                stmt = walked;
                scan
            }
            None => tidb_planner::preprocess::preprocess(&mut stmt, &*self)
                .map_err(tidb_executor::driver::planner_error_to_driver)?,
        };
        drop(region);
        // Go `checkSelectNoopFuncs` warns as the walk reaches each clause.
        for warning in std::mem::take(&mut scan.noop_warnings) {
            self.append_driver_error(
                crate::WarningLevel::Warning,
                &tidb_executor::driver::planner_error_to_driver(warning),
            );
        }
        // An execution of a prepared statement preprocesses its EXECUTE,
        // which reads no variable under a SELECT.
        self.readonly_user_vars = std::sync::Arc::new(if prepared.is_none() {
            std::mem::take(&mut scan.readonly_user_vars)
        } else {
            std::collections::HashSet::new()
        });
        self.validate_statement_in_txn(&stmt, self.in_transaction())?;
        self.validate_statement_read_only_in_staleness(&stmt)?;
        self.check_explain_target_for_update_ts(&stmt)?;
        if self.vars.snapshot_ts() != 0
            && self.in_transaction()
            && self.historical_read_ts() != Some(self.vars.snapshot_ts())
            && matches!(&stmt, Stmt::Query(_))
        {
            // Go baseTxnContextProvider changes the statement's schema/read TS,
            // not the active transaction or its write buffer.
            return self.run_snapshot_statement_in_transaction(self.vars.snapshot_ts(), stmt);
        }
        self.record_mdl_related_tables(&stmt, &scan.table_names)?;
        // A statement whose table references carry `AS OF TIMESTAMP` runs
        // against the store's history -- Go's stale statement
        // (`StalenessTxnContextProvider` for one statement). Intercepted at
        // this one funnel so text and prepared spellings share the rules.
        if !self.in_transaction()
            && reads_as_select(&stmt)
            && !scan.has_as_of
            && (!scan.table_names.is_empty() || prepared.is_some() || self.vars.snapshot_ts() != 0)
        {
            if let Some(ts) = self.configured_historical_read_ts()? {
                // `tidb_snapshot` comes first and is no staleness read.
                let is_staleness = self.vars.snapshot_ts() == 0;
                return self.run_statement_as_of(ts, stmt, is_staleness);
            }
        }
        // Go `DDLExec.executeCreateView` preprocesses the view's SELECT as a
        // statement of its own, after the DDL's implicit commit, and refuses
        // a stale read there: a table read `AS OF`, or `tidb_read_staleness`.
        if let Stmt::Ddl(ddl) = &stmt {
            if let tidb_ast::DdlStmt::CreateView(create) = ddl.as_ref() {
                self.refuse_stale_view_body(create, scan.has_as_of)?;
            }
        }
        if scan.has_as_of && self.in_transaction() {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_AS_OF.gen_with_stack_by_args(&[
                    "as of timestamp can't be set in transaction.".into(),
                ]),
            ));
        }
        if scan.has_as_of && !self.in_transaction() {
            if let Some(output) = self.execute_as_of_statement(&stmt)? {
                return Ok(output);
            }
        }
        // The pessimistic lock step and its retry are the store session's
        // statement loop (Go `handlePessimisticDML`, `StatementLifecycle`).
        let execution =
            self.execute_parsed_statement_inner(sql, stmt, prepared, select_plan, dml_plan);
        let execution = execution?;
        // Go runs a statement without a result set through
        // `handleNoDelayExecutor`, whose `TryNewCacheChunk` draws from the
        // statement's pool (marking `useChunkAlloc` through its hook).
        if matches!(
            execution,
            PendingExecution::Complete(StmtOutput::Affected(_) | StmtOutput::Done(_))
        ) {
            let _ = self.statement_pool_chunk(&[]);
        }
        Ok(execution)
    }

    /// Go's non-prepared plan cache for a DML statement (the
    /// `tidb_enable_non_prepared_plan_cache_for_dml` switch, true by
    /// default): the statement is parameterized and, on a hit, the cached
    /// plan executes.
    fn execute_non_prepared_cached_dml(
        &mut self,
        stmt: &Stmt,
        prepared: Option<&[tidb_executor::table_privilege::TablePrivilegeRequest]>,
        binding_sql: Option<&str>,
        sql: &str,
    ) -> Result<Option<PendingExecution>, DriverError> {
        let non_prepared_dml = prepared
            .is_none()
            .then(|| self.parameterize_non_prepared_dml(stmt))
            .flatten();
        let Some(parameterized) = non_prepared_dml.as_ref() else {
            return Ok(None);
        };
        let mut effective_parameterized = parameterized.statement.clone();
        if binding_sql.is_some() {
            let binding_hints = tidb_bindinfo::binding::collect_hints(stmt);
            tidb_bindinfo::binding::bind_hints(&mut effective_parameterized, &binding_hints);
        }
        let Some((execution, requests)) =
            self.bind_non_prepared_dml(parameterized, &effective_parameterized, binding_sql)?
        else {
            return Ok(None);
        };
        // The cache entry's requests, Go's `PlanCacheStmt.VisitInfos`, which
        // `checkPreparedPriv` reads on every use.
        self.execute_cached_prepared_dml(&execution, sql, &requests)
            .map(|output| Some(PendingExecution::Complete(output)))
    }

    /// Go `ResetContextOfStmt`'s DML arm (`ResetUpdateStmtCtx`,
    /// `ResetDeleteStmtCtx`) and the plan's privilege check, the context a
    /// DML statement compiles and executes under.
    fn dml_stmt_ctx(&mut self, stmt: &Stmt) -> Result<tidb_sessionctx::StmtContext, DriverError> {
        let Stmt::Dml(dml) = stmt else {
            unreachable!("a compiled DML statement")
        };
        let ctx = match dml.as_ref() {
            DmlStmt::Insert(insert) => self.reset_insert_stmt_ctx(insert, stmt),
            DmlStmt::Update(update) => self.reset_update_stmt_ctx(update, stmt),
            DmlStmt::Delete(delete) => self.reset_delete_stmt_ctx(delete, stmt),
            _ => unreachable!("a compiled DML statement"),
        };
        self.with_plan_privilege_check(ctx, stmt)
    }

    /// Go `Compiler.Compile` for an INSERT / UPDATE / DELETE: its final plan
    /// over the statement's context, which the first execution runs under.
    fn compile_dml(
        &mut self,
        stmt: Stmt,
        warnings_start: usize,
    ) -> Result<(CompiledDml, tidb_sessionctx::StmtContext), DriverError> {
        let current_db = self.vars.current_db.clone();
        let ctx = self.dml_stmt_ctx(&stmt)?;
        let Stmt::Dml(dml) = &stmt else {
            unreachable!("a compiled DML statement")
        };
        let plan = match dml.as_ref() {
            DmlStmt::Insert(insert) => {
                if insert.source.is_some() {
                    self.write_sli.set_invalid();
                }
                self.with_catalog_retrieving(&ctx, |catalog| {
                    tidb_executor::driver::compile_insert_plan(insert, catalog, &current_db, &ctx)
                })
                .map(CompiledDmlPlan::Insert)
            }
            DmlStmt::Update(update) => self
                .with_catalog_retrieving(&ctx, |catalog| {
                    tidb_executor::driver::compile_update_plan(update, catalog, &current_db, &ctx)
                })
                .map(CompiledDmlPlan::Update),
            DmlStmt::Delete(delete) => self
                .with_catalog_retrieving(&ctx, |catalog| {
                    tidb_executor::driver::compile_delete_plan(delete, catalog, &current_db, &ctx)
                })
                .map(CompiledDmlPlan::Delete),
            _ => unreachable!("a compiled DML statement"),
        };
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => {
                self.drain_eval_warnings(&ctx);
                if matches!(dml.as_ref(), DmlStmt::Insert(_)) {
                    self.vars.statement_insert_id = ctx
                        .published_last_insert_id()
                        .unwrap_or_else(|| ctx.given_insert_id());
                }
                self.statement_message = ctx.message();
                return Err(error);
            }
        };
        let stmt_hints = self.stmt_hints.clone();
        Ok((
            CompiledDml {
                stmt,
                current_db,
                plan,
                stmt_hints,
                warnings_start,
            },
            ctx,
        ))
    }

    /// Go `ExecStmt.Exec` -> `handleNoDelayExecutor` for a compiled INSERT /
    /// UPDATE / DELETE: the executor over the final plan, under the
    /// compile's context the first time and a fresh one on a retry. A
    /// top-level statement keeps its compiled form for a pessimistic retry.
    fn exec_compiled_dml(
        &mut self,
        mut compiled: CompiledDml,
        ctx: Option<tidb_sessionctx::StmtContext>,
    ) -> Result<StmtOutput, DriverError> {
        let ctx = match ctx {
            Some(ctx) => ctx,
            None => self.dml_stmt_ctx(&compiled.stmt)?,
        };
        let CompiledDml { stmt, current_db, plan, .. } = &mut compiled;
        let Stmt::Dml(dml) = &*stmt else {
            unreachable!("a compiled DML statement")
        };
        let output = match (dml.as_ref(), plan) {
            (DmlStmt::Insert(insert), CompiledDmlPlan::Insert(plan)) => {
                let result = self.retrieving_memory_tables(&ctx, |session| {
                    session.with_staged_catalog(|catalog| {
                        tidb_executor::driver::run_compiled_insert(insert, catalog, current_db, &ctx, plan)
                    })
                });
                self.drain_eval_warnings(&ctx);
                // Go `session.LastInsertID()`, the OK packet's field:
                // `StmtCtx.LastInsertID` when the statement PUBLISHED an
                // allocated id, `StmtCtx.InsertID` -- the last explicit value
                // -- otherwise.
                self.vars.statement_insert_id = ctx
                    .published_last_insert_id()
                    .unwrap_or_else(|| ctx.given_insert_id());
                // Go `InsertExec.setMessage`'s `Records: ...` text.
                self.statement_message = ctx.message();
                result.map(|(affected, _)| StmtOutput::Affected(affected))
            }
            (DmlStmt::Update(update), CompiledDmlPlan::Update(plan)) => {
                let result = self.retrieving_memory_tables(&ctx, |session| {
                    session.with_staged_catalog(|catalog| {
                        tidb_executor::run_update_stmt_with_physical(update, catalog, current_db, &ctx, Some(&mut *plan))
                    })
                });
                self.drain_eval_warnings(&ctx);
                self.statement_message = ctx.message();
                result.map(StmtOutput::Affected)
            }
            (DmlStmt::Delete(delete), CompiledDmlPlan::Delete(plan)) => {
                let result = self.retrieving_memory_tables(&ctx, |session| {
                    session.with_staged_catalog(|catalog| {
                        tidb_executor::run_delete_stmt_with_physical(delete, catalog, current_db, &ctx, Some(&mut *plan))
                    })
                });
                self.drain_eval_warnings(&ctx);
                // Go's info is the statement context's message, which a
                // DELETE's foreign-key cascade `UPDATE` sets.
                self.statement_message = ctx.message();
                result.map(StmtOutput::Affected)
            }
            _ => unreachable!("a compiled DML statement and its plan agree"),
        };
        if CompileDepth::depth() <= 1 {
            self.exec_stmt = Some(Box::new(ExecStmt::Dml(compiled)));
        }
        output
    }

    /// Go `Compiler.Compile` for a locking SELECT (Go
    /// `ExecStmt.isSelectForUpdate`): its final plan over the statement's
    /// context, which the first open runs under.
    fn compile_locking_query(
        &mut self,
        stmt: Stmt,
        warnings_start: usize,
    ) -> Result<(CompiledQuery, tidb_sessionctx::StmtContext), DriverError> {
        let current_db = self.vars.current_db.clone();
        let ctx = self.with_plan_privilege_check(self.statement_context_for_stmt(&stmt, false), &stmt)?;
        let Stmt::Query(query) = &stmt else {
            unreachable!("a locking SELECT")
        };
        let physical = self.with_catalog_retrieving(&ctx, |catalog| {
            tidb_executor::driver::compile_query_plan(query, catalog, &current_db, &ctx)
        })?;
        let stmt_hints = self.stmt_hints.clone();
        Ok((
            CompiledQuery {
                stmt,
                current_db,
                physical,
                stmt_hints,
                warnings_start,
            },
            ctx,
        ))
    }

    /// Go `ExecStmt.Exec` for a compiled locking SELECT: the executors over
    /// the final plan and its opened record set, under the compile's context
    /// the first time and a fresh one on a retry. A top-level statement keeps
    /// its compiled form for a pessimistic retry.
    fn open_compiled_locking_query(
        &mut self,
        mut compiled: CompiledQuery,
        ctx: Option<tidb_sessionctx::StmtContext>,
    ) -> Result<PendingQuery, DriverError> {
        let ctx = match ctx {
            Some(ctx) => ctx,
            None => self.with_plan_privilege_check(
                self.statement_context_for_stmt(&compiled.stmt, false),
                &compiled.stmt,
            )?,
        };
        let CompiledQuery { stmt, physical, .. } = &mut compiled;
        let Stmt::Query(query) = &*stmt else {
            unreachable!("a locking SELECT")
        };
        let opened = self.with_catalog_retrieving(&ctx, |catalog| {
            tidb_executor::driver::open_compiled_query(query, physical, catalog, &ctx)
        });
        if CompileDepth::depth() <= 1 {
            self.exec_stmt = Some(Box::new(ExecStmt::Query(compiled)));
        }
        Ok(PendingQuery::new(opened?, ctx))
    }

    /// Go `ExecStmt.handlePessimisticLockError`'s re-execution of a compiled
    /// locking SELECT: `StmtCtx.ResetForRetry`, then the same plan opened
    /// again, with no compile.
    pub(crate) fn open_compiled_query_retry(
        &mut self,
        compiled: Box<CompiledQuery>,
        sql: &str,
    ) -> Result<crate::OpenedStatement, DriverError> {
        let result = self.begin_statement_execution(sql).and_then(|()| {
            self.warnings.truncate(compiled.warnings_start);
            self.statement_kind = statement_kind_of(&compiled.stmt);
            self.stmt_hints = compiled.stmt_hints.clone();
            self.open_compiled_locking_query(*compiled, None)
                .map(PendingExecution::Query)
        });
        self.return_opened_record_set(result)
    }

    /// The statement [`Self::exec_compiled_dml`] kept, when the statement is
    /// being retried after a pessimistic lock conflict
    /// ([`Self::record_statement_retry`]); any other entry forgets it.
    pub(crate) fn forget_exec_stmt(&mut self) {
        self.exec_stmt = None;
        self.pessimistic_retry = false;
    }

    pub(crate) fn take_retry_exec_stmt(&mut self) -> Option<Box<ExecStmt>> {
        let retry = std::mem::take(&mut self.pessimistic_retry);
        self.exec_stmt.take().filter(|_| retry)
    }

    /// Go `ExecStmt.handlePessimisticLockError`'s re-execution of a compiled
    /// INSERT / UPDATE / DELETE: `StmtCtx.ResetForRetry` (the statement's
    /// warnings go, the parser's stay), then the executor over the same plan,
    /// with no compile.
    pub(crate) fn exec_compiled_dml_retry(
        &mut self,
        compiled: CompiledDml,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_using(sql, false, move |session| {
            session.warnings.truncate(compiled.warnings_start);
            session.statement_kind = statement_kind_of(&compiled.stmt);
            session.stmt_hints = compiled.stmt_hints.clone();
            let output = session.exec_compiled_dml(compiled, None)?;
            // Go `handleNoDelayExecutor`'s `TryNewCacheChunk`.
            let _ = session.statement_pool_chunk(&[]);
            Ok(output)
        })
        .map(|(output, _)| output)
    }

    /// Go `Compiler.Compile` and then `ExecStmt.Exec` for a query: the
    /// final plan is the one the statement retained while its schema still
    /// holds, else `planner.Optimize`'s
    /// ([`tidb_executor::driver::compile_query_plan`]); the execution builds
    /// the executors over it and opens the record set. Each step answers the
    /// memory-table reads it asks for and runs again
    /// ([`Self::with_catalog_retrieving`]); the execution's rerun reuses the
    /// compiled plan, as Go's `ExecStmt` keeps its `Plan`.
    fn compile_and_open_query(
        &mut self,
        query: &tidb_ast::QueryStmt,
        select_plan: &mut Option<RetainedSelectPlan<'_>>,
        current_db: &str,
        ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<tidb_executor::driver::RecordSet, DriverError> {
        let mut compiled = None;
        self.with_catalog_retrieving(ctx, |catalog| {
            compiled = match select_plan {
                Some(retained) if retained.schema_version == catalog.schema_version() => None,
                _ => Some(tidb_executor::driver::compile_query_plan(query, catalog, current_db, ctx)?),
            };
            Ok(())
        })?;
        self.with_catalog_retrieving(ctx, |catalog| match (compiled.as_mut(), select_plan.as_mut()) {
            (Some(physical), _) => {
                tidb_executor::driver::open_compiled_query(query, physical, catalog, ctx)
            }
            (None, Some(retained)) => {
                *retained.used = true;
                tidb_executor::driver::open_query_meta_stmt_with_physical(
                    query,
                    Some(&mut *retained.physical),
                    catalog,
                    current_db,
                    ctx,
                )
            }
            (None, None) => unreachable!("a query compiles to a plan or keeps its retained one"),
        })
    }

    // Go keys memory arbitration by StatementContext.SQLDigest's normalized
    // OriginalSQL, not binding normalization of a restored AST. The caller
    // supplies PREPARE's original text, so bound values do not change the key.
    fn set_statement_arbitration_key(&mut self, sql: &str) {
        self.current_sql_digest_key = if self.session_memory.arbitrator_enabled() {
            // `begin_statement_execution` normalized this statement already.
            self.statement_normalized_sql
                .take()
                .unwrap_or_else(|| tidb_parser::normalize(sql, tidb_parser::RedactMode::Enabled))
        } else {
            String::new()
        };
    }

    /// Go `FinishExecuteStmt` adds the statement's processed keys to
    /// `KeysExamined`; folding the finished statement in as the next one
    /// starts is the same total by the time anything reads it.
    fn begin_statement_keys_read(&mut self) {
        let finished = std::mem::take(&mut self.statement_keys_read);
        self.vars.keys_examined = self
            .vars
            .keys_examined
            .saturating_add(finished.load(std::sync::atomic::Ordering::Relaxed));
        self.statement_in_select = self.statement_kind == StatementKind::Select;
    }


    fn execute_parsed_statement_inner(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        prepared: Option<&[tidb_executor::table_privilege::TablePrivilegeRequest]>,
        mut select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    ) -> Result<PendingExecution, DriverError> {
        // Where the statement's warnings begin: what a pessimistic retry
        // truncates back to (Go `StmtCtx.ResetForRetry`), the parser's
        // warnings before it staying.
        let warnings_start = self.warnings.len();
        self.set_statement_arbitration_key(sql);
        // Go `ResetContextOfStmt`: every statement's `StmtCtx.ResourceGroupName`
        // starts as the session's group (a RESOURCE_GROUP hint replaces it
        // after binding selection, below).
        self.active_resource_group.clone_from(&self.vars.resource_group);
        self.publish_statement_resource_group();
        // Go `ResetContextOfStmt` sets the `In*Stmt` bits before any
        // statement-specific door -- an EXPLAIN by its target -- so a
        // statement that FAILS still classifies itself for the next
        // statement's `ROW_COUNT()` (captured: a failed SELECT leaves -1, a
        // failed INSERT leaves 0).
        self.statement_kind = statement_kind_of(&stmt);
        self.statement_read_only = stmt.is_read_only(true);
        self.begin_statement_keys_read();
        // Go `buildDo`: DO evaluates its expressions as a Projection over a
        // one-row TableDual and discards the row, so `DO @a := 1` assigns and
        // the expressions' errors and warnings surface, while the client
        // sees an OK.
        if let Stmt::Admin(admin) = &stmt {
            if let tidb_ast::AdminStmt::Do(exprs) = &**admin {
                let query = do_projection_query(exprs);
                self.execute_parsed_statement(sql, query, prepared)?;
                return Ok(PendingExecution::Complete(StmtOutput::Affected(0)));
            }
        }
        self.start_statement_transaction_scope();
        // Go parameterizes a non-prepared statement before optimization, then
        // sends the retained marker-bearing statement through the same plan
        // cache as PREPARE. Keep the candidate beside this ordinary statement
        // funnel; privilege, binding, transaction, and context setup below
        // remain shared whether the physical plan hits or misses.
        // Unsupported-statement counting lives inside
        // `parameterize_non_prepared_select` (Go's
        // `GetNonPrepPlanCacheUnsupportedCounter`), which distinguishes
        // counted checker-walk refusals from clause-level fast-check ones.
        let non_prepared = prepared
            .is_none()
            .then(|| self.parameterize_non_prepared_select(&stmt))
            .flatten();
        // `apply_schema_stmt` dispatches administrative statements early.
        // EXPLAIN is the one such wrapper whose inner query/DML can own
        // `SET_VAR`, so install that direct-AST overlay before the early
        // dispatch builds the target plan. The ordinary query/DML path below
        // applies its own hints after its early control-statement doors.
        if matches!(&stmt, Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Explain(
            _,
        ))) {
            self.apply_set_var_hints(&stmt)?;
        }
        // Go `Compile` for every statement that is not a query or DML: the
        // preprocessor, then `PlanBuilder.Build`'s visit infos, checked while
        // the statement is still side-effect free.
        let compiled = self.compile_stmt(&stmt, prepared)?;
        self.statement_preamble(&stmt)?;
        let executed = match &compiled {
            Some(plan) => tidb_executor::builder::ExecutorBuild::execute_plan(self, plan, &stmt)?,
            None => None,
        };
        if let Some(output) = executed {
            // The partition DDL's fold-stash warnings (the reorganize
            // statistics-outdated row) drain here: DDL statements produce no
            // record set, so the record-set-only drain never runs for them.
            let statement_ctx = self.statement_context(false);
            self.drain_eval_warnings(&statement_ctx);
            return Ok(PendingExecution::Complete(output));
        }
        // BEGIN / COMMIT / ROLLBACK and the savepoint statements are the
        // store session's (`StoreSession::control_transaction_stmt`), which
        // the connection routes them to before they reach this executor.
        if tidb_planner::transaction_control::classify_transaction_control_stmt(&stmt).is_some() {
            return Err(DriverError::unsupported(
                "transaction statements run on the store session",
            ));
        }
        match self.apply_set_stmt(&stmt) {
            Ok(Some(_)) => {
                return Ok(PendingExecution::Complete(StmtOutput::Affected(0)));
            }
            // go's SetExecutor routes every failure through `handleErr`,
            // which appends it to the statement context: after a failed SET
            // both error_count and warning_count read 1 and SHOW WARNINGS
            // carries the row (captured: `SET bogus_var=1`).
            Err(error) => {
                self.append_driver_error(WarningLevel::Error, &error);
                return Err(error);
            }
            Ok(None) => {}
        }
        // SET and transaction control must remain available to unpin or roll back.
        // Snapshot writes are rejected; sessions without a storage provider fail closed.
        self.refuse_pinned_historical_read(&stmt)?;
        // SQL-level prepared statements are answered before variable binding,
        // because a `USING` entry is a variable whose VALUE this statement
        // must read itself (Go's `usingParam.Eval`) rather than one to
        // substitute into the `EXECUTE`'s own text. `EXECUTE` re-enters this
        // same function with the bound statement, so the inner statement takes
        // every door -- schema, transaction control, dispatch -- that a
        // directly written one does.
        if let Stmt::Session(session_stmt) = &stmt {
            match &**session_stmt {
                SessionStmt::Prepare { name, source } => {
                    // Go `PrepareExec.Next` does not install its inner plan's visits
                    // in the outer PREPARE statement context.
                    let result = self.prepare_statement(name, source);
                    self.observe_statement_tables(std::iter::empty());
                    result?;
                    return Ok(PendingExecution::Complete(StmtOutput::Affected(0)));
                }
                SessionStmt::Execute { name, using } => {
                    // Go plans the prepared statement inside the EXECUTE's
                    // `planner.Optimize` (`GetPlanFromPlanCache`).
                    tidb_executor::driver::hold_optimize_region();
                    return self.execute_prepared_statement(name, using);
                }
                SessionStmt::Deallocate(name) => {
                    self.deallocate_prepared_statement(name)?;
                    return Ok(PendingExecution::Complete(StmtOutput::Affected(0)));
                }
                _ => {}
            }
        }
        // `@@x` / `@x` read the session's own state, so they are bound before
        // the statement reaches the driver.
        // A `SET_VAR` hint overlays the session BEFORE anything reads a
        // variable, which is where Go applies it too: the optimizer installs
        // it, and expression rewriting -- the `@@x` reads below -- happens
        // after.
        self.apply_set_var_hints(&stmt)?;
        // Go first applies the statement's own StmtHints, then tries the
        // matched binding and applies that binding's StmtHints before
        // optimizing the replacement tree. Prepared execution has already
        // performed this match in order to construct its binding-aware cache
        // key, so only an ordinary statement matches here.
        let binding_sql = if prepared.is_none() && matches!(stmt, Stmt::Query(_) | Stmt::Dml(_)) {
            if let Some((bound, bind_sql)) = self.bind_statement_hints_with_sql(&stmt) {
                self.apply_set_var_hints(&bound)?;
                stmt = bound;
                Some(bind_sql)
            } else {
                None
            }
        } else {
            None
        };
        // Go applies the effective statement's RESOURCE_GROUP hint only
        // after binding selection.  Activating the original tree first can
        // leak its group (and warning) when a binding supplies another hint.
        self.activate_statement_resource_group(&stmt);
        self.bind_variables(&mut stmt)?;
        self.try_add_extra_limit(&mut stmt);
        // Only an allocating INSERT sets it; every other statement reports 0.
        self.vars.statement_insert_id = 0;
        // The Apply channel describes THIS statement's plan.
        self.planned_apply
            .store(false, std::sync::atomic::Ordering::Relaxed);
        // Go's statement context starts without `ReadFromTableCache` and
        // without `WaitLockLeaseTime`.
        self.read_from_table_cache
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.wait_lock_lease_time = std::time::Duration::ZERO;
        // Go's row-id shard generator belongs to the TRANSACTION, so a
        // statement that IS its own transaction starts a fresh run. Inside an
        // explicit `BEGIN`/`COMMIT` the run continues across statements,
        // which is what makes `tidb_shard_allocate_step` count rows rather
        // than statements.
        if !self.in_transaction() {
            self.row_id_shards
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .end_run();
        }
        // With autocommit OFF a read or a write joins a transaction rather
        // than standing alone; DDL is left out because it commits the open
        // transaction instead of joining it.
        if self.statement_kind != StatementKind::Other {
            self.begin_implicit_transaction()?;
        }
        // Go's `CheckPrivilege` runs on the `visitInfo` the planner
        // collected, so it too refuses before any table is touched. Every
        // statement whose privileges are table-scoped reaches here: the
        // account statements `apply_schema_stmt` answers earlier demand their
        // own, statement-specific privileges instead. A query or a DML is
        // checked by the executor's build of it, over the visit infos of
        // that one build (its context carries the check, see
        // `Session::with_plan_privilege_check`); here it only meets Go's
        // preprocessor. A prepared EXECUTE checks the requests derived once
        // for its text (Go `checkPreparedPriv` on the stored `VisitInfos`).
        if matches!(&stmt, Stmt::Dml(dml) if matches!(dml.as_ref(),
            DmlStmt::Update(update) if matches!(&update.kind, tidb_ast::UpdateKind::Multi { .. })))
        {
            // Joined DML is rebuilt on every execution, including PREPARE,
            // and that build checks its visits against the current schema:
            // DDL can move an unqualified column to another table after
            // PREPARE.
            self.require_statement_table_privileges(&stmt)?;
        } else if compiled.is_some() {
            // `compile_stmt` already ran Go's `CheckPrivilege` for it.
        } else if let Some(requests) = prepared {
            self.check_table_privilege_requests(requests)?;
        } else {
            self.require_statement_table_privileges(&stmt)?;
        }
        // Go `Compiler.Compile` and then `ExecStmt.Exec` for an INSERT /
        // UPDATE / DELETE that brought no cached plan: the compiled statement
        // outlives its first execution, so a pessimistic retry executes it
        // again ([`Self::exec_compiled_dml_retry`]) without compiling.
        if dml_plan.is_none() && compiles_to_dml_plan(&stmt) {
            if let Some(cached) =
                self.execute_non_prepared_cached_dml(&stmt, prepared, binding_sql.as_deref(), sql)?
            {
                return Ok(cached);
            }
            let (compiled, ctx) = self.compile_dml(stmt, warnings_start)?;
            return self
                .exec_compiled_dml(compiled, Some(ctx))
                .map(PendingExecution::Complete);
        }
        // The same for a locking SELECT (Go `isSelectForUpdate`), the query a
        // pessimistic retry re-opens.
        if select_plan.is_none()
            && non_prepared.is_none()
            && self.statement_read_shape_bound(&stmt) == tidb_executor::access_path::StatementReadShape::LockingRead
            && matches!(&stmt, Stmt::Query(query) if matches!(query.as_ref(),
                tidb_ast::QueryStmt::Select(select) if select.into_outfile.is_none()))
        {
            let (compiled, ctx) = self.compile_locking_query(stmt, warnings_start)?;
            return self
                .open_compiled_locking_query(compiled, Some(ctx))
                .map(PendingExecution::Query);
        }
        // Go raises ErrNoDB where an unqualified NAME is resolved, not for
        // every statement: `SELECT 1` and `SELECT DATABASE()` both run with
        // no database selected (captured). The driver's own
        // `split_table_path` raises it at the resolution point, which is
        // where Go's does.
        let output = match &stmt {
            Stmt::Query(query) => {
                let current_db = self.vars.current_db.clone();
                let ctx = self.with_plan_privilege_check(
                    self.statement_context_for_stmt(&stmt, false),
                    &stmt,
                )?;
                // Go `buildSelectInto` wraps the target plan in a
                // `SelectIntoExec`, which drains it into the file and answers
                // no rows (the statement's affected rows are the rows written).
                let finish_select = |mut record_set: tidb_executor::driver::RecordSet, ctx: tidb_sessionctx::StmtContext| {
                    let into = match query.as_ref() {
                        tidb_ast::QueryStmt::Select(select) => select.into_outfile.as_ref(),
                        tidb_ast::QueryStmt::SetOpr(_) => None,
                    };
                    let Some(into) = into else {
                        return Ok(PendingExecution::Query(PendingQuery::new(record_set, ctx)));
                    };
                    let mut exec = tidb_executor::select_into::SelectIntoExec::new(into);
                    exec.open()?;
                    let drained = exec.next(&mut record_set, &ctx);
                    let closed = exec.close();
                    record_set.finish()?;
                    drained?;
                    Ok(PendingExecution::Complete(tidb_executor::StmtOutput::Affected(closed?)))
                };
                let tidb_ast::QueryStmt::Select(_) = &**query else {
                    let record_set =
                        self.compile_and_open_query(query, &mut select_plan, &current_db, &ctx)?;
                    return Ok(PendingExecution::Query(PendingQuery::new(record_set, ctx)));
                };
                if let Some(parameterized) = non_prepared.as_ref() {
                    let mut effective_parameterized = parameterized.statement.clone();
                    if binding_sql.is_some() {
                        let binding_hints = tidb_bindinfo::binding::collect_hints(&stmt);
                        tidb_bindinfo::binding::bind_hints(
                            &mut effective_parameterized,
                            &binding_hints,
                        );
                    }
                    if let Some(execution) = self.bind_non_prepared_select(
                        parameterized,
                        &effective_parameterized,
                        binding_sql.as_deref(),
                    )? {
                        let cache_hit = execution.cache_hit();
                        let schema_version = execution.schema_version();
                        // Go `PlanCacheParams`: the cached plan's markers read
                        // this statement's literals.
                        let ctx = ctx.clone().with_plan_cache_params(execution.parameters());
                        let result = execution.with_plan(|statement, physical| {
                            let Stmt::Query(query) = statement else {
                                unreachable!("a retained SELECT owns a query statement")
                            };
                            let tidb_ast::QueryStmt::Select(_) = query.as_ref() else {
                                unreachable!("a retained SELECT owns a SELECT query")
                            };
                            self.with_catalog_retrieving(&ctx, |catalog| {
                                (schema_version == catalog.schema_version())
                                    .then(|| {
                                        tidb_executor::driver::open_query_meta_stmt_with_physical(
                                            query,
                                            Some(&mut *physical),
                                            catalog,
                                            &current_db,
                                            &ctx,
                                        )
                                    })
                                    .transpose()
                            })
                        });
                        if let Some(Some(record_set)) = result.transpose()? {
                            self.vars.found_in_plan_cache = cache_hit;
                            return finish_select(record_set, ctx);
                        }
                    }
                }
                let record_set =
                    self.compile_and_open_query(query, &mut select_plan, &current_db, &ctx)?;
                return finish_select(record_set, ctx);
            }
            Stmt::Dml(dml) => {
                // Go parameterizes a non-prepared DML statement too (the
                // `tidb_enable_non_prepared_plan_cache_for_dml` switch, true
                // by default) and sends the retained marker-bearing statement
                // through the same plan cache as PREPARE.
                if let Some(cached) =
                    self.execute_non_prepared_cached_dml(&stmt, prepared, binding_sql.as_deref(), sql)?
                {
                    return Ok(cached);
                }
                match &**dml {
                    DmlStmt::Insert(insert) => {
                        let mut physical_plan = cached_dml_plan(dml_plan, "Insert")?;
                        let current_db = self.vars.current_db.clone();
                        let ctx = self.reset_insert_stmt_ctx(insert, &stmt);
                        let ctx = self.with_plan_privilege_check(ctx, &stmt)?;
                        if insert.source.is_some() {
                            self.write_sli.set_invalid();
                        }
                        // The statement's cached plan (a statement without one
                        // compiled above, `compile_dml`).
                        let result = self.retrieving_memory_tables(&ctx, |session| {
                            session.with_staged_catalog(|catalog| {
                                // Go executes the statement the protocol
                                // BOUND (`pkg/server`'s `statement` /
                                // `executableParams` carry the values):
                                // re-parsing `sql` here would run a tree whose
                                // markers never met their execute-time values.
                                tidb_executor::run_insert_stmt_with_physical(
                                    insert,
                                    catalog,
                                    &current_db,
                                    &ctx,
                                    physical_plan.as_deref_mut(),
                                )
                            })
                        });
                        self.drain_eval_warnings(&ctx);
                        // Go `session.LastInsertID()`, the OK packet's field:
                        // `StmtCtx.LastInsertID` when the statement PUBLISHED an
                        // allocated id, `StmtCtx.InsertID` -- the last explicit
                        // value -- otherwise. Both come off the same context the
                        // publication above reads, so the wire and
                        // `LAST_INSERT_ID()` cannot drift apart: what differs is
                        // only the fallback Go itself applies.
                        //
                        // Captured from TiDB: an allocating insert reports the id
                        // on both; `INSERT INTO t (id,v) VALUES (50,2)` reports 50
                        // on the wire while `LAST_INSERT_ID()` stays where it was;
                        // an `INSERT IGNORE` whose only row is a duplicate burns
                        // an id but reports 0 on the wire.
                        // The publication itself is promoted at the statement
                        // boundary by `publish_statement_status`, off the same
                        // cell this reads -- one channel, two readers.
                        self.vars.statement_insert_id = ctx
                            .published_last_insert_id()
                            .unwrap_or_else(|| ctx.given_insert_id());
                        // Go `InsertExec.setMessage`'s `Records: ...` text.
                        self.statement_message = ctx.message();
                        let (affected, _) = result?;
                        Ok(StmtOutput::Affected(affected))
                    }
                    DmlStmt::Update(update) => {
                        let physical_plan = cached_dml_plan(dml_plan, "Update")?;
                        let current_db = self.vars.current_db.clone();
                        let ctx = self.reset_update_stmt_ctx(update, &stmt);
                        let ctx = self.with_plan_privilege_check(ctx, &stmt)?;
                        // The statement's cached plan (a statement without one
                        // compiled above, `compile_dml`).
                        let mut physical_plan = physical_plan;
                        let output = match &update.kind {
                            tidb_ast::UpdateKind::Single(_) => {
                                self.retrieving_memory_tables(&ctx, |session| {
                                    session.with_staged_catalog(|catalog| {
                                        // Bound AST, not SQL text: the text still
                                        // carries the markers the binary protocol
                                        // already replaced. See the INSERT arm.
                                        Ok(StmtOutput::Affected(
                                            tidb_executor::run_update_stmt_with_physical(
                                                update,
                                                catalog,
                                                &current_db,
                                                &ctx,
                                                physical_plan.as_deref_mut(),
                                            )?,
                                        ))
                                    })
                                })
                            }
                            tidb_ast::UpdateKind::Multi { .. } => {
                                self.retrieving_memory_tables(&ctx, |session| {
                                    session.with_staged_catalog(|catalog| {
                                        Ok(StmtOutput::Affected(
                                            tidb_executor::run_update_stmt_with_physical(
                                                update,
                                                catalog,
                                                &current_db,
                                                &ctx,
                                                physical_plan.as_deref_mut(),
                                            )?,
                                        ))
                                    })
                                })
                            }
                        };
                        self.drain_eval_warnings(&ctx);
                        self.statement_message = ctx.message();
                        output
                    }
                    DmlStmt::Delete(delete) => {
                        let physical_plan = cached_dml_plan(dml_plan, "Delete")?;
                        let current_db = self.vars.current_db.clone();
                        let ctx = self.reset_delete_stmt_ctx(delete, &stmt);
                        let ctx = self.with_plan_privilege_check(ctx, &stmt)?;
                        // The statement's cached plan (a statement without one
                        // compiled above, `compile_dml`).
                        let mut physical_plan = physical_plan;
                        let output = match &delete.kind {
                            tidb_ast::DeleteKind::Single(_) => {
                                self.retrieving_memory_tables(&ctx, |session| {
                                    session.with_staged_catalog(|catalog| {
                                        // Bound AST, not SQL text -- see the
                                        // UPDATE arm.
                                        Ok(StmtOutput::Affected(
                                            tidb_executor::run_delete_stmt_with_physical(
                                                delete,
                                                catalog,
                                                &current_db,
                                                &ctx,
                                                physical_plan.as_deref_mut(),
                                            )?,
                                        ))
                                    })
                                })
                            }
                            tidb_ast::DeleteKind::Multi { .. } => {
                                self.retrieving_memory_tables(&ctx, |session| {
                                    session.with_staged_catalog(|catalog| {
                                        Ok(StmtOutput::Affected(
                                            tidb_executor::run_delete_stmt_with_physical(
                                                delete,
                                                catalog,
                                                &current_db,
                                                &ctx,
                                                physical_plan.as_deref_mut(),
                                            )?,
                                        ))
                                    })
                                })
                            }
                        };
                        self.drain_eval_warnings(&ctx);
                        // Go's info is the statement context's message, which
                        // a DELETE's foreign-key cascade `UPDATE` sets
                        // (`UpdateExec.setMessage` from its `Close`).
                        self.statement_message = ctx.message();
                        output
                    }
                    DmlStmt::ImportInto(import) => {
                        // Go `PlanBuilder.buildImportInto` checks assignments
                        // before `ImportIntoExec.Next` builds the import plan.
                        tidb_planner::import_into::check_import_into_input_vars(import).map_err(
                            |error| {
                                DriverError::Mysql(tidb_terror::MysqlError::unknown(
                                    error.message(),
                                ))
                            },
                        )?;
                        self.check_table_mode(&stmt)?;
                        // Physical ingest and the import task owner are still
                        // absent; successful validation must not write rows.
                        tidb_executor_importer::import_into_options::validate_import_into_options(import)
                            .and_then(|()| {
                                Err(DriverError::unsupported("IMPORT INTO is not supported yet"))
                            })
                    }
                    DmlStmt::Call(_) => {
                        // go's unsupported-statement wall names the AST node:
                        // `executor/executor.go`'s `Unsupported type
                        // *ast.CallStmt` (8108), not a generic 1105.
                        Err(DriverError::from(
                            (&tidb_plannererrors::ERR_UNSUPPORTED_TYPE)
                                .gen_with_stack("Unsupported type *ast.CallStmt".to_owned()),
                        ))
                    }
                    DmlStmt::LoadData(load) => self.run_load_data_stmt(load, &stmt),
                    DmlStmt::Batch(batch) => self.handle_non_transactional_dml(batch),
                    other => Err(DriverError::unsupported(format!(
                        "this DML statement kind ({}) is not supported yet",
                        variant_name(other)
                    ))),
                }
            }
            // Every other DDL is a job the session's node runs (Go's
            // `DDLExec` over the domain's DDL); only a local temporary
            // table and a hypothetical index belong to the session.
            Stmt::Ddl(ddl) => Err(DriverError::unsupported(format!(
                "{} is a DDL job, which the session's node runs",
                variant_name(&**ddl)
            ))),
            // SHOW compiles itself (`run_planned_show`), and a logical
            // ADMIN plan (`LogicalShowDDLJobs`) reaches its executor here.
            Stmt::Admin(admin) => tidb_executor::admin::AdminExec::admin_next(self, admin)
                .and_then(|output| {
                    output.ok_or_else(|| {
                        DriverError::unsupported(format!(
                            "this statement kind (ADMIN {}) is not supported yet",
                            variant_name(&**admin)
                        ))
                    })
                }),
            Stmt::Session(session) => Err(DriverError::unsupported(format!(
                "this statement kind ({}) is not supported yet",
                variant_name(&**session)
            ))),
        };
        output.map(PendingExecution::Complete)
    }
}

impl Session {
    /// Go `validateStatementInTxn`, which `ExecuteStmt` runs before
    /// `validateStatementReadOnlyInStaleness`: IMPORT INTO, PURGE
    /// MATERIALIZED VIEW LOG and REFRESH MATERIALIZED VIEW (also as the
    /// statement an EXECUTE names) cannot run in an explicit transaction.
    pub fn validate_statement_in_txn(&self, stmt: &Stmt, in_txn: bool) -> Result<(), DriverError> {
        if !in_txn {
            return Ok(());
        }
        let refused = |stmt: &Stmt| match stmt {
            Stmt::Dml(dml) if matches!(dml.as_ref(), tidb_ast::DmlStmt::ImportInto(_)) => Some("IMPORT INTO"),
            Stmt::Admin(admin) => match admin.as_ref() {
                tidb_ast::AdminStmt::PurgeMaterializedViewLog(_) => Some("PURGE MATERIALIZED VIEW LOG"),
                tidb_ast::AdminStmt::RefreshMaterializedView(_) => Some("REFRESH MATERIALIZED VIEW"),
                _ => None,
            },
            _ => None,
        };
        let refused = match stmt {
            Stmt::Session(session) => match session.as_ref() {
                tidb_ast::SessionStmt::Execute { name, .. } => self
                    .prepared_statement_sql(name)
                    .and_then(|sql| self.parse(sql).ok())
                    .and_then(|inner| refused(&inner)),
                _ => None,
            },
            stmt => refused(stmt),
        };
        if let Some(statement) = refused {
            return Err(DriverError::from(tidb_terror::MysqlError::unknown(format!(
                "cannot run {statement} in explicit transaction"
            ))));
        }
        Ok(())
    }

    /// The condition under which Go `validateStatementReadOnlyInStaleness`
    /// checks a statement at all: a stale transaction, a pinned read ts or
    /// external-ts reads, outside restricted SQL.
    pub(crate) fn staleness_requires_read_only(&self) -> bool {
        !self.vars.restricted_sql
            && (self.is_staleness_txn()
                || self.vars.txn_read_ts() != 0
                || self.external_timestamp_read_enabled())
    }

    /// Go `validateStatementReadOnlyInStaleness`'s `ExecuteStmt` arm, for the
    /// text `EXECUTE` and COM_STMT_EXECUTE alike: it asks only
    /// `IsReadOnlyInternal`, which answers by the prepared statement (not
    /// read-only when there is none), so a prepared locking SELECT is refused
    /// as not read-only rather than for its lock.
    pub(crate) fn validate_execute_read_only_in_staleness(
        &self,
        prepared: Option<&Stmt>,
    ) -> Result<(), DriverError> {
        if self.staleness_requires_read_only()
            && !prepared.is_some_and(|stmt| stmt.is_read_only(true))
        {
            return Err(DriverError::unsupported(
                "only support read-only statement during read-only staleness transactions",
            ));
        }
        Ok(())
    }

    /// Go `validateStatementReadOnlyInStaleness` (`session.go:2822`), which
    /// `ExecuteStmt` runs before `Compile`. It switches on the statement node
    /// itself: a top-level SELECT's lock, the read-only question for the
    /// statements it lists, and any DML; a lock inside a derived table or a
    /// view body, and every DDL, pass it.
    pub fn validate_statement_read_only_in_staleness(&self, stmt: &Stmt) -> Result<(), DriverError> {
        // Go's server driver hands a `NonTransactionalDMLStmt` to
        // `HandleNonTransactionalDML` instead of `ExecuteStmt`, so this check
        // never sees it: `checkConstraint` refuses a pending snapshot or
        // `tx_read_ts` read first, and each split statement it runs is checked
        // here on its own.
        if matches!(stmt, Stmt::Dml(dml) if matches!(dml.as_ref(), tidb_ast::DmlStmt::Batch(_))) {
            return Ok(());
        }
        if self.staleness_requires_read_only() {
            // Go master: COMPARE reads its snapshots on its own; only its
            // OUTPUT table is a write.
            if let Stmt::Admin(admin) = stmt {
                if let tidb_ast::AdminStmt::CompareMaterializedView(compare) = admin.as_ref() {
                    if compare.output_table.is_some() {
                        return Err(DriverError::unsupported(
                            "only support read-only statement during read-only staleness transactions",
                        ));
                    }
                    return Ok(());
                }
            }
            if let Stmt::Session(session) = stmt {
                if let tidb_ast::SessionStmt::Execute { name, .. } = session.as_ref() {
                    return self.validate_execute_read_only_in_staleness(
                        self.prepared_statements.get(name).map(|prepared| &prepared.statement),
                    );
                }
            }
            let locked = matches!(stmt, Stmt::Query(query) if matches!(
                query.as_ref(),
                tidb_ast::QueryStmt::Select(select) if select.lock.is_some()
            ));
            if locked {
                return Err(DriverError::unsupported(
                    "select lock hasn't been supported in stale read yet",
                ));
            }
            let read_only_checked = matches!(&stmt, Stmt::Dml(_) | Stmt::Query(_))
                || matches!(&stmt, Stmt::Admin(admin) if matches!(admin.as_ref(), tidb_ast::AdminStmt::Explain(
                    _,
                ) | tidb_ast::AdminStmt::Trace(_)));
            if read_only_checked && !stmt.is_read_only(true) {
                return Err(DriverError::unsupported(
                    "only support read-only statement during read-only staleness transactions",
                ));
            }
        }
        Ok(())
    }

    /// Go `buildExplain` builds the EXPLAIN's target executor; for an INSERT,
    /// UPDATE or DELETE that is `updateForUpdateTS`, which a stale
    /// transaction's provider refuses (`StalenessTxnContextProvider
    /// .GetStmtForUpdateTS`). A SELECT ... FOR UPDATE is refused before this
    /// (`validateStatementReadOnlyInStaleness`).
    fn check_explain_target_for_update_ts(&self, stmt: &Stmt) -> Result<(), DriverError> {
        if !self.is_staleness_txn() {
            return Ok(());
        }
        let Stmt::Admin(admin) = stmt else {
            return Ok(());
        };
        let tidb_ast::AdminStmt::Explain(explain) = admin.as_ref() else {
            return Ok(());
        };
        if matches!(
            explain.statement(),
            Some(Stmt::Dml(dml)) if matches!(
                dml.as_ref(),
                tidb_ast::DmlStmt::Insert(_) | tidb_ast::DmlStmt::Update(_) | tidb_ast::DmlStmt::Delete(_)
            )
        ) {
            return Err(DriverError::unsupported("GetForUpdateTS not supported for stalenessTxnProvider"));
        }
        Ok(())
    }

    /// Go `NeedSetRCCheckTSFlag`, which `PessimisticRCTxnContextProvider`
    /// asks at statement start: a user connection's read-only statement,
    /// not a retry, inside a transaction, with `tidb_rc_read_check_ts` on.
    pub(crate) fn need_set_rc_check_ts_flag(&self, stmt: &Stmt) -> bool {
        self.vars.connection_id.is_some_and(|id| id > 0)
            && self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_RC_READ_CHECK_TS)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
            && self.in_transaction()
            && !self.retrying
            && stmt.is_read_only(true)
    }

    /// Go `TxnContextProvider.OnStmtStart`'s statement-scoped decision,
    /// made before the statement's snapshot is prepared: whether it reads
    /// with `RCCheckTS`.
    pub(crate) fn on_stmt_start(&mut self, stmt: &Stmt) {
        // Go's fresh `StmtCtx`; the RC provider's `OnStmtStart` sets it.
        self.stmt_rc_check_ts = false;
        self.stmt_txn_plan_advisor = None;
        self.stmt_weak_consistency = self.is_weak_consistency_read(stmt);
        self.stmt_is_read_only = stmt.is_read_only(true);
    }

    /// Go `ExecStmt.IsReadOnly(vars)`: `vars.StmtCtx.IsReadOnly`.
    pub(crate) const fn stmt_is_read_only(&self) -> bool {
        self.stmt_is_read_only
    }

    /// Go `isWeakConsistencyRead`: a user connection's read-only statement
    /// in an autocommit transaction (`IsAutoCommitTxn`) under
    /// `tidb_read_consistency = 'weak'`.
    fn is_weak_consistency_read(&self, stmt: &Stmt) -> bool {
        self.vars.connection_id.is_some_and(|id| id > 0)
            && self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_READ_CONSISTENCY)
                .is_ok_and(|value| value.eq_ignore_ascii_case("weak"))
            && self.is_autocommit()
            && !self.in_transaction()
            && stmt.is_read_only(true)
    }

    /// Go `StmtCtx.RCCheckTS` for the running statement.
    pub(crate) fn stmt_rc_check_ts(&self) -> bool {
        self.stmt_rc_check_ts
    }

    /// Go `StmtCtx.RCCheckTS = true` (the RC provider's `OnStmtStart`).
    pub(crate) fn set_stmt_rc_check_ts(&mut self) {
        self.stmt_rc_check_ts = true;
    }

    /// The plan advisor of the session transaction's provider (Go
    /// `AdviseOptimizeWithPlan`), as its `OnStmtStart` published it.
    pub(crate) fn txn_plan_advisor(&self) -> Option<std::sync::Arc<dyn tidb_sessionctx::TxnPlanAdvisor>> {
        self.stmt_txn_plan_advisor.clone()
    }

    /// Publishes the provider's advisor for a statement that reads neither a
    /// `tidb_snapshot` nor a stale timestamp (the providers return early for
    /// those).
    pub(crate) fn set_txn_plan_advisor(
        &mut self,
        provider: &dyn tidb_sessiontxn::TxnContextProvider,
    ) {
        // Go `IsAutoCommitTxn`.
        let auto_commit_txn = self.is_autocommit() && !self.in_transaction();
        self.stmt_txn_plan_advisor = (self.historical_read_ts().is_none() && !self.txn_is_stale_read())
            .then(|| provider.plan_advisor(self.retrying, self.rc_write_check_ts(), auto_commit_txn))
            .flatten();
    }

    /// Go `SessionVars.RcWriteCheckTS`, set by `tidb_rc_write_check_ts`.
    pub(crate) fn rc_write_check_ts(&self) -> bool {
        self.vars
            .get_system(tidb_vardef::tidb_vars::TIDB_RC_WRITE_CHECK_TS)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    /// The running statement's next execution is a retry of it (Go
    /// `handlePessimisticLockError` and `session.retry` keep the `StmtCtx`
    /// and call `ResetForRetry`).
    pub fn set_retrying_statement(&mut self, retrying: bool) {
        self.retrying_statement = retrying;
    }

    /// Go `RetryInfo.Retrying`, set around a statement the connection
    /// re-runs.
    pub fn set_retrying(&mut self, retrying: bool) {
        self.retrying = retrying;
    }

    /// Go `OnStmtErrorForNextAction(StmtErrAfterQuery)` on the transaction's
    /// provider, for a statement that failed before any of its rows reached
    /// the client: whether the connection retries it.
    pub fn retry_after_query_error(&mut self, code: u16) -> bool {
        if self.node.is_none() {
            return false;
        }
        self.node_conn().retry_after_query_error(code)
    }

    pub(crate) fn external_timestamp_read_enabled(&self) -> bool {
        !self.vars.restricted_sql
            && self
                .vars
                .get_system("tidb_enable_external_ts_read")
                .is_ok_and(|v| v == "ON")
    }

    pub(crate) fn external_read_ts(&self) -> Result<Option<u64>, DriverError> {
        if !self.external_timestamp_read_enabled() {
            return Ok(None);
        }
        let ts = self
            .vars
            .get_global("tidb_external_ts")
            .map_err(crate::variables::var_error)?
            .parse::<u64>()
            .map_err(|_| DriverError::unsupported("invalid external timestamp"))?;
        Ok((ts != 0).then_some(ts))
    }

    /// Selects one statement timestamp before historical schema and row reads.
    /// Explicit transactions retain their selected timestamp in Transaction.
    fn configured_historical_read_ts(&mut self) -> Result<Option<u64>, DriverError> {
        if self.historical_read_provider.is_none() {
            return Ok(None);
        }
        let snapshot = self.vars.snapshot_ts();
        if snapshot != 0 {
            return Ok(Some(snapshot));
        }
        let one_shot = self.vars.use_txn_read_ts();
        if one_shot != 0 {
            return Ok(Some(one_shot));
        }
        let seconds = self
            .vars
            .get_system("tidb_read_staleness")
            .unwrap_or_default()
            .parse::<i64>()
            .unwrap_or(0);
        if seconds == 0 {
            return self.external_read_ts();
        }
        let now = self.eval_value(&tidb_ast::Expr::Func {
            name: "NOW".into(),
            args: vec![tidb_ast::Expr::Int("6".into())],
            origin_position: 0,
        })?;
        let now_ts = self.as_of_value_to_ts(&tidb_ast::Expr::String(
            now.sql_string().unwrap_or_default(),
        ))?;
        // Go `CalculateTsWithReadStaleness`: `now + readStaleness`, clamped
        // by `CalAppropriateTime` between it and `now` at the stores' min
        // SafeTS (`GetStmtMinSafeTime`; zero without a store). A read above
        // the SafeTS is validated when its snapshot opens
        // (`ValidateSnapshotReadTS`).
        let staleness_ms = i64::try_from(seconds.unsigned_abs().saturating_mul(1000)).unwrap_or(i64::MAX);
        let now_physical = tikv_client::oracle::extract_physical(now_ts);
        let min_physical = now_physical.saturating_sub(staleness_ms).max(0);
        let min_safe_ts = self
            .min_safe_ts_provider
            .as_ref()
            .map_or(0, |provider| provider(&tidb_config::tikvcfg::get_txn_scope_from_config()));
        let safe_physical = tikv_client::oracle::extract_physical(min_safe_ts);
        let read_physical = safe_physical.clamp(min_physical, now_physical);
        Ok(Some(tikv_client::oracle::compose_ts(read_physical, 0)))
    }

    fn refuse_pinned_historical_read(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        if self.historical_read_provider.is_some() {
            self.validate_statement_read_only_in_staleness(stmt)?;
            return tidb_executor::adapter::check_no_delay_snapshot_writes(self, stmt);
        }

        if self.vars.snapshot_ts() != 0 {
            return Err(DriverError::unsupported(
                "reading at @@tidb_snapshot is not supported yet",
            ));
        }
        // `@@tidb_read_staleness` needs no history here. Go applies it to a
        // SELECT only (preprocess `p.stmtTp == TypeSelect`), reading at
        // `CalAppropriateTime(now + staleness, now, minSafeTS)`. This store
        // reports no safe ts, as unistore's `GetStoreSafeTS` does, which
        // client-go's `getMinSafeTSByStores` reads as MaxUint64: the read is
        // clamped to now, the current read. Any other statement ignores it.
        Ok(())
    }
}

impl tidb_executor::builder::BuildSession for Session {
    fn with_account_service(
        &mut self,
        run: &mut dyn FnMut(&mut Self) -> Result<Option<StmtOutput>, DriverError>,
    ) -> Result<Option<StmtOutput>, DriverError> {
        Session::with_account_service(self, |session| run(session))
    }

    fn is_account_statement(&self, stmt: &Stmt) -> bool {
        Session::is_account_statement(stmt)
    }

    fn client_statement_sql(&self, stmt: &Stmt) -> String {
        let client = self.vars.build_parser_config().charset_client;
        tidb_parser::carry_client_sql(stmt.original_text(), client).into_owned()
    }

    fn set_config_server_info(
        &self,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<tidb_domain::cluster_topology::ClusterServer>, String> {
        match &self.test_set_config_server_info {
            Some(server_info) => server_info(),
            None => tidb_executor::calibrate_resource::get_cluster_server_info(self.cluster_topology.as_deref(), warnings),
        }
    }

    fn cluster_config_client(&self) -> Result<std::sync::Arc<tidb_executor::cluster_config::ClusterConfigClient>, String> {
        match &self.cluster_config {
            Some(client) => Ok(std::sync::Arc::clone(client)),
            None => tidb_executor::cluster_config::ClusterConfigClient::new(&Default::default()).map(std::sync::Arc::new),
        }
    }

    fn set_config_http_handler(
        &self,
    ) -> Option<std::sync::Arc<dyn Fn(&str, String) -> Result<(u16, String, String), String> + Send + Sync>> {
        self.test_set_config_http_handler.clone()
    }

    fn alter_ddl_job(&mut self, alter: &tidb_planner::common_plans::AlterDdlJob) -> Result<(), DriverError> {
        match self.domain_services() {
            Some(services) => services.ddl.alter_ddl_job(alter),
            None => Err(DriverError::Mysql(tidb_executor::operate_ddl_jobs::ddl_job_not_running(alter.job_id))),
        }
    }
}
