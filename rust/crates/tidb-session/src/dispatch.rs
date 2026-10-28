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

use tidb_ast::{DmlStmt, SessionStmt, Stmt};
use tidb_executor::{Catalog, DriverError};
use tidb_hack::GoCaseCompare;

use crate::record_set::{PendingExecution, PendingQuery, QueryTransactionEnd};
use crate::{
    statement_kind_of, statement_priority_of, Session, StatementKind, StmtOutput, WarningLevel,
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

fn sem_table_option(option: &tidb_ast::TableOption) -> tidb_util::sem_v2::TableOptionType {
    match option {
        tidb_ast::TableOption::Ttl { .. } => tidb_util::sem_v2::TableOptionType::Ttl,
        tidb_ast::TableOption::TtlEnable(_) => tidb_util::sem_v2::TableOptionType::TtlEnable,
        tidb_ast::TableOption::TtlJobInterval(_) => {
            tidb_util::sem_v2::TableOptionType::TtlJobInterval
        }
        _ => tidb_util::sem_v2::TableOptionType::Other,
    }
}

fn sem_stmt_kind(stmt: &Stmt) -> tidb_util::sem_v2::StmtKind {
    use tidb_util::sem_v2::{AlterTableSpec, AlterTableType, StmtKind};

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

fn sem_stmt_view(stmt: &Stmt) -> tidb_util::sem_v2::StmtView {
    tidb_util::sem_v2::StmtView {
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

pub(crate) fn filter_sem_restricted_hints(stmt: &mut Stmt) -> Vec<String> {
    struct Filter {
        warnings: Vec<String>,
    }

    impl Filter {
        fn retain(&mut self, hints: &mut Vec<tidb_ast::Hint>) {
            hints.retain(
                |hint| match tidb_util::sem_v2::is_restricted_hint(&hint.lower_name()) {
                    Ok(()) => true,
                    Err(warning) => {
                        self.warnings.push(warning);
                        false
                    }
                },
            );
        }
    }

    impl tidb_ast::Visitor for Filter {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
                self.retain(&mut select.hints);
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.retain(&mut insert.hints);
            } else if let Some(update) = node.downcast_mut::<tidb_ast::UpdateStmt>() {
                self.retain(&mut update.hints);
            } else if let Some(delete) = node.downcast_mut::<tidb_ast::DeleteStmt>() {
                self.retain(&mut delete.hints);
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut filter = Filter {
        warnings: Vec::new(),
    };
    tidb_ast::Visitable::accept(stmt, &mut filter);
    filter.warnings
}

/// A planner-owned SELECT tree offered to the ordinary statement executor.
/// `used` records whether the schema still matched while the catalog was
/// locked; a moved schema falls through to ordinary physical planning.
struct RetainedSelectPlan<'a> {
    physical: &'a mut tidb_planner::physical::PhysicalPlan,
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

/// Names the AST variant behind a refused statement, for a "not supported
/// yet" message that says WHAT it refused instead of just that it did.
///
/// Every statement enum here derives `Debug`, and a derived `Debug` for an
/// enum always starts with the bare variant name (`ShowMasterStatus`, or
/// `Explain(ExplainStmt { .. })` for one carrying a payload) -- so taking the
/// leading identifier out of `{:?}` names the variant without a second,
/// separately-maintained match arm per statement kind.
pub(crate) fn variant_name<T: std::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Names a statement one layer deeper than [`Stmt`]'s own variant: naming the
/// wrapper alone ("Ddl", "Admin") would repeat what the caller already knows
/// it saw a DDL or admin statement, so this names the DDL/ADMIN/SESSION kind
/// it wraps instead -- the same depth `explain_stmt`'s own "EXPLAIN of a WITH
/// clause" / "EXPLAIN of a set operation" messages already name theirs at.
pub(crate) fn stmt_kind_name(stmt: &Stmt) -> String {
    match stmt {
        Stmt::Query(query) => variant_name(&**query),
        Stmt::Dml(dml) => variant_name(&**dml),
        Stmt::Ddl(ddl) => variant_name(&**ddl),
        Stmt::Admin(admin) => variant_name(&**admin),
        Stmt::Session(session) => variant_name(&**session),
    }
}

fn cached_dml_plan<'a>(
    plan: Option<&'a mut tidb_planner::physical::PhysicalPlan>,
    operator: &str,
) -> Result<Option<&'a mut tidb_planner::physical::PhysicalPlan>, DriverError> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let tidb_planner::physical::PhysicalPlan::Dml(dml) = &*plan else {
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
impl Session {
    /// The checks every compiled statement passes before its executor: the
    /// `tidb_snapshot` write refusal, the implicit commit Go's `DDLExec`,
    /// `GrantExec`, `RevokeExec` and `SimpleExec.autoNewTxn` make. (The staleness refusal precedes
    /// `Compile`, and the DDL preprocess runs inside it.)
    pub(crate) fn statement_preamble(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        self.validate_snapshot_write(stmt)?;
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

    /// Go `executorBuilder.build` and the executor's `Next` for a common
    /// plan: `SimpleExec`, `DDLExec` (the schema statements this tier
    /// answers here; table DDL runs in the statement's later arms) and the
    /// ADMIN plans. `None` for a plan another door runs.
    pub(crate) fn execute_plan(
        &mut self,
        plan: &tidb_planner::common_plans::Plan,
        stmt: &Stmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        use tidb_planner::common_plans::Plan;
        match plan {
            // The account statements' executors write the domain's
            // privilege tables (`Session::with_account_service`).
            Plan::Simple(_)
                if Self::is_account_statement(stmt) =>
            {
                self.with_account_service(|session| {
                    tidb_executor::simple::exec::SimpleExec::simple_next(session, stmt)
                })
            }
            Plan::Simple(_) => tidb_executor::simple::exec::SimpleExec::simple_next(self, stmt),
            Plan::Ddl(_) => self.ddl_exec(stmt),
            Plan::CompactTable(compact) => self.compact_table_tiflash_exec(compact).map(Some),
            Plan::Logical(_) | Plan::Physical(_) | Plan::ShowNextRowID(_) | Plan::Set(_) => {
                Ok(None)
            }
            _ => match stmt {
                Stmt::Admin(admin) => tidb_executor::admin::AdminExec::admin_next(self, admin),
                _ => Ok(None),
            },
        }
    }

    /// Go `CompactTableTiFlashExec.Next` over the table the planner resolved
    /// through the session's infoschema (`buildCompactTable`'s
    /// `resolveCtx.GetTableName`), a read.
    fn compact_table_tiflash_exec(
        &mut self,
        compact: &tidb_ast::CompactTableStmt,
    ) -> Result<StmtOutput, DriverError> {
        let ctx = self.statement_context(false);
        let result = self.with_planning_catalog(|catalog| {
            tidb_executor::compact_table::execute_compact_table(
                compact,
                catalog,
                &self.vars.current_db,
                &ctx,
            )
        });
        self.drain_eval_warnings(&ctx);
        result.map(|()| StmtOutput::Affected(0))
    }

    /// Answers the memory-table scans the statement's executors asked for
    /// (`StmtContext::mem_table_retrieval`): Go's `MemTableReaderExec` calls
    /// its retriever when it is read; the retrievers here need this session,
    /// so the executor states what it needs and the session retrieves it.
    /// Returns how many scans were answered.
    pub(crate) fn retrieve_memory_tables(
        &mut self,
        ctx: &tidb_executor::StmtContext,
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
        let retriever = tidb_executor::memtable_retrievers::InformationSchemaRetriever::new(
            self.schema_visibility(),
            self.table_storage_stats.clone(),
            self.active_resource_group.clone(),
        );
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = crate::instance_state::InstanceStateRetriever::new(self.viewer_state());
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
        let retriever = crate::account_state::AccountRetriever::new(self.account_state());
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = crate::account_state::VariablesRetriever::new(self.variables_state());
        if tidb_executor::mem_table_retrieval::MemTableRetriever::serves(&retriever, read) {
            return Some(std::sync::Arc::new(retriever));
        }
        let retriever = crate::cluster_state::ClusterTablesRetriever::new(self.viewer_state());
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
        ctx: &tidb_executor::StmtContext,
        mut attempt: impl FnMut(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.retrieving_memory_tables(ctx, |session| session.with_catalog_mut(&mut attempt))
    }

    /// [`Self::with_catalog_retrieving`] over any way of running the
    /// statement, such as a staged catalog for a write.
    pub(crate) fn retrieving_memory_tables<T>(
        &mut self,
        ctx: &tidb_executor::StmtContext,
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
        registry: Option<&crate::process::ProcessRegistry>,
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        self.local_cluster_table_rows_for(table_name, registry, zone, None, None)
    }

    /// Receiving scan requirements use original column names, including predicates.
    /// None retains full-row callers; a present empty set needs no expensive fields.
    pub fn local_cluster_table_rows_for(
        &mut self,
        table_name: &str,
        registry: Option<&crate::process::ProcessRegistry>,
        zone: &tidb_datatype::SessionTimeZone,
        required: Option<&std::collections::HashSet<String>>,
        digests: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let ctx = self.statement_context(false);
        let viewer = self.viewer_state();
        let rows = self.with_catalog_mut(|catalog| {
            viewer.local_cluster_table_rows_for(
                &ctx, catalog, table_name, registry, zone, required, digests,
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
    /// sizes estimated from Go `TableRowStatsCache` (and its column lengths
    /// with `needs_column_lengths`) through the node's provider, which serves
    /// its cache when a refresh fails; the in-process store answers from its
    /// own table statistics.
    ///
    /// Go's `setDataFromTables` / `setDataFromPartitions` walk the
    /// statement's InfoSchema and refresh the cache for exactly those
    /// physical IDs (`updateStatsCacheIfNeed(sctx, tbls)`), so a table the
    /// statement's snapshot holds is sized even when a later DDL dropped it.
    pub(crate) fn table_storage_statistics_catalog(
        &mut self,
        needs_column_lengths: bool,
    ) -> Result<tidb_executor::Catalog, DriverError> {
        let scratch = self.with_catalog_mut(|catalog| Ok(catalog.clone()))?;
        tidb_executor::memtable_retrievers::table_storage_statistics_catalog_of(
            scratch,
            &self.stats_handle,
            self.table_storage_stats.as_deref(),
            &self.active_resource_group,
            needs_column_lengths,
        )
    }

    /// Pinned Go `memtableRetriever.setDataForClientErrorsSummary`.
    pub(crate) fn client_errors_summary_rows(
        table_name: &str,
        has_process: bool,
        login_user: Option<&str>,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use chrono::{DateTime, Local};
        use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};
        use tidb_error::tidb::infoschema::{self, ErrorStats};

        fn text(value: &str) -> Datum {
            Datum::new_string(value.as_bytes())
        }

        fn count(value: isize) -> Datum {
            Datum::Int(i64::try_from(value).expect("client error count fits i64"))
        }

        fn timestamp(value: Option<std::time::SystemTime>) -> Datum {
            let time = match value {
                Some(value) => {
                    let local: DateTime<Local> = value.into();
                    Time::new(core_time_from_datetime(local), TimeType::Timestamp, 0)
                }
                None => Time::new(CoreTime::from_raw(0), TimeType::Timestamp, 0),
            }
            .expect("fsp 0 is valid for client-error timestamps");
            Datum::new_time(time)
        }

        fn message(code: u16) -> &'static str {
            tidb_error::mysql::message_by_code(code)
                .or_else(|| tidb_error::tidb::message_by_code(code))
                .map_or("", |message| message.raw)
        }

        fn summary_cells(code: u16, summary: &infoschema::ErrorSummary) -> Vec<Datum> {
            vec![
                Datum::Int(i64::from(code)),
                text(message(code)),
                count(summary.error_count),
                count(summary.warning_count),
                timestamp(Some(summary.first_seen)),
                timestamp(summary.last_seen),
            ]
        }

        fn append_scoped(rows: &mut Vec<Vec<Datum>>, scope: &str, stats: ErrorStats) {
            for (code, summary) in stats {
                let mut row = Vec::with_capacity(7);
                row.push(text(scope));
                row.extend(summary_cells(code, &summary));
                rows.push(row);
            }
        }

        if !has_process
            && (table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_GLOBAL")
                || table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_BY_HOST"))
        {
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["PROCESS".into()]),
            ));
        }

        let mut rows = Vec::new();
        if table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_GLOBAL") {
            for (code, summary) in infoschema::global_stats() {
                rows.push(summary_cells(code, &summary));
            }
        } else if table_name.go_lower_eq("CLIENT_ERRORS_SUMMARY_BY_HOST") {
            for (host, stats) in infoschema::host_stats() {
                append_scoped(&mut rows, &host, stats);
            }
        } else {
            let login_username = login_user
                .and_then(|identity| identity.split_once('@').map(|(user, _)| user));
            for (user, stats) in infoschema::user_stats() {
                if !has_process && login_username.is_some_and(|login| login != user) {
                    continue;
                }
                append_scoped(&mut rows, &user, stats);
            }
        }
        Ok(rows)
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
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        self.execute_prepared_ast(sql, stmt, privilege_requests)
    }

    /// `privilege_requests` are the ones PREPARE derived for this statement
    /// (Go `PlanCacheStmt.VisitInfos`, checked by `checkPreparedPriv`).
    pub(crate) fn execute_prepared_ast(
        &mut self,
        sql: &str,
        stmt: Stmt,
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
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
                for warning in tidb_executor::take_fold_warnings() {
                    self.append_sql_warning(warning);
                }
                Err(error)
            }
        }
    }

    pub(crate) fn prepare_bound_execution(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) -> Result<PendingExecution, DriverError> {
        let parameters = tidb_executor::bound_parameter_values(&mut stmt)?;
        self.begin_prepared_statement_boundary(&stmt, parameters);
        if let Some(reason) = self.pending_plan_cache_refusal.take() {
            self.append_warning(
                crate::warnings::WarningLevel::Warning,
                1105,
                format!("skip prepared plan-cache: {reason}"),
            );
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
            self.append_warning(
                crate::warnings::WarningLevel::Warning,
                1105,
                format!("skip prepared plan-cache: {reason}"),
            );
        } else if crate::prepared_statements::plans_over_table_dual(&stmt) {
            self.append_warning(
                crate::warnings::WarningLevel::Warning,
                1105,
                "skip prepared plan-cache: get a TableDual plan".to_owned(),
            );
        }
        self.prepare_parsed_statement_with_optional_physical_plan(
            sql,
            stmt,
            Some(privilege_requests),
            None,
            None,
        )
    }

    /// Executes a bound PREPARE tree, keeping a query open until its result closes.
    pub fn execute_bound_record_set_for<'a>(
        &'a mut self,
        stmt: Stmt,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::StatementExecution<'a>, DriverError> {
        // On a node a write commits through the node, as Go's
        // `ExecutePreparedStmt` does on its store; privileges are checked
        // there against the schema the bound statement resolves to now.
        if !matches!(stmt, Stmt::Query(_)) {
            if let Some(mut node) = self.node.take() {
                let output = node.run_bound_prepared_write(self, stmt, prepared);
                self.node = Some(node);
                return Ok(crate::StatementExecution::Complete(output?));
            }
        }
        Ok(self.open_bound_record_set_for(stmt, prepared)?.attach(self))
    }

    /// Opens a bound PREPARE tree for a front end that owns session teardown.
    pub fn open_bound_record_set_for(
        &mut self,
        stmt: Stmt,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::OpenedStatement, DriverError> {
        self.bind_node_snapshot_for_open_statement()?;
        self.begin_statement_execution(prepared.sql())?;
        let result =
            self.prepare_bound_execution(prepared.sql(), stmt, prepared.privilege_requests());
        self.return_opened_record_set(result)
    }

    /// Executes the subset Go serves through a prepared `PointGetPlan`. The
    /// plan has already passed the statement-shape, schema, stale-read,
    /// binding, and hint gates, and each call creates fresh mutable execution
    /// state. The execution carries the complete binding-aware cache-key hit
    /// result rather than receiving a protocol-local readiness flag.
    pub fn open_prepared_point_get(
        &mut self,
        execution: tidb_executor::PreparedPointGetExecution,
        statement: &Stmt,
        sql: &str,
    ) -> Result<Option<crate::OpenedStatement>, DriverError> {
        self.bind_node_snapshot_for_open_statement()?;
        self.begin_statement_execution(sql)?;
        let result = (|| {
            self.set_statement_arbitration_key(sql);
            let cache_hit = execution.cache_hit();
            self.active_resource_group.clone_from(&self.vars.resource_group);
            self.publish_statement_resource_group();
            self.begin_cached_prepared_query_boundary();
            // Go Optimize applies StmtHints.SetVars for cached plans too. The
            // effective PREPARE tree carries the same binding-selected hints as
            // the ordinary SELECT path; the point plan alone cannot supply them.
            self.apply_set_var_hints(statement)?;
            self.activate_statement_resource_group(statement);
            let plan = execution.plan();
            self.require_named_table_privilege(
                plan.names().0,
                plan.names().1,
                tidb_mysql::SelectPriv,
            )?;
            // `dirty_content` only gates scan/access-path planning. This cached
            // executor owns one handle read and the admission gate already refuses
            // an open transaction, so walking every catalog table cannot affect
            // its result.
            self.vars.statement_insert_id = 0;
            self.statement_kind = StatementKind::Select;

            if self.in_transaction() {
                let names = [(plan.names().0.to_owned(), plan.names().1.to_owned())];
                self.record_mdl_related_table_names(&names);
            }
            let ctx = self.prepared_point_get_context();
            let stmt_ctx = self.statement_context_for_stmt(statement, false);
            let result = self.with_catalog_mut(|catalog| {
                tidb_executor::open_prepared_point_get(&execution, catalog, &ctx, &stmt_ctx)
            })?;
            let Some(record_set) = result else {
                return Ok(None);
            };
            self.vars.found_in_plan_cache = cache_hit;
            let query = PendingQuery::new(record_set, stmt_ctx);
            Ok(Some(PendingExecution::Query(query)))
        })();
        match result {
            Ok(None) => Ok(None),
            Ok(Some(execution)) => self.return_opened_record_set(Ok(execution)).map(Some),
            Err(error) => self.return_opened_record_set(Err(error)).map(Some),
        }
    }

    /// Executes a prepared SELECT through the ordinary statement and
    /// executor funnel, offering the retained physical tree at the same seam
    /// where a fresh plan is handed to the executor builder.
    /// Runs a cached prepared SELECT for the definition PREPARE retained;
    /// its privilege requests are the ones derived then (Go
    /// `checkPreparedPriv` on `PlanCacheStmt.VisitInfos`).
    pub fn execute_prepared_select_for(
        &mut self,
        execution: &tidb_executor::PreparedSelectExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<StmtOutput, DriverError> {
        self.bind_node_snapshot_for_open_statement()?;
        self.execute_prepared_select(execution, prepared.sql(), prepared.privilege_requests())
    }

    pub(crate) fn execute_prepared_select(
        &mut self,
        execution: &tidb_executor::PreparedSelectExecution,
        sql: &str,
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        self.execute_prepared_select_internal(execution, sql, privilege_requests, false)
            .map(|(output, _)| output)
    }

    pub(crate) fn execute_prepared_select_internal(
        &mut self,
        execution: &tidb_executor::PreparedSelectExecution,
        sql: &str,
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
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
        execution: &tidb_executor::PreparedSelectExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::StatementExecution<'a>, DriverError> {
        self.bind_node_snapshot_for_open_statement()?;
        Ok(self
            .open_prepared_record_set_for(execution, prepared)?
            .attach(self))
    }

    /// Opens a cached SELECT for a front end that owns session teardown.
    pub fn open_prepared_record_set_for(
        &mut self,
        execution: &tidb_executor::PreparedSelectExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<crate::OpenedStatement, DriverError> {
        self.bind_node_snapshot_for_open_statement()?;
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
        execution: &tidb_executor::PreparedSelectExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<Option<PendingExecution>, DriverError> {
        let started = std::time::Instant::now();
        let cache_hit = execution.cache_hit();
        let schema_version = execution.schema_version();
        let table_names = execution.plan().table_names().to_vec();
        let parameters = execution.parameters();
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
            if self.in_transaction() {
                self.record_mdl_related_table_names(&table_names);
            }
            let current_db = self.vars.current_db.clone();
            let stmt_ctx = self
                .statement_context_for_stmt(statement, false)
                .with_prepared_params(parameters.clone());
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

        crate::metrics::observe_compile_duration(
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
        execution: &tidb_executor::PreparedSelectExecution,
        sql: &str,
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
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

    /// Executes a bound prepared DML statement through the ordinary statement
    /// funnel, then publishes whether Go's complete cache key was reused.
    /// Cache hits and misses therefore share privilege checks, metadata locks,
    /// resource-group selection, statement context, and the DML executor.
    /// Runs a cached prepared DML for the definition PREPARE retained (see
    /// [`Self::execute_prepared_select_for`]).
    pub fn execute_cached_prepared_dml_for(
        &mut self,
        execution: &tidb_executor::PreparedDmlExecution,
        prepared: &crate::PreparedAst,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_cached_prepared_dml(execution, prepared.sql(), prepared.privilege_requests())
    }

    pub(crate) fn execute_cached_prepared_dml(
        &mut self,
        execution: &tidb_executor::PreparedDmlExecution,
        sql: &str,
        privilege_requests: &[crate::table_privilege::TablePrivilegeRequest],
    ) -> Result<StmtOutput, DriverError> {
        // go's stats meta bumps the target table's row count on every INSERT
        // (`stats_meta.count`), which `I_S.TABLES`' TABLE_ROWS reads back.
        let current_db = self.vars.current_db.clone();
        let mut stats_update: Option<(String, String, i64)> = None;
        let output = execution
            .with_plan(|statement, physical| {
                self.run_with_columns_using(sql, false, |session| {
                    session
                        .begin_prepared_statement_boundary(statement, Some(execution.parameters()));
                    for warning in execution.take_planning_warnings() {
                        session.append_sql_warning(warning);
                    }
                    let output = session.execute_parsed_statement_with_dml_plan(
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
                    Ok(output)
                })
                .map(|(output, _)| output)
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
    fn record_mdl_related_tables(&mut self, stmt: &Stmt, names: &[(String, String)]) {
        // Go's `IsAutoCommitTxn && StmtCtx.IsReadOnly` exemption does not
        // apply to locking reads. `SELECT ... FOR UPDATE/FOR SHARE` enters
        // the transaction's related-table map even when autocommit is on.
        let read_only = matches!(stmt, Stmt::Query(_)) && !query_has_lock(stmt);
        self.mdl_statement_locks = !(read_only && !self.in_transaction() && self.is_autocommit());
        if self.mdl_statement_locks {
            self.record_mdl_named_tables(names);
        }
    }

    /// [`Self::record_mdl_related_tables`] for a statement whose single table
    /// name the cached plan already carries. A cached point read is a
    /// SELECT: under autocommit it records nothing (the read-only rule
    /// above), and inside an explicit transaction it records exactly that
    /// one table.
    fn record_mdl_related_table_names(&mut self, names: &[(String, String)]) {
        if let Ok(mut locked) = self.mdl_statement_tables.lock() {
            locked.clear();
        }
        self.mdl_statement_locks = self.in_transaction();
        if self.mdl_statement_locks {
            self.record_mdl_named_tables(names);
        }
    }

    /// The preprocessor's half: each name the statement writes, as its own
    /// stored object.
    fn record_mdl_named_tables(&mut self, names: &[(String, String)]) {
        let Some(sink) = self.mdl_related_tables.clone() else {
            return;
        };
        let version = self.cluster_schema_version_now();
        let current_db = self.current_database().to_owned();
        let catalog = match self.lock_catalog() {
            Ok(catalog) => catalog,
            Err(_) => {
                sink.record_unresolved();
                return;
            }
        };
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
        struct StripAsOf {
            taken: Vec<tidb_ast::Expr>,
        }
        impl tidb_ast::Visitor for StripAsOf {
            fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                    if let Some(expr) = table_ref.as_of.take() {
                        self.taken.push(*expr);
                    }
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
        if visitor.taken.is_empty() {
            return Ok(None);
        }
        let mut resolved: Option<u64> = None;
        for expr in &visitor.taken {
            let ts = self.resolve_as_of_ts(expr)?;
            match resolved {
                None => resolved = Some(ts),
                Some(previous) if previous == ts => {}
                Some(_) => {
                    // Go refuses a statement naming two different instants.
                    return Err(DriverError::from(
                        tidb_error::plannererrors::ERR_AS_OF.generate_with_stack_by_args(&[
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
                tidb_error::plannererrors::ERR_AS_OF.generate_with_stack_by_args(&[
                    "can't use select as of while already set transaction as of".into(),
                ]),
            ));
        }
        let output = self.run_statement_as_of(ts, stripped);
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
    ) -> Result<PendingExecution, DriverError> {
        self.open_stale_transaction(ts)?;
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
        prepared: Option<&[crate::table_privilege::TablePrivilegeRequest]>,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_parsed_statement_with_optional_physical_plan(sql, stmt, prepared, None, None)
    }

    fn execute_parsed_statement_with_dml_plan(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: Option<&[crate::table_privilege::TablePrivilegeRequest]>,
        physical: &mut tidb_planner::physical::PhysicalPlan,
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
        prepared: Option<&[crate::table_privilege::TablePrivilegeRequest]>,
        select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
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

    /// Executes a parsed statement while retaining ordinary query chunks and
    /// session state until the returned record set closes.
    pub fn execute_record_set_parsed<'a>(
        &'a mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<crate::StatementExecution<'a>, DriverError> {
        // A node session streams a plain read outside a transaction over a
        // committed snapshot bound for it (Go's record set holds the
        // statement open until Close); anything that writes, locks, or must
        // see a transaction's own writes runs through the node connection.
        if self.node.is_some()
            && !(crate::classify::statement_kind_of(&stmt)
                == crate::classify::StatementKind::Select
                && !self.in_transaction()
                && self.statement_read_shape_bound(&stmt)
                    != tidb_executor::access_path::StatementReadShape::LockingRead)
        {
            return self.run_with_columns_parsed(stmt, sql).map(crate::StatementExecution::Complete);
        }
        Ok(self.open_record_set_parsed(stmt, sql)?.attach(self))
    }

    /// Opens a parsed statement for a front end that retains its own snapshot.
    pub fn open_record_set_parsed(
        &mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<crate::OpenedStatement, DriverError> {
        self.bind_node_snapshot_for_open_statement()?;
        self.begin_statement_execution(sql)?;
        self.begin_text_statement_boundary(&stmt);
        let result =
            self.prepare_parsed_statement_with_optional_physical_plan(sql, stmt, None, None, None);
        self.return_opened_record_set(result)
    }

    fn prepare_parsed_statement_with_optional_physical_plan(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: Option<&[crate::table_privilege::TablePrivilegeRequest]>,
        select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
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
        if !compile.nested && tidb_util::tracing::current_span().is_some() {
            tidb_util::tracing::begin_thread_region("executor.Compile", Some(sql.to_owned()));
        }
        let result =
            self.prepare_parsed_statement_compile_phase(sql, stmt, prepared, select_plan, dml_plan);
        if !compile.nested {
            tidb_executor::driver::end_held_optimize_region();
            if result.is_ok() {
                tidb_executor::driver::begin_run_stmt_region();
            } else {
                tidb_util::tracing::end_thread_region("executor.Compile");
            }
        }
        drop(compile);
        if result.is_err() && !self.in_transaction() {
            self.cleanup_txn_read_ts();
        }
        if result.is_ok() {
            crate::metrics::observe_compile_duration(
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
        if tidb_util::sem_v2::is_enabled()
            && tidb_util::sem_v2::is_restricted_sql(&sem_stmt_view(stmt))
            && !self.has_dynamic_privilege("RESTRICTED_SQL_ADMIN", false)
        {
            let statement = if stmt.text().is_empty() {
                sql.to_owned()
            } else {
                String::from_utf8_lossy(stmt.text()).into_owned()
            };
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_NOT_SUPPORTED_WITH_SEM
                    .generate_with_stack_by_args(&[statement.into()]),
            ));
        }
        Ok(())
    }

    fn prepare_parsed_statement_compile_phase(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        prepared: Option<&[crate::table_privilege::TablePrivilegeRequest]>,
        select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    ) -> Result<PendingExecution, DriverError> {
        // A statement locks nothing until its preprocessing decides it does
        // (`record_mdl_related_tables`).
        self.mdl_statement_locks = false;
        if let Ok(mut locked) = self.mdl_statement_tables.lock() {
            locked.clear();
        }
        self.check_sem_stmt(sql, &stmt)?;
        for warning in filter_sem_restricted_hints(&mut stmt) {
            self.append_warning(WarningLevel::Warning, 1105, warning);
        }
        // Go's `Preprocess` walks the AST once per statement and answers
        // every table-shaped question from that pass (`preprocess.go`); the
        // three consumers below share this one walk instead of each cloning
        // and re-walking the statement.
        // Go preprocesses the statement it compiles: an EXECUTE, not the
        // prepared statement it runs.
        let preprocess = (!CompileDepth::is_nested())
            .then(|| tidb_util::tracing::start_thread_region("planner.Preprocess"));
        let scan = crate::binding::scan_statement_tables(&mut stmt);
        drop(preprocess);
        // Go `preprocessor.Enter`'s `*ast.ColumnName` arm: the commit-TS
        // pseudo column is not yet usable by SELECT, UPDATE or DELETE.
        if scan.names_commit_ts {
            return Err(DriverError::Mysql(tidb_executor::MysqlError::from_terror(
                &tidb_error::plannererrors::ERR_INTERNAL.generate_with_stack(format!(
                    "Usage of column name '{}' is not supported for now",
                    tidb_planner::plan_builder::EXTRA_COMMIT_TS_NAME
                )),
            )));
        }
        if let Some(function) = &scan.qualified_function {
            return Err(DriverError::Mysql(tidb_executor::MysqlError::from_terror(
                &tidb_error::catalog::expression::ERR_FUNCTION_NOT_EXISTS
                    .generate_with_stack(format!("FUNCTION {function} does not exist")),
            )));
        }
        self.validate_statement_read_only_in_staleness(&stmt)?;
        if self.vars.snapshot_ts() != 0
            && self.in_transaction()
            && self.historical_read_ts() != Some(self.vars.snapshot_ts())
            && matches!(&stmt, Stmt::Query(_))
        {
            // Go baseTxnContextProvider changes the statement's schema/read TS,
            // not the active transaction or its write buffer.
            return self.run_snapshot_statement_in_transaction(self.vars.snapshot_ts(), stmt);
        }
        self.record_mdl_related_tables(&stmt, &scan.names);
        // A statement whose table references carry `AS OF TIMESTAMP` runs
        // against the store's history -- Go's stale statement
        // (`StalenessTxnContextProvider` for one statement). Intercepted at
        // this one funnel so text and prepared spellings share the rules.
        if !self.in_transaction()
            && matches!(&stmt, Stmt::Query(_))
            && !scan.has_as_of
            && (!scan.names.is_empty() || prepared.is_some() || self.vars.snapshot_ts() != 0)
        {
            if let Some(ts) = self.configured_historical_read_ts()? {
                return self.run_statement_as_of(ts, stmt);
            }
        }
        // Go `DDLExec.executeCreateView` preprocesses the view's SELECT as a
        // statement of its own, after the DDL's implicit commit, and refuses
        // a stale read there: a table read `AS OF`, or `tidb_read_staleness`.
        if let Stmt::Ddl(ddl) = &stmt {
            if let tidb_ast::DdlStmt::CreateView(create) = ddl.as_ref() {
                // The preprocessor walks the whole CREATE VIEW statement at
                // compile time, so a gated clause in the body is refused
                // before the executor ever looks at the view. It enters the
                // CREATE VIEW first, whose `checkCreateViewWithSelect` has
                // already dropped each body SELECT's lock.
                let mut body = (*create.query).clone();
                tidb_executor::view::check_create_view_with_select(&mut body)?;
                self.check_noop_functions(&body)?;
                self.refuse_stale_view_body(create, scan.has_as_of)?;
            }
        }
        if scan.has_as_of && self.in_transaction() {
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_AS_OF.generate_with_stack_by_args(&[
                    "as of timestamp can't be set in transaction.".into(),
                ]),
            ));
        }
        if scan.has_as_of && !self.in_transaction() {
            if let Some(output) = self.execute_as_of_statement(&stmt)? {
                return Ok(output);
            }
        }
        if matches!(&stmt, Stmt::Admin(admin) if matches!(admin.as_ref(),
            tidb_ast::AdminStmt::Explain(_) | tidb_ast::AdminStmt::ExplainFor(_)))
        {
            self.process_plan_info
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .explain_marker = true;
        }
        // The pessimistic lock step and its retry are the store session's
        // statement loop (Go `handlePessimisticDML`, `StatementLifecycle`).
        let execution =
            self.execute_parsed_statement_inner(sql, stmt, prepared, select_plan, dml_plan);
        let execution = execution?;
        // Go runs a statement without a result set through
        // `handleNoDelayExecutor`, whose `TryNewCacheChunk` draws from the
        // session's reusable allocator and so marks `useChunkAlloc` -- the
        // value `@@last_sql_use_alloc` reports for the next statement.
        if matches!(
            execution,
            PendingExecution::Complete(StmtOutput::Affected(_) | StmtOutput::Done(_))
        ) {
            self.vars.use_chunk_alloc = self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_REUSECHUNK)
                .is_ok_and(|value| value.eq_ignore_ascii_case("ON") || value == "1");
        }
        Ok(execution)
    }

    /// Runs Go's `matchAgainstToLike` before physical planning. Prepared
    /// cache misses and ordinary statements both call this method, so the
    /// cache retains the same rewritten tree the normal executor receives.
    pub(crate) fn rewrite_fts_for_planning(&self, stmt: &mut Stmt) -> Result<(), DriverError> {
        let enabled = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS)
            .is_ok_and(|value| value.eq_ignore_ascii_case("on") || value == "1");
        if !enabled {
            return Ok(());
        }

        let current_db = self.vars.current_db.clone();
        // The context takes its catalog-backed snapshots, so construct it
        // before holding the catalog guard used by every resolved-type probe.
        let ctx = self.statement_context(false);
        let Ok(catalog) = self.lock_catalog() else {
            return Ok(());
        };
        let columns_are_strings = |select: &tidb_ast::SelectStmt, columns: &[Vec<String>]| {
            tidb_executor::fts_columns_are_strings(select, columns, &catalog, &current_db, &ctx)
        };
        struct FtsRewriter<'a> {
            columns_are_strings: &'a tidb_executor::fts_like_rewrite::ColumnsAreStrings<'a>,
            error: Option<DriverError>,
        }
        impl tidb_ast::Visitor for FtsRewriter<'_> {
            fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                if self.error.is_some() {
                    return false;
                }
                if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
                    if let Err(error) = tidb_executor::fts_like_rewrite::rewrite_select_fts(
                        select,
                        self.columns_are_strings,
                    ) {
                        self.error = Some(error);
                    }
                }
                false
            }

            fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
                true
            }
        }
        use tidb_ast::Visitable as _;
        let mut rewriter = FtsRewriter {
            columns_are_strings: &columns_are_strings,
            error: None,
        };
        stmt.accept(&mut rewriter);
        rewriter.error.map_or(Ok(()), Err)
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
        prepared: Option<&[crate::table_privilege::TablePrivilegeRequest]>,
        mut select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    ) -> Result<PendingExecution, DriverError> {
        self.set_statement_arbitration_key(sql);
        // Go `ResetContextOfStmt` sets the `In*Stmt` bits before any
        // statement-specific door -- an EXPLAIN by its target -- so a
        // statement that FAILS still classifies itself for the next
        // statement's `ROW_COUNT()` (captured: a failed SELECT leaves -1, a
        // failed INSERT leaves 0).
        self.statement_kind = statement_kind_of(&stmt);
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
        )))
        {
            self.apply_set_var_hints(&stmt)?;
        }
        // Go `Compile` for every statement that is not a query or DML: the
        // preprocessor, then `PlanBuilder.Build`'s visit infos, checked while
        // the statement is still side-effect free.
        let compiled = self.compile_stmt(&stmt, prepared)?;
        self.statement_preamble(&stmt)?;
        let executed = match &compiled {
            Some(plan) => self.execute_plan(plan, &stmt)?,
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
                    self.prepare_statement(name, source)?;
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
        self.rewrite_fts_for_planning(&mut stmt)?;
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
        // Go's preprocessor runs before planning, so a gated clause is
        // refused before any table is touched.
        if let Stmt::Query(query) = &stmt {
            self.check_noop_functions(query)?;
        }
        // Go's `CheckPrivilege` runs on the `visitInfo` the planner
        // collected, so it too refuses before any table is touched. This is
        // the single seam for it, because every statement whose privileges
        // are table-scoped reaches here: the account statements
        // `apply_schema_stmt` answers earlier demand their own, statement-
        // specific privileges instead. A prepared EXECUTE checks the
        // requests derived once for its text (Go `checkPreparedPriv` on the
        // stored `VisitInfos`).
        if matches!(&stmt, Stmt::Dml(dml) if matches!(dml.as_ref(),
            DmlStmt::Update(update) if matches!(&update.kind, tidb_ast::UpdateKind::Multi { .. })))
        {
            // Joined DML is rebuilt on every execution, including PREPARE.
            // Resolve its visits against that same current schema: DDL can
            // move an unqualified column to another table after PREPARE.
            self.require_statement_table_privileges(&stmt)?;
        } else if compiled.is_some() {
            // `compile_stmt` already ran Go's `CheckPrivilege` for it.
        } else if let Some(requests) = prepared {
            self.check_table_privilege_requests(requests)?;
        } else {
            self.require_statement_table_privileges(&stmt)?;
        }
        // Go raises ErrNoDB where an unqualified NAME is resolved, not for
        // every statement: `SELECT 1` and `SELECT DATABASE()` both run with
        // no database selected (captured). The driver's own
        // `split_table_path` raises it at the resolution point, which is
        // where Go's does.
        let output = match &stmt {
            Stmt::Query(query) => {
                let current_db = self.vars.current_db.clone();
                let ctx = self.statement_context_for_stmt(&stmt, false);
                // go `SelectIntoOutfile` opens the destination with O_EXCL:
                // an existing file is refused with the raw open error (the
                // oracle answers `open <path>: file exists`). This tier does
                // not write data files, so the refusal is the only observable
                // part of the clause that can be honored faithfully.
                if let tidb_ast::QueryStmt::Select(select) = query.as_ref() {
                    if let Some(outfile) = &select.into_outfile {
                        if std::path::Path::new(&outfile.file_name).exists() {
                            return Err(DriverError::Unsupported(
                                format!("open {}: file exists", outfile.file_name).into(),
                            ));
                        }
                    }
                }
                let tidb_ast::QueryStmt::Select(_) = &**query else {
                    let record_set = self.with_catalog_retrieving(&ctx, |catalog| {
                        let physical = select_plan.as_mut().and_then(|retained| {
                            (retained.schema_version == catalog.schema_version()).then(|| {
                                *retained.used = true;
                                &mut *retained.physical
                            })
                        });
                        tidb_executor::driver::open_query_meta_stmt_with_physical(
                            query,
                            physical,
                            catalog,
                            &current_db,
                            &ctx,
                        )
                    })?;
                    return Ok(PendingExecution::Query(PendingQuery::new(record_set, ctx)));
                };
                if let Some(parameterized) = non_prepared.as_ref() {
                    let mut effective_parameterized = parameterized.statement.clone();
                    if binding_sql.is_some() {
                        let binding_hints = crate::binding::collect_hints(&stmt);
                        crate::binding::bind_hints(&mut effective_parameterized, &binding_hints);
                    }
                    if let Some(execution) = self.bind_non_prepared_select(
                        parameterized,
                        &effective_parameterized,
                        binding_sql.as_deref(),
                    ) {
                        let cache_hit = execution.cache_hit();
                        let schema_version = execution.schema_version();
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
                            return Ok(PendingExecution::Query(PendingQuery::new(record_set, ctx)));
                        }
                    }
                }
                let record_set = self.with_catalog_retrieving(&ctx, |catalog| {
                    let physical = select_plan.as_mut().and_then(|retained| {
                        (retained.schema_version == catalog.schema_version()).then(|| {
                            *retained.used = true;
                            &mut *retained.physical
                        })
                    });
                    tidb_executor::driver::open_query_meta_stmt_with_physical(
                        query,
                        physical,
                        catalog,
                        &current_db,
                        &ctx,
                    )
                })?;
                return Ok(PendingExecution::Query(PendingQuery::new(record_set, ctx)));
            }
            Stmt::Dml(dml) => {
                // Go parameterizes a non-prepared DML statement too (the
                // `tidb_enable_non_prepared_plan_cache_for_dml` switch, true
                // by default) and sends the retained marker-bearing statement
                // through the same plan cache as PREPARE.
                let non_prepared_dml = prepared
                    .is_none()
                    .then(|| self.parameterize_non_prepared_dml(&stmt))
                    .flatten();
                if let Some(parameterized) = non_prepared_dml.as_ref() {
                    let mut effective_parameterized = parameterized.statement.clone();
                    if binding_sql.is_some() {
                        let binding_hints = crate::binding::collect_hints(&stmt);
                        crate::binding::bind_hints(&mut effective_parameterized, &binding_hints);
                    }
                    if let Some(execution) = self.bind_non_prepared_dml(
                        &parameterized,
                        &effective_parameterized,
                        binding_sql.as_deref(),
                    ) {
                        // A non-prepared statement's plan-cache hit carries
                        // the requests its own walk derived, as Go's
                        // non-prepared cache path hands `checkPreparedPriv`
                        // the statement's `VisitInfos`.
                        let requests = self.collect_table_privileges(&stmt, false)?;
                        return self
                            .execute_cached_prepared_dml(&execution, sql, &requests)
                            .map(PendingExecution::Complete);
                    }
                }
                match &**dml {
                    DmlStmt::Insert(insert) => {
                        let mut physical_plan = cached_dml_plan(dml_plan, "Insert")?;
                        let current_db = self.vars.current_db.clone();
                        let enable_strict_not_null_check = !matches!(
                            self.vars
                                .get_system(
                                    tidb_vardef::tidb_vars::TIDB_ENABLE_STRICT_NOT_NULL_CHECK
                                )
                                .as_deref(),
                            Ok("OFF" | "off" | "0")
                        );
                        // Go `ResetContextOfStmt`'s `*ast.InsertStmt` arm. The class
                        // is what `StmtContext::push_down_flags` turns into the
                        // statement-kind bit of any coprocessor request this
                        // statement's read half issues, and `IgnoreErr` is the
                        // `IGNORE` modifier Go reads off this same AST to downgrade
                        // every value-level error to a warning.
                        let ctx = self
                            .statement_context_ignoring(true, insert.ignore)
                            // Go's `*ast.InsertStmt` arm sets
                            // `sc.Priority = stmt.Priority`.
                            .with_statement_priority(statement_priority_of(&stmt))
                            .with_statement_class(tidb_executor::StatementClass::Insert)
                            .with_single_insert_bad_null_policy(
                                insert.rows.len() == 1,
                                enable_strict_not_null_check,
                            );
                        if insert.source.is_some() {
                            self.write_sli.set_invalid();
                        }
                        let result = self.retrieving_memory_tables(&ctx, |session| {
                            session.with_staged_catalog_for_path(
                                &insert.table,
                                &current_db,
                                |catalog| {
                                    // Go executes the statement the protocol BOUND
                                    // (`pkg/server`'s `statement`/`executableParams`
                                    // carry the values): re-parsing `sql` here would
                                    // run a tree whose markers never met their
                                    // execute-time values, which is a wrong answer
                                    // for every binary-protocol write.
                                    tidb_executor::run_insert_stmt_with_physical(
                                        insert,
                                        catalog,
                                        &current_db,
                                        &ctx,
                                        physical_plan.as_deref_mut(),
                                    )
                                },
                            )
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
                        let mut physical_plan = cached_dml_plan(dml_plan, "Update")?;
                        let current_db = self.vars.current_db.clone();
                        // Go `ResetUpdateStmtCtx`, which applies the same
                        // `!strictSQLMode || stmt.IgnoreErr` rule the INSERT arm
                        // does; the class is what `StmtContext::push_down_flags`
                        // turns into the statement-kind bit of any coprocessor
                        // request this statement's read half issues.
                        let ctx = self
                            .statement_context_for_update_read(update.ignore)
                            // Go `ResetUpdateStmtCtx` sets
                            // `sc.Priority = stmt.Priority`.
                            .with_statement_priority(statement_priority_of(&stmt))
                            .with_statement_class(tidb_executor::StatementClass::Update);
                        let output = match &update.kind {
                            tidb_ast::UpdateKind::Single(table_ref) => self
                                .retrieving_memory_tables(&ctx, |session| {
                                    session.with_staged_catalog_for_path(
                                        &table_ref.name,
                                        &current_db,
                                        |catalog| {
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
                                        },
                                    )
                                }),
                            tidb_ast::UpdateKind::Multi { .. } => self
                                .retrieving_memory_tables(&ctx, |session| {
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
                                }),
                        };
                        self.drain_eval_warnings(&ctx);
                        self.statement_message = ctx.message();
                        output
                    }
                    DmlStmt::Delete(delete) => {
                        let mut physical_plan = cached_dml_plan(dml_plan, "Delete")?;
                        let current_db = self.vars.current_db.clone();
                        // Go `ResetDeleteStmtCtx`, which applies the same
                        // `!strictSQLMode || stmt.IgnoreErr` rule the INSERT arm
                        // does; the class is what `StmtContext::push_down_flags`
                        // turns into the statement-kind bit of any coprocessor
                        // request this statement's read half issues.
                        let ctx = self
                            .statement_context_for_update_read(delete.ignore)
                            // Go `ResetDeleteStmtCtx` sets
                            // `sc.Priority = stmt.Priority`.
                            .with_statement_priority(statement_priority_of(&stmt))
                            .with_statement_class(tidb_executor::StatementClass::Delete);
                        let output = match &delete.kind {
                            tidb_ast::DeleteKind::Single(table_ref) => self
                                .retrieving_memory_tables(&ctx, |session| {
                                    session.with_staged_catalog_for_path(
                                        &table_ref.name,
                                        &current_db,
                                        |catalog| {
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
                                        },
                                    )
                                }),
                            tidb_ast::DeleteKind::Multi { .. } => self
                                .retrieving_memory_tables(&ctx, |session| {
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
                                }),
                        };
                        self.drain_eval_warnings(&ctx);
                        output
                    }
                    DmlStmt::ImportInto(import) => {
                        // Go validates the FORMAT and WITH options before it
                        // reads anything; the import itself is not modelled.
                        tidb_executor::import_into_options::validate_import_into_options(import)
                            .and_then(|()| {
                                Err(DriverError::unsupported("IMPORT INTO is not supported yet"))
                            })
                    }
                    DmlStmt::Call(_) => {
                        // go's unsupported-statement wall names the AST node:
                        // `executor/executor.go`'s `Unsupported type
                        // *ast.CallStmt` (8108), not a generic 1105.
                        Err(DriverError::from(
                            (&tidb_error::plannererrors::ERR_UNSUPPORTED_TYPE)
                                .generate_with_stack("Unsupported type *ast.CallStmt".to_owned()),
                        ))
                    }
                    DmlStmt::LoadData(load) => self.run_load_data_stmt(load, &stmt),
                    DmlStmt::Batch(batch) => self.run_non_transactional_dml(batch),
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

pub(crate) fn memory_usage_table_rows() -> Vec<Vec<tidb_datatype::Datum>> {
    use std::sync::atomic::Ordering;

    use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};

    let stats = tidb_util::memory::read_mem_stats();
    let current_ops = tidb_util::servermemorylimit::IS_KILLING
        .load(Ordering::SeqCst)
        .then(|| Datum::new_string("shrink"))
        .unwrap_or(Datum::Null);
    let session_kill_last = tidb_util::servermemorylimit::SESSION_KILL_LAST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .map(|value| {
            Datum::new_time(
                Time::new(core_time_from_datetime(value), TimeType::DateTime, 0)
                    .expect("fsp 0 is valid"),
            )
        })
        .unwrap_or(Datum::Null);
    let zero_datetime = Datum::new_time(
        Time::new(CoreTime::default(), TimeType::DateTime, 0).expect("fsp 0 is valid"),
    );
    vec![vec![
        Datum::new_int(
            tidb_util::memory::mem_total()
                .ok()
                .and_then(|value| i64::try_from(value).ok())
                .unwrap_or(0),
        ),
        Datum::new_int(
            i64::try_from(tidb_util::memory::SERVER_MEMORY_LIMIT.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        Datum::new_int(stats.heap_inuse),
        Datum::new_int(
            i64::try_from(tidb_util::servermemorylimit::MEMORY_MAX_USED.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        current_ops,
        session_kill_last,
        Datum::new_int(tidb_util::servermemorylimit::SESSION_KILL_TOTAL.load(Ordering::SeqCst)),
        zero_datetime,
        Datum::new_int(0),
        Datum::new_int(0),
        Datum::new_int(0),
    ]]
}

impl Session {
    /// Go `validateStatementReadOnlyInStaleness` (`session.go:2822`), which
    /// `ExecuteStmt` runs before `Compile`. It switches on the statement node
    /// itself: a top-level SELECT's lock, the read-only question for the
    /// statements it lists, and any DML; a lock inside a derived table or a
    /// view body, and every DDL, pass it.
    pub fn validate_statement_read_only_in_staleness(&self, stmt: &Stmt) -> Result<(), DriverError> {
        if !self.vars.restricted_sql
            && (self.historical_read_ts().is_some()
                || self.vars.txn_read_ts() != 0
                || self.external_timestamp_read_enabled())
        {
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

    /// Go `ExecStmt`'s history-read refusal (`adapter.go:1067,1152`): a write
    /// under `tidb_snapshot`, once the statement has compiled.
    pub fn validate_snapshot_write(&self, stmt: &Stmt) -> Result<(), DriverError> {
        let checked = matches!(stmt, Stmt::Dml(_) | Stmt::Ddl(_) | Stmt::Query(_))
            || matches!(stmt, Stmt::Admin(admin) if matches!(admin.as_ref(), tidb_ast::AdminStmt::Explain(
                _,
            ) | tidb_ast::AdminStmt::Trace(_)));
        if self.vars.snapshot_ts() != 0 && checked && !stmt.is_read_only(true) {
            return Err(DriverError::unsupported(
                "can not execute write statement when 'tidb_snapshot' is set",
            ));
        }
        Ok(())
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
        // This store has no published SafeTS; Go's zero SafeTS selects the lower bound.
        let now = self.eval_value(&tidb_ast::Expr::Func {
            name: "NOW".into(),
            args: vec![tidb_ast::Expr::Int("6".into())],
            origin_position: 0,
        })?;
        let ts = self.resolve_as_of_ts(&tidb_ast::Expr::String(
            now.sql_string().unwrap_or_default(),
        ))?;
        Ok(Some(
            ((ts >> 18).saturating_sub(seconds.unsigned_abs().saturating_mul(1000))) << 18,
        ))
    }

    fn refuse_pinned_historical_read(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        if self.historical_read_provider.is_some() {
            self.validate_statement_read_only_in_staleness(stmt)?;
            return self.validate_snapshot_write(stmt);
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

/// The rows of a statement-summary table for `user` (`None`: every user,
/// as Go's restricted SQL reads them), over the v1 map or the persistent v2
/// owner the instance configuration selects.
pub(crate) fn statement_summary_rows(
    table_name: &str,
    columns: &[(String, tidb_datatype::FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    user: Option<tidb_parser::auth::UserIdentity>,
    has_process: bool,
    digests: Option<&std::collections::HashSet<String>>,
    scan_concurrency: usize,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    use tidb_ast::CiString;
    use tidb_model::ColumnInfo;
    use tidb_stmtsummary::reader::{StmtSummaryChecker, StmtSummaryReader};

    if table_name.go_lower_eq("STATEMENTS_SUMMARY_EVICTED") {
        if !has_process {
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["PROCESS".into()]),
            ));
        }
        let full: Vec<Vec<tidb_datatype::Datum>> = if tidb_config::config_tree::config::get_global_config()
            .instance
            .stmt_summary_enable_persistent
        {
            tidb_stmtsummary::v2::stmtsummary::global_stmt_summary()
                .map(|summary| summary.evicted())
                .filter(|row| !row.is_empty())
                .into_iter()
                .collect()
        } else {
            tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.to_evicted_count_datum()
        };
        // The caller asks for `columns` (a pruned scan reads only some of the
        // table's); the evicted row is produced at full width.
        let schema = tidb_executor::infoschema_reader::table_schema("STATEMENTS_SUMMARY_EVICTED")
            .unwrap_or_default();
        let positions: Vec<usize> = columns
            .iter()
            .filter_map(|(name, _)| schema.iter().position(|(have, _)| have.eq_ignore_ascii_case(name)))
            .collect();
        if positions.len() != columns.len() || positions.len() == schema.len() {
            return Ok(full);
        }
        return Ok(full
            .into_iter()
            .map(|row| positions.iter().map(|position| row[*position].clone()).collect())
            .collect());
    }

    let columns: Vec<ColumnInfo> = columns
        .iter()
        .enumerate()
        .map(|(offset, (name, _))| ColumnInfo {
            id: i64::try_from(offset).expect("information-schema column count fits i64"),
            name: CiString::new(name),
            offset: i64::try_from(offset).expect("information-schema column count fits i64"),
            ..ColumnInfo::default()
        })
        .collect();
    let persistent = tidb_config::config_tree::config::get_global_config()
        .instance
        .stmt_summary_enable_persistent;
    let cumulative = table_name.go_lower_eq("TIDB_STATEMENTS_STATS");
    let history = table_name.go_lower_eq("STATEMENTS_SUMMARY_HISTORY");
    if persistent {
        // Go refuses cumulative retrieval in v2; never silently substitute
        // an unrelated v1 map for the selected persistent owner.
        if cumulative {
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_NOT_SUPPORTED_YET.generate_with_stack_by_args(&[
                    "cumulative statement summary table with persistent mode (v2)".into(),
                ]),
            ));
        }
        let mut rows = tidb_stmtsummary::v2::reader::new_mem_reader(
            tidb_stmtsummary::v2::stmtsummary::global_stmt_summary(),
            &columns,
            String::new(),
            zone.clone(),
            user.clone(),
            has_process,
            digests.cloned(),
            Vec::new(),
        )
        .rows();
        if history {
            let mut reader = tidb_stmtsummary::v2::reader::HistoryReader::new(
                None,
                &columns,
                String::new(),
                zone.clone(),
                user,
                has_process,
                digests.cloned(),
                Vec::new(),
                scan_concurrency,
            )
            .map_err(|error| DriverError::Unsupported(error.into()))?;
            let result = (|| {
                while let Some(batch) = reader
                    .rows()
                    .map_err(|error| DriverError::Unsupported(error.into()))?
                {
                    rows.extend(batch);
                }
                Ok(rows)
            })();
            let _ = reader.close();
            return result;
        }
        return Ok(rows);
    }
    let mut reader =
        StmtSummaryReader::new(user, has_process, columns, String::new(), zone.clone());
    reader.set_checker(digests.cloned().map(StmtSummaryChecker::new));
    Ok(if cumulative {
        reader.get_stmt_summary_cumulative_rows()
    } else if history {
        reader.get_stmt_summary_history_rows()
    } else {
        reader.get_stmt_summary_current_rows()
    })
}
