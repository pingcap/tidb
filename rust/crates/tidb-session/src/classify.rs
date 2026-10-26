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

//! What a statement is, decided without running it: the shape of its answer,
//! the persistent state outside this process it would change, and the class
//! `ROW_COUNT()` distinguishes.
//!
//! None of these run the statement. A front end has to know the answer's shape
//! before executing (running twice would duplicate a write), and a front end
//! whose catalog or account table is a READ of somebody else's stored state has
//! to know that before answering OK to a change that went nowhere.
//!
//! The parse decides all three, with one exception this file owns: `EXECUTE p`
//! answers with whatever `p` answers, so the answer shape comes from the
//! statement this session prepared under that name. It is still decided before
//! execution -- it is just not decided by the `EXECUTE`'s own tree.
//!
//! These are separate questions and stay separate. The answer shape and the
//! stored state a statement changes are independent: `GRANT` is `Write`-shaped
//! and rewrites stored accounts, `INSERT` is `Write`-shaped and changes nothing
//! stored outside this process.

use tidb_ast::{DmlStmt, QueryStmt, StatementPriority, Stmt};
use tidb_datatype::Datum;
use tidb_executor::access_path::StatementReadShape;
use tidb_executor::DriverError;

use crate::Session;

/// What kind of answer a statement produces, decided by parsing alone.
///
/// The MySQL text protocol answers a query with a result set and a write or
/// DDL with an OK packet, so a server front end must know which shape a
/// statement takes *before* running it (running it twice would duplicate the
/// write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtKind {
    /// A query: answers with rows.
    Query,
    /// A DML write or DDL: answers with an affected-row count.
    Write,
}

/// Which piece of somebody else's persistent state a statement would change.
///
/// See [`Session::statement_stored_state_change`] for why the schema half and
/// the account half are named apart rather than lumped into one boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredStateChange {
    /// Nothing persistent outside this process.
    None,
    /// The stored schema: every `ast.DDLNode`.
    Schema,
    /// The stored accounts: `mysql.user`, `mysql.db` and the role edges.
    Accounts,
    /// The stored `SET GLOBAL` overrides: `mysql.global_variables`.
    GlobalVars,
    /// The stored statistics: `mysql.stats_meta` and its histogram tables.
    /// `ANALYZE TABLE` and `LOAD STATS` write them.
    Statistics,
    /// The persisted statistics-lock state and the deltas unlocked into
    /// `mysql.stats_meta`.
    StatsLock,
    /// The cluster's BDR role, Go's meta key `BDRRole`: `ADMIN SET` and
    /// `UNSET BDR ROLE` write it and `ADMIN SHOW BDR ROLE` reads it from
    /// the store rather than from any node's schema.
    BdrRole,
}

/// Whether a `SET` statement carries at least one GLOBAL-scoped assignment.
///
/// A statement can mix `SESSION` and `GLOBAL` assignments in one `SET`
/// (Go allows this), so the whole statement is routed to the GLOBAL path the
/// moment any assignment is GLOBAL-scoped; the session-scoped assignments in
/// the same statement still run, against the same driver, once routed.
fn has_global_assignment(session: &tidb_ast::SessionStmt) -> bool {
    match session {
        tidb_ast::SessionStmt::Set(set) => set.variables.iter().any(|variable| variable.is_global),
        _ => false,
    }
}

/// The statement classes `ROW_COUNT()` distinguishes.
///
/// Go spells this as four independent `StmtCtx` bits (`InSelectStmt`,
/// `InInsertStmt`, `InUpdateStmt`, `InDeleteStmt`) and reads them in one
/// if/else chain in `ResetContextOfStmt` (`pkg/executor/select.go:1229-1237`);
/// one enum says the same thing without letting two of them be true at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatementKind {
    /// A SELECT or set operation, after which `ROW_COUNT()` is `-1`.
    Select,
    /// INSERT/REPLACE/UPDATE/DELETE, after which `ROW_COUNT()` is the
    /// affected-row count.
    Dml,
    /// Everything else -- DDL, SHOW, SET, transaction control -- after which
    /// `ROW_COUNT()` is `0`.
    Other,
}

/// Classifies a parsed statement for `ROW_COUNT()` -- a `WITH` prefix belongs
/// to the mutation, so `WITH x AS (...) DELETE ...` still sets
/// `InDeleteStmt` -- unwrapping an EXPLAIN,
/// which `ResetContextOfStmt` replaces by its target before setting the bits
/// (`EXPLAIN SELECT` leaves `ROW_COUNT()` at -1).
pub(crate) fn statement_kind_of(stmt: &Stmt) -> StatementKind {
    fn dml_kind(dml: &DmlStmt) -> StatementKind {
        match dml {
            DmlStmt::Insert(_) | DmlStmt::Update(_) | DmlStmt::Delete(_) => StatementKind::Dml,
            _ => StatementKind::Other,
        }
    }
    match stmt {
        Stmt::Query(_) => StatementKind::Select,
        Stmt::Dml(dml) => dml_kind(dml),
        Stmt::Admin(admin) => match &**admin {
            tidb_ast::AdminStmt::Explain(explain) => explain
                .statement()
                .map_or(StatementKind::Other, statement_kind_of),
            _ => StatementKind::Other,
        },
        _ => StatementKind::Other,
    }
}

/// Go `ResetContextOfStmt`'s `sc.Priority` assignment: the statement's own
/// `LOW_PRIORITY`/`HIGH_PRIORITY`/`DELAYED` modifier. SELECT reads it from
/// `SelectStmtOpts`, UPDATE/DELETE from `ResetUpdateStmtCtx`/`ResetDeleteStmtCtx`,
/// and INSERT from its own arm; every other statement leaves the zero value.
/// A `WITH` prefix is unwrapped because Go sets the priority on the mutation
/// the CTE belongs to.
pub(crate) fn statement_priority_of(stmt: &Stmt) -> StatementPriority {
    fn dml_priority(dml: &DmlStmt) -> StatementPriority {
        match dml {
            DmlStmt::Insert(insert) => insert.priority,
            DmlStmt::Update(update) => update.priority,
            DmlStmt::Delete(delete) => delete.priority,
            // Go's `*ast.LoadDataStmt` arm has a dedicated `LowPriority` word
            // rather than the shared `stmt.Priority` field.
            DmlStmt::LoadData(load) if load.low_priority => StatementPriority::Low,
            _ => StatementPriority::None,
        }
    }
    match stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select.priority,
            QueryStmt::SetOpr(_) => StatementPriority::None,
        },
        Stmt::Dml(dml) => dml_priority(dml),
        _ => StatementPriority::None,
    }
}

/// Go `ResetContextOfStmt`'s `sc.NotFillCache = !SelectStmtOpts.SQLCache`.
/// Only a SELECT carries the modifier; every other statement leaves it false.
pub(crate) fn statement_not_fill_cache(stmt: &Stmt) -> bool {
    matches!(
        stmt,
        Stmt::Query(query) if matches!(&**query, QueryStmt::Select(select) if select.sql_no_cache)
    )
}

/// Whether the plan Go builds for this statement has a non-empty schema, so
/// that the connection answers with a result set rather than an OK packet
/// (`ExecStmt.Exec` returns no record set for an empty schema).
///
/// The `Admin` statement bucket holds every statement Go plans through
/// `buildAdmin`, `buildSimple`, `buildShow` and their neighbours; each arm
/// names the Go builder that decides its schema. The match is exhaustive so a
/// new variant has to choose.
fn admin_plan_has_result_schema(admin: &tidb_ast::AdminStmt) -> bool {
    use tidb_ast::AdminStmt;
    match admin {
        // `buildSimple` with no schema: account, privilege, process and
        // server-control statements, extended statistics DDL, and the
        // job-cancelling statements.
        AdminStmt::Grant(_)
        | AdminStmt::GrantProxy(_)
        | AdminStmt::GrantRole(_)
        | AdminStmt::Revoke(_)
        | AdminStmt::RevokeRole(_)
        | AdminStmt::Binlog(_)
        | AdminStmt::Kill(_)
        | AdminStmt::Flush(_)
        | AdminStmt::CreateStatistics(_)
        | AdminStmt::DropStatistics(_)
        | AdminStmt::CancelDistributionJob(_)
        | AdminStmt::DropQueryWatch(_)
        | AdminStmt::CancelImportJob(_)
        | AdminStmt::DropStats(_) => false,
        // `buildSimple` sets a schema for these three.
        AdminStmt::Brie(_) | AdminStmt::CalibrateResource(_) | AdminStmt::AddQueryWatch(_) => true,
        // `ShutdownStmt` is `Simple`; `HELP` reports the topics it finds.
        AdminStmt::ServerControl(control) => {
            matches!(control.as_ref(), tidb_ast::ServerControlStmt::Help(_))
        }
        // `buildSetConfig` -> `SetConfig`; `buildDo` -> a no-delay
        // projection over an empty schema.
        AdminStmt::SetConfig(_) | AdminStmt::Do(_) => false,
        // `buildRecommendIndex`: `run` and `show` report rows, `set` does
        // not; every other action is refused before execution.
        AdminStmt::RecommendIndex(recommend) => {
            !matches!(recommend.as_ref(), tidb_ast::RecommendIndexStmt::Set(_))
        }
        // `buildAdmin`: `Simple`, `AlterDDLJob`, `AdminPlugins`,
        // `SQLBindPlan`, the blacklist reloads, `WorkloadRepoCreate`, and
        // `CheckTable`, which appends nothing on success.
        AdminStmt::FlushPlanCache(_)
        | AdminStmt::Reload(_)
        | AdminStmt::SetBdrRole(_)
        | AdminStmt::UnsetBdrRole
        | AdminStmt::AlterDdlJobs(_)
        | AdminStmt::Plugins { .. }
        | AdminStmt::BindingControl(_)
        | AdminStmt::CreateWorkloadSnapshot => false,
        AdminStmt::AdminCheck(check) => match check.as_ref() {
            tidb_ast::AdminCheckStmt::Table { .. } => false,
            // `AdminCheckIndexRange` reports the rows it checked.
            tidb_ast::AdminCheckStmt::Index { handle_ranges, .. } => !handle_ranges.is_empty(),
        },
        // `buildAdmin` with a schema.
        AdminStmt::ShowBdrRole
        | AdminStmt::ShowSlow(_)
        | AdminStmt::ShowDdl
        | AdminStmt::ShowDdlJobs(_)
        | AdminStmt::ShowDdlJobQueries(_)
        | AdminStmt::DdlJobControl(_)
        | AdminStmt::ShowNextRowId(_)
        | AdminStmt::AdminChecksum(_)
        | AdminStmt::AdminRecoverIndex(_)
        | AdminStmt::AdminCleanupIndex(_) => true,
        // `ast.CleanupTableLockStmt` is `Simple`.
        AdminStmt::CleanupTableLock(_) => false,
        // `buildShow`, `buildExplain`, `buildExplainFor`, `buildTrace`,
        // `buildPlanReplayer` and `buildSplitRegion` all set a schema.
        AdminStmt::Show(_)
        | AdminStmt::Trace(_)
        | AdminStmt::ExplainFor(_)
        | AdminStmt::Explain(_)
        | AdminStmt::PlanReplayer(_)
        | AdminStmt::SplitRegion(_) => true,
        // `buildCreateBindPlan`/`buildDropBindPlan`/
        // `buildSetBindingStatusPlan` -> `SQLBindPlan`; `buildAnalyze`,
        // `buildRefreshStats`, `buildLockStats`/`buildUnlockStats` and
        // `buildLoadStats` set none.
        AdminStmt::CreateBinding(_)
        | AdminStmt::DropBinding(_)
        | AdminStmt::SetBinding(_)
        | AdminStmt::AnalyzeTable(_)
        | AdminStmt::AnalyzeIncremental(_)
        | AdminStmt::RefreshStats(_)
        | AdminStmt::LockStats(_)
        | AdminStmt::UnlockStats(_)
        | AdminStmt::LoadStats(_) => false,
        // `buildTraffic`: only `SHOW TRAFFIC JOBS` sets a schema.
        AdminStmt::Traffic(traffic) => {
            matches!(traffic.as_ref(), tidb_ast::TrafficStmt::ShowJobs)
        }
    }
}

impl Session {
    /// What this statement's WHOLE read is, decided by parsing alone, so a
    /// front end that binds a read snapshot can tell it before the statement
    /// runs -- and therefore before its first read spends a timestamp.
    ///
    /// This is the fourth question of this file's set, and it stays separate
    /// from the other three for the same reason they stay separate from each
    /// other: a `SELECT` is `Query`-shaped whether or not it is a point get,
    /// and a point get inside an `UPDATE` is `Write`-shaped while reading the
    /// very same row by the very same key. Only
    /// [`tidb_executor::access_path::statement_read_shape`] answers this one,
    /// and it answers it from the statement's own tree.
    ///
    /// `params` are a prepared statement's execute-time values; they are bound
    /// into the text first, because a `?` is not a value a point get can be
    /// planned from. Empty for the text protocol. A statement that fails to
    /// parse or bind declares nothing, which costs a timestamp and never a
    /// row.
    #[must_use]
    pub fn statement_read_shape(&self, sql: &str, params: &[Datum]) -> StatementReadShape {
        // One parse, in the client's encodings, with the values installed on
        // its markers -- not a bound text re-lexed in another encoding.
        let Ok(stmt) = self.parse(sql) else {
            return StatementReadShape::Unknown;
        };
        let stmt = if params.is_empty() {
            stmt
        } else {
            match tidb_executor::bind_statement(stmt, params) {
                Ok(stmt) => stmt,
                Err(_) => return StatementReadShape::Unknown,
            }
        };
        self.statement_read_shape_bound(&stmt)
    }

    /// Classifies an already-bound prepared statement without replacing its
    /// markers again. This is the fast path used by the cluster-session wire
    /// executor, which binds once and shares the resulting tree with planning.
    #[must_use]
    pub fn statement_read_shape_bound(&self, stmt: &Stmt) -> StatementReadShape {
        // `EXECUTE p` answers with whatever `p` answers, so a prepared
        // `SELECT ... FOR UPDATE` is a locking read: classifying the
        // `EXECUTE` keyword alone as Unknown ran it without the locks its
        // transaction owes (Go `ExecuteExec` runs the prepared plan, whose
        // `SelectLock` takes them). Only the locking shape is taken from the
        // template; every other shape needs bound parameters.
        if let Stmt::Session(session) = stmt {
            if let tidb_ast::SessionStmt::Execute { name, .. } = &**session {
                return match self.prepared_statements.get(name) {
                    Some(prepared)
                        if self.statement_read_shape_bound(&prepared.statement)
                            == StatementReadShape::LockingRead =>
                    {
                        StatementReadShape::LockingRead
                    }
                    _ => StatementReadShape::Unknown,
                };
            }
        }
        let disable_point_get =
            crate::variables::effective_fix_52592(stmt, self.vars.optimizer_fix_control());
        let catalog = self
            .catalog
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let shape = tidb_executor::access_path::statement_read_shape(
            stmt,
            &catalog,
            self.current_database(),
            &self.session_time_zone(),
        );
        // Fix52592 disables TryFastPlan, not MaxTS for a TableReader whose
        // range is one complete clustered key (Go common_plans.go). A LIMIT
        // can only use this AST proof when TryFastPlan folds it away; retain
        // the conservative answer for that shape when the fast path is off.
        let has_limit = matches!(stmt, Stmt::Query(query)
            if matches!(query.as_ref(), tidb_ast::QueryStmt::Select(select)
                if select.limit.is_some()));
        if disable_point_get && has_limit && shape == StatementReadShape::AutocommitPointGet {
            StatementReadShape::Unknown
        } else {
            shape
        }
    }

    /// Classifies a statement by parsing alone (no execution), so a caller can
    /// choose the protocol answer shape before running it.
    ///
    /// This decides the SHAPE of the answer, not whether the statement is
    /// supported: a `SHOW` this tier cannot answer still classifies as a
    /// query and reports its own error when it runs. Classifying it as
    /// unsupported here is what made every `SHOW` fail over the wire while
    /// `run` answered it in process -- the two callers of one session have to
    /// agree.
    pub fn statement_kind(&self, sql: &str) -> Result<StmtKind, DriverError> {
        let stmt = self.parse(sql)?;
        Ok(self.statement_kind_parsed(&stmt))
    }

    /// [`Self::statement_kind`] over a statement a front end already parsed.
    ///
    /// This answers only what SHAPE the answer takes.
    /// [`Self::stored_state_change_parsed`] asks a different question of the
    /// same statement, and the two stay apart because neither answer implies
    /// the other: `GRANT` is `Write`-shaped AND changes stored accounts,
    /// `INSERT` is `Write`-shaped and changes nothing stored outside this
    /// process, and `SELECT` is `Query`-shaped and changes nothing.
    #[must_use]
    pub fn statement_kind_parsed(&self, stmt: &Stmt) -> StmtKind {
        match stmt {
            // `EXECUTE p` answers with whatever `p` answers, so its shape is
            // not in its own parse -- it is in the statement this session
            // prepared under that name. Reading the `EXECUTE` keyword alone
            // and calling it a write is what made a text-protocol
            // `PREPARE p FROM 'SELECT 1'; EXECUTE p` run the SELECT down the
            // write path and report 1105 "a write statement unexpectedly
            // produced rows" instead of the row.
            //
            // A name this session does not hold has no shape to resolve; it
            // classifies as a query so the 8111 the execution raises reaches
            // the client through the ordinary path rather than being
            // reshaped into an internal error.
            Stmt::Session(session) => match &**session {
                tidb_ast::SessionStmt::Execute { name, .. } => self
                    .prepared_statement_sql(name)
                    .and_then(|sql| self.parse(sql).ok())
                    .map_or(StmtKind::Query, |inner| self.statement_kind_parsed(&inner)),
                _ => StmtKind::Write,
            },
            // An `Admin` statement answers with an OK packet exactly when the
            // plan Go builds for it has an empty schema; see
            // `admin_plan_has_result_schema`.
            Stmt::Admin(admin) if !admin_plan_has_result_schema(admin) => StmtKind::Write,
            // `SHOW`/`DESCRIBE`/`EXPLAIN` all answer with a result set.
            Stmt::Query(_) | Stmt::Admin(_) => StmtKind::Query,
            // Go `NonTransactionalDMLExec` answers a `BATCH ...` with a result
            // set -- the job count and status, or under `DRY RUN` the split
            // and query statements -- never an OK packet.
            Stmt::Dml(dml) if matches!(dml.as_ref(), tidb_ast::DmlStmt::Batch(_)) => {
                StmtKind::Query
            }
            // `USE`, `SET` and the transaction controls answer with an OK
            // packet, the same shape a write uses.
            Stmt::Dml(_) | Stmt::Ddl(_) => StmtKind::Write,
        }
    }

    /// Go DDLExec.Next's session-local CREATE exception.
    fn is_local_temporary_create_parsed(stmt: &Stmt) -> bool {
        let Stmt::Ddl(ddl) = stmt else {
            return false;
        };
        matches!(
            ddl.as_ref(),
            tidb_ast::DdlStmt::CreateTable(create)
                if create.temporary == tidb_ast::CreateTableTemporary::Local
        )
    }

    /// Whether the session overlay owns DDL execution or its local-target
    /// refusal. Mixed DROP retains a separate persistent operation.
    pub fn is_local_temporary_ddl_parsed(&self, stmt: &Stmt) -> bool {
        if Self::is_local_temporary_create_parsed(stmt) {
            return true;
        }
        let Stmt::Ddl(ddl) = stmt else {
            return false;
        };
        use tidb_ast::DdlStmt;
        match ddl.as_ref() {
            DdlStmt::TruncateTable(name) => self.is_local_temporary_table_path(name),
            DdlStmt::DropTable(drop) => {
                drop.temporary == tidb_ast::DropTemporary::Local
                    || self.split_local_temporary_drop(drop).0.names.is_empty()
            }
            _ => self.local_temporary_ddl_refusal(stmt).is_some(),
        }
    }

    /// DDLExec checks every rename source before starting any durable job.
    /// The refusal is an executor error, after the implicit commit.
    pub(crate) fn local_temporary_ddl_refusal(&self, stmt: &Stmt) -> Option<DriverError> {
        let Stmt::Ddl(ddl) = stmt else {
            return None;
        };
        use tidb_ast::DdlStmt;
        let operation = match ddl.as_ref() {
            DdlStmt::AlterTable(alter) if self.is_local_temporary_table_path(&alter.name) => {
                "ALTER TABLE"
            }
            DdlStmt::CreateIndex(index) if self.is_local_temporary_table_path(&index.table) => {
                "CREATE INDEX"
            }
            DdlStmt::DropIndex(index) if self.is_local_temporary_table_path(&index.table) => {
                "DROP INDEX"
            }
            DdlStmt::RenameTable(rename)
                if rename
                    .pairs
                    .iter()
                    .any(|(from, _)| self.is_local_temporary_table_path(from)) =>
            {
                "RENAME TABLE"
            }
            _ => return None,
        };
        Some(DriverError::UnsupportedLocalTempTableDDL(operation))
    }

    /// Resolve DROP against the same overlay used by execution and privileges.
    pub fn split_local_temporary_drop(
        &self,
        drop: &tidb_ast::DropTableStmt,
    ) -> (tidb_ast::DropTableStmt, Vec<Vec<String>>) {
        tidb_executor::split_drop_table_targets(drop, |name| {
            self.is_local_temporary_table_path(name)
        })
    }

    /// DDLExec.Next's early-return exceptions preserve the user's transaction.
    /// Explicit local DROP returns early even when some names are missing.
    pub fn ddl_preserves_transaction(&self, stmt: &Stmt) -> bool {
        if Self::is_local_temporary_create_parsed(stmt) {
            return true;
        }
        // Go parses `ALTER TABLE ... COMPACT` as its own `CompactTableStmt`,
        // not a DDL node, so it runs inside the open transaction.
        if matches!(stmt, Stmt::Ddl(ddl) if matches!(ddl.as_ref(),
            tidb_ast::DdlStmt::AlterTable(alter)
                if matches!(alter.actions.as_slice(), [tidb_ast::AlterTableAction::Compact { .. }])))
        {
            return true;
        }
        matches!(stmt, Stmt::Ddl(ddl) if matches!(ddl.as_ref(), tidb_ast::DdlStmt::DropTable(drop)
            if drop.temporary == tidb_ast::DropTemporary::Local
                || self.split_local_temporary_drop(drop).0.names.is_empty()))
    }

    /// Go's `ErrOptOnCacheTable` refusals for a cached table, which its
    /// executor raises before any job is built (`AlterTable`, `createIndex`,
    /// `dropIndex`, `TruncateTable`, `renameTable(s)`).
    pub fn validate_cached_table_ddl(&self, stmt: &tidb_ast::Stmt) -> Result<(), DriverError> {
        let catalog = self.lock_catalog()?;
        tidb_executor::ddl::check_cached_table_ddl(&catalog, &self.vars.current_db, stmt)
    }

    /// Go's submitter validates persistent FK references after implicit commit.
    /// These admission checks remain active when the global feature is OFF;
    /// the gated worker rechecks cannot replace them.
    pub fn validate_persistent_ddl_references(
        &mut self,
        statement: &tidb_exec::cluster_ddl::DdlStatement,
    ) -> Result<(), DriverError> {
        use tidb_exec::cluster_ddl::DdlStatement;
        // Use the shared persistent image, never a local temporary overlay.
        let ctx = self.statement_context(false);
        let mut catalog = self.lock_catalog()?;
        match statement {
            // Go `GetModifiableColumnJob` admits the change before it submits
            // the job; the rows are checked in the DDL transaction.
            DdlStatement::ModifyColumn {
                schema,
                table,
                column,
                position,
                rename_from,
                if_exists,
                ..
            } => {
                match tidb_executor::ddl::check_modifiable_column(
                    &mut catalog,
                    schema,
                    table,
                    rename_from.as_deref().unwrap_or(&column.name),
                    column,
                    position,
                    &ctx,
                ) {
                    // Go `ModifyColumn` / `ChangeColumn`: under IF EXISTS
                    // `ErrColumnNotExists` becomes the job builder's note.
                    Err(DriverError::UnknownColumnInTable { .. }) if *if_exists => {}
                    other => {
                        other?;
                    }
                }
            }
            // Each MODIFY / CHANGE of a multi-schema change is admitted
            // against the original table, as Go builds every sub-job from it.
            DdlStatement::MultiSchemaChange {
                schema,
                table,
                actions,
            } => {
                for action in actions {
                    if let tidb_exec::cluster_ddl::AlterColumnAction::Modify {
                        column,
                        position,
                        rename_from,
                        ..
                    } = action
                    {
                        tidb_executor::ddl::check_modifiable_column(
                            &mut catalog,
                            schema,
                            table,
                            rename_from.as_deref().unwrap_or(&column.name),
                            column,
                            position,
                            &ctx,
                        )?;
                    }
                }
            }
            DdlStatement::DropIndex { schema, table, index, .. } => {
                tidb_executor::check_index_needed(&catalog, schema, table, index)?;
            }
            DdlStatement::DropTable { schema, table, .. } if self.foreign_key_checks() => {
                tidb_executor::ddl::check_drop_table_references(
                    &catalog, &[(schema.clone(), table.clone())],
                )?;
            }
            DdlStatement::DropTables { names, .. } if self.foreign_key_checks() => {
                tidb_executor::ddl::check_drop_table_references(&catalog, names)?;
            }
            DdlStatement::TruncateTable { schema, table } if self.foreign_key_checks() => {
                if let Some(error) = tidb_executor::find_table_referred(
                    &catalog, schema, table, &[(schema.clone(), table.clone())],
                ) {
                    return Err(error);
                }
            }
            DdlStatement::DropDatabase { name, .. } if self.foreign_key_checks() => {
                if let Some(error) = tidb_executor::find_database_referred(&catalog, name) {
                    return Err(error);
                }
            }
            DdlStatement::ExchangePartition {
                schema,
                table,
                standalone_schema,
                standalone_table,
                ..
            } => {
                Self::exchange_local_temporary_refusal(
                    &catalog,
                    (schema, table),
                    self.local_temporary_table_name(standalone_schema, standalone_table)
                        .as_deref(),
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Complete only the already-resolved local leg of a successful cluster DROP.
    /// This does not start another statement or replace its warnings/attribution.
    pub fn drop_local_temporary_targets(
        &mut self,
        names: Vec<Vec<String>>,
    ) -> Result<(), DriverError> {
        let database = self.vars.current_db.clone();
        // The shared splitter reverses local names; restore input order here
        // so its executor walks the same reverse order as the original DROP.
        let drop = tidb_ast::DropTableStmt {
            temporary: tidb_ast::DropTemporary::Local,
            if_exists: false,
            names: names.into_iter().rev().collect(),
        };
        self.with_catalog_mut(|catalog| {
            // A LOCAL temporary drop submits no DDL job, so no BDR admission.
            tidb_executor::run_drop_table_stmt_in(
                &drop,
                catalog,
                &database,
                false,
                &tidb_executor::StmtContext::for_query(),
            )
            .map(|_| ())
        })
    }

    pub(crate) fn is_local_temporary_table_path(&self, name: &[String]) -> bool {
        let (database, table) = match name {
            [table] => (self.vars.current_db.as_str(), table.as_str()),
            [database, table] => (database.as_str(), table.as_str()),
            _ => return false,
        };
        self.is_local_temporary_table(database, table)
    }

    /// The stored name of the session's LOCAL temporary table `table`, if
    /// it has one.
    pub(crate) fn local_temporary_table_name(&self, database: &str, table: &str) -> Option<String> {
        let database = tidb_util::stringutil::go_to_lower(database);
        let table = tidb_util::stringutil::go_to_lower(table);
        self.local_temporary_tables
            .iter()
            .find(|(db, name, _)| {
                tidb_util::stringutil::go_to_lower(db) == database
                    && tidb_util::stringutil::go_to_lower(name) == table
            })
            .map(|(_, _, stored)| stored.name.clone())
    }

    /// Go `ExchangeTablePartition` resolves the table exchanged in against
    /// the session's infoschema before the shared one, because only the
    /// session holds a LOCAL temporary table: 1733, once the partitioned
    /// table has resolved.
    pub(crate) fn exchange_local_temporary_refusal(
        catalog: &tidb_executor::Catalog,
        partitioned: (&str, &str),
        local_temporary: Option<&str>,
    ) -> Result<(), DriverError> {
        match local_temporary {
            Some(name) if catalog.table_in(partitioned.0, partitioned.1).is_some() => {
                Err(DriverError::DdlCoded {
                    proto: &tidb_util::dbterror::ERR_PARTITION_EXCHANGE_TEMP_TABLE,
                    message: format!("Table to exchange with partition is temporary: '{name}'"),
                })
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn is_local_temporary_table(&self, database: &str, table: &str) -> bool {
        let database = tidb_util::stringutil::go_to_lower(database);
        let table = tidb_util::stringutil::go_to_lower(table);
        self.local_temporary_tables.iter().any(|(db, name, _)| {
            tidb_util::stringutil::go_to_lower(db) == database
                && tidb_util::stringutil::go_to_lower(name) == table
        })
    }

    /// Which persistent state `sql` would change: the stored schema (Go's
    /// `ast.DDLNode`), the stored accounts (the privilege and role statements
    /// TiDB's parser builds as administrative rather than DDL, plus `SET
    /// PASSWORD`), or neither.
    ///
    /// A front end whose catalog and account table are a *read* of somebody
    /// else's stored state needs this: running such a statement would change
    /// only this process's in-memory copy, which is a silently wrong answer
    /// rather than a slow one. The two halves are named apart because a front
    /// end can gain a route for one without gaining a route for the other --
    /// the convergence node writes the cluster's catalog but not its `mysql.*`
    /// rows. Classifying it here keeps that decision on the parse, next to
    /// [`Self::statement_kind`], instead of in each front end's own matcher.
    pub fn statement_stored_state_change(
        &self,
        sql: &str,
    ) -> Result<StoredStateChange, DriverError> {
        let stmt = self.parse(sql)?;
        Ok(Self::stored_state_change_parsed(&stmt))
    }

    /// [`Self::statement_stored_state_change`] over a statement a front end
    /// already parsed. See [`Self::statement_kind_parsed`] for why this stays
    /// a separate question rather than folding into the answer shape.
    #[must_use]
    pub fn stored_state_change_parsed(stmt: &Stmt) -> StoredStateChange {
        match stmt {
            // The account statements the parser builds as DDL nodes, because
            // Go's `ast.DDLNode` covers them too: they write `mysql.user` and
            // the role edges, never the catalog.
            Stmt::Ddl(ddl)
                if matches!(
                    ddl.as_ref(),
                    tidb_ast::DdlStmt::CreateUser { .. }
                        | tidb_ast::DdlStmt::CreateRole { .. }
                        | tidb_ast::DdlStmt::AlterUser(_)
                        | tidb_ast::DdlStmt::DropUser { .. }
                        | tidb_ast::DdlStmt::RenameUser { .. }
                ) =>
            {
                StoredStateChange::Accounts
            }
            // Only ALTER INSTANCE changes process state. Sequence and
            // partition DDL change persistent schema and must reach the same
            // cluster authority as every other schema change.
            Stmt::Ddl(ddl) if matches!(ddl.as_ref(), tidb_ast::DdlStmt::AlterInstance(_)) => {
                StoredStateChange::None
            }
            // Go's hypothetical indexes and TiFlash replicas are
            // `SessionVars` state: no job runs and no schema version moves.
            Stmt::Ddl(ddl) if is_hypo_ddl(ddl) => StoredStateChange::None,
            // `ALTER TABLE ... COMPACT` is Go's separate `CompactTableStmt`
            // (a `DDLNode` only in the parser's spelling): it validates the
            // table and partition names and, without a TiFlash replica,
            // answers a warning. It never writes the catalog.
            Stmt::Ddl(ddl) if is_compact_table(ddl) => StoredStateChange::None,
            Stmt::Ddl(_) => StoredStateChange::Schema,
            // `ADMIN CLEANUP TABLE LOCK` is a DDL job (`ActionUnlockTable`).
            Stmt::Admin(admin)
                if matches!(admin.as_ref(), tidb_ast::AdminStmt::CleanupTableLock(_)) =>
            {
                StoredStateChange::Schema
            }
            // The privilege/role statements: everything under `Admin` that
            // writes `mysql.user`, `mysql.db`, or the role edges. `SHOW
            // GRANTS` and the other inspections read and are left alone.
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::Grant(_)
                        | tidb_ast::AdminStmt::GrantProxy(_)
                        | tidb_ast::AdminStmt::GrantRole(_)
                        | tidb_ast::AdminStmt::Revoke(_)
                        | tidb_ast::AdminStmt::RevokeRole(_)
                ) =>
            {
                StoredStateChange::Accounts
            }
            // `SET PASSWORD` and `SET DEFAULT ROLE` write `mysql.user` and
            // `mysql.default_roles`; every other `SET` is session- or
            // process-local.
            Stmt::Session(session)
                if matches!(
                    session.as_ref(),
                    tidb_ast::SessionStmt::SetPassword(_)
                        | tidb_ast::SessionStmt::SetDefaultRole(_)
                ) =>
            {
                StoredStateChange::Accounts
            }
            // `SET GLOBAL x = v` (alone or mixed with SESSION assignments in
            // the same statement) writes `mysql.global_variables`. A `SET`
            // with no GLOBAL-scoped assignment at all changes only this
            // session's own copies, so it takes the ordinary path.
            Stmt::Session(session) if has_global_assignment(session) => {
                StoredStateChange::GlobalVars
            }
            // `ANALYZE TABLE` writes `mysql.stats_*`, which every node in the
            // cluster reads. A front end whose tables live in the cluster must
            // route it to a node that can write them; answering it from this
            // process's own catalog would tell the client its histograms moved
            // when no other node would ever see them.
            //
            // This says nothing about an IN-PROCESS session, whose catalog is
            // the whole world: there `crate::analyze_arm` runs the statement
            // and publishes the result, because there is no other node to tell.
            // The two are not in tension -- this classification answers "would
            // this change state outside the process", and in-process the answer
            // is no.
            // `LOAD STATS` writes the same `mysql.stats_*` tables an
            // `ANALYZE` does (Go's `loadStatsFromJSON` ends in
            // `SaveColOrIdxStatsToStorage` + `SaveMetaToStorage`), so it
            // classifies with it and is routed to the cluster statistics
            // writer.
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::AnalyzeTable(_)
                        | tidb_ast::AdminStmt::AnalyzeIncremental(_)
                        | tidb_ast::AdminStmt::LoadStats(_)
                        | tidb_ast::AdminStmt::DropStats(_)
                ) || matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::Flush(flush)
                        if matches!(flush.target, tidb_ast::FlushTarget::StatsDelta { .. })
                ) =>
            {
                StoredStateChange::Statistics
            }
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::LockStats(_) | tidb_ast::AdminStmt::UnlockStats(_)
                ) =>
            {
                StoredStateChange::StatsLock
            }
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::SetBdrRole(_)
                        | tidb_ast::AdminStmt::UnsetBdrRole
                        | tidb_ast::AdminStmt::ShowBdrRole
                ) =>
            {
                StoredStateChange::BdrRole
            }
            Stmt::Admin(_) | Stmt::Session(_) | Stmt::Query(_) | Stmt::Dml(_) => {
                StoredStateChange::None
            }
        }
    }
}

/// Go's `CompactTableStmt`, which the parser spells as a lone
/// `ALTER TABLE ... COMPACT` action.
pub(crate) fn is_compact_table(ddl: &tidb_ast::DdlStmt) -> bool {
    matches!(
        ddl,
        tidb_ast::DdlStmt::AlterTable(alter)
            if matches!(
                alter.actions.as_slice(),
                [tidb_ast::AlterTableAction::Compact { .. }]
            )
    )
}

/// The statements Go answers from `SessionVars` alone: `CREATE INDEX ... TYPE
/// HYPO`, `DROP HYPO INDEX` and a lone `ALTER TABLE ... SET HYPO TIFLASH
/// REPLICA`.
pub(crate) fn is_hypo_ddl(ddl: &tidb_ast::DdlStmt) -> bool {
    match ddl {
        tidb_ast::DdlStmt::CreateIndex(create) => {
            create.options.index_type == Some(tidb_ast::IndexType::HYPO)
        }
        tidb_ast::DdlStmt::DropIndex(drop) => drop.is_hypo,
        tidb_ast::DdlStmt::AlterTable(alter) => matches!(
            alter.actions.as_slice(),
            [tidb_ast::AlterTableAction::SetTiFlashReplica { hypo: true, .. }]
        ),
        _ => false,
    }
}
