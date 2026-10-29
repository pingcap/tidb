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

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::compiler::PreparedExec;
use tidb_ast::Stmt;
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

pub(crate) use tidb_executor::sessionctx::{
    statement_kind_of, statement_priority_of, StatementKind,
};

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
        // Go builds the account statements, ALTER INSTANCE / RANGE and
        // DROP PROCEDURE through `buildSimple`, with no schema. CREATE
        // PROCEDURE never builds (`ErrUnsupportedType`), so it has no
        // result either.
        AdminStmt::CreateUser { .. }
        | AdminStmt::CreateRole { .. }
        | AdminStmt::AlterUser(_)
        | AdminStmt::DropUser { .. }
        | AdminStmt::RenameUser { .. }
        | AdminStmt::AlterInstance(_)
        | AdminStmt::AlterRange(_)
        | AdminStmt::CreateProcedure(_)
        | AdminStmt::DropProcedure(_)
        | AdminStmt::CompactTable(_) => false,
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

    /// Go `ExchangeTablePartition` resolves the table exchanged in against
    /// the session's infoschema first: a LOCAL temporary table lives only in
    /// the session, so this is the one submitter check the session makes
    /// itself. The foreign-key checks are the DDL executor's
    /// (`tidb_ddl::cluster_ddl`), against the latest schema.
    pub fn validate_persistent_ddl_references(
        &mut self,
        statement: &tidb_ddl::cluster_ddl::DdlStatement,
    ) -> Result<(), DriverError> {
        use tidb_ddl::cluster_ddl::DdlStatement;
        // Use the shared persistent image, never a local temporary overlay.
        let catalog = self.lock_catalog()?;
        if let DdlStatement::ExchangePartition {
            schema,
            table,
            standalone_schema,
            standalone_table,
            ..
        } = statement
        {
            Self::exchange_local_temporary_refusal(
                &catalog,
                (schema, table),
                self.local_temporary_table(standalone_schema, standalone_table)
                    .map(|table| table.name())
                    .as_deref(),
            )?;
        }
        Ok(())
    }

    /// Go `ExchangeTablePartition` resolves the table exchanged in against
    /// the session's infoschema before the shared one, because only the
    /// session holds a LOCAL temporary table: 1733, once the partitioned
    /// table has resolved.
    pub(crate) fn exchange_local_temporary_refusal(
        catalog: &tidb_infoschema::Catalog,
        partitioned: (&str, &str),
        local_temporary: Option<&str>,
    ) -> Result<(), DriverError> {
        match local_temporary {
            Some(name) if catalog.table_in(partitioned.0, partitioned.1).is_some() => {
                Err(DriverError::from(
                    (&tidb_dbterror::ERR_PARTITION_EXCHANGE_TEMP_TABLE).generate_with_stack(
                        format!("Table to exchange with partition is temporary: '{name}'"),
                    ),
                ))
            }
            _ => Ok(()),
        }
    }

    /// Go `SimpleExec.Next`'s account arms (and `GrantExec` / `RevokeExec`):
    /// the statements that write `mysql.user`, `mysql.db`, the role edges or
    /// `mysql.default_roles`. `SHOW GRANTS` and the other inspections read.
    #[must_use]
    pub fn is_account_statement(stmt: &Stmt) -> bool {
        match stmt {
            Stmt::Admin(admin) => matches!(
                admin.as_ref(),
                tidb_ast::AdminStmt::CreateUser { .. }
                    | tidb_ast::AdminStmt::CreateRole { .. }
                    | tidb_ast::AdminStmt::AlterUser(_)
                    | tidb_ast::AdminStmt::DropUser { .. }
                    | tidb_ast::AdminStmt::RenameUser { .. }
                    | tidb_ast::AdminStmt::Grant(_)
                    | tidb_ast::AdminStmt::GrantProxy(_)
                    | tidb_ast::AdminStmt::GrantRole(_)
                    | tidb_ast::AdminStmt::Revoke(_)
                    | tidb_ast::AdminStmt::RevokeRole(_)
            ),
            Stmt::Session(session) => matches!(
                session.as_ref(),
                tidb_ast::SessionStmt::SetPassword(_) | tidb_ast::SessionStmt::SetDefaultRole(_)
            ),
            Stmt::Ddl(_) | Stmt::Query(_) | Stmt::Dml(_) => false,
        }
    }
}
