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

use tidb_ast::{DmlStmt, Stmt};
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
    /// `ANALYZE TABLE` is the only statement that writes them.
    Statistics,
}

/// Whether a `SET` statement carries at least one GLOBAL-scoped assignment.
///
/// A statement can mix `SESSION` and `GLOBAL` assignments in one `SET`
/// (Go allows this), so the whole statement is routed to the GLOBAL path the
/// moment any assignment is GLOBAL-scoped; the session-scoped assignments in
/// the same statement still run, against the same driver, once routed.
fn has_global_assignment(session: &tidb_ast::SessionStmt) -> bool {
    fn is_global(assignment: &tidb_ast::SystemVariableAssignment) -> bool {
        assignment.scope == tidb_ast::SystemVariableScope::Global
    }
    match session {
        tidb_ast::SessionStmt::Set(set) => set.assignments.iter().any(is_global),
        tidb_ast::SessionStmt::SetCharset { assignments, .. } => assignments.iter().any(is_global),
        tidb_ast::SessionStmt::SetMixed(items) => items.iter().any(|item| match item {
            tidb_ast::SetItem::System(assignment) => is_global(assignment),
            tidb_ast::SetItem::Charset { .. } => false,
        }),
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

/// Classifies a parsed statement for `ROW_COUNT()`, unwrapping a `WITH`
/// prefix the way Go does -- the CTE belongs to the mutation, so
/// `WITH x AS (...) DELETE ...` still sets `InDeleteStmt`.
pub(crate) fn statement_kind_of(stmt: &Stmt) -> StatementKind {
    fn dml_kind(dml: &DmlStmt) -> StatementKind {
        match dml {
            DmlStmt::With { statement, .. } => dml_kind(statement),
            DmlStmt::Insert(_) | DmlStmt::Update(_) | DmlStmt::Delete(_) => StatementKind::Dml,
            _ => StatementKind::Other,
        }
    }
    match stmt {
        Stmt::Query(_) => StatementKind::Select,
        Stmt::Dml(dml) => dml_kind(dml),
        _ => StatementKind::Other,
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
        let bound;
        let sql = if params.is_empty() {
            sql
        } else {
            match tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode()) {
                Ok(text) => {
                    bound = text;
                    &bound
                }
                Err(_) => return StatementReadShape::Unknown,
            }
        };
        let Ok(stmt) = self.parse(sql) else {
            return StatementReadShape::Unknown;
        };
        self.statement_read_shape_bound(&stmt)
    }

    /// Classify a prepared statement from the AST retained at PREPARE time.
    ///
    /// Binary-protocol EXECUTE already has this tree, so reparsing the SQL
    /// text here only adds latency before the statement can start. Bind a
    /// clone of the template for the same access-path decision without
    /// changing the template stored in the prepared handle.
    #[must_use]
    pub fn statement_read_shape_parsed(
        &self,
        statement: &Stmt,
        params: &[Datum],
    ) -> StatementReadShape {
        let stmt = if params.is_empty() {
            statement.clone()
        } else {
            match tidb_executor::bind_statement(statement.clone(), params) {
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
        let catalog = self
            .catalog
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        tidb_executor::access_path::statement_read_shape(
            stmt,
            &catalog,
            self.current_database(),
            &self.session_time_zone(),
        )
    }

    /// Classifies the narrow prepared clustered-handle point read directly
    /// from its retained template and execute values.  Unlike
    /// [`Self::statement_read_shape_parsed`], this path does not clone and
    /// bind the complete AST; a refusal returns `Unknown` and lets the caller
    /// use the ordinary bound-tree path.
    #[must_use]
    pub fn fast_prepared_statement_read_shape(
        &self,
        statement: &Stmt,
        params: &[Datum],
    ) -> StatementReadShape {
        let catalog = self
            .catalog
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        tidb_executor::access_path::prepared_statement_read_shape(
            statement,
            params,
            &catalog,
            self.current_database(),
            &self.session_time_zone(),
        )
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
            // `KILL` and `FLUSH` are the admin statements that answer with an
            // OK packet rather than a result set, as they do in Go: both are
            // `SimpleExec` there, which produces no rows.
            Stmt::Admin(admin)
                if matches!(
                    &**admin,
                    tidb_ast::AdminStmt::Kill(_) | tidb_ast::AdminStmt::Flush(_)
                ) =>
            {
                StmtKind::Write
            }
            // `SHOW`/`DESCRIBE`/`EXPLAIN` all answer with a result set.
            Stmt::Query(_) | Stmt::Admin(_) => StmtKind::Query,
            // `USE`, `SET` and the transaction controls answer with an OK
            // packet, the same shape a write uses.
            Stmt::Dml(_) | Stmt::Ddl(_) => StmtKind::Write,
        }
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
            Stmt::Ddl(_) => StoredStateChange::Schema,
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
            // classifies with it: routed at a cluster node when the tables
            // live in the cluster, run by `crate::load_stats_arm` in-process.
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::AnalyzeTable(_)
                        | tidb_ast::AdminStmt::AnalyzeIncremental(_)
                        | tidb_ast::AdminStmt::LoadStats(_)
                ) =>
            {
                StoredStateChange::Statistics
            }
            Stmt::Admin(_) | Stmt::Session(_) | Stmt::Query(_) | Stmt::Dml(_) => {
                StoredStateChange::None
            }
        }
    }
}
