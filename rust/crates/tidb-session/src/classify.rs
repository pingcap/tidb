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

//! What a statement is, decided by parsing alone: the shape of its answer, the
//! persistent state outside this process it would change, and the class
//! `ROW_COUNT()` distinguishes.
//!
//! None of these run the statement. A front end has to know the answer's shape
//! before executing (running twice would duplicate a write), and a front end
//! whose catalog or account table is a READ of somebody else's stored state has
//! to know that before answering OK to a change that went nowhere.

use tidb_ast::{DmlStmt, Stmt};
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
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        Ok(match &stmt {
            // `KILL` is the one admin statement that answers with an OK
            // packet rather than a result set, as it does in Go.
            Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Kill(_)) => {
                StmtKind::Write
            }
            // `SHOW`/`DESCRIBE`/`EXPLAIN` all answer with a result set.
            Stmt::Query(_) | Stmt::Admin(_) => StmtKind::Query,
            // `USE`, `SET` and the transaction controls answer with an OK
            // packet, the same shape a write uses.
            Stmt::Dml(_) | Stmt::Ddl(_) | Stmt::Session(_) => StmtKind::Write,
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
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        Ok(match &stmt {
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
            // cluster reads. Running it against this process's own loaded
            // statistics would answer OK to a client whose table's histograms
            // did not move anywhere.
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::AnalyzeTable(_)
                        | tidb_ast::AdminStmt::AnalyzeIncremental(_)
                ) =>
            {
                StoredStateChange::Statistics
            }
            Stmt::Admin(_) | Stmt::Session(_) | Stmt::Query(_) | Stmt::Dml(_) => {
                StoredStateChange::None
            }
        })
    }
}
