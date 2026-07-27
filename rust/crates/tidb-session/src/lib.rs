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

//! The session: the single entry point that owns catalog state and runs SQL
//! statements through the wired parse -> plan -> execute pipeline.
//!
//! This is the seam of Go's `pkg/session` `session.ExecuteStmt`: one object a
//! client holds, dispatching each statement kind to its executor path.
//!
//! SEED SCOPE: [`Session::run`] dispatches `SELECT` (rows), `INSERT` (affected
//! count), and `CREATE TABLE` over the session's [`Catalog`]. DEFERRED
//! (documented): transactions (autocommit is implicit and immediate --
//! `BEGIN`/`COMMIT`/`ROLLBACK` land with the txnkv integration), session
//! variables (`SET`), prepared statements, the MySQL wire protocol, privileges,
//! and every other statement kind. Statements are currently parsed twice (once
//! here for dispatch, once in the driver's runner) -- a wiring simplification
//! to remove when the driver's runners take parsed statements.

use std::sync::{Arc, Mutex, MutexGuard};

use tidb_ast::{DdlStmt, DmlStmt, Stmt};
use tidb_datatype::{Datum, FieldType};
use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog, DriverError};

/// The result of running one statement.
#[derive(Debug, PartialEq)]
pub enum StmtResult {
    /// A query's result rows.
    Rows(Vec<Vec<Datum>>),
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// The result of running one statement, with wire-facing column metadata.
///
/// [`StmtResult::Rows`] loses column names/types; a server front end needs one
/// `(name, type)` per result column to build protocol column definitions, so
/// [`Session::run_with_columns`] returns this richer shape instead.
#[derive(Debug, PartialEq)]
pub enum StmtOutput {
    /// A query's result columns and rows.
    Rows {
        /// One `(display name, field type)` per output column.
        columns: Vec<(String, FieldType)>,
        /// The result rows (one `Datum` per column).
        rows: Vec<Vec<Datum>>,
    },
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

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

/// A process-wide catalog shared by every session, as Go's domain-owned
/// `infoschema` is shared by every session of a TiDB instance.
pub type SharedCatalog = Arc<Mutex<Catalog>>;

/// A session: runs statements against a catalog shared with its peers.
///
/// Go sessions borrow the process's schema state rather than owning private
/// copies, so a table one connection creates is visible to the others. This
/// mirrors that with a shared, mutex-guarded catalog; the statement-level lock
/// stands in for Go's schema-version/lease machinery, which is a separate
/// tier (documented deferral).
#[derive(Default)]
pub struct Session {
    catalog: SharedCatalog,
}

impl Session {
    /// A fresh session with its own empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Session::default()
    }

    /// A session sharing `catalog` with its peers.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        Session { catalog }
    }

    /// The shared catalog handle, for opening a peer session over the same
    /// schema state.
    #[must_use]
    pub fn shared_catalog(&self) -> SharedCatalog {
        Arc::clone(&self.catalog)
    }

    /// Borrows the shared catalog for one statement. The lock is held for the
    /// statement's duration only, which is the granularity Go's schema state
    /// is consumed at.
    fn lock_catalog(&self) -> Result<MutexGuard<'_, Catalog>, DriverError> {
        self.catalog
            .lock()
            .map_err(|_| DriverError::CatalogPoisoned)
    }

    /// Classifies a statement by parsing alone (no execution), so a caller can
    /// choose the protocol answer shape before running it.
    pub fn statement_kind(&self, sql: &str) -> Result<StmtKind, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        match &stmt {
            Stmt::Query(_) => Ok(StmtKind::Query),
            Stmt::Dml(_) | Stmt::Ddl(_) => Ok(StmtKind::Write),
            _ => Err(DriverError::Unsupported(
                "this statement kind is not supported yet",
            )),
        }
    }

    /// Runs one SQL statement (Go `session.ExecuteStmt`): parses, dispatches by
    /// statement kind, and executes over the session catalog.
    pub fn run(&mut self, sql: &str) -> Result<StmtResult, DriverError> {
        Ok(match self.run_with_columns(sql)? {
            StmtOutput::Rows { rows, .. } => StmtResult::Rows(rows),
            StmtOutput::Affected(count) => StmtResult::Affected(count),
            StmtOutput::Done(created) => StmtResult::Done(created),
        })
    }

    /// Like [`Session::run`], but a query result also carries its column
    /// metadata (`(name, type)` per column) for wire-protocol fronts.
    pub fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        match &stmt {
            Stmt::Query(_) => {
                let (columns, rows) = run_select_meta_on(sql, &*self.lock_catalog()?)?;
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Dml(dml) => match &**dml {
                DmlStmt::Insert(_) => {
                    let mut catalog = self.lock_catalog()?;
                    Ok(StmtOutput::Affected(run_insert_on(sql, &mut catalog)?))
                }
                _ => Err(DriverError::Unsupported(
                    "this DML statement kind is not supported yet",
                )),
            },
            Stmt::Ddl(ddl) => match &**ddl {
                DdlStmt::CreateTable(_) => {
                    let mut catalog = self.lock_catalog()?;
                    Ok(StmtOutput::Done(run_create_table_on(sql, &mut catalog)?))
                }
                _ => Err(DriverError::Unsupported(
                    "this DDL statement kind is not supported yet",
                )),
            },
            _ => Err(DriverError::Unsupported(
                "this statement kind is not supported yet",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A whole session lifecycle from SQL strings alone: DDL, writes, reads.
    #[test]
    fn session_runs_a_sql_lifecycle() {
        let mut session = Session::new();
        assert_eq!(
            session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap(),
            StmtResult::Done(true)
        );
        assert_eq!(
            session
                .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
                .unwrap(),
            StmtResult::Affected(3)
        );
        assert_eq!(
            session
                .run("SELECT a + b FROM t WHERE a >= 2 ORDER BY a DESC LIMIT 1")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(33)]])
        );
        // A second table coexists in the same catalog.
        session.run("CREATE TABLE u (x BIGINT)").unwrap();
        session.run("INSERT INTO u VALUES (42)").unwrap();
        assert_eq!(
            session.run("SELECT x FROM u").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(42)]])
        );
    }

    #[test]
    fn unsupported_kinds_error() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a INT)").unwrap();
        assert!(session.run("DELETE FROM t").is_err());
        assert!(session.run("DROP TABLE t").is_err());
    }
}
