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

//! Source-visible payloads for TiDB's ordinary `SHOW` inspection grammar.
//!
//! Security-owned `SHOW GRANTS`/`SHOW CREATE USER`, SQL bindings, and
//! `ADMIN SHOW` controls deliberately remain with their semantic domains.

use crate::util::push_name_path;
use crate::Expr;

#[path = "show/character_set.rs"]
mod character_set;
#[path = "show/engines.rs"]
mod engines;
#[path = "show/open_tables.rs"]
mod open_tables;
#[path = "show/stats_buckets.rs"]
mod stats_buckets;
#[path = "show/stats_locked.rs"]
mod stats_locked;

pub use character_set::{ShowCharsetFilter, ShowCharsetStmt};
pub use engines::{ShowEnginesFilter, ShowEnginesStmt};
pub use open_tables::ShowOpenTablesStmt;
pub use stats_buckets::{ShowStatsBucketsFilter, ShowStatsBucketsStmt};
pub use stats_locked::{ShowStatsLockedFilter, ShowStatsLockedStmt};

/// The object kind of a `SHOW CREATE ...` statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowCreateKind {
    /// `SHOW CREATE TABLE`.
    Table,
    /// `SHOW CREATE VIEW`.
    View,
    /// `SHOW CREATE SEQUENCE`.
    Sequence,
    /// `SHOW CREATE DATABASE` (also `SHOW CREATE SCHEMA`, restored as
    /// `DATABASE`).
    Database,
}

/// TiDB's `SHOW WARNINGS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowWarningsStmt {
    /// Optional filter over the virtual `Level`, `Code`, and `Message` rows.
    pub filter: Option<ShowWarningsFilter>,
}

/// The two optional `SHOW WARNINGS` filters accepted by Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowWarningsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowWarningsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW WARNINGS");
        match &self.filter {
            None => {}
            Some(ShowWarningsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowWarningsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW [GLOBAL | SESSION] STATUS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatusStmt {
    /// `GLOBAL` scope; `false` represents the explicit or implicit session
    /// scope that Go restores as `SESSION`.
    pub global: bool,
    /// Optional filter over the status-variable result rows.
    pub filter: Option<ShowStatusFilter>,
}

/// The mutually exclusive `SHOW STATUS` filters in Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatusFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowStatusStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(if self.global {
            "SHOW GLOBAL STATUS"
        } else {
            "SHOW SESSION STATUS"
        });
        match &self.filter {
            None => {}
            Some(ShowStatusFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowStatusFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW ERRORS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowErrorsStmt {
    /// Whether source used `SHOW COUNT(*) ERRORS`.
    pub count_only: bool,
    /// Optional filter over the virtual `Level`, `Code`, and `Message` rows.
    pub filter: Option<ShowErrorsFilter>,
}

/// The optional `SHOW ERRORS` filter accepted by Go's shared SHOW grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowErrorsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowErrorsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW ERRORS");
        match &self.filter {
            None => {}
            Some(ShowErrorsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowErrorsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW COLLATION` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCollationStmt {
    /// Optional filter over virtual collation metadata.
    pub filter: Option<ShowCollationFilter>,
}

/// The two optional `SHOW COLLATION` filters accepted by Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowCollationFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowCollationStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW COLLATION");
        match &self.filter {
            None => {}
            Some(ShowCollationFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowCollationFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW STATS_HISTOGRAMS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatsHistogramsStmt {
    /// Optional predicate over the virtual histogram metadata rows.
    pub filter: Option<ShowStatsHistogramsFilter>,
}

/// The optional `SHOW STATS_HISTOGRAMS` filter accepted by Go's shared SHOW
/// grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatsHistogramsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowStatsHistogramsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW STATS_HISTOGRAMS");
        match &self.filter {
            None => {}
            Some(ShowStatsHistogramsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowStatsHistogramsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW STATS_TOPN` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatsTopNStmt {
    /// Optional predicate over the virtual TopN statistics rows.
    pub filter: Option<ShowStatsTopNFilter>,
}

/// The optional filter accepted by Go's `SHOW STATS_TOPN` source entry.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatsTopNFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowStatsTopNStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW STATS_TOPN");
        match &self.filter {
            None => {}
            Some(ShowStatsTopNFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowStatsTopNFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW DATABASES` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowDatabasesStmt {
    /// Optional virtual-schema filter.
    pub filter: Option<ShowDatabasesFilter>,
}

/// The optional filter carried by [`ShowDatabasesStmt`].
#[derive(Debug, Clone, PartialEq)]
pub enum ShowDatabasesFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowDatabasesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW DATABASES");
        match &self.filter {
            None => {}
            Some(ShowDatabasesFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowDatabasesFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// The optional predicates accepted by Go's `SHOW TABLES` grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowTablesFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

/// TiDB's `SHOW [FULL] TABLES` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTablesStmt {
    /// Whether source SQL contained the `FULL` modifier.
    pub full: bool,
    /// Optional database selected by `FROM` or `IN`, restored as `IN`.
    pub database: Option<String>,
    /// Optional predicate over the table metadata rows.
    pub filter: Option<ShowTablesFilter>,
}

impl ShowTablesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(if self.full {
            "SHOW FULL TABLES"
        } else {
            "SHOW TABLES"
        });
        if let Some(database) = &self.database {
            out.push_str(" IN ");
            push_name_path(out, std::slice::from_ref(database));
        }
        match &self.filter {
            None => {}
            Some(ShowTablesFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowTablesFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW TABLE STATUS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTableStatusStmt {
    /// Optional database selected by `FROM` or `IN` and restored as `IN`.
    pub database: Option<String>,
    /// Optional predicate over the virtual table-status metadata rows.
    pub filter: Option<ShowTableStatusFilter>,
}

/// The mutually exclusive `SHOW TABLE STATUS` filters in Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowTableStatusFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowTableStatusStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW TABLE STATUS");
        if let Some(database) = &self.database {
            out.push_str(" IN ");
            push_name_path(out, std::slice::from_ref(database));
        }
        match &self.filter {
            None => {}
            Some(ShowTableStatusFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowTableStatusFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// `SHOW TABLE name NEXT_ROW_ID`, distinct from both `SHOW TABLES` and
/// `ADMIN SHOW table NEXT_ROW_ID` in Go's AST.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTableNextRowIdStmt {
    /// The table whose allocator state is requested.
    pub table: Vec<String>,
}

impl ShowTableNextRowIdStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW TABLE ");
        push_name_path(out, &self.table);
        out.push_str(" NEXT_ROW_ID");
    }
}

/// TiDB's selected `SHOW COLUMNS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowColumnsStmt {
    /// The required table path after `FROM` or `IN`.
    pub table: Vec<String>,
    /// Optional filter over virtual column metadata rows.
    pub filter: Option<ShowColumnsFilter>,
}

/// The mutually exclusive `SHOW COLUMNS` filters accepted by this slice.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowColumnsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowColumnsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW COLUMNS IN ");
        push_name_path(out, &self.table);
        match &self.filter {
            None => {}
            Some(ShowColumnsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowColumnsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW INDEX` grammar form supported by this rewrite slice.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowIndexStmt {
    /// The table path written after the required `FROM` or `IN`.
    pub table: Vec<String>,
    /// Optional filter over the virtual index metadata rows.
    pub filter: Option<ShowIndexFilter>,
}

/// The mutually exclusive `SHOW INDEX` filters accepted by this slice.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowIndexFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowIndexStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW INDEX IN ");
        push_name_path(out, &self.table);
        match &self.filter {
            None => {}
            Some(ShowIndexFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowIndexFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}
