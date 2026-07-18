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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The in-memory catalog's data shapes: a stored [`Table`] (plus its
//! [`ForeignKey`] metadata), the materialized [`Relation`]/[`Column`]
//! produced by scans and joins, and the [`table_key`] name-normalization
//! helper — everything that describes WHAT data exists, as opposed to the
//! session machinery (`crate::session`) that describes WHO is asking.

use tidb_ast::{ColumnType, Expr, ReferentialAction};

use crate::Row;

/// An in-memory table: ordered column names and stored rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Table {
    /// The column names, in declaration order.
    pub cols: Vec<String>,
    /// The declared column types, aligned to `cols`. Retained so
    /// `INSERT`/`UPDATE` can enforce type-width constraints
    /// (`VARCHAR`/`CHAR` length, `BIT` width, `DECIMAL` precision — see
    /// `crate::dml`'s `coerce_column`);
    /// most execution paths never consult it, since this seed executor's
    /// value domain is otherwise type-erased.
    pub(crate) col_types: Vec<ColumnType>,
    /// Each column's declared `DEFAULT` expression, aligned to `cols`:
    /// `Some(expr)` if the column was created with a `DEFAULT` option,
    /// `None` otherwise. Consulted when an `INSERT` omits a column (a
    /// column-subset or `SET`-form insert) or writes a bare `DEFAULT`
    /// value — a `None` (no explicit default) resolves to `NULL`, matching
    /// this executor's lenient, nullable-by-default value domain.
    pub(crate) col_defaults: Vec<Option<Expr>>,
    /// The sole `AUTO_INCREMENT` column, resolved once from the immutable
    /// `CREATE TABLE` declaration. The allocation cursor intentionally does
    /// not live here: table snapshots are transactional, while TiDB auto-ID
    /// allocation is not (see `Database::auto_increment_next`).
    pub(crate) auto_increment: Option<AutoIncrementColumn>,
    /// The stored rows (each aligned to `cols`).
    pub rows: Vec<Row>,
    /// The table's key constraint groups, as 0-based column indices: the
    /// `PRIMARY KEY` group first (if any — from a single column's own
    /// option or a table-level composite constraint), then each `UNIQUE`
    /// group (from a column's own option or a table-level constraint). A
    /// row that matches an existing row on ANY group is a duplicate-key
    /// conflict for `INSERT ... ON DUPLICATE KEY UPDATE` / `INSERT IGNORE` —
    /// real MySQL does not distinguish which constraint was violated for
    /// this purpose, so neither does this executor.
    pub(crate) key_groups: Vec<Vec<usize>>,
    /// Every declared key's name and resolved column positions. Unlike
    /// [`Self::key_groups`], this includes ordinary secondary indexes too:
    /// their rows do not participate in duplicate-key detection, but their
    /// names still share TiDB's table-local index namespace.
    pub(crate) indexes: Vec<IndexMetadata>,
    /// The table's own `FOREIGN KEY` constraints (this table is the
    /// "child" referencing another "parent" table). See
    /// `Database::check_foreign_keys` (child-side `INSERT`/`UPDATE`
    /// enforcement), `Database::delete_row_cascading` (parent-side
    /// `DELETE` cascading), and `Database::propagate_parent_update`
    /// (parent-side `UPDATE` cascading) for what's enforced.
    pub(crate) foreign_keys: Vec<ForeignKey>,
}

/// The schema identity of a table's `AUTO_INCREMENT` column.
///
/// The declared type remains in [`Table::col_types`], the one source of
/// signed/unsigned range truth for DML coercion. Keeping only the resolved
/// index here prevents a second, drifting type representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AutoIncrementColumn {
    pub(crate) column: usize,
}

/// One column part of an index after the DDL layer has resolved its name
/// against the table schema. Retaining the parse-level options keeps this
/// catalog ready for index-aware scans without making the full-scan executor
/// pretend to implement physical index storage today.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexColumn {
    pub(crate) column: usize,
    pub(crate) prefix_len: Option<i64>,
    pub(crate) desc: bool,
}

/// Table-local metadata for a primary, unique, or ordinary secondary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexMetadata {
    pub(crate) name: String,
    pub(crate) columns: Vec<IndexColumn>,
    pub(crate) unique: bool,
}

/// A resolved `FOREIGN KEY` constraint on a [`Table`]: `local_cols` are
/// 0-based indices into THIS table's own `cols` (resolved once, at `CREATE
/// TABLE` time); `ref_cols` are the referenced table's column NAMES,
/// resolved dynamically against `ref_table`'s current schema each time the
/// constraint is checked — this (rather than resolving indices eagerly too)
/// is what lets a self-referencing `FOREIGN KEY` work with no special case:
/// at `CREATE TABLE` time the table being created doesn't exist in the
/// catalog yet, but by the time any row is inserted, it does.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ForeignKey {
    pub(crate) local_cols: Vec<usize>,
    pub(crate) ref_table: String,
    pub(crate) ref_cols: Vec<String>,
    /// The `ON DELETE` action, if written — used only when a row is
    /// removed from `ref_table` (the "parent" side); `check_foreign_keys`
    /// (the "child side", run on this table's own `INSERT`/`UPDATE`)
    /// never reads it.
    pub(crate) on_delete: Option<ReferentialAction>,
    /// The `ON UPDATE` action, if written — used only when `ref_table`'s
    /// own referenced-column VALUES change (an `UPDATE` touching a
    /// column of `ref_table` that isn't one of `ref_cols` never triggers
    /// this).
    pub(crate) on_update: Option<ReferentialAction>,
}

/// Normalizes a table name path to a case-insensitive catalog key (last segment).
pub(crate) fn table_key(path: &[String]) -> String {
    path.last()
        .cloned()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// A relation column: the table qualifier(s) it is reachable under, and its
/// name. Almost always one qualifier; a `USING` join's coalesced column is
/// reachable under both sides' qualifiers (so `t1.a` and `t2.a` both resolve
/// to the same single physical column, matching MySQL).
#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) tables: Vec<String>,
    pub(crate) name: String,
}

/// A materialized relation: qualified columns and their rows. Produced by a
/// table scan or a join, then filtered/grouped/projected by `select`.
#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub(crate) cols: Vec<Column>,
    pub(crate) rows: Vec<Row>,
}
