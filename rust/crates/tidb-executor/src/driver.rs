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

//! A minimal query driver: parse a SQL string, rewrite its expressions, wire the
//! executors, and run it -- the first end-to-end parse -> plan -> execute of a
//! SQL string.
//!
//! SCOPE: `SELECT <exprs | *> [FROM <table>] [WHERE <pred>] [ORDER BY ...]
//! [LIMIT ...]` over a single in-memory [`Catalog`] table or the implicit dual
//! row. It parses via `tidb-parser`, resolves `FROM` against the catalog,
//! rewrites fields/predicates/by-items through
//! [`tidb_expr::rewriter::rewrite_expr_resolved`] (columns bound by the
//! [`TableResolver`]), and wires `MemTableSource|TableDual ->
//! [Selection] -> [Sort] -> Projection -> [Limit]`.
//!
//! DEFERRED (documented): joins and derived tables, `db.t` qualification
//! (single-schema catalog), ordering by select alias/position, and everything
//! the rewriter does not yet handle. The real storage-backed `TableReaderExec`
//! replaces [`MemTableSourceExec`] when storage/tablecodec integration lands.

use crate::access_path::{HandleSourceExec, IndexRangeSourceExec};
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, HashAggExec};
use crate::join::{JoinExec, JoinKind};
use crate::kv_table::{IndexRange, KvTable, TableHandle, TableScanExec};
use crate::limit::LimitExec;
use crate::mem_table::MemTableSourceExec;
use crate::plan_trace::{PlanTrace, Qualifier};
use crate::projection::ProjectionExec;
use crate::scan_pushdown::{PushedScanFilter, ScanComparison, ScanComparisonOp};
use crate::selection::SelectionExec;
use crate::sort::{SortByItem, SortExec};
use crate::table_dual::TableDualExec;
use std::collections::HashMap;
use tidb_ast::{JoinNode, QueryStmt, SelectField, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver, NoResolver};
use tidb_expr::schema::Schema;

/// An in-memory table: named, typed columns plus row values.
#[derive(Clone, Debug, Default)]
pub struct MemTable {
    /// The columns, in row order: `(name, type)`.
    pub columns: Vec<(String, FieldType)>,
    /// The rows (one `Datum` per column).
    pub rows: Vec<Vec<Datum>>,
}

/// Splits a table reference into its schema and table names. A bare name
/// resolves in the default schema; `db.t` names its schema explicitly.
///
/// Splits a table path for another module in this crate.
pub(crate) fn split_table_path_pub<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    split_table_path(path, current_db)
}

/// `current_db` is the session's selected schema (Go `SessionVars.CurrentDB`);
/// an empty one is Go's `ErrNoDB`.
fn split_table_path<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    match path {
        [name] => {
            if current_db.is_empty() {
                return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
            }
            Ok((current_db, name))
        }
        [database, name] => Ok((database, name)),
        _ => Err(DriverError::Unsupported("empty table name")),
    }
}

/// Go's default schema: TiDB's bootstrap runs
/// `CREATE DATABASE IF NOT EXISTS test`, so a fresh server always has it and
/// a connection with no explicit database lands there.
pub const DEFAULT_DATABASE: &str = "test";

/// One schema: Go `model.DBInfo`, reduced to the name and its tables.
///
/// NOT MODELLED (documented): the schema's charset, collation, placement
/// policy and state, which live on Go's `DBInfo` and matter to DDL rather
/// than to resolving a name.
#[derive(Clone, Debug, Default)]
struct Database {
    /// The name as written, for `SHOW DATABASES` output.
    name: String,
    tables: HashMap<String, TableEntry>,
}

/// A catalog of databases and their tables, the position Go's `infoschema`
/// occupies. Database and table names are case-insensitive, as in MySQL.
#[derive(Clone, Debug)]
pub struct Catalog {
    databases: HashMap<String, Database>,
    next_table_id: i64,
    /// Bumped by every mutation, so a transaction can detect that the shared
    /// catalog moved under it (Go detects the same at commit through TiKV's
    /// optimistic conflict check on the written keys).
    version: u64,
}

impl Default for Catalog {
    /// A catalog holding only `test`, as a freshly bootstrapped TiDB does.
    ///
    /// `INFORMATION_SCHEMA` is present because its tables are implemented
    /// (see `tidb-session`'s `infoschema`), and holds no stored tables of its
    /// own -- its rows are computed at query time.
    ///
    /// DIVERGENCE (documented): real TiDB also exposes `mysql`,
    /// `performance_schema`, `sys` and `metrics_schema`. Those are system
    /// schemas whose tables this seed does not implement, and listing them
    /// empty would claim more than is true, so they stay absent until their
    /// contents are ported.
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_owned(),
            Database {
                name: DEFAULT_DATABASE.to_owned(),
                tables: HashMap::new(),
            },
        );
        databases.insert(
            "information_schema".to_owned(),
            Database {
                name: "INFORMATION_SCHEMA".to_owned(),
                tables: HashMap::new(),
            },
        );
        Catalog {
            databases,
            next_table_id: 0,
            version: 0,
        }
    }
}

/// A view: Go `model.TableInfo` whose `View` field is set. A view stores no
/// rows -- its `SELECT` is re-run whenever the name is read.
///
/// `columns` holds the names the view reports -- the explicit
/// `CREATE VIEW v (...)` list when one was written, the body's field names
/// otherwise -- paired with the types the body had at `CREATE VIEW`. The
/// names are fixed, but the types are not: Go re-plans the body for every
/// read and for every metadata answer, so `ALTER TABLE base MODIFY` shows
/// through immediately (captured: `DESCRIBE` and
/// `information_schema.columns` both report the new type). Use
/// [`view_column_list`] rather than these cached types wherever the answer is
/// user-visible.
///
/// NOT MODELLED (documented): enforcing `WITH CHECK OPTION` on write-through
/// -- the mode is recorded and reported, but this tier refuses writes through
/// a view outright, which is the only place the check would apply.
#[derive(Clone, Debug)]
pub struct ViewDef {
    /// The view name as written, for `SHOW CREATE VIEW`.
    pub name: String,
    /// The output columns, resolved when the view was created. The names are
    /// the explicit `CREATE VIEW v (...)` list when one was written.
    pub columns: Vec<(String, FieldType)>,
    /// The view's `SELECT`, canonicalized as Go stores it: every field
    /// explicitly aliased, every table reference schema-qualified.
    pub select_sql: String,
    /// The definer's user name (empty when the session has no user, which is
    /// this tier's only case).
    pub definer_user: String,
    /// The definer's host name.
    pub definer_host: String,
    /// The `ALGORITHM` as written, defaulting to `UNDEFINED`.
    pub algorithm: String,
    /// The `SQL SECURITY` mode as written, defaulting to `DEFINER`.
    pub security: String,
    /// The `WITH CHECK OPTION` mode, `CASCADED` unless `LOCAL` was written.
    /// Go records it on every view, written or not, and reports it as
    /// `information_schema.views.CHECK_OPTION`; `SHOW CREATE VIEW` never
    /// prints it.
    pub check_option: String,
}

/// A catalog table's backing store.
#[derive(Clone, Debug)]
pub enum TableEntry {
    /// A plain value matrix (the original mock backing).
    Mem(MemTable),
    /// Rows stored as real TiKV-format bytes (see [`crate::kv_table`]).
    Kv(KvTable),
    /// A view: a stored `SELECT` rather than stored rows.
    View(ViewDef),
}

impl TableEntry {
    /// The table's columns as `(name, type)` in schema order.
    pub(crate) fn column_list(&self) -> Vec<(String, FieldType)> {
        match self {
            TableEntry::Mem(mem) => mem.columns.clone(),
            TableEntry::Kv(kv) => kv
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.field_type.clone()))
                .collect(),
            TableEntry::View(view) => view.columns.clone(),
        }
    }

    /// The table's column names in schema order, for the callers outside
    /// this crate that resolve a written column name against the table
    /// (`GRANT SELECT (a) ON db.t`).
    #[must_use]
    pub fn column_names(&self) -> Vec<String> {
        self.column_list()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }

    /// Whether this entry is a view, which decides which of MySQL's two
    /// object kinds a statement is allowed to name.
    #[must_use]
    pub fn is_view(&self) -> bool {
        matches!(self, TableEntry::View(_))
    }
}

impl Catalog {
    /// Registers a matrix-backed `table` in the default database.
    pub fn register(&mut self, name: &str, table: MemTable) {
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Mem(table));
    }

    /// Registers a TiKV-format-byte-backed `table` in the default database.
    pub fn register_kv(&mut self, name: &str, table: KvTable) {
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Kv(table));
    }

    /// Registers `table` in `database`, which must exist.
    fn register_in(&mut self, database: &str, name: &str, table: TableEntry) {
        self.version += 1;
        if let Some(database) = self.databases.get_mut(&database.to_lowercase()) {
            database.tables.insert(name.to_lowercase(), table);
        }
    }

    /// Every database name, sorted, with `information_schema` first when it
    /// exists -- Go's `fetchShowDatabases` ordering.
    #[must_use]
    pub fn database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .databases
            .values()
            .map(|database| database.name.clone())
            .collect();
        names.sort();
        if let Some(position) = names
            .iter()
            .position(|name| name.eq_ignore_ascii_case("information_schema"))
        {
            let front = names.remove(position);
            names.insert(0, front);
        }
        names
    }

    /// Every table name in `database`, sorted as Go's `fetchShowTables` sorts
    /// them. `None` when the database does not exist.
    #[must_use]
    pub fn table_names(&self, database: &str) -> Option<Vec<String>> {
        let database = self.databases.get(&database.to_lowercase())?;
        let mut names: Vec<String> = database.tables.keys().cloned().collect();
        names.sort();
        Some(names)
    }

    /// Whether `database` exists (Go `is.SchemaExists`).
    #[must_use]
    pub fn has_database(&self, database: &str) -> bool {
        self.databases.contains_key(&database.to_lowercase())
    }

    /// Creates `database`, reporting whether it was new. Go raises
    /// `ErrDBCreateExists` (1007) unless `IF NOT EXISTS` was written.
    pub fn create_database(&mut self, database: &str) -> bool {
        self.version += 1;
        let key = database.to_lowercase();
        if self.databases.contains_key(&key) {
            return false;
        }
        self.databases.insert(
            key,
            Database {
                name: database.to_owned(),
                tables: HashMap::new(),
            },
        );
        true
    }

    /// Moves a table to a new schema and name, which is what RENAME does.
    /// Returns `false` when the source does not exist.
    pub fn rename_table(
        &mut self,
        from_database: &str,
        from_name: &str,
        to_database: &str,
        to_name: &str,
    ) -> bool {
        self.version += 1;
        let Some(source) = self
            .databases
            .get_mut(&from_database.to_lowercase())
            .and_then(|database| database.tables.remove(&from_name.to_lowercase()))
        else {
            return false;
        };
        // The table carries its own name for duplicate-key messages.
        let mut source = source;
        if let TableEntry::Kv(table) = &mut source {
            table.set_name(to_name);
        }
        if let Some(database) = self.databases.get_mut(&to_database.to_lowercase()) {
            database.tables.insert(to_name.to_lowercase(), source);
        }
        true
    }

    /// Drops one table, reporting whether it existed.
    pub fn drop_table_in(&mut self, database: &str, name: &str) -> bool {
        self.version += 1;
        match self.databases.get_mut(&database.to_lowercase()) {
            Some(database) => database.tables.remove(&name.to_lowercase()).is_some(),
            None => false,
        }
    }

    /// Drops `database` and its tables, reporting whether it existed. Go
    /// raises `ErrDBDropExists` (1008) unless `IF EXISTS` was written.
    pub fn drop_database(&mut self, database: &str) -> bool {
        self.version += 1;
        self.databases.remove(&database.to_lowercase()).is_some()
    }

    /// Resolves a table in `database`.
    pub(crate) fn get_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.databases
            .get(&database.to_lowercase())?
            .tables
            .get(&name.to_lowercase())
    }

    fn get(&self, name: &str) -> Option<&TableEntry> {
        self.get_in(DEFAULT_DATABASE, name)
    }

    /// A mutable handle on a table of `database`, for the write paths.
    ///
    /// Taking it bumps [`Catalog::version`], which is what a transaction's
    /// conflict check observes. The count is deliberately over-approximate:
    /// every write path goes through here, so a statement that ends up
    /// changing nothing still bumps it. That can refuse a commit Go would
    /// allow, never the reverse.
    fn get_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.version += 1;
        self.databases
            .get_mut(&database.to_ascii_lowercase())?
            .tables
            .get_mut(&name.to_ascii_lowercase())
    }

    /// The catalog's mutation counter.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// A mutable table of `database`, for the schema-changing statements.
    pub fn table_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.get_mut_in(database, name)
    }

    /// A table of `database`, for the metadata statements.
    #[must_use]
    pub fn table_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.get_in(database, name)
    }

    /// A table of the default database, for tests that inspect the entry.
    #[must_use]
    pub fn get_table_for_test(&self, name: &str) -> Option<&TableEntry> {
        self.get(name)
    }

    /// Whether `database` holds a table called `name`.
    #[must_use]
    pub fn contains_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some()
    }

    /// Registers a matrix-backed table in `database`, creating the schema
    /// when it does not exist. Used to materialize a virtual table before
    /// running an ordinary plan over it.
    pub fn register_mem_in(&mut self, database: &str, name: &str, table: MemTable) {
        let key = database.to_lowercase();
        self.databases.entry(key).or_insert_with(|| Database {
            name: database.to_owned(),
            tables: HashMap::new(),
        });
        self.register_in(database, name, TableEntry::Mem(table));
    }

    /// Registers a TiKV-format-byte-backed table in `database`.
    pub fn register_kv_in(&mut self, database: &str, name: &str, table: KvTable) {
        self.register_in(database, name, TableEntry::Kv(table));
    }

    /// Registers a view in `database`, replacing whatever the name held --
    /// which is what `CREATE OR REPLACE VIEW` means.
    pub fn register_view_in(&mut self, database: &str, name: &str, view: ViewDef) {
        self.register_in(database, name, TableEntry::View(view));
    }

    /// Whether `name` in `database` is a view.
    #[must_use]
    pub fn is_view_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some_and(TableEntry::is_view)
    }

    /// Whether a table with `name` exists in the default database.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Allocates the next table id (a monotone counter standing in for the
    /// global autoid allocator, like KvTable's handle counter).
    pub fn allocate_table_id(&mut self) -> i64 {
        self.next_table_id += 1;
        self.next_table_id
    }
}

/// Resolves unqualified/`t.`-qualified column names against one table's schema
/// (case-insensitive, as in MySQL).
struct TableResolver<'a> {
    table_name: &'a str,
    columns: &'a [(String, FieldType)],
}

impl ColumnResolver for TableResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] => (Some(table), name),
            // db.t.a qualification waits on a multi-schema catalog.
            _ => return None,
        };
        if let Some(q) = qualifier {
            if !q.eq_ignore_ascii_case(self.table_name) {
                return None;
            }
        }
        self.columns
            .iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|i| (i, self.columns[i].1.clone(), (i + 1) as i64))
    }
}

mod access;
mod agg_select;
mod dml;
mod errors;
mod from;
mod only_full_group_by;

// Re-exported flat, so every caller inside and outside this module keeps
// naming these as `driver::…` exactly as before the split.
pub(crate) use access::*;
pub(crate) use agg_select::*;
pub use dml::*;
pub(crate) use from::*;

pub use errors::{DriverError, MysqlError, SchemaErrorKind, TxnErrorKind, VarErrorKind};

const INIT_CAP: usize = 1;
const MAX_CHUNK_SIZE: usize = 1024;

/// Parses and runs a `FROM`-less `SELECT`, returning its rows as `Datum`s.
pub fn run_select(sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_on(sql, &Catalog::default(), &crate::StmtContext::for_query())
}

/// Parses and runs a single-table (or `FROM`-less) `SELECT` against `catalog`,
/// returning its rows as `Datum`s.
pub fn run_select_on(
    sql: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_meta_on(sql, catalog, ctx).map(|(_, rows)| rows)
}

/// A `SELECT` result with metadata: the output columns as `(name, type)`, then
/// the rows.
pub type SelectMeta = (Vec<(String, FieldType)>, Vec<Vec<Datum>>);

/// Like [`run_select_on`], but also returns the result-column metadata the
/// wire protocol needs: one `(name, type)` per output column.
///
/// Naming follows Go's result-field resolution in spirit, simplified for the
/// seed driver: an `AS` alias wins; a plain column reference uses the column's
/// own name; any other expression uses its restored text (Go's
/// `RestoreString`); `*` expands to the table's column names.
pub fn run_select_meta_on(
    sql: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_meta_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_select_meta_on`] resolving unqualified names in `current_db`.
pub fn run_select_meta_in(
    sql: &str,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let select = match &stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select,
            QueryStmt::SetOpr(set_opr) => {
                return run_set_opr_stmt(set_opr, catalog, current_db, ctx)
            }
        },
        _ => return Err(DriverError::Unsupported("only SELECT is supported")),
    };
    run_select_stmt(select, catalog, current_db, ctx)
}

/// Runs a set-operation statement: `UNION`, `EXCEPT` or `INTERSECT`.
///
/// Go plans the terms left to right and folds each into the accumulated
/// result (`buildSetOpr`), which is what this does over materialized rows.
/// The distinct forms deduplicate, the `ALL` forms keep multiplicity, and a
/// statement-level `ORDER BY`/`LIMIT` applies to the whole result rather than
/// to the last term.
///
/// Row order is unspecified for the deduplicating forms -- TiDB returns them
/// in hash order -- so only `UNION ALL` and an explicit `ORDER BY` have an
/// order worth relying on.
///
/// DEFERRED (documented): pushing the work into executors instead of
/// materializing each term, and the type unification Go performs across terms
/// (the column metadata here comes from the first term).
pub fn run_set_opr_stmt(
    stmt: &tidb_ast::SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    // A CTE prefix belongs to the whole statement, so it is materialized once
    // and every term sees it.
    let with_catalog;
    let catalog = match &stmt.with {
        Some(with) => {
            with_catalog = materialize_ctes(with, catalog, current_db, ctx)?;
            &with_catalog
        }
        None => catalog,
    };

    let mut columns: Option<Vec<(String, FieldType)>> = None;
    let mut accumulated: Vec<Vec<Datum>> = Vec::new();
    for (index, term) in stmt.terms.iter().enumerate() {
        let (term_columns, term_rows) = run_set_opr_term(term, catalog, current_db, ctx)?;
        match &mut columns {
            None => {
                columns = Some(term_columns);
                accumulated = term_rows;
            }
            Some(existing) => {
                // Go raises ErrWrongNumberOfColumnsInSelect for a term whose
                // width differs.
                if term_columns.len() != existing.len() {
                    return Err(DriverError::WrongNumberOfColumnsInSelect);
                }
                let Some(op) = term.op else {
                    return Err(DriverError::Unsupported(
                        "a set-operation term after the first needs an operator",
                    ));
                };
                accumulated = combine_set_opr(op, accumulated, term_rows)?;
            }
        }
        debug_assert!(index == 0 || columns.is_some());
    }
    let columns = columns.ok_or(DriverError::Unsupported("an empty set operation"))?;

    // The statement-level ORDER BY and LIMIT apply to the folded result.
    if !stmt.order_by.is_empty() {
        sort_rows_by_output(&mut accumulated, &columns, &stmt.order_by)?;
    }
    if let Some(limit) = &stmt.limit {
        let count = eval_limit_bound(&limit.count)? as usize;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)? as usize,
            None => 0,
        };
        accumulated = accumulated.into_iter().skip(offset).take(count).collect();
    }
    Ok((columns, accumulated))
}

/// One term of a set operation, which is a `SELECT` or a nested set operation.
fn run_set_opr_term(
    term: &tidb_ast::SetOprTerm,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    match &term.body {
        tidb_ast::SetOprTermBody::Select(select) => {
            run_select_stmt(select, catalog, current_db, ctx)
        }
        tidb_ast::SetOprTermBody::Nested(nested) => {
            run_set_opr_stmt(nested, catalog, current_db, ctx)
        }
    }
}

/// Folds one term into the accumulated rows.
fn combine_set_opr(
    op: tidb_ast::SetOp,
    left: Vec<Vec<Datum>>,
    right: Vec<Vec<Datum>>,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    use tidb_ast::SetOp;
    Ok(match op {
        SetOp::Union { all: true } => {
            let mut rows = left;
            rows.extend(right);
            rows
        }
        SetOp::Union { all: false } => {
            let mut rows = left;
            rows.extend(right);
            dedup_rows(rows)?
        }
        SetOp::Except { all } => {
            let mut remaining = row_counts(&right)?;
            let mut rows = Vec::new();
            for row in left {
                let key = row_key(&row)?;
                match remaining.get_mut(&key) {
                    // EXCEPT ALL removes one occurrence per matching right row.
                    Some(count) if *count > 0 && all => *count -= 1,
                    Some(count) if *count > 0 => {}
                    _ => rows.push(row),
                }
            }
            if all {
                rows
            } else {
                dedup_rows(rows)?
            }
        }
        SetOp::Intersect { all } => {
            let mut available = row_counts(&right)?;
            let mut rows = Vec::new();
            for row in left {
                let key = row_key(&row)?;
                if let Some(count) = available.get_mut(&key) {
                    if *count > 0 {
                        if all {
                            *count -= 1;
                        }
                        rows.push(row);
                    }
                }
            }
            if all {
                rows
            } else {
                dedup_rows(rows)?
            }
        }
    })
}

/// The key a row is compared by, which is the codec encoding its datums use
/// for grouping elsewhere.
fn row_key(row: &[Datum]) -> Result<Vec<u8>, DriverError> {
    let mut key = Vec::new();
    for value in row {
        key.extend_from_slice(
            &value
                .to_hash_key()
                .map_err(|_| DriverError::Unsupported("this datum kind cannot be deduplicated"))?,
        );
        key.push(0xff);
    }
    Ok(key)
}

/// How many times each row appears.
fn row_counts(rows: &[Vec<Datum>]) -> Result<HashMap<Vec<u8>, usize>, DriverError> {
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    for row in rows {
        *counts.entry(row_key(row)?).or_insert(0) += 1;
    }
    Ok(counts)
}

/// Keeps the first occurrence of each distinct row.
fn dedup_rows(rows: Vec<Vec<Datum>>) -> Result<Vec<Vec<Datum>>, DriverError> {
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if seen.insert(row_key(&row)?) {
            out.push(row);
        }
    }
    Ok(out)
}

/// Sorts the folded rows by a statement-level `ORDER BY`, whose items name
/// output columns rather than any term's source columns.
fn sort_rows_by_output(
    rows: &mut [Vec<Datum>],
    columns: &[(String, FieldType)],
    order_by: &[tidb_ast::OrderItem],
) -> Result<(), DriverError> {
    let mut keys = Vec::with_capacity(order_by.len());
    for item in order_by {
        let index = match &item.expr {
            tidb_ast::Expr::Column(path) => {
                let name = path
                    .last()
                    .ok_or(DriverError::Unsupported("empty ORDER BY column"))?;
                columns
                    .iter()
                    .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                    .ok_or(DriverError::Unsupported(
                        "a set operation's ORDER BY must name an output column",
                    ))?
            }
            // MySQL also allows ordering by output position.
            tidb_ast::Expr::Int(text) => {
                let position: usize = text
                    .parse()
                    .map_err(|_| DriverError::Unsupported("bad ORDER BY position"))?;
                if position == 0 || position > columns.len() {
                    return Err(DriverError::Unsupported("ORDER BY position out of range"));
                }
                position - 1
            }
            _ => {
                return Err(DriverError::Unsupported(
                    "a set operation's ORDER BY must name an output column",
                ))
            }
        };
        keys.push((index, item.desc));
    }
    let mut failure = None;
    rows.sort_by(|left, right| {
        for (index, desc) in &keys {
            let ordering = match tidb_expr::compare_datums(&left[*index], &right[*index]) {
                Ok(ordering) => ordering,
                Err(error) => {
                    failure = Some(error);
                    std::cmp::Ordering::Equal
                }
            };
            if ordering != std::cmp::Ordering::Equal {
                return if *desc { ordering.reverse() } else { ordering };
            }
        }
        std::cmp::Ordering::Equal
    });
    match failure {
        Some(error) => Err(DriverError::Exec(ExecError::Eval(error))),
        None => Ok(()),
    }
}

/// Materializes a `WITH` clause's CTEs into `catalog`, so the query that
/// follows resolves them like ordinary tables.
///
/// Go plans a non-recursive CTE as its own subtree the outer query reads from
/// (`buildWith`), and a later CTE may reference an earlier one; materializing
/// them in written order gives that.
///
/// DEFERRED (documented): `WITH RECURSIVE`, which needs the iterate-to-fixpoint
/// executor, and is rejected rather than run as if it were non-recursive --
/// that would silently return only the seed rows.
fn materialize_ctes(
    with: &tidb_ast::WithClause,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Catalog, DriverError> {
    if with.recursive {
        return Err(DriverError::Unsupported(
            "WITH RECURSIVE is not supported yet",
        ));
    }
    // The scratch catalog carries the real tables too, since the CTE bodies
    // and the outer query both read them.
    let mut scratch = catalog.clone();
    for cte in &with.ctes {
        let tidb_ast::QueryStmt::Select(select) = &*cte.query else {
            return Err(DriverError::Unsupported(
                "a set-operation CTE is not supported yet",
            ));
        };
        // Each CTE sees the ones already materialized, which is what lets a
        // later one reference an earlier one.
        let (mut columns, rows) = run_select_stmt(select, &scratch, current_db, ctx)?;
        if !cte.columns.is_empty() {
            if cte.columns.len() != columns.len() {
                return Err(DriverError::Unsupported(
                    "the CTE column list does not match its query's columns",
                ));
            }
            for (column, name) in columns.iter_mut().zip(&cte.columns) {
                column.0 = name.clone();
            }
        }
        scratch.register_mem_in(current_db, &cte.name, MemTable { columns, rows });
    }
    Ok(scratch)
}

/// Runs a `QueryStmt` of either shape against the catalog: the same dispatch
/// [`build_derived_source`] makes over a derived table's subquery, factored
/// out so the lateral-over-set-operation path can share it.
pub(crate) fn run_query_stmt(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    match query {
        QueryStmt::Select(select) => run_select_stmt(select, catalog, current_db, ctx),
        QueryStmt::SetOpr(set_opr) => run_set_opr_stmt(set_opr, catalog, current_db, ctx),
    }
}

/// Runs one parsed `SELECT` against the catalog, for a caller that has
/// already rewritten the statement (session-variable binding, for instance)
/// and must not go back through SQL text.
pub fn run_select_meta_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_stmt(select, catalog, current_db, ctx)
}

/// The name a source operator's `access object` prints: the alias the FROM
/// clause gave the table, which is what Go prints too.
fn source_table_name<'a>(scope: &'a FromScope, table: &'a str) -> &'a str {
    match scope.tables.first() {
        Some(first) => &first.name,
        None => table,
    }
}

/// Runs one parsed `SELECT` against the catalog.
fn run_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_traced(select, catalog, current_db, ctx, None)
}

/// [`run_select_stmt`], recording the plan it builds into `trace`.
///
/// This is the one control flow that decides a `SELECT`'s shape, so it is
/// also the only place that describes one: each site that commits to an
/// executor records the matching node (see [`crate::plan_trace`]), and in
/// `EXPLAIN ANALYZE` mode the executor is metered so the node's `actRows` is
/// the count that operator really produced. A plan-only trace stops before
/// the drain below, so plain `EXPLAIN` yields no result row.
pub(crate) fn run_select_traced(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<SelectMeta, DriverError> {
    // The statement as written, which the plan text is rendered from: the
    // rewrites below (CTE materialization, subquery folding, window
    // hoisting) change what is EXECUTED, not what the user asked for.
    let traced_select = select;
    // A WITH clause's CTEs are materialized first, then the query runs against
    // a catalog that contains them.
    let with_catalog;
    let catalog = match &select.with {
        Some(with) => {
            with_catalog = materialize_ctes(with, catalog, current_db, ctx)?;
            &with_catalog
        }
        None => catalog,
    };
    // Uncorrelated subqueries are evaluated now and folded into literals, so
    // everything below plans against ordinary expressions (Go's
    // handleScalarSubquery for the non-Apply case).
    let folded;
    let select = if select_has_uncorrelated_subquery(select, catalog, current_db, ctx) {
        let outer = select_outer_scope(select, catalog, current_db, ctx);
        folded = fold_select_subqueries(select, &outer, catalog, current_db, ctx)?;
        &folded
    } else {
        select
    };

    // Resolve FROM: none -> table-dual; otherwise the (possibly joined) tables.
    let (mut from_source, mut scope): (Option<Box<dyn Executor>>, FromScope) = match &select.from {
        None => {
            if let Some(trace) = trace.as_deref_mut() {
                trace.table_dual();
            }
            (None, FromScope::default())
        }
        Some(join) => {
            let (exec, scope) = build_join(
                join,
                catalog,
                current_db,
                ctx,
                trace.as_deref_mut(),
                Some(select),
            )?;
            (Some(exec), scope)
        }
    };

    // The access-path decision and the work handed down to it live in
    // `driver::access`; `index_order` is set when the committed source emits
    // rows in an index's order, which is what lets a `LIMIT` under a matching
    // `ORDER BY` stop the scan early.
    let index_order = commit_fast_path_source(
        select,
        catalog,
        current_db,
        &scope,
        &mut from_source,
        trace.as_deref_mut(),
    )?;
    // Column pruning: over a single base-table scan the fast paths left
    // alone, narrow the scan -- and with it the scope -- to the columns the
    // statement actually reads.
    prune_scan_columns(select, &mut scope, &mut from_source);

    // The column resolver for this query's scope.
    let resolver = ScopeResolver { scope: &scope };

    // GROUPING() reads which grouping set produced a row, so it means nothing
    // without WITH ROLLUP: Go rejects it with ErrInvalidGroupFuncUse (1111),
    // whether or not the query groups at all.
    if !select.rollup && select_has_grouping(select) {
        return Err(DriverError::InvalidGroupFuncUse);
    }

    // A window function outside the select list / ORDER BY is Go's
    // ErrWindowInvalidWindowFuncUse (3593), whichever path runs below.
    crate::window::reject_windows_outside_select_list(select)?;

    // Aggregate path: GROUP BY, or any select field that is an aggregate call.
    let is_aggregate = !select.group_by.is_empty()
        || select.fields.fields().iter().any(|f| {
            matches!(
                f,
                SelectField::Expr {
                    expr: tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. },
                    ..
                }
            )
        });
    if is_aggregate {
        return run_aggregate_select(
            select,
            traced_select,
            from_source,
            &resolver,
            catalog,
            current_db,
            ctx,
            trace,
        );
    }

    // Source: the table rows (matrix- or TiKV-byte-backed), or one virtual row
    // from a table-dual.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match from_source {
        Some(exec) => {
            let schema = exec.schema().clone();
            (exec, schema)
        }
        None => {
            let exec: Box<dyn Executor> = Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            ));
            let exec = match trace.as_deref_mut() {
                Some(trace) => trace.meter(exec),
                None => exec,
            };
            (exec, Schema::new(vec![]))
        }
    };
    // The plan text quotes the statement as written, against the FROM scope
    // the driver just built.
    let qualify = Qualifier {
        db: current_db,
        scope: &scope,
    };

    // Optional WHERE: a selection over the source rows. A correlated
    // subquery in the predicate first becomes an Apply below the selection,
    // appending the column the rewritten predicate reads (Go's plan shape).
    // The scope the rows above the WHERE have: the FROM tables, plus the
    // column a correlated WHERE subquery's Apply appends.
    let mut current_scope = scope.clone();
    // Predicate push-down: over a single base table, offer the source the
    // conjuncts it can apply itself; only the residual needs a `Selection`.
    let executed_where =
        negotiate_scan_filter(select, &scope, &mut source, ctx, trace.as_deref_mut());
    // LIMIT push-down: offer the source the row cap, when nothing between it
    // and the `LimitExec` can add, drop or reorder a row.
    offer_scan_limit(
        select,
        executed_where.as_ref(),
        index_order.as_ref(),
        &resolver,
        &mut source,
    );

    // A `WHERE` whose conjuncts all moved into the scan still records its
    // `Selection`, over the predicate as written, and meters the filtered
    // rows the scan now emits.
    if executed_where.is_none() && select.where_clause.is_some() {
        if let Some(trace) = trace.as_deref_mut() {
            if let Some(written) = &traced_select.where_clause {
                trace.selection(written, &qualify);
                source = trace.meter(source);
            }
        }
    }
    if let Some(predicate) = &executed_where {
        let mut correlated = None;
        let appended = scope.width();
        let predicate = extract_correlated_subquery(
            predicate,
            &scope,
            catalog,
            current_db,
            appended,
            &mut correlated,
            ctx,
        )?;
        let (predicate_resolver, predicate_scope);
        let mut source_schema = source_schema;
        if let Some(correlated) = correlated {
            // The Apply's schema is the source's columns plus the subquery's.
            let mut applied = scope.clone();
            let mut value_type = FieldType::new(FieldTypeCode::LongLong);
            if matches!(correlated.kind, SubqueryKind::Scalar) {
                value_type = subquery_result_type(&correlated, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            }
            applied.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{appended}"), value_type)],
                offset: appended,
                determinants: Vec::new(),
            });
            let columns: Vec<Column> = applied
                .column_list()
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let apply_schema = Schema::new(columns);
            let inner_scope = scope.clone();
            // The apply callback outlives this borrow of the catalog, so it
            // owns a snapshot (see ApplyExec::new).
            let inner_catalog = catalog.clone();
            let inner_db = current_db.to_owned();
            // The statement context is a handle, so the callback shares the
            // one warning buffer the statement reports.
            let inner_ctx = ctx.clone();
            let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
                run_correlated_subquery(
                    &correlated,
                    values,
                    &inner_scope,
                    &inner_catalog,
                    &inner_db,
                    &inner_ctx,
                )
                .map_err(|e| match e {
                    DriverError::Exec(exec) => exec,
                    DriverError::SubqueryReturnsMoreThanOneRow => {
                        ExecError::SubqueryReturnsMoreThanOneRow
                    }
                    other => ExecError::Unsupported(driver_error_text(&other)),
                })
            });
            source = Box::new(crate::apply::ApplyExec::new(
                ExecutorMeta::new(apply_schema.clone(), 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
            ));
            source_schema = apply_schema;
            current_scope = applied;
            predicate_scope = current_scope.clone();
        } else {
            predicate_scope = scope.clone();
        }
        predicate_resolver = ScopeResolver {
            scope: &predicate_scope,
        };
        let pred = rewrite_expr_resolved(&predicate, &predicate_resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        source = Box::new(SelectionExec::new(
            ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            source,
            ctx.clone(),
        ));
        if let Some(trace) = trace.as_deref_mut() {
            // An Apply below this selection (a correlated subquery in the
            // WHERE) adds an executor the recorder has never printed, so it
            // stays out of the trace rather than changing the shape EXPLAIN
            // reports.
            if let Some(written) = &traced_select.where_clause {
                trace.selection(written, &qualify);
                source = trace.meter(source);
            }
        }
    }

    // Window functions: the source rows are materialized here, each window
    // call is computed over them (see `crate::window`), and its values are
    // appended as one synthetic source column per call. Every `Expr::Window`
    // in the select list / ORDER BY is then rewritten to read that column, so
    // everything below -- projection, outer ORDER BY, DISTINCT, LIMIT -- runs
    // unchanged, and the outer ORDER BY sorts the already-computed values.
    let window_rewritten;
    let select = if crate::window::select_has_window(select) {
        let calls = crate::window::collect_window_calls(select)?;
        let source_types: Vec<FieldType> = current_scope
            .column_list()
            .into_iter()
            .map(|(_, field_type)| field_type)
            .collect();
        let rows = drain_executor_rows(source, &source_types)?;
        let (rows, scope_with_windows) =
            crate::window::compute_windows(&calls, rows, &current_scope, ctx)?;
        let columns: Vec<Column> = scope_with_windows
            .column_list()
            .iter()
            .enumerate()
            .map(|(i, (_, ft))| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        source = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(Schema::new(columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
            rows,
        ));
        current_scope = scope_with_windows;
        window_rewritten = crate::window::rewrite_windows(select, &calls);
        &window_rewritten
    } else {
        select
    };

    // A correlated subquery in the SELECT list becomes an Apply above the
    // WHERE's selection, appending the column the rewritten field reads --
    // the same shape the WHERE path builds, and Go's plan for
    // `handleScalarSubquery` when the subquery cannot be folded. It sits
    // ABOVE the filter, so the inner query runs only for the rows the WHERE
    // kept, as Go's plan does.
    let mut projected: Vec<(SelectField, Option<String>)> = Vec::new();
    for field in select.fields.fields() {
        let SelectField::Expr { expr, alias } = field else {
            projected.push((field.clone(), None));
            continue;
        };
        let name = match (alias, expr) {
            (Some(alias), _) => alias.clone(),
            (None, tidb_ast::Expr::Column(path)) => {
                path.last().cloned().unwrap_or_else(|| expr.restore())
            }
            (None, _) => expr.restore(),
        };
        let mut correlated = None;
        let appended = current_scope.width();
        let rewritten = extract_correlated_subquery(
            expr,
            &current_scope,
            catalog,
            current_db,
            appended,
            &mut correlated,
            ctx,
        )?;
        if let Some(correlated) = correlated {
            let mut value_type = FieldType::new(FieldTypeCode::LongLong);
            if matches!(correlated.kind, SubqueryKind::Scalar) {
                value_type = subquery_result_type(&correlated, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            }
            let inner_scope = current_scope.clone();
            current_scope.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{appended}"), value_type)],
                offset: appended,
                determinants: Vec::new(),
            });
            let columns: Vec<Column> = current_scope
                .column_list()
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let apply_schema = Schema::new(columns);
            // The callback outlives this borrow of the catalog, so it owns a
            // snapshot (see ApplyExec::new); the context is a handle, so the
            // inner query's warnings reach the statement's one buffer.
            let inner_catalog = catalog.clone();
            let inner_db = current_db.to_owned();
            let inner_ctx = ctx.clone();
            let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
                run_correlated_subquery(
                    &correlated,
                    values,
                    &inner_scope,
                    &inner_catalog,
                    &inner_db,
                    &inner_ctx,
                )
                .map_err(|e| match e {
                    DriverError::Exec(exec) => exec,
                    DriverError::SubqueryReturnsMoreThanOneRow => {
                        ExecError::SubqueryReturnsMoreThanOneRow
                    }
                    other => ExecError::Unsupported(driver_error_text(&other)),
                })
            });
            source = Box::new(crate::apply::ApplyExec::new(
                ExecutorMeta::new(apply_schema, 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
            ));
        }
        projected.push((
            SelectField::Expr {
                expr: rewritten,
                alias: alias.clone(),
            },
            Some(name),
        ));
    }
    let projected_fields: Vec<SelectField> =
        projected.iter().map(|(field, _)| field.clone()).collect();
    let resolver = ScopeResolver {
        scope: &current_scope,
    };

    // Rewrite each projected field into an evaluable expression; `*` expands to
    // every table column in order (Go's unfoldWildStar).
    let mut exprs: Vec<Expression> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for (field, name) in &projected {
        match field {
            SelectField::Expr { expr, .. } => {
                let rewritten = rewrite_expr_resolved(expr, &resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                exprs.push(rewritten);
                names.push(name.clone().unwrap_or_default());
            }
            SelectField::Wildcard(qualifier) => {
                if scope.tables.is_empty() {
                    return Err(DriverError::Unsupported(
                        "`*` is not supported in a FROM-less SELECT",
                    ));
                }
                // `*` expands to every column of every FROM table in order,
                // `t.*` to one table's (Go's unfoldWildStar).
                let selected: Vec<&FromTable> = match qualifier.last() {
                    None => scope.tables.iter().collect(),
                    Some(q) => {
                        let matching: Vec<&FromTable> = scope
                            .tables
                            .iter()
                            .filter(|t| t.name.eq_ignore_ascii_case(q))
                            .collect();
                        if matching.is_empty() {
                            return Err(DriverError::Unsupported(
                                "`t.*` qualifier does not match a FROM table",
                            ));
                        }
                        matching
                    }
                };
                for table in selected {
                    for (i, (name, ft)) in table.columns.iter().enumerate() {
                        let index = table.offset + i;
                        let mut col = Column::new((index + 1) as i64, ft.clone());
                        col.index = index as i64;
                        exprs.push(Expression::Column(col));
                        names.push(name.clone());
                    }
                }
            }
        }
    }

    // Output schema: one column per field, typed by the expression's static type.
    let out_columns: Vec<Column> = exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            let field_type = expr
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            let mut col = Column::new((i + 1) as i64, field_type);
            col.index = i as i64;
            col
        })
        .collect();
    let out_schema = Schema::new(out_columns);
    let ret_types: Vec<FieldType> = out_schema
        .columns
        .iter()
        .map(|c| c.ret_type.clone().expect("output column has a type"))
        .collect();

    // ORDER BY: a sort below the projection, with by-items resolved against
    // the SELECT list first and the SOURCE schema second -- Go's own
    // resolution order, which is why ordering by a column that is not
    // projected still works while an alias shadows one that is.
    if !select.order_by.is_empty() {
        let mut by_items = Vec::with_capacity(select.order_by.len());
        for item in &select.order_by {
            let resolved = substitute_output_aliases(&item.expr, &projected_fields, true)?;
            let expr = rewrite_expr_resolved(&resolved, &resolver).map_err(|e| {
                order_by_column_error(&resolved).unwrap_or(DriverError::Exec(ExecError::Eval(e)))
            })?;
            by_items.push(SortByItem {
                expr,
                desc: item.desc,
            });
        }
        let sort_schema = source.schema().clone();
        source = Box::new(SortExec::new(
            ExecutorMeta::new(sort_schema, 3, INIT_CAP, MAX_CHUNK_SIZE),
            by_items,
            source,
            ctx.clone(),
        ));
        if let Some(trace) = trace.as_deref_mut() {
            trace.sort(&traced_select.order_by, &qualify);
            source = trace.meter(source);
        }
    }

    // Projection of the rewritten fields.
    let mut root: Box<dyn Executor> = Box::new(ProjectionExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        exprs,
        source,
        ctx.clone(),
    ));
    if let Some(trace) = trace.as_deref_mut() {
        trace.projection(traced_select.fields.fields(), &qualify);
        root = trace.meter(root);
    }

    // SELECT DISTINCT: Go `buildDistinct` builds an aggregation grouping by
    // every projected column, with a FIRST_ROW aggregate per column, which is
    // exactly a deduplication. It sits above the projection and below LIMIT.
    if select.distinct {
        root = Box::new(distinct_over(root, &out_schema, ctx));
        if let Some(trace) = trace.as_deref_mut() {
            trace.distinct(traced_select.fields.fields(), &qualify);
            root = trace.meter(root);
        }
    }

    // LIMIT [offset,] count: both bounds must be non-negative integer literals
    // (as in SQL; Go validates the same in the planner).
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        let limit_schema = root.schema().clone();
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(limit_schema, 4, INIT_CAP, MAX_CHUNK_SIZE),
            offset,
            count,
            root,
        ));
        if let Some(trace) = trace.as_deref_mut() {
            trace.limit(offset, count);
            root = trace.meter(root);
        }
    }

    // Plain `EXPLAIN`: the pipeline is built and recorded, then dropped
    // undrained -- no row of the result is ever produced.
    if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
        return Ok((names.into_iter().zip(ret_types).collect(), Vec::new()));
    }

    root.open()?;
    let mut req = root.new_chunk();
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    loop {
        root.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            let values = ret_types
                .iter()
                .enumerate()
                .map(|(c, ft)| row.get_datum(c, ft))
                .collect();
            rows.push(values);
        }
    }
    root.close()?;
    let columns = names.into_iter().zip(ret_types).collect();
    Ok((columns, rows))
}

/// Binds a prepared statement's parameters, replacing every `?` marker with
/// the literal for its execute-time value.
///
/// Go keeps the parsed statement and installs the values on the markers
/// themselves; this tier reaches execution through SQL text, so the markers
/// become literals and the statement is restored. That round trip is exact
/// for every value kind `datum_to_literal` covers, and a byte string that is
/// not UTF-8 becomes a hex literal rather than a lossy conversion.
///
/// Returns the bound SQL, or `ErrWrongParamCount` when the count does not
/// match the markers the statement carries.
pub fn bind_parameters(sql: &str, values: &[Datum]) -> Result<String, DriverError> {
    let mut stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let mut bound = 0usize;
    bind_statement_markers(&mut stmt, values, &mut bound)?;
    if bound != values.len() {
        return Err(DriverError::WrongParamCount);
    }
    Ok(stmt.restore())
}

/// The number of `?` markers a statement carries, which `COM_STMT_PREPARE`
/// reports to the client.
pub fn parameter_count(sql: &str) -> Result<usize, DriverError> {
    let mut stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let mut counted = 0usize;
    // Counting binds nothing: every marker reports itself and stays put.
    count_statement_markers(&mut stmt, &mut counted);
    Ok(counted)
}

/// Walks a statement's expressions, applying `visit` to every marker.
fn walk_statement_markers(stmt: &mut Stmt, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    let walk_expr = walk_expr_markers;
    match stmt {
        Stmt::Query(query) => walk_query_markers(query, visit),
        Stmt::Dml(dml) => match &mut **dml {
            tidb_ast::DmlStmt::Insert(insert) => {
                for row in &mut insert.rows {
                    for value in row {
                        walk_expr(value, visit);
                    }
                }
                for assignment in &mut insert.on_duplicate {
                    walk_expr(&mut assignment.value, visit);
                }
                if let Some(source) = &mut insert.source {
                    walk_query_markers(source, visit);
                }
            }
            tidb_ast::DmlStmt::Update(update) => {
                for assignment in &mut update.assignments {
                    walk_expr(&mut assignment.value, visit);
                }
                if let Some(where_clause) = &mut update.where_clause {
                    walk_expr(where_clause, visit);
                }
            }
            tidb_ast::DmlStmt::Delete(delete) => {
                if let Some(where_clause) = &mut delete.where_clause {
                    walk_expr(where_clause, visit);
                }
            }
            _ => {}
        },
        _ => {}
    }
}

/// The markers inside one query, including its set-operation terms.
fn walk_query_markers(query: &mut tidb_ast::QueryStmt, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    match query {
        tidb_ast::QueryStmt::Select(select) => walk_select_markers(select, visit),
        tidb_ast::QueryStmt::SetOpr(set_opr) => walk_set_opr_markers(set_opr, visit),
    }
}

/// The markers inside one set operation and, recursively, its nested terms.
fn walk_set_opr_markers(
    set_opr: &mut tidb_ast::SetOprStmt,
    visit: &mut dyn FnMut(&mut tidb_ast::Expr),
) {
    for term in &mut set_opr.terms {
        match &mut term.body {
            tidb_ast::SetOprTermBody::Select(select) => walk_select_markers(select, visit),
            tidb_ast::SetOprTermBody::Nested(nested) => walk_set_opr_markers(nested, visit),
        }
    }
}

/// The markers inside one `SELECT`.
fn walk_select_markers(
    select: &mut tidb_ast::SelectStmt,
    visit: &mut dyn FnMut(&mut tidb_ast::Expr),
) {
    for field in select.fields.fields_mut() {
        if let tidb_ast::SelectField::Expr { expr, .. } = field {
            walk_expr_markers(expr, visit);
        }
    }
    if let Some(where_clause) = &mut select.where_clause {
        walk_expr_markers(where_clause, visit);
    }
    if let Some(having) = &mut select.having {
        walk_expr_markers(having, visit);
    }
    for item in &mut select.order_by {
        walk_expr_markers(&mut item.expr, visit);
    }
    for item in &mut select.group_by {
        walk_expr_markers(&mut item.expr, visit);
    }
    if let Some(limit) = &mut select.limit {
        walk_expr_markers(&mut limit.count, visit);
        if let Some(offset) = &mut limit.offset {
            walk_expr_markers(offset, visit);
        }
    }
}

/// The markers inside one expression tree.
fn walk_expr_markers(expr: &mut tidb_ast::Expr, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    use tidb_ast::Expr;
    if matches!(expr, Expr::ParamMarker { .. }) {
        visit(expr);
        return;
    }
    match expr {
        Expr::Paren(inner) | Expr::Unary(_, inner) => walk_expr_markers(inner, visit),
        Expr::Binary(_, left, right) => {
            walk_expr_markers(left, visit);
            walk_expr_markers(right, visit);
        }
        Expr::Func { args, .. } => {
            for arg in args {
                walk_expr_markers(arg, visit);
            }
        }
        Expr::In { expr, list, .. } => {
            walk_expr_markers(expr, visit);
            for item in list {
                walk_expr_markers(item, visit);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            walk_expr_markers(expr, visit);
            walk_expr_markers(low, visit);
            walk_expr_markers(high, visit);
        }
        Expr::Like { expr, pattern, .. } => {
            walk_expr_markers(expr, visit);
            walk_expr_markers(pattern, visit);
        }
        Expr::Is { expr, .. } => walk_expr_markers(expr, visit),
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            if let Some(value) = value {
                walk_expr_markers(value, visit);
            }
            for (condition, result) in when_clauses {
                walk_expr_markers(condition, visit);
                walk_expr_markers(result, visit);
            }
            if let Some(else_clause) = else_clause {
                walk_expr_markers(else_clause, visit);
            }
        }
        Expr::Cast(cast) => walk_expr_markers(&mut cast.expr, visit),
        _ => {}
    }
}

/// Replaces each marker with its value, in the parser's own left-to-right
/// marker order.
fn bind_statement_markers(
    stmt: &mut Stmt,
    values: &[Datum],
    bound: &mut usize,
) -> Result<(), DriverError> {
    let mut failure = None;
    walk_statement_markers(stmt, &mut |expr| {
        let order = match expr {
            tidb_ast::Expr::ParamMarker { order, .. } => *order,
            _ => return,
        };
        match values.get(order) {
            Some(value) => match datum_to_literal(value) {
                Ok(literal) => {
                    *expr = literal;
                    *bound += 1;
                }
                Err(error) => failure = Some(error),
            },
            None => failure = Some(DriverError::WrongParamCount),
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Counts the markers without changing them.
fn count_statement_markers(stmt: &mut Stmt, counted: &mut usize) {
    walk_statement_markers(stmt, &mut |_| *counted += 1);
}

/// Go `havingWindowAndOrderbyExprResolver`: an `ORDER BY` item is resolved
/// against the SELECT list first, so a select alias and an output position
/// both name a projected expression.
///
/// Go rewrites the reference into the projected expression itself, which is
/// what this does -- the sort then runs over the source rows with no plan
/// reshuffle, and an expression BUILT on an alias (`ORDER BY twice + 0`)
/// falls out for free.
///
/// Captured from TiDB: an alias SHADOWS a real column of the same name
/// (`SELECT b AS a FROM t ORDER BY a` sorts by `b`); a bare integer is a
/// 1-based output position, and only at the top level (`ORDER BY twice + 0`
/// is arithmetic, not position 1); an out-of-range position and an unknown
/// name are both `ErrUnknownColumn` naming the `order clause`.
fn substitute_output_aliases(
    expr: &tidb_ast::Expr,
    fields: &[SelectField],
    top_level: bool,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // A bare integer at the top of an ORDER BY item is an output position.
    if top_level {
        if let Some((text, index)) = positional_field_index(expr) {
            let index = index.map_err(|_| unknown_order_column(text))?;
            let projected = fields
                .iter()
                .filter_map(|field| match field {
                    SelectField::Expr { expr, .. } => Some(expr),
                    SelectField::Wildcard(_) => None,
                })
                .nth(index)
                .ok_or_else(|| unknown_order_column(text))?;
            return Ok(projected.clone());
        }
    }
    Ok(match expr {
        // A one-segment name may be a select alias; a qualified one
        // (`t.a`) always addresses the source.
        Expr::Column(path) if path.len() == 1 => {
            let alias = fields.iter().find_map(|field| match field {
                SelectField::Expr {
                    expr,
                    alias: Some(alias),
                } if alias.eq_ignore_ascii_case(&path[0]) => Some(expr),
                _ => None,
            });
            match alias {
                Some(expr) => expr.clone(),
                None => expr.clone(),
            }
        }
        Expr::Paren(inner) => {
            Expr::Paren(Box::new(substitute_output_aliases(inner, fields, false)?))
        }
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_output_aliases(inner, fields, false)?),
        ),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(substitute_output_aliases(left, fields, false)?),
            Box::new(substitute_output_aliases(right, fields, false)?),
        ),
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| substitute_output_aliases(arg, fields, false))
                .collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        other => other.clone(),
    })
}

/// Go `gbyResolver`: a bare integer at the top of a `GROUP BY` item is a
/// 1-based output position resolved against the SELECT list -- the same rule
/// `ORDER BY`'s [`substitute_output_aliases`] applies, but `GROUP BY` has no
/// alias-substitution counterpart (a bare name is resolved by the ordinary
/// column resolver instead), so only the position form needs handling here.
///
/// Captured from TiDB: an out-of-range position is `ErrUnknownColumn` naming
/// the `group statement`; a position landing on an aggregate or
/// window-function select field is `ErrWrongGroupField` ("Can't group on
/// '<name>'"), naming the field's alias if it has one and its written text
/// otherwise.
fn resolve_group_by_position<'a>(
    expr: &'a tidb_ast::Expr,
    fields: &'a [SelectField],
) -> Result<std::borrow::Cow<'a, tidb_ast::Expr>, DriverError> {
    let Some((text, index)) = positional_field_index(expr) else {
        return Ok(std::borrow::Cow::Borrowed(expr));
    };
    let index = index.map_err(|_| unknown_group_position(text))?;
    let (target, alias) = fields
        .iter()
        .filter_map(|field| match field {
            SelectField::Expr { expr, alias } => Some((expr, alias)),
            SelectField::Wildcard(_) => None,
        })
        .nth(index)
        .ok_or_else(|| unknown_group_position(text))?;
    if matches!(
        target,
        tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. }
    ) || !crate::window::windows_in(target).is_empty()
    {
        let name = alias.clone().unwrap_or_else(|| target.restore());
        return Err(DriverError::WrongGroupField(name));
    }
    Ok(std::borrow::Cow::Borrowed(target))
}

/// Why a bare-integer clause item is not a usable output position.
///
/// The clause decides what this REPORTS: `ORDER BY` and `GROUP BY` raise
/// `ErrUnknownColumn` naming their own clause, and the DML tier refuses the
/// statement outright. The rule itself -- which integers are positions and
/// which index they name -- is the same everywhere, so it lives once in
/// [`positional_field_index`].
pub(crate) enum PositionalError {
    /// The digits do not fit a `usize` (Go's `strconv.ParseUint` failure).
    Malformed,
    /// Position `0`, which MySQL numbers from 1 and so never names a field.
    Zero,
}

/// Go's shared "bare integer is a 1-based output position" rule, as it applies
/// in `ORDER BY`, `GROUP BY` and the DML tier's own `ORDER BY`.
///
/// Returns `None` when `expr` is not a bare integer at all -- the item is then
/// an ordinary expression and every caller falls through to its usual
/// resolution. Otherwise it yields the integer AS WRITTEN (which the callers'
/// errors quote verbatim, as MySQL does) together with the ZERO-based field
/// index it names, or why it names none.
///
/// `TRUE`/`FALSE` are positions too: Go's parser builds them with
/// `ast.NewValueExpr(bool)`, and `types.Datum` has no boolean kind, so they
/// reach the clause as the plain integers `1`/`0` and the position rule sees
/// nothing else. Captured from TiDB: `GROUP BY TRUE` groups by the first
/// select field exactly like `GROUP BY 1`, and `GROUP BY FALSE` reports the
/// same "Unknown column '0' in 'group statement'" `GROUP BY 0` does.
fn positional_field_index(expr: &tidb_ast::Expr) -> Option<(&str, Result<usize, PositionalError>)> {
    let text = match expr {
        tidb_ast::Expr::Int(text) => text.as_str(),
        tidb_ast::Expr::Bool(true) => "1",
        tidb_ast::Expr::Bool(false) => "0",
        _ => return None,
    };
    let index = match text.parse::<usize>() {
        Err(_) => Err(PositionalError::Malformed),
        Ok(0) => Err(PositionalError::Zero),
        Ok(position) => Ok(position - 1),
    };
    Some((text, index))
}

/// Whether a clause item is the bare-integer output position form, without
/// resolving it -- see [`positional_field_index`].
pub(crate) fn is_positional_field(expr: &tidb_ast::Expr) -> bool {
    positional_field_index(expr).is_some()
}

/// Go `ErrUnknownColumn` naming the `group statement`, for a `GROUP BY`
/// position that is zero or past the end of the SELECT list.
fn unknown_group_position(text: &str) -> DriverError {
    DriverError::UnknownColumnInClause {
        column: text.to_owned(),
        clause: "group statement".to_owned(),
    }
}

/// The `ErrUnknownColumn` an unresolvable `ORDER BY` item reports, when the
/// item is a plain name -- anything else keeps the rewriter's own error.
fn order_by_column_error(expr: &tidb_ast::Expr) -> Option<DriverError> {
    match expr {
        tidb_ast::Expr::Column(path) => Some(unknown_order_column(&path.join("."))),
        _ => None,
    }
}

/// Go `ErrUnknownColumn` naming the `order clause`.
fn unknown_order_column(name: &str) -> DriverError {
    DriverError::UnknownColumnInClause {
        column: name.to_owned(),
        clause: "order clause".to_owned(),
    }
}

/// Go `aggregation.NewAggFuncDesc` + `baseFuncDesc.TypeInfer`: the aggregate
/// kind and the result type inferred for its argument.
pub(crate) fn agg_kind_and_type(
    name: &str,
    args: &[Expression],
) -> Result<(AggKind, FieldType), DriverError> {
    // Every aggregate here reads its FIRST argument for type inference;
    // `APPROX_PERCENTILE` is the only one that also reads a second.
    let null = Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::Null,
        FieldType::new(FieldTypeCode::LongLong),
    ));
    let arg = args.first().unwrap_or(&null);
    Ok(match name {
        // Go `typeInfer4Count`: a binary `BIGINT(21)` that never returns NULL
        // -- an empty group (and an empty window frame) counts 0.
        "COUNT" => {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.set_flen(21);
            t.set_decimal(0);
            t.add_flags(
                tidb_datatype::FieldTypeFlags::BINARY | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
            (AggKind::Count, t)
        }
        // Go `typeInfer4Sum`: DOUBLE for a real argument, DECIMAL for every
        // other numeric one -- `SUM` over a BIGINT column is a DECIMAL in
        // MySQL, not a BIGINT (captured: `sum(a)` reports type 246).
        "SUM" => {
            let real = arg
                .static_type()
                .is_some_and(|t| t.eval_type() == tidb_datatype::EvalType::Real);
            let t = if real {
                FieldType::new(FieldTypeCode::Double)
            } else {
                FieldType::new(FieldTypeCode::NewDecimal)
            };
            (AggKind::Sum, t)
        }
        // Go `typeInfer4MaxMin`: the result carries the argument's
        // own type (with NOT NULL dropped, which this seed does not
        // track on result columns).
        "MIN" | "MAX" => {
            let t = arg
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            let kind = if name == "MIN" {
                AggKind::Min
            } else {
                AggKind::Max
            };
            (kind, t)
        }
        // Go `typeInfer4Avg`: DOUBLE for real arguments, otherwise
        // DECIMAL. The decimal scale Go derives from
        // div_precision_increment is display metadata this seed
        // does not set on result columns (documented deferral).
        "AVG" => {
            let code = arg
                .static_type()
                .map_or(FieldTypeCode::NewDecimal, |t| match t.code() {
                    FieldTypeCode::Float | FieldTypeCode::Double => FieldTypeCode::Double,
                    _ => FieldTypeCode::NewDecimal,
                });
            (AggKind::Avg, FieldType::new(code))
        }
        // Go `typeInfer4BitFuncs`: a binary `BIGINT(21)` that never returns
        // NULL -- an empty (or all-NULL) input folds to the operator's
        // identity, not NULL. The column is SIGNED, which is why an all-NULL
        // `BIT_AND` reads back as `-1` (captured from TiDB).
        "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.set_flen(21);
            t.set_decimal(0);
            t.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            let op = match name {
                "BIT_AND" => crate::hash_agg::BitOp::And,
                "BIT_OR" => crate::hash_agg::BitOp::Or,
                _ => crate::hash_agg::BitOp::Xor,
            };
            (AggKind::Bit(op), t)
        }
        // Go `typeInfer4PopOrSamp`: a nullable `DOUBLE(23)` with an
        // unspecified scale, regardless of the argument's own type.
        // The parser canonicalizes `VARIANCE` to `VAR_POP` and
        // `STD`/`STDDEV` to `STDDEV_POP`, so only the four canonical names
        // reach here.
        "VAR_POP" | "VAR_SAMP" | "STDDEV_POP" | "STDDEV_SAMP" => {
            let mut t = FieldType::new(FieldTypeCode::Double);
            t.set_flen(23);
            t.set_decimal(tidb_datatype::UNSPECIFIED_FSP);
            let kind = AggKind::Variance {
                sample: matches!(name, "VAR_SAMP" | "STDDEV_SAMP"),
                sqrt: matches!(name, "STDDEV_POP" | "STDDEV_SAMP"),
            };
            (kind, t)
        }
        // Go `typeInfer4JsonArrayAgg`/`typeInfer4JsonObjectAgg`: a binary
        // JSON column with no written width (captured: type 245, flen -1,
        // decimals -1, the BINARY flag set).
        "JSON_ARRAYAGG" | "JSON_OBJECTAGG" => {
            let mut t = FieldType::new(FieldTypeCode::Json);
            t.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
            // The VALUE argument's own field type -- `JSON_ARRAYAGG`'s first
            // argument, `JSON_OBJECTAGG`'s second -- decides how a
            // BINARY-charset string embeds: Go's `getRealJSONValue` tags the
            // JSON `Opaque` it builds with `ft.GetType()`, the source
            // column's exact MySQL type code (captured: VARBINARY is 15,
            // fixed-length BINARY(n) is 254 and zero-padded to `n`, the
            // TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB family is 249/252/250/251).
            let value_arg = if name == "JSON_ARRAYAGG" {
                args.first()
            } else {
                args.get(1)
            };
            let value_type = value_arg
                .and_then(Expression::static_type)
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::VarString));
            let kind = if name == "JSON_ARRAYAGG" {
                AggKind::JsonArrayAgg { value_type }
            } else {
                // The KEY argument's own field type decides 3144 -- Go:
                // `e.args[0].GetType(sctx).GetCharset() == charset.CharsetBin`
                // -- a STATIC property of the declared argument type, not
                // the evaluated key datum (see `AggKind::JsonObjectAgg`'s own
                // doc for why the datum kind alone is not enough).
                let key_is_binary = args
                    .first()
                    .and_then(Expression::static_type)
                    .is_some_and(FieldType::is_binary_string);
                AggKind::JsonObjectAgg {
                    value_type,
                    key_is_binary,
                }
            };
            (kind, t)
        }
        // Go `typeInfer4ApproxCountDistinct` delegates to `typeInfer4Count`,
        // so the result is COUNT's own NOT NULL binary `BIGINT(21)`.
        "APPROX_COUNT_DISTINCT" => {
            let mut t = FieldType::new(FieldTypeCode::LongLong);
            t.set_flen(21);
            t.set_decimal(0);
            t.add_flags(
                tidb_datatype::FieldTypeFlags::BINARY | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
            (AggKind::ApproxCountDistinct, t)
        }
        // Go `typeInfer4ApproxPercentile`: two arguments, the second a
        // CONSTANT percentage in [1, 100], and a result type read off the
        // first argument.
        "APPROX_PERCENTILE" => {
            let [_, percent_arg] = args else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE should take 2 arguments",
                ));
            };
            let Some(folded) = fold_constant(percent_arg) else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE should take a constant expression as percentage argument",
                ));
            };
            let Some(percent) = constant_eval_int(&folded) else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE: Percentage value cannot be NULL",
                ));
            };
            if percent <= 0 || percent > 100 {
                return Err(DriverError::PercentageOutOfRange(percent));
            }
            let arg_type = arg.static_type().cloned();
            let code = arg_type
                .as_ref()
                .map_or(FieldTypeCode::LongLong, |t| t.code());
            let ret = match code {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong => FieldType::new(FieldTypeCode::LongLong),
                FieldTypeCode::Double | FieldTypeCode::Float => {
                    FieldType::new(FieldTypeCode::Double)
                }
                FieldTypeCode::NewDecimal => {
                    let mut t = FieldType::new(FieldTypeCode::NewDecimal);
                    t.set_flen(MAX_DECIMAL_WIDTH);
                    let scale = arg_type.as_ref().map_or(-1, FieldType::decimal);
                    t.set_decimal(if (0..=MAX_DECIMAL_SCALE).contains(&scale) {
                        scale
                    } else {
                        MAX_DECIMAL_SCALE
                    });
                    t
                }
                FieldTypeCode::Date
                | FieldTypeCode::Datetime
                | FieldTypeCode::NewDate
                | FieldTypeCode::Timestamp => arg_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
                _ => {
                    let mut t = arg_type
                        .clone()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                    t.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                    t
                }
            };
            // `buildApproxPercentile` picks a typed accumulator by the
            // argument's EVAL type -- and `getEvalTypeForApproxPercentile`
            // forces ENUM/SET/BIT to the string domain. Every other eval type
            // (a string column, say) gets Go's `basePercentile`, which
            // appends NULL for every group.
            let eval_type = arg_type.as_ref().map(FieldType::eval_type);
            let ranks = !matches!(
                code,
                FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
            ) && matches!(
                eval_type,
                Some(
                    tidb_datatype::EvalType::Int
                        | tidb_datatype::EvalType::Real
                        | tidb_datatype::EvalType::Decimal
                        | tidb_datatype::EvalType::Datetime
                        | tidb_datatype::EvalType::Timestamp
                        | tidb_datatype::EvalType::Duration
                )
            );
            (AggKind::ApproxPercentile(ranks.then_some(percent)), ret)
        }
        _ => {
            return Err(DriverError::Unsupported(
                "this aggregate function is deferred",
            ))
        }
    })
}

/// Go `mysql.MaxDecimalWidth`, the width `APPROX_PERCENTILE` gives a DECIMAL
/// result.
const MAX_DECIMAL_WIDTH: i64 = 65;
/// Go `mysql.MaxDecimalScale`.
const MAX_DECIMAL_SCALE: i64 = 30;

/// The value of a row-independent expression, or `None` when it reads a
/// column (Go's `ConstLevel() == ConstNone`).
///
/// Go's expression rewriter FOLDS a constant subtree into one `Constant`
/// before the aggregate descriptor inspects it, which is why
/// `APPROX_PERCENTILE(v, -1)` -- a unary minus over a literal, not a literal
/// -- passes Go's constant check. Folding here at the point of use reaches the
/// same answer without a rewriter-wide folding pass.
fn fold_constant(expr: &Expression) -> Option<Datum> {
    match expr {
        Expression::Constant(constant) => Some(constant.value.clone()),
        Expression::Column(_) | Expression::CorrelatedColumn(_) => None,
        Expression::ScalarFunction(function) => {
            if !function.args.iter().all(|arg| fold_constant(arg).is_some()) {
                return None;
            }
            let chunk = {
                let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
                chunk.set_num_virtual_rows(1);
                chunk
            };
            expr.eval(&crate::StmtContext::for_query(), chunk.get_row(0))
                .ok()
        }
    }
}

/// Go `Constant.EvalInt` for a literal percentage argument.
///
/// The tail of Go's `EvalInt` is `dt.GetInt64()`, an UNCONVERTED read of the
/// datum's own int64 field: only an integer (or a string, which takes the
/// `ToInt64` branch above it) yields the number as written. A DECIMAL literal
/// stores nothing in that field, so `APPROX_PERCENTILE(v, 50.5)` reports
/// "Percentage value 0"; a FLOAT literal stores its IEEE-754 bits there, so
/// `APPROX_PERCENTILE(v, 50e0)` reports "Percentage value
/// 4632233691727265792" (both captured from TiDB). `None` is Go's `isNull`.
fn constant_eval_int(value: &Datum) -> Option<i64> {
    match value {
        Datum::Null => None,
        Datum::Int(number) => Some(*number),
        Datum::UInt(number) => Some(*number as i64),
        // Go's `dt.Kind() == KindString` branch, which DOES convert.
        Datum::String(_) | Datum::Bytes(_) => Some(value.to_i64().map_or(0, |result| result.value)),
        Datum::Real(number) | Datum::Float32(number) => Some(number.to_bits() as i64),
        _ => Some(0),
    }
}

/// The aggregation's output columns, addressed by name.
///
/// Go rewrites `HAVING`/`ORDER BY` to reference the aggregation's output
/// schema (`resolveHavingAndOrderBy` + `buildProjection`), so those clauses see
/// the aggregate results rather than the source rows. This resolver is that
/// output schema: a name is a select field's alias or column name, or an
/// aggregate's restored text.
struct AggOutputResolver {
    names: Vec<String>,
    types: Vec<FieldType>,
}

impl ColumnResolver for AggOutputResolver {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?;
        let index = self
            .names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))?;
        Some((index, self.types[index].clone(), (index + 1) as i64))
    }
}

/// Go `havingWindowAndOrderbyExprResolver`: rewrites a `HAVING`/`ORDER BY`
/// expression so every aggregate in it refers to an aggregation output column,
/// appending a hidden aggregate when the select list does not already compute
/// it.
///
/// The substitution is textual in the same sense Go's is structural: an
/// aggregate node becomes a column reference whose name is the aggregate's
/// restored text, which [`AggOutputResolver`] then binds to the output column.
///
/// Only the expression forms the expression rewriter itself supports are
/// walked (literals, parentheses, unary, binary, columns, aggregates); any
/// other form would fail to rewrite anyway and is returned unchanged.
fn substitute_aggregates(
    expr: &tidb_ast::Expr,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_names: &[String],
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // GROUPING() is hoisted the same way an aggregate is: the value is
    // computed by the rollup pass into an output column, and the clause reads
    // that column. A GROUPING() only HAVING or ORDER BY needs becomes a hidden
    // column and is trimmed by the final projection.
    if let Some(args) = grouping_call_args(expr) {
        let display = expr.restore();
        let (name, _) = add_grouping_column(
            args,
            display,
            agg_funcs,
            names,
            types,
            grouping_specs,
            group_by_names,
        )?;
        return Ok(Expr::Column(vec![name]));
    }
    Ok(match expr {
        // A column that HAVING/ORDER BY references but the select list does
        // not project: Go carries it out of the aggregation as a hidden
        // FIRST_ROW column, exactly as it does for a selected group column,
        // whether or not the column is grouped. Whether an UNGROUPED one may
        // be read at all is `only_full_group_by`'s question, asked once at the
        // top of the pipeline over the clauses as written -- this path must
        // not re-decide it from the grouped-name list alone, which knows
        // nothing of the candidate-key dependency that permits
        // `GROUP BY id ORDER BY z` on a primary-keyed table.
        // A hoisted window column is computed ABOVE the aggregation, so it is
        // neither grouped nor aggregated and must be left alone here; it
        // resolves once the window stage has appended it.
        Expr::Column(path)
            if path
                .last()
                .is_some_and(|name| crate::window::is_window_column(name)) =>
        {
            expr.clone()
        }
        Expr::Column(path) => {
            let name = path.last().cloned().unwrap_or_default();
            // `__apply_N` is not a real column: it is the placeholder a
            // correlated subquery's extraction left behind, standing in for
            // the column an Apply appends above the aggregation once the
            // subquery is bound and run. It carries no ONLY_FULL_GROUP_BY
            // obligation of its own, so it passes through untouched.
            if name.starts_with("__apply_") {
                return Ok(expr.clone());
            }
            if names
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&name))
            {
                return Ok(expr.clone());
            }
            let carrier = rewrite_expr_resolved(expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            let ftype = carrier
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            agg_funcs.push(AggFunc {
                kind: AggKind::FirstRow,
                arg: Some(carrier),
                extra_args: Vec::new(),
                distinct: false,
                order_by: Vec::new(),
            });
            names.push(name.clone());
            types.push(ftype);
            Expr::Column(vec![name])
        }
        // GROUP_CONCAT is substituted the same way: the aggregate is hoisted
        // and the field becomes a reference to its output column.
        Expr::Aggregate { .. } | Expr::GroupConcat { .. } => {
            let text = expr.restore();
            if !names.iter().any(|name| name.eq_ignore_ascii_case(&text)) {
                let (func, ftype) = build_agg_func(expr, resolver)?;
                agg_funcs.push(func);
                names.push(text.clone());
                types.push(ftype);
            }
            Expr::Column(vec![text])
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(substitute_aggregates(
            inner,
            agg_funcs,
            names,
            types,
            grouping_specs,
            group_by_names,
            resolver,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_aggregates(
                inner,
                agg_funcs,
                names,
                types,
                grouping_specs,
                group_by_names,
                resolver,
            )?),
        ),
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(substitute_aggregates(
                lhs,
                agg_funcs,
                names,
                types,
                grouping_specs,
                group_by_names,
                resolver,
            )?),
            Box::new(substitute_aggregates(
                rhs,
                agg_funcs,
                names,
                types,
                grouping_specs,
                group_by_names,
                resolver,
            )?),
        ),
        other => other.clone(),
    })
}

/// The window-call index a select field IS, once
/// [`crate::window::hoist_windows`] has replaced the call with its computed
/// column.
fn hoisted_window_index(expr: &tidb_ast::Expr) -> Option<usize> {
    let tidb_ast::Expr::Column(path) = expr else {
        return None;
    };
    let name = path.last()?;
    crate::window::is_window_column(name)
        .then(|| crate::window::window_column_index(name))
        .flatten()
}

/// Whether `expr` reads a hoisted window column anywhere inside a larger
/// expression.
fn expr_has_hoisted_window(expr: &tidb_ast::Expr) -> bool {
    struct Finder {
        found: bool,
    }
    impl tidb_ast::Visitor for Finder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_ref::<tidb_ast::Expr>() {
                if path
                    .last()
                    .is_some_and(|name| crate::window::is_window_column(name))
                {
                    self.found = true;
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut finder = Finder { found: false };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut finder);
    finder.found
}

/// Builds one aggregate function (and its Go-inferred result type) from an
/// `Expr::Aggregate` node.
fn build_agg_func(
    expr: &tidb_ast::Expr,
    resolver: &ScopeResolver<'_>,
) -> Result<(AggFunc, FieldType), DriverError> {
    // GROUP_CONCAT is its own AST shape: it carries a separator and its own
    // row ORDER BY rather than being a one-argument aggregate.
    if let tidb_ast::Expr::GroupConcat {
        distinct,
        args,
        order_by,
        separator,
    } = expr
    {
        // `GROUP_CONCAT(a, b, ...)` concatenates its arguments per row before
        // the rows are joined; the first argument rides `arg` and the rest
        // ride `extra_args`.
        let Some((first, rest)) = args.split_first() else {
            return Err(DriverError::Unsupported(
                "GROUP_CONCAT requires at least one argument",
            ));
        };
        let arg = rewrite_expr_resolved(first, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        let mut extra_args = Vec::with_capacity(rest.len());
        for extra in rest {
            extra_args.push(
                rewrite_expr_resolved(extra, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
            );
        }
        // The aggregate's own ORDER BY items resolve against the SOURCE row,
        // the same scope the concatenated argument does.
        let mut order_items = Vec::with_capacity(order_by.len());
        for item in order_by {
            let expr = rewrite_expr_resolved(&item.expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            order_items.push((expr, item.desc));
        }
        let mut ret_type = FieldType::new(FieldTypeCode::VarString);
        ret_type.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        return Ok((
            AggFunc {
                kind: AggKind::GroupConcat {
                    separator: separator.value.clone(),
                },
                arg: Some(arg),
                extra_args,
                distinct: *distinct,
                order_by: order_items,
            },
            ret_type,
        ));
    }
    let tidb_ast::Expr::Aggregate {
        name,
        distinct,
        args,
    } = expr
    else {
        return Err(DriverError::Unsupported("not an aggregate function"));
    };
    // `COUNT(DISTINCT a, b, ...)` is the one non-GROUP_CONCAT aggregate the
    // parser lets through with more than one argument (`parse_aggregate`
    // rejects a bare `COUNT(a, b)` and every multi-argument `SUM`/`AVG`/etc.
    // at parse time), so only COUNT needs an `extra_args`-carrying path here.
    let Some((first, rest)) = args.split_first() else {
        return Err(DriverError::Unsupported(
            "multi-argument aggregates are deferred",
        ));
    };
    if !rest.is_empty()
        && !matches!(
            name.as_str(),
            "COUNT" | "JSON_OBJECTAGG" | "APPROX_COUNT_DISTINCT" | "APPROX_PERCENTILE"
        )
    {
        return Err(DriverError::Unsupported(
            "multi-argument aggregates are deferred",
        ));
    }
    // A subquery inside an aggregate's own argument (`SUM((SELECT ...))`,
    // `SUM(CASE WHEN EXISTS(...) THEN v END)`) would need to run once per
    // SOURCE row, before the aggregate accumulates it -- an Apply BELOW the
    // aggregation, rather than the Apply above it this driver builds for a
    // select-field/HAVING/ORDER BY subquery (which reads the already-grouped
    // value). That per-row Apply is not built here; refuse precisely rather
    // than let the per-row rewriter reject it with its generic message.
    if expr_has_subquery(first) || rest.iter().any(expr_has_subquery) {
        return Err(DriverError::Unsupported(
            "a subquery inside an aggregate function's argument is not supported yet",
        ));
    }
    let arg = rewrite_expr_resolved(first, resolver)
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    let mut extra_args = Vec::with_capacity(rest.len());
    for extra in rest {
        extra_args.push(
            rewrite_expr_resolved(extra, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }
    let mut all_args = Vec::with_capacity(1 + extra_args.len());
    all_args.push(arg.clone());
    all_args.extend(extra_args.iter().cloned());
    let (kind, ftype) = agg_kind_and_type(name, &all_args)?;
    // `APPROX_PERCENTILE`'s percentage rides the KIND, not the argument list:
    // it is a plan-time constant Go reads once in `buildApproxPercentile`,
    // never a per-row input.
    if matches!(kind, AggKind::ApproxPercentile(_)) {
        extra_args.clear();
    }
    Ok((
        AggFunc {
            kind,
            arg: Some(arg),
            extra_args,
            distinct: *distinct,
            order_by: Vec::new(),
        },
        ftype,
    ))
}

/// The type of the column an Apply appends for a correlated scalar subquery.
///
/// Go infers it statically from the subquery's select field; here the query is
/// planned with every correlated column bound to NULL, which reaches the same
/// field type without depending on any outer row -- and it must, because the
/// appended column's width is fixed before the first inner run (a `SUM` is a
/// 40-byte decimal, not an 8-byte integer). Falling back to `LongLong`
/// matches what the rest of the seed does for an uninferred expression.
fn subquery_result_type(
    correlated: &CorrelatedSubquery,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<FieldType> {
    let nulls: Vec<(Vec<String>, Datum)> = correlated
        .columns
        .iter()
        .map(|path| (path.clone(), Datum::Null))
        .collect();
    let typed = bind_subquery_columns(&correlated.select, &nulls).ok()?;
    run_select_stmt(&typed, catalog, current_db, ctx)
        .ok()
        .and_then(|(columns, _)| columns.first().map(|(_, ft)| ft.clone()))
}

/// A short description of a driver error, for the executor-level error the
/// apply callback must return.
fn driver_error_text(error: &DriverError) -> &'static str {
    match error {
        DriverError::SubqueryReturnsMoreThanOneRow => "Subquery returns more than 1 row",
        DriverError::Unsupported(text) => text,
        _ => "the correlated subquery failed",
    }
}

/// What the outer expression asks of a correlated subquery's result.
///
/// Go builds a different plan for each: `handleScalarSubquery` for a scalar
/// read, and a semi join (`LogicalJoin` with `SemiJoin`/`AntiSemiJoin`/
/// `LeftOuterSemiJoin`) for `EXISTS`, `IN` and `ANY`/`ALL`. Here they all ride
/// one Apply, because the join's answer for one outer row is exactly what
/// running the inner query for that row and folding the result yields.
enum SubqueryKind {
    /// A scalar read: the one value the subquery selects, NULL if no row.
    Scalar,
    /// `[NOT] EXISTS`.
    Exists { not: bool },
    /// `lhs [NOT] IN (subquery)`. `lhs` belongs to the OUTER scope and is
    /// evaluated per outer row against that row's inner result.
    In { lhs: tidb_ast::Expr, not: bool },
    /// `lhs <op> ANY|ALL (subquery)`.
    Compare {
        op: tidb_ast::BinaryOp,
        lhs: tidb_ast::Expr,
        all: bool,
    },
}

/// A correlated subquery found in an outer expression: the subquery itself and
/// what its result is asked for.
struct CorrelatedSubquery {
    select: tidb_ast::SelectStmt,
    kind: SubqueryKind,
    columns: Vec<Vec<String>>,
}

/// Whether `expr` references a column of the OUTER scope, which is what makes
/// a subquery correlated (Go's `ExtractCorrelatedCols4LogicalPlan`).
///
/// A reference is correlated when the inner query's own `FROM` cannot resolve
/// it but the outer scope can -- the same two-scope test Go's name resolver
/// applies when it binds a column to an outer plan's schema.
/// [`collect_correlated_columns`], widened to a `QueryStmt`: a set operation's
/// correlated columns are the union of what each of its terms references,
/// since every term is re-run per outer row exactly like a lone `SELECT` is.
/// A statement-level `ORDER BY`/`LIMIT` names an output column or position
/// (see `sort_rows_by_output`), never an outer one, so it contributes nothing
/// here.
pub(crate) fn collect_correlated_columns_query(
    query: &QueryStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    found: &mut Vec<Vec<String>>,
    ctx: &crate::StmtContext,
) {
    match query {
        QueryStmt::Select(select) => {
            collect_correlated_columns(select, outer, catalog, current_db, found, ctx)
        }
        QueryStmt::SetOpr(set_opr) => {
            for term in &set_opr.terms {
                match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => {
                        collect_correlated_columns(select, outer, catalog, current_db, found, ctx)
                    }
                    tidb_ast::SetOprTermBody::Nested(nested) => collect_correlated_columns_query(
                        &QueryStmt::SetOpr(nested.clone()),
                        outer,
                        catalog,
                        current_db,
                        found,
                        ctx,
                    ),
                }
            }
        }
    }
}

fn collect_correlated_columns(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    found: &mut Vec<Vec<String>>,
    ctx: &crate::StmtContext,
) {
    let inner = match &select.from {
        None => FromScope::default(),
        Some(join) => match build_join(join, catalog, current_db, ctx, None, None) {
            Ok((_, scope)) => scope,
            // An unresolvable inner FROM is reported by the inner run itself.
            Err(_) => FromScope::default(),
        },
    };
    let mut visit = |expr: &tidb_ast::Expr| {
        collect_outer_columns(expr, &inner, outer, found);
    };
    for field in select.fields.fields() {
        if let SelectField::Expr { expr, .. } = field {
            visit(expr);
        }
    }
    if let Some(where_clause) = &select.where_clause {
        visit(where_clause);
    }
    if let Some(having) = &select.having {
        visit(having);
    }
    for item in &select.group_by {
        visit(&item.expr);
    }
    for item in &select.order_by {
        visit(&item.expr);
    }
}

/// Records every column reference in `expr` that the inner scope cannot
/// resolve but the outer scope can.
fn collect_outer_columns(
    expr: &tidb_ast::Expr,
    inner: &FromScope,
    outer: &FromScope,
    found: &mut Vec<Vec<String>>,
) {
    use tidb_ast::Expr;
    match expr {
        Expr::Column(path) => {
            let inner_resolver = ScopeResolver { scope: inner };
            let outer_resolver = ScopeResolver { scope: outer };
            if inner_resolver.resolve(path).is_none()
                && outer_resolver.resolve(path).is_some()
                && !found.contains(path)
            {
                found.push(path.clone());
            }
        }
        Expr::Paren(inner_expr)
        | Expr::Unary(_, inner_expr)
        | Expr::Is {
            expr: inner_expr, ..
        } => {
            collect_outer_columns(inner_expr, inner, outer, found);
        }
        Expr::Binary(_, lhs, rhs) => {
            collect_outer_columns(lhs, inner, outer, found);
            collect_outer_columns(rhs, inner, outer, found);
        }
        Expr::In { expr, list, .. } => {
            collect_outer_columns(expr, inner, outer, found);
            for item in list {
                collect_outer_columns(item, inner, outer, found);
            }
        }
        Expr::Aggregate { args, .. } => {
            for arg in args {
                collect_outer_columns(arg, inner, outer, found);
            }
        }
        _ => {}
    }
}

/// Replaces each correlated column reference with the literal for the outer
/// row's value, which is this port's equivalent of Go's apply loop writing
/// `*col.Data` before re-running the inner plan.
fn bind_correlated_columns(
    expr: &tidb_ast::Expr,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match expr {
        Expr::Column(path) => match bindings.iter().find(|(bound, _)| paths_match(bound, path)) {
            Some((_, value)) => datum_to_literal(value)?,
            None => expr.clone(),
        },
        Expr::Paren(inner) => Expr::Paren(Box::new(bind_correlated_columns(inner, bindings)?)),
        Expr::Unary(op, inner) => {
            Expr::Unary(*op, Box::new(bind_correlated_columns(inner, bindings)?))
        }
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(bind_correlated_columns(expr, bindings)?),
            target: *target,
            not: *not,
        },
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(bind_correlated_columns(lhs, bindings)?),
            Box::new(bind_correlated_columns(rhs, bindings)?),
        ),
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(bind_correlated_columns(expr, bindings)?),
            list: list
                .iter()
                .map(|item| bind_correlated_columns(item, bindings))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        Expr::Aggregate {
            name,
            distinct,
            args,
        } => Expr::Aggregate {
            name: name.clone(),
            distinct: *distinct,
            args: args
                .iter()
                .map(|arg| bind_correlated_columns(arg, bindings))
                .collect::<Result<_, _>>()?,
        },
        other => other.clone(),
    })
}

/// Whether a bound path and a reference name the same column. A bare `a`
/// matches a bound `t.a`, since the inner reference may be unqualified.
fn paths_match(bound: &[String], candidate: &[String]) -> bool {
    if bound.len() == candidate.len() {
        return bound
            .iter()
            .zip(candidate)
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
    }
    match (bound.last(), candidate.last()) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

/// Substitutes `bindings` for the correlated column references in every clause
/// of `select`.
fn bind_subquery_columns(
    select: &tidb_ast::SelectStmt,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut bound = select.clone();
    for field in bound.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = bind_correlated_columns(expr, bindings)?;
        }
    }
    if let Some(where_clause) = &bound.where_clause {
        bound.where_clause = Some(bind_correlated_columns(where_clause, bindings)?);
    }
    if let Some(having) = &bound.having {
        bound.having = Some(bind_correlated_columns(having, bindings)?);
    }
    for item in &mut bound.group_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    for item in &mut bound.order_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    Ok(bound)
}

/// [`bind_subquery_columns`], widened to a `QueryStmt`: every term of a set
/// operation gets the same substitution, since each is re-run per outer row.
pub(crate) fn bind_subquery_columns_query(
    query: &QueryStmt,
    bindings: &[(Vec<String>, Datum)],
) -> Result<QueryStmt, DriverError> {
    Ok(match query {
        QueryStmt::Select(select) => {
            QueryStmt::Select(Box::new(bind_subquery_columns(select, bindings)?))
        }
        QueryStmt::SetOpr(set_opr) => {
            let mut bound = (**set_opr).clone();
            for term in &mut bound.terms {
                term.body = match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => tidb_ast::SetOprTermBody::Select(
                        Box::new(bind_subquery_columns(select, bindings)?),
                    ),
                    tidb_ast::SetOprTermBody::Nested(nested) => {
                        let QueryStmt::SetOpr(nested) = bind_subquery_columns_query(
                            &QueryStmt::SetOpr(nested.clone()),
                            bindings,
                        )?
                        else {
                            unreachable!("SetOpr input binds to SetOpr output")
                        };
                        tidb_ast::SetOprTermBody::Nested(nested)
                    }
                };
            }
            QueryStmt::SetOpr(Box::new(bound))
        }
    })
}

/// Binds every correlated column in `select` and runs it for one outer row.
fn run_correlated_subquery(
    correlated: &CorrelatedSubquery,
    outer_values: &[Datum],
    outer_scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let mut bindings = Vec::with_capacity(correlated.columns.len());
    for path in &correlated.columns {
        let resolver = ScopeResolver { scope: outer_scope };
        // The aggregation's output row has no table qualifiers of its own
        // (Go's post-aggregation schema is one flat row), so a qualified
        // correlated reference (`t.g`) falls back to a bare-name lookup
        // (`g`) when the qualified one does not resolve. The plain WHERE/
        // SELECT Apply paths bind against the real `FromScope`, where the
        // qualified lookup already succeeds and this fallback never fires.
        let (index, _, _) = resolver
            .resolve(path)
            .or_else(|| {
                let name = path.last()?;
                resolver.resolve(std::slice::from_ref(name))
            })
            .ok_or(DriverError::Unsupported("unresolved correlated column"))?;
        let value = outer_values
            .get(index)
            .cloned()
            .ok_or(DriverError::Unsupported("correlated column out of range"))?;
        bindings.push((path.clone(), value));
    }

    let bound = bind_subquery_columns(&correlated.select, &bindings)?;
    let (_, rows) = run_select_stmt(&bound, catalog, current_db, ctx)?;
    match &correlated.kind {
        // EXISTS folds to 1/0 per outer row.
        SubqueryKind::Exists { not } => Ok(Datum::Int(i64::from(!rows.is_empty() != *not))),
        SubqueryKind::Scalar => match rows.len() {
            0 => Ok(Datum::Null),
            1 => {
                let [value] = rows[0].as_slice() else {
                    return Err(DriverError::Unsupported(
                        "a scalar subquery selecting several columns is not supported yet",
                    ));
                };
                Ok(value.clone())
            }
            _ => Err(DriverError::SubqueryReturnsMoreThanOneRow),
        },
        // The semi-join shapes: this outer row's inner result becomes a value
        // list, and the test is evaluated over it exactly as the uncorrelated
        // fold evaluates its own folded list -- same `IN`, same comparisons,
        // so the same three-valued answers.
        SubqueryKind::In { lhs, not } => {
            let list = subquery_value_list(
                &rows,
                "an IN subquery selecting several columns is not supported yet",
            )?;
            let test = in_list_expr(lhs.clone(), list, *not);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
        SubqueryKind::Compare { op, lhs, all } => {
            let list = subquery_value_list(
                &rows,
                "an ANY/ALL subquery selecting several columns is not supported yet",
            )?;
            let test = any_all_expr(*op, lhs.clone(), *all, list);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
    }
}

/// A subquery result's single column, as the literals a value list needs.
fn subquery_value_list(
    rows: &[Vec<Datum>],
    several_columns: &'static str,
) -> Result<Vec<tidb_ast::Expr>, DriverError> {
    let mut list = Vec::with_capacity(rows.len());
    for row in rows {
        let [value] = row.as_slice() else {
            return Err(DriverError::Unsupported(several_columns));
        };
        list.push(datum_to_literal(value)?);
    }
    Ok(list)
}

/// `lhs [NOT] IN (list)`, with the empty list written as the constant it is.
///
/// `x IN ()` is not sayable in SQL: an empty subquery result makes `IN` false
/// and `NOT IN` true for every x INCLUDING NULL, because MySQL evaluates the
/// semi join, which finds no row to match. The non-empty case keeps the
/// ordinary `IN`, whose NULL rules are the three-valued ones (an unmatched x
/// against a list holding NULL is NULL, not false).
fn in_list_expr(lhs: tidb_ast::Expr, list: Vec<tidb_ast::Expr>, not: bool) -> tidb_ast::Expr {
    if list.is_empty() {
        return tidb_ast::Expr::Int(i64::from(not).to_string());
    }
    tidb_ast::Expr::In {
        expr: Box::new(lhs),
        list,
        not,
    }
}

/// `lhs <op> ANY|ALL (list)` as the OR/AND chain it is defined to be.
///
/// Go's `buildSemiApply` for a comparison subquery builds the same disjunction
/// (`ANY`) or conjunction (`ALL`) of per-value comparisons, which is where the
/// three-valued behaviour comes from: `20 > ANY (25, NULL)` is
/// `false OR NULL` = NULL, while `20 > ALL (25, NULL)` is `false AND NULL` =
/// false. An empty list has no comparison at all, so `ALL` is vacuously TRUE
/// and `ANY` is FALSE -- both for a NULL `lhs` too.
fn any_all_expr(
    op: tidb_ast::BinaryOp,
    lhs: tidb_ast::Expr,
    all: bool,
    list: Vec<tidb_ast::Expr>,
) -> tidb_ast::Expr {
    use tidb_ast::{BinaryOp, Expr};
    let compare = |value: Expr| Expr::Binary(op, Box::new(lhs.clone()), Box::new(value));
    let mut values = list.into_iter();
    let Some(first) = values.next() else {
        return Expr::Int(i64::from(all).to_string());
    };
    let combine = if all {
        BinaryOp::LogicAnd
    } else {
        BinaryOp::LogicOr
    };
    values.fold(compare(first), |acc, value| {
        Expr::Binary(combine, Box::new(acc), Box::new(compare(value)))
    })
}

/// Evaluates an expression over the OUTER scope's columns for one outer row.
///
/// The semi-join folds keep their left operand in the outer scope rather than
/// binding it to a literal, so the comparison runs through the very same
/// expression evaluator the uncorrelated path uses.
fn eval_expr_on_row(
    expr: &tidb_ast::Expr,
    scope: &FromScope,
    values: &[Datum],
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let types: Vec<FieldType> = scope.column_list().into_iter().map(|(_, ft)| ft).collect();
    let rewritten = rewrite_expr_resolved(expr, &ScopeResolver { scope })
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    let chunk = row_chunk(values, &types)?;
    rewritten
        .eval(ctx, chunk.get_row(0))
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))
}

/// Finds the one correlated subquery in `expr`, replacing it with a reference
/// to the column an [`ApplyExec`] will append at `index`.
///
/// Go's rewriter does the same substitution: after building the Apply, the
/// subquery expression becomes the Apply schema's last column.
fn extract_correlated_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    index: usize,
    found: &mut Option<CorrelatedSubquery>,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // The synthetic name the appended column answers to.
    let placeholder = |index: usize| Expr::Column(vec![format!("__apply_{index}")]);
    Ok(match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                return Err(DriverError::Unsupported(
                    "set-operation subqueries are not supported yet",
                ));
            };
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                // Uncorrelated: the folding pass handles it.
                return Ok(expr.clone());
            }
            if found.is_some() {
                return Err(DriverError::Unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            let kind = match expr {
                Expr::Exists { not, .. } => SubqueryKind::Exists { not: *not },
                _ => SubqueryKind::Scalar,
            };
            *found = Some(CorrelatedSubquery {
                select: (**select).clone(),
                kind,
                columns,
            });
            placeholder(index)
        }
        // Go turns a correlated IN / ANY / ALL into a semi join; the Apply
        // here answers the same question one outer row at a time, with the
        // tested left operand staying in the outer expression.
        Expr::InSubquery {
            expr: lhs,
            subquery,
            not,
        } => {
            let select = subquery_select(subquery)?;
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                return Ok(expr.clone());
            }
            if found.is_some() || expr_has_subquery(lhs) {
                return Err(DriverError::Unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            *found = Some(CorrelatedSubquery {
                select: select.clone(),
                kind: SubqueryKind::In {
                    lhs: (**lhs).clone(),
                    not: *not,
                },
                columns,
            });
            placeholder(index)
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } => {
            let select = subquery_select(subquery)?;
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                return Ok(expr.clone());
            }
            if found.is_some() || expr_has_subquery(left) {
                return Err(DriverError::Unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            *found = Some(CorrelatedSubquery {
                select: select.clone(),
                kind: SubqueryKind::Compare {
                    op: *op,
                    lhs: (**left).clone(),
                    all: *all,
                },
                columns,
            });
            placeholder(index)
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(extract_correlated_subquery(
            inner, outer, catalog, current_db, index, found, ctx,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(extract_correlated_subquery(
                inner, outer, catalog, current_db, index, found, ctx,
            )?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(extract_correlated_subquery(
                expr, outer, catalog, current_db, index, found, ctx,
            )?),
            target: *target,
            not: *not,
        },
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(extract_correlated_subquery(
                lhs, outer, catalog, current_db, index, found, ctx,
            )?),
            Box::new(extract_correlated_subquery(
                rhs, outer, catalog, current_db, index, found, ctx,
            )?),
        ),
        other => other.clone(),
    })
}

/// The `SELECT` a subquery carries, rejecting the set-operation body.
fn subquery_select(query: &tidb_ast::QueryStmt) -> Result<&tidb_ast::SelectStmt, DriverError> {
    match query {
        tidb_ast::QueryStmt::Select(select) => Ok(select),
        _ => Err(DriverError::Unsupported(
            "set-operation subqueries are not supported yet",
        )),
    }
}

/// The scope a subquery inside `select` sees as its OUTER scope: `select`'s
/// own `FROM` tables. An unresolvable `FROM` yields an empty scope, and the
/// error surfaces when the query itself is built.
fn select_outer_scope(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> FromScope {
    match &select.from {
        None => FromScope::default(),
        Some(join) => match build_join(join, catalog, current_db, ctx, None, None) {
            Ok((_, scope)) => scope,
            Err(_) => FromScope::default(),
        },
    }
}

/// Whether any clause of `select` contains a subquery the folding pass should
/// run on. A correlated subquery in the `WHERE` is left for the Apply path.
fn select_has_uncorrelated_subquery(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    if let Some(where_clause) = &select.where_clause {
        if expr_has_subquery(where_clause) {
            let outer = select_outer_scope(select, catalog, current_db, ctx);
            let mut found = None;
            // A correlated WHERE subquery is the Apply path's job.
            if extract_correlated_subquery(
                where_clause,
                &outer,
                catalog,
                current_db,
                0,
                &mut found,
                ctx,
            )
            .is_ok()
                && found.is_some()
            {
                return false;
            }
        }
    }
    select_has_subquery(select)
}

/// Whether any clause of `select` contains a subquery, so the fold pass runs
/// only when it has something to do.
fn select_has_subquery(select: &tidb_ast::SelectStmt) -> bool {
    let fields = select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => expr_has_subquery(expr),
        SelectField::Wildcard(_) => false,
    });
    fields
        || select.where_clause.as_ref().is_some_and(expr_has_subquery)
        || select.having.as_ref().is_some_and(expr_has_subquery)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
        || select
            .group_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
}

/// Whether `expr` contains a subquery in a position the fold pass walks.
fn expr_has_subquery(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            expr_has_subquery(inner)
        }
        Expr::Binary(_, lhs, rhs) => expr_has_subquery(lhs) || expr_has_subquery(rhs),
        Expr::In { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}

/// Folds every subquery in `select`'s clauses, returning the rewritten copy.
fn fold_select_subqueries(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut folded = select.clone();
    for field in folded.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = fold_subqueries(expr, outer, catalog, current_db, ctx)?;
        }
    }
    if let Some(where_clause) = &folded.where_clause {
        folded.where_clause = Some(fold_subqueries(
            where_clause,
            outer,
            catalog,
            current_db,
            ctx,
        )?);
    }
    if let Some(having) = &folded.having {
        folded.having = Some(fold_subqueries(having, outer, catalog, current_db, ctx)?);
    }
    for item in &mut folded.order_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    for item in &mut folded.group_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    Ok(folded)
}

/// Replaces every uncorrelated subquery in `expr` with the value it produces.
///
/// This is Go's `handleScalarSubquery` path for a subquery with no correlated
/// columns: the subquery is planned and run on the spot
/// (`EvalSubqueryFirstRow`) and its result folded into a `Constant`, so the
/// outer statement plans against ordinary literals. Go's `buildMaxOneRow`
/// wrapper is the "more than one row" check below; a subquery producing no
/// rows yields NULL.
///
/// `EXISTS` folds to 1 or 0, `x IN (subquery)` folds to `x IN (values)` and
/// `x <op> ANY|ALL (subquery)` to the OR/AND chain of comparisons, all of
/// which evaluate identically for an uncorrelated subquery -- including the
/// NULL rules, since the folded list is compared by the same code.
///
/// DEFERRED (documented): CORRELATED subqueries, which Go turns into an Apply
/// operator rather than folding, and which this leaves for the Apply path
/// rather than silently evaluating the inner query against the wrong row; and
/// row constructors (a subquery selecting several columns).
fn fold_subqueries(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // A subquery reading the OUTER query's columns has no single value to fold
    // to: it is the Apply path's job, one run per outer row.
    if let Expr::Subquery(query)
    | Expr::Exists {
        subquery: query, ..
    }
    | Expr::InSubquery {
        subquery: query, ..
    }
    | Expr::CompareSubquery {
        subquery: query, ..
    } = expr
    {
        if let tidb_ast::QueryStmt::Select(select) = &**query {
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if !columns.is_empty() {
                return Ok(expr.clone());
            }
        }
    }
    Ok(match expr {
        Expr::Subquery(query) => {
            let rows = run_subquery(query, catalog, current_db, ctx)?;
            match rows.len() {
                // Go: a scalar subquery with no rows is NULL.
                0 => Expr::Null,
                1 => {
                    let row = &rows[0];
                    let [value] = row.as_slice() else {
                        return Err(DriverError::Unsupported(
                            "a scalar subquery selecting several columns is not supported yet",
                        ));
                    };
                    datum_to_literal(value)?
                }
                // Go's buildMaxOneRow raises ER_SUBQUERY_NO_1_ROW here.
                _ => return Err(DriverError::SubqueryReturnsMoreThanOneRow),
            }
        }
        Expr::Exists { subquery, not } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let exists = !rows.is_empty();
            Expr::Int(i64::from(exists != *not).to_string())
        }
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(
                &rows,
                "an IN subquery selecting several columns is not supported yet",
            )?;
            in_list_expr(
                fold_subqueries(expr, outer, catalog, current_db, ctx)?,
                list,
                *not,
            )
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(
                &rows,
                "an ANY/ALL subquery selecting several columns is not supported yet",
            )?;
            any_all_expr(
                *op,
                fold_subqueries(left, outer, catalog, current_db, ctx)?,
                *all,
                list,
            )
        }
        // Walk the forms the expression rewriter itself supports; anything
        // else is returned unchanged and fails to rewrite as it already does.
        Expr::Paren(inner) => Expr::Paren(Box::new(fold_subqueries(
            inner, outer, catalog, current_db, ctx,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(fold_subqueries(inner, outer, catalog, current_db, ctx)?),
        ),
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(fold_subqueries(lhs, outer, catalog, current_db, ctx)?),
            Box::new(fold_subqueries(rhs, outer, catalog, current_db, ctx)?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            target: *target,
            not: *not,
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            list: list
                .iter()
                .map(|item| fold_subqueries(item, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        other => other.clone(),
    })
}

/// Runs a subquery against the catalog, rejecting the correlated case.
///
/// A correlated subquery references a column of the OUTER query, which this
/// resolver cannot see -- so it fails to resolve here and the error surfaces
/// rather than the subquery being evaluated against the wrong scope.
fn run_subquery(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let tidb_ast::QueryStmt::Select(_) = query else {
        return Err(DriverError::Unsupported(
            "set-operation subqueries are not supported yet",
        ));
    };
    let tidb_ast::QueryStmt::Select(select) = query else {
        unreachable!("the set-operation case is rejected above")
    };
    run_select_stmt(select, catalog, current_db, ctx).map(|(_, rows)| rows)
}

/// Go turns a subquery's result `Datum` into an `expression.Constant`; the
/// same value has to travel back through the AST here, so it becomes the
/// literal that parses to it.
/// A byte string as a literal expression: readable text stays a string, and
/// anything that is not UTF-8 becomes a hex literal so no byte is lost.
pub(crate) fn bytes_to_literal(bytes: &[u8]) -> tidb_ast::Expr {
    match std::str::from_utf8(bytes) {
        Ok(text) => tidb_ast::Expr::String(text.to_owned()),
        Err(_) => tidb_ast::Expr::Hex(hex_digits(bytes)),
    }
}

/// The lowercase, even-length hex digits an `Expr::Hex` carries.
fn hex_digits(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn datum_to_literal(value: &Datum) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match value {
        Datum::Null => Expr::Null,
        Datum::Int(v) => {
            // A negative literal is a unary minus over a positive one, which
            // is how the parser itself represents it.
            if *v < 0 {
                Expr::Unary(
                    tidb_ast::UnaryOp::Minus,
                    Box::new(Expr::Int(v.unsigned_abs().to_string())),
                )
            } else {
                Expr::Int(v.to_string())
            }
        }
        Datum::UInt(v) => Expr::Int(v.to_string()),
        Datum::Real(v) => Expr::Float(*v),
        Datum::Decimal(d) => Expr::Decimal(d.to_string()),
        // A byte string that is not UTF-8 becomes a hex literal, which is
        // lossless where a lossy string conversion would corrupt it.
        Datum::String(s) => bytes_to_literal(s.bytes()),
        Datum::Bytes(b) => bytes_to_literal(b),
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
            Expr::Hex(hex_digits(literal.as_bytes()))
        }
        _ => {
            return Err(DriverError::Unsupported(
                "this subquery result kind is not supported yet",
            ))
        }
    })
}

/// One table in a query's `FROM`: the name a qualifier must match (its alias
/// when it has one, as in Go's `TableSource`), its columns, and the offset of
/// its first column in the joined row.
#[derive(Clone, Debug)]
pub(crate) struct FromTable {
    pub(crate) name: String,
    /// The schema the table lives in, when a `db.t.column` reference may name
    /// it. `None` for a source that cannot be schema-qualified: an aliased
    /// table (MySQL's alias replaces the whole path) or a synthetic scope.
    pub(crate) database: Option<String>,
    pub(crate) columns: Vec<(String, FieldType)>,
    pub(crate) offset: usize,
    /// Go `checkColFuncDepend`'s candidate keys: each entry is a set of this
    /// source's column names that together determine the whole row, so once
    /// `GROUP BY` pins all of them every other column of the source is a
    /// single value per group and `ONLY_FULL_GROUP_BY` permits it. Only a
    /// base table has any: the primary key, plus every UNIQUE index whose
    /// columns are all `NOT NULL` (a nullable unique key permits repeated
    /// NULLs and so determines nothing). A derived table, a view or a
    /// synthetic scope carries none.
    pub(crate) determinants: Vec<Vec<String>>,
}

/// One `GROUPING(c1, ..., cn)` call hoisted into an aggregation output column.
///
/// Go computes `GROUPING` from the `gid` column Expand attaches to every
/// replicated row; this seed's rollup runs one aggregation pass per grouping
/// set, so the pass itself already knows which columns are rolled up and the
/// bitmask is filled straight into the output row.
#[derive(Clone, Debug)]
struct GroupingSpec {
    /// The aggregation output column this call's value is written into.
    out_index: usize,
    /// Each argument's position in the `GROUP BY` list, in argument order.
    /// The LEFTMOST argument owns the HIGHEST bit (captured from real TiDB:
    /// with `GROUP BY a, b WITH ROLLUP`, the `b`-only subtotal row reports
    /// `GROUPING(a,b) = 1` and `GROUPING(b,a) = 2`).
    group_positions: Vec<usize>,
}

impl GroupingSpec {
    /// The bitmask this call reports for a pass that groups by the first `k`
    /// `GROUP BY` expressions, i.e. one where positions `k..` are rolled up.
    fn mask_for_prefix(&self, k: usize) -> u64 {
        let width = self.group_positions.len();
        self.group_positions
            .iter()
            .enumerate()
            .filter(|(_, &position)| position >= k)
            .map(|(arg, _)| 1u64 << (width - 1 - arg))
            .sum()
    }
}

/// The `GROUPING(...)` arguments when `expr` IS such a call, else `None`.
fn grouping_call_args(expr: &tidb_ast::Expr) -> Option<&[tidb_ast::Expr]> {
    match expr {
        tidb_ast::Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("grouping") => {
            Some(args)
        }
        _ => None,
    }
}

/// Whether `expr` mentions `GROUPING()` anywhere the aggregate path can reach
/// it. The recursion covers the same shapes [`substitute_aggregates`] walks;
/// a `GROUPING` buried in a shape neither one descends into is not detected
/// and simply evaluates as an unknown function, as it does today.
fn expr_has_grouping(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    if grouping_call_args(expr).is_some() {
        return true;
    }
    match expr {
        Expr::Paren(inner) | Expr::Unary(_, inner) => expr_has_grouping(inner),
        Expr::Binary(_, lhs, rhs) => expr_has_grouping(lhs) || expr_has_grouping(rhs),
        Expr::Func { args, .. } => args.iter().any(expr_has_grouping),
        _ => false,
    }
}

/// Whether the statement writes `GROUPING()` in any clause the aggregate path
/// evaluates.
fn select_has_grouping(select: &tidb_ast::SelectStmt) -> bool {
    select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => expr_has_grouping(expr),
        SelectField::Wildcard { .. } => false,
    }) || select.having.as_ref().is_some_and(expr_has_grouping)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_grouping(&item.expr))
}

/// The output type Go gives a `GROUPING()` column: `BIGINT UNSIGNED`, flen 20,
/// with the binary flag (captured from real TiDB: `tp=8 flag=160 flen=20`).
fn grouping_result_type() -> FieldType {
    let mut ftype = FieldType::new(FieldTypeCode::LongLong);
    ftype.add_flags(FieldTypeFlags::UNSIGNED | FieldTypeFlags::BINARY);
    ftype.set_flen(20);
    ftype
}

/// Resolves each `GROUPING()` argument to its position in the `GROUP BY` list.
///
/// Go rejects an argument that is not grouped with `ErrFieldInGroupingNotGroupBy`
/// (3602), naming the argument's 0-based position.
fn grouping_arg_positions(
    args: &[tidb_ast::Expr],
    group_by_names: &[String],
) -> Result<Vec<usize>, DriverError> {
    let mut positions = Vec::with_capacity(args.len());
    for (arg, expr) in args.iter().enumerate() {
        let tidb_ast::Expr::Column(path) = expr else {
            return Err(DriverError::FieldInGroupingNotGroupBy(arg));
        };
        let name = path.last().cloned().unwrap_or_default();
        let position = group_by_names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(&name))
            .ok_or(DriverError::FieldInGroupingNotGroupBy(arg))?;
        positions.push(position);
    }
    Ok(positions)
}

/// Adds a `GROUPING()` call as an aggregation output column and returns that
/// column's name and INDEX -- the index matters because a repeated call text
/// reuses the existing column rather than adding one, so a caller that
/// reserved the next index for it must read the real one back.
///
/// The column is a placeholder as far as the aggregation is concerned -- a
/// `FIRST_ROW` over the constant `0`, so the column exists and every group
/// produces exactly one value -- and [`run_rollup_aggregate`] overwrites it
/// with the per-grouping-set bitmask. Repeating the same call text reuses the
/// column already added, as the aggregate path does for a repeated aggregate.
fn add_grouping_column(
    args: &[tidb_ast::Expr],
    display: String,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_names: &[String],
) -> Result<(String, usize), DriverError> {
    if let Some(index) = names
        .iter()
        .position(|name| name.eq_ignore_ascii_case(&display))
    {
        if grouping_specs.iter().any(|spec| spec.out_index == index) {
            return Ok((display, index));
        }
    }
    let group_positions = grouping_arg_positions(args, group_by_names)?;
    let placeholder = Expression::Constant(tidb_expr::constant::Constant::new(
        Datum::Int(0),
        FieldType::new(FieldTypeCode::LongLong),
    ));
    agg_funcs.push(AggFunc {
        kind: AggKind::FirstRow,
        arg: Some(placeholder),
        extra_args: Vec::new(),
        distinct: false,
        order_by: Vec::new(),
    });
    grouping_specs.push(GroupingSpec {
        out_index: names.len(),
        group_positions,
    });
    let index = names.len();
    names.push(display.clone());
    types.push(grouping_result_type());
    Ok((display, index))
}

/// Where one select field of an aggregate query reads its value from.
enum OutputSlot {
    /// An aggregation output column, by index.
    Agg(usize),
    /// An expression over the aggregation's (+ Apply's) output columns, by
    /// index into `post_agg_exprs` -- a select field that CONTAINS a
    /// correlated subquery alongside aggregates/columns, e.g.
    /// `SUM(v) + (SELECT ...)`.
    Expr(usize),
    /// The column the n-th window call appends above the aggregation.
    Window(usize),
}

/// Extracts the one correlated subquery in a post-aggregation expression (a
/// select field, `HAVING`, or an `ORDER BY` item), hoists any aggregate calls
/// left in the remainder into `agg_funcs`/`names`/`types`, and returns the
/// resulting expression: aggregates and grouped columns become output column
/// references (Go's `havingWindowAndOrderbyExprResolver`), and the subquery
/// becomes a `__apply_N` placeholder column reference that the caller's
/// Apply (built once every correlated subquery in the statement is known)
/// makes real. `EXISTS`, `IN` and `ANY`/`ALL` ride the same placeholder,
/// because the Apply appends whatever [`run_correlated_subquery`] folds.
///
/// Returns `(expr, true)` when a correlated subquery was found and hoisted,
/// `(expr, false)` otherwise (uncorrelated, or no subquery at all).
#[allow(clippy::too_many_arguments)]
fn extract_and_hoist_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    applies: &mut Vec<(CorrelatedSubquery, String, FieldType)>,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_names: &[String],
    resolver: &ScopeResolver<'_>,
    ctx: &crate::StmtContext,
) -> Result<(tidb_ast::Expr, bool), DriverError> {
    // No subquery anywhere in the expression, so there is nothing to hoist
    // out of the way of a per-group Apply: the caller decides how (or
    // whether) to run the aggregate hoist itself, exactly as it did before
    // this function existed.
    if !expr_has_subquery(expr) {
        return Ok((expr.clone(), false));
    }
    let index = applies.len();
    let mut found = None;
    let rewritten =
        extract_correlated_subquery(expr, outer, catalog, current_db, index, &mut found, ctx)?;
    let Some(correlated) = found else {
        // Uncorrelated, or no subquery reachable through this expression
        // shape: left for the caller / the fold pass / the rewriter's own
        // error.
        return Ok((rewritten, false));
    };
    let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
        subquery_result_type(&correlated, catalog, current_db, ctx)
            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
    } else {
        FieldType::new(FieldTypeCode::LongLong)
    };
    let hoisted = substitute_aggregates(
        &rewritten,
        agg_funcs,
        names,
        types,
        grouping_specs,
        group_by_names,
        resolver,
    )?;
    applies.push((correlated, format!("__apply_{index}"), value_type));
    Ok((hoisted, true))
}

/// Runs `GROUP BY g1..gn WITH ROLLUP` by materializing the source rows once
/// and aggregating every grouping-set prefix `(g1..gk)`, `k = n..0`, over
/// them -- logically what Go's Expand operator does by replicating each input
/// row once per grouping set. The rolled-up columns are NULLed in the
/// materialized SOURCE rows, so every expression over them (the `FIRST_ROW`
/// carriers, `a+1`, a `HAVING` reference) evaluates against NULL exactly as
/// it does over Expand's replicated rows; a genuinely-NULL data value and a
/// rollup NULL are then indistinguishable in the output, as in TiDB (captured
/// from real TiDB: `a=1` rows `(b=1,c=10)`/`(b=NULL,c=20)` yield both
/// `[1 NULL 20]` and the subtotal `[1 NULL 30]`). `GROUPING()` is what tells
/// the two apart, and each pass fills its `grouping_specs` columns with the
/// bitmask for the grouping set that pass computes.
///
/// Row order: Go's hash aggregation over Expand output emits rollup rows in a
/// NONDETERMINISTIC order (verified against real TiDB -- the order changes
/// across runs), so only the row multiset is contractual and `ORDER BY` is the
/// only ordering guarantee. This tier emits full groups first (first-seen
/// order), then each shorter prefix's subtotals, then the grand total. An
/// empty source yields no rows at all -- not even the grand total -- because
/// Expand replicates zero rows (unlike a scalar aggregate).
fn run_rollup_aggregate(
    source: Box<dyn Executor>,
    group_by: &[Expression],
    agg_funcs: &[AggFunc],
    out_schema: &Schema,
    out_types: &[FieldType],
    grouping_specs: &[GroupingSpec],
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    // Each rolled-up position must be a plain column so it can be NULLed in
    // the materialized source rows (Go's Expand projects grouping expressions
    // into dedicated columns; that generality is deferred).
    let mut group_cols = Vec::with_capacity(group_by.len());
    for expr in group_by {
        let Expression::Column(col) = expr else {
            return Err(DriverError::Unsupported(
                "WITH ROLLUP over a non-column GROUP BY expression is not supported yet",
            ));
        };
        group_cols.push(
            usize::try_from(col.index).map_err(|_| {
                DriverError::Parse("GROUP BY column has no source index".to_string())
            })?,
        );
    }

    // Materialize the source once; every prefix pass replays these rows.
    let source_schema = source.schema().clone();
    let source_types = source.ret_field_types().to_vec();
    let rows = drain_executor_rows(source, &source_types)?;

    let mut out_rows: Vec<Vec<Datum>> = Vec::new();
    if !rows.is_empty() {
        for k in (0..=group_cols.len()).rev() {
            let mut pass_rows = rows.clone();
            for row in &mut pass_rows {
                for &idx in &group_cols[k..] {
                    row[idx] = Datum::Null;
                }
            }
            let pass_source = Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(source_schema.clone(), 1, INIT_CAP, MAX_CHUNK_SIZE),
                pass_rows,
            ));
            let agg = HashAggExec::new(
                ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
                group_by[..k].to_vec(),
                agg_funcs.to_vec(),
                pass_source,
                ctx.clone(),
            );
            // This pass rolls up positions `k..`, which IS the grouping bit
            // each GROUPING() call reports -- the one thing that distinguishes
            // a subtotal's NULL from a data NULL.
            let mut pass_out = drain_executor_rows(Box::new(agg), out_types)?;
            for spec in grouping_specs {
                let mask = Datum::UInt(spec.mask_for_prefix(k));
                for row in &mut pass_out {
                    row[spec.out_index] = mask.clone();
                }
            }
            out_rows.extend(pass_out);
        }
    }
    Ok(Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        out_rows,
    )))
}

/// Opens `exec`, drains every row as datums of `types`, and closes it.
fn drain_executor_rows(
    mut exec: Box<dyn Executor>,
    types: &[FieldType],
) -> Result<Vec<Vec<Datum>>, DriverError> {
    exec.open()?;
    let mut rows = Vec::new();
    let mut req = exec.new_chunk();
    loop {
        exec.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            rows.push(
                types
                    .iter()
                    .enumerate()
                    .map(|(c, ft)| row.get_datum(c, ft))
                    .collect(),
            );
        }
    }
    exec.close()?;
    Ok(rows)
}

/// Go `buildDistinct`: an aggregation grouping by every column of `schema`,
/// carrying each one through a `FIRST_ROW` aggregate.
///
/// The hash aggregation emits groups in first-seen order, so a sort below it
/// still orders the deduplicated rows -- the first row of each group is the
/// one the sort put first.
fn distinct_over(
    child: Box<dyn Executor>,
    schema: &Schema,
    ctx: &crate::StmtContext,
) -> HashAggExec<crate::StmtContext> {
    let group_by: Vec<Expression> = schema
        .columns
        .iter()
        .map(|column| Expression::Column(column.clone()))
        .collect();
    let agg_funcs: Vec<AggFunc> = group_by
        .iter()
        .map(|column| AggFunc::new(AggKind::FirstRow, Some(column.clone())))
        .collect();
    HashAggExec::new(
        ExecutorMeta::new(schema.clone(), 5, INIT_CAP, MAX_CHUNK_SIZE),
        group_by,
        agg_funcs,
        child,
        ctx.clone(),
    )
}

/// Evaluates a `LIMIT` bound, which must be a non-negative integer literal.
pub(crate) fn eval_limit_bound(expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
    match expr {
        tidb_ast::Expr::Int(text) => text
            .parse::<u64>()
            .map_err(|_| DriverError::Unsupported("LIMIT bound must be a non-negative integer")),
        _ => Err(DriverError::Unsupported(
            "LIMIT bound must be an integer literal",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_constant_arithmetic() {
        assert_eq!(
            run_select("SELECT 1 + 1").unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select("SELECT 1 + 1, 2 * 3").unwrap(),
            vec![vec![Datum::Int(2), Datum::Int(6)]]
        );
        assert_eq!(
            run_select("SELECT 2 * 3 - 1").unwrap(),
            vec![vec![Datum::Int(5)]]
        );
    }

    #[test]
    fn select_with_where() {
        // A true predicate keeps the row.
        assert_eq!(
            run_select("SELECT 42 WHERE 1 = 1").unwrap(),
            vec![vec![Datum::Int(42)]]
        );
        // A false predicate yields no rows.
        assert_eq!(
            run_select("SELECT 42 WHERE 1 = 0").unwrap(),
            Vec::<Vec<Datum>>::new()
        );
    }

    #[test]
    fn limit_and_order_by_wire_up() {
        // LIMIT truncates / zeroes the single row.
        assert_eq!(
            run_select("SELECT 42 LIMIT 1").unwrap(),
            vec![vec![Datum::Int(42)]]
        );
        assert_eq!(
            run_select("SELECT 42 LIMIT 0").unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select("SELECT 42 LIMIT 1, 1").unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // ORDER BY over the single dual row passes through the sort.
        assert_eq!(
            run_select("SELECT 42 ORDER BY 1 DESC").unwrap(),
            vec![vec![Datum::Int(42)]]
        );
    }

    #[test]
    fn unknown_table_is_rejected() {
        assert!(matches!(
            run_select("SELECT a FROM missing"),
            Err(DriverError::Unsupported(_))
        ));
    }

    /// The split rule itself, at the shape boundary: a column-versus-constant
    /// comparison moves into the scan and everything else stays above it.
    #[test]
    fn the_scan_takes_comparisons_against_constants_and_nothing_else() {
        use tidb_datatype::FieldTypeCode;
        let scope = FromScope {
            tables: vec![FromTable {
                name: "t".to_owned(),
                database: None,
                columns: vec![
                    ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ],
                offset: 0,
                determinants: Vec::new(),
            }],
        };
        let split = |sql: &str| {
            let stmt = tidb_parser::parse(sql).expect("a select");
            let Stmt::Query(query) = &stmt else {
                panic!("a select");
            };
            let QueryStmt::Select(statement) = &**query else {
                panic!("a select");
            };
            let where_clause = statement.where_clause.clone().expect("a where clause");
            let (pushed, residual) =
                split_scan_predicates(&where_clause, &ScopeResolver { scope: &scope });
            (
                pushed
                    .comparisons()
                    .iter()
                    .map(|c| (c.column_offset, c.op, c.literal.clone(), c.column_on_left))
                    .collect::<Vec<_>>(),
                residual.map(|expr| expr.restore()),
            )
        };

        // Either operand order pushes, and the order is preserved.
        assert_eq!(
            split("SELECT 1 FROM t WHERE a > 5"),
            (vec![(0, ScanComparisonOp::Gt, Datum::Int(5), true)], None)
        );
        assert_eq!(
            split("SELECT 1 FROM t WHERE 5 < a"),
            (vec![(0, ScanComparisonOp::Lt, Datum::Int(5), false)], None)
        );
        // A qualified name resolves to the same column.
        assert_eq!(
            split("SELECT 1 FROM t WHERE t.b = 1").0,
            vec![(1, ScanComparisonOp::Eq, Datum::Int(1), true)]
        );
        // Mixed: the comparison pushes, the arithmetic does not.
        let (pushed, residual) = split("SELECT 1 FROM t WHERE a > 5 AND b + 1 < 10");
        assert_eq!(pushed, vec![(0, ScanComparisonOp::Gt, Datum::Int(5), true)]);
        assert!(residual.is_some(), "the arithmetic conjunct stays above");
        // Shapes that push nothing: a disjunction, a column-to-column
        // comparison, a NULL constant, an operator outside the accepted set.
        for sql in [
            "SELECT 1 FROM t WHERE a > 5 OR b < 10",
            "SELECT 1 FROM t WHERE a > b",
            "SELECT 1 FROM t WHERE a = NULL",
            "SELECT 1 FROM t WHERE a IS NULL",
            "SELECT 1 FROM t WHERE a <=> 5",
        ] {
            let (pushed, residual) = split(sql);
            assert!(pushed.is_empty(), "{sql} must not push");
            assert!(residual.is_some(), "{sql} keeps its whole predicate");
        }
    }

    fn test_catalog() -> Catalog {
        use tidb_datatype::FieldTypeCode;
        let mut catalog = Catalog::default();
        catalog.register(
            "t",
            MemTable {
                columns: vec![
                    ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ],
                rows: vec![
                    vec![Datum::Int(1), Datum::Int(30)],
                    vec![Datum::Int(2), Datum::Int(20)],
                    vec![Datum::Int(3), Datum::Int(10)],
                ],
            },
        );
        catalog
    }

    #[test]
    fn select_from_table() {
        let catalog = test_catalog();
        // Column projection.
        assert_eq!(
            run_select_on(
                "SELECT a FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ]
        );
        // Wildcard, qualified column, and an expression over columns.
        assert_eq!(
            run_select_on(
                "SELECT * FROM t WHERE t.a > 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(3), Datum::Int(10)],
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a + b FROM t WHERE a = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(22)]]
        );
    }

    #[test]
    fn insert_then_select_round_trip() {
        let mut catalog = test_catalog();
        // Full-row insert.
        assert_eq!(
            run_insert_on(
                "INSERT INTO t VALUES (4, 40), (5, 50)",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            2
        );
        // Column-list insert: unspecified column fills with NULL.
        assert_eq!(
            run_insert_on(
                "INSERT INTO t (a) VALUES (6)",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM t WHERE a > 3 ORDER BY a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(4), Datum::Int(40)],
                vec![Datum::Int(5), Datum::Int(50)],
                vec![Datum::Int(6), Datum::Null],
            ]
        );
        // Arity mismatch and unknown table are rejected.
        assert!(run_insert_on(
            "INSERT INTO t (a) VALUES (1, 2)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert!(run_insert_on(
            "INSERT INTO missing VALUES (1)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// The deployment-ladder proof: INSERT and SELECT round-trip through a
    /// table whose rows are genuine TiKV-format bytes (record keys + v2 row
    /// values), not a value matrix.
    #[test]
    fn sql_round_trips_through_real_tikv_bytes() {
        use crate::kv_table::{KvColumn, KvTable};
        use tidb_datatype::FieldTypeCode;
        let mut catalog = Catalog::default();
        catalog.register_kv(
            "kt",
            KvTable::new(
                77,
                vec![
                    KvColumn {
                        name: "a".to_owned(),
                        id: 1,
                        field_type: FieldType::new(FieldTypeCode::LongLong),
                        default_value: None,
                        // A column present at CREATE TABLE has no pre-existing rows.
                        origin_default: None,
                    },
                    KvColumn {
                        name: "b".to_owned(),
                        id: 2,
                        field_type: FieldType::new(FieldTypeCode::LongLong),
                        default_value: None,
                        // A column present at CREATE TABLE has no pre-existing rows.
                        origin_default: None,
                    },
                ],
            ),
        );

        assert_eq!(
            run_insert_on(
                "INSERT INTO kt VALUES (1, 10), (2, 20), (3, 30)",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            3
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM kt WHERE a > 1 ORDER BY b DESC",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(3), Datum::Int(30)],
                vec![Datum::Int(2), Datum::Int(20)],
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a + b FROM kt WHERE a = 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(11)]]
        );
    }

    #[test]
    fn aggregate_selects() {
        let catalog = test_catalog();
        // Global aggregates: rows (1,30),(2,20),(3,10).
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*), SUM(a) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            // SUM is a DECIMAL in MySQL even over a BIGINT column.
            vec![vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(6))
            ]]
        );
        // GROUP BY with a carried key column, WHERE below the agg.
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(1)],
                vec![Datum::Int(2), Datum::Int(1)],
            ]
        );
        // Empty-input rules through SQL: global agg over no rows -> one row.
        assert_eq!(
            run_select_on(
                "SELECT COUNT(a) FROM t WHERE a > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(0)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM t WHERE a > 100 GROUP BY a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // MIN/MAX over the shared datum ordering.
        assert_eq!(
            run_select_on(
                "SELECT MIN(a), MAX(b) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1), Datum::Int(30)]]
        );
        // AVG over integers is DECIMAL, scaled by div_precision_increment.
        assert_eq!(
            run_select_on(
                "SELECT AVG(a) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
                "2.0000"
            ))]]
        );
        // DISTINCT folds repeated inputs once per group: a is 1,2,3 while the
        // constant 1 collapses to a single counted value.
        assert_eq!(
            run_select_on(
                "SELECT COUNT(DISTINCT a), COUNT(DISTINCT 1) FROM t",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3), Datum::Int(1)]]
        );
        // An all-NULL / empty group is NULL for MIN/MAX and AVG, as in Go.
        assert_eq!(
            run_select_on(
                "SELECT MIN(a), MAX(a), AVG(a) FROM t WHERE a > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Null, Datum::Null, Datum::Null]]
        );
    }

    /// HAVING filters aggregate output rows, ORDER BY sorts them, and an
    /// aggregate that appears only in those clauses is computed as a hidden
    /// column and trimmed from the result (Go's resolveHavingAndOrderBy plus
    /// the final projection).
    #[test]
    fn aggregate_having_and_order_by() {
        let mut catalog = test_catalog();
        crate::run_create_table_on("CREATE TABLE g (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO g VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // HAVING over an aggregate that IS in the select list.
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(3), Datum::Int(2)],
            ]
        );
        // HAVING over an aggregate that is NOT selected: one output column.
        assert_eq!(
            run_select_on(
                "SELECT a FROM g GROUP BY a HAVING SUM(b) > 15",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // ORDER BY an aggregate that is not selected, descending.
        assert_eq!(
            run_select_on(
                "SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(3)],
                vec![Datum::Int(2)]
            ]
        );
        // HAVING and ORDER BY together, with LIMIT applied after both.
        assert_eq!(
            run_select_on(
                "SELECT a, SUM(b) FROM g GROUP BY a HAVING COUNT(*) > 1 ORDER BY SUM(b) LIMIT 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(15))
            ]]
        );
        // ORDER BY a selected alias.
        assert_eq!(
            run_select_on(
                "SELECT a, SUM(b) AS total FROM g GROUP BY a ORDER BY total",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5))
                ],
                vec![
                    Datum::Int(3),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(15))
                ],
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(30))
                ],
            ]
        );
        // A grouped column that is not selected is still visible to HAVING
        // and ORDER BY (Go carries it as a hidden FIRST_ROW column).
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        // A global aggregate's HAVING filters the single group.
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*) FROM g HAVING COUNT(*) > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
    }

    /// A bare integer in `UPDATE`/`DELETE ... ORDER BY` is a POSITIONAL
    /// reference to the table's own column at that 1-based position, not a
    /// constant. Captured via `zz_dump_parity_test.go`
    /// (`TestZZDumpParityDMLPositionalOrderBy`, run with
    /// `go test -tags=intest -run TestZZDumpParityDMLPositionalOrderBy
    /// ./pkg/executor/ -v`): on `t(a, b)` seeded with
    /// `(1,30),(2,20),(3,10)`, `UPDATE t SET a = a + 100 ORDER BY 2 LIMIT 1`
    /// updated the row with the SMALLEST `b` (`(3,10)` -> `(103,10)`), and
    /// `DELETE FROM t ORDER BY 2 LIMIT 1` removed that same smallest-`b`
    /// row. `2` resolves to column `b`, exactly like `SELECT`'s positional
    /// `ORDER BY`/`GROUP BY` against the select list -- there is no select
    /// list in a single-table `UPDATE`/`DELETE`, so it indexes the table's
    /// declared columns instead. Do not "fix" this back to a constant.
    #[test]
    fn dml_positional_order_by_resolves_to_column() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE t (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO t VALUES (1, 30), (2, 20), (3, 10)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        assert_eq!(
            run_update_on(
                "UPDATE t SET a = a + 100 ORDER BY 2 LIMIT 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM t ORDER BY b",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(103), Datum::Int(10)],
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(1), Datum::Int(30)],
            ]
        );

        crate::run_create_table_on("CREATE TABLE t2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO t2 VALUES (1, 30), (2, 20), (3, 10)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_delete_on(
                "DELETE FROM t2 ORDER BY 2 LIMIT 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM t2 ORDER BY b",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(1), Datum::Int(30)],
            ]
        );
    }

    /// UPDATE and DELETE over both table backings, including MySQL's
    /// affected-row rule: an UPDATE counts CHANGED rows, so a row whose new
    /// values equal its old ones is touched but not affected.
    #[test]
    fn update_and_delete_rows() {
        for kv in [false, true] {
            let mut catalog = Catalog::default();
            if kv {
                crate::run_create_table_on("CREATE TABLE w (a BIGINT, b BIGINT)", &mut catalog)
                    .unwrap();
            } else {
                catalog.register(
                    "w",
                    MemTable {
                        columns: vec![
                            ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                            ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                        ],
                        rows: vec![],
                    },
                );
            }
            run_insert_on(
                "INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();

            // WHERE-selected update, counting only changed rows.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET b = b + 1 WHERE a >= 2",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                2,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a, b FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![
                    vec![Datum::Int(1), Datum::Int(10)],
                    vec![Datum::Int(2), Datum::Int(21)],
                    vec![Datum::Int(3), Datum::Int(31)],
                ],
                "kv={kv}"
            );

            // A no-op update matches rows but changes none: MySQL reports 0.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET b = b WHERE a = 1",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                0,
                "kv={kv}"
            );

            // Every assignment reads the row as the statement found it, so
            // `b` takes the ORIGINAL `a` (1), not the just-assigned 7.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET a = 7, b = a WHERE a = 1",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                1,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a, b FROM w WHERE a = 7",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(7), Datum::Int(1)]],
                "kv={kv}"
            );

            // A WHERE-less UPDATE touches every row.
            assert_eq!(
                run_update_on(
                    "UPDATE w SET b = 0",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                3,
                "kv={kv}"
            );

            // DELETE removes the selected rows and reports their count.
            assert_eq!(
                run_delete_on(
                    "DELETE FROM w WHERE a >= 3",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                2,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(2)]],
                "kv={kv}"
            );

            // A WHERE-less DELETE empties the table, and re-inserting works
            // after it (the store is genuinely empty, not just filtered).
            assert_eq!(
                run_delete_on(
                    "DELETE FROM w",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                1,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on(
                    "SELECT a FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                Vec::<Vec<Datum>>::new(),
                "kv={kv}"
            );
            run_insert_on(
                "INSERT INTO w VALUES (9, 9)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(
                run_select_on(
                    "SELECT a FROM w",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(9)]],
                "kv={kv}"
            );

            // ORDER BY and LIMIT are supported now (see the session's
            // `insert_select_and_ordered_dml`); an unknown SET column and
            // the IGNORE form still fail closed.
            assert!(run_update_on(
                "UPDATE w SET zzz = 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .is_err());
            assert!(run_update_on(
                "UPDATE IGNORE w SET a = 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .is_err());
        }
    }

    /// Two-table joins: inner, left/right outer with NULL padding, the
    /// ON-vs-WHERE distinction, qualified and ambiguous column references,
    /// wildcard expansion, and a three-table left-deep chain.
    #[test]
    fn joins() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE l (id BIGINT, v BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE r (id BIGINT, w BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO r VALUES (1, 100), (3, 300), (3, 301)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // INNER JOIN: only matches, and a left row matching twice emits twice.
        assert_eq!(
            run_select_on(
                "SELECT l.id, l.v, r.w FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(10), Datum::Int(100)],
                vec![Datum::Int(3), Datum::Int(30), Datum::Int(300)],
                vec![Datum::Int(3), Datum::Int(30), Datum::Int(301)],
            ]
        );

        // LEFT JOIN pads the unmatched left row with NULLs.
        assert_eq!(
            run_select_on(
                "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(100)],
                vec![Datum::Int(2), Datum::Null],
                vec![Datum::Int(3), Datum::Int(300)],
                vec![Datum::Int(3), Datum::Int(301)],
            ]
        );

        // The ON/WHERE distinction: filtering the padded rows is an anti-join.
        assert_eq!(
            run_select_on(
                "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        // A condition in ON does NOT drop the left row; it only stops matching.
        assert_eq!(
            run_select_on(
                "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id AND r.w > 200",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Null],
                vec![Datum::Int(2), Datum::Null],
                vec![Datum::Int(3), Datum::Int(300)],
                vec![Datum::Int(3), Datum::Int(301)],
            ]
        );

        // RIGHT JOIN keeps every right row, padding the left side.
        assert_eq!(
            run_select_on(
                "SELECT l.v, r.id FROM l RIGHT JOIN r ON l.id = r.id AND l.v > 100",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Null, Datum::Int(1)],
                vec![Datum::Null, Datum::Int(3)],
                vec![Datum::Null, Datum::Int(3)],
            ]
        );

        // A comma join with no ON is a Cartesian product.
        assert_eq!(
            run_select_on(
                "SELECT l.id FROM l, r",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            9
        );

        // `*` expands across both tables in FROM order; `t.*` over one.
        assert_eq!(
            run_select_on(
                "SELECT * FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .first()
            .unwrap()
            .len(),
            4
        );
        assert_eq!(
            run_select_on(
                "SELECT r.* FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .first()
            .unwrap()
            .len(),
            2
        );

        // An unqualified column present in both tables is ambiguous, as in
        // MySQL; one present in only one table resolves.
        assert!(run_select_on(
            "SELECT id FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert_eq!(
            run_select_on(
                "SELECT v, w FROM l JOIN r ON l.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );

        // An alias replaces the table name for qualification.
        assert_eq!(
            run_select_on(
                "SELECT a.id FROM l AS a JOIN r AS b ON a.id = b.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );

        // A three-table left-deep chain, and an aggregate over a join.
        crate::run_create_table_on("CREATE TABLE m (id BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO m VALUES (3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*) FROM l JOIN r ON l.id = r.id JOIN m ON m.id = r.id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );

        // Unsupported join shapes fail closed.
        assert!(run_select_on(
            "SELECT * FROM l NATURAL JOIN r",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert!(run_select_on(
            "SELECT * FROM l JOIN r USING (id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// Uncorrelated subqueries are evaluated and folded into literals, the way
    /// Go's handleScalarSubquery does for the non-Apply case.
    #[test]
    fn subqueries() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE s (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE u (a BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (2), (3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A scalar subquery in the select list and in a predicate.
        assert_eq!(
            run_select_on(
                "SELECT (SELECT MAX(b) FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(30)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE b = (SELECT MAX(b) FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // No rows is NULL, as Go's buildMaxOneRow leaves it.
        assert_eq!(
            run_select_on(
                "SELECT (SELECT a FROM s WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Null]]
        );
        // More than one row is Go's ER_SUBQUERY_NO_1_ROW.
        assert!(matches!(
            run_select_on(
                "SELECT (SELECT a FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::SubqueryReturnsMoreThanOneRow)
        ));

        // IN / NOT IN over a subquery.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a IN (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // An empty IN subquery matches nothing, and NOT IN over it matches all.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a IN (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );

        // EXISTS / NOT EXISTS fold to 1 / 0.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE EXISTS (SELECT 1 FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE NOT EXISTS (SELECT 1 FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // A subquery in HAVING, over the aggregate path.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s GROUP BY a HAVING SUM(b) > (SELECT MIN(b) FROM s)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );

        // ANY is the OR chain over the folded values, ALL the AND chain:
        // `a > ANY (2, 3)` holds only for 3, and `a > ALL (2, 3)` for nothing.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ANY (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ALL (SELECT a FROM u)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // An empty inner result: ALL is vacuously true, ANY is false.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ALL (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a > ANY (SELECT a FROM u WHERE a > 100)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
    }

    /// A correlated subquery becomes an Apply: the inner query re-runs once
    /// per outer row with the outer row's values bound, which is Go's
    /// NestedLoopApplyExec loop.
    #[test]
    fn correlated_subqueries() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE o (id BIGINT, v BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE i (id BIGINT, w BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO o VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO i VALUES (1, 10), (2, 5), (2, 25), (4, 40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Scalar: each outer row compares against its own inner maximum.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v = (SELECT MAX(w) FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // id 2's inner rows are 5 and 25, so its max is 25 and 20 < 25 holds;
        // id 1 compares 10 < 10, which does not.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v < (SELECT MAX(w) FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        // An outer row whose inner query returns nothing compares against
        // NULL, so the predicate is unknown and the row drops -- id 3 has no
        // matching inner rows.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE (SELECT MAX(w) FROM i WHERE i.id = o.id) IS NULL",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // Correlated EXISTS / NOT EXISTS, the semi- and anti-join shapes.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // An unqualified inner reference to an outer column still correlates.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.w = v)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );

        // A correlated subquery returning several rows is still the 1242 case,
        // raised from inside the apply loop and reported as the same error the
        // folded path reports.
        assert!(matches!(
            run_select_on(
                "SELECT id FROM o WHERE v = (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::SubqueryReturnsMoreThanOneRow)
        ));

        // Correlated IN / NOT IN and ANY / ALL: the same Apply, folding this
        // outer row's inner result into the three-valued answer. id 3's inner
        // result is EMPTY, which is why NOT IN and ALL keep it.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v IN (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v NOT IN (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v > ANY (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v > ALL (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
    }

    /// A correlated subquery nested inside a larger aggregate-path
    /// expression: arithmetic over an aggregate in the select list, and a
    /// comparison against an aggregate in `HAVING`. The Apply sits above the
    /// aggregation (Go's plan shape), so the subquery sees the GROUPED value
    /// and runs once per group.
    ///
    /// Every result here was cross-checked against a
    /// `testkit.CreateMockStore` capture of real TiDB on the same schema.
    #[test]
    fn grouped_correlated_subqueries() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE t (g BIGINT, v BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE s (k BIGINT, x BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO t VALUES (1, 10), (1, 20), (2, 5), (3, 100), (NULL, 7)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO s VALUES (1, 1), (1, 2), (2, 3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A correlated scalar subquery combined with an aggregate by
        // arithmetic in the select list: SUM(v) is the group's own total,
        // (SELECT COUNT...) reads how many `s` rows share the group's key.
        assert_eq!(
            run_select_on(
                "SELECT g, SUM(v) + (SELECT COUNT(*) FROM s WHERE s.k = t.g) \
                 FROM t GROUP BY g ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![
                    Datum::Null,
                    Datum::Decimal(tidb_datatype::Decimal::from_int(7))
                ],
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(32))
                ],
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(6))
                ],
                vec![
                    Datum::Int(3),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(100))
                ],
            ]
        );

        // A correlated scalar subquery compared against an aggregate in
        // HAVING: only groups whose SUM(v) beats the correlated average
        // survive.
        assert_eq!(
            run_select_on(
                "SELECT g FROM t GROUP BY g \
                 HAVING SUM(v) > (SELECT AVG(x) FROM s WHERE s.k = t.g) \
                 ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );

        // The same HAVING subquery, ANDed with a plain grouped-column
        // predicate -- both conjuncts must be readable off the same
        // post-Apply row.
        assert_eq!(
            run_select_on(
                "SELECT g, SUM(v) FROM t GROUP BY g \
                 HAVING SUM(v) > (SELECT COUNT(*) FROM s WHERE s.k = t.g) AND g IS NOT NULL \
                 ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(30))
                ],
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5))
                ],
                vec![
                    Datum::Int(3),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(100))
                ],
            ]
        );

        // HAVING a correlated subquery against a bare (unaggregated) GROUP
        // BY column, with a NULL group in the mix.
        assert_eq!(
            run_select_on(
                "SELECT g, COUNT(*) FROM t GROUP BY g \
                 HAVING (SELECT COUNT(*) FROM s WHERE s.k = g) >= 0 \
                 ORDER BY g",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Null, Datum::Int(1)],
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(2), Datum::Int(1)],
                vec![Datum::Int(3), Datum::Int(1)],
            ]
        );

        // DEFERRED (documented, not silently wrong): a correlated subquery
        // inside an AGGREGATE'S OWN ARGUMENT needs a per-SOURCE-ROW Apply
        // below the aggregation, not the per-GROUP Apply above it this
        // driver builds -- refused precisely rather than mis-evaluated.
        assert!(matches!(
            run_select_on(
                "SELECT g, SUM((SELECT COUNT(*) FROM s WHERE s.k = g)) FROM t GROUP BY g",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::Unsupported(_))
        ));
        assert!(matches!(
            run_select_on(
                "SELECT g, SUM(CASE WHEN EXISTS(SELECT 1 FROM s WHERE s.k = t.g) THEN v ELSE 0 END) \
                 FROM t GROUP BY g",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::Exec(_))
        ));

        // A HAVING clause referencing a non-grouped, non-aggregated column
        // stays refused even with a correlated subquery alongside it -- the
        // subquery does not launder the column reference. Captured from
        // TiDB, this is `ErrUnknownColumn` naming the `having clause` (HAVING
        // resolves against the aggregation's output), in every sql_mode.
        assert!(matches!(
            run_select_on(
                "SELECT g, SUM(v) FROM t GROUP BY g \
                 HAVING v > (SELECT AVG(x) FROM s WHERE s.k = t.g)",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::UnknownColumnInClause { .. })
        ));

        // DEFERRED: two-level nesting (a correlated subquery whose own body
        // contains a subquery correlated to ITS outer scope) is refused
        // rather than mis-evaluated.
        assert!(run_select_on(
            "SELECT g, (SELECT COUNT(*) FROM s WHERE s.k = t.g \
             AND s.x > (SELECT AVG(x) FROM s s2 WHERE s2.k = s.k)) FROM t GROUP BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// A single-column integer PRIMARY KEY becomes the row handle (Go's
    /// TableInfo.PKIsHandle), so the key value addresses the row and a repeat
    /// is ErrDupEntry. Transcreated from Go's own duplicate-key behavior in
    /// pkg/table/tables `AddRecord`.
    #[test]
    fn integer_primary_key_is_the_row_handle() {
        for ddl in [
            "CREATE TABLE p (id BIGINT PRIMARY KEY, v BIGINT)",
            "CREATE TABLE p (id BIGINT, v BIGINT, PRIMARY KEY (id))",
        ] {
            let mut catalog = Catalog::default();
            crate::run_create_table_on(ddl, &mut catalog).unwrap();
            run_insert_on(
                "INSERT INTO p VALUES (10, 100), (20, 200)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();

            // The rows come back in handle order, which is the key's order --
            // not insertion order, because the handle IS the primary key.
            run_insert_on(
                "INSERT INTO p VALUES (5, 50)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(
                run_select_on(
                    "SELECT id FROM p",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![
                    vec![Datum::Int(5)],
                    vec![Datum::Int(10)],
                    vec![Datum::Int(20)],
                ],
                "{ddl}"
            );

            // A repeated key is Go's ErrDupEntry.
            assert!(
                matches!(
                    run_insert_on(
                        "INSERT INTO p VALUES (10, 999)",
                        &mut catalog,
                        &crate::StmtContext::for_query()
                    ),
                    Err(DriverError::DuplicateEntry { .. })
                ),
                "{ddl}"
            );
            // The failed insert left the original row untouched.
            assert_eq!(
                run_select_on(
                    "SELECT v FROM p WHERE id = 10",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap(),
                vec![vec![Datum::Int(100)]],
                "{ddl}"
            );
            // A negative key works too: the key codec sign-flips handles.
            run_insert_on(
                "INSERT INTO p VALUES (-1, 1)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(
                run_select_on(
                    "SELECT id FROM p",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap()
                .len(),
                4,
                "{ddl}"
            );
        }
    }

    /// Without a primary key the handle is the allocated row id, so repeated
    /// values are fine -- the table is a heap, as in Go with _tidb_rowid.
    #[test]
    fn without_a_primary_key_rows_repeat_freely() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE h (a BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO h VALUES (1), (1), (1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a FROM h",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );
    }

    /// Constraint shapes that need tiers this seed lacks are rejected rather
    /// than silently dropped, so a table never claims what it cannot enforce.
    #[test]
    fn unsupported_constraints_are_rejected() {
        let mut catalog = Catalog::default();
        for ddl in [
            // Two primary keys is not a table.
            "CREATE TABLE c (a BIGINT PRIMARY KEY, b BIGINT PRIMARY KEY)",
            // A prefix-length primary key needs prefix index support.
            "CREATE TABLE c (a VARCHAR(10), PRIMARY KEY (a(3)))",
        ] {
            assert!(
                crate::run_create_table_on(ddl, &mut catalog).is_err(),
                "{ddl} should be rejected"
            );
        }
    }

    /// A non-integer primary key is not a handle -- Go only sets PKIsHandle
    /// for a single integer column -- so the table keeps allocating row ids
    /// and enforces the key through a unique index instead.
    #[test]
    fn a_non_integer_primary_key_is_enforced_by_its_index() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE s (k VARCHAR(10) PRIMARY KEY)", &mut catalog)
            .unwrap();
        run_insert_on(
            "INSERT INTO s VALUES ('a'), ('b')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT k FROM s",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );
        // The duplicate is now caught by the index, as in real TiDB.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO s VALUES ('a')",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));
    }

    /// The text of a string datum, however the codec chose to represent it.
    fn datum_text_for_test(value: &Datum) -> String {
        match value {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
            other => panic!("expected a string datum, got {other:?}"),
        }
    }

    /// UNIQUE indexes are enforced on every write path, and MySQL's rule that
    /// a unique index permits any number of NULLs is Go's `distinct` flag:
    /// an entry with a NULL indexed value is stored the non-distinct way and
    /// never collides.
    #[test]
    fn unique_indexes() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE u (id BIGINT PRIMARY KEY, email VARCHAR(32) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (1, 'a@x', 10), (2, 'b@x', 20)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A repeated unique value is rejected, naming the index.
        match run_insert_on(
            "INSERT INTO u VALUES (3, 'a@x', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        ) {
            Err(DriverError::DuplicateEntry { value, key }) => {
                assert_eq!(value, "a@x");
                // Captured from TiDB: the key is qualified table.index, as in
                // "Duplicate entry 'a' for key 'm.code'".
                assert_eq!(key, "u.email");
            }
            other => panic!("expected a duplicate-entry error, got {other:?}"),
        }
        // The rejected insert wrote nothing.
        assert_eq!(
            run_select_on(
                "SELECT id FROM u",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );

        // UPDATE is checked too, and a rejected update leaves the row alone.
        assert!(matches!(
            run_update_on(
                "UPDATE u SET email = 'a@x' WHERE id = 2",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));
        assert_eq!(
            datum_text_for_test(
                &run_select_on(
                    "SELECT email FROM u WHERE id = 2",
                    &catalog,
                    &crate::StmtContext::for_query()
                )
                .unwrap()[0][0]
            ),
            "b@x"
        );
        // An update that frees a value lets another row take it.
        run_update_on(
            "UPDATE u SET email = 'c@x' WHERE id = 1",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (4, 'a@x', 40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // DELETE frees the value as well.
        run_delete_on(
            "DELETE FROM u WHERE id = 4",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (5, 'a@x', 50)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // MySQL permits many NULLs in a unique index.
        run_insert_on(
            "INSERT INTO u VALUES (6, NULL, 60)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u VALUES (7, NULL, 70)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM u",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            5
        );
    }

    /// A non-unique index accepts repeats: its key carries the handle, so two
    /// rows with the same value are two entries (Go's non-distinct path).
    #[test]
    fn a_non_unique_index_accepts_repeats() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE n (id BIGINT PRIMARY KEY, tag VARCHAR(8), KEY tag_idx (tag))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO n VALUES (1, 'x'), (2, 'x'), (3, 'y')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM n",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            3
        );
    }

    /// A unique index stores the handle as its value, which is what makes a
    /// unique-key lookup a point read (Go's PointGetPlan on a unique key).
    #[test]
    fn a_unique_index_entry_points_at_its_row() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE k (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO k VALUES (7, 'abc'), (8, 'def')",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("k") else {
            panic!("expected a kv table");
        };
        let mut table = table.clone();
        let index_id = table
            .indexes()
            .iter()
            .find(|index| index.name == "code")
            .expect("the unique index exists")
            .id;
        assert_eq!(
            table
                .lookup_unique(index_id, &[Datum::Bytes(b"abc".to_vec())])
                .unwrap(),
            Some(TableHandle::Int(7)),
            "the entry carries the row's handle"
        );
        assert_eq!(
            table
                .lookup_unique(index_id, &[Datum::Bytes(b"nope".to_vec())])
                .unwrap(),
            None
        );
    }

    /// Go's TryFastPlan: a single-table SELECT whose WHERE pins the handle or
    /// a whole unique index reads one row instead of scanning. The results
    /// must be identical to the scan in every case, including the cases that
    /// do NOT qualify and fall back.
    #[test]
    fn point_get_plans() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE g (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO g VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Handle point get.
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(20)]]
        );
        // A handle that does not exist reads nothing.
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 99",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // Unique-index point get, through the entry's stored handle.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE code = 'c'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE code = 'zz'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // The WHERE stays in the pipeline, so an extra condition still
        // filters: the point get narrows the source, it does not replace the
        // filter.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 AND v = 20",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 AND v = 999",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // Shapes that do not qualify fall back to the scan and stay correct.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE v = 30",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]],
            "a non-key column is not a point get"
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id > 1 ORDER BY id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]],
            "a range is not a point get"
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 1 OR id = 3",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(3)]],
            "Go recurses only through AND, so OR is not a point get"
        );
        // Go rejects the fast plan when ORDER BY or HAVING is present, or when
        // LIMIT could remove the row; the answers stay right either way.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 LIMIT 1 OFFSET 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 2 ORDER BY id",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );

        // A non-integer constant cannot name an integer handle: no row, not a
        // wrong row.
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE id = 'x'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // A point get sees writes, including the row a DELETE removed.
        run_update_on(
            "UPDATE g SET v = 99 WHERE id = 2",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(99)]]
        );
        run_delete_on(
            "DELETE FROM g WHERE id = 2",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM g WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM g WHERE code = 'b'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new(),
            "the deleted row's index entry is gone too"
        );
    }

    /// The results above would be right even if the fast plan never fired, so
    /// this asserts the DECISION: which shapes Go's tryPointGetPlan accepts
    /// and which it rejects.
    #[test]
    fn point_get_is_chosen_only_for_the_shapes_go_accepts() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE d (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO d VALUES (1, 'a', 10)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("d") else {
            panic!("expected a kv table");
        };
        let columns = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.field_type.clone()))
            .collect::<Vec<_>>();

        let decides = |sql: &str| {
            let stmt = tidb_parser::parse(sql).unwrap();
            let Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            try_point_get(select, table, &columns).unwrap()
        };

        // Accepted: the handle, and a whole unique index.
        assert_eq!(
            decides("SELECT v FROM d WHERE id = 1"),
            Some(Some(TableHandle::Int(1)))
        );
        assert_eq!(
            decides("SELECT v FROM d WHERE 1 = id"),
            Some(Some(TableHandle::Int(1)))
        );
        assert_eq!(
            decides("SELECT v FROM d WHERE code = 'a'"),
            Some(Some(TableHandle::Int(1)))
        );
        // The handle path does not probe: it hands the plan the handle the
        // constant names, and the row read finds nothing. The index path does
        // probe, because the handle only exists in an index entry.
        assert_eq!(
            decides("SELECT v FROM d WHERE id = 7"),
            Some(Some(TableHandle::Int(7)))
        );
        assert_eq!(decides("SELECT v FROM d WHERE code = 'z'"), Some(None));
        // The index path allows extra pairs beyond the key.
        assert_eq!(
            decides("SELECT v FROM d WHERE code = 'a' AND v = 10"),
            Some(Some(TableHandle::Int(1)))
        );

        // Rejected, so the scan runs: Go requires the handle pair to be the
        // ONLY pair, a conjunction of equalities, no ORDER BY or HAVING, and
        // a LIMIT that cannot drop the row.
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 AND v = 10"), None);
        assert_eq!(decides("SELECT v FROM d WHERE v = 10"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id > 1"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 OR id = 2"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 ORDER BY v"), None);
        assert_eq!(decides("SELECT v FROM d WHERE id = 1 LIMIT 0"), None);
        assert_eq!(
            decides("SELECT v FROM d WHERE id = 1 LIMIT 1 OFFSET 1"),
            None
        );
        assert_eq!(decides("SELECT v FROM d"), None);
    }

    /// A composite-index range spans several datums per bound, an IN list
    /// produces several ranges, and an OR unions them. The answers must be
    /// the same rows a full scan would return -- a range that reads too few
    /// rows is invisible to the range text alone.
    #[test]
    fn multi_column_and_multi_range_scans_read_the_same_rows_as_a_full_scan() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE m (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ab (a, b))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO m VALUES (1, 1, 1), (2, 1, 5), (3, 1, 9), (4, 2, 5), \
             (5, 3, 5), (6, NULL, 1), (7, 2, NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let ids = |sql: &str| {
            let mut ids: Vec<i64> = run_select_on(sql, &catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(v) => v,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect();
            ids.sort_unstable();
            ids
        };

        // Equality on the leading column plus a range on the next.
        assert_eq!(ids("SELECT id FROM m WHERE a = 1 AND b > 1"), vec![2, 3]);
        assert_eq!(
            ids("SELECT id FROM m WHERE a = 1 AND b BETWEEN 1 AND 5"),
            vec![1, 2]
        );
        // An IN list on the leading column: several point ranges, each
        // extended by the equality on the next column.
        assert_eq!(
            ids("SELECT id FROM m WHERE a IN (1, 3) AND b = 5"),
            vec![2, 5]
        );
        // A disjunction: the branches' ranges are unioned.
        assert_eq!(
            ids("SELECT id FROM m WHERE (a = 1 AND b = 5) OR (a = 3 AND b = 5)"),
            vec![2, 5]
        );
        // A NULL in the indexed columns is reachable only through IS NULL,
        // never through a comparison.
        assert_eq!(ids("SELECT id FROM m WHERE a IS NULL"), vec![6]);
        assert_eq!(ids("SELECT id FROM m WHERE a = 2 AND b IS NULL"), vec![7]);
        // The residual half still filters: `id` is not in the index, so the
        // range cannot express it and the Selection above must.
        assert_eq!(ids("SELECT id FROM m WHERE a = 1 AND id > 1"), vec![2, 3]);
    }

    /// Index range scans: a comparison on an indexed column reads the rows the
    /// index covers instead of scanning the table, with Go's range semantics.
    #[test]
    fn index_range_scans() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE r (id BIGINT PRIMARY KEY, score BIGINT, KEY score_idx (score))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO r VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let ids = |sql: &str, catalog: &Catalog| {
            let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect();
            got.sort_unstable();
            got
        };

        assert_eq!(
            ids("SELECT id FROM r WHERE score > 15", &catalog),
            vec![2, 3, 4]
        );
        assert_eq!(
            ids("SELECT id FROM r WHERE score >= 20", &catalog),
            vec![2, 3, 4]
        );
        assert_eq!(
            ids("SELECT id FROM r WHERE score < 30", &catalog),
            vec![1, 2, 4]
        );
        assert_eq!(ids("SELECT id FROM r WHERE score <= 10", &catalog), vec![1]);
        assert_eq!(
            ids("SELECT id FROM r WHERE score = 20", &catalog),
            vec![2, 4]
        );
        // The constant may sit on the left, with the operator flipped.
        assert_eq!(
            ids("SELECT id FROM r WHERE 15 < score", &catalog),
            vec![2, 3, 4]
        );

        // Several conditions on the column intersect into one range.
        assert_eq!(
            ids("SELECT id FROM r WHERE score > 10 AND score < 30", &catalog),
            vec![2, 4]
        );
        assert_eq!(
            ids(
                "SELECT id FROM r WHERE score >= 20 AND score <= 20",
                &catalog
            ),
            vec![2, 4]
        );

        // Go's ranges start at MinNotNull, so a NULL satisfies no comparison
        // -- row 5 never appears, and IS NULL still finds it through the scan.
        assert_eq!(
            ids("SELECT id FROM r WHERE score > -100", &catalog),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            ids("SELECT id FROM r WHERE score IS NULL", &catalog),
            vec![5]
        );

        // A condition the ranges do not consume still filters, because the
        // WHERE stays above the read.
        assert_eq!(
            ids("SELECT id FROM r WHERE score > 15 AND id = 3", &catalog),
            vec![3]
        );

        // Writes are visible to a later range scan, including through the
        // index entries a DELETE removed.
        run_update_on(
            "UPDATE r SET score = 99 WHERE id = 1",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(ids("SELECT id FROM r WHERE score > 50", &catalog), vec![1]);
        run_delete_on(
            "DELETE FROM r WHERE id = 1",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            ids("SELECT id FROM r WHERE score > 50", &catalog),
            Vec::<i64>::new()
        );
    }

    /// A range scan over a UNIQUE index reads its handles out of the entry
    /// VALUES, not the key, so this covers the other half of the entry format
    /// -- including the NULL entries a unique index stores non-distinctly.
    #[test]
    fn index_range_scan_over_a_unique_index() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE u2 (id BIGINT PRIMARY KEY, code BIGINT UNIQUE)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u2 VALUES (1, 100), (2, 200), (3, 300), (4, NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let mut ids: Vec<Datum> = run_select_on(
            "SELECT id FROM u2 WHERE code >= 200",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap()
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
        ids.sort_by_key(|value| match value {
            Datum::Int(v) => *v,
            other => panic!("expected an int, got {other:?}"),
        });
        assert_eq!(ids, vec![Datum::Int(2), Datum::Int(3)]);
        // The NULL row is reachable, just never through a comparison.
        assert_eq!(
            run_select_on(
                "SELECT id FROM u2 WHERE code IS NULL",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(4)]]
        );
    }

    /// The answers above would be right even from a full scan, so this asserts
    /// the DECISION and the ranges themselves.
    #[test]
    fn index_ranges_are_built_the_way_go_builds_them() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE q (id BIGINT PRIMARY KEY, score BIGINT, note VARCHAR(8), KEY s (score))",
            &mut catalog,
        )
        .unwrap();
        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("q") else {
            panic!("expected a kv table");
        };
        let columns = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.field_type.clone()))
            .collect::<Vec<_>>();
        let ranges = |sql: &str| {
            let stmt = tidb_parser::parse(sql).unwrap();
            let Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            try_index_ranges(select, table, &columns)
        };

        // Go: GT is (v, MaxValue], LT is [MinNotNull, v).
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > 5"),
            Some((
                1,
                vec![IndexRange {
                    low: vec![Datum::Int(5)],
                    high: vec![Datum::MaxValue],
                    low_exclusive: true,
                    high_exclusive: false,
                }]
            ))
        );
        assert_eq!(
            ranges("SELECT id FROM q WHERE score < 5"),
            Some((
                1,
                vec![IndexRange {
                    low: vec![Datum::MinNotNull],
                    high: vec![Datum::Int(5)],
                    low_exclusive: false,
                    high_exclusive: true,
                }]
            ))
        );
        // An intersection keeps the tighter end of each side.
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > 5 AND score <= 9"),
            Some((
                1,
                vec![IndexRange {
                    low: vec![Datum::Int(5)],
                    high: vec![Datum::Int(9)],
                    low_exclusive: true,
                    high_exclusive: false,
                }]
            ))
        );
        // A NULL constant matches nothing, which Go represents as no ranges.
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > NULL"),
            Some((1, vec![]))
        );

        // An OR is detached branch by branch and the branches' ranges are
        // unioned (Go `detachDNFCondAndBuildRangeForIndex` + `UnionRanges`).
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > 1 OR score < 0"),
            Some((
                1,
                vec![
                    IndexRange {
                        low: vec![Datum::MinNotNull],
                        high: vec![Datum::Int(0)],
                        low_exclusive: false,
                        high_exclusive: true,
                    },
                    IndexRange {
                        low: vec![Datum::Int(1)],
                        high: vec![Datum::MaxValue],
                        low_exclusive: true,
                        high_exclusive: false,
                    }
                ]
            ))
        );

        // No usable index: an unindexed column, or no WHERE at all.
        assert_eq!(ranges("SELECT id FROM q WHERE note = 'x'"), None);
        assert_eq!(ranges("SELECT id FROM q"), None);
    }

    /// Column defaults and the NOT NULL rules, following Go's fillColValue
    /// and CheckNotNull: an omitted column takes its DEFAULT, an omitted NOT
    /// NULL column with no DEFAULT is ErrNoDefaultForField, and an explicit
    /// NULL into a NOT NULL column is the different ErrColumnCantNull.
    #[test]
    fn column_defaults_and_not_null() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE d (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, \
             w BIGINT DEFAULT 7, s VARCHAR(4) DEFAULT 'zz', plain BIGINT)",
            &mut catalog,
        )
        .unwrap();

        // Omitted columns take their defaults; a nullable one with no DEFAULT
        // is NULL.
        run_insert_on(
            "INSERT INTO d (id, n) VALUES (1, 5)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let row = &run_select_on(
            "SELECT w, s, plain FROM d WHERE id = 1",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap()[0];
        assert_eq!(row[0], Datum::Int(7));
        assert_eq!(datum_text_for_test(&row[1]), "zz");
        assert_eq!(row[2], Datum::Null);

        // An explicit value overrides the default.
        run_insert_on(
            "INSERT INTO d (id, n, w) VALUES (2, 5, 100)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT w FROM d WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(100)]]
        );

        // An omitted NOT NULL column with no default is 1364.
        assert!(matches!(
            run_insert_on("INSERT INTO d (id) VALUES (3)", &mut catalog, &crate::StmtContext::for_query()),
            Err(DriverError::NoDefaultForField(name)) if name == "n"
        ));
        // An explicit NULL into that column is the other error, 1048.
        assert!(matches!(
            run_insert_on("INSERT INTO d (id, n) VALUES (3, NULL)", &mut catalog, &crate::StmtContext::for_query()),
            Err(DriverError::ColumnCannotBeNull(name)) if name == "n"
        ));
        // A NULL into a nullable column is fine.
        run_insert_on(
            "INSERT INTO d (id, n, plain) VALUES (3, 5, NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A DEFAULT NULL column is not the same as no DEFAULT: it is
        // omittable even when the column is otherwise unconstrained.
        crate::run_create_table_on(
            "CREATE TABLE e (id BIGINT PRIMARY KEY, v BIGINT DEFAULT NULL)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO e (id) VALUES (1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM e",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Null]]
        );

        // A primary key is NOT NULL, so omitting it is 1364 as well.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO e (v) VALUES (1)",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::NoDefaultForField(_))
        ));

        // An AUTO_INCREMENT column supplies its own value, so omitting it is
        // never the missing-default case (see the auto_increment test).
        crate::run_create_table_on("CREATE TABLE f (a BIGINT AUTO_INCREMENT)", &mut catalog)
            .unwrap();
        run_insert_on(
            "INSERT INTO f () VALUES ()",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .or_else(|_| {
            run_insert_on(
                "INSERT INTO f VALUES (NULL)",
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
        })
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a FROM f",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // A generated column is still rejected rather than ignored.
        assert!(crate::run_create_table_on(
            "CREATE TABLE g2 (a BIGINT, b BIGINT GENERATED ALWAYS AS (a+1) VIRTUAL)",
            &mut catalog
        )
        .is_err());
    }

    /// A primary key that is not a single integer column becomes a clustered
    /// COMMON handle: its encoding IS the row key, so rows scan in key order,
    /// the columns live in the key rather than the value, and a repeat is a
    /// duplicate (Go's IsCommonHandle path in addRecord).
    #[test]
    fn clustered_common_handle() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE c (k VARCHAR(8) PRIMARY KEY, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO c VALUES ('b', 2), ('a', 1), ('c', 3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Key order, not insertion order -- the key IS the primary key.
        assert_eq!(
            run_select_on(
                "SELECT k, v FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .into_iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );
        // The key column round-trips even though the value omits it.
        assert_eq!(
            run_select_on(
                "SELECT v FROM c WHERE k = 'b'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        // A repeated key is a duplicate.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO c VALUES ('a', 9)",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));

        // Writes address the row through its clustered key.
        run_update_on(
            "UPDATE c SET v = 20 WHERE k = 'b'",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT v FROM c WHERE k = 'b'",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(20)]]
        );
        run_delete_on(
            "DELETE FROM c WHERE k = 'a'",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT k FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );
        // The freed key can be inserted again.
        run_insert_on(
            "INSERT INTO c VALUES ('a', 1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A multi-column primary key is a clustered common handle too.
        crate::run_create_table_on(
            "CREATE TABLE m (a BIGINT, b VARCHAR(4), v BIGINT, PRIMARY KEY (a, b))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO m VALUES (1, 'y', 10), (1, 'x', 20), (2, 'a', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM m",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .into_iter()
            .map(|row| format!("{:?}/{}", row[0], datum_text_for_test(&row[1])))
            .collect::<Vec<_>>(),
            vec![
                "Int(1)/x".to_owned(),
                "Int(1)/y".to_owned(),
                "Int(2)/a".to_owned()
            ]
        );
        // Only the whole key must be unique; a repeated leading column is fine.
        assert!(matches!(
            run_insert_on(
                "INSERT INTO m VALUES (1, 'x', 99)",
                &mut catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::DuplicateEntry { .. })
        ));
        run_insert_on(
            "INSERT INTO m VALUES (1, 'z', 40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // A secondary index over a clustered table stores the common handle
        // and still resolves to its row.
        crate::run_create_table_on(
            "CREATE TABLE s (k VARCHAR(4) PRIMARY KEY, tag BIGINT, KEY tag_idx (tag))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO s VALUES ('p', 1), ('q', 2)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT k FROM s WHERE tag >= 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .into_iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect::<Vec<_>>(),
            vec!["q".to_owned()]
        );
    }

    /// AUTO_INCREMENT, checked against behavior captured from real TiDB:
    /// inserting 1,2 then an explicit 100 rebases the allocator, so the next
    /// rows are 101, 102, 103 -- NULL and 0 both allocate.
    #[test]
    fn auto_increment() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE a1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 (v) VALUES (10), (20)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 VALUES (100, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 (v) VALUES (40)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a1 VALUES (NULL, 50), (0, 60)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // Captured from TiDB: [[1 10] [2 20] [100 30] [101 40] [102 50] [103 60]]
        assert_eq!(
            run_select_on(
                "SELECT id, v FROM a1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(100), Datum::Int(30)],
                vec![Datum::Int(101), Datum::Int(40)],
                vec![Datum::Int(102), Datum::Int(50)],
                vec![Datum::Int(103), Datum::Int(60)],
            ]
        );

        // TiDB does NOT require the auto column to be a key -- captured, and
        // unlike MySQL, which raises 1075 for it.
        crate::run_create_table_on(
            "CREATE TABLE bad (a BIGINT AUTO_INCREMENT, b BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO bad (b) VALUES (1), (2)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a FROM bad",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );

        // A second auto column is Go's 1075, and a non-integer one is its
        // "Incorrect column specifier" -- both captured from TiDB.
        assert!(matches!(
            crate::run_create_table_on(
                "CREATE TABLE two (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT AUTO_INCREMENT)",
                &mut catalog
            ),
            Err(DriverError::WrongAutoKey)
        ));
        assert!(matches!(
            crate::run_create_table_on(
                "CREATE TABLE strk (a VARCHAR(4) AUTO_INCREMENT PRIMARY KEY)",
                &mut catalog
            ),
            Err(DriverError::WrongColumnSpecifier(_))
        ));
    }

    /// Go's tryWhereIn2BatchPointGet: `col IN (constants)` over the handle or
    /// a single-column unique index reads those rows directly. Results must
    /// match the scan in every case, including the shapes Go rejects.
    #[test]
    fn batch_point_get() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE b (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO b VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let ids = |sql: &str, catalog: &Catalog| {
            let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect();
            got.sort_unstable();
            got
        };

        // Handle path, including a value that matches nothing.
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (1, 3)", &catalog),
            vec![1, 3]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (3, 99)", &catalog),
            vec![3]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (99)", &catalog),
            Vec::<i64>::new()
        );
        // Unique-index path.
        assert_eq!(
            ids("SELECT id FROM b WHERE code IN ('a', 'c')", &catalog),
            vec![1, 3]
        );

        // Shapes Go rejects fall back to the scan and stay correct: NOT IN,
        // a non-key column, and an IN with anything else in the WHERE.
        assert_eq!(
            ids("SELECT id FROM b WHERE id NOT IN (1, 3)", &catalog),
            vec![2]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE v IN (20, 30)", &catalog),
            vec![2, 3]
        );
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (1, 3) AND v = 30", &catalog),
            vec![3]
        );
        // Go also rejects it with ORDER BY, LIMIT or DISTINCT present.
        assert_eq!(
            ids("SELECT id FROM b WHERE id IN (3, 1) ORDER BY id", &catalog),
            vec![1, 3]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM b WHERE id IN (1, 2, 3) LIMIT 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            2
        );
    }

    /// The answers above would be right from a scan too, so this asserts the
    /// DECISION: which shapes Go's batch point get claims.
    #[test]
    fn batch_point_get_is_chosen_only_for_the_shapes_go_accepts() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE bd (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO bd VALUES (1, 'a', 10)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("bd") else {
            panic!("expected a kv table");
        };
        let columns = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.field_type.clone()))
            .collect::<Vec<_>>();
        let decides = |sql: &str| {
            let stmt = tidb_parser::parse(sql).unwrap();
            let Stmt::Query(query) = &stmt else {
                panic!("not a query")
            };
            let QueryStmt::Select(select) = &**query else {
                panic!("not a select")
            };
            try_batch_point_get(select, table, &columns).unwrap()
        };

        assert_eq!(
            decides("SELECT v FROM bd WHERE id IN (1, 2)"),
            Some(vec![TableHandle::Int(1), TableHandle::Int(2)]),
            "the handle path does not probe, as the single point get does not"
        );
        assert_eq!(
            decides("SELECT v FROM bd WHERE code IN ('a', 'zz')"),
            Some(vec![TableHandle::Int(1)]),
            "the index path probes, so a missing key yields no handle"
        );
        // Rejected shapes.
        assert_eq!(decides("SELECT v FROM bd WHERE id NOT IN (1)"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE v IN (1)"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) AND v = 1"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) ORDER BY v"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) LIMIT 1"), None);
        assert_eq!(decides("SELECT DISTINCT v FROM bd WHERE id IN (1)"), None);
        assert_eq!(decides("SELECT v FROM bd WHERE id = 1"), None);
    }

    /// SELECT DISTINCT deduplicates the projected rows, which Go builds as an
    /// aggregation grouping by every projected column with FIRST_ROW
    /// aggregates. The plain path silently returned duplicates before.
    #[test]
    fn select_distinct() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE d2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO d2 VALUES (1, 1), (1, 2), (1, 1), (2, 2)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        // Every projected column takes part, so (1,1) collapses but (1,2)
        // stays.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a, b FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(1)],
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(2), Datum::Int(2)],
            ]
        );
        // Without DISTINCT every row survives.
        assert_eq!(
            run_select_on(
                "SELECT a FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            4
        );

        // DISTINCT applies to the projected expression, not the source rows.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a + b FROM d2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(2)],
                vec![Datum::Int(3)],
                vec![Datum::Int(4)]
            ]
        );

        // The dedup emits groups in first-seen order, so a sort below it still
        // orders the surviving rows.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2 ORDER BY a DESC",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(1)]]
        );
        // LIMIT applies after the dedup.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2 LIMIT 1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // A WHERE below it still filters.
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT a FROM d2 WHERE b = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );

        // Over an aggregate result, DISTINCT deduplicates the output rows.
        crate::run_create_table_on("CREATE TABLE g3 (k BIGINT, v BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO g3 VALUES (1, 5), (2, 5), (3, 9)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT DISTINCT SUM(v) FROM g3 GROUP BY k",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Decimal(tidb_datatype::Decimal::from_int(5))],
                vec![Datum::Decimal(tidb_datatype::Decimal::from_int(9))],
            ]
        );
    }

    /// Non-recursive CTEs: each is materialized in written order and then
    /// resolves like an ordinary table, which is the shape Go's buildWith
    /// plans. The previous behavior was an "unknown table" error.
    #[test]
    fn common_table_expressions() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE c1 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO c1 VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a FROM c1 WHERE a > 1) SELECT a FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );
        // The outer query filters, orders and aggregates the CTE like a table.
        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a, b FROM c1) SELECT SUM(b) FROM c WHERE a >= 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(50))]]
        );
        // A column list renames the CTE's columns.
        assert_eq!(
            run_select_on(
                "WITH c (x) AS (SELECT a FROM c1 WHERE a = 3) SELECT x FROM c",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        // A later CTE may read an earlier one, which is why they are
        // materialized in written order.
        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a FROM c1 WHERE a > 1), d AS (SELECT a FROM c WHERE a > 2) \
                 SELECT a FROM d",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );
        // A CTE and a real table join.
        assert_eq!(
            run_select_on(
                "WITH c AS (SELECT a FROM c1 WHERE a = 2) SELECT c1.b FROM c JOIN c1 ON c.a = c1.a",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(20)]]
        );
        // A CTE shadows a real table of the same name, as in SQL.
        assert_eq!(
            run_select_on(
                "WITH c1 AS (SELECT 9 AS a) SELECT a FROM c1",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(9)]]
        );

        // WITH RECURSIVE is rejected rather than run as if it were plain,
        // which would silently return only the seed rows.
        assert!(run_select_on(
            "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) \
             SELECT n FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        // A mismatched column list is an error, not a silent rename of some.
        assert!(run_select_on(
            "WITH c (x, y) AS (SELECT a FROM c1) SELECT x FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }

    /// Set operations, checked against results captured from a running TiDB
    /// for the same data: u1 = 1,2,2,3 and u2 = 2,3,4.
    #[test]
    fn set_operations() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE u1 (a BIGINT)", &mut catalog).unwrap();
        crate::run_create_table_on("CREATE TABLE u2 (a BIGINT)", &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO u1 VALUES (1), (2), (2), (3)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO u2 VALUES (2), (3), (4)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        let sorted = |sql: &str, catalog: &Catalog| {
            let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect();
            got.sort_unstable();
            got
        };
        let listed = |sql: &str, catalog: &Catalog| {
            run_select_on(sql, catalog, &crate::StmtContext::for_query())
                .unwrap()
                .into_iter()
                .map(|row| match row[0] {
                    Datum::Int(value) => value,
                    ref other => panic!("expected an int, got {other:?}"),
                })
                .collect::<Vec<_>>()
        };

        // Captured: UNION dedups (TiDB returned 4,1,2,3 in hash order, so the
        // comparison sorts); UNION ALL concatenates in term order.
        assert_eq!(
            sorted("SELECT a FROM u1 UNION SELECT a FROM u2", &catalog),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            listed("SELECT a FROM u1 UNION ALL SELECT a FROM u2", &catalog),
            vec![1, 2, 2, 3, 2, 3, 4],
            "captured: UNION ALL keeps duplicates and term order"
        );
        // Captured: EXCEPT -> [1], INTERSECT -> [2, 3] (hash order).
        assert_eq!(
            listed("SELECT a FROM u1 EXCEPT SELECT a FROM u2", &catalog),
            vec![1]
        );
        assert_eq!(
            sorted("SELECT a FROM u1 INTERSECT SELECT a FROM u2", &catalog),
            vec![2, 3]
        );
        // The ALL forms keep multiplicity: u1 has 2 twice, u2 once.
        assert_eq!(
            listed("SELECT a FROM u1 INTERSECT ALL SELECT a FROM u2", &catalog),
            vec![2, 3]
        );
        assert_eq!(
            listed("SELECT a FROM u1 EXCEPT ALL SELECT a FROM u2", &catalog),
            vec![1, 2],
            "one of the two 2s survives EXCEPT ALL"
        );

        // A statement-level ORDER BY and LIMIT apply to the folded result.
        // Captured: ... ORDER BY a DESC -> 4,3,2,1.
        assert_eq!(
            listed(
                "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a DESC",
                &catalog
            ),
            vec![4, 3, 2, 1]
        );
        assert_eq!(
            listed(
                "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a LIMIT 2",
                &catalog
            ),
            vec![1, 2]
        );

        // Three terms fold left to right.
        assert_eq!(
            sorted(
                "SELECT a FROM u1 UNION SELECT a FROM u2 UNION SELECT 9",
                &catalog
            ),
            vec![1, 2, 3, 4, 9]
        );
        // A CTE prefix belongs to the whole statement.
        assert_eq!(
            sorted(
                "WITH c AS (SELECT a FROM u1 WHERE a = 3) \
                 SELECT a FROM c UNION SELECT a FROM u2",
                &catalog
            ),
            vec![2, 3, 4]
        );

        // Captured: a term of a different width is 1222.
        assert!(matches!(
            run_select_on(
                "SELECT a FROM u1 UNION SELECT a, a FROM u2",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::WrongNumberOfColumnsInSelect)
        ));
    }

    #[test]
    fn select_from_table_order_limit() {
        let catalog = test_catalog();
        // ORDER BY a column that is not projected (sort runs below projection).
        assert_eq!(
            run_select_on(
                "SELECT a FROM t ORDER BY b",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(2)],
                vec![Datum::Int(1)]
            ]
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM t ORDER BY b DESC LIMIT 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
    }
}
