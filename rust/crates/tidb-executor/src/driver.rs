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
use std::sync::Arc;
use tidb_ast::{JoinNode, QueryStmt, SelectField, SelectFieldList, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver, NoResolver};

/// The name an unaliased field takes: a column reference keeps its column
/// name, anything else keeps the text it was WRITTEN with -- Go's
/// `SelectField.Text`, backed here by the parser-recorded per-field source
/// span (see `tidb_ast::SelectFieldList::text`). `count(*)` therefore names
/// the column `count(*)` even though `expr` itself restores as `COUNT(1)`
/// (the parser lowers a bare `*` argument to the AST literal `1`, matching
/// the same lowering Go's own hand-written parser performs -- see
/// `pkg/parser/expr_func_parser.go`'s `parseAggregateFuncCall`). A user who
/// writes `count(1)` literally still gets `count(1)`, since both cases read
/// the same original bytes; nothing here special-cases the star string.
///
/// Falls back to `expr.restore()` when the parser recorded no source text
/// for this field (for example a field synthesized by a rewrite pass rather
/// than parsed from source).
pub(crate) fn default_field_display_name(
    fields: &SelectFieldList,
    index: usize,
    expr: &tidb_ast::Expr,
) -> String {
    match expr {
        tidb_ast::Expr::Column(path) => path.last().cloned().unwrap_or_default(),
        other => fields
            .text(index)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map_or_else(|| other.restore(), str::to_owned),
    }
}
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
    /// Loaded statistics by physical table id, Go's `StatsHandle` cache as
    /// the planner sees it.
    ///
    /// A missing entry is Go's `statistics.PseudoTable`, and that is what
    /// makes an unanalyzed table's `EXPLAIN` print `stats:pseudo`. The map
    /// lives on the catalog rather than on a table because it is loaded from
    /// `mysql.stats_*` on its own cadence (see `tidb-exec`'s `stats_watch`),
    /// so the two are published independently.
    statistics: HashMap<i64, Arc<crate::access_cost::TableStatistics>>,
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
            statistics: HashMap::new(),
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

    /// Every `(database, table)` in the catalog, which is how referential
    /// integrity finds the tables that REFER to a given one -- Go keeps the
    /// same relation as `infoschema`'s referred-foreign-key index.
    pub(crate) fn table_paths(&self) -> Vec<(String, String)> {
        let mut paths = Vec::new();
        for database in self.databases.values() {
            for name in database.tables.keys() {
                paths.push((database.name.clone(), name.clone()));
            }
        }
        // The catalog is a hash map, so a stable order has to be imposed here
        // for a cascade to visit dependents deterministically.
        paths.sort();
        paths
    }

    /// A mutable handle for the referential-integrity paths, which reach
    /// tables the statement did not name.
    pub(crate) fn get_mut_for_foreign_key(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut TableEntry> {
        self.get_mut_in(database, name)
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

    /// Publishes one table's loaded statistics, which the access-path choice
    /// and `EXPLAIN`'s `estRows` then read instead of the pseudo constants.
    pub fn set_table_statistics(
        &mut self,
        table_id: i64,
        statistics: Arc<crate::access_cost::TableStatistics>,
    ) {
        self.statistics.insert(table_id, statistics);
    }

    /// One table's loaded statistics; `None` is Go's `PseudoTable`.
    #[must_use]
    pub fn table_statistics(
        &self,
        table_id: i64,
    ) -> Option<&Arc<crate::access_cost::TableStatistics>> {
        self.statistics.get(&table_id)
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
mod multi_dml;
mod only_full_group_by;
mod params;
mod recursive_cte;
mod subquery;
#[cfg(test)]
mod tests;

// Re-exported flat, so every caller inside and outside this module keeps
// naming these as `driver::…` exactly as before the split.
pub(crate) use access::*;
pub(crate) use agg_select::*;
pub use dml::*;
pub(crate) use from::*;
pub use params::*;
pub(crate) use subquery::*;

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
/// own name; any other expression uses the text it was WRITTEN with (Go's
/// `SelectField.Text`, see [`default_field_display_name`]), falling back to
/// its restored text when the parser recorded no source span for the field;
/// `*` expands to the table's column names.
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
/// `WITH RECURSIVE` reaches the same loop: `RECURSIVE` is a clause-level flag
/// that only PERMITS a CTE to name itself, so it is passed through to
/// [`recursive_cte::materialize_cte_body`] rather than branched on here.
fn materialize_ctes(
    with: &tidb_ast::WithClause,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Catalog, DriverError> {
    // The scratch catalog carries the real tables too, since the CTE bodies
    // and the outer query both read them.
    let mut scratch = catalog.clone();
    for cte in &with.ctes {
        // Each CTE sees the ones already materialized, which is what lets a
        // later one reference an earlier one.
        let (columns, rows) = recursive_cte::materialize_cte_body(
            &cte.name,
            &cte.columns,
            &cte.query,
            &scratch,
            current_db,
            ctx,
            with.recursive,
        )?;
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
                trace.selection(
                    written,
                    &qualify,
                    select_stats_selectivity(select, catalog, current_db, &scope),
                );
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
                trace.selection(
                    written,
                    &qualify,
                    select_stats_selectivity(select, catalog, current_db, &scope),
                );
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
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr, alias } = field else {
            projected.push((field.clone(), None));
            continue;
        };
        let name = alias
            .clone()
            .unwrap_or_else(|| default_field_display_name(&select.fields, field_index, expr));
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
                // `t.*` to one table's (Go's unfoldWildStar). A coalesced
                // join reorders the former and hides the duplicates from it;
                // `t.*` is untouched, so `u2.*` still reports `u2`'s own copy
                // of a `USING` column (captured from Go).
                let selected: Vec<&FromTable> = match qualifier.last() {
                    None => {
                        for (index, name, ft) in scope.star_columns() {
                            let mut col = Column::new((index + 1) as i64, ft);
                            col.index = index as i64;
                            exprs.push(Expression::Column(col));
                            names.push(name);
                        }
                        continue;
                    }
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
    substitute_output_aliases_where(expr, fields, top_level, &|_| false)
}

/// [`substitute_output_aliases`], with the names that already resolve where
/// the caller is standing held back.
///
/// `HAVING` needs that and `ORDER BY` does not, which IS Go's difference
/// between the two: `havingWindowAndOrderbyExprResolver` resolves a `HAVING`
/// name against the aggregation's own output FIRST and reaches the select
/// list only for a name that output lacks, while an `ORDER BY` alias shadows
/// a source column outright.
fn substitute_output_aliases_where(
    expr: &tidb_ast::Expr,
    fields: &[SelectField],
    top_level: bool,
    resolves_already: &dyn Fn(&str) -> bool,
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
        Expr::Column(path) if path.len() == 1 && !resolves_already(&path[0]) => {
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
        Expr::Paren(inner) => Expr::Paren(Box::new(substitute_output_aliases_where(
            inner,
            fields,
            false,
            resolves_already,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_output_aliases_where(
                inner,
                fields,
                false,
                resolves_already,
            )?),
        ),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(substitute_output_aliases_where(
                left,
                fields,
                false,
                resolves_already,
            )?),
            Box::new(substitute_output_aliases_where(
                right,
                fields,
                false,
                resolves_already,
            )?),
        ),
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| substitute_output_aliases_where(arg, fields, false, resolves_already))
                .collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        other => other.clone(),
    })
}

/// Go `gbyResolver`, whole: a `GROUP BY` item's positions AND its select-list
/// aliases, resolved the way that resolver does.
///
/// The two rules are one pass because they are one clause: `GROUP BY 1` and
/// `GROUP BY x` both end up naming a select field's expression, and every
/// consumer below (the aggregation's keys, `ONLY_FULL_GROUP_BY`, `GROUPING`)
/// then reads the RESOLVED item and needs no notion of either.
///
/// The alias rule is not `ORDER BY`'s. Captured from TiDB, and this is the
/// difference: in `ORDER BY` an alias SHADOWS a real column of the same name,
/// while in `GROUP BY` the REAL COLUMN WINS -- `SELECT y AS x FROM t GROUP BY
/// x` groups by `t.x`, not by `y`, and then rejects the select list under
/// `ONLY_FULL_GROUP_BY` because `y` is not determined by `t.x`. Go's
/// `gbyResolver.Leave` is where that falls out: it substitutes only when
/// `FindFieldName` found nothing.
fn resolve_group_by_item<'a>(
    expr: &'a tidb_ast::Expr,
    fields: &'a SelectFieldList,
    resolver: &ScopeResolver<'_>,
) -> Result<std::borrow::Cow<'a, tidb_ast::Expr>, DriverError> {
    if positional_field_index(expr).is_some() {
        return resolve_group_by_position(expr, fields);
    }
    Ok(std::borrow::Cow::Owned(substitute_group_by_aliases(
        expr, fields, resolver,
    )?))
}

/// One node of [`resolve_group_by_item`]'s alias substitution.
///
/// Go carries a `gbyResolver.inExpr` flag that says whether the name sits at
/// the TOP of the item or inside a larger expression. It changes nothing
/// here, and deliberately has no counterpart: both of Go's branches keep a
/// name the `FROM` scope has and substitute one it lacks, so the flag only
/// ever selects between two paths that agree. `GROUP BY x + 0` over `SELECT
/// dept AS x` therefore groups by `dept + 0`, which is what TiDB does.
fn substitute_group_by_aliases(
    expr: &tidb_ast::Expr,
    fields: &SelectFieldList,
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    let recurse = |inner: &Expr| substitute_group_by_aliases(inner, fields, resolver);
    Ok(match expr {
        Expr::Column(path) if path.len() == 1 => {
            if resolver.resolve(path).is_some() {
                // A real column of the `FROM` scope always wins.
                return Ok(expr.clone());
            }
            let alias = fields.fields().iter().find_map(|field| match field {
                SelectField::Expr {
                    expr,
                    alias: Some(alias),
                } if alias.eq_ignore_ascii_case(&path[0]) => Some(expr),
                _ => None,
            });
            let Some(target) = alias else {
                // Not a column and not an alias: the ordinary resolver
                // reports it, with its own error.
                return Ok(expr.clone());
            };
            // Grouping happens BEFORE aggregates and window functions have a
            // value, so an alias naming one is Go's ErrIllegalReference.
            let reason = if aggregates_in(target) {
                Some("reference to group function")
            } else if !crate::window::windows_in(target).is_empty() {
                Some("reference to window function")
            } else {
                None
            };
            if let Some(reason) = reason {
                return Err(DriverError::IllegalReference {
                    name: path[0].clone(),
                    reason,
                });
            }
            target.clone()
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(recurse(inner)?)),
        Expr::Unary(op, inner) => Expr::Unary(*op, Box::new(recurse(inner)?)),
        Expr::Binary(op, left, right) => {
            Expr::Binary(*op, Box::new(recurse(left)?), Box::new(recurse(right)?))
        }
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args.iter().map(recurse).collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        other => other.clone(),
    })
}

/// Whether `expr` calls an aggregate anywhere, which is what makes a `GROUP
/// BY` alias reference to it illegal.
fn aggregates_in(expr: &tidb_ast::Expr) -> bool {
    match expr {
        tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. } => true,
        tidb_ast::Expr::Paren(inner) | tidb_ast::Expr::Unary(_, inner) => aggregates_in(inner),
        tidb_ast::Expr::Binary(_, left, right) => aggregates_in(left) || aggregates_in(right),
        tidb_ast::Expr::Func { args, .. } => args.iter().any(aggregates_in),
        _ => false,
    }
}

/// Go `gbyResolver`: a bare integer at the top of a `GROUP BY` item is a
/// 1-based output position resolved against the SELECT list.
///
/// Captured from TiDB: an out-of-range position is `ErrUnknownColumn` naming
/// the `group statement`; a position landing on an aggregate or
/// window-function select field is `ErrWrongGroupField` ("Can't group on
/// '<name>'"), naming the field's alias if it has one and its written text
/// otherwise.
fn resolve_group_by_position<'a>(
    expr: &'a tidb_ast::Expr,
    fields: &'a SelectFieldList,
) -> Result<std::borrow::Cow<'a, tidb_ast::Expr>, DriverError> {
    let Some((text, index)) = positional_field_index(expr) else {
        return Ok(std::borrow::Cow::Borrowed(expr));
    };
    let index = index.map_err(|_| unknown_group_position(text))?;
    let (target, alias, field_index) = fields
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(field_index, field)| match field {
            SelectField::Expr { expr, alias } => Some((expr, alias, field_index)),
            SelectField::Wildcard(_) => None,
        })
        .nth(index)
        .ok_or_else(|| unknown_group_position(text))?;
    if matches!(
        target,
        tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. }
    ) || !crate::window::windows_in(target).is_empty()
    {
        let name = alias
            .clone()
            .unwrap_or_else(|| default_field_display_name(fields, field_index, target));
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
pub(crate) struct GroupingSpec {
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
