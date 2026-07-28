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

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, HashAggExec};
use crate::join::{JoinExec, JoinKind};
use crate::kv_table::{IndexRange, KvTable, TableHandle, TableScanExec};
use crate::limit::LimitExec;
use crate::mem_table::MemTableSourceExec;
use crate::projection::ProjectionExec;
use crate::selection::SelectionExec;
use crate::sort::{SortByItem, SortExec};
use crate::table_dual::TableDualExec;
use std::collections::HashMap;
use tidb_ast::{JoinNode, QueryStmt, SelectField, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
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

/// A catalog table's backing store.
#[derive(Clone, Debug)]
pub enum TableEntry {
    /// A plain value matrix (the original mock backing).
    Mem(MemTable),
    /// Rows stored as real TiKV-format bytes (see [`crate::kv_table`]).
    Kv(KvTable),
}

impl TableEntry {
    /// The table's columns as `(name, type)` in schema order.
    fn column_list(&self) -> Vec<(String, FieldType)> {
        match self {
            TableEntry::Mem(mem) => mem.columns.clone(),
            TableEntry::Kv(kv) => kv
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.field_type.clone()))
                .collect(),
        }
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
    fn get_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
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

/// A failure while running a SQL string through the driver.
#[derive(Debug, Clone)]
pub enum DriverError {
    /// The SQL failed to parse.
    Parse(String),
    /// The statement is not a supported `FROM`-less `SELECT`.
    Unsupported(&'static str),
    /// Rewriting an expression or executing failed.
    Exec(ExecError),
    /// The shared catalog is unusable because a statement panicked while
    /// holding it, so its schema state may be half-written.
    CatalogPoisoned,
    /// A transaction could not be committed.
    Txn(TxnErrorKind),
    /// A session-variable statement failed.
    Var(VarErrorKind),
    /// A schema statement failed.
    Schema(SchemaErrorKind),
    /// Go `ErrDupFieldName` (1060).
    DuplicateColumnName(String),
    /// Go `ErrDupKeyName` (1061).
    DuplicateKeyName(String),
    /// Go `ErrCantDropFieldOrKey` (1091), with the index-specific message.
    UnknownIndex(String),
    /// Go `ErrCantDropFieldOrKey` (1091).
    UnknownColumnInAlter(String),
    /// Go `ErrCantRemoveAllFields` (1090).
    CannotDropOnlyColumn {
        /// The column the statement named.
        column: String,
        /// The table it belongs to.
        table: String,
    },
    /// TiDB `ErrUnsupportedModifyColumn`-family (8200).
    UnsupportedDropIntegerPrimaryKey,
    /// Go `ErrFunctionsNoopImpl` (1235): a clause TiDB only implements as a
    /// no-op, refused unless `tidb_enable_noop_functions` allows it.
    FunctionsNoopImpl(&'static str),
    /// TiDB `ErrUnsupportedModifyColumn` (8200), carrying Go's reason text.
    UnsupportedModifyColumn(&'static str),
    /// Go `ErrBadField` (1054): the column is not in the table.
    UnknownColumnInTable {
        /// The column the statement named.
        column: String,
        /// The table it looked in.
        table: String,
    },
    /// Go `ErrBlobKeyWithoutLength` (1170).
    BlobKeyWithoutLength(String),
    /// Go `ErrTruncatedWrongValue` (1292).
    TruncatedIncorrectValue {
        /// The numeric domain Go names.
        kind: &'static str,
        /// The value it could not read.
        value: String,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265), value form.
    DataTruncatedValue {
        /// The column being modified.
        column: String,
        /// The value that does not fit.
        value: String,
    },
    /// Go `types.ErrInvalidDefault` (1067).
    InvalidDefault(String),
    /// Go `ErrDataTooLong` (1406).
    DataTooLong {
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `ErrWarnDataOutOfRange` (1264).
    DataOutOfRange {
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `table.ErrTruncatedWrongValueForField` (1366).
    IncorrectValue {
        /// The column type's name, as Go `types.TypeStr` prints it.
        type_name: String,
        /// The rejected value.
        value: String,
        /// The column written.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265), row form.
    DataTruncatedAtRow {
        /// The column being modified.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// TiDB 8200: the column is covered by a composite index.
    CannotDropColumnWithCompositeIndex(String),
    /// Go `ErrWrongNumberOfColumnsInSelect` (1222).
    WrongNumberOfColumnsInSelect,
    /// Go `ErrWrongAutoKey` (1075): more than one auto column.
    WrongAutoKey,
    /// Go `ErrWrongFieldSpec` (1063): AUTO_INCREMENT on a non-integer column.
    WrongColumnSpecifier(String),
    /// Go `ErrColumnCantNull` (1048).
    ColumnCannotBeNull(String),
    /// Go `ErrNoDefaultForField` (1364).
    NoDefaultForField(String),
    /// Go `ErrDupEntry` (1062).
    DuplicateEntry {
        /// The rejected key value.
        value: String,
        /// The violated key's name.
        key: String,
    },
    /// Go `ER_SUBQUERY_NO_1_ROW` (1242): a scalar subquery produced more than
    /// one row.
    SubqueryReturnsMoreThanOneRow,
}

/// Why a schema statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaErrorKind {
    /// Go `infoschema.ErrDatabaseNotExists` / `ErrBadDB` (1049).
    UnknownDatabase(String),
    /// Go `infoschema.ErrTableNotExists` (1146): a statement read a table
    /// that does not exist.
    UnknownTable(String),
    /// Go `ErrTableExists` (1050).
    TableExists(String),
    /// Go `ErrBadTable` (1051): `DROP TABLE` named a table that does not
    /// exist. MySQL uses a different code and message here than for a read.
    BadTable(String),
    /// Go `ErrDBCreateExists` (1007).
    DatabaseExists(String),
    /// Go `plannererrors.ErrNoDB` (1046).
    NoDatabaseSelected,
}

/// Why a session-variable statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarErrorKind {
    /// Go `ErrUnknownSystemVar` (1193).
    UnknownSystemVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238): the variable is read-only.
    ReadOnlyVariable(String),
    /// Go `ErrWrongTypeForVar` (1232).
    WrongTypeForVar(String),
    /// Go `ErrWrongValueForVar` (1231).
    WrongValueForVar(String, String),
}

/// Why a transaction statement failed (Go `kv.ErrWriteConflict` and friends).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnErrorKind {
    /// The catalog moved under the transaction, so committing would discard
    /// another session's writes.
    WriteConflict,
}

impl From<ExecError> for DriverError {
    fn from(err: ExecError) -> Self {
        DriverError::Exec(err)
    }
}

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

/// Runs one parsed `SELECT` against the catalog.
fn run_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
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
        folded = fold_select_subqueries(select, catalog, current_db, ctx)?;
        &folded
    } else {
        select
    };

    // Resolve FROM: none -> table-dual; otherwise the (possibly joined) tables.
    let (mut from_source, scope): (Option<Box<dyn Executor>>, FromScope) = match &select.from {
        None => (None, FromScope::default()),
        Some(join) => {
            let (exec, scope) = build_join(join, catalog, current_db, ctx)?;
            (Some(exec), scope)
        }
    };

    // Go's TryFastPlan runs before the ordinary plan: a single-table SELECT
    // whose WHERE pins the handle or a whole unique index reads that one row
    // instead of scanning. The WHERE stays in the pipeline below, so an
    // unsatisfied extra condition still filters the row out -- the point get
    // narrows the source, it does not replace the filter.
    if let Some(table) = single_kv_table(&select.from, catalog, current_db) {
        let columns = scope.column_list();
        // Go tries the batch point get before the single one.
        if let Some(handles) = try_batch_point_get(select, &table, &columns)? {
            let mut table = table.clone();
            let mut rows = Vec::with_capacity(handles.len());
            for handle in &handles {
                if let Some(row) = table
                    .get_row_by_handle(handle)
                    .map_err(|e| DriverError::Parse(format!("batch point get failed: {e:?}")))?
                {
                    rows.push(row);
                }
            }
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            from_source = Some(Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
                rows,
            )));
        } else
        // An index range scan, when no point get applies: the ranges replace
        // the full scan with the rows the index covers, and the WHERE stays
        // above to apply the conditions the ranges did not consume.
        if try_point_get(select, &table, &columns)?.is_none() {
            if let Some((index_id, ranges)) = try_index_ranges(select, &table, &columns) {
                let mut table = table.clone();
                let mut rows = Vec::new();
                for range in &ranges {
                    for handle in table
                        .scan_index_range(index_id, range)
                        .map_err(|e| DriverError::Parse(format!("index scan failed: {e:?}")))?
                    {
                        if let Some(row) = table
                            .get_row_by_handle(&handle)
                            .map_err(|e| DriverError::Parse(format!("row read failed: {e:?}")))?
                        {
                            rows.push(row);
                        }
                    }
                }
                let schema_columns: Vec<Column> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, (_, ft))| {
                        let mut col = Column::new((i + 1) as i64, ft.clone());
                        col.index = i as i64;
                        col
                    })
                    .collect();
                from_source = Some(Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
                    rows,
                )));
            }
        }
        if let Some(handle) = try_point_get(select, &table, &columns)? {
            let mut table = table;
            let rows = match handle {
                Some(handle) => table
                    .get_row_by_handle(&handle)
                    .map_err(|e| DriverError::Parse(format!("point get failed: {e:?}")))?
                    .map(|row| vec![row])
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            from_source = Some(Box::new(MemTableSourceExec::new(
                ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
                rows,
            )));
        }
    }

    // The column resolver for this query's scope.
    let resolver = ScopeResolver { scope: &scope };

    // Aggregate path: GROUP BY, or any select field that is an aggregate call.
    let is_aggregate = !select.group_by.is_empty()
        || select.fields.fields().iter().any(|f| {
            matches!(
                f,
                SelectField::Expr {
                    expr: tidb_ast::Expr::Aggregate { .. },
                    ..
                }
            )
        });
    if is_aggregate {
        return run_aggregate_select(select, from_source, &resolver, catalog, ctx);
    }

    // Rewrite each projected field into an evaluable expression; `*` expands to
    // every table column in order (Go's unfoldWildStar).
    let mut exprs: Vec<Expression> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for field in select.fields.fields() {
        match field {
            SelectField::Expr { expr, alias } => {
                let rewritten = rewrite_expr_resolved(expr, &resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                exprs.push(rewritten);
                names.push(match (alias, expr) {
                    (Some(alias), _) => alias.clone(),
                    (None, tidb_ast::Expr::Column(path)) => {
                        path.last().cloned().unwrap_or_else(|| expr.restore())
                    }
                    (None, _) => expr.restore(),
                });
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

    // Source: the table rows (matrix- or TiKV-byte-backed), or one virtual row
    // from a table-dual.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match from_source {
        Some(exec) => {
            let schema = exec.schema().clone();
            (exec, schema)
        }
        None => (
            Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            )),
            Schema::new(vec![]),
        ),
    };

    // Optional WHERE: a selection over the source rows. A correlated
    // subquery in the predicate first becomes an Apply below the selection,
    // appending the column the rewritten predicate reads (Go's plan shape).
    if let Some(predicate) = &select.where_clause {
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
            if correlated.exists.is_none() {
                value_type = subquery_result_type(&correlated.select, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            }
            applied.tables.push(FromTable {
                name: String::new(),
                columns: vec![(format!("__apply_{appended}"), value_type)],
                offset: appended,
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
                    other => ExecError::Unsupported(driver_error_text(&other)),
                })
            });
            source = Box::new(crate::apply::ApplyExec::new(
                ExecutorMeta::new(apply_schema.clone(), 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
            ));
            source_schema = apply_schema;
            predicate_scope = applied;
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
    }

    // ORDER BY: a sort below the projection, with by-items resolved against the
    // SOURCE schema (Go plans Sort against the child schema, so ordering by a
    // column that is not projected still works). Ordering by a select alias or
    // output position waits on output-schema resolution (a positional
    // ORDER BY <n> currently rewrites as a constant, which is order-neutral).
    if !select.order_by.is_empty() {
        let mut by_items = Vec::with_capacity(select.order_by.len());
        for item in &select.order_by {
            let expr = rewrite_expr_resolved(&item.expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
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
    }

    // Projection of the rewritten fields.
    let mut root: Box<dyn Executor> = Box::new(ProjectionExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        exprs,
        source,
        ctx.clone(),
    ));

    // SELECT DISTINCT: Go `buildDistinct` builds an aggregation grouping by
    // every projected column, with a FIRST_ROW aggregate per column, which is
    // exactly a deduplication. It sits above the projection and below LIMIT.
    if select.distinct {
        root = Box::new(distinct_over(root, &out_schema, ctx));
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

/// Parses and runs a plain `INSERT INTO t [(cols)] VALUES (...), ...` against
/// `catalog`, returning the number of inserted rows.
///
/// The write half of the in-memory gateway (the storage-backed `InsertExec`
/// with autoid/defaults/constraints lands with real tables). Unsupported here
/// (rejected, documented): `REPLACE`, `IGNORE`, `ON DUPLICATE KEY UPDATE`,
/// `SET` syntax, `INSERT ... SELECT`, partitions, and `RETURNING`. Columns not
/// listed in an explicit column list are filled with NULL (column defaults
/// wait on ColumnInfo default-value wiring).
pub fn run_insert_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_insert_on`] resolving unqualified names in `current_db`.
pub fn run_insert_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_reporting(sql, catalog, current_db, ctx).map(|outcome| outcome.0)
}

/// [`run_insert_in`], also reporting the first auto-increment id the statement
/// allocated, which is what MySQL answers with as `LAST_INSERT_ID`.
///
/// `None` when the statement allocated nothing: an explicit auto value or a
/// table with no auto column leaves the session's value untouched, which is
/// the behavior captured from TiDB.
pub fn run_insert_reporting(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(u64, Option<i64>), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let insert = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Insert(insert) => insert,
            _ => return Err(DriverError::Unsupported("only INSERT is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only INSERT is supported here")),
    };

    if insert.replace
        || insert.ignore
        || !insert.on_duplicate.is_empty()
        || insert.set_syntax
        || insert.source.is_some()
        || !insert.partitions.is_empty()
        || !insert.returning.fields().is_empty()
    {
        return Err(DriverError::Unsupported(
            "only plain INSERT INTO t [(cols)] VALUES is supported",
        ));
    }

    let (database, table_name) = split_table_path(&insert.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    let table = catalog
        .get_mut_in(&database, &table_name)
        .ok_or(DriverError::Unsupported("table not found in catalog"))?;
    let column_list = table.column_list();

    // Map an explicit column list to table offsets; without one, values map to
    // every column in order.
    let target_offsets: Vec<usize> = if insert.columns_specified {
        insert
            .columns
            .iter()
            .map(|name| {
                column_list
                    .iter()
                    .position(|(n, _)| n.eq_ignore_ascii_case(name))
                    .ok_or(DriverError::Unsupported("unknown column in column list"))
            })
            .collect::<Result<_, _>>()?
    } else {
        (0..column_list.len()).collect()
    };

    // Evaluate each VALUES row (constant expressions over the dual row).
    let eval_chunk = {
        let mut c = tidb_chunk::chunk::Chunk::new_empty(&[]);
        c.set_num_virtual_rows(1);
        c
    };
    // The per-column metadata the default and NOT NULL rules read.
    let column_meta: Vec<(Option<Datum>, bool, String)> = match table {
        TableEntry::Kv(kv) => kv
            .columns
            .iter()
            .map(|c| {
                (
                    c.default_value.clone(),
                    c.field_type.flags() & 1 != 0,
                    c.name.clone(),
                )
            })
            .collect(),
        // A matrix-backed table carries no column metadata, so every column
        // is nullable with no default -- the original mock behavior.
        TableEntry::Mem(mem) => mem
            .columns
            .iter()
            .map(|(name, _)| (None, false, name.clone()))
            .collect(),
    };

    let auto_increment_offset = match table {
        TableEntry::Kv(kv) => kv.auto_increment_offset(),
        TableEntry::Mem(_) => None,
    };
    let mut auto_rows: Vec<usize> = Vec::new();
    let mut first_allocated: Option<i64> = None;

    let mut inserted = 0u64;
    let mut new_rows: Vec<Vec<Datum>> = Vec::with_capacity(insert.rows.len());
    for value_row in &insert.rows {
        if value_row.len() != target_offsets.len() {
            return Err(DriverError::Unsupported(
                "VALUES arity does not match the column list",
            ));
        }
        let mut row = vec![Datum::Null; column_list.len()];
        let mut assigned = vec![false; column_list.len()];
        for (expr, &offset) in value_row.iter().zip(&target_offsets) {
            let rewritten = rewrite_expr_resolved(expr, &NoResolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            let value = rewritten
                .eval(ctx, eval_chunk.get_row(0))
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            row[offset] = value;
            assigned[offset] = true;
        }
        // Go fills the auto-increment column before the default and NOT NULL
        // rules run, so an omitted auto column never looks like a missing
        // value (`adjustAutoIncrementDatum` runs inside the row build).
        if let Some(offset) = auto_increment_offset {
            // An omitted or explicitly NULL auto column becomes the zero
            // marker, which allocation replaces; Go does this before the
            // NOT NULL check, so a NULL here is never a bad-null error.
            if !assigned[offset] || row[offset] == Datum::Null {
                row[offset] = Datum::Int(0);
            }
            assigned[offset] = true;
            auto_rows.push(new_rows.len());
        }
        // Only a column the statement omits takes its default, and only such
        // a column can raise ErrNoDefaultForField (Go `fillColValue`).
        for offset in 0..column_list.len() {
            if !assigned[offset] {
                row[offset] = column_default(&column_meta, offset)?;
            }
        }
        // Go `Column.CheckNotNull`: an explicit NULL in a NOT NULL column is
        // ErrColumnCantNull, which is a different error from omitting a
        // column that has no default.
        for (offset, value) in row.iter().enumerate() {
            if *value == Datum::Null && column_is_not_null(&column_meta, offset) && assigned[offset]
            {
                return Err(DriverError::ColumnCannotBeNull(
                    column_list[offset].0.clone(),
                ));
            }
        }
        // Go casts each value to its column's type before the row is
        // written, which is what rounds a decimal to the column's scale and
        // parses a numeric string.
        if let TableEntry::Kv(kv) = &*table {
            for (offset, value) in row.iter_mut().enumerate() {
                let column = &kv.columns[offset];
                *value = cast_value_for_column(
                    std::mem::replace(value, Datum::Null),
                    &column.field_type,
                    &column.name,
                    new_rows.len(),
                    ctx,
                )?;
            }
        }
        new_rows.push(row);
        inserted += 1;
    }
    match table {
        TableEntry::Mem(mem) => mem.rows.extend(new_rows),
        TableEntry::Kv(kv) => {
            // The allocator lives on the table, so the ids are handed out here
            // rather than while the rows were being built.
            for index in &auto_rows {
                if let Some(allocated) = kv.apply_auto_increment(&mut new_rows[*index]) {
                    // Go keeps the FIRST allocated id of the statement.
                    if first_allocated.is_none() {
                        first_allocated = Some(allocated);
                    }
                }
            }
            for row in &new_rows {
                kv.insert_row(row).map_err(|e| match e {
                    crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                        DriverError::DuplicateEntry { value, key }
                    }
                    other => DriverError::Parse(format!("row encode failed: {other:?}")),
                })?;
            }
        }
    }
    Ok((inserted, first_allocated))
}

/// Whether a conversion event is one TiDB reports nothing for.
///
/// Rounding a NUMBER into a narrower decimal is the case: captured, both
/// `INSERT INTO t(d DECIMAL(10,3)) VALUES (1.23456)` and
/// `ALTER TABLE t ADD COLUMN e DECIMAL(6,2) DEFAULT 3.14159` are accepted in
/// silence, storing 1.235 and 3.14. Go reaches that through
/// `ProduceDecWithSpecifiedTp`, whose rounding notice never becomes a
/// statement error. A STRING source is a different case -- it may not be a
/// number at all -- so it is never silent.
pub(crate) fn conversion_event_is_silent(
    value: &Datum,
    field_type: &FieldType,
    event: &tidb_datatype::ScalarConversionEvent,
) -> bool {
    let numeric_source = matches!(
        value,
        Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) | Datum::Float32(_) | Datum::Decimal(_)
    );
    numeric_source
        && matches!(field_type.eval_type(), tidb_datatype::EvalType::Decimal)
        && matches!(event, tidb_datatype::ScalarConversionEvent::Truncated)
}

/// Go `table.CastValue` + `completeInsertErr`: converts one written value into
/// the column's own type, and names the failure the way the insert path does.
///
/// The strict SQL mode makes a bad value fail the statement; without it the
/// converted (clamped or truncated) value is stored and the same message is a
/// warning, which is what `sql_mode = ''` produces in TiDB.
fn cast_value_for_column(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    if value.is_null() {
        return Ok(value);
    }
    let converted = value
        .convert_to(field_type, ctx.conversion_flags())
        .map_err(|_| DriverError::IncorrectValue {
            type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
            value: datum_error_text(&value),
            column: column.to_owned(),
            row: row_index + 1,
        })?;
    let Some(event) = converted.event else {
        return Ok(converted.value);
    };
    if conversion_event_is_silent(&value, field_type, &event) {
        return Ok(converted.value);
    }
    // Go picks the message from the conversion's own error kind: a string
    // that does not fit is ErrDataTooLong, a number outside the column's
    // range is ErrWarnDataOutOfRange, and anything else is the
    // "Incorrect <type> value" form.
    let error = match event {
        tidb_datatype::ScalarConversionEvent::Overflow(_) => DriverError::DataOutOfRange {
            column: column.to_owned(),
            row: row_index + 1,
        },
        tidb_datatype::ScalarConversionEvent::Truncated
            if matches!(field_type.eval_type(), tidb_datatype::EvalType::String) =>
        {
            DriverError::DataTooLong {
                column: column.to_owned(),
                row: row_index + 1,
            }
        }
        tidb_datatype::ScalarConversionEvent::Truncated => DriverError::IncorrectValue {
            type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
            value: datum_error_text(&value),
            column: column.to_owned(),
            row: row_index + 1,
        },
    };
    if ctx.strict() {
        return Err(error);
    }
    let reported = error.to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    Ok(converted.value)
}

/// A value as MySQL prints it inside a conversion error message.
fn datum_error_text(value: &Datum) -> String {
    match value {
        Datum::Int(v) => v.to_string(),
        Datum::UInt(v) => v.to_string(),
        Datum::Real(v) => v.to_string(),
        Datum::Decimal(v) => v.to_string(),
        Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
        other => format!("{other:?}"),
    }
}

/// Go `aggregation.NewAggFuncDesc` + `baseFuncDesc.TypeInfer`: the aggregate
/// kind and the result type inferred for its argument.
fn agg_kind_and_type(name: &str, arg: &Expression) -> Result<(AggKind, FieldType), DriverError> {
    Ok(match name {
        "COUNT" => (AggKind::Count, FieldType::new(FieldTypeCode::LongLong)),
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
        _ => {
            return Err(DriverError::Unsupported(
                "this aggregate function is deferred",
            ))
        }
    })
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
    group_by_names: &[String],
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match expr {
        // A column that HAVING/ORDER BY references but the select list does
        // not project: Go carries it out of the aggregation as a hidden
        // FIRST_ROW column, exactly as it does for a selected group column.
        // A column that is not grouped is rejected, which is what
        // ONLY_FULL_GROUP_BY reports in Go.
        Expr::Column(path) => {
            let name = path.last().cloned().unwrap_or_default();
            if names
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&name))
            {
                return Ok(expr.clone());
            }
            if !group_by_names
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&name))
            {
                return Err(DriverError::Unsupported(
                    "this clause references a column that is neither grouped nor aggregated",
                ));
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
                distinct: false,
            });
            names.push(name.clone());
            types.push(ftype);
            Expr::Column(vec![name])
        }
        Expr::Aggregate { .. } => {
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
                group_by_names,
                resolver,
            )?),
            Box::new(substitute_aggregates(
                rhs,
                agg_funcs,
                names,
                types,
                group_by_names,
                resolver,
            )?),
        ),
        other => other.clone(),
    })
}

/// Builds one aggregate function (and its Go-inferred result type) from an
/// `Expr::Aggregate` node.
fn build_agg_func(
    expr: &tidb_ast::Expr,
    resolver: &ScopeResolver<'_>,
) -> Result<(AggFunc, FieldType), DriverError> {
    let tidb_ast::Expr::Aggregate {
        name,
        distinct,
        args,
    } = expr
    else {
        return Err(DriverError::Unsupported("not an aggregate function"));
    };
    let [arg] = args.as_slice() else {
        return Err(DriverError::Unsupported(
            "multi-argument aggregates are deferred",
        ));
    };
    let arg =
        rewrite_expr_resolved(arg, resolver).map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    let (kind, ftype) = agg_kind_and_type(name, &arg)?;
    Ok((
        AggFunc {
            kind,
            arg: Some(arg),
            distinct: *distinct,
        },
        ftype,
    ))
}

/// A static-ish result type for a correlated scalar subquery's column: the
/// type its select field reports when the query is planned with no bindings.
/// Falling back to `LongLong` matches what the rest of the seed does for an
/// expression whose type is not inferred.
fn subquery_result_type(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<FieldType> {
    run_select_stmt(select, catalog, current_db, ctx)
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

/// A correlated subquery found in an outer expression: the subquery itself and
/// whether it is an `EXISTS` test rather than a scalar read.
struct CorrelatedSubquery {
    select: tidb_ast::SelectStmt,
    exists: Option<bool>,
    columns: Vec<Vec<String>>,
}

/// Whether `expr` references a column of the OUTER scope, which is what makes
/// a subquery correlated (Go's `ExtractCorrelatedCols4LogicalPlan`).
///
/// A reference is correlated when the inner query's own `FROM` cannot resolve
/// it but the outer scope can -- the same two-scope test Go's name resolver
/// applies when it binds a column to an outer plan's schema.
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
        Some(join) => match build_join(join, catalog, current_db, ctx) {
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
        let (index, _, _) = resolver
            .resolve(path)
            .ok_or(DriverError::Unsupported("unresolved correlated column"))?;
        let value = outer_values
            .get(index)
            .cloned()
            .ok_or(DriverError::Unsupported("correlated column out of range"))?;
        bindings.push((path.clone(), value));
    }

    let mut bound = correlated.select.clone();
    for field in bound.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = bind_correlated_columns(expr, &bindings)?;
        }
    }
    if let Some(where_clause) = &bound.where_clause {
        bound.where_clause = Some(bind_correlated_columns(where_clause, &bindings)?);
    }
    if let Some(having) = &bound.having {
        bound.having = Some(bind_correlated_columns(having, &bindings)?);
    }
    for item in &mut bound.group_by {
        item.expr = bind_correlated_columns(&item.expr, &bindings)?;
    }
    for item in &mut bound.order_by {
        item.expr = bind_correlated_columns(&item.expr, &bindings)?;
    }

    let (_, rows) = run_select_stmt(&bound, catalog, current_db, ctx)?;
    match correlated.exists {
        // EXISTS folds to 1/0 per outer row.
        Some(not) => Ok(Datum::Int(i64::from(!rows.is_empty() != not))),
        None => match rows.len() {
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
    }
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
            let exists = match expr {
                Expr::Exists { not, .. } => Some(*not),
                _ => None,
            };
            *found = Some(CorrelatedSubquery {
                select: (**select).clone(),
                exists,
                columns,
            });
            placeholder(index)
        }
        Expr::InSubquery { .. } | Expr::CompareSubquery { .. } => {
            // Correlated IN / ANY / ALL become semi-joins in Go, not the
            // one-appended-column Apply shape this builds.
            expr.clone()
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
            let outer = match &select.from {
                None => FromScope::default(),
                Some(join) => match build_join(join, catalog, current_db, ctx) {
                    Ok((_, scope)) => scope,
                    Err(_) => FromScope::default(),
                },
            };
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
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut folded = select.clone();
    for field in folded.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = fold_subqueries(expr, catalog, current_db, ctx)?;
        }
    }
    if let Some(where_clause) = &folded.where_clause {
        folded.where_clause = Some(fold_subqueries(where_clause, catalog, current_db, ctx)?);
    }
    if let Some(having) = &folded.having {
        folded.having = Some(fold_subqueries(having, catalog, current_db, ctx)?);
    }
    for item in &mut folded.order_by {
        item.expr = fold_subqueries(&item.expr, catalog, current_db, ctx)?;
    }
    for item in &mut folded.group_by {
        item.expr = fold_subqueries(&item.expr, catalog, current_db, ctx)?;
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
/// `EXISTS` folds to 1 or 0, and `x IN (subquery)` folds to `x IN (values)`,
/// which evaluates identically for an uncorrelated subquery -- including the
/// NULL rules, since the folded list is compared by the same `IN` code.
///
/// DEFERRED (documented): CORRELATED subqueries, which Go turns into an Apply
/// operator rather than folding, and which this rejects rather than silently
/// evaluating the inner query against the wrong row; `ANY`/`ALL` comparison
/// subqueries; and row constructors (a subquery selecting several columns).
fn fold_subqueries(
    expr: &tidb_ast::Expr,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
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
            let mut list = Vec::with_capacity(rows.len());
            for row in &rows {
                let [value] = row.as_slice() else {
                    return Err(DriverError::Unsupported(
                        "an IN subquery selecting several columns is not supported yet",
                    ));
                };
                list.push(datum_to_literal(value)?);
            }
            if list.is_empty() {
                // `x IN ()` is not sayable in SQL: an empty subquery result is
                // false, and `NOT IN` over it is true, for every x including
                // NULL (MySQL evaluates the semi join, which finds nothing).
                return Ok(Expr::Int(i64::from(*not).to_string()));
            }
            Expr::In {
                expr: Box::new(fold_subqueries(expr, catalog, current_db, ctx)?),
                list,
                not: *not,
            }
        }
        Expr::CompareSubquery { .. } => {
            return Err(DriverError::Unsupported(
                "ANY/ALL comparison subqueries are not supported yet",
            ))
        }
        // Walk the forms the expression rewriter itself supports; anything
        // else is returned unchanged and fails to rewrite as it already does.
        Expr::Paren(inner) => {
            Expr::Paren(Box::new(fold_subqueries(inner, catalog, current_db, ctx)?))
        }
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(fold_subqueries(inner, catalog, current_db, ctx)?),
        ),
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(fold_subqueries(lhs, catalog, current_db, ctx)?),
            Box::new(fold_subqueries(rhs, catalog, current_db, ctx)?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(fold_subqueries(expr, catalog, current_db, ctx)?),
            target: *target,
            not: *not,
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(fold_subqueries(expr, catalog, current_db, ctx)?),
            list: list
                .iter()
                .map(|item| fold_subqueries(item, catalog, current_db, ctx))
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
fn datum_to_literal(value: &Datum) -> Result<tidb_ast::Expr, DriverError> {
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
        Datum::String(s) => Expr::String(String::from_utf8_lossy(s.bytes()).into_owned()),
        Datum::Bytes(b) => Expr::String(String::from_utf8_lossy(b).into_owned()),
        _ => {
            return Err(DriverError::Unsupported(
                "this subquery result kind is not supported yet",
            ))
        }
    })
}

/// Go `ranger`'s point pair for one comparison, before points become ranges.
///
/// Go builds a sorted point list and folds it into ranges; for the
/// single-column access this covers, each comparison yields exactly one range,
/// which is that fold already applied.
fn range_for_comparison(op: tidb_ast::BinaryOp, value: Datum) -> Option<IndexRange> {
    use tidb_ast::BinaryOp;
    // Go's builder starts every ordinary comparison at MinNotNull rather than
    // at NULL, which is what makes a NULL value satisfy no comparison.
    Some(match op {
        BinaryOp::Eq => IndexRange {
            low: vec![value.clone()],
            high: vec![value],
            low_exclusive: false,
            high_exclusive: false,
        },
        BinaryOp::Lt => IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![value],
            low_exclusive: false,
            high_exclusive: true,
        },
        BinaryOp::Le => IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![value],
            low_exclusive: false,
            high_exclusive: false,
        },
        BinaryOp::Gt => IndexRange {
            low: vec![value],
            high: vec![Datum::MaxValue],
            low_exclusive: true,
            high_exclusive: false,
        },
        BinaryOp::Ge => IndexRange {
            low: vec![value],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        },
        _ => return None,
    })
}

/// The index access path for a `WHERE`, when one applies.
///
/// Go's `DetachCondAndBuildRangeForIndex` splits a predicate into access
/// conditions, which become index ranges, and filter conditions, which stay
/// above the read. This builds ranges for the conditions on one index's
/// leading column and leaves the whole `WHERE` in the pipeline, so the filter
/// half is applied by the selection rather than dropped.
///
/// DEFERRED (documented): multi-column ranges (Go extends the prefix while
/// equalities pin leading columns), `IN` lists and `BETWEEN` as ranges, `OR`
/// unions across ranges, and cost-based choice among several usable indexes --
/// this takes the first index whose leading column the `WHERE` constrains.
fn try_index_ranges(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Option<(i64, Vec<IndexRange>)> {
    let where_clause = select.where_clause.as_ref()?;
    let mut conditions = Vec::new();
    collect_conjuncts(where_clause, &mut conditions);

    for index in table.indexes() {
        // Only the leading column can be constrained without the multi-column
        // range builder.
        let leading = &columns[*index.column_offsets.first()?].0;
        let mut ranges: Vec<IndexRange> = Vec::new();
        for condition in &conditions {
            let tidb_ast::Expr::Binary(op, lhs, rhs) = condition else {
                continue;
            };
            // Go accepts the constant on either side, flipping the operator
            // when the column is on the right.
            let (op, value) = match (&**lhs, &**rhs) {
                (tidb_ast::Expr::Column(path), other)
                    if path
                        .last()
                        .is_some_and(|name| name.eq_ignore_ascii_case(leading)) =>
                {
                    (*op, other)
                }
                (other, tidb_ast::Expr::Column(path))
                    if path
                        .last()
                        .is_some_and(|name| name.eq_ignore_ascii_case(leading)) =>
                {
                    (flip_comparison(*op)?, other)
                }
                _ => continue,
            };
            let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(value, &NoResolver)
            else {
                continue;
            };
            let Ok(value) = constant.eval() else {
                continue;
            };
            // A NULL constant makes every comparison unknown, so no row
            // qualifies; Go represents that as an empty range set.
            if value == Datum::Null {
                return Some((index.id, Vec::new()));
            }
            if let Some(range) = range_for_comparison(op, value) {
                ranges.push(range);
            }
        }
        if ranges.is_empty() {
            continue;
        }
        // Several conditions on the same column intersect.
        let combined = ranges
            .into_iter()
            .reduce(intersect_ranges)
            .expect("at least one range");
        return Some((index.id, vec![combined]));
    }
    None
}

/// Flattens an `AND` chain into its conjuncts.
fn collect_conjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_conjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) => {
            collect_conjuncts(lhs, out);
            collect_conjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The operator with its operands swapped, so `5 < a` reads as `a > 5`.
fn flip_comparison(op: tidb_ast::BinaryOp) -> Option<tidb_ast::BinaryOp> {
    use tidb_ast::BinaryOp;
    Some(match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Le => BinaryOp::Ge,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Ge => BinaryOp::Le,
        _ => return None,
    })
}

/// The intersection of two ranges over the same column, which is what several
/// conditions on that column mean together.
fn intersect_ranges(left: IndexRange, right: IndexRange) -> IndexRange {
    let (low, low_exclusive) = match compare_bounds(&left.low, &right.low) {
        std::cmp::Ordering::Greater => (left.low, left.low_exclusive),
        std::cmp::Ordering::Less => (right.low, right.low_exclusive),
        // Equal bounds: the exclusive one wins, being the tighter.
        std::cmp::Ordering::Equal => (left.low, left.low_exclusive || right.low_exclusive),
    };
    let (high, high_exclusive) = match compare_bounds(&left.high, &right.high) {
        std::cmp::Ordering::Less => (left.high, left.high_exclusive),
        std::cmp::Ordering::Greater => (right.high, right.high_exclusive),
        std::cmp::Ordering::Equal => (left.high, left.high_exclusive || right.high_exclusive),
    };
    IndexRange {
        low,
        high,
        low_exclusive,
        high_exclusive,
    }
}

/// Orders two single-datum bounds, with `MinNotNull` below and `MaxValue`
/// above every ordinary value.
fn compare_bounds(left: &[Datum], right: &[Datum]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (Some(left), Some(right)) = (left.first(), right.first()) else {
        return Ordering::Equal;
    };
    let rank = |value: &Datum| match value {
        Datum::MinNotNull => 0,
        Datum::MaxValue => 2,
        _ => 1,
    };
    match rank(left).cmp(&rank(right)) {
        Ordering::Equal => {}
        other => return other,
    }
    if rank(left) != 1 {
        return Ordering::Equal;
    }
    tidb_expr::compare_datums(left, right).unwrap_or(Ordering::Equal)
}

/// The single TiKV-backed table a `FROM` names, when it names exactly one.
/// A point get applies only to that shape (Go `getSingleTableNameAndAlias`).
fn single_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let join = from.as_ref()?;
    if join.right.is_some() {
        return None;
    }
    let JoinNode::Table(table_ref) = &join.left else {
        return None;
    };
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        TableEntry::Mem(_) => None,
    }
}

/// Go `tryWhereIn2BatchPointGet`: a single-table `SELECT` whose whole `WHERE`
/// is `column IN (constants)` over the handle or a single-column unique index
/// reads those rows directly instead of scanning.
///
/// Go rejects the fast plan when `ORDER BY`, `GROUP BY`, `LIMIT`, `HAVING`,
/// `DISTINCT` or a window spec is present, when the `IN` is negated, and when
/// its list is empty. The handle path applies when the table's primary key IS
/// the handle and the column names it; otherwise a unique index whose only
/// column it is.
///
/// DEFERRED (documented): Go's row form, `(a, b) IN ((1, 2), (3, 4))`, which
/// needs multi-column key lookup.
fn try_batch_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Result<Option<Vec<TableHandle>>, DriverError> {
    if select.having.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.limit.is_some()
        || select.distinct
    {
        return Ok(None);
    }
    let Some(where_clause) = &select.where_clause else {
        return Ok(None);
    };
    // The WHERE must be exactly the IN, as Go requires a PatternInExpr.
    let tidb_ast::Expr::In { expr, list, not } = where_clause else {
        return Ok(None);
    };
    if *not || list.is_empty() {
        return Ok(None);
    }
    let tidb_ast::Expr::Column(path) = &**expr else {
        return Ok(None);
    };
    let Some(name) = path.last() else {
        return Ok(None);
    };

    // Every list element must be a constant, or this is not a point plan.
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(item, &NoResolver) else {
            return Ok(None);
        };
        let Ok(value) = constant.eval() else {
            return Ok(None);
        };
        values.push(value);
    }

    // The handle path.
    if let Some(offset) = table.pk_handle_offset() {
        if columns[offset].0.eq_ignore_ascii_case(name) {
            let mut handles = Vec::with_capacity(values.len());
            for value in &values {
                match value {
                    Datum::Int(v) => handles.push(TableHandle::Int(*v)),
                    Datum::UInt(v) => handles.push(TableHandle::Int(*v as i64)),
                    // A non-integer constant names no integer handle, so it
                    // simply matches nothing.
                    _ => {}
                }
            }
            return Ok(Some(handles));
        }
    }

    // The unique-index path.
    let mut table = table.clone();
    for index in table.indexes().to_vec() {
        if !index.unique || index.column_offsets.len() != 1 {
            continue;
        }
        if !columns[index.column_offsets[0]]
            .0
            .eq_ignore_ascii_case(name)
        {
            continue;
        }
        let mut handles = Vec::new();
        for value in &values {
            if let Some(handle) = table
                .lookup_unique(index.id, std::slice::from_ref(value))
                .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
            {
                handles.push(handle);
            }
        }
        return Ok(Some(handles));
    }
    Ok(None)
}

/// One `column = constant` equality from a `WHERE`, Go's `nameValuePair`.
struct NameValuePair {
    column: String,
    value: Datum,
}

/// Go `getNameValuePairs`: flattens a `WHERE` that is a conjunction of
/// `column = constant` equalities into pairs, returning `None` for any other
/// shape.
///
/// Go accepts the constant on either side of the `=`, and recurses only
/// through `AND`; anything else (an `OR`, a comparison, a function call)
/// makes the statement ineligible for a point get, which is what returning
/// `None` means here.
fn name_value_pairs(expr: &tidb_ast::Expr, pairs: &mut Vec<NameValuePair>) -> bool {
    use tidb_ast::{BinaryOp, Expr};
    match expr {
        Expr::Paren(inner) => name_value_pairs(inner, pairs),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            name_value_pairs(lhs, pairs) && name_value_pairs(rhs, pairs)
        }
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
            let (column, value) = match (&**lhs, &**rhs) {
                (Expr::Column(path), other) => (path, other),
                (other, Expr::Column(path)) => (path, other),
                _ => return false,
            };
            let Some(name) = column.last() else {
                return false;
            };
            // Only a literal qualifies; anything needing evaluation against a
            // row is not a point-get key.
            let Ok(value) = rewrite_expr_resolved(value, &NoResolver) else {
                return false;
            };
            let Expression::Constant(constant) = value else {
                return false;
            };
            let Ok(value) = constant.eval() else {
                return false;
            };
            pairs.push(NameValuePair {
                column: name.clone(),
                value,
            });
            true
        }
        _ => false,
    }
}

/// The row a point get reads, when the statement qualifies for one.
///
/// Go `TryFastPlan`/`tryPointGetPlan`: a single-table `SELECT` with no
/// `HAVING` and no `ORDER BY`, whose `WHERE` is a conjunction of equalities
/// that pins either the handle or every column of a unique index, reads one
/// row directly instead of scanning. `LIMIT` is allowed only when it cannot
/// remove the row (`count > 0` and `offset == 0`), matching Go's check.
///
/// Returns `Ok(None)` when the statement does not qualify, so the caller
/// falls back to the ordinary scan.
fn try_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Result<Option<Option<TableHandle>>, DriverError> {
    if select.having.is_some() || !select.order_by.is_empty() || !select.group_by.is_empty() {
        return Ok(None);
    }
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        if count == 0 || offset > 0 {
            return Ok(None);
        }
    }
    let Some(where_clause) = &select.where_clause else {
        return Ok(None);
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs) || pairs.is_empty() {
        return Ok(None);
    }

    // The handle path: the primary key pinned by exactly one equality, which
    // is Go's `len(pairs) == 1` condition on the handle pair.
    if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if pairs.len() == 1 && pairs[0].column.eq_ignore_ascii_case(handle_column) {
            return Ok(Some(match &pairs[0].value {
                Datum::Int(value) => Some(TableHandle::Int(*value)),
                Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                // A non-integer constant cannot name an integer handle, so no
                // row matches rather than the plan being wrong.
                _ => None,
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    let mut table = table.clone();
    for index in table.indexes().to_vec() {
        if !index.unique {
            continue;
        }
        let mut values = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            let name = &columns[*offset].0;
            let Some(pair) = pairs
                .iter()
                .find(|pair| pair.column.eq_ignore_ascii_case(name))
            else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() != index.column_offsets.len() {
            continue;
        }
        let handle = table
            .lookup_unique(index.id, &values)
            .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?;
        return Ok(Some(handle));
    }
    Ok(None)
}

/// One table in a query's `FROM`: the name a qualifier must match (its alias
/// when it has one, as in Go's `TableSource`), its columns, and the offset of
/// its first column in the joined row.
#[derive(Clone, Debug)]
struct FromTable {
    name: String,
    columns: Vec<(String, FieldType)>,
    offset: usize,
}

/// The joined `FROM` scope: every table's columns concatenated left to right,
/// which is the row layout [`JoinExec`] produces.
#[derive(Clone, Debug, Default)]
struct FromScope {
    tables: Vec<FromTable>,
}

impl FromScope {
    /// Every column of the scope in row order.
    fn column_list(&self) -> Vec<(String, FieldType)> {
        self.tables
            .iter()
            .flat_map(|t| t.columns.iter().cloned())
            .collect()
    }

    fn width(&self) -> usize {
        self.tables.iter().map(|t| t.columns.len()).sum()
    }
}

/// Resolves a column reference against the joined `FROM` scope.
///
/// A qualified `t.a` binds to table `t`'s column; an unqualified `a` binds to
/// the one table that has such a column, and is rejected as ambiguous when
/// several do -- MySQL's `ERROR 1052 (23000): Column 'a' in field list is
/// ambiguous`, which Go raises from `expression.buildColumn`.
struct ScopeResolver<'a> {
    scope: &'a FromScope,
}

impl ColumnResolver for ScopeResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] => (Some(table), name),
            // db.t.a qualification waits on a multi-schema catalog.
            _ => return None,
        };
        let mut found: Option<(usize, FieldType)> = None;
        for table in &self.scope.tables {
            if let Some(q) = qualifier {
                if !q.eq_ignore_ascii_case(&table.name) {
                    continue;
                }
            }
            for (i, (candidate, ft)) in table.columns.iter().enumerate() {
                if candidate.eq_ignore_ascii_case(name) {
                    if found.is_some() {
                        // Ambiguous across tables: MySQL errors rather than
                        // picking one.
                        return None;
                    }
                    found = Some((table.offset + i, ft.clone()));
                }
            }
        }
        let (index, ft) = found?;
        Some((index, ft, (index + 1) as i64))
    }
}

/// Builds the `FROM` scope and the executor that produces its rows.
///
/// Go's `buildJoin` builds a left-deep tree of `LogicalJoin`s over the
/// `FROM` list; this walks the same tree, so `a JOIN b JOIN c` nests as
/// `(a JOIN b) JOIN c` and the row layout is `a`'s columns, then `b`'s, then
/// `c`'s.
///
/// DEFERRED (documented): derived tables, `USING`, `NATURAL`, and
/// `STRAIGHT_JOIN`'s ordering guarantee.
fn build_from(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    match node {
        JoinNode::Table(table_ref) => {
            // A `db.t` reference resolves in that schema; a bare `t` resolves
            // in the session's current one (Go's name resolution).
            let (database, name) = split_table_path(&table_ref.name, current_db)?;
            let entry = catalog
                .get_in(database, name)
                .ok_or(DriverError::Unsupported("table not found in catalog"))?;
            let columns = entry.column_list();
            // A table alias replaces the name for qualification, as in Go.
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let schema = Schema::new(schema_columns);
            let exec: Box<dyn Executor> = match entry {
                TableEntry::Mem(mem) => Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    mem.rows.clone(),
                )),
                TableEntry::Kv(kv) => Box::new(TableScanExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    kv.clone(),
                )),
            };
            let scope = FromScope {
                tables: vec![FromTable {
                    name: visible,
                    columns,
                    offset: 0,
                }],
            };
            Ok((exec, scope))
        }
        JoinNode::Join(join) => build_join(join, catalog, current_db, ctx),
        JoinNode::Derived { .. } => Err(DriverError::Unsupported(
            "derived tables are not supported yet",
        )),
    }
}

/// Builds one join node (or passes through the single-table wrapper).
fn build_join(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let (left_exec, left_scope) = build_from(&join.left, catalog, current_db, ctx)?;
    let Some(right_node) = &join.right else {
        // The single-table wrapper the parser always produces.
        return Ok((left_exec, left_scope));
    };
    if join.natural || !join.using.is_empty() {
        return Err(DriverError::Unsupported(
            "NATURAL and USING joins are not supported yet",
        ));
    }
    let (right_exec, right_scope) = build_from(right_node, catalog, current_db, ctx)?;

    // The joined scope: the right tables' columns follow the left's.
    let left_width = left_scope.width();
    let mut scope = left_scope;
    for table in right_scope.tables {
        scope.tables.push(FromTable {
            name: table.name,
            columns: table.columns,
            offset: table.offset + left_width,
        });
    }

    let column_list = scope.column_list();
    let schema_columns: Vec<Column> = column_list
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let meta = ExecutorMeta::new(Schema::new(schema_columns), 6, INIT_CAP, MAX_CHUNK_SIZE);

    let conditions = match &join.on {
        Some(expr) => {
            let resolver = ScopeResolver { scope: &scope };
            vec![rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?]
        }
        None => Vec::new(),
    };
    let kind = match join.tp {
        tidb_ast::JoinType::Cross => JoinKind::Inner,
        tidb_ast::JoinType::Left => JoinKind::Left,
        tidb_ast::JoinType::Right => JoinKind::Right,
    };
    let exec: Box<dyn Executor> = Box::new(JoinExec::new(
        meta,
        kind,
        conditions,
        left_exec,
        right_exec,
        ctx.clone(),
    ));
    Ok((exec, scope))
}

/// The table a single-table `UPDATE`/`DELETE` targets.
fn single_table_name(
    table_ref: &tidb_ast::TableRef,
    current_db: &str,
) -> Result<(String, String), DriverError> {
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    Ok((database.to_owned(), name.to_owned()))
}

/// The value an omitted column takes, following Go `GetColDefaultValue` and
/// `getColDefaultValueFromNil`: the stored `DEFAULT` when one was written, or
/// NULL for a nullable column; a NOT NULL column with no default is Go's
/// `ErrNoDefaultForField` under strict mode.
///
/// DEFERRED (documented): non-strict mode, where Go warns and writes the
/// type's zero value instead of failing. This seed always behaves as strict
/// mode, which is TiDB's default sql_mode.
fn column_default(
    meta: &[(Option<Datum>, bool, String)],
    offset: usize,
) -> Result<Datum, DriverError> {
    let (default_value, not_null, name) = &meta[offset];
    match default_value {
        Some(value) => Ok(value.clone()),
        None if *not_null => Err(DriverError::NoDefaultForField(name.clone())),
        None => Ok(Datum::Null),
    }
}

/// Whether the column at `offset` carries Go's `NotNullFlag`.
fn column_is_not_null(meta: &[(Option<Datum>, bool, String)], offset: usize) -> bool {
    meta[offset].1
}

/// Runs a single-table `UPDATE`, returning MySQL's affected-row count.
///
/// Go `executor.UpdateExec` + `updateRecord`: each row the `WHERE` selects is
/// re-evaluated with the `SET` assignments applied, and a row is written back
/// only when a column actually changed. The affected-row count is the number
/// of CHANGED rows, not the number matched -- an unchanged row is "touched"
/// instead, and only a client that negotiated `CLIENT_FOUND_ROWS` sees it
/// counted (that capability is not modelled here, so the count is always the
/// changed-row count).
///
/// Assignments are evaluated against the row's ORIGINAL values, left to right,
/// with each assignment seeing the effects of the previous ones -- Go's
/// `composeNewRow` order.
///
/// DEFERRED (documented): multi-table UPDATE, `ORDER BY`/`LIMIT` tails,
/// `IGNORE`, `RETURNING`, generated and `ON UPDATE CURRENT_TIMESTAMP` columns,
/// and the handle-changed path (a row whose primary-key handle column is
/// assigned is deleted and re-inserted in Go; this seed rejects it).
pub fn run_update_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_update_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_update_on`] resolving unqualified names in `current_db`.
pub fn run_update_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let update = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Update(update) => update,
            _ => return Err(DriverError::Unsupported("only UPDATE is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only UPDATE is supported here")),
    };
    if update.ignore
        || !update.order_by.is_empty()
        || update.limit.is_some()
        || !update.returning.fields().is_empty()
    {
        return Err(DriverError::Unsupported(
            "only plain UPDATE t SET ... [WHERE ...] is supported",
        ));
    }
    let table_ref = match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => table_ref,
        tidb_ast::UpdateKind::Multi { .. } => {
            return Err(DriverError::Unsupported(
                "multi-table UPDATE is not supported yet",
            ))
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let column_list = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?
        .column_list();

    // SET targets, as offsets into the row.
    let mut assignments = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let column = assignment
            .col
            .last()
            .ok_or(DriverError::Unsupported("empty assignment target"))?;
        let offset = column_list
            .iter()
            .position(|(candidate, _)| candidate.eq_ignore_ascii_case(column))
            .ok_or(DriverError::Unsupported("unknown column in SET"))?;
        assignments.push((offset, assignment.value.clone()));
    }

    let resolver = TableResolver {
        table_name: &name,
        columns: &column_list,
    };
    let predicate = match &update.where_clause {
        Some(expr) => Some(
            rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ),
        None => None,
    };
    let mut set_exprs = Vec::with_capacity(assignments.len());
    for (offset, value) in &assignments {
        set_exprs.push((
            *offset,
            rewrite_expr_resolved(value, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ));
    }

    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let column_names: Vec<String> = column_list.iter().map(|(name, _)| name.clone()).collect();
    let entry = catalog
        .get_mut_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?;

    let mut changed = 0u64;
    match entry {
        TableEntry::Mem(mem) => {
            let mut updates = Vec::new();
            for (index, row) in mem.rows.iter().enumerate() {
                if let Some(new_row) = compute_updated_row(
                    row,
                    &field_types,
                    &column_names,
                    &predicate,
                    &set_exprs,
                    ctx,
                )? {
                    updates.push((index, new_row));
                }
            }
            changed = updates.len() as u64;
            for (index, new_row) in updates {
                mem.rows[index] = new_row;
            }
        }
        TableEntry::Kv(kv) => {
            let rows = kv
                .scan_rows_with_handles()
                .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
            for (handle, row) in rows {
                if let Some(new_row) = compute_updated_row(
                    &row,
                    &field_types,
                    &column_names,
                    &predicate,
                    &set_exprs,
                    ctx,
                )? {
                    kv.update_row(&handle, &new_row).map_err(|e| match e {
                        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                            DriverError::DuplicateEntry { value, key }
                        }
                        other => DriverError::Parse(format!("row encode failed: {other:?}")),
                    })?;
                    changed += 1;
                }
            }
        }
    }
    Ok(changed)
}

/// Applies the `SET` assignments to one row, returning the new row only when
/// the `WHERE` selected it AND a column actually changed (Go's `changed` flag).
fn compute_updated_row(
    row: &[Datum],
    field_types: &[FieldType],
    column_names: &[String],
    predicate: &Option<Expression>,
    set_exprs: &[(usize, Expression)],
    ctx: &crate::StmtContext,
) -> Result<Option<Vec<Datum>>, DriverError> {
    let chunk = row_chunk(row, field_types)?;
    if let Some(predicate) = predicate {
        let selected = predicate
            .eval(ctx, chunk.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        if !datum_is_true(&selected) {
            return Ok(None);
        }
    }
    let mut new_row = row.to_vec();
    for (offset, expr) in set_exprs {
        // Go evaluates each assignment over the row as the previous
        // assignments left it, so `SET a = 1, b = a` sees the new `a`.
        let source = row_chunk(&new_row, field_types)?;
        let value = expr
            .eval(ctx, source.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        // Go casts an assigned value to its column's type here too, which is
        // what stores `SET d = 9.87654` in a DECIMAL(10,3) column as 9.877.
        new_row[*offset] =
            cast_value_for_column(value, &field_types[*offset], &column_names[*offset], 0, ctx)?;
    }
    if new_row == row {
        // Go counts this row as touched, not affected.
        return Ok(None);
    }
    Ok(Some(new_row))
}

/// Runs a single-table `DELETE`, returning the number of removed rows.
///
/// Go `executor.DeleteExec`: every row the `WHERE` selects is removed, and the
/// affected-row count is simply that count.
///
/// DEFERRED (documented): multi-table DELETE, `ORDER BY`/`LIMIT` tails,
/// `IGNORE`, and `RETURNING`.
pub fn run_delete_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_delete_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_delete_on`] resolving unqualified names in `current_db`.
pub fn run_delete_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let delete = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Delete(delete) => delete,
            _ => return Err(DriverError::Unsupported("only DELETE is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only DELETE is supported here")),
    };
    if delete.ignore
        || delete.quick
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
        || !delete.returning.fields().is_empty()
    {
        return Err(DriverError::Unsupported(
            "only plain DELETE FROM t [WHERE ...] is supported",
        ));
    }
    let table_ref = match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => table_ref,
        tidb_ast::DeleteKind::Multi { .. } => {
            return Err(DriverError::Unsupported(
                "multi-table DELETE is not supported yet",
            ))
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let column_list = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?
        .column_list();
    let resolver = TableResolver {
        table_name: &name,
        columns: &column_list,
    };
    let predicate = match &delete.where_clause {
        Some(expr) => Some(
            rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ),
        None => None,
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let entry = catalog
        .get_mut_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?;

    let mut deleted = 0u64;
    match entry {
        TableEntry::Mem(mem) => {
            let mut kept = Vec::with_capacity(mem.rows.len());
            for row in std::mem::take(&mut mem.rows) {
                if row_is_selected(&row, &field_types, &predicate, ctx)? {
                    deleted += 1;
                } else {
                    kept.push(row);
                }
            }
            mem.rows = kept;
        }
        TableEntry::Kv(kv) => {
            let rows = kv
                .scan_rows_with_handles()
                .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
            for (handle, row) in rows {
                if row_is_selected(&row, &field_types, &predicate, ctx)? {
                    kv.delete_row(&handle)
                        .map_err(|e| DriverError::Parse(format!("row delete failed: {e:?}")))?;
                    deleted += 1;
                }
            }
        }
    }
    Ok(deleted)
}

/// Whether the `WHERE` predicate (absent = every row) selects this row.
fn row_is_selected(
    row: &[Datum],
    field_types: &[FieldType],
    predicate: &Option<Expression>,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    let chunk = row_chunk(row, field_types)?;
    let selected = predicate
        .eval(ctx, chunk.get_row(0))
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    Ok(datum_is_true(&selected))
}

/// A one-row chunk holding `row`, so an expression can be evaluated over it.
fn row_chunk(
    row: &[Datum],
    field_types: &[FieldType],
) -> Result<tidb_chunk::chunk::Chunk, DriverError> {
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(field_types, 1);
    for (i, value) in row.iter().enumerate() {
        chunk.append_datum(i, value);
    }
    Ok(chunk)
}

/// Go's `WHERE` truth test: NULL and zero are false.
fn datum_is_true(value: &Datum) -> bool {
    match value {
        Datum::Null => false,
        Datum::Int(v) => *v != 0,
        Datum::UInt(v) => *v != 0,
        Datum::Real(v) => *v != 0.0,
        other => !matches!(other, Datum::Null),
    }
}

/// Runs an aggregate `SELECT` (`GROUP BY` and/or aggregate select fields)
/// through [`HashAggExec`].
///
/// Faithful scope (deferred items documented): `COUNT`/`SUM` (Go models
/// `COUNT(*)` as the literal-`1` argument, which counts every row identically);
/// any non-aggregate select field becomes a `FIRST_ROW` carrier (Go's planner
/// does the same; `ONLY_FULL_GROUP_BY` validation is deferred); `DISTINCT`
/// other aggregate functions and `WITH ROLLUP` are rejected as unsupported.
/// `HAVING` and `ORDER BY` run over the aggregation's output, as in Go: an
/// aggregate appearing only in those clauses is appended as a hidden output
/// column and trimmed by a final projection.
fn run_aggregate_select(
    select: &tidb_ast::SelectStmt,
    from_source: Option<Box<dyn Executor>>,
    resolver: &ScopeResolver<'_>,
    _catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    if select.rollup {
        return Err(DriverError::Unsupported("WITH ROLLUP is not supported yet"));
    }

    // Fields -> aggregate functions (+ output names/types).
    let mut agg_funcs: Vec<AggFunc> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<FieldType> = Vec::new();
    for field in select.fields.fields() {
        let SelectField::Expr { expr, alias } = field else {
            return Err(DriverError::Unsupported(
                "`*` is not supported in an aggregate SELECT",
            ));
        };
        let display = alias.clone().unwrap_or_else(|| expr.restore());
        match expr {
            tidb_ast::Expr::Aggregate {
                name,
                distinct,
                args,
            } => {
                let [arg] = args.as_slice() else {
                    return Err(DriverError::Unsupported(
                        "multi-argument aggregates are deferred",
                    ));
                };
                let arg = rewrite_expr_resolved(arg, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let (kind, ftype) = agg_kind_and_type(name, &arg)?;
                agg_funcs.push(AggFunc {
                    kind,
                    arg: Some(arg),
                    distinct: *distinct,
                });
                names.push(display);
                types.push(ftype);
            }
            other => {
                // A plain field in an aggregate query rides FIRST_ROW.
                let rewritten = rewrite_expr_resolved(other, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let t = rewritten
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                agg_funcs.push(AggFunc {
                    kind: AggKind::FirstRow,
                    arg: Some(rewritten),
                    distinct: false,
                });
                names.push(match other {
                    tidb_ast::Expr::Column(path) => {
                        path.last().cloned().unwrap_or_else(|| other.restore())
                    }
                    _ => display,
                });
                types.push(t);
            }
        }
    }

    // Every select field has an output column; anything HAVING/ORDER BY adds
    // beyond this point is hidden and trimmed at the end.
    let visible_columns = names.len();

    // The grouped column names, which HAVING/ORDER BY may reference even when
    // the select list does not project them.
    let group_by_names: Vec<String> = select
        .group_by
        .iter()
        .filter_map(|item| match &item.expr {
            tidb_ast::Expr::Column(path) => path.last().cloned(),
            _ => None,
        })
        .collect();

    // HAVING / ORDER BY aggregates -> aggregation output columns.
    let having_expr = match &select.having {
        Some(having) => Some(substitute_aggregates(
            having,
            &mut agg_funcs,
            &mut names,
            &mut types,
            &group_by_names,
            resolver,
        )?),
        None => None,
    };
    let mut order_by_exprs = Vec::with_capacity(select.order_by.len());
    for item in &select.order_by {
        order_by_exprs.push((
            substitute_aggregates(
                &item.expr,
                &mut agg_funcs,
                &mut names,
                &mut types,
                &group_by_names,
                resolver,
            )?,
            item.desc,
        ));
    }

    // GROUP BY expressions (legacy ASC/DESC direction ignored, as in MySQL 8).
    let mut group_by = Vec::with_capacity(select.group_by.len());
    for item in &select.group_by {
        group_by.push(
            rewrite_expr_resolved(&item.expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }

    // Source (+ WHERE), as in the plain path.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match from_source {
        Some(exec) => {
            let schema = exec.schema().clone();
            (exec, schema)
        }
        None => (
            Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            )),
            Schema::new(vec![]),
        ),
    };
    if let Some(predicate) = &select.where_clause {
        let pred = rewrite_expr_resolved(predicate, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        source = Box::new(SelectionExec::new(
            ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            source,
            ctx.clone(),
        ));
    }

    // The aggregation output schema.
    let out_columns: Vec<Column> = types
        .iter()
        .enumerate()
        .map(|(i, ft)| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let out_schema = Schema::new(out_columns);

    let mut root: Box<dyn Executor> = Box::new(HashAggExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        group_by,
        agg_funcs,
        source,
        ctx.clone(),
    ));

    // HAVING filters the aggregation's output rows (Go's Selection above the
    // Aggregation), and ORDER BY sorts them.
    let agg_resolver = AggOutputResolver {
        names: names.clone(),
        types: types.clone(),
    };
    if let Some(having) = &having_expr {
        let predicate = rewrite_expr_resolved(having, &agg_resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        root = Box::new(SelectionExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            vec![predicate],
            root,
            ctx.clone(),
        ));
    }
    if !order_by_exprs.is_empty() {
        let mut by_items = Vec::with_capacity(order_by_exprs.len());
        for (expr, desc) in &order_by_exprs {
            by_items.push(SortByItem {
                expr: rewrite_expr_resolved(expr, &agg_resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
                desc: *desc,
            });
        }
        root = Box::new(SortExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            by_items,
            root,
            ctx.clone(),
        ));
    }
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
    }

    // Aggregates that only HAVING or ORDER BY needed are computed but not
    // selected, so a projection trims them back to the select list (Go's
    // final projection over the aggregation's schema).
    if visible_columns < names.len() {
        let visible: Vec<Expression> = (0..visible_columns)
            .map(|i| {
                let mut col = Column::new((i + 1) as i64, types[i].clone());
                col.index = i as i64;
                Expression::Column(col)
            })
            .collect();
        let visible_columns_schema: Vec<Column> = (0..visible_columns)
            .map(|i| {
                let mut col = Column::new((i + 1) as i64, types[i].clone());
                col.index = i as i64;
                col
            })
            .collect();
        root = Box::new(ProjectionExec::new(
            ExecutorMeta::new(
                Schema::new(visible_columns_schema),
                5,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            visible,
            root,
            ctx.clone(),
        ));
        names.truncate(visible_columns);
        types.truncate(visible_columns);
    }
    let ret_types: Vec<FieldType> = types.clone();

    // `SELECT DISTINCT` over an aggregate result deduplicates the output
    // rows, the same buildDistinct step the plain path applies.
    if select.distinct {
        let columns: Vec<Column> = ret_types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        let schema = Schema::new(columns);
        root = Box::new(distinct_over(root, &schema, ctx));
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
    Ok((names.into_iter().zip(ret_types).collect(), rows))
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
fn eval_limit_bound(expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
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

            // Later assignments see earlier ones, as in Go's composeNewRow.
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
                vec![vec![Datum::Int(7), Datum::Int(7)]],
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

            // Unsupported shapes fail closed.
            assert!(run_update_on(
                "UPDATE w SET a = 1 LIMIT 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .is_err());
            assert!(run_delete_on(
                "DELETE FROM w ORDER BY a LIMIT 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .is_err());
            assert!(run_update_on(
                "UPDATE w SET zzz = 1",
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

        // A CORRELATED subquery runs through Apply (see the apply tests).
        // ANY/ALL comparison subqueries are not supported yet.
        assert!(run_select_on(
            "SELECT a FROM s WHERE a > ANY (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
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

        // A correlated subquery returning several rows is still the 1242 case.
        assert!(matches!(
            run_select_on(
                "SELECT id FROM o WHERE v = (SELECT w FROM i WHERE i.id = o.id)",
                &catalog,
                &crate::StmtContext::for_query()
            ),
            Err(DriverError::Exec(ExecError::Unsupported(_)))
        ));

        // Correlated IN and ANY/ALL are still the deferred semi-join shapes.
        assert!(run_select_on(
            "SELECT id FROM o WHERE v IN (SELECT w FROM i WHERE i.id = o.id)",
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

        // No usable index: an unindexed column, no WHERE, or an OR.
        assert_eq!(ranges("SELECT id FROM q WHERE note = 'x'"), None);
        assert_eq!(ranges("SELECT id FROM q"), None);
        assert_eq!(
            ranges("SELECT id FROM q WHERE score > 1 OR score < 0"),
            None
        );
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

/// The MySQL error a driver failure becomes on the wire, which is also what
/// `SHOW WARNINGS` reports for a failed statement.
///
/// Go attaches the code, the SQLSTATE and the rendered message to the error
/// itself (`terror.Error`), so every surface that reports an error -- the
/// protocol, `SHOW WARNINGS`, the log -- reads the same three fields. This
/// keeps that single source of truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlError {
    /// MySQL error number.
    pub code: u16,
    /// Five-byte SQLSTATE.
    pub state: [u8; 5],
    /// The rendered message.
    pub message: String,
}

impl MysqlError {
    fn new(code: u16, state: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            code,
            state,
            message: message.into(),
        }
    }

    /// Go's catch-all `ER_UNKNOWN_ERROR` (1105), whose SQLSTATE is HY000.
    fn unknown(message: impl Into<String>) -> Self {
        Self::new(1105, *b"HY000", message)
    }
}

/// MySQL `ER_PARSE_ERROR`.
const ER_PARSE_ERROR: u16 = 1064;
/// TiDB `ErrWriteConflict`.
const ER_WRITE_CONFLICT: u16 = 9007;
/// MySQL `ER_UNKNOWN_SYSTEM_VARIABLE`.
const ER_UNKNOWN_SYSTEM_VARIABLE: u16 = 1193;
/// MySQL `ER_INCORRECT_GLOBAL_LOCAL_VAR`.
const ER_INCORRECT_GLOBAL_LOCAL_VAR: u16 = 1238;
/// MySQL `ER_SUBQUERY_NO_1_ROW`.
const ER_SUBQUERY_NO_1_ROW: u16 = 1242;
/// MySQL `ER_DB_CREATE_EXISTS`.
const ER_DB_CREATE_EXISTS: u16 = 1007;
/// MySQL `ER_NO_DB_ERROR`.
const ER_NO_DB_ERROR: u16 = 1046;
/// MySQL `ER_BAD_DB_ERROR`.
const ER_BAD_DB_ERROR: u16 = 1049;

impl DriverError {
    /// The code, SQLSTATE and message this failure reports.
    #[must_use]
    pub fn to_mysql_error(self) -> MysqlError {
        match self {
        DriverError::Parse(message) => MysqlError::new(
            ER_PARSE_ERROR,
            *b"42000",
            format!("You have an error in your SQL syntax: {message}"),
        ),
        DriverError::Unsupported(message) => MysqlError::unknown(message),
        DriverError::Exec(error) => MysqlError::unknown(format!("{error:?}")),
        DriverError::Txn(crate::TxnErrorKind::WriteConflict) => {
            MysqlError::new(
                ER_WRITE_CONFLICT,
                *b"HY000",
                "Write conflict, please retry the transaction".to_owned(),
            )
        }
        // Go: "The used SELECT statements have a different number of columns".
        DriverError::WrongNumberOfColumnsInSelect => MysqlError::new(
            1222,
            *b"21000",
            "The used SELECT statements have a different number of columns".to_owned(),
        ),
        // Go: "Incorrect table definition; there can be only one auto column
        // and it must be defined as a key".
        DriverError::WrongAutoKey => MysqlError::new(
            1075,
            *b"42000",
            "Incorrect table definition; there can be only one auto column and it must be defined as a key".to_owned(),
        ),
        // Go: "Incorrect column specifier for column '%-.192s'".
        DriverError::WrongColumnSpecifier(name) => MysqlError::new(
            1063,
            *b"42000",
            format!("Incorrect column specifier for column '{name}'"),
        ),
        // Go: "Column '%-.192s' cannot be null".
        DriverError::ColumnCannotBeNull(name) => {
            MysqlError::new(1048, *b"23000", format!("Column '{name}' cannot be null"))
        }
        // Go: "Field '%-.192s' doesn't have a default value".
        DriverError::NoDefaultForField(name) => MysqlError::new(
            1364,
            *b"HY000",
            format!("Field '{name}' doesn't have a default value"),
        ),
        // Go: "Duplicate entry '%-.64s' for key '%-.192s'".
        DriverError::DuplicateEntry { value, key } => MysqlError::new(
            1062,
            *b"23000",
            format!("Duplicate entry '{value}' for key '{key}'"),
        ),
        // Go: "Duplicate key name '%-.192s'".
        DriverError::DuplicateKeyName(name) => {
            MysqlError::new(1061, *b"42000", format!("Duplicate key name '{name}'"))
        }
        // Go: "index %s doesn't exist" -- 1091's index-specific message.
        DriverError::UnknownIndex(name) => {
            MysqlError::new(1091, *b"42000", format!("index {name} doesn't exist"))
        }
        // Go: "Duplicate column name '%-.192s'".
        DriverError::DuplicateColumnName(name) => {
            MysqlError::new(1060, *b"42S21", format!("Duplicate column name '{name}'"))
        }
        // Go: "Can't DROP '%-.192s'; check that column/key exists".
        DriverError::UnknownColumnInAlter(name) => MysqlError::new(
            1091,
            *b"42000",
            format!("Can't DROP '{name}'; check that column/key exists"),
        ),
        // Go: "can't drop only column %s in table %s".
        DriverError::CannotDropOnlyColumn { column, table } => MysqlError::new(
            1090,
            *b"42000",
            format!("can't drop only column {column} in table {table}"),
        ),
        // TiDB: "can't drop column %s with composite index covered or Primary
        // Key covered now".
        DriverError::CannotDropColumnWithCompositeIndex(name) => MysqlError::new(
            8200,
            *b"HY000",
            format!(
                "can't drop column {name} with composite index covered or Primary Key covered now"
            ),
        ),
        // Go: "function %s has only noop implementation in tidb now, use
        // tidb_enable_noop_functions to enable these functions" (1235).
        DriverError::FunctionsNoopImpl(clause) => MysqlError::new(
            1235,
            *b"42000",
            format!(
                "function {clause} has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            ),
        ),
        // TiDB: "Unsupported modify column: %s".
        DriverError::UnsupportedModifyColumn(reason) => MysqlError::new(
            8200,
            *b"HY000",
            format!("Unsupported modify column: {reason}"),
        ),
        // Go: "Unknown column '%-.192s' in '%-.192s'".
        DriverError::UnknownColumnInTable { column, table } => MysqlError::new(
            1054,
            *b"42S22",
            format!("Unknown column '{column}' in '{table}'"),
        ),
        // Go: "BLOB/TEXT column '%-.192s' used in key specification without a
        // key length".
        DriverError::BlobKeyWithoutLength(column) => MysqlError::new(
            1170,
            *b"42000",
            format!("BLOB/TEXT column '{column}' used in key specification without a key length"),
        ),
        // Go: "Truncated incorrect %-.32s value: '%-.128s'".
        DriverError::TruncatedIncorrectValue { kind, value } => MysqlError::new(
            1292,
            *b"22007",
            format!("Truncated incorrect {kind} value: '{value}'"),
        ),
        // Go: "Data truncated for column '%s', value is '%s'".
        DriverError::DataTruncatedValue { column, value } => MysqlError::new(
            1265,
            *b"01000",
            format!("Data truncated for column '{column}', value is '{value}'"),
        ),
        // Go: "Data truncated for column '%s' at row %d".
        DriverError::DataTruncatedAtRow { column, row } => MysqlError::new(
            1265,
            *b"01000",
            format!("Data truncated for column '{column}' at row {row}"),
        ),
        // TiDB: "Unsupported drop integer primary key".
        DriverError::UnsupportedDropIntegerPrimaryKey => MysqlError::new(
            8200,
            *b"HY000",
            "Unsupported drop integer primary key".to_owned(),
        ),
        // Go: "Table '%-.192s' already exists".
        DriverError::Schema(crate::SchemaErrorKind::TableExists(name)) => {
            MysqlError::new(1050, *b"42S01", format!("Table '{name}' already exists"))
        }
        // Go: "Unknown table '%-.129s'" -- DROP TABLE's own code, distinct
        // from the 1146 a read of a missing table reports.
        DriverError::Schema(crate::SchemaErrorKind::BadTable(name)) => {
            MysqlError::new(1051, *b"42S02", format!("Unknown table '{name}'"))
        }
        // Go: "Table '%-.192s' doesn't exist".
        DriverError::Schema(crate::SchemaErrorKind::UnknownTable(name)) => {
            MysqlError::new(1146, *b"42S02", format!("Table '{name}' doesn't exist"))
        }
        // Go: "Unknown database '%-.192s'".
        DriverError::Schema(crate::SchemaErrorKind::UnknownDatabase(
            name,
        )) => MysqlError::new(
            ER_BAD_DB_ERROR,
            *b"42000",
            format!("Unknown database '{name}'"),
        ),
        // Go: "Can't create database '%-.192s'; database exists".
        DriverError::Schema(crate::SchemaErrorKind::DatabaseExists(
            name,
        )) => MysqlError::new(
            ER_DB_CREATE_EXISTS,
            *b"HY000",
            format!("Can't create database '{name}'; database exists"),
        ),
        // Go: "No database selected".
        DriverError::Schema(crate::SchemaErrorKind::NoDatabaseSelected) => {
            MysqlError::new(ER_NO_DB_ERROR, *b"3D000", "No database selected".to_owned())
        }
        // Go: "Incorrect argument type to variable '%-.64s'".
        DriverError::Var(crate::VarErrorKind::WrongTypeForVar(name)) => {
            MysqlError::new(
                1232,
                *b"42000",
                format!("Incorrect argument type to variable '{name}'"),
            )
        }
        // Go: "Variable '%-.64s' can't be set to the value of '%-.200s'".
        DriverError::Var(crate::VarErrorKind::WrongValueForVar(
            name,
            value,
        )) => MysqlError::new(
            1231,
            *b"42000",
            format!("Variable '{name}' can't be set to the value of '{value}'"),
        ),
        // Go: "Unknown system variable '%-.64s'".
        DriverError::Var(crate::VarErrorKind::UnknownSystemVariable(
            name,
        )) => MysqlError::new(
            ER_UNKNOWN_SYSTEM_VARIABLE,
            *b"HY000",
            format!("Unknown system variable '{name}'"),
        ),
        // Go: "Variable '%-.192s' is a %s variable".
        DriverError::Var(crate::VarErrorKind::ReadOnlyVariable(name)) => {
            MysqlError::new(
                ER_INCORRECT_GLOBAL_LOCAL_VAR,
                *b"HY000",
                format!("Variable '{name}' is a read only variable"),
            )
        }
        DriverError::SubqueryReturnsMoreThanOneRow => MysqlError::new(
            ER_SUBQUERY_NO_1_ROW,
            *b"21000",
            "Subquery returns more than 1 row".to_owned(),
        ),
        // Go: "Invalid default value for '%-.192s'".
        DriverError::InvalidDefault(column) => MysqlError::new(
            1067,
            *b"42000",
            format!("Invalid default value for '{column}'"),
        ),
        // Go: "Data too long for column '%s' at row %d".
        DriverError::DataTooLong { column, row } => MysqlError::new(
            1406,
            *b"22001",
            format!("Data too long for column '{column}' at row {row}"),
        ),
        // Go: "Out of range value for column '%s' at row %d".
        DriverError::DataOutOfRange { column, row } => MysqlError::new(
            1264,
            *b"22003",
            format!("Out of range value for column '{column}' at row {row}"),
        ),
        // Go: "Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d".
        DriverError::IncorrectValue {
            type_name,
            value,
            column,
            row,
        } => MysqlError::new(
            1366,
            *b"HY000",
            format!("Incorrect {type_name} value: '{value}' for column '{column}' at row {row}"),
        ),
        DriverError::CatalogPoisoned => {
            MysqlError::unknown("the shared catalog is unusable after a failed statement")
        }
        }
    }
}
