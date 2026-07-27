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
use crate::kv_table::{KvTable, TableScanExec};
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
use tidb_expr::NoColumns;

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
    /// DIVERGENCE (documented): real TiDB also exposes `mysql`,
    /// `information_schema`, `performance_schema`, `sys` and `metrics_schema`.
    /// Those are system schemas whose tables this seed does not implement, and
    /// listing them empty would claim more than is true, so they are absent
    /// until their contents are ported.
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_owned(),
            Database {
                name: DEFAULT_DATABASE.to_owned(),
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

    /// Whether `database` holds a table called `name`.
    #[must_use]
    pub fn contains_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some()
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
#[derive(Debug)]
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
    /// Go `ER_SUBQUERY_NO_1_ROW` (1242): a scalar subquery produced more than
    /// one row.
    SubqueryReturnsMoreThanOneRow,
}

/// Why a schema statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaErrorKind {
    /// Go `infoschema.ErrDatabaseNotExists` / `ErrBadDB` (1049).
    UnknownDatabase(String),
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
    run_select_on(sql, &Catalog::default())
}

/// Parses and runs a single-table (or `FROM`-less) `SELECT` against `catalog`,
/// returning its rows as `Datum`s.
pub fn run_select_on(sql: &str, catalog: &Catalog) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_meta_on(sql, catalog).map(|(_, rows)| rows)
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
pub fn run_select_meta_on(sql: &str, catalog: &Catalog) -> Result<SelectMeta, DriverError> {
    run_select_meta_in(sql, catalog, DEFAULT_DATABASE)
}

/// [`run_select_meta_on`] resolving unqualified names in `current_db`.
pub fn run_select_meta_in(
    sql: &str,
    catalog: &Catalog,
    current_db: &str,
) -> Result<SelectMeta, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let select = match &stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select,
            QueryStmt::SetOpr(_) => {
                return Err(DriverError::Unsupported("set operations are not supported"))
            }
        },
        _ => return Err(DriverError::Unsupported("only SELECT is supported")),
    };
    run_select_stmt(select, catalog, current_db)
}

/// Runs one parsed `SELECT` against the catalog, for a caller that has
/// already rewritten the statement (session-variable binding, for instance)
/// and must not go back through SQL text.
pub fn run_select_meta_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Result<SelectMeta, DriverError> {
    run_select_stmt(select, catalog, current_db)
}

/// Runs one parsed `SELECT` against the catalog.
fn run_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Result<SelectMeta, DriverError> {
    // Uncorrelated subqueries are evaluated now and folded into literals, so
    // everything below plans against ordinary expressions (Go's
    // handleScalarSubquery for the non-Apply case).
    let folded;
    let select = if select_has_uncorrelated_subquery(select, catalog, current_db) {
        folded = fold_select_subqueries(select, catalog, current_db)?;
        &folded
    } else {
        select
    };

    // Resolve FROM: none -> table-dual; otherwise the (possibly joined) tables.
    let (from_source, scope): (Option<Box<dyn Executor>>, FromScope) = match &select.from {
        None => (None, FromScope::default()),
        Some(join) => {
            let (exec, scope) = build_join(join, catalog, current_db)?;
            (Some(exec), scope)
        }
    };

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
        return run_aggregate_select(select, from_source, &resolver, catalog);
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
        )?;
        let (predicate_resolver, predicate_scope);
        let mut source_schema = source_schema;
        if let Some(correlated) = correlated {
            // The Apply's schema is the source's columns plus the subquery's.
            let mut applied = scope.clone();
            let mut value_type = FieldType::new(FieldTypeCode::LongLong);
            if correlated.exists.is_none() {
                value_type = subquery_result_type(&correlated.select, catalog, current_db)
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
            let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
                run_correlated_subquery(
                    &correlated,
                    values,
                    &inner_scope,
                    &inner_catalog,
                    &inner_db,
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
            NoColumns,
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
            NoColumns,
        ));
    }

    // Projection of the rewritten fields.
    let mut root: Box<dyn Executor> = Box::new(ProjectionExec::new(
        ExecutorMeta::new(out_schema, 2, INIT_CAP, MAX_CHUNK_SIZE),
        exprs,
        source,
        NoColumns,
    ));

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
pub fn run_insert_on(sql: &str, catalog: &mut Catalog) -> Result<u64, DriverError> {
    run_insert_in(sql, catalog, DEFAULT_DATABASE)
}

/// [`run_insert_on`] resolving unqualified names in `current_db`.
pub fn run_insert_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<u64, DriverError> {
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
    let mut inserted = 0u64;
    let mut new_rows: Vec<Vec<Datum>> = Vec::with_capacity(insert.rows.len());
    for value_row in &insert.rows {
        if value_row.len() != target_offsets.len() {
            return Err(DriverError::Unsupported(
                "VALUES arity does not match the column list",
            ));
        }
        let mut row = vec![Datum::Null; column_list.len()];
        for (expr, &offset) in value_row.iter().zip(&target_offsets) {
            let rewritten = rewrite_expr_resolved(expr, &NoResolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            let value = rewritten
                .eval(&NoColumns, eval_chunk.get_row(0))
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            row[offset] = value;
        }
        new_rows.push(row);
        inserted += 1;
    }
    match table {
        TableEntry::Mem(mem) => mem.rows.extend(new_rows),
        TableEntry::Kv(kv) => {
            for row in &new_rows {
                kv.insert_row(row)
                    .map_err(|e| DriverError::Parse(format!("row encode failed: {e:?}")))?;
            }
        }
    }
    Ok(inserted)
}

/// Go `aggregation.NewAggFuncDesc` + `baseFuncDesc.TypeInfer`: the aggregate
/// kind and the result type inferred for its argument.
fn agg_kind_and_type(name: &str, arg: &Expression) -> Result<(AggKind, FieldType), DriverError> {
    Ok(match name {
        "COUNT" => (AggKind::Count, FieldType::new(FieldTypeCode::LongLong)),
        "SUM" => {
            let t = arg
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
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
) -> Option<FieldType> {
    run_select_stmt(select, catalog, current_db)
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
) {
    let inner = match &select.from {
        None => FromScope::default(),
        Some(join) => match build_join(join, catalog, current_db) {
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

    let (_, rows) = run_select_stmt(&bound, catalog, current_db)?;
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
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns);
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
            inner, outer, catalog, current_db, index, found,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(extract_correlated_subquery(
                inner, outer, catalog, current_db, index, found,
            )?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(extract_correlated_subquery(
                expr, outer, catalog, current_db, index, found,
            )?),
            target: *target,
            not: *not,
        },
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(extract_correlated_subquery(
                lhs, outer, catalog, current_db, index, found,
            )?),
            Box::new(extract_correlated_subquery(
                rhs, outer, catalog, current_db, index, found,
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
) -> bool {
    if let Some(where_clause) = &select.where_clause {
        if expr_has_subquery(where_clause) {
            let outer = match &select.from {
                None => FromScope::default(),
                Some(join) => match build_join(join, catalog, current_db) {
                    Ok((_, scope)) => scope,
                    Err(_) => FromScope::default(),
                },
            };
            let mut found = None;
            // A correlated WHERE subquery is the Apply path's job.
            if extract_correlated_subquery(where_clause, &outer, catalog, current_db, 0, &mut found)
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
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut folded = select.clone();
    for field in folded.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = fold_subqueries(expr, catalog, current_db)?;
        }
    }
    if let Some(where_clause) = &folded.where_clause {
        folded.where_clause = Some(fold_subqueries(where_clause, catalog, current_db)?);
    }
    if let Some(having) = &folded.having {
        folded.having = Some(fold_subqueries(having, catalog, current_db)?);
    }
    for item in &mut folded.order_by {
        item.expr = fold_subqueries(&item.expr, catalog, current_db)?;
    }
    for item in &mut folded.group_by {
        item.expr = fold_subqueries(&item.expr, catalog, current_db)?;
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
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match expr {
        Expr::Subquery(query) => {
            let rows = run_subquery(query, catalog, current_db)?;
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
            let rows = run_subquery(subquery, catalog, current_db)?;
            let exists = !rows.is_empty();
            Expr::Int(i64::from(exists != *not).to_string())
        }
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => {
            let rows = run_subquery(subquery, catalog, current_db)?;
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
                expr: Box::new(fold_subqueries(expr, catalog, current_db)?),
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
        Expr::Paren(inner) => Expr::Paren(Box::new(fold_subqueries(inner, catalog, current_db)?)),
        Expr::Unary(op, inner) => {
            Expr::Unary(*op, Box::new(fold_subqueries(inner, catalog, current_db)?))
        }
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(fold_subqueries(lhs, catalog, current_db)?),
            Box::new(fold_subqueries(rhs, catalog, current_db)?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(fold_subqueries(expr, catalog, current_db)?),
            target: *target,
            not: *not,
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(fold_subqueries(expr, catalog, current_db)?),
            list: list
                .iter()
                .map(|item| fold_subqueries(item, catalog, current_db))
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
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let tidb_ast::QueryStmt::Select(_) = query else {
        return Err(DriverError::Unsupported(
            "set-operation subqueries are not supported yet",
        ));
    };
    let tidb_ast::QueryStmt::Select(select) = query else {
        unreachable!("the set-operation case is rejected above")
    };
    run_select_stmt(select, catalog, current_db).map(|(_, rows)| rows)
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
        JoinNode::Join(join) => build_join(join, catalog, current_db),
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
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let (left_exec, left_scope) = build_from(&join.left, catalog, current_db)?;
    let Some(right_node) = &join.right else {
        // The single-table wrapper the parser always produces.
        return Ok((left_exec, left_scope));
    };
    if join.natural || !join.using.is_empty() {
        return Err(DriverError::Unsupported(
            "NATURAL and USING joins are not supported yet",
        ));
    }
    let (right_exec, right_scope) = build_from(right_node, catalog, current_db)?;

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
        meta, kind, conditions, left_exec, right_exec, NoColumns,
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
pub fn run_update_on(sql: &str, catalog: &mut Catalog) -> Result<u64, DriverError> {
    run_update_in(sql, catalog, DEFAULT_DATABASE)
}

/// [`run_update_on`] resolving unqualified names in `current_db`.
pub fn run_update_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
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
    let entry = catalog
        .get_mut_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?;

    let mut changed = 0u64;
    match entry {
        TableEntry::Mem(mem) => {
            let mut updates = Vec::new();
            for (index, row) in mem.rows.iter().enumerate() {
                if let Some(new_row) =
                    compute_updated_row(row, &field_types, &predicate, &set_exprs)?
                {
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
                if let Some(new_row) =
                    compute_updated_row(&row, &field_types, &predicate, &set_exprs)?
                {
                    kv.update_row(handle, &new_row)
                        .map_err(|e| DriverError::Parse(format!("row encode failed: {e:?}")))?;
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
    predicate: &Option<Expression>,
    set_exprs: &[(usize, Expression)],
) -> Result<Option<Vec<Datum>>, DriverError> {
    let chunk = row_chunk(row, field_types)?;
    if let Some(predicate) = predicate {
        let selected = predicate
            .eval(&NoColumns, chunk.get_row(0))
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
            .eval(&NoColumns, source.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        new_row[*offset] = value;
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
pub fn run_delete_on(sql: &str, catalog: &mut Catalog) -> Result<u64, DriverError> {
    run_delete_in(sql, catalog, DEFAULT_DATABASE)
}

/// [`run_delete_on`] resolving unqualified names in `current_db`.
pub fn run_delete_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
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
                if row_is_selected(&row, &field_types, &predicate)? {
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
                if row_is_selected(&row, &field_types, &predicate)? {
                    kv.delete_row(handle)
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
) -> Result<bool, DriverError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    let chunk = row_chunk(row, field_types)?;
    let selected = predicate
        .eval(&NoColumns, chunk.get_row(0))
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
            NoColumns,
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
        NoColumns,
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
            NoColumns,
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
            NoColumns,
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
            NoColumns,
        ));
        names.truncate(visible_columns);
        types.truncate(visible_columns);
    }
    let ret_types: Vec<FieldType> = types.clone();

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
            run_select_on("SELECT a FROM t", &catalog).unwrap(),
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ]
        );
        // Wildcard, qualified column, and an expression over columns.
        assert_eq!(
            run_select_on("SELECT * FROM t WHERE t.a > 1", &catalog).unwrap(),
            vec![
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(3), Datum::Int(10)],
            ]
        );
        assert_eq!(
            run_select_on("SELECT a + b FROM t WHERE a = 2", &catalog).unwrap(),
            vec![vec![Datum::Int(22)]]
        );
    }

    #[test]
    fn insert_then_select_round_trip() {
        let mut catalog = test_catalog();
        // Full-row insert.
        assert_eq!(
            run_insert_on("INSERT INTO t VALUES (4, 40), (5, 50)", &mut catalog).unwrap(),
            2
        );
        // Column-list insert: unspecified column fills with NULL.
        assert_eq!(
            run_insert_on("INSERT INTO t (a) VALUES (6)", &mut catalog).unwrap(),
            1
        );
        assert_eq!(
            run_select_on("SELECT a, b FROM t WHERE a > 3 ORDER BY a", &catalog).unwrap(),
            vec![
                vec![Datum::Int(4), Datum::Int(40)],
                vec![Datum::Int(5), Datum::Int(50)],
                vec![Datum::Int(6), Datum::Null],
            ]
        );
        // Arity mismatch and unknown table are rejected.
        assert!(run_insert_on("INSERT INTO t (a) VALUES (1, 2)", &mut catalog).is_err());
        assert!(run_insert_on("INSERT INTO missing VALUES (1)", &mut catalog).is_err());
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
                    },
                    KvColumn {
                        name: "b".to_owned(),
                        id: 2,
                        field_type: FieldType::new(FieldTypeCode::LongLong),
                    },
                ],
            ),
        );

        assert_eq!(
            run_insert_on(
                "INSERT INTO kt VALUES (1, 10), (2, 20), (3, 30)",
                &mut catalog
            )
            .unwrap(),
            3
        );
        assert_eq!(
            run_select_on("SELECT a, b FROM kt WHERE a > 1 ORDER BY b DESC", &catalog).unwrap(),
            vec![
                vec![Datum::Int(3), Datum::Int(30)],
                vec![Datum::Int(2), Datum::Int(20)],
            ]
        );
        assert_eq!(
            run_select_on("SELECT a + b FROM kt WHERE a = 1", &catalog).unwrap(),
            vec![vec![Datum::Int(11)]]
        );
    }

    #[test]
    fn aggregate_selects() {
        let catalog = test_catalog();
        // Global aggregates: rows (1,30),(2,20),(3,10).
        assert_eq!(
            run_select_on("SELECT COUNT(*), SUM(a) FROM t", &catalog).unwrap(),
            vec![vec![Datum::Int(3), Datum::Int(6)]]
        );
        // GROUP BY with a carried key column, WHERE below the agg.
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a",
                &catalog
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(1)],
                vec![Datum::Int(2), Datum::Int(1)],
            ]
        );
        // Empty-input rules through SQL: global agg over no rows -> one row.
        assert_eq!(
            run_select_on("SELECT COUNT(a) FROM t WHERE a > 100", &catalog).unwrap(),
            vec![vec![Datum::Int(0)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM t WHERE a > 100 GROUP BY a",
                &catalog
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        // MIN/MAX over the shared datum ordering.
        assert_eq!(
            run_select_on("SELECT MIN(a), MAX(b) FROM t", &catalog).unwrap(),
            vec![vec![Datum::Int(1), Datum::Int(30)]]
        );
        // AVG over integers is DECIMAL, scaled by div_precision_increment.
        assert_eq!(
            run_select_on("SELECT AVG(a) FROM t", &catalog).unwrap(),
            vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
                "2.0000"
            ))]]
        );
        // DISTINCT folds repeated inputs once per group: a is 1,2,3 while the
        // constant 1 collapses to a single counted value.
        assert_eq!(
            run_select_on(
                "SELECT COUNT(DISTINCT a), COUNT(DISTINCT 1) FROM t",
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(3), Datum::Int(1)]]
        );
        // An all-NULL / empty group is NULL for MIN/MAX and AVG, as in Go.
        assert_eq!(
            run_select_on(
                "SELECT MIN(a), MAX(a), AVG(a) FROM t WHERE a > 100",
                &catalog
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
        )
        .unwrap();

        // HAVING over an aggregate that IS in the select list.
        assert_eq!(
            run_select_on(
                "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1",
                &catalog
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(3), Datum::Int(2)],
            ]
        );
        // HAVING over an aggregate that is NOT selected: one output column.
        assert_eq!(
            run_select_on("SELECT a FROM g GROUP BY a HAVING SUM(b) > 15", &catalog).unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // ORDER BY an aggregate that is not selected, descending.
        assert_eq!(
            run_select_on("SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC", &catalog).unwrap(),
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
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(3), Datum::Int(15)]]
        );
        // ORDER BY a selected alias.
        assert_eq!(
            run_select_on(
                "SELECT a, SUM(b) AS total FROM g GROUP BY a ORDER BY total",
                &catalog
            )
            .unwrap(),
            vec![
                vec![Datum::Int(2), Datum::Int(5)],
                vec![Datum::Int(3), Datum::Int(15)],
                vec![Datum::Int(1), Datum::Int(30)],
            ]
        );
        // A grouped column that is not selected is still visible to HAVING
        // and ORDER BY (Go carries it as a hidden FIRST_ROW column).
        assert_eq!(
            run_select_on("SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1", &catalog).unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        // A global aggregate's HAVING filters the single group.
        assert_eq!(
            run_select_on("SELECT COUNT(*) FROM g HAVING COUNT(*) > 100", &catalog).unwrap(),
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
            )
            .unwrap();

            // WHERE-selected update, counting only changed rows.
            assert_eq!(
                run_update_on("UPDATE w SET b = b + 1 WHERE a >= 2", &mut catalog).unwrap(),
                2,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on("SELECT a, b FROM w", &catalog).unwrap(),
                vec![
                    vec![Datum::Int(1), Datum::Int(10)],
                    vec![Datum::Int(2), Datum::Int(21)],
                    vec![Datum::Int(3), Datum::Int(31)],
                ],
                "kv={kv}"
            );

            // A no-op update matches rows but changes none: MySQL reports 0.
            assert_eq!(
                run_update_on("UPDATE w SET b = b WHERE a = 1", &mut catalog).unwrap(),
                0,
                "kv={kv}"
            );

            // Later assignments see earlier ones, as in Go's composeNewRow.
            assert_eq!(
                run_update_on("UPDATE w SET a = 7, b = a WHERE a = 1", &mut catalog).unwrap(),
                1,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on("SELECT a, b FROM w WHERE a = 7", &catalog).unwrap(),
                vec![vec![Datum::Int(7), Datum::Int(7)]],
                "kv={kv}"
            );

            // A WHERE-less UPDATE touches every row.
            assert_eq!(
                run_update_on("UPDATE w SET b = 0", &mut catalog).unwrap(),
                3,
                "kv={kv}"
            );

            // DELETE removes the selected rows and reports their count.
            assert_eq!(
                run_delete_on("DELETE FROM w WHERE a >= 3", &mut catalog).unwrap(),
                2,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on("SELECT a FROM w", &catalog).unwrap(),
                vec![vec![Datum::Int(2)]],
                "kv={kv}"
            );

            // A WHERE-less DELETE empties the table, and re-inserting works
            // after it (the store is genuinely empty, not just filtered).
            assert_eq!(
                run_delete_on("DELETE FROM w", &mut catalog).unwrap(),
                1,
                "kv={kv}"
            );
            assert_eq!(
                run_select_on("SELECT a FROM w", &catalog).unwrap(),
                Vec::<Vec<Datum>>::new(),
                "kv={kv}"
            );
            run_insert_on("INSERT INTO w VALUES (9, 9)", &mut catalog).unwrap();
            assert_eq!(
                run_select_on("SELECT a FROM w", &catalog).unwrap(),
                vec![vec![Datum::Int(9)]],
                "kv={kv}"
            );

            // Unsupported shapes fail closed.
            assert!(run_update_on("UPDATE w SET a = 1 LIMIT 1", &mut catalog).is_err());
            assert!(run_delete_on("DELETE FROM w ORDER BY a LIMIT 1", &mut catalog).is_err());
            assert!(run_update_on("UPDATE w SET zzz = 1", &mut catalog).is_err());
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
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO r VALUES (1, 100), (3, 300), (3, 301)",
            &mut catalog,
        )
        .unwrap();

        // INNER JOIN: only matches, and a left row matching twice emits twice.
        assert_eq!(
            run_select_on(
                "SELECT l.id, l.v, r.w FROM l JOIN r ON l.id = r.id",
                &catalog
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
                &catalog
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
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );
        // A condition in ON does NOT drop the left row; it only stops matching.
        assert_eq!(
            run_select_on(
                "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id AND r.w > 200",
                &catalog
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
                &catalog
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
            run_select_on("SELECT l.id FROM l, r", &catalog)
                .unwrap()
                .len(),
            9
        );

        // `*` expands across both tables in FROM order; `t.*` over one.
        assert_eq!(
            run_select_on("SELECT * FROM l JOIN r ON l.id = r.id", &catalog)
                .unwrap()
                .first()
                .unwrap()
                .len(),
            4
        );
        assert_eq!(
            run_select_on("SELECT r.* FROM l JOIN r ON l.id = r.id", &catalog)
                .unwrap()
                .first()
                .unwrap()
                .len(),
            2
        );

        // An unqualified column present in both tables is ambiguous, as in
        // MySQL; one present in only one table resolves.
        assert!(run_select_on("SELECT id FROM l JOIN r ON l.id = r.id", &catalog).is_err());
        assert_eq!(
            run_select_on("SELECT v, w FROM l JOIN r ON l.id = r.id", &catalog)
                .unwrap()
                .len(),
            3
        );

        // An alias replaces the table name for qualification.
        assert_eq!(
            run_select_on(
                "SELECT a.id FROM l AS a JOIN r AS b ON a.id = b.id",
                &catalog
            )
            .unwrap()
            .len(),
            3
        );

        // A three-table left-deep chain, and an aggregate over a join.
        crate::run_create_table_on("CREATE TABLE m (id BIGINT)", &mut catalog).unwrap();
        run_insert_on("INSERT INTO m VALUES (3)", &mut catalog).unwrap();
        assert_eq!(
            run_select_on(
                "SELECT COUNT(*) FROM l JOIN r ON l.id = r.id JOIN m ON m.id = r.id",
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]]
        );

        // Unsupported join shapes fail closed.
        assert!(run_select_on("SELECT * FROM l NATURAL JOIN r", &catalog).is_err());
        assert!(run_select_on("SELECT * FROM l JOIN r USING (id)", &catalog).is_err());
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
        )
        .unwrap();
        run_insert_on("INSERT INTO u VALUES (2), (3)", &mut catalog).unwrap();

        // A scalar subquery in the select list and in a predicate.
        assert_eq!(
            run_select_on("SELECT (SELECT MAX(b) FROM s)", &catalog).unwrap(),
            vec![vec![Datum::Int(30)]]
        );
        assert_eq!(
            run_select_on("SELECT a FROM s WHERE b = (SELECT MAX(b) FROM s)", &catalog).unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // No rows is NULL, as Go's buildMaxOneRow leaves it.
        assert_eq!(
            run_select_on("SELECT (SELECT a FROM s WHERE a > 100)", &catalog).unwrap(),
            vec![vec![Datum::Null]]
        );
        // More than one row is Go's ER_SUBQUERY_NO_1_ROW.
        assert!(matches!(
            run_select_on("SELECT (SELECT a FROM s)", &catalog),
            Err(DriverError::SubqueryReturnsMoreThanOneRow)
        ));

        // IN / NOT IN over a subquery.
        assert_eq!(
            run_select_on("SELECT a FROM s WHERE a IN (SELECT a FROM u)", &catalog).unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );
        assert_eq!(
            run_select_on("SELECT a FROM s WHERE a NOT IN (SELECT a FROM u)", &catalog).unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // An empty IN subquery matches nothing, and NOT IN over it matches all.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a IN (SELECT a FROM u WHERE a > 100)",
                &catalog
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u WHERE a > 100)",
                &catalog
            )
            .unwrap()
            .len(),
            3
        );

        // EXISTS / NOT EXISTS fold to 1 / 0.
        assert_eq!(
            run_select_on("SELECT a FROM s WHERE EXISTS (SELECT 1 FROM u)", &catalog)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM s WHERE NOT EXISTS (SELECT 1 FROM u)",
                &catalog
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );

        // A subquery in HAVING, over the aggregate path.
        assert_eq!(
            run_select_on(
                "SELECT a FROM s GROUP BY a HAVING SUM(b) > (SELECT MIN(b) FROM s)",
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
        );

        // A CORRELATED subquery runs through Apply (see the apply tests).
        // ANY/ALL comparison subqueries are not supported yet.
        assert!(
            run_select_on("SELECT a FROM s WHERE a > ANY (SELECT a FROM u)", &catalog).is_err()
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
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO i VALUES (1, 10), (2, 5), (2, 25), (4, 40)",
            &mut catalog,
        )
        .unwrap();

        // Scalar: each outer row compares against its own inner maximum.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v = (SELECT MAX(w) FROM i WHERE i.id = o.id)",
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );
        // id 2's inner rows are 5 and 25, so its max is 25 and 20 < 25 holds;
        // id 1 compares 10 < 10, which does not.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE v < (SELECT MAX(w) FROM i WHERE i.id = o.id)",
                &catalog
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
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // Correlated EXISTS / NOT EXISTS, the semi- and anti-join shapes.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(3)]]
        );

        // An unqualified inner reference to an outer column still correlates.
        assert_eq!(
            run_select_on(
                "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.w = v)",
                &catalog
            )
            .unwrap(),
            vec![vec![Datum::Int(1)]]
        );

        // A correlated subquery returning several rows is still the 1242 case.
        assert!(matches!(
            run_select_on(
                "SELECT id FROM o WHERE v = (SELECT w FROM i WHERE i.id = o.id)",
                &catalog
            ),
            Err(DriverError::Exec(ExecError::Unsupported(_)))
        ));

        // Correlated IN and ANY/ALL are still the deferred semi-join shapes.
        assert!(run_select_on(
            "SELECT id FROM o WHERE v IN (SELECT w FROM i WHERE i.id = o.id)",
            &catalog
        )
        .is_err());
    }

    #[test]
    fn select_from_table_order_limit() {
        let catalog = test_catalog();
        // ORDER BY a column that is not projected (sort runs below projection).
        assert_eq!(
            run_select_on("SELECT a FROM t ORDER BY b", &catalog).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(2)],
                vec![Datum::Int(1)]
            ]
        );
        assert_eq!(
            run_select_on("SELECT a FROM t ORDER BY b DESC LIMIT 2", &catalog).unwrap(),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
        );
    }
}
