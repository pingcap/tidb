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

/// A catalog of in-memory tables the driver can resolve `FROM` against.
/// Table names are case-insensitive, as in MySQL.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, TableEntry>,
    next_table_id: i64,
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
    /// Registers a matrix-backed `table` under `name` (case-insensitive).
    pub fn register(&mut self, name: &str, table: MemTable) {
        self.tables
            .insert(name.to_lowercase(), TableEntry::Mem(table));
    }

    /// Registers a TiKV-format-byte-backed `table` under `name`.
    pub fn register_kv(&mut self, name: &str, table: KvTable) {
        self.tables
            .insert(name.to_lowercase(), TableEntry::Kv(table));
    }

    fn get(&self, name: &str) -> Option<&TableEntry> {
        self.tables.get(&name.to_lowercase())
    }

    /// Whether a table with `name` exists (case-insensitive).
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_lowercase())
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

    // Resolve FROM: none -> table-dual; a single plain table -> the catalog.
    let table: Option<(&str, &TableEntry)> = match &select.from {
        None => None,
        Some(join) => {
            if join.right.is_some() {
                return Err(DriverError::Unsupported("joins are not supported yet"));
            }
            match &join.left {
                JoinNode::Table(table_ref) => {
                    let name = table_ref
                        .name
                        .last()
                        .ok_or(DriverError::Unsupported("empty table name"))?;
                    let table = catalog
                        .get(name)
                        .ok_or(DriverError::Unsupported("table not found in catalog"))?;
                    Some((name.as_str(), table))
                }
                _ => {
                    return Err(DriverError::Unsupported(
                        "derived tables are not supported yet",
                    ))
                }
            }
        }
    };

    // The column resolver for this query's scope.
    let column_list: Vec<(String, FieldType)> =
        table.map_or_else(Vec::new, |(_, t)| t.column_list());
    let resolver = TableResolver {
        table_name: table.map_or("", |(name, _)| name),
        columns: &column_list,
    };

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
        return run_aggregate_select(select, table, &column_list, &resolver, catalog);
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
                let Some((table_name, _)) = table else {
                    return Err(DriverError::Unsupported(
                        "`*` is not supported in a FROM-less SELECT",
                    ));
                };
                if let Some(q) = qualifier.last() {
                    if !q.eq_ignore_ascii_case(table_name) {
                        return Err(DriverError::Unsupported(
                            "`t.*` qualifier does not match the FROM table",
                        ));
                    }
                }
                for (i, (name, ft)) in column_list.iter().enumerate() {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    exprs.push(Expression::Column(col));
                    names.push(name.clone());
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
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match table {
        Some((_, entry)) => {
            let source_columns: Vec<Column> = column_list
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let schema = Schema::new(source_columns);
            let exec: Box<dyn Executor> = match entry {
                TableEntry::Mem(mem) => Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(schema.clone(), 0, INIT_CAP, MAX_CHUNK_SIZE),
                    mem.rows.clone(),
                )),
                TableEntry::Kv(kv) => Box::new(TableScanExec::new(
                    ExecutorMeta::new(schema.clone(), 0, INIT_CAP, MAX_CHUNK_SIZE),
                    kv.clone(),
                )),
            };
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

    // Optional WHERE: a selection over the source rows.
    if let Some(predicate) = &select.where_clause {
        let pred = rewrite_expr_resolved(predicate, &resolver)
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

    let table_name = insert
        .table
        .last()
        .ok_or(DriverError::Unsupported("empty table name"))?
        .clone();
    let table = catalog
        .tables
        .get_mut(&table_name.to_lowercase())
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

/// Runs an aggregate `SELECT` (`GROUP BY` and/or aggregate select fields)
/// through [`HashAggExec`].
///
/// Faithful scope (deferred items documented): `COUNT`/`SUM` (Go models
/// `COUNT(*)` as the literal-`1` argument, which counts every row identically);
/// any non-aggregate select field becomes a `FIRST_ROW` carrier (Go's planner
/// does the same; `ONLY_FULL_GROUP_BY` validation is deferred); `DISTINCT`
/// modifiers, other aggregate functions, `WITH ROLLUP`, `HAVING`, and
/// `ORDER BY` over aggregate output (needs output-schema resolution) are
/// rejected as unsupported.
fn run_aggregate_select(
    select: &tidb_ast::SelectStmt,
    table: Option<(&str, &TableEntry)>,
    column_list: &[(String, FieldType)],
    resolver: &TableResolver<'_>,
    _catalog: &Catalog,
) -> Result<SelectMeta, DriverError> {
    if select.rollup {
        return Err(DriverError::Unsupported("WITH ROLLUP is not supported yet"));
    }
    if select.having.is_some() {
        return Err(DriverError::Unsupported("HAVING is not supported yet"));
    }
    if !select.order_by.is_empty() {
        return Err(DriverError::Unsupported(
            "ORDER BY over aggregate output is not supported yet",
        ));
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
                if *distinct {
                    return Err(DriverError::Unsupported("DISTINCT aggregates are deferred"));
                }
                let [arg] = args.as_slice() else {
                    return Err(DriverError::Unsupported(
                        "multi-argument aggregates are deferred",
                    ));
                };
                let arg = rewrite_expr_resolved(arg, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let (kind, ftype) = match name.as_str() {
                    "COUNT" => (AggKind::Count, FieldType::new(FieldTypeCode::LongLong)),
                    "SUM" => {
                        let t = arg
                            .static_type()
                            .cloned()
                            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                        (AggKind::Sum, t)
                    }
                    _ => {
                        return Err(DriverError::Unsupported(
                            "this aggregate function is deferred",
                        ))
                    }
                };
                agg_funcs.push(AggFunc {
                    kind,
                    arg: Some(arg),
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

    // GROUP BY expressions (legacy ASC/DESC direction ignored, as in MySQL 8).
    let mut group_by = Vec::with_capacity(select.group_by.len());
    for item in &select.group_by {
        group_by.push(
            rewrite_expr_resolved(&item.expr, resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }

    // Source (+ WHERE), as in the plain path.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match table {
        Some((_, entry)) => {
            let source_columns: Vec<Column> = column_list
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let schema = Schema::new(source_columns);
            let exec: Box<dyn Executor> = match entry {
                TableEntry::Mem(mem) => Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(schema.clone(), 0, INIT_CAP, MAX_CHUNK_SIZE),
                    mem.rows.clone(),
                )),
                TableEntry::Kv(kv) => Box::new(TableScanExec::new(
                    ExecutorMeta::new(schema.clone(), 0, INIT_CAP, MAX_CHUNK_SIZE),
                    kv.clone(),
                )),
            };
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
    let ret_types: Vec<FieldType> = types.clone();

    let mut root: Box<dyn Executor> = Box::new(HashAggExec::new(
        ExecutorMeta::new(out_schema, 2, INIT_CAP, MAX_CHUNK_SIZE),
        group_by,
        agg_funcs,
        source,
        NoColumns,
    ));
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
        // Unsupported aggregate shapes fail closed.
        assert!(run_select_on("SELECT AVG(a) FROM t", &catalog).is_err());
        assert!(run_select_on("SELECT COUNT(DISTINCT a) FROM t", &catalog).is_err());
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
