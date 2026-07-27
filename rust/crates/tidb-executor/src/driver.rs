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
//! SCOPE: `FROM`-less `SELECT <exprs> [WHERE <pred>]` over constants and
//! operators. It parses via `tidb-parser`, rewrites each field/predicate through
//! [`tidb_expr::rewriter::rewrite_expr`], sources one virtual row from
//! [`TableDualExec`], optionally filters with [`SelectionExec`], projects with
//! [`ProjectionExec`], and collects the output rows as [`Datum`]s.
//!
//! DEFERRED (documented): `FROM` clauses / table sources (need a table source +
//! column resolution), `*` wildcards, result-type inference (a projected value
//! whose expression is a `ScalarFunction` is stored in a placeholder `LongLong`
//! column, so non-integer arithmetic results would be misread until inference
//! lands), and everything the rewriter does not yet handle.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::limit::LimitExec;
use crate::projection::ProjectionExec;
use crate::selection::SelectionExec;
use crate::sort::{SortByItem, SortExec};
use crate::table_dual::TableDualExec;
use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::rewrite_expr;
use tidb_expr::schema::Schema;
use tidb_expr::NoColumns;

/// A failure while running a SQL string through the driver.
#[derive(Debug)]
pub enum DriverError {
    /// The SQL failed to parse.
    Parse(String),
    /// The statement is not a supported `FROM`-less `SELECT`.
    Unsupported(&'static str),
    /// Rewriting an expression or executing failed.
    Exec(ExecError),
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

    if select.from.is_some() {
        return Err(DriverError::Unsupported(
            "FROM clauses are not supported yet (no table source)",
        ));
    }

    // Rewrite each projected field into an evaluable expression.
    let mut exprs: Vec<Expression> = Vec::new();
    for field in select.fields.fields() {
        match field {
            SelectField::Expr { expr, .. } => {
                let rewritten =
                    rewrite_expr(expr).map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                exprs.push(rewritten);
            }
            SelectField::Wildcard(_) => {
                return Err(DriverError::Unsupported(
                    "`*` is not supported in a FROM-less SELECT",
                ))
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

    // Source: one virtual row from a table-dual.
    let dual = TableDualExec::new(
        ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
        1,
    );

    // Optional WHERE: a selection over the dual row.
    let source: Box<dyn Executor> = if let Some(predicate) = &select.where_clause {
        let pred = rewrite_expr(predicate).map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        Box::new(SelectionExec::new(
            ExecutorMeta::new(Schema::new(vec![]), 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            Box::new(dual),
            NoColumns,
        ))
    } else {
        Box::new(dual)
    };

    // Projection of the rewritten fields.
    let mut root: Box<dyn Executor> = Box::new(ProjectionExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        exprs,
        source,
        NoColumns,
    ));

    // ORDER BY: a sort above the projection. The by-item expressions are
    // rewritten like fields; column references (ordering by a select alias or
    // output column) wait on schema resolution.
    if !select.order_by.is_empty() {
        let mut by_items = Vec::with_capacity(select.order_by.len());
        for item in &select.order_by {
            let expr =
                rewrite_expr(&item.expr).map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            by_items.push(SortByItem {
                expr,
                desc: item.desc,
            });
        }
        root = Box::new(SortExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            by_items,
            root,
            NoColumns,
        ));
    }

    // LIMIT [offset,] count: both bounds must be non-negative integer literals
    // (as in SQL; Go validates the same in the planner).
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(out_schema, 4, INIT_CAP, MAX_CHUNK_SIZE),
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
    Ok(rows)
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
    fn rejects_from_clause() {
        assert!(matches!(
            run_select("SELECT a FROM t"),
            Err(DriverError::Unsupported(_)) | Err(DriverError::Parse(_))
        ));
    }
}
