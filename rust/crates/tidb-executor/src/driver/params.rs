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

//! Prepared-statement parameter markers: counting the `?` markers a statement
//! carries, and replacing each with the literal for its execute-time value.
//!
//! Go keeps the parsed statement and installs the values on the marker nodes
//! themselves. This tier reaches execution through SQL text, so the markers
//! become literals and the statement is restored -- see [`bind_parameters`]
//! for why that round trip is exact. Both directions share one AST walk
//! ([`walk_statement_markers`]), so a statement shape that can be counted can
//! always be bound.

use super::*;
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
        // `PREPARE ps FROM 'set @z = ?'` is a prepared statement like any
        // other (captured: `EXECUTE ps USING @one` then `SELECT @z` is 1), so
        // its assigned values carry markers too.
        Stmt::Session(session) => {
            if let tidb_ast::SessionStmt::SetUserVar(set) = &mut **session {
                for assignment in &mut set.assignments {
                    walk_expr(&mut assignment.value, visit);
                }
            }
        }
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
        // A subquery is a query, so its own clauses carry markers: captured,
        // `select a from t where a in (select a from t where b = ?)` binds one
        // parameter. Without these arms such a marker is simply not counted,
        // and the count check then rejects the statement -- a silent refusal
        // of a shape TiDB accepts.
        Expr::Subquery(query) => walk_query_markers(query, visit),
        Expr::Exists { subquery, .. } => walk_query_markers(subquery, visit),
        Expr::InSubquery { expr, subquery, .. } => {
            walk_expr_markers(expr, visit);
            walk_query_markers(subquery, visit);
        }
        Expr::CompareSubquery { left, subquery, .. } => {
            walk_expr_markers(left, visit);
            walk_query_markers(subquery, visit);
        }
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
