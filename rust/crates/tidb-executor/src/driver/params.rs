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
pub fn bind_parameters(
    sql: &str,
    values: &[Datum],
    // The scanner `sql_mode` the statement was PREPARED under: binding
    // re-parses the prepared text, and the restore below then writes it in
    // the parser's canonical form, exactly as Go's stored AST would.
    sql_mode: tidb_parser::SqlMode,
) -> Result<String, DriverError> {
    let stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    bind_statement(stmt, values).map(|stmt| stmt.restore())
}

/// Binds execute-time values into an already parsed prepared statement.
///
/// Go keeps the parsed AST on the prepared statement and changes only its
/// parameter-marker nodes for each execute.  Keeping this seam separate from
/// [`bind_parameters`] lets the wire front end reuse that parse without
/// changing the ordinary text-query path.
pub fn bind_statement(mut stmt: Stmt, values: &[Datum]) -> Result<Stmt, DriverError> {
    let mut bound = 0usize;
    bind_statement_markers(&mut stmt, values, &mut bound)?;
    if bound != values.len() {
        return Err(DriverError::WrongParamCount);
    }
    Ok(stmt)
}

/// Binds execute-time values into a clone of the AST retained by PREPARE.
///
/// Go stores `PlanCacheStmt.PreparedAst` and assigns values to its parameter
/// markers without lexing the SQL text again. Cloning before binding keeps the
/// retained tree immutable across executions and retries while preserving the
/// SQL mode that gave the statement its meaning at PREPARE time.
pub fn bind_prepared_statement(stmt: &Stmt, values: &[Datum]) -> Result<Stmt, DriverError> {
    let mut bound_stmt = stmt.clone();
    let mut bound = 0usize;
    bind_statement_markers(&mut bound_stmt, values, &mut bound)?;
    if bound != values.len() {
        return Err(DriverError::WrongParamCount);
    }
    Ok(bound_stmt)
}

/// The number of `?` markers a statement carries, which `COM_STMT_PREPARE`
/// reports to the client.
pub fn parameter_count(sql: &str, sql_mode: tidb_parser::SqlMode) -> Result<usize, DriverError> {
    let mut stmt = tidb_parser::parse_with_sql_mode(sql, sql_mode)
        .map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let mut counted = 0usize;
    // Counting binds nothing: every marker reports itself and stays put.
    count_statement_markers(&mut stmt, &mut counted);
    Ok(counted)
}

/// Counts markers on an already parsed statement without changing its tree.
#[must_use]
pub fn parsed_parameter_count(stmt: &Stmt) -> usize {
    let mut stmt = stmt.clone();
    let mut counted = 0usize;
    count_statement_markers(&mut stmt, &mut counted);
    counted
}

/// Walks a statement's expressions, applying `visit` to every marker.
fn walk_statement_markers(stmt: &mut Stmt, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
    struct MarkerVisitor<'a> {
        visit: &'a mut dyn FnMut(&mut tidb_ast::Expr),
    }

    impl tidb_ast::Visitor for MarkerVisitor<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr @ tidb_ast::Expr::ParamMarker { .. }) =
                node.downcast_mut::<tidb_ast::Expr>()
            else {
                return false;
            };
            (self.visit)(expr);
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    tidb_ast::Visitable::accept(stmt, &mut MarkerVisitor { visit });
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

#[cfg(test)]
mod tests {
    use super::{bind_parameters, parameter_count};
    use tidb_datatype::Datum;

    #[test]
    fn markers_in_derived_tables_and_join_conditions_are_counted_and_bound() {
        let sql = "SELECT COUNT(*) FROM (SELECT o.o_id FROM orders o \
                   LEFT JOIN order_line ol ON ol.ol_o_id = ? \
                   WHERE o.o_w_id = ?) AS t WHERE t.o_id > 0";
        let mode = tidb_parser::SqlMode::default();

        assert_eq!(parameter_count(sql, mode).unwrap(), 2);
        let bound = bind_parameters(sql, &[Datum::Int(7), Datum::Int(3)], mode).unwrap();
        assert_eq!(parameter_count(&bound, mode).unwrap(), 0);
        assert!(bound.contains("`ol`.`ol_o_id`=7"), "{bound}");
        assert!(bound.contains("`o`.`o_w_id`=3"), "{bound}");
    }

    #[test]
    fn markers_inside_row_in_are_bound_in_order() {
        let sql = "SELECT o_d_id FROM orders WHERE (o_w_id, o_d_id, o_id) IN ((?,?,?),(?,?,?))";
        let mode = tidb_parser::SqlMode::default();
        let bound = bind_parameters(
            sql,
            &[
                Datum::Int(1),
                Datum::Int(2),
                Datum::Int(3),
                Datum::Int(4),
                Datum::Int(5),
                Datum::Int(6),
            ],
            mode,
        )
        .unwrap();
        assert_eq!(parameter_count(&bound, mode).unwrap(), 0);
        assert!(bound.contains("ROW(1,2,3)"), "{bound}");
        assert!(bound.contains("ROW(4,5,6)"), "{bound}");
    }
}
