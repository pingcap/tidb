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
fn walk_statement_markers<N: tidb_ast::Visitable>(stmt: &mut N, visit: &mut dyn FnMut(&mut tidb_ast::Expr)) {
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
fn bind_statement_markers<N: tidb_ast::Visitable>(
    stmt: &mut N,
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

/// Bind a key predicate using the same marker-to-literal conversion as the
/// complete statement. Marker positions remain absolute within the prepared
/// statement; the wire executor validates its full parameter count first.
pub(crate) fn bind_prelock_predicate(
    predicate: &tidb_ast::Expr,
    values: &[Datum],
) -> Result<tidb_ast::Expr, DriverError> {
    let mut predicate = predicate.clone();
    bind_statement_markers(&mut predicate, values, &mut 0)?;
    Ok(predicate)
}

/// Counts the markers without changing them.
fn count_statement_markers(stmt: &mut Stmt, counted: &mut usize) {
    walk_statement_markers(stmt, &mut |_| *counted += 1);
}

#[cfg(test)]
mod tests {
    use super::{bind_parameters, parameter_count};
    use tidb_datatype::Datum;

    fn prepared_catalog() -> crate::Catalog {
        let mut catalog = crate::Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE p (id BIGINT PRIMARY KEY, v BIGINT)",
            &mut catalog,
        )
        .unwrap();
        crate::run_insert_on(
            "INSERT INTO p VALUES (1,10),(2,20),(3,30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        catalog
    }

    fn run_prepared_select(sql: &str, values: Vec<Datum>) -> Vec<Vec<Datum>> {
        let catalog = prepared_catalog();
        let context = crate::StmtContext::for_query().with_prepared_params(values.into());
        crate::run_select_on(sql, &catalog, &context).unwrap()
    }

    #[test]
    fn prepared_typed_markers_reach_projection_filter_and_order() {
        let sql = "SELECT IF(?, v + ?, v - ?) FROM p \
                   WHERE id BETWEEN ? AND ? ORDER BY id DESC LIMIT 2";
        for (values, expected) in [
            ([1, 3, 4, 1, 3], [33, 23]),
            ([0, 3, 4, 1, 2], [16, 6]),
        ] {
            assert_eq!(
                run_prepared_select(sql, values.into_iter().map(Datum::Int).collect()),
                expected
                    .into_iter()
                    .map(|v| vec![Datum::Int(v)])
                    .collect::<Vec<_>>(),
            );
        }
    }

    #[test]
    fn prepared_typed_markers_reach_aggregate_output_and_having() {
        let sql = "SELECT SUM(v) + ? AS total FROM p WHERE id >= ? HAVING SUM(v) > ?";
        assert_eq!(
            run_prepared_select(sql, vec![Datum::Int(5), Datum::Int(2), Datum::Int(40)]),
            vec![vec![Datum::Decimal(
                tidb_datatype::Decimal::from_scaled_i128(55, 0),
            )]],
        );
        assert!(run_prepared_select(
            sql,
            vec![Datum::Int(5), Datum::Int(2), Datum::Int(60)],
        )
        .is_empty());
    }

    #[test]
    fn prepared_typed_markers_reach_join_scopes() {
        assert_eq!(
            run_prepared_select(
                "SELECT l.v + ?, r.v - ? FROM p l JOIN p r ON r.id = l.id + ? \
                 WHERE l.id >= ? ORDER BY l.id",
                vec![Datum::Int(3), Datum::Int(4), Datum::Int(1), Datum::Int(1)],
            ),
            vec![
                vec![Datum::Int(13), Datum::Int(16)],
                vec![Datum::Int(23), Datum::Int(26)],
            ],
        );
    }

    #[test]
    fn prepared_typed_markers_reach_derived_scopes() {
        assert_eq!(
            run_prepared_select(
                "SELECT v + ? FROM (SELECT id, v + ? AS v FROM p WHERE id >= ?) d ORDER BY id",
                vec![Datum::Int(3), Datum::Int(4), Datum::Int(2)],
            ),
            vec![vec![Datum::Int(27)], vec![Datum::Int(37)]],
        );
    }

    #[test]
    fn prepared_typed_markers_match_go_result_metadata_on_type_changes() {
        use tidb_datatype::{FieldTypeCode as C, FieldTypeFlags as F};
        let catalog = crate::Catalog::default();
        let sql = "SELECT POW(?,?), IFNULL(?,11), ? + 11, ? AS marker";
        // Live Go PREPARE/EXECUTE oracle, 2026-09-08. These are logical
        // FieldType lengths, before Go column.ConvertColumnInfo maps
        // unspecified BIGINT/DOUBLE lengths to 20/22 and decimal to 31.
        for (param, expected) in [
            (
                Datum::Int(2),
                [
                    (C::Double, 23, -1, F::NOT_NULL | F::BINARY),
                    (C::LongLong, 24, 0, F::NOT_NULL | F::BINARY),
                    (C::LongLong, 20, 0, F::NOT_NULL | F::BINARY),
                    (C::LongLong, -1, 0, F::NOT_NULL | F::BINARY),
                ],
            ),
            (
                Datum::Null,
                [
                    (C::Double, 23, -1, F::NOT_NULL | F::BINARY),
                    (C::LongLong, 2, 0, F::NOT_NULL | F::BINARY),
                    (C::Double, -1, -1, F::BINARY),
                    (C::Null, 0, 0, 0),
                ],
            ),
        ] {
            let ctx = crate::StmtContext::for_query().with_prepared_params(
                vec![
                    Datum::Int(2),
                    Datum::Int(3),
                    param.clone(),
                    param.clone(),
                    param.clone(),
                ]
                .into(),
            );
            let (columns, _) = crate::run_select_meta_on(sql, &catalog, &ctx).unwrap();
            assert_eq!(columns.len(), expected.len());
            for ((name, actual), (code, flen, decimal, flags)) in columns.iter().zip(expected) {
                assert_eq!(
                    (actual.code(), actual.flen(), actual.decimal(), actual.flags()),
                    (code, flen, decimal, flags),
                    "{name}, parameter {param:?}",
                );
                assert_eq!(
                    actual.charset_name(),
                    if code == C::Null { "utf8mb4" } else { "binary" },
                );
                assert_eq!(
                    actual.collation_name(),
                    if code == C::Null { "utf8mb4_bin" } else { "binary" },
                );
            }
        }
    }

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
