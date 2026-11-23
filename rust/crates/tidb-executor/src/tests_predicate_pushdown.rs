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

//! The tests of `tidb_executor_internal_exec::predicate_pushdown` that drive
//! the executor, and the condition builders the scan tests share.

/// Builders of the retained physical conditions the scan tests offer.
pub(crate) mod fixtures {
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::expression::{Expression, ScalarFunction};

    /// A scan-output column at `offset`.
    pub(crate) fn column(offset: usize, field_type: FieldType) -> Expression {
        column_with_id(offset, offset as i64 + 1, field_type)
    }

    /// A scan-output column at `offset` with the given plan column id, which
    /// survives a remap of the offset.
    pub(crate) fn column_with_id(
        offset: usize,
        unique_id: i64,
        field_type: FieldType,
    ) -> Expression {
        let mut column = tidb_expr::column::Column::new(unique_id, field_type);
        column.index = offset as i64;
        Expression::Column(column)
    }

    /// A strict literal.
    pub(crate) fn literal(value: Datum, field_type: FieldType) -> Expression {
        Expression::Constant(tidb_expr::constant::Constant::new(value, field_type))
    }

    /// A call of the builtin `name`.
    pub(crate) fn call(name: &str, args: Vec<Expression>) -> Expression {
        tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            name,
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ).unwrap()
    }

    /// `column OP literal` (or `literal OP column`), `name` one of `eq`,
    /// `ne`, `lt`, `le`, `gt`, `ge`.
    pub(crate) fn compare(
        offset: usize,
        column_type: FieldType,
        name: &str,
        value: Datum,
        literal_type: FieldType,
        column_on_left: bool,
    ) -> Expression {
        let column = column(offset, column_type);
        let literal = literal(value, literal_type);
        call(
            name,
            if column_on_left {
                vec![column, literal]
            } else {
                vec![literal, column]
            },
        )
    }

    /// `column IS NULL`, or `column IS NOT NULL` when `negated`.
    pub(crate) fn is_null(offset: usize, field_type: FieldType, negated: bool) -> Expression {
        let test = call("isnull", vec![column(offset, field_type)]);
        if negated {
            call("not", vec![test])
        } else {
            test
        }
    }

    /// The scan-output offsets `expression` reads, in traversal order.
    pub(crate) fn column_offsets(expression: &Expression) -> Vec<usize> {
        tidb_expr::simple_expr::extract_columns(expression)
            .into_iter()
            .map(|column| usize::try_from(column.index).expect("a scan offset"))
            .collect()
    }
}

mod tests {
    use std::sync::Arc;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    #[test]
    fn a_later_selection_offer_keeps_the_pushdown_blacklist() {
        use tidb_expr::expression::Expression;
        let gt = |column_id: i64, index: i64, value: i64| {
            let mut column = tidb_expr::column::Column::new(column_id, long());
            column.index = index;
            tidb_expr::new_function_base(
                &tidb_exprctx::NoColumns,
                "gt",
                long(),
                vec![
                    Expression::Column(column),
                    Expression::Constant(tidb_expr::constant::Constant::new(
                        Datum::Int(value),
                        long(),
                    )),
                ],
            ).unwrap()
        };
        let mut blacklist = tidb_expr::infer_pushdown::ExprPushDownBlacklist::new();
        blacklist.insert(
            "gt".to_owned(),
            tidb_expr::infer_pushdown::blacklist_store_mask("tikv"),
        );
        let blocked = crate::StmtContext::for_query()
            .with_pushdown_blacklists((Arc::new(blacklist), Arc::default()));
        let open = crate::StmtContext::for_query();
        let mut first = tidb_executor_internal_exec::predicate_pushdown::PushedScanFilter::from_physical_conditions(vec![gt(1, 0, 5)], &open);
        assert!(first.fully_pushable());
        let later = tidb_executor_internal_exec::predicate_pushdown::PushedScanFilter::from_physical_conditions(vec![gt(2, 1, 6)], &blocked);
        assert!(!later.fully_pushable(), "the blacklist refuses `gt`");
        first.conjoin(&later);
        assert_eq!(first.filters().len(), 2, "both conditions still filter");
        assert_eq!(
            first.conditions().len(),
            2,
            "the request builder is offered every condition, never a subset"
        );
        assert!(
            !first.fully_pushable(),
            "one refused condition keeps the whole Selection local"
        );
    }

    /// The planner splits a Selection per conjunct (Go `PushDownExprs`); the
    /// request builder then converts the planner's pushed list whole. The two
    /// must agree: a condition the planner admits for TiKV is one `ExprToPB`
    /// converts, or the executor would silently keep a Selection the plan says
    /// the coprocessor evaluates.
    #[test]
    fn the_converter_expresses_every_condition_the_planner_admits() {
        use tidb_datatype::{FieldType, FieldTypeCode};
        let int = FieldType::new(FieldTypeCode::LongLong);
        let text = FieldType::new(FieldTypeCode::Varchar).with_flen(10);
        let columns = vec![
            ("a".to_owned(), int.clone()),
            ("b".to_owned(), int.clone()),
            ("c".to_owned(), int.clone()),
            ("s".to_owned(), text),
            (
                "u".to_owned(),
                FieldType::new(FieldTypeCode::LongLong)
                    .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED),
            ),
            (
                "d".to_owned(),
                FieldType::new(FieldTypeCode::Datetime).with_decimal(0),
            ),
        ];
        let context = crate::StmtContext::for_query();
        let resolver = crate::driver::TableResolver {
            database: Some("test"),
            table_name: "t",
            columns: &columns,
            constant_context: context.clone(),
            zone: context.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            clause_message: "expression",
            subquery_constants: &[],
        };
        const PREDICATES: &[&str] = &[
            "a > 5",
            "5 < a",
            "a = b",
            "a <> c",
            "a IS NULL",
            "a IS NOT NULL",
            "a IN (1, 2, 3)",
            "a NOT IN (1, 2)",
            "s = 'x'",
            "s IN ('x', 'y')",
            "s LIKE 'x%'",
            "s NOT LIKE '%y'",
            "a BETWEEN 1 AND 9",
            "a > 1 AND b < 3",
            "a = 1 OR b = 2",
            "NOT (a > 1)",
            "a + b > 3",
            "b * 2 = c",
            "a - 1 = 0",
            "ABS(a - b) = 1",
            "COALESCE(b, 0) > 0",
            "IF(b > 0, a, c) > 2",
            "CASE WHEN b > 0 THEN a > 1 ELSE FALSE END",
            "a IN (b, 1)",
            "b IS TRUE",
            "b IS NOT TRUE",
            "u > 2",
            "u >= 9223372036854775808",
            "d > '2024-01-01'",
            "a = '5'",
            "a > 1.5",
            "s > 'u' AND a > 1",
            "UPPER(s) = 'X'",
            "LENGTH(s) > 2",
            "SUBSTRING(s, 1, 2) = 'ab'",
            "a % 3 = 0",
            "a DIV 2 = 1",
            "IFNULL(a, 0) = 0",
            "LEAST(a, b) > 1",
            "a IN (1, 2) AND s LIKE 'a%'",
        ];
        let mut refused = Vec::new();
        for sql in PREDICATES {
            let select = tidb_parser::parse(&format!("SELECT {sql}")).expect("parses");
            let tidb_ast::Stmt::Query(query) = select else {
                panic!("not a query");
            };
            let tidb_ast::QueryStmt::Select(select) = &*query else {
                panic!("not a select");
            };
            let Some(tidb_ast::SelectField::Expr { expr, .. }) = select.fields.fields().first()
            else {
                panic!("no field");
            };
            let Ok(mut condition) = tidb_expr::column_resolver::rewrite_expr_resolved(expr, &resolver)
            else {
                continue;
            };
            tidb_expr::scalar_function::into_coprocessor_form(&mut condition);
            if !tidb_expr::infer_pushdown::can_exprs_push_down_tikv(std::slice::from_ref(&condition)) {
                continue;
            }
            if tidb_executor_internal_exec::predicate_pushdown::condition_to_pb(&condition, Some(&context), &tidb_datatype::SessionTimeZone::utc()).is_none() {
                refused.push(*sql);
            }
        }
        // The planner's admission is Go's name whitelist; the catalog holds
        // the signatures converted so far. These are the admitted shapes it
        // does not hold yet: their Selection is evaluated locally, whole (never
        // a subset), while EXPLAIN keeps Go's cop Selection. The list may only
        // shrink -- a new entry is a planner/converter disagreement.
        const NOT_CONVERTED_YET: &[&str] = &[];
        assert_eq!(
            refused, NOT_CONVERTED_YET,
            "the planner admits for TiKV what ExprToPB refuses"
        );
    }

    /// Go `scalarExprSupportedByTiKV`: `round` travels to TiKV only as the
    /// one-argument `RoundInt`/`RoundReal`/`RoundDec` ("We don't push round
    /// with frac"), though `ExprToPB` itself expresses `RoundWithFrac*`, for
    /// TiFlash. The request builder asks TiKV's list, with a statement context
    /// and without one.
    #[test]
    fn the_request_carries_only_what_tikv_evaluates() {
        use tidb_datatype::{FieldType, FieldTypeCode};
        let columns = vec![("a".to_owned(), FieldType::new(FieldTypeCode::LongLong))];
        let context = crate::StmtContext::for_query();
        let resolver = crate::driver::TableResolver {
            database: Some("test"),
            table_name: "t",
            columns: &columns,
            constant_context: context.clone(),
            zone: context.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            clause_message: "expression",
            subquery_constants: &[],
        };
        for (sql, travels) in [("ROUND(a) = 1", true), ("ROUND(a, 1) = 1", false)] {
            let select = tidb_parser::parse(&format!("SELECT {sql}")).expect("parses");
            let tidb_ast::Stmt::Query(query) = select else {
                panic!("not a query");
            };
            let tidb_ast::QueryStmt::Select(select) = &*query else {
                panic!("not a select");
            };
            let Some(tidb_ast::SelectField::Expr { expr, .. }) = select.fields.fields().first()
            else {
                panic!("no field");
            };
            let mut condition =
                tidb_expr::column_resolver::rewrite_expr_resolved(expr, &resolver).expect("rewrites");
            tidb_expr::scalar_function::into_coprocessor_form(&mut condition);
            let zone = tidb_datatype::SessionTimeZone::utc();
            assert!(
                tidb_expr::expr_to_pb::PbConverter::new(&tidb_exprctx::NoColumns, false, &zone)
                    .expr_to_pb(&condition)
                    .is_some(),
                "ExprToPB expresses {sql}"
            );
            use tidb_executor_internal_exec::predicate_pushdown::condition_to_pb;
            assert_eq!(
                condition_to_pb(&condition, None, &zone).is_some(),
                travels,
                "{sql} without a statement"
            );
            assert_eq!(
                condition_to_pb(&condition, Some(&context), &zone).is_some(),
                travels,
                "{sql}"
            );
        }
    }

}
