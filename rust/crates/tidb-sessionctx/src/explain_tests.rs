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

//! The statement context driving Go `expression/explain.go`'s rendering
//! (`tidb_expr::explain`) of prepared parameters and deferred constants.

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::explain::{physical_expression_text_with_columns, ExpressionTextStyle};
    use tidb_expr::expression::Expression;

    #[test]
    fn dynamic_constants_use_go_explain_and_string_semantics() {
        use tidb_datatype::Datum;
        use tidb_expr::constant::{Constant, ParamMarker};
        use tidb_expr::scalar_function::ScalarFunction;
        let ctx = crate::StmtContext::default()
            .with_prepared_params(vec![Datum::new_string("current")].into());
        let mut parameter = Constant::new(
            Datum::new_string("stale"),
            FieldType::new(FieldTypeCode::VarString),
        );
        parameter.param_marker = Some(ParamMarker { order: 0 });
        parameter.subquery_ref_id = 9;
        let integer = |value| {
            Expression::Constant(Constant::new(
                Datum::Int(value),
                FieldType::new(FieldTypeCode::LongLong),
            ))
        };
        let mut deferred = Constant::new(Datum::Int(-1), FieldType::new(FieldTypeCode::LongLong));
        deferred.deferred_expr = Some(Box::new(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![integer(40), integer(2)],
        ))));
        deferred.subquery_ref_id = 7;
        let mut missing = parameter.clone();
        missing.param_marker = Some(ParamMarker { order: 1 });
        let mut overflow = Constant::new(Datum::Int(-1), FieldType::new(FieldTypeCode::LongLong));
        overflow.deferred_expr = Some(Box::new(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![integer(i64::MAX), integer(1)],
        ))));
        overflow.subquery_ref_id = 8;
        let mut converted = Constant::new(
            Datum::new_string("stale"),
            FieldType::new(FieldTypeCode::VarString),
        );
        converted.deferred_expr = Some(Box::new(integer(42)));
        let mut both = parameter.clone();
        both.deferred_expr = overflow.deferred_expr.clone();
        let cases = [
            (
                overflow,
                "not recognized const value",
                "plus(9223372036854775807, 1)",
            ),
            (converted, "\"42\"", "42"),
            (
                both,
                "ScalarQueryCol#9(\"current\")",
                "ScalarQueryCol#9(current)",
            ),
            (
                parameter,
                "ScalarQueryCol#9(\"current\")",
                "ScalarQueryCol#9(current)",
            ),
            (deferred, "ScalarQueryCol#7(42)", "plus(40, 2)"),
            (missing, "not recognized const value", "?"),
        ];
        for (constant, explain, display) in cases {
            let expression = Expression::Constant(constant);
            for (style, expected) in [
                (ExpressionTextStyle::Explain, explain),
                (ExpressionTextStyle::StringWithCtx, display),
            ] {
                assert_eq!(
                    physical_expression_text_with_columns(&ctx, &expression, &[], style).as_deref(),
                    Some(expected)
                );
            }
        }
    }
}
