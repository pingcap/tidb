// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/planner/core/constraint/exprs.go`.

use tidb_datatype::FieldTypeFlags;
use tidb_expr::expr_util::predicates::maybe_over_optimized_4_plan_cache;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

/// Go `DeleteTrueExprs`: remove constants that successfully convert to SQL
/// true, except parameter/deferred constants whose removal would over-optimize
/// a reusable plan.
#[must_use]
pub fn delete_true_exprs(use_plan_cache: bool, conditions: Vec<Expression>) -> Vec<Expression> {
    conditions
        .into_iter()
        .filter(|condition| {
            let Expression::Constant(constant) = condition else {
                return true;
            };
            if maybe_over_optimized_4_plan_cache(use_plan_cache, std::slice::from_ref(condition)) {
                return true;
            }
            constant
                .value
                .to_bool()
                .map_or(true, |value| value.value != 1)
        })
        .collect()
}

/// Go `DeleteTrueExprsBySchema`: remove exactly
/// `NOT(ISNULL(not-null-column))` when `schema` resolves that column and its
/// own field type carries the NOT NULL flag.
#[must_use]
pub fn delete_true_exprs_by_schema(
    schema: &Schema,
    conditions: Vec<Expression>,
) -> Vec<Expression> {
    conditions
        .into_iter()
        .filter(|condition| !is_not_null_column_proof(schema, condition))
        .collect()
}

fn is_not_null_column_proof(schema: &Schema, expression: &Expression) -> bool {
    let Expression::ScalarFunction(not) = expression else {
        return false;
    };
    let [Expression::ScalarFunction(is_null)] = not.get_args() else {
        return false;
    };
    if not.func_name.lowercase() != "not" || is_null.func_name.lowercase() != "isnull" {
        return false;
    }
    let [Expression::Column(column)] = is_null.get_args() else {
        return false;
    };
    schema
        .retrieve_column(column)
        .and_then(tidb_expr::column::Column::get_static_type)
        .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL))
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::scalar_function::ScalarFunction;

    use super::*;

    fn integer_type(not_null: bool) -> FieldType {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        if not_null {
            field_type.set_flags(FieldTypeFlags::NOT_NULL);
        }
        field_type
    }

    fn call(name: &str, arguments: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::Tiny),
            arguments,
        ))
    }

    fn not_is_null(column: Column) -> Expression {
        call(
            "not",
            vec![call("isnull", vec![Expression::Column(column)])],
        )
    }

    #[test]
    fn delete_true_exprs_matches_conversion_and_plan_cache_guards() {
        let field_type = integer_type(false);
        let plain_true = Expression::Constant(Constant::new(Datum::Int(2), field_type.clone()));
        let false_value = Expression::Constant(Constant::new(Datum::Int(0), field_type.clone()));
        let conversion_error = Expression::Constant(Constant::new(
            Datum::Bytes(b"not a boolean".to_vec()),
            field_type.clone(),
        ));
        let real_condition = Expression::Column(Column::new(1, field_type.clone()));
        let mut parameter = Constant::new(Datum::Int(1), field_type);
        parameter.param_marker = Some(ParamMarker { order: 0 });

        let result = delete_true_exprs(
            true,
            vec![
                plain_true,
                false_value.clone(),
                conversion_error.clone(),
                real_condition.clone(),
                Expression::Constant(parameter),
            ],
        );
        assert_eq!(result.len(), 4);
        assert!(matches!(result[0], Expression::Constant(_)));
        assert!(matches!(result[1], Expression::Constant(_)));
        assert!(matches!(result[2], Expression::Column(_)));
        assert!(matches!(result[3], Expression::Constant(_)));
    }

    #[test]
    fn delete_true_exprs_by_schema_requires_the_exact_go_shape() {
        let not_null = Column::new(1, integer_type(true));
        let nullable = Column::new(2, integer_type(false));
        let missing = Column::new(3, integer_type(true));
        let schema = Schema::new(vec![not_null.clone(), nullable.clone()]);
        let malformed = call("not", vec![Expression::Column(not_null.clone())]);

        let result = delete_true_exprs_by_schema(
            &schema,
            vec![
                not_is_null(not_null),
                not_is_null(nullable),
                not_is_null(missing),
                malformed,
            ],
        );
        assert_eq!(result.len(), 3);
    }
}
