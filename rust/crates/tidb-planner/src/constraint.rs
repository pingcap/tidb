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

use tidb_datatype::{Datum, FieldTypeFlags};
use tidb_expr::expr_util::predicates::maybe_over_optimized_4_plan_cache;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

/// Go `DeleteTrueExprs`: remove constants that successfully convert to SQL
/// true, except parameter/deferred constants whose removal would over-optimize
/// a reusable plan.
pub fn delete_true_exprs(
    use_plan_cache: bool,
    context: &dyn Columns,
    conditions: Vec<Expression>,
) -> Vec<Expression> {
    conditions
        .into_iter()
        .filter(|condition| {
            let Expression::Constant(constant) = condition else {
                return true;
            };
            if maybe_over_optimized_4_plan_cache(use_plan_cache, std::slice::from_ref(condition)) {
                return true;
            }
            constant_to_bool(context, &constant.value) != Some(1)
        })
        .collect()
}

/// Shared planner equivalent of Datum.ToBool with the statement TypeContext.
/// A rejected conversion must not become proof that a predicate is true/false.
pub(crate) fn constant_to_bool(context: &dyn Columns, value: &Datum) -> Option<i64> {
    let converted = value.to_bool().ok()?;
    if converted.event.is_some() {
        let message = match value {
            Datum::String(value) => format!(
                "Truncated incorrect DOUBLE value: '{}'",
                tidb_datatype::float_warning_input(
                    value.as_utf8().expect("ToBool validated the string")
                ),
            ),
            Datum::Bytes(value) => format!(
                "Truncated incorrect DOUBLE value: '{}'",
                tidb_datatype::float_warning_input(
                    std::str::from_utf8(value).expect("ToBool validated the bytes")
                ),
            ),
            Datum::BinaryLiteral(value) | Datum::Bit(value) => format!(
                "Truncated incorrect BINARY value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&value.to_string()),
            ),
            _ => return None,
        };
        context.handle_truncate(&message).ok()?;
    }
    Some(converted.value)
}

/// Go `DeleteTrueExprsBySchema`: remove exactly
/// `NOT(ISNULL(not-null-column))` when `schema` resolves that column and its
/// own field type carries the NOT NULL flag.
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
            &tidb_expr::NoColumns,
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

    struct WarningContext {
        level: tidb_expr::ErrorLevel,
        warnings: std::cell::RefCell<Vec<(u16, String)>>,
    }

    impl WarningContext {
        fn new(level: tidb_expr::ErrorLevel) -> Self {
            Self {
                level,
                warnings: Default::default(),
            }
        }
    }

    impl Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> tidb_expr::ErrorLevel {
            self.level
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    #[test]
    fn conversion_uses_statement_warning_and_ignore_policy() {
        for level in [
            tidb_expr::ErrorLevel::Error,
            tidb_expr::ErrorLevel::Warn,
            tidb_expr::ErrorLevel::Ignore,
        ] {
            let context = WarningContext::new(level);
            let input = [
                Datum::new_string(" 1garbage "),
                Datum::Bytes(b"0garbage".to_vec()),
                Datum::new_string("2"),
            ]
            .into_iter()
            .map(|value| {
                Expression::Constant(Constant::new(
                    value,
                    FieldType::new(FieldTypeCode::VarString),
                ))
            })
            .collect();
            let result = delete_true_exprs(false, &context, input);
            assert_eq!(
                result.len(),
                if level == tidb_expr::ErrorLevel::Error {
                    2
                } else {
                    1
                }
            );
            let warnings = context.warnings.borrow();
            if level == tidb_expr::ErrorLevel::Warn {
                assert_eq!(
                    *warnings,
                    vec![
                        (
                            1292,
                            "Truncated incorrect DOUBLE value: '1garbage'".to_owned()
                        ),
                        (
                            1292,
                            "Truncated incorrect DOUBLE value: '0garbage'".to_owned()
                        )
                    ]
                );
            } else {
                assert!(warnings.is_empty());
            }
        }
    }

    #[test]
    fn binary_truncation_and_plan_cache_guard_use_the_same_context() {
        let context = WarningContext::new(tidb_expr::ErrorLevel::Warn);
        let literal = tidb_datatype::BinaryLiteral::from(vec![1; 9]);
        let expected = format!("Truncated incorrect BINARY value: '{literal}'");
        let constant = Constant::new(Datum::BinaryLiteral(literal), integer_type(false));
        assert!(delete_true_exprs(
            false,
            &context,
            vec![Expression::Constant(constant.clone())]
        )
        .is_empty());
        assert_eq!(*context.warnings.borrow(), vec![(1292, expected)]);
        context.warnings.borrow_mut().clear();
        let mut parameter = constant;
        parameter.param_marker = Some(ParamMarker { order: 0 });
        assert_eq!(
            delete_true_exprs(true, &context, vec![Expression::Constant(parameter)]).len(),
            1
        );
        assert!(context.warnings.borrow().is_empty());
    }

    #[test]
    fn strict_conversion_does_not_delete_truncated_true_constant() {
        let condition = Expression::Constant(Constant::new(
            Datum::new_string("1garbage"),
            FieldType::new(FieldTypeCode::VarString),
        ));
        let context = WarningContext::new(tidb_expr::ErrorLevel::Error);
        assert_eq!(delete_true_exprs(false, &context, vec![condition]).len(), 1);
        assert!(context.warnings.borrow().is_empty());
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

    #[test]
    #[deny(unused_must_use)]
    fn source_return_values_may_be_ignored_like_go() {
        delete_true_exprs(true, &tidb_expr::NoColumns, vec![]);
        delete_true_exprs_by_schema(&Schema::new(vec![]), vec![]);
    }
}
