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

//! Pinned Go package `pkg/planner/core/rule/util` (`misc.go`).

use std::collections::{BTreeMap, BTreeSet};

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use super::data_source::DataSourceColumn;
use crate::plan_builder::catalog::SourceIndex;

/// Go `ResolveExprAndReplace` and `ResolveColumnAndReplace`.
#[must_use]
pub fn resolve_expr_and_replace(
    expression: Expression,
    replace: &BTreeMap<Vec<u8>, Column>,
) -> Expression {
    fn replacement(mut origin: Column, replace: &BTreeMap<Vec<u8>, Column>) -> Column {
        let Some(destination) = replace.get(origin.hash_code()) else {
            return origin;
        };
        let mut destination = destination.clone();
        destination.ret_type = origin.ret_type.take();
        destination.in_operand = origin.in_operand;
        destination
    }

    match expression {
        Expression::Column(column) => Expression::Column(replacement(column, replace)),
        Expression::CorrelatedColumn(mut correlated) => {
            correlated.column = replacement(correlated.column, replace);
            Expression::CorrelatedColumn(correlated)
        }
        Expression::ScalarFunction(mut function) => {
            function.args = function
                .args
                .into_iter()
                .map(|argument| resolve_expr_and_replace(argument, replace))
                .collect();
            function.invalidate_cached_arguments();
            Expression::ScalarFunction(function)
        }
        other => other,
    }
}

/// Go `ReplaceColumnOfExpr`.
#[must_use]
pub fn replace_column_of_expr(
    expression: &Expression,
    expressions: &[Expression],
    schema: &Schema,
) -> (Expression, bool) {
    match expression {
        Expression::Column(column) => usize::try_from(schema.column_index(column))
            .ok()
            .and_then(|index| expressions.get(index))
            .map_or_else(
                || (Expression::Column(column.clone()), false),
                |replacement| (replacement.clone(), true),
            ),
        Expression::ScalarFunction(function) => {
            let mut function = function.clone();
            let mut changed = false;
            function.args = function
                .args
                .into_iter()
                .map(|argument| {
                    let (argument, argument_changed) =
                        replace_column_of_expr(&argument, expressions, schema);
                    changed |= argument_changed;
                    argument
                })
                .collect();
            if changed {
                function.invalidate_cached_arguments();
            }
            (Expression::ScalarFunction(function), changed)
        }
        other => (other.clone(), false),
    }
}

/// Go `IsColsAllFromOuterTable`.
#[must_use]
pub fn are_all_columns_from_outer(columns: &[Column], outer_ids: &BTreeSet<i64>) -> bool {
    !columns.is_empty()
        && columns
            .iter()
            .all(|column| outer_ids.contains(&column.unique_id))
}

/// Go `IsColFromInnerTable`.
#[must_use]
pub fn has_column_from_inner(columns: &[Column], inner_ids: &BTreeSet<i64>) -> bool {
    columns
        .iter()
        .any(|column| inner_ids.contains(&column.unique_id))
}

/// Go `CheckMaxOneRowCond`, including nullable unique keys.
#[must_use]
pub fn check_max_one_row_cond(equal_column_ids: &BTreeSet<i64>, child: &Schema) -> bool {
    !equal_column_ids.is_empty()
        && child.pk_or_uk.iter().chain(&child.nullable_uk).any(|key| {
            key.iter()
                .all(|column| equal_column_ids.contains(&column.unique_id))
        })
}

/// Go `CheckIndexCanBeKey` for the planner's source metadata.
///
/// The first result is Go's nullable `uniqueKey`; the second is its strong
/// all-NOT-NULL `newKey`.
#[must_use]
pub fn check_index_can_be_key(
    index: &SourceIndex,
    columns: &[DataSourceColumn],
    schema: &Schema,
) -> (Option<Vec<Column>>, Option<Vec<Column>>) {
    if !index.unique {
        return (None, None);
    }
    let mut unique_key = Vec::with_capacity(index.columns.len());
    let mut strong_key = Vec::with_capacity(index.columns.len());
    let mut strong = true;
    for index_column in &index.columns {
        let Some(position) = columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(&index_column.name))
        else {
            return (None, None);
        };
        let Some(column) = schema.columns.get(position).cloned() else {
            return (None, None);
        };
        unique_key.push(column.clone());
        if strong {
            if columns[position].is_not_null {
                strong_key.push(column);
            } else {
                strong = false;
            }
        }
    }
    if strong {
        (None, Some(strong_key))
    } else {
        (Some(unique_key), None)
    }
}

/// Rust's direct-function form of Go's import-cycle hook.
#[must_use]
pub const fn set_predicate_push_down_flag(flag: u64) -> u64 {
    super::rule::set_predicate_push_down_flag(flag)
}

/// Rust's direct-function form of Go's ordinary simplification hook.
#[must_use]
pub fn apply_predicate_simplification(
    context: &super::rule::RuleContext<'_>,
    predicates: Vec<Expression>,
    propagate_constant: bool,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> Vec<Expression> {
    super::rule::apply_predicate_simplification(context, predicates, propagate_constant, valid)
}

/// Rust's direct-function form of Go's join simplification hook.
#[must_use]
pub fn apply_predicate_simplification_for_join(
    context: &super::rule::RuleContext<'_>,
    predicates: Vec<Expression>,
    left_schema: &tidb_expr::schema::Schema,
    right_schema: &tidb_expr::schema::Schema,
    propagate_constant: bool,
) -> Vec<Expression> {
    super::rule::apply_predicate_simplification_for_join(
        context,
        predicates,
        left_schema,
        right_schema,
        propagate_constant,
        None,
    )
}

/// Go `BuildKeyInfoPortal`; the implementation is iterative in Rust.
pub use super::rewrite::build_key_info_portal;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_builder::catalog::SourceIndexColumn;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
    use tidb_expr::scalar_function::ScalarFunction;

    fn column(unique_id: i64, not_null: bool) -> Column {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        if not_null {
            field_type.add_flags(FieldTypeFlags::NOT_NULL);
        }
        let mut column = Column::new(unique_id, field_type);
        column.id = unique_id;
        column
    }

    #[test]
    fn rule_util_resolves_columns_and_nested_expressions() {
        let mut origin = column(1, true);
        origin.in_operand = true;
        let destination = column(9, false);
        let mut replacements = BTreeMap::new();
        replacements.insert(origin.hash_code().to_vec(), destination);
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                Expression::Column(origin.clone()),
                Expression::Column(column(2, false)),
            ],
        ));

        let Expression::ScalarFunction(resolved) =
            resolve_expr_and_replace(expression, &replacements)
        else {
            panic!("the scalar function remains a scalar function")
        };
        let [Expression::Column(replaced), Expression::Column(untouched)] = resolved.get_args()
        else {
            panic!("both arguments remain columns")
        };
        assert_eq!(replaced.unique_id, 9);
        assert_eq!(replaced.ret_type, origin.ret_type);
        assert!(replaced.in_operand);
        assert_eq!(untouched.unique_id, 2);
    }

    #[test]
    fn rule_util_replaces_projection_columns_by_schema_position() {
        let schema = Schema::new(vec![column(1, false), column(2, false)]);
        let expressions = vec![
            Expression::Column(column(10, false)),
            Expression::Column(column(20, false)),
        ];
        let (replaced, changed) =
            replace_column_of_expr(&Expression::Column(column(2, false)), &expressions, &schema);
        assert!(changed);
        assert!(matches!(replaced, Expression::Column(column) if column.unique_id == 20));
        let (_, changed) =
            replace_column_of_expr(&Expression::Column(column(3, false)), &expressions, &schema);
        assert!(!changed);
    }

    #[test]
    fn rule_util_matches_outer_and_inner_column_sets() {
        let columns = vec![column(1, false), column(2, false)];
        assert!(!are_all_columns_from_outer(&[], &BTreeSet::from([1, 2])));
        assert!(are_all_columns_from_outer(
            &columns,
            &BTreeSet::from([1, 2, 3])
        ));
        assert!(!are_all_columns_from_outer(&columns, &BTreeSet::from([1])));
        assert!(has_column_from_inner(&columns, &BTreeSet::from([2])));
        assert!(!has_column_from_inner(&columns, &BTreeSet::from([3])));
    }

    #[test]
    fn rule_util_max_one_row_accepts_nullable_unique_keys() {
        let mut schema = Schema::new(vec![column(1, false), column(2, false)]);
        schema.nullable_uk = vec![vec![column(1, false), column(2, false)]];
        assert!(check_max_one_row_cond(&BTreeSet::from([1, 2]), &schema));
        assert!(!check_max_one_row_cond(&BTreeSet::from([1]), &schema));
        assert!(!check_max_one_row_cond(&BTreeSet::new(), &schema));
    }

    #[test]
    fn rule_util_classifies_strong_nullable_and_incomplete_unique_indexes() {
        let index = SourceIndex {
            unique: true,
            columns: vec![
                SourceIndexColumn {
                    name: "a".to_owned(),
                    ..SourceIndexColumn::default()
                },
                SourceIndexColumn {
                    name: "b".to_owned(),
                    ..SourceIndexColumn::default()
                },
            ],
            ..SourceIndex::default()
        };
        let strong_columns = vec![
            DataSourceColumn {
                name: "a".to_owned(),
                is_not_null: true,
                ..DataSourceColumn::default()
            },
            DataSourceColumn {
                name: "b".to_owned(),
                is_not_null: true,
                ..DataSourceColumn::default()
            },
        ];
        let strong_schema = Schema::new(vec![column(1, true), column(2, true)]);
        let (nullable, strong) = check_index_can_be_key(&index, &strong_columns, &strong_schema);
        assert!(nullable.is_none());
        assert_eq!(strong.expect("a strong key").len(), 2);

        let nullable_columns = vec![
            DataSourceColumn {
                name: "a".to_owned(),
                is_not_null: true,
                ..DataSourceColumn::default()
            },
            DataSourceColumn {
                name: "b".to_owned(),
                is_not_null: false,
                ..DataSourceColumn::default()
            },
        ];
        // Deliberately disagree with the metadata: Go reads ColumnInfo flags,
        // not the expression schema's return types.
        let nullable_schema = Schema::new(vec![column(1, true), column(2, true)]);
        let (nullable, strong) =
            check_index_can_be_key(&index, &nullable_columns, &nullable_schema);
        assert!(strong.is_none());
        assert_eq!(nullable.expect("a nullable key").len(), 2);

        let incomplete = Schema::new(vec![column(1, true)]);
        let (nullable, strong) = check_index_can_be_key(&index, &strong_columns, &incomplete);
        assert!(nullable.is_none());
        assert!(strong.is_none());
    }
}
