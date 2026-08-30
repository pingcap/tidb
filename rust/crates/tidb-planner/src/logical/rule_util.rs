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
    expression: &Expression,
    replace: &BTreeMap<Vec<u8>, Column>,
) -> Expression {
    resolve_expr_and_replace_inner(expression, replace).0
}

fn resolve_expr_and_replace_inner(
    expression: &Expression,
    replace: &BTreeMap<Vec<u8>, Column>,
) -> (Expression, bool) {
    fn replacement(origin: &Column, replace: &BTreeMap<Vec<u8>, Column>) -> Option<Column> {
        let mut hashed_origin = origin.clone();
        replace.get(hashed_origin.hash_code()).map(|destination| {
            let mut destination = destination.clone();
            destination.ret_type = origin.ret_type.clone();
            destination.in_operand = origin.in_operand;
            destination
        })
    }

    match expression {
        Expression::Column(column) => replacement(column, replace).map_or_else(
            || (expression.clone(), false),
            |column| (Expression::Column(column), true),
        ),
        Expression::CorrelatedColumn(correlated) => {
            let Some(column) = replacement(&correlated.column, replace) else {
                return (expression.clone(), false);
            };
            let mut correlated = correlated.clone();
            correlated.column = column;
            (Expression::CorrelatedColumn(correlated), true)
        }
        Expression::ScalarFunction(function) => {
            let mut cloned = None;
            for (index, argument) in function.args.iter().enumerate() {
                let (argument, changed) = resolve_expr_and_replace_inner(argument, replace);
                if changed {
                    cloned.get_or_insert_with(|| function.clone()).args[index] = argument;
                }
            }
            cloned.map_or_else(
                || (expression.clone(), false),
                |mut function| {
                    function.invalidate_cached_arguments();
                    (Expression::ScalarFunction(function), true)
                },
            )
        }
        _ => (expression.clone(), false),
    }
}

/// Go `ReplaceColumnOfExpr`.
#[must_use]
pub fn replace_column_of_expr(
    expression: &Expression,
    expressions: &[Expression],
    schema: &Schema,
) -> Expression {
    replace_column_of_expr_inner(expression, expressions, schema).0
}

fn replace_column_of_expr_inner(
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
            let mut cloned = None;
            for (index, argument) in function.args.iter().enumerate() {
                let (argument, changed) =
                    replace_column_of_expr_inner(argument, expressions, schema);
                if changed {
                    cloned.get_or_insert_with(|| function.clone()).args[index] = argument;
                }
            }
            cloned.map_or_else(
                || (expression.clone(), false),
                |mut function| {
                    function.invalidate_cached_arguments();
                    (Expression::ScalarFunction(function), true)
                },
            )
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
        let index_column_name = tidb_ast::CiString::new(&index_column.name);
        let mut found = false;
        for (position, source_column) in columns.iter().enumerate() {
            if tidb_ast::CiString::new(&source_column.name) != index_column_name {
                continue;
            }
            let column = schema.columns[position].clone();
            unique_key.push(column.clone());
            found = true;
            if strong {
                if !source_column.is_not_null {
                    strong = false;
                    break;
                }
                strong_key.push(column);
                break;
            }
        }
        if !found {
            return (None, None);
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
