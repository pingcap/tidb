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

//! Copy-on-write ports from pinned
//! `pkg/planner/core/operator/logicalop/logicalop_test/logical_operator_test.go`.

use std::collections::BTreeMap;

use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;
use tidb_planner::logical::rule_util::{replace_column_of_expr, resolve_expr_and_replace};

fn column(unique_id: i64) -> Column {
    let mut column = Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    column.index = 0;
    column
}

fn plus(column: &Column) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            Expression::Column(column.clone()),
            Expression::Column(column.clone()),
        ],
    ))
}

fn argument_ids(expression: &Expression) -> [i64; 2] {
    let Expression::ScalarFunction(function) = expression else {
        panic!("the expression remains a scalar function")
    };
    let [Expression::Column(left), Expression::Column(right)] = function.get_args() else {
        panic!("both scalar arguments remain columns")
    };
    [left.unique_id, right.unique_id]
}

/// Go `TestReplaceColumnOfExprCopyOnWrite`.
#[test]
fn rule_util_copy_on_write_replaces_projection_columns() {
    let source = column(1);
    let destination = column(2);
    let original = plus(&source);

    let replaced = replace_column_of_expr(
        &original,
        &[Expression::Column(destination)],
        &Schema::new(vec![source]),
    );

    assert_eq!(argument_ids(&original), [1, 1]);
    assert_eq!(argument_ids(&replaced), [2, 2]);
}

/// Go `TestResolveExprAndReplaceCopyOnWrite`.
#[test]
fn rule_util_copy_on_write_resolves_columns_by_hash() {
    let mut source = column(1);
    source.in_operand = true;
    let destination = column(2);
    let original = plus(&source);
    let mut replacements = BTreeMap::new();
    replacements.insert(source.hash_code().to_vec(), destination);

    let replaced = resolve_expr_and_replace(&original, &replacements);

    assert_eq!(argument_ids(&original), [1, 1]);
    assert_eq!(argument_ids(&replaced), [2, 2]);
    let Expression::ScalarFunction(function) = replaced else {
        unreachable!()
    };
    for argument in function.get_args() {
        let Expression::Column(column) = argument else {
            unreachable!()
        };
        assert_eq!(column.ret_type, source.ret_type);
        assert!(column.in_operand);
    }
}
