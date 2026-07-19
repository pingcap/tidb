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

//! Source-derived binding tests for the bounded signed-BIGINT Selection.

use tidb_planner::{
    physical_selection::{ComparisonOp, ComparisonOperand},
    read_only_scan::{
        ConfiguredColumn, ConfiguredTable, ReadOnlyScanError, ReadOnlyScanPlan,
        UnsupportedReadOnlyPredicate,
    },
};

fn table() -> ConfiguredTable {
    ConfiguredTable::new(
        "test",
        "accounts",
        42,
        [
            ConfiguredColumn::clustered_primary_key("id", 7),
            ConfiguredColumn::stored_not_null("balance", 9),
            ConfiguredColumn::stored_not_null("version", 11),
        ],
    )
}

#[test]
fn six_comparisons_preserve_and_order_operand_order_and_signed_extremes() {
    // pkg/expression/expr_to_pb_test.go:222 TestCompareFunc2Pb
    // pkg/planner/core/casetest/dag/dag_test.go:82 TestDAGPlanBuilderSimpleCase
    let plan = ReadOnlyScanPlan::lower(
        "SELECT id FROM accounts \
         WHERE id < -9223372036854775808 \
           AND -5 <= balance \
           AND id > 0 \
           AND balance >= 1 \
           AND id = 2 \
           AND 3 != balance",
        &table(),
    )
    .expect("all six signed comparisons must bind in flattened-AND order");

    assert_eq!(plan.projection_output_offsets(), [0]);
    assert_eq!(
        plan.table_scan()
            .pushdown()
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        [7, 9]
    );
    let conditions = plan.selection().unwrap().conditions();
    assert_eq!(conditions.len(), 6);
    assert_eq!(conditions[0].op(), ComparisonOp::Lt);
    assert_eq!(conditions[0].lhs(), ComparisonOperand::InputOffset(0));
    assert_eq!(conditions[0].rhs(), ComparisonOperand::Int(i64::MIN));
    assert_eq!(conditions[1].op(), ComparisonOp::Le);
    assert_eq!(conditions[1].lhs(), ComparisonOperand::Int(-5));
    assert_eq!(conditions[1].rhs(), ComparisonOperand::InputOffset(1));
    assert_eq!(conditions[2].op(), ComparisonOp::Gt);
    assert_eq!(conditions[3].op(), ComparisonOp::Ge);
    assert_eq!(conditions[4].op(), ComparisonOp::Eq);
    assert_eq!(conditions[5].op(), ComparisonOp::Ne);
    assert_eq!(
        plan.selection()
            .unwrap()
            .condition_input_offsets()
            .collect::<Vec<_>>(),
        [0, 1, 0, 1, 0, 1]
    );
}

#[test]
fn projection_prefix_is_stable_and_predicate_only_columns_are_appended_once() {
    let plan = ReadOnlyScanPlan::lower(
        "SELECT version AS v, id FROM test.accounts AS a \
         WHERE (a.balance > 10) AND ((20 < a.balance)) AND a.id != -1",
        &table(),
    )
    .expect("qualified predicate-only columns must extend the scan once");

    assert_eq!(plan.projection_output_offsets(), [0, 1]);
    assert_eq!(plan.projected_columns()[0].output_name(), "v");
    assert_eq!(
        plan.table_scan()
            .pushdown()
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        [11, 7, 9]
    );
    assert_eq!(
        plan.selection()
            .unwrap()
            .condition_input_offsets()
            .collect::<Vec<_>>(),
        [2, 2, 1]
    );
}

#[test]
fn no_where_keeps_the_existing_scan_without_a_physical_selection() {
    let plan = ReadOnlyScanPlan::lower("SELECT balance, id FROM accounts", &table()).unwrap();
    assert!(plan.selection().is_none());
    assert_eq!(plan.projection_output_offsets(), [0, 1]);
    assert_eq!(plan.table_scan().pushdown().columns.len(), 2);
}

fn unsupported(sql: &str, reason: UnsupportedReadOnlyPredicate) {
    assert_eq!(
        ReadOnlyScanPlan::lower(sql, &table()),
        Err(ReadOnlyScanError::UnsupportedPredicate(reason)),
        "{sql}"
    );
}

#[test]
fn unsupported_predicate_shapes_fail_closed_with_typed_reasons() {
    unsupported(
        "SELECT id FROM accounts WHERE id = 1 OR balance = 2",
        UnsupportedReadOnlyPredicate::BooleanOperator,
    );
    unsupported(
        "SELECT id FROM accounts WHERE id = 1 XOR balance = 2",
        UnsupportedReadOnlyPredicate::BooleanOperator,
    );
    unsupported(
        "SELECT id FROM accounts WHERE id <=> 1",
        UnsupportedReadOnlyPredicate::ComparisonOperator,
    );
    unsupported(
        "SELECT id FROM accounts WHERE balance + 1",
        UnsupportedReadOnlyPredicate::ComparisonOperator,
    );
    unsupported(
        "SELECT id FROM accounts WHERE balance + 1 > 2",
        UnsupportedReadOnlyPredicate::Operand,
    );
    unsupported(
        "SELECT id FROM accounts WHERE balance > 1.5",
        UnsupportedReadOnlyPredicate::Operand,
    );
    unsupported(
        "SELECT id FROM accounts WHERE balance > version",
        UnsupportedReadOnlyPredicate::ColumnIntegerPair,
    );
    unsupported(
        "SELECT id FROM accounts WHERE 1 < 2",
        UnsupportedReadOnlyPredicate::ColumnIntegerPair,
    );
    unsupported(
        "SELECT id FROM accounts WHERE balance > 9223372036854775808",
        UnsupportedReadOnlyPredicate::IntegerOutOfRange,
    );
    unsupported(
        "SELECT id FROM accounts WHERE balance > -9223372036854775809",
        UnsupportedReadOnlyPredicate::IntegerOutOfRange,
    );
}

#[test]
fn unknown_predicate_columns_remain_catalog_errors() {
    assert_eq!(
        ReadOnlyScanPlan::lower("SELECT id FROM accounts WHERE missing = 1", &table()),
        Err(ReadOnlyScanError::UnknownColumn("missing".to_owned()))
    );
    assert_eq!(
        ReadOnlyScanPlan::lower(
            "SELECT id FROM accounts AS a WHERE accounts.id = 1",
            &table()
        ),
        Err(ReadOnlyScanError::UnknownColumn("accounts.id".to_owned()))
    );
}
