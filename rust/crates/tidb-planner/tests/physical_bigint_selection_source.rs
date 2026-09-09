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

#![allow(missing_docs)]

use tidb_planner::{
    read_only_scan::{ConfiguredColumn, ConfiguredTable, ReadOnlyScanPlan},
    signed_bigint_ranger::{
        BigIntComparison, BigIntComparisonError, ComparisonOp, ComparisonOperand,
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
        ],
    )
}

#[test]
fn bounded_comparison_preserves_operand_order_and_rejects_invalid_pairs() {
    let column_left = BigIntComparison::new(
        ComparisonOp::Lt,
        ComparisonOperand::InputOffset(2),
        ComparisonOperand::Int(-7),
    )
    .unwrap();
    let literal_left = BigIntComparison::new(
        ComparisonOp::Le,
        ComparisonOperand::Int(77),
        ComparisonOperand::InputOffset(1),
    )
    .unwrap();

    assert_eq!(column_left.lhs(), ComparisonOperand::InputOffset(2));
    assert_eq!(column_left.rhs(), ComparisonOperand::Int(-7));
    assert_eq!(literal_left.lhs(), ComparisonOperand::Int(77));
    assert_eq!(literal_left.rhs(), ComparisonOperand::InputOffset(1));
    assert_eq!(column_left.input_offset(), 2);
    assert_eq!(literal_left.input_offset(), 1);
    assert_eq!(
        BigIntComparison::new(
            ComparisonOp::Eq,
            ComparisonOperand::InputOffset(0),
            ComparisonOperand::InputOffset(1),
        ),
        Err(BigIntComparisonError::InvalidOperands)
    );
}

#[test]
fn real_physical_selection_is_the_only_stored_condition_authority() {
    let plan = ReadOnlyScanPlan::lower(
        "SELECT id FROM accounts WHERE balance > 10 AND 20 < balance",
        &table(),
    )
    .unwrap();
    let selection: &tidb_planner::physical::PhysicalSelection = plan.selection().unwrap();
    assert_eq!(selection.base.base.tp(), "Selection");
    assert!(selection.from_data_source);

    let comparisons = selection
        .conditions
        .iter()
        .map(BigIntComparison::from_expression)
        .collect::<Option<Vec<_>>>()
        .unwrap();
    assert_eq!(comparisons.len(), 2);
    assert_eq!(comparisons[0].op(), ComparisonOp::Gt);
    assert_eq!(comparisons[0].lhs(), ComparisonOperand::InputOffset(1));
    assert_eq!(comparisons[0].rhs(), ComparisonOperand::Int(10));
    assert_eq!(comparisons[1].op(), ComparisonOp::Lt);
    assert_eq!(comparisons[1].lhs(), ComparisonOperand::Int(20));
    assert_eq!(comparisons[1].rhs(), ComparisonOperand::InputOffset(1));
}
