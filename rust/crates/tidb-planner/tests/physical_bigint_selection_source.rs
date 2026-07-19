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

use tidb_planner::physical_selection::{
    BigIntComparison, ComparisonOp, ComparisonOperand, PhysicalSelectionError,
    PhysicalSelectionPlan, SelectionPushdownLayout,
};

#[test]
fn physical_selection_owns_all_signed_comparisons_and_exact_input_offsets() {
    // pkg/planner/core/operator/physicalop/physical_utils_test.go:31
    // TestFlattenListPushDownPlan
    let operators = [
        ComparisonOp::Lt,
        ComparisonOp::Le,
        ComparisonOp::Gt,
        ComparisonOp::Ge,
        ComparisonOp::Eq,
        ComparisonOp::Ne,
    ];
    let conditions = operators
        .into_iter()
        .enumerate()
        .map(|(index, op)| {
            let offset = u32::try_from(index % 3).unwrap();
            if index % 2 == 0 {
                BigIntComparison::new(
                    op,
                    ComparisonOperand::InputOffset(offset),
                    ComparisonOperand::Int(index as i64 - 3),
                )
            } else {
                BigIntComparison::new(
                    op,
                    ComparisonOperand::Int(index as i64 - 3),
                    ComparisonOperand::InputOffset(offset),
                )
            }
            .unwrap()
        })
        .collect::<Vec<_>>();

    let plan = PhysicalSelectionPlan::with_conditions(
        "lt(a, -3), le(-2, b), gt(c, -1), ge(0, a), eq(b, 1), ne(2, c)",
        4,
        0,
        conditions,
    )
    .unwrap();

    assert_eq!(plan.conditions().len(), 6);
    assert_eq!(
        plan.conditions()
            .iter()
            .copied()
            .map(BigIntComparison::op)
            .collect::<Vec<_>>(),
        operators
    );
    assert_eq!(
        plan.condition_input_offsets().collect::<Vec<_>>(),
        [0, 1, 2, 0, 1, 2]
    );
    assert_eq!(plan.query_block_offset(), 4);
}

#[test]
fn operand_order_is_preserved_for_expression_lowering() {
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
}

#[test]
fn malformed_or_empty_selection_fails_closed() {
    assert_eq!(
        BigIntComparison::new(
            ComparisonOp::Eq,
            ComparisonOperand::InputOffset(0),
            ComparisonOperand::InputOffset(1),
        ),
        Err(PhysicalSelectionError::InvalidComparisonOperands)
    );
    assert_eq!(
        BigIntComparison::new(
            ComparisonOp::Eq,
            ComparisonOperand::Int(1),
            ComparisonOperand::Int(2),
        ),
        Err(PhysicalSelectionError::InvalidComparisonOperands)
    );
    assert_eq!(
        PhysicalSelectionPlan::with_conditions("", 0, 0, Vec::new()),
        Err(PhysicalSelectionError::EmptyConditions)
    );
}

#[test]
fn tikv_uses_list_pushdown_without_embedded_selection_child() {
    let condition = BigIntComparison::new(
        ComparisonOp::Gt,
        ComparisonOperand::InputOffset(0),
        ComparisonOperand::Int(0),
    )
    .unwrap();
    let plan = PhysicalSelectionPlan::with_conditions("gt(a, 0)", 0, 0, vec![condition]).unwrap();

    assert_eq!(
        plan.pushdown_layout(),
        SelectionPushdownLayout::TiKvExecutorList
    );
    assert!(!plan.tikv_embeds_child());
}

#[test]
fn runtime_constructor_owns_semantics_without_inventing_plan_metadata() {
    let condition = BigIntComparison::new(
        ComparisonOp::Eq,
        ComparisonOperand::InputOffset(1),
        ComparisonOperand::Int(-7),
    )
    .unwrap();
    let plan = PhysicalSelectionPlan::from_bigint_conditions(vec![condition]).unwrap();

    assert_eq!(plan.conditions(), [condition]);
    assert_eq!(plan.condition_explain(), "");
    assert_eq!(plan.query_block_offset(), 0);
    assert_eq!(plan.stream_count(), 0);
}

#[test]
fn metadata_only_init_keeps_existing_explain_contract() {
    let plan = PhysicalSelectionPlan::init("gt(a, 1)", -2, 10);
    assert!(plan.conditions().is_empty());
    assert_eq!(plan.plan_type(), "Selection");
    assert_eq!(plan.query_block_offset(), -2);
    assert_eq!(plan.condition_explain(), "gt(a, 1)");
    assert_eq!(plan.stream_count(), 10);
    assert_eq!(plan.explain_info(), "gt(a, 1), stream_count: 10");
}
