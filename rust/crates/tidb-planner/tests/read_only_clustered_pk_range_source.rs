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
    physical_selection::{ComparisonOp, ComparisonOperand},
    read_only_scan::{ConfiguredColumn, ConfiguredTable, ReadOnlyScanPlan},
    signed_bigint_ranger::SignedBigIntRange,
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

fn range(start: i64, end: i64) -> SignedBigIntRange {
    SignedBigIntRange::new(start, end).unwrap()
}

#[test]
fn all_six_pk_comparisons_detach_from_selection() {
    let cases = [
        ("id = 0", vec![range(0, 0)]),
        ("id != 0", vec![range(i64::MIN, -1), range(1, i64::MAX)]),
        ("id < 0", vec![range(i64::MIN, -1)]),
        ("id <= 0", vec![range(i64::MIN, 0)]),
        ("id > 0", vec![range(1, i64::MAX)]),
        ("id >= 0", vec![range(0, i64::MAX)]),
    ];

    for (predicate, expected) in cases {
        let plan = ReadOnlyScanPlan::lower(
            &format!("SELECT balance FROM accounts WHERE {predicate}"),
            &table(),
        )
        .unwrap();
        assert_eq!(plan.handle_ranges(), expected, "{predicate}");
        assert!(plan.selection().is_none(), "{predicate}");
        assert!(!plan.is_contradiction(), "{predicate}");
    }
}

#[test]
fn reversed_operands_detach_with_reversed_ordering() {
    let cases = [
        ("7 = id", vec![range(7, 7)]),
        ("7 != id", vec![range(i64::MIN, 6), range(8, i64::MAX)]),
        ("7 < id", vec![range(8, i64::MAX)]),
        ("7 <= id", vec![range(7, i64::MAX)]),
        ("7 > id", vec![range(i64::MIN, 6)]),
        ("7 >= id", vec![range(i64::MIN, 7)]),
    ];

    for (predicate, expected) in cases {
        let plan = ReadOnlyScanPlan::lower(
            &format!("SELECT id FROM accounts WHERE {predicate}"),
            &table(),
        )
        .unwrap();
        assert_eq!(plan.handle_ranges(), expected, "{predicate}");
        assert!(plan.selection().is_none(), "{predicate}");
    }
}

#[test]
fn ordered_and_intersection_and_not_equal_remain_disjoint() {
    let plan = ReadOnlyScanPlan::lower(
        "SELECT id FROM accounts WHERE id >= -7 AND id <= 77 AND id != 0",
        &table(),
    )
    .unwrap();

    assert_eq!(plan.handle_ranges(), [range(-7, -1), range(1, 77)]);
    assert!(plan.selection().is_none());
}

#[test]
fn signed_extremes_eliminate_impossible_halves_without_overflow() {
    let cases = [
        ("id < -9223372036854775808", vec![]),
        (
            "id <= -9223372036854775808",
            vec![range(i64::MIN, i64::MIN)],
        ),
        ("id > 9223372036854775807", vec![]),
        ("id >= 9223372036854775807", vec![range(i64::MAX, i64::MAX)]),
        (
            "id != -9223372036854775808",
            vec![range(i64::MIN + 1, i64::MAX)],
        ),
        (
            "id != 9223372036854775807",
            vec![range(i64::MIN, i64::MAX - 1)],
        ),
    ];

    for (predicate, expected) in cases {
        let plan = ReadOnlyScanPlan::lower(
            &format!("SELECT id FROM accounts WHERE {predicate}"),
            &table(),
        )
        .unwrap();
        assert_eq!(plan.handle_ranges(), expected, "{predicate}");
        assert_eq!(plan.is_contradiction(), expected.is_empty(), "{predicate}");
    }
}

#[test]
fn contradictions_are_an_explicit_zero_range_plan() {
    let plan = ReadOnlyScanPlan::lower("SELECT id FROM accounts WHERE id > 5 AND id < 5", &table())
        .unwrap();

    assert!(plan.handle_ranges().is_empty());
    assert!(plan.is_contradiction());
    assert!(plan.selection().is_none());
}

#[test]
fn stored_predicate_remains_selection_and_projection_offsets_do_not_move() {
    let plan = ReadOnlyScanPlan::lower(
        "SELECT balance AS amount, id FROM accounts \
         WHERE id >= -7 AND version != 0 AND id <= 77",
        &table(),
    )
    .unwrap();

    assert_eq!(plan.handle_ranges(), [range(-7, 77)]);
    assert_eq!(plan.projection_output_offsets(), [0, 1]);
    assert_eq!(plan.projected_columns()[0].output_name(), "amount");
    assert_eq!(
        plan.table_scan()
            .pushdown()
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        [9, 7, 11]
    );
    let selection = plan.selection().unwrap();
    assert_eq!(selection.conditions().len(), 1);
    assert_eq!(selection.conditions()[0].op(), ComparisonOp::Ne);
    assert_eq!(
        selection.conditions()[0].lhs(),
        ComparisonOperand::InputOffset(2)
    );
    assert_eq!(selection.conditions()[0].rhs(), ComparisonOperand::Int(0));
}

#[test]
fn stored_only_predicate_keeps_full_range_and_selection() {
    let plan =
        ReadOnlyScanPlan::lower("SELECT id FROM accounts WHERE balance > 100", &table()).unwrap();

    assert_eq!(plan.handle_ranges(), [SignedBigIntRange::full()]);
    assert_eq!(plan.selection().unwrap().conditions().len(), 1);
    assert!(!plan.is_contradiction());
}

#[test]
fn unfiltered_scan_keeps_full_range_without_selection() {
    let plan = ReadOnlyScanPlan::lower("SELECT id FROM accounts", &table()).unwrap();

    assert_eq!(plan.handle_ranges(), [SignedBigIntRange::full()]);
    assert!(plan.selection().is_none());
    assert!(!plan.is_contradiction());
}
