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

//! Source-derived tests for configured FullSchema join planning.

use tidb_planner::{
    configured_catalog::ConfiguredCatalog,
    configured_join_plan::{ConfiguredJoinPlan, ConfiguredJoinPlanError},
    configured_relation_tree::RelationSide,
    join_condition::{JoinSide, UnsupportedJoinCondition},
    read_only_scan::{ConfiguredColumn, ConfiguredTable},
};

fn catalog() -> ConfiguredCatalog {
    ConfiguredCatalog::new([
        ConfiguredTable::new(
            "Sales",
            "Accounts",
            101,
            [
                ConfiguredColumn::clustered_primary_key("AccountID", 11),
                ConfiguredColumn::stored_not_null("Balance", 19),
            ],
        ),
        ConfiguredTable::new(
            "Sales",
            "Orders",
            202,
            [
                ConfiguredColumn::clustered_primary_key("OrderID", 7),
                ConfiguredColumn::stored_not_null("AccountID", 23),
                ConfiguredColumn::stored_not_null("Amount", 31),
            ],
        ),
    ])
    .expect("valid configured catalog")
}

#[test]
fn on_equality_uses_stable_fullschema_and_projection_offsets() {
    let plan = ConfiguredJoinPlan::lower(
        "SELECT a.Balance AS balance, o.Amount, a.AccountID \
         FROM Sales.Accounts a JOIN Sales.Orders o ON o.AccountID = a.AccountID",
        &catalog(),
    )
    .expect("direct non-null BIGINT equality must lower");

    assert_eq!(
        plan.full_schema()
            .iter()
            .map(|column| (
                column.side(),
                column.side_offset(),
                column.full_offset(),
                column.table_id(),
                column.column_id(),
                column.qualifier(),
                column.name(),
            ))
            .collect::<Vec<_>>(),
        [
            (RelationSide::Left, 0, 0, 101, 11, "a", "AccountID"),
            (RelationSide::Left, 1, 1, 101, 19, "a", "Balance"),
            (RelationSide::Right, 0, 2, 202, 7, "o", "OrderID"),
            (RelationSide::Right, 1, 3, 202, 23, "o", "AccountID"),
            (RelationSide::Right, 2, 4, 202, 31, "o", "Amount"),
        ]
    );
    assert_eq!(plan.visible_full_offsets(), [0, 1, 2, 3, 4]);
    assert_eq!(
        plan.projections()
            .iter()
            .map(|projection| (projection.output_name(), projection.full_offset()))
            .collect::<Vec<_>>(),
        [("balance", 1), ("Amount", 4), ("AccountID", 0)]
    );
    let equality = plan.equality().expect("ON equality");
    assert_eq!(equality.left().side(), JoinSide::Left);
    assert_eq!(equality.left().side_index(), 0);
    assert_eq!(equality.left().full_index(), 0);
    assert_eq!(equality.right().side(), JoinSide::Right);
    assert_eq!(equality.right().side_index(), 1);
    assert_eq!(equality.right().full_index(), 3);
}

#[test]
fn local_predicates_lower_through_existing_range_and_selection_planner() {
    let plan = ConfiguredJoinPlan::lower(
        "SELECT a.AccountID, o.Amount FROM Accounts a JOIN Orders o \
         ON a.AccountID=o.AccountID WHERE a.AccountID >= 10 AND o.Amount < 100",
        &catalog(),
    )
    .expect("local predicates must retain owning scan");

    assert_eq!(plan.left_scan().handle_ranges().len(), 1);
    assert_eq!(plan.left_scan().handle_ranges()[0].start(), 10);
    assert_eq!(plan.left_scan().handle_ranges()[0].end(), i64::MAX);
    assert!(plan.left_scan().selection().is_none());
    assert_eq!(plan.right_scan().handle_ranges().len(), 1);
    assert_eq!(plan.right_scan().handle_ranges()[0].start(), i64::MIN);
    assert_eq!(plan.right_scan().handle_ranges()[0].end(), i64::MAX);
    assert!(plan.right_scan().selection().is_some());
}

#[test]
fn literal_left_local_predicate_lowers_without_reconstructing_sql() {
    let plan = ConfiguredJoinPlan::lower(
        "SELECT a.AccountID, o.Amount FROM Accounts a JOIN Orders o \
         ON a.AccountID=o.AccountID WHERE 10 <= a.AccountID",
        &catalog(),
    )
    .expect("literal-left predicate must retain its comparison direction");

    assert_eq!(plan.left_scan().handle_ranges().len(), 1);
    assert_eq!(plan.left_scan().handle_ranges()[0].start(), 10);
    assert_eq!(plan.left_scan().handle_ranges()[0].end(), i64::MAX);
    assert!(plan.left_scan().selection().is_none());
}

#[test]
fn three_part_join_columns_validate_each_relation_schema() {
    ConfiguredJoinPlan::lower(
        "SELECT a.Balance, o.Amount FROM Sales.Accounts a JOIN Sales.Orders o \
         ON Sales.a.AccountID = Sales.o.AccountID",
        &catalog(),
    )
    .expect("matching configured schemas must bind");

    assert_eq!(
        ConfiguredJoinPlan::lower(
            "SELECT a.Balance FROM Sales.Accounts a JOIN Sales.Orders o \
             ON Archive.a.AccountID = Sales.o.AccountID",
            &catalog(),
        ),
        Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::UnknownColumn {
                path: vec!["Archive".to_owned(), "a".to_owned(), "AccountID".to_owned(),]
            }
        ))
    );
}

#[test]
fn using_coalesces_visible_metadata_but_retains_both_physical_keys() {
    let plan = ConfiguredJoinPlan::lower(
        "SELECT AccountID, o.OrderID, a.Balance FROM Accounts a JOIN Orders o USING (AccountID)",
        &catalog(),
    )
    .expect("one USING key must coalesce the visible column");

    assert_eq!(plan.full_schema().len(), 5);
    assert_eq!(plan.visible_full_offsets(), [0, 1, 2, 4]);
    assert_eq!(
        plan.projections()
            .iter()
            .map(|projection| projection.full_offset())
            .collect::<Vec<_>>(),
        [0, 2, 1]
    );
    assert_eq!(plan.left_scan().projected_columns().len(), 2);
    assert_eq!(plan.right_scan().projected_columns().len(), 3);
    let equality = plan.equality().expect("USING equality");
    assert_eq!(equality.left().full_index(), 0);
    assert_eq!(equality.right().full_index(), 3);
}

#[test]
fn cross_and_comma_keep_fullschema_without_an_equality() {
    for sql in [
        "SELECT a.Balance, o.Amount FROM Accounts a CROSS JOIN Orders o",
        "SELECT a.Balance, o.Amount FROM Accounts a, Orders o",
    ] {
        let plan = ConfiguredJoinPlan::lower(sql, &catalog()).expect("bounded cross relation");
        assert!(plan.equality().is_none());
        assert_eq!(plan.visible_full_offsets(), [0, 1, 2, 3, 4]);
    }
}

#[test]
fn unsupported_join_key_semantics_fail_closed() {
    assert_eq!(
        ConfiguredJoinPlan::lower(
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID <=> o.AccountID",
            &catalog(),
        ),
        Err(ConfiguredJoinPlanError::NullSafeEquality)
    );
    assert_eq!(
        ConfiguredJoinPlan::lower(
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID = o.AccountID AND a.Balance > 0",
            &catalog(),
        ),
        Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::Other
        ))
    );
    assert_eq!(
        ConfiguredJoinPlan::lower(
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID = a.Balance",
            &catalog(),
        ),
        Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::SameSide {
                side: JoinSide::Left
            }
        ))
    );
}

#[test]
fn using_requires_one_existing_key_on_each_physical_input() {
    assert_eq!(
        ConfiguredJoinPlan::lower(
            "SELECT a.Balance FROM Accounts a JOIN Orders o USING (AccountID, Balance)",
            &catalog(),
        ),
        Err(ConfiguredJoinPlanError::ExactlyOneUsingColumnRequired)
    );
    assert_eq!(
        ConfiguredJoinPlan::lower(
            "SELECT a.Balance FROM Accounts a JOIN Orders o USING (Balance)",
            &catalog(),
        ),
        Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::UnknownColumn {
                path: vec!["Balance".to_owned()]
            }
        ))
    );
}
