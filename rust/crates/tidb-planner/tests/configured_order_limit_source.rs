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

//! Source-shaped tests for configured ORDER BY/LIMIT planner lowering.

use tidb_parser::parse;
use tidb_planner::{
    configured_catalog::ConfiguredCatalog,
    configured_join_plan::{ConfiguredJoinPlan, ConfiguredJoinPlanError},
    configured_order_limit::{
        ConfiguredOrderLimit, ConfiguredOrderLimitError, ConfiguredOrderedJoinPlan,
    },
    configured_order_limit_contract::ConfiguredLimitWindowError,
    configured_order_limit_contract::ConfiguredOrderDirection,
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
fn lower_select_binds_direct_alias_and_ordinal_keys_to_fullschema() {
    let statement = parse(
        "SELECT a.Balance AS balance, o.Amount, a.AccountID FROM Accounts a \
         JOIN Orders o ON a.AccountID=o.AccountID ORDER BY balance DESC, 2, o.OrderID LIMIT 3 OFFSET 2",
    )
    .expect("valid configured query");
    let select = match statement {
        tidb_ast::Stmt::Query(query) => match query.into_inner() {
            tidb_ast::QueryStmt::Select(select) => select,
            _ => panic!("plain SELECT"),
        },
        _ => panic!("query"),
    };
    let plan = ConfiguredOrderedJoinPlan::lower_select(&select, &catalog())
        .expect("typed select must not restore or reparse");
    let ConfiguredOrderLimit::TopN(spec) = plan.order_limit().expect("TopN tail") else {
        panic!("ORDER BY plus LIMIT lowers to TopN");
    };
    assert_eq!(spec.limit().offset(), 2);
    assert_eq!(spec.limit().count(), 3);
    assert_eq!(
        spec.order_keys()
            .iter()
            .map(|key| (key.full_offset(), key.direction()))
            .collect::<Vec<_>>(),
        [
            (1, ConfiguredOrderDirection::Descending),
            (4, ConfiguredOrderDirection::Ascending),
            (2, ConfiguredOrderDirection::Ascending),
        ]
    );
    assert!(plan.join().is_some());
}

#[test]
fn using_hidden_right_key_and_unprojected_columns_keep_physical_offsets() {
    let plan = ConfiguredOrderedJoinPlan::lower(
        "SELECT AccountID, a.Balance FROM Accounts a JOIN Orders o USING (AccountID) \
         ORDER BY o.AccountID DESC, o.Amount LIMIT 4",
        &catalog(),
    )
    .expect("ORDER BY may use unprojected and USING-hidden physical columns");
    let ConfiguredOrderLimit::TopN(spec) = plan.order_limit().expect("TopN tail") else {
        panic!("ordered bounded plan");
    };
    assert_eq!(
        spec.order_keys()
            .iter()
            .map(|key| (key.full_offset(), key.direction()))
            .collect::<Vec<_>>(),
        [
            (3, ConfiguredOrderDirection::Descending),
            (4, ConfiguredOrderDirection::Ascending),
        ]
    );
}

#[test]
fn checked_limit_syntaxes_and_limit_zero_have_distinct_plan_shapes() {
    for sql in [
        "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID LIMIT 5",
        "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID LIMIT 2,5",
        "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID LIMIT 5 OFFSET 2",
    ] {
        let plan = ConfiguredOrderedJoinPlan::lower(sql, &catalog()).expect("checked LIMIT");
        let ConfiguredOrderLimit::Limit(limit) = plan.order_limit().expect("LIMIT tail") else {
            panic!("LIMIT-only shape");
        };
        assert_eq!(limit.count(), 5);
        assert!(plan.join().is_some());
    }

    let empty = ConfiguredOrderedJoinPlan::lower(
        "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID \
         ORDER BY o.Amount LIMIT 0",
        &catalog(),
    )
    .expect("LIMIT zero is known empty after typed binding");
    assert!(empty.is_empty());
    assert!(empty.join().is_none());
    assert_eq!(empty.metadata_join().projections().len(), 1);
    assert_eq!(
        empty.metadata_join().projections()[0].output_name(),
        "Balance"
    );
    assert!(matches!(
        empty.order_limit(),
        Some(ConfiguredOrderLimit::TopN(_))
    ));

    let max_offset_empty = ConfiguredOrderedJoinPlan::lower(
        &format!(
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID \
             ORDER BY o.Amount LIMIT 0 OFFSET {}",
            usize::MAX
        ),
        &catalog(),
    )
    .expect("zero-count LIMIT accepts the largest checked offset");
    let ConfiguredOrderLimit::TopN(spec) = max_offset_empty.order_limit().expect("TopN tail")
    else {
        panic!("ordered zero-count shape");
    };
    assert!(max_offset_empty.is_empty());
    assert_eq!(spec.limit().offset(), usize::MAX);
    assert_eq!(spec.limit().count(), 0);
    assert_eq!(spec.limit().end_exclusive(), usize::MAX);
}

#[test]
fn unsupported_order_and_limit_semantics_fail_closed_without_changing_normal_lowering() {
    assert!(matches!(
        ConfiguredJoinPlan::lower(
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID \
             ORDER BY a.Balance LIMIT 1",
            &catalog(),
        ),
        Err(ConfiguredJoinPlanError::RelationBinding(_))
    ));

    let cases = [
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID ORDER BY a.Balance",
            ConfiguredOrderLimitError::OrderRequiresLimit,
        ),
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID ORDER BY 0 LIMIT 1",
            ConfiguredOrderLimitError::InvalidOrderOrdinal,
        ),
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID ORDER BY 2 LIMIT 1",
            ConfiguredOrderLimitError::InvalidOrderOrdinal,
        ),
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID ORDER BY a.Balance + 1 LIMIT 1",
            ConfiguredOrderLimitError::UnsupportedOrderExpression,
        ),
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID LIMIT -1",
            ConfiguredOrderLimitError::InvalidLimitLiteral,
        ),
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID LIMIT 18446744073709551616",
            ConfiguredOrderLimitError::InvalidLimitLiteral,
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            ConfiguredOrderedJoinPlan::lower(sql, &catalog()),
            Err(expected)
        );
    }

    assert!(matches!(
        ConfiguredOrderedJoinPlan::lower(
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID \
             LIMIT 18446744073709551615 OFFSET 1",
            &catalog(),
        ),
        Err(ConfiguredOrderLimitError::LimitWindow(
            ConfiguredLimitWindowError::EndOverflow { .. }
        ))
    ));

    assert_eq!(
        ConfiguredOrderedJoinPlan::lower(
            "SELECT a.Balance AS label, o.Amount AS label FROM Accounts a \
             JOIN Orders o ON a.AccountID=o.AccountID ORDER BY label LIMIT 1",
            &catalog(),
        ),
        Err(ConfiguredOrderLimitError::AmbiguousOrderAlias(
            "label".to_owned()
        ))
    );
}
