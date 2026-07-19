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

//! Source-derived tests for configured two-relation name binding.

mod read_only_scan {
    pub mod configured_catalog {
        pub use tidb_planner::read_only_scan::configured_catalog::*;
    }
    pub use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

    pub(crate) fn fold_identifier(identifier: &str) -> String {
        identifier.to_lowercase()
    }
}

#[path = "../src/configured_relation_tree.rs"]
mod configured_relation_tree;

use configured_relation_tree::{
    BoundJoinConstraint, ConfiguredRelationTree, RelationBindError, RelationSide,
};
use read_only_scan::{configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredTable};

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
fn binds_two_relations_projections_and_local_predicates_in_source_order() {
    let tree = ConfiguredRelationTree::bind_sql(
        "SELECT a.AccountID, o.Amount AS total FROM Sales.Accounts AS a \
         INNER JOIN Sales.Orders AS o ON a.AccountID = o.AccountID \
         WHERE a.Balance >= 0 AND o.Amount < 100",
        &catalog(),
    )
    .expect("bounded two-relation query must bind");

    assert_eq!(tree.left().table().table_id(), 101);
    assert_eq!(tree.left().qualifier(), "a");
    assert_eq!(tree.right().table().table_id(), 202);
    assert_eq!(tree.right().qualifier(), "o");
    assert_eq!(
        tree.projections()
            .iter()
            .map(|projection| (
                projection.side(),
                projection.column_offset(),
                projection.output_name(),
            ))
            .collect::<Vec<_>>(),
        [
            (RelationSide::Left, 0, "AccountID"),
            (RelationSide::Right, 2, "total"),
        ]
    );
    assert_eq!(
        tree.local_predicates()
            .iter()
            .map(|predicate| predicate.side())
            .collect::<Vec<_>>(),
        [RelationSide::Left, RelationSide::Right]
    );
    assert!(tree
        .local_predicates()
        .iter()
        .all(|predicate| matches!(predicate.expression(), tidb_ast::Expr::Binary(..))));
    assert!(matches!(tree.join_constraint(), BoundJoinConstraint::On(_)));
}

#[test]
fn using_cross_and_comma_syntax_retain_their_join_boundary() {
    let using = ConfiguredRelationTree::bind_sql(
        "SELECT AccountID, a.Balance, o.Amount FROM Accounts a JOIN Orders o USING (AccountID)",
        &catalog(),
    )
    .expect("USING remains for typed join planning");
    assert_eq!(
        using.join_constraint(),
        &BoundJoinConstraint::Using(vec!["AccountID".to_owned()])
    );
    assert_eq!(using.projections()[0].side(), RelationSide::Left);
    assert_eq!(using.projections()[0].column_offset(), 0);
    assert_eq!(using.projections()[0].output_name(), "AccountID");

    for sql in [
        "SELECT a.Balance, o.Amount FROM Accounts a CROSS JOIN Orders o",
        "SELECT a.Balance, o.Amount FROM Accounts a, Orders o",
    ] {
        let tree = ConfiguredRelationTree::bind_sql(sql, &catalog())
            .expect("CROSS and comma are the same bounded relation shape");
        assert_eq!(tree.join_constraint(), &BoundJoinConstraint::Cross);
    }
}

#[test]
fn aliases_hide_base_names_and_duplicate_qualifiers_fail() {
    assert_eq!(
        ConfiguredRelationTree::bind_sql(
            "SELECT Accounts.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID",
            &catalog(),
        ),
        Err(RelationBindError::UnknownQualifier("Accounts".to_owned()))
    );
    assert_eq!(
        ConfiguredRelationTree::bind_sql(
            "SELECT a.Balance FROM Accounts a JOIN Orders A ON a.AccountID=A.AccountID",
            &catalog(),
        ),
        Err(RelationBindError::DuplicateQualifier("A".to_owned()))
    );
}

#[test]
fn unqualified_columns_are_unique_or_explicitly_ambiguous() {
    let tree = ConfiguredRelationTree::bind_sql(
        "SELECT Balance, Amount FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID",
        &catalog(),
    )
    .expect("unique unqualified names bind to their owning relation");
    assert_eq!(tree.projections()[0].side(), RelationSide::Left);
    assert_eq!(tree.projections()[1].side(), RelationSide::Right);

    assert_eq!(
        ConfiguredRelationTree::bind_sql(
            "SELECT AccountID FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID",
            &catalog(),
        ),
        Err(RelationBindError::AmbiguousColumn(vec![
            "AccountID".to_owned()
        ]))
    );
}

#[test]
fn local_predicates_reject_cross_side_and_non_bigint_comparison_shapes() {
    assert_eq!(
        ConfiguredRelationTree::bind_sql(
            "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID \
             WHERE a.Balance=o.Amount",
            &catalog(),
        ),
        Err(RelationBindError::CrossRelationWherePredicate)
    );
    for sql in [
        "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID WHERE a.Balance + 1 > 2",
        "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID WHERE a.Balance > 1 OR o.Amount > 2",
        "SELECT a.Balance FROM Accounts a JOIN Orders o ON a.AccountID=o.AccountID WHERE a.Balance > 9223372036854775808",
    ] {
        assert_eq!(
            ConfiguredRelationTree::bind_sql(sql, &catalog()),
            Err(RelationBindError::UnsupportedPredicate)
        );
    }
}

#[test]
fn unsupported_relation_and_query_shapes_fail_before_planning() {
    let cases = [
        (
            "SELECT a.Balance FROM Accounts a",
            RelationBindError::ExactlyTwoBaseRelationsRequired,
        ),
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o JOIN Accounts z",
            RelationBindError::ExactlyTwoBaseRelationsRequired,
        ),
        (
            "SELECT a.Balance FROM Accounts a LEFT JOIN Orders o ON a.AccountID=o.AccountID",
            RelationBindError::UnsupportedJoin,
        ),
        (
            "SELECT a.Balance FROM Accounts PARTITION(p0) a JOIN Orders o",
            RelationBindError::UnsupportedTableOption,
        ),
        (
            "SELECT a.Balance FROM (SELECT Balance FROM Accounts) a JOIN Orders o",
            RelationBindError::ExactlyTwoBaseRelationsRequired,
        ),
        (
            "SELECT COUNT(a.Balance) FROM Accounts a JOIN Orders o",
            RelationBindError::UnsupportedProjection,
        ),
        (
            "SELECT a.Balance FROM Accounts a JOIN Orders o ORDER BY a.Balance",
            RelationBindError::UnsupportedQueryShape,
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            ConfiguredRelationTree::bind_sql(sql, &catalog()),
            Err(expected)
        );
    }
}
