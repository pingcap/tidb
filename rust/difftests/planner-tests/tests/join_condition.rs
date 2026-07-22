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

//! Source-shaped tests for the `LogicalJoin` condition and `FullSchema`
//! boundary (`pkg/planner/core/operator/logicalop/logical_join.go`).

use tidb_ast::{BinaryOp, Expr};
use tidb_planner::join_condition::{
    ColumnSpec, EqualitySemantics, JoinCondition, JoinSchema, JoinSide, TruthValue,
    UnsupportedJoinCondition,
};

fn schema() -> JoinSchema {
    JoinSchema::new(
        [
            ColumnSpec::with_qualifiers("id", ["left"], false),
            ColumnSpec::new("payload", "left"),
        ],
        [
            ColumnSpec::with_qualifiers("id", ["right"], true),
            ColumnSpec::new("payload", "right"),
        ],
    )
}

fn col(path: &[&str]) -> Expr {
    Expr::Column(path.iter().map(|part| (*part).to_owned()).collect())
}

fn eq(lhs: Expr, rhs: Expr) -> Expr {
    Expr::Binary(BinaryOp::Eq, Box::new(lhs), Box::new(rhs))
}

#[test]
fn qualified_equality_normalizes_operand_order_and_full_schema_indices() {
    let condition = schema().classify_on(&Expr::Paren(Box::new(eq(
        col(&["right", "id"]),
        col(&["left", "id"]),
    ))));
    let JoinCondition::Equality(equality) = condition else {
        panic!("expected cross-side equality")
    };
    assert_eq!(equality.left().side(), JoinSide::Left);
    assert_eq!(equality.left().side_index(), 0);
    assert_eq!(equality.left().full_index(), 0);
    assert!(!equality.left().nullable());
    assert_eq!(equality.right().side(), JoinSide::Right);
    assert_eq!(equality.right().side_index(), 0);
    assert_eq!(equality.right().full_index(), 2);
    assert!(equality.right().nullable());
    assert_eq!(equality.semantics(), EqualitySemantics::ThreeValued);
    assert_eq!(equality.null_truth(true, false), TruthValue::Unknown);
}

#[test]
fn unqualified_unique_names_bind_across_children() {
    let unique = JoinSchema::new(
        [ColumnSpec::new("left_only", "left")],
        [ColumnSpec::new("right_only", "right")],
    );
    let condition = unique.classify_on(&eq(col(&["left_only"]), col(&["right_only"])));
    let JoinCondition::Equality(equality) = condition else {
        panic!("expected unqualified names to bind")
    };
    assert_eq!(equality.left().name(), "left_only");
    assert_eq!(equality.right().name(), "right_only");
}

#[test]
fn unqualified_using_names_do_not_become_their_own_qualifier() {
    let conditions = schema().bind_using(["id"]);
    let JoinCondition::Equality(equality) = &conditions[0] else {
        panic!("USING id should bind without a qualifier")
    };
    assert_eq!(equality.left().full_index(), 0);
    assert_eq!(equality.right().full_index(), 2);
}

#[test]
fn using_produces_one_three_valued_pair_per_name() {
    let conditions = schema().bind_using(["id", "payload"]);
    assert_eq!(conditions.len(), 2);
    for condition in conditions {
        let JoinCondition::Equality(equality) = condition else {
            panic!("USING names should bind")
        };
        assert_eq!(equality.semantics(), EqualitySemantics::ThreeValued);
        assert_eq!(equality.null_truth(true, true), TruthValue::Unknown);
    }
}

#[test]
fn null_safe_equality_has_distinct_null_contract() {
    let expr = Expr::Binary(
        BinaryOp::NullEq,
        Box::new(col(&["left", "id"])),
        Box::new(col(&["right", "id"])),
    );
    let JoinCondition::Equality(equality) = schema().classify_on(&expr) else {
        panic!("expected null-safe equality")
    };
    assert_eq!(equality.semantics(), EqualitySemantics::NullSafe);
    assert_eq!(equality.null_truth(true, true), TruthValue::True);
    assert_eq!(equality.null_truth(true, false), TruthValue::False);
    assert_eq!(
        equality.null_truth(false, false),
        TruthValue::DependsOnValues
    );
}

#[test]
fn ambiguous_and_unsupported_shapes_are_never_join_keys() {
    let duplicate = JoinSchema::new(
        [ColumnSpec::new("id", "left")],
        [ColumnSpec::new("id", "right")],
    );
    assert!(matches!(
        duplicate.classify_on(&eq(col(&["id"]), col(&["id"]))),
        JoinCondition::Unsupported(UnsupportedJoinCondition::AmbiguousColumn { .. })
    ));

    assert!(matches!(
        schema().classify_on(&Expr::Binary(
            BinaryOp::LogicAnd,
            Box::new(col(&["left", "id"])),
            Box::new(col(&["right", "id"])),
        )),
        JoinCondition::Unsupported(UnsupportedJoinCondition::Compound {
            operator: BinaryOp::LogicAnd
        })
    ));

    assert!(matches!(
        schema().classify_on(&eq(
            Expr::Func {
                name: "LOWER".to_owned(),
                args: vec![col(&["left", "id"])],
                origin_position: 0,
            },
            col(&["right", "id"]),
        )),
        JoinCondition::Unsupported(UnsupportedJoinCondition::Function { name }) if name == "LOWER"
    ));

    assert!(matches!(
        schema().classify_on(&eq(col(&["left", "id"]), Expr::Int("1".to_owned()))),
        JoinCondition::Unsupported(UnsupportedJoinCondition::NonColumnOperand)
    ));
}
