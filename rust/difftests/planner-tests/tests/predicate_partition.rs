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

//! Source-shaped tests for conservative predicate dependency routing.
//!
//! The Go owner is `LogicalJoin.ExtractOnCondition` and its predicate
//! pushdown callers.  The Rust boundary records only source dependencies and
//! typed-check requirements; it deliberately does not evaluate values or
//! pick a physical join implementation.

use tidb_ast::{BinaryOp, Expr};
use tidb_planner::join_condition::JoinSchema;
use tidb_planner::predicate_partition::{partition_predicates, PredicateRoute, PredicateSafety};

fn schema() -> JoinSchema {
    JoinSchema::new(
        [
            tidb_planner::join_condition::ColumnSpec::new("id", "left"),
            tidb_planner::join_condition::ColumnSpec::new("payload", "left"),
        ],
        [
            tidb_planner::join_condition::ColumnSpec::new("id", "right"),
            tidb_planner::join_condition::ColumnSpec::new("payload", "right"),
        ],
    )
}

fn col(table: &str, name: &str) -> Expr {
    Expr::Column(vec![table.to_owned(), name.to_owned()])
}

fn binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary(operator, Box::new(left), Box::new(right))
}

#[test]
fn source_dependencies_choose_child_or_join_residual_routes() {
    let predicates = partition_predicates(
        [
            binary(
                BinaryOp::Gt,
                col("left", "payload"),
                Expr::Int("1".to_owned()),
            ),
            binary(BinaryOp::Eq, col("left", "id"), col("right", "id")),
        ],
        &schema(),
    )
    .expect("known columns should partition");
    assert_eq!(predicates.predicates().len(), 2);
    assert_eq!(
        predicates.predicates()[0].route(),
        PredicateRoute::LeftPushdown
    );
    assert_eq!(
        predicates.predicates()[0].safety(),
        PredicateSafety::ShapeOnly
    );
    assert_eq!(
        predicates.predicates()[1].route(),
        PredicateRoute::JoinResidual
    );
}

#[test]
fn mutable_and_opaque_shapes_never_become_pushdown_claims() {
    let predicates = partition_predicates(
        [
            Expr::Func {
                name: "RAND".to_owned(),
                args: Vec::new(),
            },
            Expr::In {
                expr: Box::new(col("left", "id")),
                list: vec![Expr::Int("1".to_owned())],
                not: false,
            },
        ],
        &schema(),
    )
    .expect("unsupported shapes remain deferred");
    assert_eq!(predicates.predicates()[0].route(), PredicateRoute::Deferred);
    assert_eq!(
        predicates.predicates()[0].safety(),
        PredicateSafety::RequiresTypedCheck
    );
    assert_eq!(predicates.predicates()[1].route(), PredicateRoute::Deferred);
    assert_eq!(
        predicates.predicates()[1].safety(),
        PredicateSafety::RequiresTypedCheck
    );
}

#[test]
fn a_single_child_function_keeps_dependency_but_requires_effects_check() {
    let predicates = partition_predicates(
        [Expr::Func {
            name: "LOWER".to_owned(),
            args: vec![col("right", "payload")],
        }],
        &schema(),
    )
    .expect("function argument should bind");
    assert_eq!(
        predicates.predicates()[0].route(),
        PredicateRoute::RightPushdown
    );
    assert_eq!(
        predicates.predicates()[0].safety(),
        PredicateSafety::RequiresTypedCheck
    );
}

#[test]
fn literal_only_predicate_is_only_a_constant_candidate() {
    let predicates = partition_predicates([Expr::Bool(true)], &schema())
        .expect("literal shape should remain deferred");
    assert_eq!(
        predicates.predicates()[0].route(),
        PredicateRoute::ConstantCandidate
    );
    assert_eq!(
        predicates.predicates()[0].safety(),
        PredicateSafety::ShapeOnly
    );
}
