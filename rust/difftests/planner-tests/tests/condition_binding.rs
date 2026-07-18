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

//! Source-shaped tests for deferred residual column binding.
//!
//! The Go owner is the expression-rewriter/LogicalJoin hand-off in
//! `pkg/planner/core/expression_rewriter.go`: non-key `OtherConditions` remain
//! executor-owned, but their column references still resolve against the
//! planner's full schema.  These tests check that mapping without evaluating a
//! Datum or claiming a join algorithm.

use tidb_ast::{BinaryOp, Expr};
use tidb_planner::condition_binding::{bind_residual, ConditionBindingError, DeferredEvaluation};
use tidb_planner::join_condition::{ColumnSpec, JoinSchema};

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

fn binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary(operator, Box::new(left), Box::new(right))
}

#[test]
fn residual_columns_bind_in_source_order_to_full_schema() {
    let expr = binary(
        BinaryOp::LogicAnd,
        binary(
            BinaryOp::Gt,
            col(&["left", "payload"]),
            Expr::Int("1".to_owned()),
        ),
        Expr::Func {
            name: "LOWER".to_owned(),
            args: vec![col(&["right", "payload"])],
        },
    );
    let plan = bind_residual(&expr, &schema()).expect("known residual columns should bind");

    assert_eq!(plan.evaluation(), DeferredEvaluation::TypedExecutor);
    assert_eq!(plan.bindings().len(), 2);
    assert_eq!(
        plan.bindings()[0].path().to_vec(),
        vec!["left".to_owned(), "payload".to_owned()]
    );
    assert_eq!(plan.bindings()[0].column().full_index(), 1);
    assert_eq!(
        plan.bindings()[1].path().to_vec(),
        vec!["right".to_owned(), "payload".to_owned()]
    );
    assert_eq!(plan.bindings()[1].column().full_index(), 3);
    assert!(plan.opaque_shapes().is_empty());
}

#[test]
fn dedicated_predicate_binds_nested_columns_for_typed_owner() {
    let expr = Expr::In {
        expr: Box::new(col(&["left", "id"])),
        list: vec![Expr::Int("1".to_owned())],
        not: false,
    };
    let plan = bind_residual(&expr, &schema()).expect("nested IN column should bind");

    assert_eq!(plan.bindings().len(), 1);
    assert_eq!(plan.bindings()[0].column().full_index(), 0);
    assert!(plan.opaque_shapes().is_empty());
}

#[test]
fn unknown_or_ambiguous_column_never_becomes_a_constant() {
    let unknown = bind_residual(&col(&["missing", "id"]), &schema());
    assert!(matches!(
        unknown,
        Err(ConditionBindingError::UnknownColumn { path })
            if path == vec!["missing".to_owned(), "id".to_owned()]
    ));

    let ambiguous = bind_residual(&col(&["id"]), &schema());
    assert!(matches!(
        ambiguous,
        Err(ConditionBindingError::AmbiguousColumn { path })
            if path == vec!["id".to_owned()]
    ));
}
