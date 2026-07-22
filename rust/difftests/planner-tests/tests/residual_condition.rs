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

//! Source-shaped tests for residual `OtherConditions` boolean shape.
//!
//! The Go source is `pkg/planner/core/expression_rewriter.go`'s boolean-chain
//! handling and `pkg/planner/core/logical_plans_test.go`'s
//! `TestDupRandJoinCondsPushDown`: direct equality remains a join-key concern,
//! while a comparison/function residual must remain an executor-owned leaf.

use tidb_ast::{BinaryOp, Expr, UnaryOp};
use tidb_planner::residual_condition::{
    classify_residual, OperandShape, ResidualEvaluation, ResidualLeafKind, ResidualPredicate,
    ResidualUnsupported,
};

fn col(name: &str) -> Expr {
    Expr::Column(vec!["t".to_owned(), name.to_owned()])
}

fn binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary(operator, Box::new(left), Box::new(right))
}

#[test]
fn conjunction_preserves_comparison_and_function_residuals() {
    let expr = binary(
        BinaryOp::LogicAnd,
        binary(BinaryOp::Gt, col("a"), Expr::Int("1".to_owned())),
        Expr::Func {
            name: "RAND".to_owned(),
            args: Vec::new(),
            origin_position: 0,
        },
    );
    let ResidualPredicate::All(children) = classify_residual(&expr) else {
        panic!("AND must remain a conjunction")
    };
    assert_eq!(children.len(), 2);

    let ResidualPredicate::Leaf(comparison) = &children[0] else {
        panic!("comparison must remain a residual leaf")
    };
    assert_eq!(comparison.evaluation(), ResidualEvaluation::Deferred);
    assert!(matches!(
        comparison.kind(),
        ResidualLeafKind::Binary {
            operator: BinaryOp::Gt,
            left: OperandShape::Column { parts: 2 },
            right: OperandShape::Constant,
        }
    ));

    let ResidualPredicate::Leaf(function) = &children[1] else {
        panic!("function must remain a residual leaf")
    };
    assert!(matches!(
        function.kind(),
        ResidualLeafKind::Function { name, arity: 0 } if name == "RAND"
    ));
}

#[test]
fn not_or_parentheses_keep_boolean_shape_without_evaluation() {
    let expr = Expr::Unary(
        UnaryOp::NotKeyword,
        Box::new(Expr::Paren(Box::new(binary(
            BinaryOp::LogicOr,
            binary(BinaryOp::Lt, col("a"), col("b")),
            Expr::Bool(true),
        )))),
    );
    let ResidualPredicate::Not(inner) = classify_residual(&expr) else {
        panic!("NOT must remain a negation")
    };
    let ResidualPredicate::Any(children) = inner.as_ref() else {
        panic!("OR under parentheses must remain a disjunction")
    };
    assert_eq!(children.len(), 2);
    assert!(matches!(&children[1], ResidualPredicate::Leaf(_)));
}

#[test]
fn dedicated_predicate_shapes_are_explicitly_unsupported() {
    let expr = Expr::In {
        expr: Box::new(col("a")),
        list: vec![Expr::Int("1".to_owned()), Expr::Int("2".to_owned())],
        not: false,
    };
    assert!(matches!(
        classify_residual(&expr),
        ResidualPredicate::Unsupported(ResidualUnsupported::AstVariant { category: "in" })
    ));

    let expr = Expr::Unary(UnaryOp::Minus, Box::new(col("a")));
    assert!(matches!(
        classify_residual(&expr),
        ResidualPredicate::Unsupported(ResidualUnsupported::UnaryOperator {
            operator: UnaryOp::Minus
        })
    ));
}
