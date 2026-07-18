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

//! Dependency-closed vectors for the projection safety gates in
//! `pkg/planner/core/rule_join_reorder_projection_inline.go`.
//!
//! The Go integration anchor is `TestJoinReorderInlineSafetyGates` at
//! `pkg/planner/core/rule_join_reorder_dp_test.go:315`.  These tests isolate
//! recursive expression support, column-reference requirements, and effect
//! gates; join-group attribution, expression substitution, and plan execution
//! remain external.

use tidb_planner::join_reorder_projection_inline::{
    can_inline_projection_basic, is_inlineable_projection_expr, ProjectionInlineExpr,
    ProjectionInlineShape,
};

fn scalar(args: Vec<ProjectionInlineExpr>) -> ProjectionInlineExpr {
    ProjectionInlineExpr::ScalarFunction {
        args,
        mutable_effects: false,
        non_deterministic: false,
        correlated: false,
    }
}

#[test]
fn accepts_column_referencing_supported_expression_tree() {
    let expression = scalar(vec![
        ProjectionInlineExpr::Column,
        ProjectionInlineExpr::Constant { deferred: false },
    ]);
    assert!(is_inlineable_projection_expr(&expression));
    assert!(can_inline_projection_basic(&ProjectionInlineShape::new(
        false,
        vec![expression],
    )));
}

#[test]
fn rejects_constant_only_and_deferred_constant_expressions() {
    let constant_only = scalar(vec![ProjectionInlineExpr::Constant { deferred: false }]);
    assert!(!can_inline_projection_basic(&ProjectionInlineShape::new(
        false,
        vec![constant_only],
    )));

    let deferred = scalar(vec![
        ProjectionInlineExpr::Column,
        ProjectionInlineExpr::Constant { deferred: true },
    ]);
    assert!(!is_inlineable_projection_expr(&deferred));
    assert!(!can_inline_projection_basic(&ProjectionInlineShape::new(
        false,
        vec![deferred],
    )));
}

#[test]
fn rejects_unsupported_expression_nodes_and_expand_projections() {
    let unsupported = ProjectionInlineExpr::Unsupported {
        referenced_columns: 1,
    };
    assert!(!is_inlineable_projection_expr(&unsupported));
    assert!(!can_inline_projection_basic(&ProjectionInlineShape::new(
        false,
        vec![unsupported],
    )));

    let expand_projection = ProjectionInlineShape::new(false, vec![ProjectionInlineExpr::Column]);
    assert!(can_inline_projection_basic(&expand_projection));
    let marked_expand = ProjectionInlineShape::new(true, expand_projection.expressions().to_vec());
    assert!(!can_inline_projection_basic(&marked_expand));
}

#[test]
fn rejects_nested_mutable_nondeterministic_or_correlated_functions() {
    let cases = [
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ];
    for (mutable_effects, non_deterministic, correlated) in cases {
        let expression = ProjectionInlineExpr::ScalarFunction {
            args: vec![ProjectionInlineExpr::Column],
            mutable_effects,
            non_deterministic,
            correlated,
        };
        assert!(!can_inline_projection_basic(&ProjectionInlineShape::new(
            false,
            vec![expression],
        )));
    }
}
