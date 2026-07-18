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

//! Dependency-closed vectors for the loose projection predicate in
//! `pkg/planner/core/rule_eliminate_projection.go`.
//!
//! The Go integration anchor is `TestProjectionEliminator` at
//! `pkg/planner/core/logical_plans_test.go:706`.  These tests isolate the
//! source expression-shape and `Proj4Expand` gates; full logical expression
//! replacement, schema mutation, and physical optimizer integration remain
//! external.

use tidb_planner::projection_elimination::{
    LogicalProjectionShape, ProjectionEliminator, ProjectionExprShape,
};

#[test]
fn empty_projection_is_loosely_eliminable_when_not_expand() {
    let projection = LogicalProjectionShape::new(false, Vec::new());
    assert!(ProjectionEliminator.can_eliminate_loose(&projection));
}

#[test]
fn direct_column_projection_is_loosely_eliminable() {
    let projection = LogicalProjectionShape::new(
        false,
        vec![
            ProjectionExprShape::Column,
            ProjectionExprShape::Column,
            ProjectionExprShape::Column,
        ],
    );
    assert!(ProjectionEliminator.can_eliminate_loose(&projection));
}

#[test]
fn computed_expression_blocks_loose_elimination() {
    let projection = LogicalProjectionShape::new(
        false,
        vec![ProjectionExprShape::Column, ProjectionExprShape::Computed],
    );
    assert!(!ProjectionEliminator.can_eliminate_loose(&projection));
}

#[test]
fn expand_projection_blocks_even_column_only_shape() {
    let projection = LogicalProjectionShape::new(
        true,
        vec![ProjectionExprShape::Column, ProjectionExprShape::Column],
    );
    assert!(!ProjectionEliminator.can_eliminate_loose(&projection));
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(ProjectionEliminator.name(), "projection_eliminate");
}
