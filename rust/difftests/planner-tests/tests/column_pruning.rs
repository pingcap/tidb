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

//! Dependency-closed vectors for the column-pruning schema invariant.
//!
//! The Go anchor is `TestColumnPruning` at
//! `pkg/planner/core/logical_plans_test.go:652`.

use tidb_planner::column_pruning::{no_unexpected_zero_column_schema, SchemaNode};

#[test]
fn ordinary_pruned_plan_has_valid_schema_shape() {
    let plan = SchemaNode::node(1, false, false, vec![SchemaNode::leaf(1, false)]);
    assert!(no_unexpected_zero_column_schema(&plan));
}

#[test]
fn zero_column_exemptions_match_source() {
    let reused = SchemaNode::node(0, true, false, vec![SchemaNode::leaf(1, false)]);
    let dual = SchemaNode::leaf(0, true);
    assert!(no_unexpected_zero_column_schema(&reused));
    assert!(no_unexpected_zero_column_schema(&dual));
}

#[test]
fn unexpected_zero_schema_is_rejected() {
    assert!(!no_unexpected_zero_column_schema(&SchemaNode::leaf(
        0, false
    )));
}
