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

//! Source-shaped planner→executor metadata selector.
//!
//! This is intentionally not an optimizer test.  It exercises the subset of
//! `baseimpl.Plan`/`BasePhysicalPlan` that can be ported without a session,
//! schema, expression, or storage owner.

use tidb_planner::plan::PlanNode;

/// Mirrors the metadata assertions in
/// `pkg/planner/core/planbuilder_test.go:1227`.
#[test]
fn index_lookup_plan_metadata_selector() {
    let table_scan = PlanNode::new("TableScan", 100, 10).with_estimated_rows(500.0);
    let selection = PlanNode::new("Selection", 101, 10)
        .with_estimated_rows(200.0)
        .with_children([table_scan]);
    let projection = PlanNode::new("Projection", 102, 10)
        .with_estimated_rows(200.0)
        .with_children([selection]);

    assert_eq!(projection.operator(), "Projection");
    assert_eq!(projection.id(), 102);
    assert_eq!(projection.query_block_offset(), 10);
    assert_eq!(projection.estimated_rows(), Some(200.0));
    assert_eq!(projection.children().len(), 1);
    assert_eq!(projection.children()[0].children().len(), 1);

    assert_eq!(
        projection.metadata_preorder(),
        vec![
            tidb_planner::plan::PlanNodeMetadata {
                operator: "Projection".to_owned(),
                id: 102,
                query_block_offset: 10,
                estimated_rows: Some(200.0),
                child_count: 1,
            },
            tidb_planner::plan::PlanNodeMetadata {
                operator: "Selection".to_owned(),
                id: 101,
                query_block_offset: 10,
                estimated_rows: Some(200.0),
                child_count: 1,
            },
            tidb_planner::plan::PlanNodeMetadata {
                operator: "TableScan".to_owned(),
                id: 100,
                query_block_offset: 10,
                estimated_rows: Some(500.0),
                child_count: 0,
            },
        ]
    );
}

/// Mirrors `pkg/planner/core/operator/baseimpl/plan.go:101`.
#[test]
fn explain_id_suffix_selector() {
    let node = PlanNode::new("TableReader", 42, 0);
    assert_eq!(node.explain_id(false), "TableReader_42");
    assert_eq!(node.explain_id(true), "TableReader");
}

/// An absent statistic is distinct from a zero-row estimate, as it is in the
/// Go `*property.StatsInfo` pointer field.
#[test]
fn plan_metadata_preserves_missing_stats() {
    let node = PlanNode::new("TableDual", 1, 3);
    assert_eq!(node.estimated_rows(), None);
    assert_eq!(node.metadata_preorder()[0].estimated_rows, None);
}
