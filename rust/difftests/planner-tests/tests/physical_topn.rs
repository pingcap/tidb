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

//! Dependency-closed vectors for PhysicalTopN metadata.
//!
//! Source anchors:
//! - `TestPhysicalPlanClone` at `pkg/planner/core/planbuilder_test.go:340`
//! - `TestVirtualExprPushDown` at `pkg/planner/core/integration_test.go:1897`

use tidb_planner::physical_topn::{PhysicalTopNPlan, RedactMode, TopNOrderItem};

#[test]
fn clone_preserves_topn_order_and_limit_fields() {
    let plan = PhysicalTopNPlan::init(
        vec![TopNOrderItem::new("test.t.a", false)],
        vec![],
        2333,
        2333,
        0,
    );
    let cloned = plan.clone_plan();
    assert_eq!(cloned.plan_type(), "TopN");
    assert_eq!(cloned.offset(), 2333);
    assert_eq!(cloned.count(), 2333);
    assert_eq!(cloned.by_items(), plan.by_items());
}

#[test]
fn virtual_expression_pushdown_topn_explain_matches_tikv_and_tiflash_shape() {
    let plan = PhysicalTopNPlan::init(
        vec![TopNOrderItem::new("test.t.c2", false)],
        vec![],
        0,
        2,
        0,
    );
    assert_eq!(
        plan.explain_info(RedactMode::Disable),
        "test.t.c2, offset:0, count:2"
    );
}

#[test]
fn partitioned_topn_explain_preserves_prefix_and_redaction() {
    let plan = PhysicalTopNPlan::init(
        vec![TopNOrderItem::new("test.t.c2", false)],
        vec![TopNOrderItem::new("test.t.p", false)],
        1,
        2,
        4,
    )
    .with_prefix_col("test.t.c2", 3);
    assert_eq!(plan.query_block_offset(), 4);
    assert_eq!(
        plan.explain_info(RedactMode::Marker),
        "partition by test.t.p order by test.t.c2, offset:‹1›, count:‹2›, prefix_col:‹test.t.c2›, prefix_len:‹3›"
    );
    assert_eq!(
        plan.explain_normalized_info(),
        "partition by test.t.p order by test.t.c2"
    );
}

#[test]
fn normalized_topn_text_can_differ_from_explain_text() {
    let plan = PhysicalTopNPlan::init(
        vec![TopNOrderItem::new("test.t.c2", true).with_normalized_text("?")],
        vec![TopNOrderItem::new("test.t.p", false).with_normalized_text("?")],
        0,
        2,
        0,
    );
    assert_eq!(
        plan.explain_info(RedactMode::Disable),
        "partition by test.t.p order by test.t.c2:desc, offset:0, count:2"
    );
    assert_eq!(
        plan.explain_normalized_info(),
        "partition by ? order by ?:desc"
    );
}
