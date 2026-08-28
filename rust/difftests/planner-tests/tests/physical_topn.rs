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

//! Vectors for the wired PhysicalTopN operator.
//!
//! Source anchors:
//! - `TestPhysicalPlanClone` at `pkg/planner/core/planbuilder_test.go:340`
//! - `PhysicalTopN.PrefixCol` / `PrefixLen` in
//!   `pkg/planner/core/operator/physicalop/physical_topn.go`

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::{aggregation::ByItems, column::Column, expression::Expression};
use tidb_planner::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalTopN};
use tidb_planner::physical_property::SortItem;

fn column(id: i64) -> Expression {
    Expression::Column(Column::new(id, FieldType::new(FieldTypeCode::LongLong)))
}

#[test]
fn clone_preserves_topn_order_and_limit_fields() {
    let plan = PhysicalPlan::TopN(PhysicalTopN {
        base: BasePhysicalPlan::with_id(1, "TopN", 0),
        by_items: vec![ByItems::new(column(1), false)],
        offset: 2333,
        count: 2333,
        ..PhysicalTopN::default()
    });
    let cloned = plan.clone_plan();
    let PhysicalPlan::TopN(original) = &plan else {
        unreachable!();
    };
    let PhysicalPlan::TopN(cloned) = &cloned else {
        unreachable!();
    };
    assert_eq!(cloned.base.base.tp(), "TopN");
    assert_eq!(cloned.offset, 2333);
    assert_eq!(cloned.count, 2333);
    assert!(cloned.by_items[0].equal(&original.by_items[0]));
}

#[test]
fn prefix_index_metadata_lives_on_the_wired_topn() {
    let plan = PhysicalTopN {
        base: BasePhysicalPlan::with_id(1, "TopN", 4),
        by_items: vec![ByItems::new(column(2), false)],
        partition_by: vec![SortItem::new(3, false)],
        offset: 1,
        count: 2,
        prefix_col: Some(2),
        prefix_len: 3,
    };
    assert_eq!(plan.base.base.query_block_offset(), 4);
    assert_eq!(plan.prefix_col, Some(2));
    assert_eq!(plan.prefix_len, 3);
}
