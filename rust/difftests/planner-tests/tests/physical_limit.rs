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

//! Vectors for the wired PhysicalLimit operator.
//!
//! The Go anchor is `TestLimitPushdown` at
//! `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1600`.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::{column::Column, schema::Schema};
use tidb_planner::physical::{BasePhysicalPlan, PhysicalLimit, RedactMode};
use tidb_planner::physical_property::SortItem;

fn limit(offset: u64, count: u64, query_block_offset: i32) -> PhysicalLimit {
    PhysicalLimit {
        base: BasePhysicalPlan::with_id(1, "Limit", query_block_offset),
        offset,
        count,
        ..PhysicalLimit::default()
    }
}

#[test]
fn limit_plan_tree_metadata_matches_source() {
    let plan = limit(0, 100, 2);
    assert_eq!(plan.base.base.tp(), "Limit");
    assert_eq!(plan.base.base.query_block_offset(), 2);
    assert_eq!(
        plan.explain_info(RedactMode::Disable),
        "offset:0, count:100"
    );
}

#[test]
fn redaction_modes_preserve_source_limit_shape() {
    let plan = limit(4, 8, 0);
    assert_eq!(
        plan.explain_info(RedactMode::Marker),
        "offset:‹4›, count:‹8›"
    );
    assert_eq!(plan.explain_info(RedactMode::Enable), "offset:?, count:?");
}

#[test]
fn partition_and_prefix_metadata_are_caller_owned() {
    let mut column = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
    column.orig_name = "a".to_owned();
    let mut plan = limit(1, 2, 0);
    plan.base.base.set_schema(Some(Schema::new(vec![column])));
    plan.partition_by = vec![SortItem::new(1, false)];
    plan.prefix_col = Some(1);
    plan.prefix_len = 3;
    assert_eq!(
        plan.explain_info(RedactMode::Disable),
        "partition by a, offset:1, count:2, prefix_col:a, prefix_len:3"
    );
}
