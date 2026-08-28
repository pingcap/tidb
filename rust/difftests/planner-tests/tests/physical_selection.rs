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

//! Dependency-closed vectors for Go's logical-to-physical Selection
//! enumeration.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::{constant::Constant, expression::Expression};
use tidb_planner::{
    logical::{BaseLogicalPlan, LogicalSelection},
    physical::{exhaust_physical_plans_4_logical_selection, PhysicalPlan},
    physical_property::PhysicalProperty,
    plan_base::PlanIdAllocator,
};

#[test]
fn logical_selection_enumerates_the_wired_physical_operator() {
    let allocator = PlanIdAllocator::new();
    let condition = Expression::Constant(Constant::new(
        Datum::Int(1),
        FieldType::new(FieldTypeCode::LongLong),
    ));
    let logical = LogicalSelection::new(
        BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 4),
        vec![condition.clone()],
    );
    let property = PhysicalProperty {
        expected_cnt: 8.0,
        ..PhysicalProperty::default()
    };

    let candidates =
        exhaust_physical_plans_4_logical_selection(&logical, &property, &allocator, 1.0);
    let [PhysicalPlan::Selection(selection)] = candidates.as_slice() else {
        panic!("Go's Selection enumeration must return one wired Selection")
    };
    assert_eq!(selection.base.base.tp(), "Selection");
    assert_eq!(selection.base.base.query_block_offset(), 4);
    assert_eq!(selection.conditions.len(), 1);
    assert!(selection.conditions[0].equal(&condition));
    assert!(!selection.from_data_source);
    assert_eq!(selection.base.child_req_prop(0).unwrap().expected_cnt, 8.0);
}

#[test]
fn physical_selection_carries_go_fine_grained_shuffle_metadata_directly() {
    let allocator = PlanIdAllocator::new();
    let logical = LogicalSelection::new(
        BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0),
        Vec::new(),
    );
    let mut candidates = exhaust_physical_plans_4_logical_selection(
        &logical,
        &PhysicalProperty::default(),
        &allocator,
        1.0,
    );
    let PhysicalPlan::Selection(selection) = &mut candidates[0] else {
        unreachable!()
    };
    selection.base.tiflash_fine_grained_shuffle_stream_count = 8;
    assert_eq!(selection.base.tiflash_fine_grained_shuffle_stream_count, 8);
}
