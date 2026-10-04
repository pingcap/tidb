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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_planner::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalSort};
use tidb_planner::physical_property::{MppPartitionColumn, PhysicalProperty};

/// GO PORT (Sort half) of `pkg/planner/core/physical_plan_test.go:677
/// TestPhysicalPlanMemoryTrace`.
///
/// Go builds a zero `physicalop.PhysicalSort`, records `MemoryUsage()`,
/// appends one `&util.ByItems{}`, and requires the usage to grow. The same
/// monotonic contract is documented on
/// [`tidb_planner::physical::PhysicalSort::memory_usage`].
#[test]
fn physical_sort_memory_usage_grows_with_each_by_item() {
    let empty = PhysicalPlan::Sort(PhysicalSort {
        base: BasePhysicalPlan::with_id(1, "Sort", 0),
        by_items: Vec::new(),
        is_partial_sort: false,
    });
    let size = empty.memory_usage();
    let with_item = PhysicalPlan::Sort(PhysicalSort {
        base: BasePhysicalPlan::with_id(1, "Sort", 0),
        by_items: vec![ByItems::new(
            Expression::Column(Column::default()),
            false,
        )],
        is_partial_sort: false,
    });
    assert!(with_item.memory_usage() > size);
}

/// GO PORT (PhysicalProperty half) of
/// `pkg/planner/core/physical_plan_test.go:677
/// TestPhysicalPlanMemoryTrace`.
///
/// Go appends a `&property.MPPPartitionColumn{}` to
/// `PhysicalProperty.MPPPartitionCols` and requires `MemoryUsage()` to grow.
#[test]
fn physical_property_memory_usage_grows_with_mpp_partition_cols() {
    let mut property = PhysicalProperty::default();
    let empty = property.memory_usage();
    property.mpp_partition_cols.push(MppPartitionColumn::new(1, 0));
    assert!(property.memory_usage() > empty);
}
