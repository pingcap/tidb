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

//! `pkg/planner.part14` ports of physical-operator METADATA invariants:
//!
//! * `physical_plan_test.go:677 TestPhysicalPlanMemoryTrace` — the Sort half
//!   RUNS against the wired [`tidb_planner::physical::PhysicalSort`]; the
//!   `PhysicalProperty` half is an honest `#[ignore]` gap port.
//! * `plan_test.go:723 TestCloneFineGrainedShuffleStreamCount` — honest
//!   `#[ignore]` gap port (the wired physical tree has no Window or MPP
//!   stream-count state).
//! * `physical_plan_test.go:843 TestExchangeSenderResolveIndices` — honest
//!   `#[ignore]` gap port (index resolution unported).

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_planner::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalSort};

/// GO PORT (Sort half) of `pkg/planner/core/physical_plan_test.go:677
/// TestPhysicalPlanMemoryTrace`.
///
/// Go builds a zero `physicalop.PhysicalSort`, records `MemoryUsage()`,
/// appends one `&util.ByItems{}`, and requires the usage to grow. The same
/// monotonic contract is documented on
/// [`tidb_planner::physical::PhysicalSort::memory_usage`]; the
/// PhysicalProperty half of the Go test is a separate gap port below.
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

/// GO PARITY GAP port (PhysicalProperty half) of
/// `pkg/planner/core/physical_plan_test.go:677
/// TestPhysicalPlanMemoryTrace`.
///
/// go-parity-gap: Go appends a `&property.MPPPartitionColumn{}` to
/// `PhysicalProperty.MPPPartitionCols` and requires `MemoryUsage()` to grow.
/// This crate's `physical_property::PhysicalProperty` deliberately omits the
/// MPP partitioning fields (see its module header: "deliberately absent
/// rather than stubbed"), and with them the property-side memory accounting,
/// so the assertion is unexpressable.
#[test]
#[ignore = "go-parity-gap: PhysicalProperty.MPPPartitionCols and its memory accounting are deliberately absent from this crate's property leaf"]
fn physical_property_memory_usage_grows_with_mpp_partition_cols() {}

/// GO PORT of `pkg/planner/core/plan_test.go:723
/// TestCloneFineGrainedShuffleStreamCount`.
///
/// Go clones both wired physical Window and Sort operators with stream counts
/// zero and eight. Rust's wired [`tidb_planner::physical::PhysicalPlan`] has a
/// Sort variant, but it has neither a Window variant nor Go's inherited
/// `TiFlashFineGrainedShuffleStreamCount` field. Testing a separate scalar
/// metadata shell would not exercise planner construction or cloning.
#[test]
#[ignore = "go-parity-gap: wired PhysicalPlan lacks Window and TiFlashFineGrainedShuffleStreamCount"]
fn clone_fine_grained_shuffle_stream_count_preserved_on_window_and_sort() {}

/// GO PARITY GAP port of `pkg/planner/core/physical_plan_test.go:843
/// TestExchangeSenderResolveIndices`.
///
/// go-parity-gap: Go shares ONE `*property.MPPPartitionColumn` between two
/// `PhysicalExchangeSender`s and, after
/// `ResolveIndicesItselfWithSchema` against schemas of four and two columns
/// (`pkg/planner/core/operator/physicalop/physical_exchange_sender.go:145`),
/// requires the two senders' `HashCols[0].Col.Index` to DIFFER (3 vs 1).
/// This crate's wired physical tree has no ExchangeSender variant or
/// hash-column index resolution against a schema, so the aliasing observation
/// cannot run.
#[test]
#[ignore = "go-parity-gap: PhysicalExchangeSender.ResolveIndicesItselfWithSchema (physical_exchange_sender.go:145) is unported"]
fn exchange_sender_resolve_indices_splits_shared_partition_col_indices() {}
