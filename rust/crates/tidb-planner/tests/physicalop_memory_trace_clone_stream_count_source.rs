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
//!   RUNS against [`tidb_planner::physical_sort::PhysicalSortPlan`]; the
//!   `PhysicalProperty` half is an honest `#[ignore]` gap port.
//! * `plan_test.go:723 TestCloneFineGrainedShuffleStreamCount` — RUNS
//!   against the Window/Sort metadata clones.
//! * `physical_plan_test.go:843 TestExchangeSenderResolveIndices` — honest
//!   `#[ignore]` gap port (index resolution unported).

use tidb_planner::physical_sort::{PhysicalSortPlan, SortItem};
use tidb_planner::physical_window::PhysicalWindowPlan;

/// GO PORT (Sort half) of `pkg/planner/core/physical_plan_test.go:677
/// TestPhysicalPlanMemoryTrace`.
///
/// Go builds a zero `physicalop.PhysicalSort`, records `MemoryUsage()`,
/// appends one `&util.ByItems{}`, and requires the usage to grow. The same
/// monotonic contract is documented on
/// [`tidb_planner::physical_sort::PhysicalSortPlan::memory_usage`]; the
/// PhysicalProperty half of the Go test is a separate gap port below.
#[test]
fn physical_sort_memory_usage_grows_with_each_by_item() {
    let empty = PhysicalSortPlan::init(Vec::new(), false, 0, 0);
    let size = empty.memory_usage();
    let with_item = PhysicalSortPlan::init(vec![SortItem::new("a", false)], false, 0, 0);
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
/// Go clones a zero `PhysicalWindow` via `window.Clone(nil)`, requires the
/// same concrete type and equal `TiFlashFineGrainedShuffleStreamCount`, then
/// sets the count to 8 and repeats; the same four steps run for
/// `PhysicalSort`. Rust's metadata leaves carry the field as
/// `stream_count`, and `clone_plan` is the source `Clone` counterpart whose
/// header documents the field preservation.
#[test]
fn clone_fine_grained_shuffle_stream_count_preserved_on_window_and_sort() {
    // Window, inherited count zero (Go's zero-value operator).
    let window = PhysicalWindowPlan::init("row_number() over()", 0, 0);
    let cloned = window.clone_plan();
    assert_eq!(window.stream_count(), cloned.stream_count());

    // Window with the count stamped to 8.
    let window = PhysicalWindowPlan::init("row_number() over()", 0, 8);
    let cloned = window.clone_plan();
    assert_eq!(8, window.stream_count());
    assert_eq!(window.stream_count(), cloned.stream_count());

    // Sort, inherited count zero.
    let sort = PhysicalSortPlan::init(Vec::new(), false, 0, 0);
    let cloned = sort.clone_plan();
    assert_eq!(sort.stream_count(), cloned.stream_count());

    // Sort with the count stamped to 8.
    let sort = PhysicalSortPlan::init(
        vec![SortItem::new("a", false)],
        false,
        0,
        8,
    );
    let cloned = sort.clone_plan();
    assert_eq!(8, sort.stream_count());
    assert_eq!(sort.stream_count(), cloned.stream_count());
}

/// GO PARITY GAP port of `pkg/planner/core/physical_plan_test.go:843
/// TestExchangeSenderResolveIndices`.
///
/// go-parity-gap: Go shares ONE `*property.MPPPartitionColumn` between two
/// `PhysicalExchangeSender`s and, after
/// `ResolveIndicesItselfWithSchema` against schemas of four and two columns
/// (`pkg/planner/core/operator/physicalop/physical_exchange_sender.go:145`),
/// requires the two senders' `HashCols[0].Col.Index` to DIFFER (3 vs 1).
/// This crate's `physical_exchange_sender` leaf preserves the Init identity
/// and ExplainInfo branches only — it carries no hash-column index
/// resolution against a schema — so the aliasing observation cannot run.
#[test]
#[ignore = "go-parity-gap: PhysicalExchangeSender.ResolveIndicesItselfWithSchema (physical_exchange_sender.go:145) is unported"]
fn exchange_sender_resolve_indices_splits_shared_partition_col_indices() {}
