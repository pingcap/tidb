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

//! Port ledger for `pkg/planner/memo/group_test.go` items 1261-1264 (the tail
//! of that file on `origin/master`; `pkg/planner.part22`).
//!
//! ONE test is a functional port over [`tidb_planner::explore_mark`]
//! (`ExploreMark`, pkg/planner/memo/group.go:30-49). THREE are documented gap
//! ports whose exercised surfaces have no honest Rust carrier yet:
//! - `GetImpl`/`InsertImpl` cache best implementations per physical-property
//!   hash (`group.go:175-184`) and need the `Implementation` interface
//!   (pkg/planner/memo/implementation.go:22) plus a physical-plan value;
//! - first-element-per-operand bookkeeping driven by fingerprint-keyed
//!   `Insert`/`Delete` (`group.go:105-150`, `group.go:167-172`);
//! - `BuildKeyInfo` over a built plan tree
//!   (`group.go:206-235` + `memo.Convert2Group`, group.go:188-198), which needs
//!   parser -> `BuildLogicalPlanForTest` -> memo conversion end to end.

use tidb_planner::explore_mark::ExploreMark;

/// GO PORT of `pkg/planner/memo/group_test.go:196 TestGetInsertGroupImpl`.
///
/// Re-derived contract: `GetImpl` misses before any insert (:202-203,
/// group.go:175-180: map lookup keyed by `PhysicalProperty.HashCode()`,
/// physical_property.go:617); after `InsertImpl(emptyProp, impl)` the SAME
/// property key returns that implementation (:205-206, group.go:181-184); a
/// different property — one sort item on an empty column — hashes to another
/// key and still misses (:208-209).
#[test]
#[ignore = "go-parity-gap: crate Group carries no impl map keyed by PhysicalProperty.HashCode and no Implementation/physical-plan carrier"]
fn get_insert_group_impl_round_trips_one_property_key() {}

/// GO PORT of `pkg/planner/memo/group_test.go:214 TestFirstElemAfterDelete`.
///
/// Re-derived contract: seeding one Limit equivalent then inserting a second
/// keeps the FIRST element as `GetFirstElem(OperandLimit)` (:220-223,
/// group.go:167-172: FirstExpr records the operand's list head at Insert);
/// deleting the head advances it to the surviving expression (:225-227,
/// group.go:125-150 head-advance branch :137-146); deleting the last operand
/// member drops the index entry so GetFirstElem returns nil (:229-230).
#[test]
#[ignore = "go-parity-gap: crate Group has no Delete path and no per-operand FirstExpr head index"]
fn first_elem_after_delete_advances_then_exhausts_operand_head() {}

/// GO PORT of `pkg/planner/memo/group_test.go:233 TestBuildKeyInfo`.
///
/// Re-derived contract over `MockSignedTable` (primary key column `a`):
/// 1. `select a from t where a = 10` builds to a Group whose BuildKeyInfo sets
///    MaxOneRow=true with exactly one PKOrUK entry (:249-254): constant-eq on
///    the primary key proves uniqueness (constant-propagation +
///    BuildKeyInfo via LogicalSelection/DataSource, planner core util).
/// 2. `select b, sum(a) from t group by b` keeps MaxOneRow=false but still has
///    exactly one PKOrUK (group-by column becomes the schema key) (:256-265).
/// 3. A fresh Selection group above group2 inherits child PKOrUK unchanged
///    (unary passthrough, group.go:213-217 + logical selection) (:268-277).
/// 4. A fresh Limit{Count:1} group above group2 sets MaxOneRow=true
///    (LogicalLimit.MaxOneRow(), group.go:227-231) (:280-285).
#[test]
#[ignore = "go-parity-gap: needs parser -> BuildLogicalPlanForTest -> Convert2Group pipeline and Schema.PKOrUK carriers, none of which exist in this crate"]
fn build_key_info_derives_pk_or_uk_and_max_one_row_from_plan_tree() {}

/// GO PORT of `pkg/planner/memo/group_test.go:287 TestExploreMark`.
///
/// Re-derived contract (group.go:30-49): rounds are independent bits; a fresh
/// mark reports nothing explored for any round (:290-292); SetExplored marks
/// only its round (:294-297); SetUnexplored clears only its target while the
/// other round stays set (:299-303). The crate widens no state beyond Go's
/// single-word bitset; out-of-range shifts are checked no-ops on the Rust
/// carrier, which these in-range rounds never touch.
#[test]
fn explore_mark_tracks_two_independent_rounds() {
    let mut mark = ExploreMark::new();
    assert!(!mark.explored(0));
    assert!(!mark.explored(1));

    mark.set_explored(0);
    mark.set_explored(1);
    assert!(mark.explored(0));
    assert!(mark.explored(1));

    mark.set_unexplored(1);
    assert!(mark.explored(0));
    assert!(!mark.explored(1));
}
