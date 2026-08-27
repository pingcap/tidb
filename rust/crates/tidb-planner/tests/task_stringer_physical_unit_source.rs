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

//! Documentary gap ports for three `pkg/planner/core` physical/stringer unit
//! tests (`pkg/planner.part15` items 869–871 on `origin/master`).
//!
//! | Go function | Rust test |
//! | --- | --- |
//! | `stringer_test.go:30 TestPlanStringer` | [`plan_stringer_show_operator_renderings`] |
//! | `task_heavy_function_optimize_test.go:36 TestGetPushedDownTopNHeavyFunctionNotFirstByItem` | [`pushed_down_topn_moves_heavy_function_byitem_last`] |
//! | `task_test.go:32 TestPhysicalUnionScanAttach2Task` | [`physical_union_scan_attach_keeps_child_plan_chain`] |

/// GO PORT of `pkg/planner/core/stringer_test.go:30 TestPlanStringer`.
///
/// Re-derived contract: after building and logically optimizing SHOW
/// statements against a mock store, `core.ToString(p)` renders the Show
/// operator with exactly one decoration per statement shape (:37-123):
/// `Show(field:[a])` for column-list/DESC-with-column, bare `Show` for
/// plain DESC, `Show(field_pattern:[a%])` for column LIKE patterns,
/// `Show(table:[t])` vs `Show(table_pattern:[t%])` for table lists/patterns
/// (case-insensitive match normalizes `'T'` → lower), mirrored database and
/// collation variants, and a WHERE-filtered show rendering as
/// `Show->Sel([eq(Column#13, a)])->Projection`.
#[test]
#[ignore = "go-parity-gap: plan Build + ToString rendering of Show operators is not transcreated"]
fn plan_stringer_show_operator_renderings() {}

/// GO PORT of
/// `pkg/planner/core/task_heavy_function_optimize_test.go:36
/// TestGetPushedDownTopNHeavyFunctionNotFirstByItem`.
///
/// Re-derived contract under fix-control 56318 (:38-40): build a PhysicalTopN
/// whose by-items are `[coalesce(score,0) (no heavy fn, Desc),
/// 1*(1-vec_cosine_distance(vec,const))+(1*(1-distance))` nested vector
/// expression (heavy, Desc)] over a TiFlash child plan (:137-188);
/// `ContainHeavyFunction` flips false/true across the two exprs (:177-178).
/// `getPushedDownTopN(topN, childPlan, kv.TiFlash)` must return both halves
/// non-nil with the pushed-down TopN REORDERED so no heavy expression sits
/// first — position 1 holds the only Column item at index == output-schema
/// length (:190-211) in BOTH the pushed-down and global TopN.
#[test]
#[ignore = "go-parity-gap: getPushedDownTopN heavy-function split is documented unported (task.rs keeps only the non-heavy half)"]
fn pushed_down_topn_moves_heavy_function_byitem_last() {}

/// GO PORT of `pkg/planner/core/task_test.go:32 TestPhysicalUnionScanAttach2Task`.
///
/// Re-derived contract: chain TableScan→TableReader→Projection→Selection as
/// root-task plans; attaching `PhysicalUnionScan{Conditions:[col,cst]}`
/// twice (:72-73, :84-86) must leave each task's plan chain POINTER-identical —
/// union scan returns the child task itself when conditions are not constant
/// (Attach2Task's passthrough arm), asserted by comparing task.Plan()
/// identity down three levels including TableReader.TablePlans[0] (:76-79,
/// :89-91). The second attach onto a Projection-topped task behaves the same,
/// requiring the union scan's Self to be initialized (:85).
#[test]
#[ignore = "go-parity-gap: PhysicalUnionScan.Attach2Task + RootTask identity wiring not on this crate's physical tree yet"]
fn physical_union_scan_attach_keeps_child_plan_chain() {}
