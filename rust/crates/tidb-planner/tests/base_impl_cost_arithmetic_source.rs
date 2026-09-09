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

//! Port for `pkg/planner/implementation/base_test.go:28
//! TestBaseImplementation` — item 1198 of `pkg/planner.part20` (all 1278
//! `Test*`/`Benchmark*` declarations under `pkg/planner/` on `origin/master`,
//! sorted by file then line, chunked by 60). The package's
//! `main_test.go:24 TestMain` (item 1199) is bootstrap-only and is recorded
//! as skipped-reason in the batch receipt.
//!
//! The Go test exercises `baseImpl` (`pkg/planner/implementation/base.go:
//! 27-70`): plan identity (`GetPlan` returns exactly the wrapped plan),
//! zero-cost initialization (`CalcCost` with no children sums nothing and
//! ignores the unused cost hint), and the Set/Get cost pair. This crate
//! models the cost state as
//! [`tidb_planner::implementation_cost::ImplementationCost`], whose module
//! doc records the deliberate boundary: physical-plan attachment
//! (`AttachChildren`/`GetPlan`) is not modeled because no memo/physical-plan
//! interface exists here. The cost assertions run for real below; the plan
//! identity assertion is the documentary twin.

use tidb_planner::implementation_cost::ImplementationCost;

/// Rust side of `pkg/planner/implementation/base_test.go:28
/// TestBaseImplementation` — the cost arithmetic the crate owns.
///
/// Go (against `base.go:27-42`): `impl.CalcCost(10, []memo.Implementation{}...)`
/// returns 0.0 — `baseImpl.CalcCost` ignores the cost hint and sums zero
/// children — and `impl.GetCost()` reads back 0.0; then `SetCost(6.0)` makes
/// `GetCost()` return exactly 6.0. `ImplementationCost::calc_cost(&[])`
/// is that same sum-over-no-children (the hint is not representable because
/// the source ignores it), `cost()` is `GetCost`, `set_cost` is `SetCost`.
#[test]
fn base_impl_cost_starts_at_zero_and_tracks_set_cost() {
    let mut impl_cost = ImplementationCost::new();

    // base_test.go:38-40: CalcCost over no children yields 0.0 and GetCost
    // reads the same; the leading `10` cost hint is discarded by the source.
    let cost = impl_cost.calc_cost(&[]);
    assert_eq!(cost, 0.0, "CalcCost with no children is 0.0");
    assert_eq!(impl_cost.cost(), 0.0, "GetCost reads the stored 0.0");

    // base_test.go:42-43: SetCost(6.0); GetCost() == 6.0.
    impl_cost.set_cost(6.0);
    assert_eq!(impl_cost.cost(), 6.0, "GetCost returns the SetCost value");
}

/// Documentary twin for the plan-identity assertion of
/// `pkg/planner/implementation/base_test.go:36-37`:
/// `require.Equal(t, p, impl.GetPlan())` — a `baseImpl` built around
/// `physicalop.PhysicalLimit{}.Init(sctx, nil, 0, nil)` must return exactly
/// that plan. `ImplementationCost` carries no plan (see its module-level
/// boundary), so the wrapped-plan identity is unobservable here.
///
/// go-parity-gap: the memo/Implementation layer (wrapped PhysicalPlan,
/// AttachChildren) is not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: baseImpl.GetPlan plan identity needs the memo/implementation layer"]
fn base_impl_get_plan_identity_documentary() {}
