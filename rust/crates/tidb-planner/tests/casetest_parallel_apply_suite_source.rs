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

//! Executable post-optimization regressions replacing empty ignored shells.
//! The three original Go casetest obligations (recursive hierarchy/full golden
//! plans/hinted warning matrices) remain tracked in the batch repair receipt;
//! these tests assert the common eligibility boundary, not full family parity.
use tidb_planner::physical::*;

fn leaf() -> PhysicalPlan {
    PhysicalPlan::TableDual(Default::default())
}
fn apply(outer: PhysicalPlan, inner: PhysicalPlan, ordered: bool) -> PhysicalPlan {
    let mut base = BasePhysicalPlan::with_id(1, "Apply", 0);
    base.set_children(vec![outer, inner]);
    PhysicalPlan::Apply(PhysicalApply {
        hash_join: PhysicalHashJoin {
            base,
            inner_child_idx: 1,
            ..Default::default()
        },
        keep_order: ordered,
        ..Default::default()
    })
}
#[test]
fn parallel_apply_only_recurses_into_the_outer_apply_child() {
    let mut plan = apply(
        apply(leaf(), leaf(), false),
        apply(leaf(), leaf(), false),
        false,
    );
    enable_parallel_apply(&mut plan, 5);
    let PhysicalPlan::Apply(root) = &plan else {
        panic!("Apply");
    };
    assert_eq!(root.concurrency, 5);
    let PhysicalPlan::Apply(outer) = &plan.children()[0] else {
        panic!("outer Apply");
    };
    let PhysicalPlan::Apply(inner) = &plan.children()[1] else {
        panic!("inner Apply");
    };
    assert_eq!(outer.concurrency, 5);
    assert_eq!(inner.concurrency, 0);
}
#[test]
fn parallel_apply_keeps_the_existing_outer_order_contract() {
    for ordered in [false, true] {
        let mut plan = apply(leaf(), leaf(), ordered);
        enable_parallel_apply(&mut plan, 4);
        let PhysicalPlan::Apply(plan) = plan else {
            panic!("Apply");
        };
        assert_eq!(plan.concurrency, 4);
        assert_eq!(plan.keep_order, ordered);
    }
}
#[test]
fn parallel_apply_falls_back_for_shared_cte_runtime_ownership() {
    let mut plan = apply(leaf(), PhysicalPlan::CTETable(Default::default()), false);
    enable_parallel_apply(&mut plan, 4);
    let PhysicalPlan::Apply(plan) = plan else {
        panic!("Apply");
    };
    assert_eq!(plan.concurrency, 0);
}
