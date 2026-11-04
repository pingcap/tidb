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

//! Go postOptimize `enableParallelApply`: an Apply whose inner child clones
//! (`SafeClone`) runs with the executor concurrency, keeping the outer order
//! its outer child requires; one that does not clone warns; inner Apply
//! descendants stay serial.
use tidb_planner::post_optimize::enable_parallel_apply;
use tidb_planner_core_operator_physicalop::*;

/// A leaf whose Go type clones.
fn cloneable() -> PhysicalPlan {
    PhysicalPlan::TableScan(Default::default())
}
fn apply(outer: PhysicalPlan, inner: PhysicalPlan, ordered: bool) -> PhysicalPlan {
    let mut base = BasePhysicalPlan::with_id(1, "Apply", 0);
    base.set_children(vec![outer, inner]);
    let outer_prop = tidb_planner_property::PhysicalProperty {
        sort_items: if ordered {
            vec![tidb_planner_property::SortItem::new(1, false)]
        } else {
            Vec::new()
        },
        ..Default::default()
    };
    base.set_children_req_props(vec![Some(outer_prop), None]);
    PhysicalPlan::Apply(PhysicalApply {
        hash_join: PhysicalHashJoin {
            base,
            inner_child_idx: 1,
            store_tp: Default::default(),
            mpp_shuffle_join: false,
            ..Default::default()
        },
        ..Default::default()
    })
}
fn concurrency(plan: &PhysicalPlan) -> usize {
    let PhysicalPlan::Apply(apply) = plan else {
        panic!("Apply");
    };
    apply.concurrency
}
#[test]
fn parallel_apply_only_recurses_into_the_outer_apply_child() {
    let ctx = tidb_sessionctx::StmtContext::default();
    let mut plan = apply(
        apply(cloneable(), cloneable(), false),
        apply(cloneable(), cloneable(), false),
        false,
    );
    enable_parallel_apply(&ctx, &mut plan);
    let concurrency_of = ctx.executor_concurrency();
    assert_eq!(concurrency(&plan), concurrency_of);
    assert_eq!(concurrency(&plan.children()[0]), concurrency_of);
    assert_eq!(concurrency(&plan.children()[1]), 0);
    assert!(ctx.take_warnings().is_empty());
}
#[test]
fn parallel_apply_keeps_the_order_its_outer_child_requires() {
    for ordered in [false, true] {
        let ctx = tidb_sessionctx::StmtContext::default();
        let mut plan = apply(PhysicalPlan::TableDual(Default::default()), cloneable(), ordered);
        enable_parallel_apply(&ctx, &mut plan);
        let PhysicalPlan::Apply(plan) = plan else {
            panic!("Apply");
        };
        assert_eq!(plan.concurrency, ctx.executor_concurrency());
        assert_eq!(plan.keep_order, ordered);
    }
}
#[test]
fn parallel_apply_warns_with_the_first_operator_that_does_not_clone() {
    for (inner, type_name) in [
        (PhysicalPlan::CTETable(Default::default()), "PhysicalCTETable"),
        (PhysicalPlan::TableDual(Default::default()), "PhysicalTableDual"),
    ] {
        let ctx = tidb_sessionctx::StmtContext::default();
        let mut plan = apply(cloneable(), inner, false);
        enable_parallel_apply(&ctx, &mut plan);
        assert_eq!(concurrency(&plan), 0);
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].level, "Warning");
        assert_eq!(
            warnings[0].err.to_string(),
            format!(
                "Some apply operators can not be executed in parallel: *physicalop.{type_name} doesn't support cloning"
            )
        );
    }
}
