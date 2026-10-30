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

//! Go `core/optimizer.go`'s `postOptimize` passes over the physical plan --
//! `eliminateUnionScanAndLock`, `mergeContinuousSelections`,
//! `enableParallelApply` -- and the physical half of
//! `rule_eliminate_projection.go` (`eliminatePhysicalProjection`).

use tidb_planner_core_operator_physicalop::*;

/// Replaces every `PhysicalUnionScan` by its child (the reader below already
/// merges the transaction's own rows, and a temporary table's).
#[must_use]
pub fn remove_union_scans(mut plan: PhysicalPlan) -> PhysicalPlan {
    while let PhysicalPlan::UnionScan(_) = &plan {
        let mut children = std::mem::take(plan.base_mut().children_mut());
        let Some(child) = children.pop() else {
            return plan;
        };
        plan = child;
    }
    let children = std::mem::take(plan.base_mut().children_mut());
    plan.base_mut()
        .set_children(children.into_iter().map(remove_union_scans).collect());
    if let PhysicalPlan::Dml(root) = &mut plan {
        if let Some(select) = root.select_plan.take() {
            root.select_plan = Some(Box::new(remove_union_scans(*select)));
        }
    }
    plan
}

/// Replaces every `PhysicalLock` by its child.
pub(crate) fn remove_locks(mut plan: PhysicalPlan) -> PhysicalPlan {
    while let PhysicalPlan::Lock(_) = &plan {
        let mut children = std::mem::take(plan.base_mut().children_mut());
        let Some(child) = children.pop() else {
            return plan;
        };
        plan = child;
    }
    let children = std::mem::take(plan.base_mut().children_mut());
    plan.base_mut()
        .set_children(children.into_iter().map(remove_locks).collect());
    plan
}

/// Marks the point get at the end of the single-child chain as locking.
pub(crate) fn set_point_get_lock(plan: &mut PhysicalPlan, lock: (tidb_planner_core_operator_logicalop::SelectLockType, u64)) {
    match plan {
        PhysicalPlan::PointGet(op) => {
            op.lock = true;
            op.select_lock = Some(lock);
        }
        PhysicalPlan::BatchPointGet(op) => {
            op.lock = true;
            op.select_lock = Some(lock);
        }
        other => {
            if other.children().len() <= 1 {
                for child in other.base_mut().children_mut() {
                    set_point_get_lock(child, lock);
                }
            }
        }
    }
}

/// Go `eliminateUnionScanAndLock` (`optimizer.go:1174`): when the
/// single-child chain from the root reaches a `Point_Get` or a
/// `Batch_Point_Get`, the `UnionScan` (a point get reads its own transaction's
/// rows itself) and the `SelectLock` above it disappear from the plan. The
/// point get takes the lock over when `lock_eligible` says the statement locks
/// (Go `getLockWaitTime`: inside a transaction, for a supported lock type);
/// otherwise the lock is simply dropped.
#[must_use]
pub fn eliminate_union_scan_over_point_get(plan: PhysicalPlan, lock_eligible: bool) -> PhysicalPlan {
    let mut has_point_get = false;
    let mut has_union_scan = false;
    let mut lock = None;
    let mut stack = vec![&plan];
    while let Some(node) = stack.pop() {
        // Go stops at a node with several children without looking at it.
        if node.children().len() > 1 {
            continue;
        }
        match node {
            PhysicalPlan::PointGet(_) | PhysicalPlan::BatchPointGet(_) => has_point_get = true,
            PhysicalPlan::UnionScan(_) => has_union_scan = true,
            PhysicalPlan::Lock(op) => lock = Some((op.lock_type, op.wait_sec)),
            _ => {}
        }
        stack.extend(node.children());
    }
    if !has_point_get || !(has_union_scan || lock.is_some()) {
        return plan;
    }
    let mut plan = plan;
    if let Some(lock) = lock.filter(|(lock_type, _)| {
        lock_eligible && *lock_type != tidb_planner_core_operator_logicalop::SelectLockType::None
    }) {
        set_point_get_lock(&mut plan, lock);
    }
    remove_locks(remove_union_scans(plan))
}

/// Go `eliminatePhysicalProjection`, the first physical post-optimization
/// pass. It removes only strict identity projections; computed expressions,
/// reordered columns, `CalculateNoDelay`, and schema-width changes remain.
#[must_use]
/// Go `mergeContinuousSelections` (`optimizer.go`, run by `postOptimize`
/// after `InjectExtraProjection`): a Selection whose child is a Selection
/// absorbs the child's conditions after its own and adopts the grandchild,
/// throughout the root tree and inside a TiFlash table reader's plan.
pub fn merge_continuous_selections(plan: &mut PhysicalPlan) {
    if let PhysicalPlan::Selection(selection) = plan {
        while matches!(
            selection.base.children().first(),
            Some(PhysicalPlan::Selection(_))
        ) {
            let mut children = selection.base.take_children();
            let PhysicalPlan::Selection(mut child) = children.remove(0) else {
                unreachable!("the first child was just matched as a Selection");
            };
            selection.conditions.append(&mut child.conditions);
            children.splice(0..0, child.base.take_children());
            selection.base.set_children(children);
        }
    }
    let mut children = plan.base_mut().take_children();
    for child in &mut children {
        merge_continuous_selections(child);
    }
    plan.base_mut().set_children(children);
    if let PhysicalPlan::TableReader(reader) = plan {
        if reader.store_type == tidb_kv::store_type::StoreType::TiFlash {
            if let Some(table_plan) = reader.table_plan.as_deref_mut() {
                merge_continuous_selections(table_plan);
            }
        }
    }
}

pub fn eliminate_physical_projection(plan: PhysicalPlan) -> PhysicalPlan {
    eliminate_physical_projection_for_update(plan, &std::collections::BTreeSet::new())
}

/// [`eliminate_physical_projection`] under Go's
/// `StmtCtx.ColRefFromUpdatePlan`: the columns an UPDATE's assignments read.
/// A projection that renames one of them is kept -- the assignments live
/// outside the plan and resolve against its output.
#[must_use]
pub fn eliminate_physical_projection_for_update(
    plan: PhysicalPlan,
    col_ref_from_update_plan: &std::collections::BTreeSet<i64>,
) -> PhysicalPlan {
    eliminate_projection_preserving_shuffle(
        plan,
        &mut std::collections::BTreeSet::new(),
        col_ref_from_update_plan,
    )
}

pub(crate) fn eliminate_projection_preserving_shuffle(
    mut plan: PhysicalPlan,
    retained: &mut std::collections::BTreeSet<i32>,
    col_ref_from_update_plan: &std::collections::BTreeSet<i64>,
) -> PhysicalPlan {
    if let PhysicalPlan::Shuffle(shuffle) = &plan {
        // Go's boundary pointers retain nodes even if projection elimination
        // removes them from ordinary child links. Keep these nodes owned until
        // the builder substitutes receivers, while still rewriting descendants.
        retained.extend(shuffle.data_sources.iter().copied());
        retained.extend(shuffle.tails.iter().copied());
    }
    let retain_node = retained.contains(&plan.id());
    let children = std::mem::take(plan.base_mut().children_mut());
    plan.base_mut().set_children(
        children
            .into_iter()
            .map(|child| {
                eliminate_projection_preserving_shuffle(child, retained, col_ref_from_update_plan)
            })
            .collect(),
    );

    if let PhysicalPlan::TableReader(reader) = &mut plan {
        if reader.store_type == tidb_kv::store_type::StoreType::TiFlash {
            if let Some(table_plan) = reader.table_plan.take() {
                reader.table_plan = Some(Box::new(eliminate_projection_preserving_shuffle(
                    *table_plan,
                    retained,
                    col_ref_from_update_plan,
                )));
            }
        }
    }

    let PhysicalPlan::Projection(projection) = &plan else {
        return plan;
    };
    if projection.calculate_no_delay {
        return plan;
    }
    // Go indexes `p.Children()[0]` inside `canProjectionBeEliminatedStrict`
    // (`rule_eliminate_projection.go:60`) and derefs both schemas
    // (`:77`/`:80-81`).
    let child = &projection.base.children()[0];
    let projection_schema = projection
        .base
        .base
        .schema()
        .cloned()
        .expect("physical projection elimination needs the projection schema");
    let strict_identity = projection_schema.is_empty() || {
        let child_schema = child
            .schema()
            .cloned()
            .expect("physical projection elimination needs the child schema");
        projection_schema.len() == child_schema.len()
            && projection.exprs.len() == projection_schema.len()
            && projection
                .exprs
                .iter()
                .zip(&child_schema.columns)
                .all(|(expression, column)| column.equal_column(expression))
    };
    if !strict_identity {
        return plan;
    }
    // Go `doPhysicalProjectionElimination`: a column an UPDATE assignment
    // reads, which the child carries under another identity, keeps the
    // projection.
    if !col_ref_from_update_plan.is_empty() {
        let child_schema = child.schema();
        let renames_update_column =
            projection_schema
                .columns
                .iter()
                .enumerate()
                .any(|(i, column)| {
                    col_ref_from_update_plan.contains(&column.unique_id)
                        && child_schema
                            .and_then(|schema| schema.columns.get(i))
                            .is_none_or(|child_column| child_column.unique_id != column.unique_id)
                });
        if renames_update_column {
            return plan;
        }
    }

    let PhysicalPlan::Projection(mut projection) = plan else {
        unreachable!("the projection arm was checked above")
    };
    let child = &mut projection.base.children_mut()[0];
    if !projection_schema.is_empty() && matches!(child, PhysicalPlan::Projection(_)) {
        child.base_mut().base.set_schema(Some(projection_schema));
    }
    if retain_node {
        return PhysicalPlan::Projection(projection);
    }
    projection.base.children_mut().remove(0)
}

/// Go postOptimize enableParallelApply: inner Apply descendants remain serial.
/// Shared recursive CTE/shuffle runtime state cannot be independently cloned
/// by the Rust builder yet, so those inner trees retain the serial fallback.
pub fn enable_parallel_apply(plan: &mut PhysicalPlan, concurrency: usize) {
    if let PhysicalPlan::Apply(apply) = plan {
        let outer = 1 - apply.hash_join.inner_child_idx;
        if outer > 1 {
            return;
        }
        let inner = 1 - outer;
        if plan
            .children()
            .get(inner)
            .is_some_and(supports_apply_worker_clone)
        {
            let PhysicalPlan::Apply(apply) = plan else {
                unreachable!()
            };
            apply.concurrency = concurrency;
        }
        if let Some(child) = plan.base_mut().children_mut().get_mut(outer) {
            enable_parallel_apply(child, concurrency);
        }
        return;
    }
    for child in plan.base_mut().children_mut() {
        enable_parallel_apply(child, concurrency);
    }
}
