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

//! The enforcer: the Sort a property inserts when no child order satisfies it.
//!
//! Go source: `pkg/planner/core/operator/physicalop/enforce.go` (99 lines),
//! whole. Its three functions are `EnforceProperty` (here as
//! [`enforce_property`]) and `MppTask.EnforceExchanger` /
//! `EnforceExchangerImpl` (here as [`crate::task::MppTask::enforce_exchanger`]).
//!
//! This is the production body behind
//! [`crate::find_best_task::dispatch::find_best_task`]: `findBestTask`'s
//! enforcer branch prices `EnforceProperty(prop, task, ...)` against the
//! un-enforced candidates, and until this file the crate had only the seam.
//!
//! # Narrowings
//!
//! * `ctx.GetSessionVars().RaiseWarningWhenMPPEnforced(...)`
//!   (`enforce.go:37`): the session-vars warning sink is unported; the
//!   not-all-for-partition MPP arm returns the invalid task Go returns, and
//!   the warning text is not raised anywhere.

use crate::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalSort};
use crate::physical_property::PhysicalProperty;
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::task::{attach2_task, MppTask, Task};
use crate::task_type::TaskType;

impl MppTask {
    /// Go `MppTask.EnforceExchanger(prop, fd)` (`enforce.go:63`): insert an
    /// exchange pair above the task when the partition property demands one.
    pub fn enforce_exchanger(
        &self,
        required: &PhysicalProperty,
        allocator: &PlanIdAllocator,
    ) -> Result<MppTask, PlanError> {
        if !crate::physical_property::need_enforce_exchanger(
            self.partition_type(),
            self.hash_cols(),
            required,
            None,
        ) {
            return Ok(self.copy());
        }
        let child = self
            .plan()
            .ok_or_else(|| PlanError::internal("MppTask.EnforceExchanger: empty plan"))?
            .deep_clone();
        let exchange_type = match required.mpp_partition_tp.exchange_kind() {
            crate::physical_property::ExchangeKind::Broadcast => {
                crate::physical::ExchangeType::Broadcast
            }
            crate::physical_property::ExchangeKind::Hash => crate::physical::ExchangeType::Hash,
            crate::physical_property::ExchangeKind::PassThrough => {
                crate::physical::ExchangeType::PassThrough
            }
        };
        let mut sender_base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "ExchangeSender",
            child.query_block_offset(),
        );
        sender_base.base.set_stats(child.stats_info().cloned());
        sender_base.base.set_schema(child.schema().cloned());
        sender_base.set_children(vec![child]);
        let sender = PhysicalPlan::ExchangeSender(crate::physical::PhysicalExchangeSender {
            base: sender_base,
            exchange_type,
            hash_cols: required.mpp_partition_cols.clone(),
        });
        let mut receiver_base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "ExchangeReceiver",
            sender.query_block_offset(),
        );
        receiver_base.base.set_stats(sender.stats_info().cloned());
        receiver_base.base.set_schema(sender.schema().cloned());
        receiver_base.set_children(vec![sender]);
        let receiver = PhysicalPlan::ExchangeReceiver(crate::physical::PhysicalExchangeReceiver {
            base: receiver_base,
            tasks: Vec::new(),
        });
        let mut enforced = MppTask::new_with_hash_cols(
            receiver,
            required.mpp_partition_tp,
            required.mpp_partition_cols.clone(),
            [],
        );
        enforced.root_task_conds = self.root_task_conds.clone();
        enforced.warnings.copy_of(&self.warnings);
        Ok(enforced)
    }
}

/// Go `EnforceProperty(p, tsk, ctx, fd)` (`enforce.go:30`): the portal that
/// makes `tsk` satisfy `p`, by exchange (MPP) and/or an inserted Sort.
///
/// The body is Go's, in Go's order:
///
/// 1. an MPP property first checks the task IS a valid MPP task (else the
///    invalid task), then that the sort is partition-local (else Go warns —
///    narrowed, module header — and returns the invalid task), then runs
///    `EnforceExchanger`;
/// 2. an empty sort property or an invalid task returns the task unchanged;
/// 3. a non-MPP task converts to a root task ([`Task::convert_to_root_task`],
///    whose cop/MPP reader-building refusals apply);
/// 4. a `PhysicalSort` is built over the task plan's stats and query-block
///    offset, its child property `{RootTaskType, p.SortItems, MaxFloat64}`,
///    its `ByItems` copied from `p.SortItems`, `IsPartialSort` from
///    `p.IsSortItemAllForPartition()`, and attached via
///    [`attach2_task`]'s Sort arm.
pub fn enforce_property(
    prop: &PhysicalProperty,
    mut task: Task,
    allocator: &PlanIdAllocator,
) -> Result<Task, PlanError> {
    if prop.task_tp == TaskType::Mpp {
        let Task::Mpp(mpp) = &task else {
            return Ok(Task::invalid_task());
        };
        if task.invalid() {
            return Ok(Task::invalid_task());
        }
        if !prop.is_sort_item_all_for_partition() {
            // Go: RaiseWarningWhenMPPEnforced("MPP mode may be blocked
            // because operator `Sort` is not supported now.") — narrowed.
            return Ok(Task::invalid_task());
        }
        task = Task::Mpp(mpp.enforce_exchanger(prop, allocator)?);
    }
    if prop.is_sort_item_empty() || task.invalid() {
        return Ok(task);
    }
    if prop.task_tp != TaskType::Mpp {
        task = task.convert_to_root_task(allocator)?;
    }
    let sort_req_prop = PhysicalProperty {
        task_tp: TaskType::Root,
        sort_items: prop.sort_items.clone(),
        expected_cnt: f64::MAX,
        can_add_enforcer: false,
        mpp_partition_cols: Vec::new(),
        mpp_partition_tp: Default::default(),
        sort_items_for_partition: Vec::new(),
        cte_producer_status: prop.cte_producer_status,
        vector_prop: Default::default(),
        no_cop_push_down: prop.no_cop_push_down,
        advisory_sort_items: Vec::new(),
        index_join_prop: None,
        partial_order_info: None,
    };
    let child = task
        .plan()
        .ok_or_else(|| PlanError::internal("EnforceProperty: the task has no plan to sort"))?;
    let mut base = BasePhysicalPlan::new(allocator, "Sort", child.query_block_offset());
    base.base.set_stats(child.stats_info().cloned());
    base.set_children_req_props(vec![Some(sort_req_prop)]);
    // Go's `prop.SortItems` hold the complete `*expression.Column`, so the
    // executor can compile a `keyCmpFunc` from `col.GetType()`. This port
    // reduces a `SortItem` to a `UniqueID` while matching properties, so the
    // typed column has to be recovered from the child schema here, where the
    // Sort is materialized. Without it the executor sees a `Column` whose
    // `ret_type` is `None` and cannot build the compare function.
    let by_items = prop
        .sort_items
        .iter()
        .map(|item| {
            let column = child
                .schema()
                .and_then(|schema| schema.retrieve_column(&item.col))
                .cloned()
                .unwrap_or_else(|| item.col.clone());
            tidb_expr::aggregation::ByItems::new(
                tidb_expr::expression::Expression::Column(column),
                item.desc,
            )
        })
        .collect();
    let sort = PhysicalPlan::Sort(PhysicalSort {
        base,
        by_items,
        is_partial_sort: prop.is_sort_item_all_for_partition(),
    });
    attach2_task(sort, vec![task], None, allocator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{BasePhysicalPlan, ExchangeType, PhysicalPlan, PhysicalTableDual};
    use crate::physical_property::{MppPartitionColumn, MppPartitionType};
    use crate::stats_info::StatsInfo;

    fn mpp_task(allocator: &PlanIdAllocator) -> MppTask {
        let mut base = BasePhysicalPlan::new(allocator, "Dual", 0);
        base.base.set_stats(Some(StatsInfo::new(8.0, [])));
        MppTask::new(
            PhysicalPlan::TableDual(PhysicalTableDual { base, row_count: 8 }),
            MppPartitionType::Any,
            [],
        )
    }

    #[test]
    fn mpp_enforcer_builds_the_source_exchange_pair() {
        let allocator = PlanIdAllocator::new();
        let task = mpp_task(&allocator);
        let mut required = PhysicalProperty::default();
        required.task_tp = TaskType::Mpp;
        required.mpp_partition_tp = MppPartitionType::Hash;
        required.mpp_partition_cols = vec![MppPartitionColumn::new(1, 0)];

        let enforced = task
            .enforce_exchanger(&required, &allocator)
            .expect("hash partitioning inserts an exchange");
        assert_eq!(enforced.partition_type(), MppPartitionType::Hash);
        assert_eq!(enforced.hash_cols(), required.mpp_partition_cols.as_slice());
        let Some(PhysicalPlan::ExchangeReceiver(receiver)) = enforced.plan() else {
            panic!("the enforced MPP plan is an ExchangeReceiver");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = receiver.base.children().first() else {
            panic!("the receiver owns one ExchangeSender child");
        };
        assert_eq!(sender.exchange_type, ExchangeType::Hash);
        assert_eq!(sender.hash_cols, required.mpp_partition_cols);
    }

    #[test]
    fn mpp_enforcer_reuses_a_matching_partition_without_an_exchange() {
        let allocator = PlanIdAllocator::new();
        let hash_cols = vec![MppPartitionColumn::new(1, 0)];
        let task = MppTask::new_with_hash_cols(
            PhysicalPlan::TableDual(PhysicalTableDual {
                base: BasePhysicalPlan::new(&allocator, "Dual", 0),
                row_count: 1,
            }),
            MppPartitionType::Hash,
            hash_cols.clone(),
            [],
        );
        let mut required = PhysicalProperty::default();
        required.task_tp = TaskType::Mpp;
        required.mpp_partition_tp = MppPartitionType::Hash;
        required.mpp_partition_cols = hash_cols;
        let enforced = task
            .enforce_exchanger(&required, &allocator)
            .expect("matching hash partitioning needs no exchange");
        assert!(matches!(enforced.plan(), Some(PhysicalPlan::TableDual(_))));
    }
}
