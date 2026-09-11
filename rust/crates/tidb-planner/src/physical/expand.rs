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

//! Physical grouping-level projections, corresponding to Go PhysicalExpand.

use super::{BasePhysicalPlan, PhysicalPlan};
use crate::logical::LogicalExpand;
use crate::physical_property::{MppPartitionType, PhysicalProperty, TaskType};
use crate::plan_base::PlanIdAllocator;
use tidb_expr::expression::Expression;

/// Expand's output is one projection per grouping level per input batch.
#[derive(Clone, Debug)]
pub struct PhysicalExpand {
    /// Shared physical operator state.
    pub base: BasePhysicalPlan,
    /// Expressions for each grouping level.
    pub level_exprs: Vec<Vec<Expression>>,
    /// Names of generated grouping identifier columns.
    pub extra_grouping_col_names: Vec<String>,
}

/// Go ExhaustPhysicalPlans4LogicalExpand, including child task alternatives.
pub fn exhaust(
    expand: &LogicalExpand,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
    mpp_allowed: bool,
) -> Vec<PhysicalPlan> {
    if !prop.sort_items.is_empty()
        || !matches!(prop.task_tp, TaskType::Root | TaskType::Mpp)
        || (prop.task_tp == TaskType::Mpp && prop.mpp_partition_tp != MppPartitionType::Any)
    {
        return vec![];
    }
    let mut plans = Vec::new();
    if mpp_allowed {
        plans.push(candidate(
            expand,
            prop,
            allocator,
            skew_ratio,
            TaskType::Mpp,
        ));
        if prop.task_tp == TaskType::Mpp {
            return plans;
        }
    }
    for task in [
        TaskType::CopSingleRead,
        TaskType::CopMultiRead,
        TaskType::Mpp,
        TaskType::Root,
    ] {
        plans.push(candidate(expand, prop, allocator, skew_ratio, task));
    }
    plans
}

fn candidate(
    expand: &LogicalExpand,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
    task: TaskType,
) -> PhysicalPlan {
    let mut base =
        BasePhysicalPlan::new(allocator, "Expand", expand.base.base.query_block_offset());
    base.base.set_schema(expand.base.base.schema().cloned());
    base.base.set_stats(
        expand
            .base
            .base
            .stats_info()
            .map(|s| s.scale_by_expect_cnt(prop.expected_cnt, skew_ratio)),
    );
    let mut child_prop = prop.clone_essential_fields();
    child_prop.task_tp = task;
    base.set_children_req_props(vec![Some(child_prop)]);
    PhysicalPlan::Expand(PhysicalExpand {
        base,
        level_exprs: expand
            .level_exprs
            .clone()
            .expect("ResolveExpand generates grouping levels"),
        extra_grouping_col_names: expand.extra_grouping_col_names.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::BaseLogicalPlan;

    #[test]
    fn expand_candidates_match_source_task_requirements() {
        let mut expand = LogicalExpand::new(BaseLogicalPlan::with_id(1, "Expand", 0));
        expand.level_exprs = Some(vec![vec![]]);
        let allocator = PlanIdAllocator::default();
        let mut prop = PhysicalProperty::default();
        let plans = exhaust(&expand, &prop, &allocator, 1.0, false);
        let tasks: Vec<_> = plans
            .iter()
            .map(|plan| plan.base().child_req_prop(0).unwrap().task_tp)
            .collect();
        assert_eq!(
            tasks,
            [
                TaskType::CopSingleRead,
                TaskType::CopMultiRead,
                TaskType::Mpp,
                TaskType::Root
            ]
        );
        assert_eq!(exhaust(&expand, &prop, &allocator, 1.0, true).len(), 5);
        prop.task_tp = TaskType::Mpp;
        assert_eq!(exhaust(&expand, &prop, &allocator, 1.0, true).len(), 1);
        prop.mpp_partition_tp = MppPartitionType::Hash;
        assert!(exhaust(&expand, &prop, &allocator, 1.0, true).is_empty());
        prop.task_tp = TaskType::CopSingleRead;
        assert!(exhaust(&expand, &prop, &allocator, 1.0, true).is_empty());
    }
}
