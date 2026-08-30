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

//! Go `pkg/planner/core/rule/rule_partition_processor.go`.

use crate::logical::{
    BaseLogicalPlan, DataSource, LogicalPartitionUnionAll, LogicalPlan, LogicalTableDual,
    LogicalUnionScan,
};
use crate::plan_base::PlanError;

use super::rule::{conds_to_table_dual, LogicalOptRule, RuleContext};
use super::rule_predicate_simplification::apply_predicate_simplification;

/// The ranger-backed half of Go's partition processor. The planner owns the
/// tree rewrite; the catalog implementation owns partition expressions and
/// therefore answers which definition ordinals survive.
pub trait PartitionPruning {
    /// Go `PartitionProcessor.prune`, reduced to its surviving definition
    /// ordinals in table-definition order.
    fn partition_indices(&self, source: &DataSource) -> Result<Vec<usize>, PlanError>;
}

fn make_children(ctx: &RuleContext<'_>, source: DataSource, indices: Vec<usize>) -> LogicalPlan {
    let schema = source.base.base.schema().cloned();
    let query_block_offset = source.base.base.query_block_offset();
    let mut children = Vec::with_capacity(indices.len());
    for index in indices {
        let Some(physical_id) = source.partition_definition_ids.get(index).copied() else {
            continue;
        };
        let mut child = source.clone_shallow();
        child.partition_def_idx = Some(index);
        child.physical_table_id = physical_id;
        children.push(LogicalPlan::DataSource(child));
    }
    match children.len() {
        0 => {
            let mut dual = LogicalTableDual::new(
                crate::logical::BaseLogicalPlan::new(
                    ctx.allocator,
                    LogicalTableDual::TYPE,
                    query_block_offset,
                ),
                0,
            );
            dual.base.base.set_schema(schema);
            LogicalPlan::TableDual(dual)
        }
        1 => children.pop().expect("one partition child"),
        _ => {
            let mut union = LogicalPartitionUnionAll::new(crate::logical::BaseLogicalPlan::new(
                ctx.allocator,
                LogicalPartitionUnionAll::TYPE,
                query_block_offset,
            ));
            union.union_all.base.base.set_schema(schema);
            union.union_all.base.set_children(children);
            LogicalPlan::PartitionUnionAll(union)
        }
    }
}

fn prune_data_source(
    ctx: &RuleContext<'_>,
    mut source: DataSource,
) -> Result<LogicalPlan, PlanError> {
    if source.partition_definition_ids.is_empty() {
        return Ok(LogicalPlan::DataSource(source));
    }
    source.pushed_down_conds = apply_predicate_simplification(ctx, source.pushed_down_conds, false);
    source.all_conds = apply_predicate_simplification(ctx, source.all_conds, false);
    if let Some(dual) = conds_to_table_dual(
        ctx,
        &source.all_conds,
        source.base.base.schema(),
        source.base.base.query_block_offset(),
    ) {
        return Ok(dual);
    }
    let indices = ctx.partition_pruning.map_or_else(
        || Ok((0..source.partition_definition_ids.len()).collect()),
        |pruning| pruning.partition_indices(&source),
    )?;
    if let Some(marker) = ctx.plan_cache_marker {
        marker.set_skip_plan_cache("Static partition pruning mode");
    }
    Ok(make_children(ctx, source, indices))
}

fn rewrite(ctx: &RuleContext<'_>, mut plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
    if let LogicalPlan::DataSource(source) = plan {
        return prune_data_source(ctx, source);
    }
    if let LogicalPlan::UnionScan(mut union_scan) = plan {
        let children = union_scan.base.take_children();
        if children.len() != 1 {
            let children = children
                .into_iter()
                .map(|child| rewrite(ctx, child))
                .collect::<Result<Vec<_>, _>>()?;
            union_scan.base.set_children(children);
            return Ok(LogicalPlan::UnionScan(union_scan));
        }
        let child = children.into_iter().next().expect("one UnionScan child");
        let LogicalPlan::DataSource(source) = child else {
            union_scan.base.set_children(vec![rewrite(ctx, child)?]);
            return Ok(LogicalPlan::UnionScan(union_scan));
        };
        let pruned = prune_data_source(ctx, source)?;
        if let LogicalPlan::PartitionUnionAll(mut partition_union) = pruned {
            let children = partition_union.union_all.base.take_children();
            let children = children
                .into_iter()
                .map(|child| {
                    let mut branch = LogicalUnionScan::new(
                        BaseLogicalPlan::new(
                            ctx.allocator,
                            LogicalUnionScan::TYPE,
                            partition_union.union_all.base.base.query_block_offset(),
                        ),
                        union_scan.handle_cols.clone(),
                    );
                    branch.conditions.clone_from(&union_scan.conditions);
                    branch.base.set_children(vec![child]);
                    LogicalPlan::UnionScan(branch)
                })
                .collect();
            partition_union.union_all.base.set_children(children);
            return Ok(LogicalPlan::PartitionUnionAll(partition_union));
        }
        union_scan.base.set_children(vec![pruned]);
        return Ok(LogicalPlan::UnionScan(union_scan));
    }
    // Go deliberately does not enter a CTE definition here.
    if matches!(plan, LogicalPlan::CTE(_)) {
        return Ok(plan);
    }
    let children = plan.base_mut().take_children();
    let children = children
        .into_iter()
        .map(|child| rewrite(ctx, child))
        .collect::<Result<Vec<_>, _>>()?;
    plan.set_children(children);
    Ok(plan)
}

/// Go `PartitionProcessor` at logical rule position 17.
#[derive(Debug)]
pub struct PartitionProcessor;

impl LogicalOptRule for PartitionProcessor {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        match rewrite(ctx, plan.clone()) {
            Ok(rewritten) => Ok((rewritten, false)),
            Err(error) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "partition_processor"
    }
}
