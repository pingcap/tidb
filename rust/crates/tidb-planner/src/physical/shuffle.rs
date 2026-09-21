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

//! The shuffle boundary from `physicalop/physical_shuffle.go`.

use super::{BasePhysicalPlan, PhysicalPlan};
use crate::plan_base::PlanError;
use tidb_expr::expression::Expression;

/// Go `PartitionSplitterType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PartitionSplitterType {
    /// Partition by the hash of all key expressions.
    #[default]
    Hash,
    /// Distribute consecutive groups from an ordered source.
    Range,
}

/// Go stores pointers into the child tree. IDs retain those references without
/// duplicating owned Rust subtrees; the planner allocator makes IDs unique.
#[derive(Clone, Debug, Default)]
pub struct PhysicalShuffle {
    /// The worker template is the sole child.
    pub base: BasePhysicalPlan,
    /// Number of worker copies.
    pub concurrency: usize,
    /// Last operator of each worker input branch.
    pub tails: Vec<i32>,
    /// First operator below each tail, fetched once outside the workers.
    pub data_sources: Vec<i32>,
    /// Source partitioning algorithm.
    pub splitter_type: PartitionSplitterType,
    /// Partition expressions, in data-source order.
    pub by_item_arrays: Vec<Vec<Expression>>,
}

impl PhysicalShuffle {
    /// Resolve a boundary reference against the current owned child tree.
    pub fn referenced_plan(&self, id: i32) -> Result<&PhysicalPlan, PlanError> {
        let mut pending: Vec<_> = self.base.children().iter().collect();
        while let Some(plan) = pending.pop() {
            if plan.id() == id {
                return Ok(plan);
            }
            pending.extend(plan.children());
            if let PhysicalPlan::ShuffleReceiver(receiver) = plan {
                pending.push(&receiver.data_source);
            }
        }
        Err(PlanError::internal(format!(
            "shuffle boundary plan {id} is absent"
        )))
    }

    /// Go `PhysicalShuffle.ExplainInfo`.
    pub fn explain_info(&self, ignore_suffix: bool) -> Result<String, PlanError> {
        let sources = self
            .data_sources
            .iter()
            .map(|id| {
                self.referenced_plan(*id)
                    .map(|plan| plan.explain_id(ignore_suffix))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!(
            "execution info: concurrency:{}, data sources:[{}]",
            self.concurrency,
            sources.join(" ")
        ))
    }

    /// Owned shuffle metadata; the normal plan walk accounts for children.
    pub fn memory_usage(&self) -> i64 {
        self.base.base.memory_usage()
            + (std::mem::size_of::<Self>() - std::mem::size_of::<BasePhysicalPlan>()) as i64
            + ((self.tails.capacity() + self.data_sources.capacity()) * std::mem::size_of::<i32>())
                as i64
            + (self.by_item_arrays.capacity() * std::mem::size_of::<Vec<Expression>>()) as i64
            + self
                .by_item_arrays
                .iter()
                .map(|items| {
                    (items.capacity() * std::mem::size_of::<Expression>()) as i64
                        + items.iter().map(Expression::memory_usage).sum::<i64>()
                })
                .sum::<i64>()
    }

    pub(crate) fn resolve_by_items(&mut self) -> Result<(), PlanError> {
        for i in 0..self.by_item_arrays.len() {
            let source = *self
                .data_sources
                .get(i)
                .ok_or_else(|| PlanError::internal("shuffle source is absent"))?;
            let schema = self
                .referenced_plan(source)?
                .schema()
                .cloned()
                .ok_or_else(|| PlanError::internal("shuffle source schema is absent"))?;
            for expr in &mut self.by_item_arrays[i] {
                tidb_expr::simple_expr::resolve_indices_in_place(expr, &schema)
                    .map_err(|error| PlanError::internal(error.to_string()))?;
            }
        }
        Ok(())
    }
}

/// Go's PhysicalShuffleReceiverStub. The source remains a separate owned
/// reference, not a normal physical child. The worker-specific receiver is
/// supplied by the builder instead of storing Go's unsafe pointer in the plan.
#[derive(Clone, Debug)]
pub struct PhysicalShuffleReceiverStub {
    /// Schema, statistics and ID; the receiver has no normal child executors.
    pub base: BasePhysicalPlan,
    /// Go DataSource, rendered below the receiver but built outside its worker.
    pub data_source: Box<PhysicalPlan>,
}

/// Install the receiver boundaries after physical rewrites. IDs come from the
/// same statement allocator as the remaining plan; one stub is shared by all
/// worker copies for a given source, as in Go buildShuffle.
pub fn install_receivers(
    plan: &mut PhysicalPlan,
    allocator: &crate::plan_base::PlanIdAllocator,
) -> Result<(), PlanError> {
    if let PhysicalPlan::ShuffleReceiver(receiver) = plan {
        return install_receivers(&mut receiver.data_source, allocator);
    }
    for child in plan.base_mut().children_mut() {
        install_receivers(child, allocator)?;
    }
    let PhysicalPlan::Shuffle(shuffle) = plan else {
        return Ok(());
    };
    for (tail_id, source_id) in shuffle.tails.iter().zip(&shuffle.data_sources) {
        let tail = find_mut(shuffle.base.children_mut(), *tail_id)
            .ok_or_else(|| PlanError::internal("shuffle tail is absent"))?;
        if matches!(
            tail.children().first(),
            Some(PhysicalPlan::ShuffleReceiver(_))
        ) {
            continue;
        }
        if !contains(tail.children(), *source_id) {
            return Err(PlanError::internal(
                "shuffle source is absent from its tail",
            ));
        }
        // Go replaces the tail's children with the stub, keeping DataSource
        // alive through the stub's separate pointer. Move that source instead.
        let source = take_source(tail.base_mut().take_children(), *source_id)
            .expect("validated source under shuffle tail");
        let mut base =
            BasePhysicalPlan::new(allocator, "ShuffleReceiver", source.query_block_offset());
        base.base.set_schema(source.schema().cloned());
        base.base.set_stats(source.stats_info().cloned());
        base.set_children_req_props(vec![None]);
        tail.set_children(vec![PhysicalPlan::ShuffleReceiver(
            PhysicalShuffleReceiverStub {
                base,
                data_source: Box::new(source),
            },
        )]);
    }
    Ok(())
}

fn find_mut(plans: &mut [PhysicalPlan], id: i32) -> Option<&mut PhysicalPlan> {
    for plan in plans {
        if plan.id() == id {
            return Some(plan);
        }
        if let Some(found) = find_mut(plan.base_mut().children_mut(), id) {
            return Some(found);
        }
    }
    None
}

fn contains(plans: &[PhysicalPlan], id: i32) -> bool {
    plans
        .iter()
        .any(|plan| plan.id() == id || contains(plan.children(), id))
}

fn take_source(plans: Vec<PhysicalPlan>, id: i32) -> Option<PhysicalPlan> {
    for mut plan in plans {
        if plan.id() == id {
            return Some(plan);
        }
        if let Some(source) = take_source(plan.base_mut().take_children(), id) {
            return Some(source);
        }
    }
    None
}
