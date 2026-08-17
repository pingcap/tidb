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

//! The physical plan tree.
//!
//! Go sources:
//! * `pkg/planner/core/base/plan_base.go` — the `PhysicalPlan` interface
//!   (lines 97-168) and `PhysicalJoin` (375-380).
//! * `pkg/planner/core/operator/physicalop/base_physical_plan.go` — the
//!   `BasePhysicalPlan` struct and its default bodies (lines 120-371).
//!
//! SEED of `pkg/planner/core`: the tree and its method surface land here; the
//! operator set does not. The closed-enum rationale is the same as
//! [`crate::logical`]'s and is argued there.
//!
//! # Narrowings, by name
//!
//! * `Attach2Task(...Task) Task`. `base.Task` (`pkg/planner/core/task.go`) is
//!   not transcreated. The method keeps its place in the surface as a `todo`
//!   rather than being typed against a placeholder task.
//! * `ToPB(ctx, storeType) (*tipb.Executor, error)`. The `tipb` executor
//!   protobuf is owned by `tidb-proto`, but the per-operator conversion is a
//!   later batch; Go's own base body is an error return
//!   (`"plan %s fails converts to PB"`), which is what this returns.
//! * `ResolveIndices`. The base body recurses into children and is ported;
//!   the per-operator column-index rewrite it wraps is a later batch.
//! * `probeParents` is carried, but `GetEstRowCountForDisplay` and
//!   `GetActualProbeCnt` need `utilfuncp.GetEstimatedProbeCntFromProbeParents`
//!   and the runtime stats collector, neither of which is here.
//! * `MemoryUsage` returns a source-shaped estimate over the Rust layout; see
//!   [`crate::plan_base`].

use tidb_expr::expression::CorrelatedColumn;
use tidb_expr::schema::Schema;

use crate::cost_usage::{CostVer2, PlanCostOption};
use crate::find_best_task::LogicalJoinType;
use crate::physical_property::{PhysicalProperty, TaskType};
use crate::physical_table_reader::StoreType;
use crate::plan_base::{BasePlan, PlanError, PlanIdAllocator};
use crate::stats_info::StatsInfo;

/// Go `physicalop.BasePhysicalPlan` (`base_physical_plan.go:120`).
///
/// The `Self base.PhysicalPlan` back-pointer is dropped for the same reason
/// as [`crate::logical::BaseLogicalPlan`]'s: an enum dispatches on its
/// variant.
#[derive(Clone, Debug, Default)]
pub struct BasePhysicalPlan {
    /// Go's embedded `baseimpl.Plan`.
    pub base: BasePlan,
    /// Go `children`.
    children: Vec<PhysicalPlan>,
    /// Go `childrenReqProps`, `plan-cache-clone:"shallow"`: the property each
    /// child was planned under.
    children_req_props: Vec<Option<PhysicalProperty>>,
    /// Go `PlanCostInit`.
    pub plan_cost_init: bool,
    /// Go `PlanCost`, the model-ver1 cost.
    pub plan_cost: f64,
    /// Go `PlanCostVer2`, `plan-cache-clone:"shallow"`.
    pub plan_cost_ver2: Option<CostVer2>,
    /// Go `probeParents`: the index joins and applies with this operator on
    /// their inner side, which is what makes a `StatsInfo` row count a
    /// per-probe rather than a total count.
    probe_parents: Vec<i32>,
    /// Go `TiFlashFineGrainedShuffleStreamCount`.
    pub tiflash_fine_grained_shuffle_stream_count: u64,
}

impl BasePhysicalPlan {
    /// Go `NewBasePhysicalPlan(ctx, tp, self, offset)` (`base_physical_plan.go:366`).
    #[must_use]
    pub fn new(
        allocator: &PlanIdAllocator,
        tp: impl Into<String>,
        query_block_offset: i32,
    ) -> Self {
        Self {
            base: BasePlan::new(allocator, tp, query_block_offset),
            ..Self::default()
        }
    }

    /// A base with a caller-chosen plan id; see [`BasePlan::with_id`].
    #[must_use]
    pub fn with_id(id: i32, tp: impl Into<String>, query_block_offset: i32) -> Self {
        Self {
            base: BasePlan::with_id(id, tp, query_block_offset),
            ..Self::default()
        }
    }

    /// Go `Children()` (`<7th>`).
    #[must_use]
    pub fn children(&self) -> &[PhysicalPlan] {
        &self.children
    }

    /// Mutable access to the child vector.
    pub const fn children_mut(&mut self) -> &mut Vec<PhysicalPlan> {
        &mut self.children
    }

    /// Go `SetChildren(...)` (`<8th>`).
    pub fn set_children(&mut self, children: Vec<PhysicalPlan>) {
        self.children = children;
    }

    /// Go `SetChild(i, child)` (`<9th>`); returns the replaced child rather
    /// than panicking on an out-of-range index.
    pub fn set_child(&mut self, i: usize, child: PhysicalPlan) -> Option<PhysicalPlan> {
        let slot = self.children.get_mut(i)?;
        Some(std::mem::replace(slot, child))
    }

    /// Takes the children, leaving the node childless.
    #[must_use]
    pub fn take_children(&mut self) -> Vec<PhysicalPlan> {
        std::mem::take(&mut self.children)
    }

    /// Go `GetChildReqProps(idx)` (`<4th>`).
    #[must_use]
    pub fn child_req_prop(&self, idx: usize) -> Option<&PhysicalProperty> {
        self.children_req_props.get(idx)?.as_ref()
    }

    /// Go `SetChildrenReqProps(reqProps)` (`base_physical_plan.go:356`).
    pub fn set_children_req_props(&mut self, props: Vec<Option<PhysicalProperty>>) {
        self.children_req_props = props;
    }

    /// Go `SetXthChildReqProps(x, reqProps)` (`base_physical_plan.go:361`).
    pub fn set_xth_child_req_props(&mut self, x: usize, prop: Option<PhysicalProperty>) {
        if let Some(slot) = self.children_req_props.get_mut(x) {
            *slot = prop;
        }
    }

    /// Go `SetProbeParents(probeParents)` (`<17th>`), carried as plan ids
    /// because the parents are aliases into a tree this crate owns by value.
    pub fn set_probe_parents(&mut self, probe_parents: Vec<i32>) {
        self.probe_parents = probe_parents;
    }

    /// The recorded probe-parent plan ids.
    #[must_use]
    pub fn probe_parents(&self) -> &[i32] {
        &self.probe_parents
    }

    /// Go `BasePhysicalPlan.ExplainInfo()`, which overrides `baseimpl.Plan`'s
    /// `"N/A"` with the empty string (`base_physical_plan.go:145`).
    #[must_use]
    pub const fn explain_info() -> &'static str {
        ""
    }

    /// Go `ExplainNormalizedInfo()` (`<13th>`), whose base body is empty.
    #[must_use]
    pub const fn explain_normalized_info() -> &'static str {
        ""
    }
}

/// Go `physicalop.PhysicalSelection`.
#[derive(Clone, Debug, Default)]
pub struct PhysicalSelection {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
}

/// Go `physicalop.PhysicalProjection`.
#[derive(Clone, Debug, Default)]
pub struct PhysicalProjection {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
}

/// Go `physicalop.PhysicalHashJoin`.
#[derive(Clone, Debug)]
pub struct PhysicalHashJoin {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `BasePhysicalJoin.JoinType`.
    pub join_type: LogicalJoinType,
    /// Go `BasePhysicalJoin.InnerChildIdx`.
    pub inner_child_idx: usize,
}

impl Default for PhysicalHashJoin {
    fn default() -> Self {
        Self {
            base: BasePhysicalPlan::default(),
            join_type: LogicalJoinType::Inner,
            inner_child_idx: 1,
        }
    }
}

/// Go `physicalop.PhysicalSort`.
#[derive(Clone, Debug, Default)]
pub struct PhysicalSort {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
}

/// Go `physicalop.PhysicalLimit`.
#[derive(Clone, Debug, Default)]
pub struct PhysicalLimit {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `Offset`.
    pub offset: u64,
    /// Go `Count`.
    pub count: u64,
}

/// Go `physicalop.PhysicalTableScan`.
#[derive(Clone, Debug, Default)]
pub struct PhysicalTableScan {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `Table.ID`.
    pub table_id: i64,
    /// Go `StoreType`: which engine serves this scan. `CopTask.GetStoreType`
    /// reads it at the leaf.
    pub store_type: crate::physical_table_reader::StoreType,
}

/// Go `physicalop.PhysicalTableDual`.
#[derive(Clone, Debug, Default)]
pub struct PhysicalTableDual {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `RowCount`.
    pub row_count: usize,
}

/// A physical operator whose own port is a later batch; the physical twin of
/// [`crate::logical::TodoLogicalOp`].
#[derive(Clone, Debug, Default)]
pub struct TodoPhysicalOp {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// The Go type this node stands in for, e.g. `"physicalop.PhysicalWindow"`.
    pub go_operator: String,
}

/// Go `base.PhysicalPlan`: a tree of physical operators.
#[derive(Clone, Debug)]
pub enum PhysicalPlan {
    /// Go `physicalop.PhysicalSelection`.
    Selection(PhysicalSelection),
    /// Go `physicalop.PhysicalProjection`.
    Projection(PhysicalProjection),
    /// Go `physicalop.PhysicalHashJoin`.
    HashJoin(PhysicalHashJoin),
    /// Go `physicalop.PhysicalSort`.
    Sort(PhysicalSort),
    /// Go `physicalop.PhysicalLimit`.
    Limit(PhysicalLimit),
    /// Go `physicalop.PhysicalTableScan`.
    TableScan(PhysicalTableScan),
    /// Go `physicalop.PhysicalTableDual`.
    TableDual(PhysicalTableDual),
    /// An operator whose port is a later batch; see [`TodoPhysicalOp`].
    Todo(TodoPhysicalOp),
}

impl PhysicalPlan {
    /// The shared physical base of whichever operator this is.
    #[must_use]
    pub const fn base(&self) -> &BasePhysicalPlan {
        match self {
            Self::Selection(op) => &op.base,
            Self::Projection(op) => &op.base,
            Self::HashJoin(op) => &op.base,
            Self::Sort(op) => &op.base,
            Self::Limit(op) => &op.base,
            Self::TableScan(op) => &op.base,
            Self::TableDual(op) => &op.base,
            Self::Todo(op) => &op.base,
        }
    }

    /// The shared physical base, mutably.
    pub const fn base_mut(&mut self) -> &mut BasePhysicalPlan {
        match self {
            Self::Selection(op) => &mut op.base,
            Self::Projection(op) => &mut op.base,
            Self::HashJoin(op) => &mut op.base,
            Self::Sort(op) => &mut op.base,
            Self::Limit(op) => &mut op.base,
            Self::TableScan(op) => &mut op.base,
            Self::TableDual(op) => &mut op.base,
            Self::Todo(op) => &mut op.base,
        }
    }

    // ***** base.Plan members, forwarded *****

    /// Go `Plan.ID()`.
    #[must_use]
    pub const fn id(&self) -> i32 {
        self.base().base.id()
    }

    /// Go `Plan.TP()`.
    #[must_use]
    pub fn tp(&self) -> &str {
        self.base().base.tp()
    }

    /// Go `Plan.ExplainID(...)`.
    #[must_use]
    pub fn explain_id(&self, ignore_suffix: bool) -> String {
        self.base().base.explain_id(ignore_suffix)
    }

    /// Go `Plan.QueryBlockOffset()`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.base().base.query_block_offset()
    }

    /// Go `BasePhysicalPlan.Schema()` (`base_physical_plan.go:150`): the
    /// operator's own schema, else the first child's. Go panics on a leaf
    /// with neither; this returns `None`.
    #[must_use]
    pub fn schema(&self) -> Option<&Schema> {
        if let Some(schema) = self.base().base.schema() {
            return Some(schema);
        }
        self.base().children().first().and_then(Self::schema)
    }

    /// Go `Plan.StatsInfo()` (`<11th>`, inherited from `baseimpl.Plan`).
    #[must_use]
    pub const fn stats_info(&self) -> Option<&StatsInfo> {
        self.base().base.stats_info()
    }

    /// Go `SetStats(s)` (`<12th>`).
    pub fn set_stats(&mut self, stats: Option<StatsInfo>) {
        self.base_mut().base.set_stats(stats);
    }

    // ***** base.PhysicalPlan members *****

    /// Go `GetPlanCostVer1(taskType, option)` (`<0th>`).
    ///
    /// The base body is dependency-closed and ported whole: the operator
    /// itself costs nothing, so the plan cost is the sum of the children's,
    /// computed once and cached unless `CostFlagRecalculate` is set.
    pub fn get_plan_cost_ver1(
        &mut self,
        task_type: TaskType,
        option: PlanCostOption,
        recalculate: bool,
    ) -> Result<f64, PlanError> {
        // Neither is read by the base body: the operator itself is free, so
        // the task type and the trace options only matter to the per-operator
        // overrides a later batch adds. They stay in the signature because Go
        // dispatches through it.
        let _ = (task_type, option);

        // Go recurses; this is the same computation over an explicit stack,
        // in two passes so the read of the children and the write of the
        // cache never borrow the tree at once. A cached node is a leaf for
        // both passes, exactly as Go's early return makes it.
        let costs = {
            let mut nodes: Vec<(&Self, Option<usize>)> = Vec::new();
            let mut stack: Vec<(&Self, Option<usize>)> = vec![(&*self, None)];
            while let Some((node, parent)) = stack.pop() {
                let index = nodes.len();
                nodes.push((node, parent));
                if node.base().plan_cost_init && !recalculate {
                    continue;
                }
                for child in node.children().iter().rev() {
                    stack.push((child, Some(index)));
                }
            }
            let mut costs = vec![0.0_f64; nodes.len()];
            for index in (0..nodes.len()).rev() {
                let (node, parent) = nodes[index];
                if node.base().plan_cost_init && !recalculate {
                    costs[index] = node.base().plan_cost;
                }
                if let Some(parent) = parent {
                    costs[parent] += costs[index];
                }
            }
            costs
        };

        let total = costs.first().copied().unwrap_or(0.0);
        let mut index = 0_usize;
        let mut stack: Vec<&mut Self> = vec![self];
        while let Some(node) = stack.pop() {
            let cached = node.base().plan_cost_init && !recalculate;
            if !cached {
                node.base_mut().plan_cost = costs[index];
                node.base_mut().plan_cost_init = true;
            }
            index += 1;
            if cached {
                continue;
            }
            for child in node.base_mut().children_mut().iter_mut().rev() {
                stack.push(child);
            }
        }
        Ok(total)
    }

    /// Go `GetPlanCostVer2(taskType, option, isChildOfINL...)` (`<1st>`).
    ///
    /// A `todo`: [`CostVer2`] has no public constructor or summation in this
    /// crate yet, and inventing one here would be a second cost model.
    pub fn get_plan_cost_ver2(
        &self,
        _task_type: TaskType,
        _option: PlanCostOption,
        _is_child_of_inl: bool,
    ) -> Result<CostVer2, PlanError> {
        Err(PlanError::internal(
            "todo: PhysicalPlan::get_plan_cost_ver2 needs costusage.SumCostVer2",
        ))
    }

    /// Go `Attach2Task(...Task) Task` (`<2nd>`). See the module header: the
    /// task layer is not transcreated.
    pub const fn attach2_task(&self) {}

    /// Go `ToPB(ctx, storeType)` (`<3rd>`). Go's base body is exactly this
    /// error, so the refusal is ported, not invented.
    pub fn to_pb(&self, _store_type: StoreType) -> Result<(), PlanError> {
        Err(PlanError::internal(format!(
            "plan {} fails converts to PB",
            self.explain_id(false)
        )))
    }

    /// Go `GetChildReqProps(idx)` (`<4th>`).
    #[must_use]
    pub fn child_req_prop(&self, idx: usize) -> Option<&PhysicalProperty> {
        self.base().child_req_prop(idx)
    }

    /// Go `StatsCount()` (`<5th>`): `StatsInfo().RowCount`. Go dereferences a
    /// possibly-nil pointer; `None` stands for that missing profile.
    #[must_use]
    pub fn stats_count(&self) -> Option<f64> {
        self.stats_info().map(StatsInfo::row_count)
    }

    /// Go `ExtractCorrelatedCols()` (`<6th>`). The base body returns `nil`.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        Vec::new()
    }

    /// Go `Children()` (`<7th>`).
    #[must_use]
    pub fn children(&self) -> &[Self] {
        self.base().children()
    }

    /// Go `SetChildren(...)` (`<8th>`).
    pub fn set_children(&mut self, children: Vec<Self>) {
        self.base_mut().set_children(children);
    }

    /// Go `SetChild(i, child)` (`<9th>`); returns the replaced child.
    pub fn set_child(&mut self, i: usize, child: Self) -> Option<Self> {
        self.base_mut().set_child(i, child)
    }

    /// Go `ResolveIndices()` (`<10th>`): resolve each child, depth first.
    ///
    /// The base body's recursion is ported with an explicit stack; the
    /// per-operator expression rewrite it wraps is a later batch.
    pub fn resolve_indices(&mut self) -> Result<(), PlanError> {
        let mut stack: Vec<&mut Self> = vec![self];
        while let Some(node) = stack.pop() {
            for child in node.base_mut().children_mut().iter_mut() {
                stack.push(child);
            }
        }
        Ok(())
    }

    /// Go `ExplainNormalizedInfo()` (`<13th>`).
    #[must_use]
    pub const fn explain_normalized_info(&self) -> &'static str {
        BasePhysicalPlan::explain_normalized_info()
    }

    /// Go `Clone(newCtx)` (`<14th>`).
    ///
    /// Go's base body REFUSES (`"%T doesn't support cloning"`) because a
    /// shallow copy of a physical plan aliases its children into two trees.
    /// Rust's `Clone` is a deep copy by construction, so the refusal has
    /// nothing to protect against and the derived clone is the answer; see
    /// the clone-cost note in [`crate::logical`].
    #[must_use]
    pub fn clone_plan(&self) -> Self {
        self.clone()
    }

    /// Go `MemoryUsage()` (`<16th>`): the node's own bytes plus the children's
    /// and the required properties'. Walked with an explicit stack.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let mut total = 0;
        let mut stack = vec![self];
        while let Some(node) = stack.pop() {
            total += node.base().base.memory_usage();
            for child in node.children() {
                stack.push(child);
            }
        }
        total
    }

    /// Go `SetProbeParents(...)` (`<17th>`).
    pub fn set_probe_parents(&mut self, probe_parents: Vec<i32>) {
        self.base_mut().set_probe_parents(probe_parents);
    }

    /// Go `GetEstRowCountForDisplay()` (`<18th>`): the StatsInfo row count
    /// scaled by the probe parents' estimated probe count.
    ///
    /// A `todo`: the scaling factor is
    /// `utilfuncp.GetEstimatedProbeCntFromProbeParents`, which reads the
    /// parents' own stats. With no probe parents the factor is 1, so that
    /// case is answered rather than deferred.
    #[must_use]
    pub fn est_row_count_for_display(&self) -> Option<f64> {
        if self.base().probe_parents().is_empty() {
            return self.stats_count();
        }
        None
    }

    /// Go `GetActualProbeCnt(statsColl)` (`<19th>`). The runtime stats
    /// collector is `pkg/util/execdetails`, outside this crate.
    #[must_use]
    pub fn actual_probe_cnt(&self) -> Option<i64> {
        if self.base().probe_parents().is_empty() {
            return Some(1);
        }
        None
    }

    /// Go `PhysicalJoin.GetJoinType()` (`plan_base.go:379`), answered only by
    /// the operators that implement `PhysicalJoin`. Go deliberately excludes
    /// `PhysicalApply` from that interface, and so does this.
    #[must_use]
    pub const fn join_type(&self) -> Option<LogicalJoinType> {
        match self {
            Self::HashJoin(join) => Some(join.join_type),
            _ => None,
        }
    }

    /// Go `PhysicalJoin.GetInnerChildIdx()` (`plan_base.go:378`).
    #[must_use]
    pub const fn inner_child_idx(&self) -> Option<usize> {
        match self {
            Self::HashJoin(join) => Some(join.inner_child_idx),
            _ => None,
        }
    }

    // ***** depth-safe tree utilities *****

    /// This node with its OWN fields copied and NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        fn base_of(base: &BasePhysicalPlan) -> BasePhysicalPlan {
            BasePhysicalPlan {
                base: base.base.clone(),
                children: Vec::new(),
                children_req_props: base.children_req_props.clone(),
                plan_cost_init: base.plan_cost_init,
                plan_cost: base.plan_cost,
                plan_cost_ver2: base.plan_cost_ver2.clone(),
                probe_parents: base.probe_parents.clone(),
                tiflash_fine_grained_shuffle_stream_count: base
                    .tiflash_fine_grained_shuffle_stream_count,
            }
        }
        match self {
            Self::Selection(op) => Self::Selection(PhysicalSelection {
                base: base_of(&op.base),
            }),
            Self::Projection(op) => Self::Projection(PhysicalProjection {
                base: base_of(&op.base),
            }),
            Self::HashJoin(op) => Self::HashJoin(PhysicalHashJoin {
                base: base_of(&op.base),
                join_type: op.join_type,
                inner_child_idx: op.inner_child_idx,
            }),
            Self::Sort(op) => Self::Sort(PhysicalSort {
                base: base_of(&op.base),
            }),
            Self::Limit(op) => Self::Limit(PhysicalLimit {
                base: base_of(&op.base),
                offset: op.offset,
                count: op.count,
            }),
            Self::TableScan(op) => Self::TableScan(PhysicalTableScan {
                base: base_of(&op.base),
                table_id: op.table_id,
                store_type: op.store_type,
            }),
            Self::TableDual(op) => Self::TableDual(PhysicalTableDual {
                base: base_of(&op.base),
                row_count: op.row_count,
            }),
            Self::Todo(op) => Self::Todo(TodoPhysicalOp {
                base: base_of(&op.base),
                go_operator: op.go_operator.clone(),
            }),
        }
    }

    /// A deep copy of this subtree, built without recursion; see
    /// [`crate::logical::LogicalPlan::deep_clone`]. This is what
    /// [`Self::clone_plan`] (Go `Clone`) should be reached through when the
    /// depth is not known to be small.
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        enum Step<'a> {
            Enter(&'a PhysicalPlan),
            Exit(&'a PhysicalPlan),
        }
        let mut work = vec![Step::Enter(self)];
        let mut done: Vec<Self> = Vec::new();
        while let Some(step) = work.pop() {
            match step {
                Step::Enter(node) => {
                    work.push(Step::Exit(node));
                    for child in node.children().iter().rev() {
                        work.push(Step::Enter(child));
                    }
                }
                Step::Exit(node) => {
                    let cloned_children = done.split_off(done.len() - node.children().len());
                    let mut copy = node.clone_shallow();
                    copy.set_children(cloned_children);
                    done.push(copy);
                }
            }
        }
        done.pop().unwrap_or_else(|| self.clone_shallow())
    }

    /// Visits every node in pre-order using an explicit stack.
    pub fn walk_preorder(&self, visitor: &mut impl FnMut(&Self)) {
        let mut stack = vec![self];
        while let Some(node) = stack.pop() {
            visitor(node);
            for child in node.children().iter().rev() {
                stack.push(child);
            }
        }
    }

    /// The number of nodes in this subtree, counted without recursion.
    #[must_use]
    pub fn plan_count(&self) -> usize {
        let mut count = 0;
        self.walk_preorder(&mut |_| count += 1);
        count
    }

    /// Tears the subtree down iteratively; see [`crate::logical::LogicalPlan::dismantle`].
    pub fn dismantle(mut self) {
        let mut stack = Vec::new();
        loop {
            stack.append(&mut self.base_mut().take_children());
            match stack.pop() {
                Some(next) => self = next,
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests;
