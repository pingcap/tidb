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
use crate::task::{RootTask, Task};

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

/// Go `physicalop.PhysicalSelection` (`physical_selection.go`, whole file):
/// a filter.
#[derive(Clone, Debug, Default)]
pub struct PhysicalSelection {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `Conditions`: the CNF conjuncts this filter applies.
    pub conditions: Vec<tidb_expr::expression::Expression>,
    /// Go `FromDataSource`: "whether this Selection is from a DataSource",
    /// read only by the cost model for compatibility (Go names issue
    /// #36243 for its planned removal).
    pub from_data_source: bool,
}

/// Go `ExhaustPhysicalPlans4LogicalSelection` (`physical_selection.go:54`):
/// one root-side Selection per admitted child property, its stats scaled to
/// the parent's expected count.
///
/// Go builds up to TWO child properties. The second is the MPP property,
/// admitted only when `canPushDownToTiFlash` — a guard over TiFlash
/// replicas (`GetHasTiFlash`), virtual columns, and
/// `expression.CanExprsPushDown`. With no TiFlash tier in this port that
/// guard evaluates false exactly as it does on a TiFlash-less Go cluster,
/// so the MPP branch is structurally absent rather than refused.
/// `admitIndexJoinProps` narrows the same way: with no index-join property
/// on the ported [`PhysicalProperty`], Go's function returns the property
/// list unchanged, which is this body.
#[must_use]
pub fn exhaust_physical_plans_4_logical_selection(
    p: &crate::logical::LogicalSelection,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
) -> Vec<PhysicalPlan> {
    let child_prop = prop.clone_essential_fields();
    let stats = p
        .base
        .base
        .stats_info()
        .map(|stats| stats.scale_by_expect_cnt(prop.expected_cnt, skew_ratio));
    let mut base = BasePhysicalPlan::new(
        allocator,
        crate::logical::LogicalSelection::TYPE,
        p.base.base.query_block_offset(),
    );
    base.base.set_stats(stats);
    base.set_children_req_props(vec![Some(child_prop)]);
    vec![PhysicalPlan::Selection(PhysicalSelection {
        base,
        conditions: p.conditions.clone(),
        from_data_source: false,
    })]
}

/// Go `physicalop.PhysicalProjection` (`physical_projection.go`, whole
/// file).
#[derive(Clone, Debug, Default)]
pub struct PhysicalProjection {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `Exprs`: one expression per output column, in schema order.
    pub exprs: Vec<tidb_expr::expression::Expression>,
    /// Go `CalculateNoDelay`.
    pub calculate_no_delay: bool,
    /// Go `AvoidColumnEvaluator`, "ONLY used to avoid building
    /// columnEvaluator for the expressions of Projection which is child of
    /// Union operator" (issue #8141).
    pub avoid_column_evaluator: bool,
}

/// Go `ExhaustPhysicalPlans4LogicalProjection` (`physical_projection.go:49`):
/// one root-side Projection per admitted child property, its stats scaled
/// to the parent's expected count and its schema the logical projection's.
///
/// `TryToGetChildProp` decides admission: a required order that runs
/// through a computed expression cannot cross a projection, and the
/// enumeration returns empty. Go's MPP and cop-pushdown candidates narrow
/// away by name: both are gated on `expression.CanExprsPushDown`, which is
/// unported — the TiFlash guard also evaluates false with no TiFlash tier,
/// exactly as on a TiFlash-less Go cluster, but the TiKV cop candidate is
/// a genuine narrowing (this enumeration offers FEWER candidates than Go's
/// when projection pushdown is allowed). `admitIndexJoinProps` narrows as
/// in [`exhaust_physical_plans_4_logical_selection`].
#[must_use]
pub fn exhaust_physical_plans_4_logical_projection(
    p: &crate::logical::LogicalProjection,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
) -> Vec<PhysicalPlan> {
    let Some(child_prop) = p.try_to_get_child_prop(prop) else {
        return Vec::new();
    };
    let stats = p
        .base
        .base
        .stats_info()
        .map(|stats| stats.scale_by_expect_cnt(prop.expected_cnt, skew_ratio));
    let mut base = BasePhysicalPlan::new(
        allocator,
        crate::logical::LogicalProjection::TYPE,
        p.base.base.query_block_offset(),
    );
    base.base.set_stats(stats);
    base.base.set_schema(p.base.base.schema().cloned());
    base.set_children_req_props(vec![Some(child_prop)]);
    vec![PhysicalPlan::Projection(PhysicalProjection {
        base,
        exprs: p.exprs.clone(),
        calculate_no_delay: p.calculate_no_delay,
        avoid_column_evaluator: false,
    })]
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
    /// Go `ByItems` (`util.ByItems{Expr, Desc}`). `EnforceProperty` builds
    /// each entry from a `property.SortItem`'s column and direction, and a
    /// column's identity is its `UniqueID` (`EqualColumn`), so the item is
    /// carried here as exactly that pair.
    pub by_items: Vec<crate::physical_property::SortItem>,
    /// Go `IsPartialSort`: sort within one partition's data rather than
    /// globally; `EnforceProperty` sets it from
    /// `prop.IsSortItemAllForPartition()`.
    pub is_partial_sort: bool,
}

/// Go `physicalop.PhysicalLimit` (`physical_limit.go`, whole file).
///
/// `ExplainInfo`'s partition-by rendering and `ToPB` follow the enum's
/// standing narrowings; `attach2Task4PhysicalLimit`'s root arm lives in
/// [`crate::task::attach2_task`].
#[derive(Clone, Debug, Default)]
pub struct PhysicalLimit {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `PartitionBy`: the enhanced-TopN partition order.
    pub partition_by: Vec<crate::physical_property::SortItem>,
    /// Go `Offset`.
    pub offset: u64,
    /// Go `Count`.
    pub count: u64,
    /// Go `PrefixCol`, the prefix-index column for partial-order
    /// optimization, by its `UniqueID`; `None` when unused.
    pub prefix_col: Option<i64>,
    /// Go `PrefixLen`, the prefix length in bytes; 0 when unused.
    pub prefix_len: usize,
}

/// Go `ExhaustPhysicalPlans4LogicalLimit` (`physical_limit.go:53`): a limit
/// admits no required order; otherwise it enumerates ONE candidate per task
/// type — cop-single, cop-multi, root, in Go's fixed order — each with the
/// child property `{TaskTp, ExpectedCnt: Count + Offset}` and the logical
/// limit's stats, schema and partition order.
///
/// Go appends an MPP candidate when TiFlash is present and MPP allowed;
/// with no TiFlash tier that guard evaluates false exactly as on a
/// TiFlash-less Go cluster. `CTEProducerStatus`/`NoCopPushDown` narrow with
/// the unported property fields.
#[must_use]
pub fn exhaust_physical_plans_4_logical_limit(
    p: &crate::logical::LogicalLimit,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
) -> Vec<PhysicalPlan> {
    if !prop.is_sort_item_empty() {
        return Vec::new();
    }
    let all_task_types = [
        TaskType::CopSingleRead,
        TaskType::CopMultiRead,
        TaskType::Root,
    ];
    let mut ret = Vec::with_capacity(all_task_types.len());
    for tp in all_task_types {
        let result_prop = PhysicalProperty {
            task_tp: tp,
            expected_cnt: (p.count + p.offset) as f64,
            ..PhysicalProperty::default()
        };
        let mut base = BasePhysicalPlan::new(
            allocator,
            crate::logical::LogicalLimit::TYPE,
            p.base.base.query_block_offset(),
        );
        base.base.set_stats(p.base.base.stats_info().cloned());
        base.base.set_schema(p.base.base.schema().cloned());
        base.set_children_req_props(vec![Some(result_prop)]);
        ret.push(PhysicalPlan::Limit(PhysicalLimit {
            base,
            partition_by: p.partition_by.clone(),
            offset: p.offset,
            count: p.count,
            prefix_col: None,
            prefix_len: 0,
        }));
    }
    ret
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
    /// Go `KeepOrder`: the scan delivers handle order.
    pub keep_order: bool,
    /// Go `Desc`: the scan runs backward.
    pub desc: bool,
}

/// Go `physicalop.PhysicalTableDual` (`physical_table_dual.go`, whole
/// file): the physical operator of dual — `RowCount` rows (0 or 1) from no
/// table. Go's private `names` field ("Dual may be inited when building
/// point get plan. So it needs to hold names for itself") is this port's
/// [`crate::plan_base::BasePlan`] `output_names`, which every operator
/// already carries — `OutputNames`/`SetOutputNames` are its accessors, not
/// a second field. `MemoryUsage` follows the enum's standing narrowing.
#[derive(Clone, Debug, Default)]
pub struct PhysicalTableDual {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `RowCount`.
    pub row_count: usize,
}

impl PhysicalTableDual {
    /// Go `PhysicalTableDual.ExplainInfo()`: `rows:N`, overriding the base
    /// body's empty string.
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!("rows:{}", self.row_count)
    }
}

/// Go `findBestTask4LogicalTableDual` (`physical_table_dual.go:79`): a dual
/// is born directly inside its own root task, never priced or attached.
///
/// The body is Go's: a required order over more than one row cannot be
/// promised, so it answers the invalid task; a 0- or 1-row dual satisfies
/// any order vacuously. The built plan carries the logical dual's stats,
/// query-block offset, schema, and output names.
///
/// # Narrowings
///
/// * `prop.IndexJoinProp != nil` returns the invalid task in Go ("even
///   enforce hint can not work with this"); the index-join runtime property
///   is unported ([`crate::task`] module header names it), so this port's
///   property never carries one and the arm has no input to fire on.
/// * `base.GetGEAndLogicalOp` is cascades-enumeration plumbing for reading
///   the operator out of a group expression; the operator arrives here
///   directly.
///
/// `findBestTask4LogicalMockDatasource` (same file) is NOT ported:
/// `logicalop.MockDataSource` is benchmark scaffolding this crate's logical
/// enum does not carry.
#[must_use]
pub fn find_best_task_4_logical_table_dual(
    p: &crate::logical::LogicalTableDual,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
) -> Task {
    if !prop.is_sort_item_empty() && p.row_count > 1 {
        return Task::invalid_task();
    }
    let mut base = BasePhysicalPlan::new(
        allocator,
        crate::logical::LogicalTableDual::TYPE,
        p.base.base.query_block_offset(),
    );
    base.base.set_stats(p.base.base.stats_info().cloned());
    base.base.set_schema(p.base.base.schema().cloned());
    base.base
        .set_output_names(p.base.base.output_names().to_vec());
    let dual = PhysicalPlan::TableDual(PhysicalTableDual {
        base,
        row_count: p.row_count,
    });
    let mut root = RootTask::default();
    root.set_plan(dual);
    Task::Root(root)
}

/// Go `physicalop.PhysicalMaxOneRow` (`physical_max_one_row.go`, whole
/// file): the physical operator of maxOneRow — assert at most one child row.
/// The struct is exactly its base; the file's other bodies land as
/// [`exhaust_physical_plans_4_logical_max_one_row`] and the default
/// `Attach2Task` arm in [`crate::task::attach2_task`]. `Clone` and
/// `MemoryUsage` follow the enum's standing narrowings (the copy impl and
/// no memory accounting).
#[derive(Clone, Debug, Default)]
pub struct PhysicalMaxOneRow {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
}

/// Go `physicalop.NominalSort` (`nominal_sort.go`, whole file): "a fake
/// operator that will not appear in final physical operator tree. It will be
/// eliminated or converted to Projection." Its `Init` stamps
/// `plancodec.TypeSort`, so its explain name is `Sort`.
///
/// `ResolveIndices` (`resolveIndicesForSort`) rewrites `ByItems` expressions
/// against the child schema; the enum world carries a by-item as the
/// column's `UniqueID`, which needs no resolution — the narrowing the
/// enum's [`PhysicalSort`] already made.
#[derive(Clone, Debug, Default)]
pub struct NominalSort {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `ByItems`, kept (issue #11653) so the NominalSorts that convert to
    /// Projections can check whether their scalar functions are out of
    /// bounds; carried as `SortItem`s like [`PhysicalSort::by_items`].
    pub by_items: Vec<crate::physical_property::SortItem>,
    /// Go `OnlyColumn`: every by-item is a bare column, and `Attach2Task`
    /// then drops the operator entirely.
    pub only_column: bool,
}

/// Go `ExhaustPhysicalPlans4LogicalMaxOneRow` (`physical_max_one_row.go:59`):
/// a `MaxOneRow` admits no required order and no TiFlash property; otherwise
/// it enumerates exactly one `PhysicalMaxOneRow` whose child property caps
/// `ExpectedCnt` at 2 — one row to keep, one to prove the violation.
///
/// Go's second return value is `true` (enumeration complete) in both arms
/// and its error is always nil, so the return narrows to the plan list.
/// Narrowed with it: `RaiseWarningWhenMPPEnforced` on the refusing arm (no
/// session-vars warning sink) and the `CTEProducerStatus` /
/// `NoCopPushDown` child-property fields (unported on
/// [`PhysicalProperty`]).
#[must_use]
pub fn exhaust_physical_plans_4_logical_max_one_row(
    p: &crate::logical::LogicalMaxOneRow,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
) -> Vec<PhysicalPlan> {
    if !prop.is_sort_item_empty() || prop.task_tp == crate::task_type::TaskType::Mpp {
        return Vec::new();
    }
    let mut base = BasePhysicalPlan::new(
        allocator,
        crate::logical::LogicalMaxOneRow::TYPE,
        p.base.base.query_block_offset(),
    );
    base.base.set_stats(p.base.base.stats_info().cloned());
    base.set_children_req_props(vec![Some(PhysicalProperty {
        expected_cnt: 2.0,
        ..PhysicalProperty::default()
    })]);
    vec![PhysicalPlan::MaxOneRow(PhysicalMaxOneRow { base })]
}

/// Go `physicalop.PhysicalCTETable` (`physical_cte_table.go`, whole file):
/// the reader of one CTE's storage inside its recursive part. Its Go `Init`
/// is the odd one out — `baseimpl.NewBasePlan(ctx, TypeCTETable, 0)`
/// directly, always query-block offset 0 and no child properties.
/// `MemoryUsage` follows the enum's standing narrowing.
#[derive(Clone, Debug, Default)]
pub struct PhysicalCTETable {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `IDForStorage`: which CTE storage this table reads.
    pub id_for_storage: i32,
}

impl PhysicalCTETable {
    /// Go `PhysicalCTETable.ExplainInfo()`: `Scan on CTE_N`.
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!("Scan on CTE_{}", self.id_for_storage)
    }
}

/// Go `findBestTask4LogicalCTETable` (`physical_cte_table.go:56`): like the
/// dual, a CTE table is born directly inside its own root task — but unlike
/// the dual it can promise NO order at all, so ANY required sort answers
/// the invalid task. The built plan carries the logical table's stats,
/// schema, and `IDForStorage`, at Go's fixed query-block offset 0.
///
/// The `prop.IndexJoinProp != nil` arm and `base.GetGEAndLogicalOp` narrow
/// exactly as [`find_best_task_4_logical_table_dual`]'s do.
#[must_use]
pub fn find_best_task_4_logical_cte_table(
    p: &crate::logical::LogicalCTETable,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
) -> Task {
    if !prop.is_sort_item_empty() {
        return Task::invalid_task();
    }
    let mut base = BasePhysicalPlan::new(allocator, crate::logical::LogicalCTETable::TYPE, 0);
    base.base.set_stats(p.base.base.stats_info().cloned());
    base.base.set_schema(p.base.base.schema().cloned());
    let cte_table = PhysicalPlan::CTETable(PhysicalCTETable {
        base,
        id_for_storage: p.id_for_storage,
    });
    let mut root = RootTask::default();
    root.set_plan(cte_table);
    Task::Root(root)
}

/// Go `physicalop.PhysicalShow` (`physical_show.go`, whole file with
/// [`PhysicalShowDDLJobs`]): the physical form of a `SHOW ...` statement.
/// Its Go `Init` pins two quirks reproduced here: query-block offset 0, and
/// pseudo stats of exactly one row — "Just use pseudo stats to avoid
/// panic." `Extractor base.ShowPredicateExtractor` narrows to the one form
/// [`crate::logical::LogicalShow`] installs. `MemoryUsage` follows the
/// enum's standing narrowing.
#[derive(Clone, Debug, Default)]
pub struct PhysicalShow {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go's embedded `logicalop.ShowContents`.
    pub contents: crate::logical::ShowContents,
    /// Go `Extractor`, in the one ported form.
    pub extractor: Option<crate::logical::ShowStatsMetaPredicateExtractor>,
}

/// Go `physicalop.PhysicalShowDDLJobs` (`physical_show.go:53`): the
/// physical form of `ADMIN SHOW DDL JOBS`. Same offset-0 and one-row
/// pseudo-stats quirks as [`PhysicalShow`].
#[derive(Clone, Debug, Default)]
pub struct PhysicalShowDDLJobs {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `JobNumber`.
    pub job_number: i64,
}

/// Go `findBestTask4LogicalShow` (`physical_show.go:91`): a show is born
/// directly inside its own root task — any required sort answers the
/// invalid task, and the built plan carries the logical show's contents,
/// extractor and schema over Go's fixed one-row pseudo stats.
///
/// The `prop.IndexJoinProp != nil` arm and `base.GetGEAndLogicalOp` narrow
/// exactly as [`find_best_task_4_logical_table_dual`]'s do.
#[must_use]
pub fn find_best_task_4_logical_show(
    p: &crate::logical::LogicalShow,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
) -> Task {
    if !prop.is_sort_item_empty() {
        return Task::invalid_task();
    }
    let mut base = BasePhysicalPlan::new(allocator, crate::logical::LogicalShow::TYPE, 0);
    base.base.set_stats(Some(StatsInfo::new(1.0, [])));
    base.base.set_schema(p.base.base.schema().cloned());
    let show = PhysicalPlan::Show(PhysicalShow {
        base,
        contents: p.contents.clone(),
        extractor: p.extractor.clone(),
    });
    let mut root = RootTask::default();
    root.set_plan(show);
    Task::Root(root)
}

/// Go `findBestTask4LogicalShowDDLJobs` (`physical_show.go:75`): the
/// `ADMIN SHOW DDL JOBS` twin of [`find_best_task_4_logical_show`], same
/// body over `JobNumber`.
#[must_use]
pub fn find_best_task_4_logical_show_ddl_jobs(
    p: &crate::logical::LogicalShowDDLJobs,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
) -> Task {
    if !prop.is_sort_item_empty() {
        return Task::invalid_task();
    }
    let mut base = BasePhysicalPlan::new(allocator, crate::logical::LogicalShowDDLJobs::TYPE, 0);
    base.base.set_stats(Some(StatsInfo::new(1.0, [])));
    base.base.set_schema(p.base.base.schema().cloned());
    let show = PhysicalPlan::ShowDDLJobs(PhysicalShowDDLJobs {
        base,
        job_number: p.job_number,
    });
    let mut root = RootTask::default();
    root.set_plan(show);
    Task::Root(root)
}

/// Go `physicalop.PhysicalLock` (`physical_lock.go`, whole file): the
/// physical operator of `SELECT ... FOR UPDATE`. Go's `Init` fixes the
/// query-block offset at 0. The `Lock *ast.SelectLockInfo` pointer narrows
/// to its decision-bearing pair — [`crate::logical::SelectLockType`] and
/// `WaitSec` — the same narrowing [`crate::logical::LogicalLock`] made.
/// `ResolveIndices` and `MemoryUsage` follow the enum's standing
/// narrowings.
#[derive(Clone, Debug, Default)]
pub struct PhysicalLock {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `Lock.LockType`.
    pub lock_type: crate::logical::SelectLockType,
    /// Go `Lock.WaitSec`.
    pub wait_sec: u64,
    /// Go `TblID2Handle`, as handle columns per table id.
    pub tbl_id_to_handle_cols: std::collections::BTreeMap<i64, Vec<tidb_expr::column::Column>>,
    /// Go `TblID2PhysTblIDCol`.
    pub tbl_id_to_phys_tbl_id_col: std::collections::BTreeMap<i64, tidb_expr::column::Column>,
}

impl PhysicalLock {
    /// Go `PhysicalLock.ExplainInfo()`: `{LockType.String()} {WaitSec}` —
    /// note Go prints the wait seconds unconditionally, even for lock types
    /// that carry none.
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!("{} {}", self.lock_type, self.wait_sec)
    }
}

/// Go `ExhaustPhysicalPlans4LogicalLock` (`physical_lock.go:44`): one
/// root-side Lock over the essential child property, its stats scaled to
/// the parent's expected count. The `IsFlashProp` arm returns empty exactly
/// as Go's does (its `RaiseWarningWhenMPPEnforced` narrows with the
/// session-vars warning sink, as in `enforce.go`'s port).
#[must_use]
pub fn exhaust_physical_plans_4_logical_lock(
    p: &crate::logical::LogicalLock,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
) -> Vec<PhysicalPlan> {
    if prop.task_tp == TaskType::Mpp {
        return Vec::new();
    }
    let child_prop = prop.clone_essential_fields();
    let stats = p
        .base
        .base
        .stats_info()
        .map(|stats| stats.scale_by_expect_cnt(prop.expected_cnt, skew_ratio));
    let mut base = BasePhysicalPlan::new(allocator, crate::logical::LogicalLock::TYPE, 0);
    base.base.set_stats(stats);
    base.set_children_req_props(vec![Some(child_prop)]);
    vec![PhysicalPlan::Lock(PhysicalLock {
        base,
        lock_type: p.lock_type,
        wait_sec: p.wait_sec,
        tbl_id_to_handle_cols: p.tbl_id_to_handle_cols.clone(),
        tbl_id_to_phys_tbl_id_col: p.tbl_id_to_phys_tbl_id_col.clone(),
    })]
}

/// Go `physicalop.PhysicalUnionAll` (`physical_union_all.go`, whole file):
/// bag union of its children. `Mpp` marks the MPP-mode candidate; with no
/// TiFlash tier every candidate here is the root-mode one, so it is always
/// false (the field is carried because Go's `Attach2Task` and cost bodies
/// read it).
#[derive(Clone, Debug, Default)]
pub struct PhysicalUnionAll {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `Mpp`.
    pub mpp: bool,
}

/// Go `ExhaustPhysicalPlans4LogicalUnionAll` (`physical_union_all.go:77`):
/// a union promises no order (Go's own TODO notes a merge-sort future), so
/// a required sort enumerates nothing; otherwise ONE candidate whose every
/// child property carries the parent's expected count, over scaled stats
/// and the union's schema.
///
/// Go's MPP arms — the MPP-mode candidate under an MPP parent property and
/// the extra `mppUA` beside the root candidate — are gated on
/// `IsMPPAllowed` over a TiFlash-backed cluster; with no TiFlash tier they
/// narrow away as in [`exhaust_physical_plans_4_logical_limit`].
/// `CTEProducerStatus`/`NoCopPushDown` narrow with the unported fields.
#[must_use]
pub fn exhaust_physical_plans_4_logical_union_all(
    p: &crate::logical::LogicalUnionAll,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
) -> Vec<PhysicalPlan> {
    if !prop.is_sort_item_empty() || prop.task_tp == TaskType::Mpp {
        return Vec::new();
    }
    let ch_req_props: Vec<Option<PhysicalProperty>> = (0..p.base.child_len())
        .map(|_| {
            Some(PhysicalProperty {
                expected_cnt: prop.expected_cnt,
                ..PhysicalProperty::default()
            })
        })
        .collect();
    let stats = p
        .base
        .base
        .stats_info()
        .map(|stats| stats.scale_by_expect_cnt(prop.expected_cnt, skew_ratio));
    let mut base = BasePhysicalPlan::new(
        allocator,
        crate::logical::LogicalUnionAll::TYPE,
        p.base.base.query_block_offset(),
    );
    base.base.set_stats(stats);
    base.base.set_schema(p.base.base.schema().cloned());
    base.set_children_req_props(ch_req_props);
    vec![PhysicalPlan::UnionAll(PhysicalUnionAll { base, mpp: false })]
}

/// Go `ExhaustPhysicalPlans4LogicalPartitionUnionAll`
/// (`physical_union_all.go:125`): the union-all enumeration with every
/// candidate's plan type re-stamped `PartitionUnion`.
#[must_use]
pub fn exhaust_physical_plans_4_logical_partition_union_all(
    p: &crate::logical::LogicalPartitionUnionAll,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
) -> Vec<PhysicalPlan> {
    let mut plans =
        exhaust_physical_plans_4_logical_union_all(&p.union_all, prop, allocator, skew_ratio);
    for plan in &mut plans {
        plan.base_mut()
            .base
            .set_tp(crate::logical::LogicalPartitionUnionAll::TYPE);
    }
    plans
}

/// Go `physicalop.PhysicalSequence` (`physical_sequence.go`, whole file):
/// "the physical node for CTE storages" — every child but the last is a
/// CTE producer, the last is the main query, and the sequence's schema is
/// the LAST child's. `ExplainInfo` is the fixed string `Sequence Node`;
/// `Attach2Task` lives in core/task.go and refuses by name in
/// [`crate::task::attach2_task`].
#[derive(Clone, Debug, Default)]
pub struct PhysicalSequence {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
}

impl PhysicalSequence {
    /// Go `PhysicalSequence.ExplainInfo()`: the fixed `Sequence Node`.
    #[must_use]
    pub const fn explain_info() -> &'static str {
        "Sequence Node"
    }
}

/// Go `ExhaustPhysicalPlans4LogicalSequence` (`physical_sequence.go:95`):
/// under a root property, ONE candidate whose producers are each planned
/// under `{RootTaskType, MaxFloat64, SomeCTEFailedMpp}` and whose main
/// query gets the parent's essential property with `SomeCTEFailedMpp`
/// stamped on it — the stamp is what stops an MPP plan from forming under
/// a producer that cannot run MPP.
///
/// Go's MPP-property arm (`AllCTECanMpp` + a second all-MPP candidate
/// beside the root one) narrows away with the TiFlash tier, as in
/// [`exhaust_physical_plans_4_logical_limit`]; under an MPP parent
/// property Go returns nothing when `SomeCTEFailedMpp`, and an MPP parent
/// property cannot arise here at all.
#[must_use]
pub fn exhaust_physical_plans_4_logical_sequence(
    p: &crate::logical::LogicalSequence,
    prop: &PhysicalProperty,
    allocator: &PlanIdAllocator,
) -> Vec<PhysicalPlan> {
    use crate::physical_property::CteProducerStatus;
    if prop.task_tp == TaskType::Mpp {
        return Vec::new();
    }
    let producer_prop = PhysicalProperty {
        cte_producer_status: CteProducerStatus::SomeCteFailedMpp,
        ..PhysicalProperty::default()
    };
    let mut main_prop = prop.clone_essential_fields();
    main_prop.cte_producer_status = CteProducerStatus::SomeCteFailedMpp;
    let child_len = p.base.child_len();
    let mut ch_req_props: Vec<Option<PhysicalProperty>> = (0..child_len.saturating_sub(1))
        .map(|_| Some(producer_prop.clone_essential_fields()))
        .collect();
    ch_req_props.push(Some(main_prop));
    let seq_schema = p
        .base
        .children()
        .last()
        .and_then(crate::logical::LogicalPlan::schema)
        .cloned();
    let mut base = BasePhysicalPlan::new(
        allocator,
        crate::logical::LogicalSequence::TYPE,
        p.base.base.query_block_offset(),
    );
    base.base.set_stats(p.base.base.stats_info().cloned());
    base.base.set_schema(seq_schema);
    base.set_children_req_props(ch_req_props);
    vec![PhysicalPlan::Sequence(PhysicalSequence { base })]
}

/// Go `physicalop.PhysicalApply` (`physical_apply.go`, whole file): the
/// correlated nested-loop join, embedding [`PhysicalHashJoin`] exactly as
/// Go embeds it. Go's `PhysicalJoinImplement() bool { return false }`
/// override — which UN-implements the `base.PhysicalJoin` interface an
/// embedded hash join would otherwise satisfy — has no enum counterpart to
/// override: an `Apply` variant simply is not a join variant, which is the
/// same fact stated structurally. `GetCost`/ver1/ver2 delegate to
/// core-cost bodies (`utilfuncp`) and follow the enum's cost narrowings;
/// `Attach2Task4PhysicalApply` (core/task.go) refuses by name in
/// [`crate::task::attach2_task`]. Go's `ExtractCorrelatedCols` override —
/// the hash join's extraction minus columns the OUTER child's schema
/// contains — narrows with the enum's condition-less hash join, whose own
/// extraction is already the empty base body.
#[derive(Clone, Debug, Default)]
pub struct PhysicalApply {
    /// Go's embedded `PhysicalHashJoin`.
    pub hash_join: PhysicalHashJoin,
    /// Go `CanUseCache`: whether the inner side may be memoized per outer
    /// correlation value.
    pub can_use_cache: bool,
    /// Go `Concurrency`.
    pub concurrency: usize,
    /// Go `KeepOrder`: parallel apply must emit rows in outer order.
    pub keep_order: bool,
    /// Go `OuterSchema`: the correlated columns the inner side reads.
    pub outer_schema: Vec<CorrelatedColumn>,
    /// Go `NoDecorrelate`: the apply stayed undecorrelated because of a
    /// `no_decorrelate` hint (read by EXPLAIN EXPLORE).
    pub no_decorrelate: bool,
}

/// Go `BuildPhysicalJoinSchema(joinType, join)`
/// (`base_physical_join.go:190`): the schema a physical join exposes, from
/// its children's — semi joins keep the left schema; the left-outer-semi
/// pair appends the join's own trailing bool column; everything else is the
/// merged pair with the outer side's opposite half made nullable.
///
/// `None` answers a child without a schema, where Go would nil-deref.
#[must_use]
pub fn build_physical_join_schema(
    join_type: LogicalJoinType,
    join: &PhysicalPlan,
) -> Option<Schema> {
    let left_schema = join.children().first().and_then(PhysicalPlan::schema)?;
    match join_type {
        LogicalJoinType::Semi | LogicalJoinType::AntiSemi => Some(left_schema.clone()),
        LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi => {
            let mut new_schema = left_schema.clone();
            let own = join.schema()?;
            new_schema.columns.push(own.columns.last()?.clone());
            Some(new_schema)
        }
        LogicalJoinType::Inner | LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
            let left_len = left_schema.len();
            let right_schema = join.children().get(1).and_then(PhysicalPlan::schema);
            let mut new_schema =
                tidb_expr::schema::merge_schema(Some(left_schema), right_schema)?;
            let total = new_schema.len();
            match join_type {
                LogicalJoinType::LeftOuter => {
                    crate::plan_builder::from::reset_not_null_flag(
                        &mut new_schema,
                        left_len,
                        total,
                    );
                }
                LogicalJoinType::RightOuter => {
                    crate::plan_builder::from::reset_not_null_flag(&mut new_schema, 0, left_len);
                }
                _ => {}
            }
            Some(new_schema)
        }
    }
}

/// Go `physicalop.PhysicalTableReader` (the reader half of
/// `convertToRootTaskImpl`'s table branch, `task_base.go:571`): the
/// TiDB-side operator that reads a pushed-down table plan's results. The
/// cop-side plan hangs off [`Self::table_plan`], NOT the child list —
/// exactly Go's `TablePlan` field, whose tree lives in another execution
/// tier.
#[derive(Clone, Debug, Default)]
pub struct PhysicalTableReader {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `TablePlan`: the pushed-down side.
    pub table_plan: Option<Box<PhysicalPlan>>,
    /// Go `StoreType`, read off the bottom `PhysicalTableScan`.
    pub store_type: crate::physical_table_reader::StoreType,
    /// Go `IsCommonHandle`, read off the scan's table.
    pub is_common_handle: bool,
}

/// Go `physicalop.PhysicalIndexScan`'s planning slice: which index the scan
/// reads and how. The full Go struct carries ranges/columns/histograms; the
/// enum's slice carries what the dispatcher's admission and the reader
/// conversion decide on, like [`PhysicalTableScan`]'s slice.
#[derive(Clone, Debug, Default)]
pub struct PhysicalIndexScan {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `Table.ID`.
    pub table_id: i64,
    /// Go `Index.ID`.
    pub index_id: i64,
    /// Go `Index.Name.O`, for explain identity.
    pub index_name: String,
    /// Go `KeepOrder`.
    pub keep_order: bool,
    /// Go `Desc`.
    pub desc: bool,
}

/// Go `physicalop.PhysicalIndexReader` (`physical_index_reader.go:34`): the
/// TiDB-side operator over a pushed-down index plan, born by
/// `convertToRootTaskImpl`'s index branch (`task_base.go:563`). The
/// pushed-down side hangs off [`Self::index_plan`], not the child list —
/// [`PhysicalTableReader`]'s own shape.
#[derive(Clone, Debug, Default)]
pub struct PhysicalIndexReader {
    /// The shared physical base.
    pub base: BasePhysicalPlan,
    /// Go `IndexPlan`.
    pub index_plan: Option<Box<PhysicalPlan>>,
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
    /// Go `physicalop.PhysicalMaxOneRow`.
    MaxOneRow(PhysicalMaxOneRow),
    /// Go `physicalop.NominalSort`.
    NominalSort(NominalSort),
    /// Go `physicalop.PhysicalCTETable`.
    CTETable(PhysicalCTETable),
    /// Go `physicalop.PhysicalShow`.
    Show(PhysicalShow),
    /// Go `physicalop.PhysicalShowDDLJobs`.
    ShowDDLJobs(PhysicalShowDDLJobs),
    /// Go `physicalop.PhysicalLock`.
    Lock(PhysicalLock),
    /// Go `physicalop.PhysicalUnionAll`.
    UnionAll(PhysicalUnionAll),
    /// Go `physicalop.PhysicalSequence`.
    Sequence(PhysicalSequence),
    /// Go `physicalop.PhysicalApply`.
    Apply(PhysicalApply),
    /// Go `physicalop.PhysicalTableReader`.
    TableReader(PhysicalTableReader),
    /// Go `physicalop.PhysicalIndexScan` (planning slice).
    IndexScan(PhysicalIndexScan),
    /// Go `physicalop.PhysicalIndexReader`.
    IndexReader(PhysicalIndexReader),
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
            Self::MaxOneRow(op) => &op.base,
            Self::NominalSort(op) => &op.base,
            Self::CTETable(op) => &op.base,
            Self::Show(op) => &op.base,
            Self::ShowDDLJobs(op) => &op.base,
            Self::Lock(op) => &op.base,
            Self::UnionAll(op) => &op.base,
            Self::Sequence(op) => &op.base,
            Self::Apply(op) => &op.hash_join.base,
            Self::TableReader(op) => &op.base,
            Self::IndexScan(op) => &op.base,
            Self::IndexReader(op) => &op.base,
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
            Self::MaxOneRow(op) => &mut op.base,
            Self::NominalSort(op) => &mut op.base,
            Self::CTETable(op) => &mut op.base,
            Self::Show(op) => &mut op.base,
            Self::ShowDDLJobs(op) => &mut op.base,
            Self::Lock(op) => &mut op.base,
            Self::UnionAll(op) => &mut op.base,
            Self::Sequence(op) => &mut op.base,
            Self::Apply(op) => &mut op.hash_join.base,
            Self::TableReader(op) => &mut op.base,
            Self::IndexScan(op) => &mut op.base,
            Self::IndexReader(op) => &mut op.base,
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

    // Go `Attach2Task(...Task) Task` (`<2nd>`) lives at
    // [`crate::task::attach2_task`], beside the task representation it
    // composes onto.

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
                conditions: op.conditions.clone(),
                from_data_source: op.from_data_source,
            }),
            Self::Projection(op) => Self::Projection(PhysicalProjection {
                base: base_of(&op.base),
                exprs: op.exprs.clone(),
                calculate_no_delay: op.calculate_no_delay,
                avoid_column_evaluator: op.avoid_column_evaluator,
            }),
            Self::HashJoin(op) => Self::HashJoin(PhysicalHashJoin {
                base: base_of(&op.base),
                join_type: op.join_type,
                inner_child_idx: op.inner_child_idx,
            }),
            Self::Sort(op) => Self::Sort(PhysicalSort {
                base: base_of(&op.base),
                by_items: op.by_items.clone(),
                is_partial_sort: op.is_partial_sort,
            }),
            Self::Limit(op) => Self::Limit(PhysicalLimit {
                base: base_of(&op.base),
                partition_by: op.partition_by.clone(),
                offset: op.offset,
                count: op.count,
                prefix_col: op.prefix_col,
                prefix_len: op.prefix_len,
            }),
            Self::TableScan(op) => Self::TableScan(PhysicalTableScan {
                base: base_of(&op.base),
                keep_order: op.keep_order,
                desc: op.desc,
                table_id: op.table_id,
                store_type: op.store_type,
            }),
            Self::TableDual(op) => Self::TableDual(PhysicalTableDual {
                base: base_of(&op.base),
                row_count: op.row_count,
            }),
            Self::MaxOneRow(op) => Self::MaxOneRow(PhysicalMaxOneRow {
                base: base_of(&op.base),
            }),
            Self::NominalSort(op) => Self::NominalSort(NominalSort {
                base: base_of(&op.base),
                by_items: op.by_items.clone(),
                only_column: op.only_column,
            }),
            Self::CTETable(op) => Self::CTETable(PhysicalCTETable {
                base: base_of(&op.base),
                id_for_storage: op.id_for_storage,
            }),
            Self::Show(op) => Self::Show(PhysicalShow {
                base: base_of(&op.base),
                contents: op.contents.clone(),
                extractor: op.extractor.clone(),
            }),
            Self::ShowDDLJobs(op) => Self::ShowDDLJobs(PhysicalShowDDLJobs {
                base: base_of(&op.base),
                job_number: op.job_number,
            }),
            Self::Lock(op) => Self::Lock(PhysicalLock {
                base: base_of(&op.base),
                lock_type: op.lock_type,
                wait_sec: op.wait_sec,
                tbl_id_to_handle_cols: op.tbl_id_to_handle_cols.clone(),
                tbl_id_to_phys_tbl_id_col: op.tbl_id_to_phys_tbl_id_col.clone(),
            }),
            Self::UnionAll(op) => Self::UnionAll(PhysicalUnionAll {
                base: base_of(&op.base),
                mpp: op.mpp,
            }),
            Self::Sequence(op) => Self::Sequence(PhysicalSequence {
                base: base_of(&op.base),
            }),
            Self::Apply(op) => Self::Apply(PhysicalApply {
                hash_join: PhysicalHashJoin {
                    base: base_of(&op.hash_join.base),
                    join_type: op.hash_join.join_type,
                    inner_child_idx: op.hash_join.inner_child_idx,
                },
                can_use_cache: op.can_use_cache,
                concurrency: op.concurrency,
                keep_order: op.keep_order,
                outer_schema: op.outer_schema.clone(),
                no_decorrelate: op.no_decorrelate,
            }),
            Self::TableReader(op) => Self::TableReader(PhysicalTableReader {
                base: base_of(&op.base),
                table_plan: op.table_plan.clone(),
                store_type: op.store_type,
                is_common_handle: op.is_common_handle,
            }),
            Self::IndexScan(op) => Self::IndexScan(PhysicalIndexScan {
                base: base_of(&op.base),
                table_id: op.table_id,
                index_id: op.index_id,
                index_name: op.index_name.clone(),
                keep_order: op.keep_order,
                desc: op.desc,
            }),
            Self::IndexReader(op) => Self::IndexReader(PhysicalIndexReader {
                base: base_of(&op.base),
                index_plan: op.index_plan.clone(),
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
