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

//! The task representation: what a physical plan costs to sit where it sits.
//!
//! Go sources:
//! * `pkg/planner/core/base/task_base.go` (58 lines) — the `Task` interface
//!   and its three asserted implementors.
//! * `pkg/planner/core/operator/physicalop/task_base.go` (608) —
//!   `SimpleWarnings`, `RootTask`, `MppTask`, `CopTask` and their methods.
//! * `pkg/planner/core/operator/physicalop/task.go` (96) —
//!   `CopTask.FinishIndexPlan`, `GetStoreType`, `handleRootTaskConds`.
//!
//! [`attach2_task`] attaches owned physical candidates. Cop conversion builds
//! table, index and index-lookup readers; MPP conversion now builds Go's
//! pass-through sender/table-reader boundary. Unsupported MPP/index-merge
//! paths in the surrounding attachment matrix remain explicit errors.
//!
//! # Narrowings
//!
//! * `context.SQLWarn` carries a Go `error`; [`SqlWarn`] carries the
//!   rendered message. Level and the `math.MaxUint16` cap are Go's.
//! * `statistics.HistColl` (`TblColHists`) is unported; the fields carrying
//!   it are absent. Network/scan-width costing that reads them is cost-model
//!   work, not representation work.
//! * `PhysPlanPartInfo` and the task fields carrying property match results
//!   are not yet wired; the property result types themselves are complete.
//! * `StatsInfo.StatsVersion` is carried by the property profile; preserving
//!   it through every task transition belongs to this task package.

use crate::PhysicalPlan;
use tidb_planner_property::MppPartitionType;
use crate::physical_table_reader::ReadReqType;
use tidb_kv::store_type::StoreType;
use tidb_planner_core_base::PlanError;
use std::sync::Arc;
use tidb_expr::expression::Expression;
use tidb_planner_cardinality::derive_stats::StatsInfoScale;

mod virtual_columns;

/// Go `utilfuncp.Attach2Task4X`: core's `attach2Task` bodies (`core/task.go`),
/// which the operators reach through these variables -- here, the physical
/// sort `EnforceProperty` puts over a task.
pub trait TaskAttacher {
    /// Go `p.Attach2Task(tasks...)` for `plan`.
    ///
    /// # Errors
    ///
    /// The attachment's planner error.
    fn attach2_task(
        &self,
        plan: PhysicalPlan,
        tasks: Vec<Task>,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> Result<Task, PlanError>;
}

/// The statistics settings from Go's statement PlanContext that must survive
/// candidate dispatch and task composition until root conversion.
#[derive(Clone)]
pub struct TaskStatsContext {
    /// Go CopTask.TblColHists: the source population, before access/filter scaling.
    pub root_filter_stats: Option<Arc<tidb_planner_property::stats_info::StatsInfo>>,
    pub estimator_options: tidb_planner_cardinality::row_count_estimator::EstimatorOptions,
    pub scale_ndv_skew_ratio: f64,
    /// Go `cardinality.CollectFilters4MVIndex` / `BuildPartialPaths4MVIndex`,
    /// which core registers (`core_init.go`); core's task builder sets it
    /// beside the root conditions it estimates.
    pub mv_index_filters: Option<
        &'static (dyn tidb_planner_cardinality::selectivity::MvIndexFilters + Send + Sync),
    >,
}

impl std::fmt::Debug for TaskStatsContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskStatsContext")
            .field("root_filter_stats", &self.root_filter_stats)
            .field("estimator_options", &self.estimator_options)
            .field("scale_ndv_skew_ratio", &self.scale_ndv_skew_ratio)
            .field("mv_index_filters", &self.mv_index_filters.is_some())
            .finish()
    }
}

impl Default for TaskStatsContext {
    fn default() -> Self {
        Self {
            root_filter_stats: None,
            estimator_options: Default::default(),
            scale_ndv_skew_ratio: tidb_planner_cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
            mv_index_filters: None,
        }
    }
}

/// The bottom-up feedback produced by an inner data source planned under an
/// index-join runtime property. This is the ported slice of Go
/// `physicalop.IndexJoinInfo` consumed when the physical index join attaches.
#[derive(Clone, Debug, Default)]
pub struct IndexJoinInfo {
    /// The selected physical table.
    pub table_id: i64,
    /// The selected secondary index, or `None` for a table/common-handle path.
    pub index_id: Option<i64>,
    /// The ranges built for the selected inner access.
    pub ranges: tidb_ranger::types::Ranges,
    /// Go `IndexJoinInfo.IdxColLens`.
    pub idx_col_lens: Vec<i64>,
    /// Go `IndexJoinInfo.KeyOff2IdxOff`; `-1` means the selected access cannot
    /// use that logical equality as a lookup key.
    pub key_off2_idx_off: Vec<i64>,
    /// Chosen range predicates used by Go to construct the scan RangeInfo.
    /// Retained as expressions for the executor renderer, not row evaluation.
    pub access_conditions: Vec<tidb_expr::expression::Expression>,
    /// Static template inputs for Go mutableIndexJoinRange on cache reuse.
    pub range_rebuild: Option<crate::range_rebuild::PointRangeRebuild>,
    /// Go `IndexJoinInfo.CompareFilters`.
    pub compare_filters: Option<crate::IndexJoinCompareFilters>,
    /// Go `AccessPath.RangeInfo`'s spelling: `indexJoinIntPKRangeInfo` renders
    /// the bare outer join keys when the probe is the integer handle
    /// (`constructDS2TableScanTask`'s int-PK branch); every other probe
    /// (index path, common handle, rowid mutable ranges) renders
    /// `indexJoinPathRangeInfo`'s `eq(inner, outer)` pairs.
    pub range_bare: bool,
}

/// Go `contextutil.WarnLevel*`; this file writes `WarnLevelWarning` and
/// `WarnLevelNote`.
use tidb_context::WarningLevel;

/// Go `context.SQLWarn`, narrowed: the `error` becomes its rendered message.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlWarn {
    /// The warning level.
    pub level: WarningLevel,
    /// The rendered message of Go's wrapped `error`.
    pub message: String,
}

/// Go `physicalop.SimpleWarnings` (`task_base.go:47-107`): the per-task
/// warning slice, copied — never shared — between task instances.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SimpleWarnings {
    pub warnings: Vec<SqlWarn>,
}

impl SimpleWarnings {
    /// Go `WarningCount`.
    #[must_use]
    pub fn warning_count(&self) -> usize {
        self.warnings.len()
    }

    /// Go `Copy(src)`: replace this slice with a copy of `src`'s, so two
    /// task instances never share one backing slice.
    pub fn copy_of(&mut self, src: &SimpleWarnings) {
        self.warnings = src.warnings.clone();
    }

    /// Go `CopyFrom(src ...)`: concatenate every source's warnings, skipping
    /// nil entries (absent here — an `Option` caller just does not pass one).
    pub fn copy_from<'a>(&mut self, sources: impl IntoIterator<Item = &'a SimpleWarnings>) {
        let sources: Vec<&SimpleWarnings> = sources.into_iter().collect();
        let length = sources.iter().map(|one| one.warnings.len()).sum();
        let mut warnings = Vec::with_capacity(length);
        for one in sources {
            warnings.extend(one.warnings.iter().cloned());
        }
        self.warnings = warnings;
    }

    /// Go `AppendWarning`: silently DROPPED once the slice holds
    /// `math.MaxUint16` entries — Go's cap, reproduced.
    pub fn append_warning(&mut self, message: impl Into<String>) {
        if self.warnings.len() < usize::from(u16::MAX) {
            self.warnings.push(SqlWarn {
                level: WarningLevel::Warning,
                message: message.into(),
            });
        }
    }

    /// Go `AppendNote`, under the same cap.
    pub fn append_note(&mut self, message: impl Into<String>) {
        if self.warnings.len() < usize::from(u16::MAX) {
            self.warnings.push(SqlWarn {
                level: WarningLevel::Note,
                message: message.into(),
            });
        }
    }

    /// Go `GetWarnings`: the stored warnings, materialized.
    #[must_use]
    pub fn get_warnings(&self) -> Vec<SqlWarn> {
        self.warnings.clone()
    }
}

/// Go `physicalop.RootTask` (`task_base.go:202-210`): the final sink of a
/// plan graph, single-goroutine on TiDB.
#[derive(Clone, Debug, Default)]
pub struct RootTask {
    /// Go's private `p`. `None` is Go's nil plan, which is what makes a
    /// root task [`Task::invalid`] — `base.InvalidTask` is exactly an empty
    /// `RootTask`.
    ///
    /// Shared, because Go's `Copy` copies the task and keeps the plan
    /// POINTER (`p: t.p`). Writes go through [`Task::plan_mut`] and
    /// [`Self::take_plan`], which copy first if another task still holds
    /// this plan, so the sharing is invisible to every caller.
    plan: Option<Arc<PhysicalPlan>>,
    /// Go `RootTask.IndexJoinInfo`, passed through unary root operators until
    /// the owning physical index join consumes it.
    pub index_join_info: Option<IndexJoinInfo>,
    /// Go `Warnings`.
    pub warnings: SimpleWarnings,
}

impl RootTask {
    /// Go `GetPlan`. Panics on an invalid task exactly where Go would
    /// nil-deref; use [`Task::invalid`] first, as Go's callers do.
    #[must_use]
    pub fn get_plan(&self) -> &PhysicalPlan {
        self.plan
            .as_deref()
            .expect("RootTask.GetPlan on an invalid task: Go nil-derefs here")
    }

    /// Go `SetPlan`.
    pub fn set_plan(&mut self, plan: PhysicalPlan) {
        self.plan = Some(Arc::new(plan));
    }

    /// Takes the plan out, for the ownership handoff `attachPlan2Task`'s
    /// `p.SetChildren(v.GetPlan()); v.SetPlan(p)` pair performs.
    ///
    /// Go hands over the pointer it shares; here the tree is copied when
    /// another task still holds it, and moved out untouched when this task
    /// is its only owner (the common case: the memo stores its copy only
    /// after the winning task is built).
    pub fn take_plan(&mut self) -> Option<PhysicalPlan> {
        self.plan
            .take()
            .map(|plan| Arc::try_unwrap(plan).unwrap_or_else(|shared| (*shared).clone()))
    }

    /// Go `Copy` (`task_base.go:131-142`): the same plan, warnings COPIED so
    /// the two instances never share a slice.
    #[must_use]
    pub fn copy(&self) -> RootTask {
        let mut copied = RootTask {
            plan: self.plan.clone(),
            index_join_info: self.index_join_info.clone(),
            warnings: SimpleWarnings::default(),
        };
        copied.warnings.copy_of(&self.warnings);
        copied
    }
}

/// Go `physicalop.MppTask` (`task_base.go:190-215`), the TiFlash fragment:
/// cannot keep order, cannot double read, cannot see virtual columns —
/// Go's own doc.
#[derive(Clone, Debug, Default)]
pub struct MppTask {
    /// Go's private `p`.
    pub plan: Option<Box<PhysicalPlan>>,
    /// Go's private `partTp`.
    pub part_tp: MppPartitionType,
    /// Go `HashCols`, retained so exchange enforcement can compare and carry
    /// the current partitioning contract.
    pub hash_cols: Vec<tidb_planner_property::MppPartitionColumn>,
    /// Go `RootTaskConds`: TableScan filters TiFlash cannot take, executed
    /// in a TiDB-side Selection when the task converts to root.
    pub root_task_conds: Vec<Expression>,
    /// Immutable session estimator settings used if root conversion must
    /// estimate `root_task_conds`, Go `PlanContext`'s session snapshot.
    pub stats_context: TaskStatsContext,
    // boundary: `tblColHists *statistics.HistColl` — row-width statistics,
    // unported.
    /// Go `Warnings`.
    pub warnings: SimpleWarnings,
}

impl MppTask {
    /// Go `NewMppTask`, minus the narrowed fields.
    #[must_use]
    pub fn new(
        plan: PhysicalPlan,
        part_tp: MppPartitionType,
        warnings: impl IntoIterator<Item = SimpleWarnings>,
    ) -> MppTask {
        Self::new_with_hash_cols(plan, part_tp, Vec::new(), warnings)
    }

    /// `NewMppTask` with the source partition columns retained.
    #[must_use]
    pub fn new_with_hash_cols(
        plan: PhysicalPlan,
        part_tp: MppPartitionType,
        hash_cols: Vec<tidb_planner_property::MppPartitionColumn>,
        warnings: impl IntoIterator<Item = SimpleWarnings>,
    ) -> MppTask {
        let mut task = MppTask {
            plan: Some(Box::new(plan)),
            part_tp,
            hash_cols,
            root_task_conds: Vec::new(),
            stats_context: TaskStatsContext::default(),
            warnings: SimpleWarnings::default(),
        };
        let sources: Vec<SimpleWarnings> = warnings.into_iter().collect();
        task.warnings.copy_from(sources.iter());
        task
    }

    /// Carry the statement's estimator configuration across task composition.
    #[must_use]
    pub fn with_stats_context(mut self, context: TaskStatsContext) -> Self {
        self.stats_context = context;
        self
    }

    /// Go `GetPartitionType`.
    #[must_use]
    pub fn partition_type(&self) -> MppPartitionType {
        self.part_tp
    }

    /// Go `GetHashCols`.
    #[must_use]
    pub fn hash_cols(&self) -> &[tidb_planner_property::MppPartitionColumn] {
        &self.hash_cols
    }

    /// Go `MppTask.Plan`.
    #[must_use]
    pub fn plan(&self) -> Option<&PhysicalPlan> {
        self.plan.as_deref()
    }

    /// Go `Copy`: struct copy plus a fresh warnings slice.
    #[must_use]
    pub fn copy(&self) -> MppTask {
        let mut copied = self.clone();
        copied.warnings = SimpleWarnings::default();
        copied.warnings.copy_of(&self.warnings);
        copied
    }

    /// Go `MppTask.ConvertToRootTaskImpl` (`task_base.go:298-355`): expose an
    /// MPP fragment through a TiFlash table reader and a pass-through sender.
    /// The sender is deliberately kept in the pushed-down `TablePlan`, so
    /// root conversion has the same reader boundary as Go's
    /// `GenerateRootMPPTasks` path.
    pub fn into_root_task(
        self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        self.into_root_task_in(allocator, &tidb_ranger::points::evaluate_static)
    }

    fn into_root_task_in(
        mut self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> Result<Task, PlanError> {
        let mut plan = self
            .plan
            .take()
            .ok_or_else(|| PlanError::internal("MppTask.ConvertToRootTaskImpl: empty plan"))?;
        virtual_columns::expand(&mut plan)?;
        if !self.root_task_conds.is_empty()
            && !matches!(&*plan, PhysicalPlan::TableScan(_))
            && !matches!(
                &*plan,
                PhysicalPlan::Selection(selection)
                    if matches!(selection.base.children(), [PhysicalPlan::TableScan(_)])
            )
        {
            // Go returns base.InvalidTask when root-only conditions are
            // attached to anything other than a table scan (or its direct
            // Selection wrapper). Keeping the invalid task is observable by
            // candidate comparison, so do not attach a root Selection here.
            return Ok(Task::invalid_task());
        }

        let mut sender_base = crate::BasePhysicalPlan::new(
            allocator,
            "ExchangeSender",
            plan.query_block_offset(),
        );
        sender_base.base.set_stats(plan.stats_info().cloned());
        sender_base.base.set_schema(plan.schema().cloned());
        sender_base.set_children(vec![*plan]);
        let sender = PhysicalPlan::ExchangeSender(crate::PhysicalExchangeSender {
            base: sender_base,
            exchange_type: crate::ExchangeType::PassThrough,
            hash_cols: Vec::new(),
        });

        let mut reader_base = crate::BasePhysicalPlan::new(
            allocator,
            "TableReader",
            sender.query_block_offset(),
        );
        reader_base.base.set_stats(sender.stats_info().cloned());
        reader_base.base.set_schema(sender.schema().cloned());
        let reader = PhysicalPlan::TableReader(crate::PhysicalTableReader {
            base: reader_base,
            table_plan: Some(Box::new(sender)),
            store_type: StoreType::TiFlash,
            is_common_handle: false,
            read_req_type: ReadReqType::Mpp,
            plan_part_info: None,
        });

        let mut root = RootTask::default();
        root.set_plan(reader);
        if self.warnings.warning_count() > 0 {
            root.warnings.copy_of(&self.warnings);
        }
        let conds = std::mem::take(&mut self.root_task_conds);
        Ok(Task::Root(CopTask::handle_root_task_conds(
            conds,
            root,
            allocator,
            self.stats_context,
            evaluate,
        )))
    }
}

/// Property feedback retained from merge candidate convergence.
#[derive(Clone, Debug)]
pub struct IndexMergeOrder {
    /// Source matching used advisory rather than required sort items.
    pub advisory: bool,
    /// One source property result for each partial plan, in the same order.
    pub partial_matches: Vec<tidb_planner_property::PhysicalPropMatchResult>,
    /// Required merge order. Advisory order does not order the final reader.
    /// Go carries these ByItems on the partial scans before reader conversion.
    pub by_items: Vec<tidb_expression_aggregation::ByItems>,
}

/// Go `physicalop.CopTask` (`task_base.go:367-430`): a task running in the
/// distributed KV store, holding the index half and the table half of a
/// double read.
#[derive(Clone, Debug, Default)]
pub struct CopTask {
    /// Go `IndexPlan`.
    pub index_plan: Option<Box<PhysicalPlan>>,
    /// Go `TablePlan`.
    pub table_plan: Option<Box<PhysicalPlan>>,
    /// Go `CommonHandleCols`.
    pub common_handle_cols: Vec<tidb_expr::column::Column>,
    /// Go `IndexLookUpPushDownBy`: whether a double read should become one
    /// storage-side local index lookup, and which authority requested it.
    pub index_lookup_push_down_by: tidb_planner_util::path::IndexLookupPushDownBy,
    /// Go `IndexPlanFinished`: whether the index half is sealed, which
    /// decides which half [`CopTask::plan`] and [`CopTask::count`] read.
    pub index_plan_finished: bool,
    /// Go `KeepOrder`.
    pub keep_order: bool,
    /// Go `NeedExtraProj`: a double read may output one extra handle column
    /// that must be pruned above.
    pub need_extra_proj: bool,
    /// Go `OriginSchema`: output before adding handle or virtual-dependency columns.
    pub origin_schema: Option<tidb_expr::schema::Schema>,
    /// Go `IdxMergePartPlans`: the real plans of an index-merge reader while
    /// `IndexPlanFinished` is false.
    pub idx_merge_part_plans: Vec<PhysicalPlan>,
    /// Go `IdxMergeIsIntersection`.
    pub idx_merge_is_intersection: bool,
    /// Go `IdxMergeAccessMVIndex`.
    pub idx_merge_access_mv_index: bool,
    /// Merge ordering feedback used by parent LIMIT/TopN attachment.
    pub idx_merge_order: Option<IndexMergeOrder>,
    /// Go `RootTaskConds`: selections carrying virtual columns, which cannot
    /// push to TiKV.
    pub root_task_conds: Vec<Expression>,
    /// Immutable session estimator settings used for root-only conditions.
    pub stats_context: TaskStatsContext,
    /// Go `ExpectCnt`: the upper task's expected row count, `0` for
    /// unlimited; decides paging distsql.
    pub expect_cnt: u64,
    /// Go `CopTask.IndexJoinInfo`, produced only while this data source is an
    /// index-join inner child.
    pub index_join_info: Option<IndexJoinInfo>,
    /// Go `PartialOrderMatchResult`, set when a prefix index supplies the
    /// partial order requested by a TopN child property.
    pub partial_order_match_result: Option<tidb_planner_property::PartialOrderMatchResult>,
    /// Go `PhysPlanPartInfo`: shared with the table scan below and carried
    /// onto the reader this task becomes.
    pub phys_plan_part_info: Option<std::sync::Arc<crate::PhysPlanPartInfo>>,
    // boundary: `ExtraHandleCol`,
    // `TblColHists`, `TblCols`,
    /// Go `Warnings`.
    pub warnings: SimpleWarnings,
}

impl CopTask {
    /// Go `Invalid`: no table half, no index half, no index-merge parts.
    #[must_use]
    pub fn invalid(&self) -> bool {
        self.table_plan.is_none()
            && self.index_plan.is_none()
            && self.idx_merge_part_plans.is_empty()
    }

    /// Go `Plan` (`task_base.go:470-476`): the table half once the index
    /// half is sealed, the index half before. Go's comment warns this is
    /// wrong for an index-merge reader whose real plans sit in
    /// `IdxMergePartPlans` — that quirk is Go's, kept.
    #[must_use]
    pub fn plan(&self) -> Option<&PhysicalPlan> {
        if self.index_plan_finished {
            self.table_plan.as_deref()
        } else {
            self.index_plan.as_deref()
        }
    }

    /// Go `Count`: the row count of whichever half [`CopTask::plan`] reads.
    /// Panics where Go nil-derefs — an invalid task has no count.
    #[must_use]
    pub fn count(&self) -> f64 {
        let plan = if self.index_plan_finished {
            self.table_plan.as_deref()
        } else {
            self.index_plan.as_deref()
        };
        plan.and_then(|plan| plan.base().base.stats_info())
            .map_or_else(
                || panic!("CopTask.Count on a task with no stats: Go nil-derefs here"),
                tidb_planner_property::stats_info::StatsInfo::row_count,
            )
    }

    /// Go `Copy`: struct copy plus a fresh warnings slice.
    #[must_use]
    pub fn copy(&self) -> CopTask {
        self.clone()
    }

    /// Go `FinishIndexPlan` (`task.go:64-81`): seal the index half. In the
    /// double-read case the table half ADOPTS the index half's stats,
    /// because the table read sees exactly the rows the index read found.
    ///
    /// Go re-pins `StatsVersion` from the original table stats; the ported
    /// profile has no version field, so the pin is a named narrowing (module
    /// header).
    pub fn finish_index_plan(&mut self) {
        if self.index_plan_finished {
            return;
        }
        self.index_plan_finished = true;
        if let (Some(table), Some(index)) =
            (self.table_plan.as_deref_mut(), self.index_plan.as_deref())
        {
            let index_stats = index.base().base.stats_info().cloned();
            // Go calls FinishIndexPlan before datasource table filters are
            // attached. Other task transformations may retain a wrapper;
            // index output stats still belong to the bottom row-ID scan.
            fn bottom(plan: &PhysicalPlan) -> &PhysicalPlan {
                plan.children().first().map_or(plan, bottom)
            }
            fn set_bottom_stats(
                plan: &mut PhysicalPlan,
                stats: Option<tidb_planner_property::stats_info::StatsInfo>,
            ) {
                let Some(child) = plan.base_mut().children_mut().first_mut() else {
                    plan.base_mut().base.set_stats(stats);
                    return;
                };
                set_bottom_stats(child, stats);
            }
            // Go "keep the original stats version": the row-ID scan takes the
            // index plan's profile but keeps the version it was created with
            // (the table's), through the profile object the two share.
            let origin_version = bottom(table)
                .base()
                .base
                .stats_info()
                .map(tidb_planner_property::stats_info::StatsInfo::stats_version);
            let index_stats = match (index_stats, origin_version) {
                (Some(stats), Some(version)) => Some(stats.with_stats_version(version)),
                (stats, _) => stats,
            };
            set_bottom_stats(table, index_stats);
        }
    }

    /// Go `CopTask.convertToRootTaskImpl` (`task_base.go:509`), the
    /// TABLE-ONLY branch: seal the index half, walk to the bottom
    /// `PhysicalTableScan`, and wrap the pushed-down plan in a
    /// `PhysicalTableReader` carrying the scan's store type — with the
    /// task's warnings copied onto the fresh root task, Go's deferred tail.
    ///
    /// Index-only, double-read, and index-merge branches build their matching
    /// retained readers before the table-only tail below. `RootTaskConds`
    /// use `handleRootTaskConds` (`cardinality.Selectivity` over a built
    /// Selection). Virtual dependencies are expanded before reader construction
    /// and an outer projection restores the original output schema.
    /// `IsCommonHandle` comes from the resolved table or retained handle columns.
    /// Readers receive fresh plan IDs from the statement allocator.
    /// Go `CopTask.handleRootTaskConds` (`physicalop/task.go:47`): the
    /// conditions that could not push down (virtual columns) become a
    /// `PhysicalSelection` at root, `FromDataSource`, its stats scaled by
    /// `cardinality.Selectivity(ctx, t.TblColHists, t.RootTaskConds, nil)`.
    /// The retained source profile supplies the histogram population; the
    /// reader profile is only the output being scaled. Go's error fallback
    /// (`cost.SelectionFactor`) applies only when the estimate cannot be
    /// computed. The immutable settings come from the task's statement
    /// snapshot, because conversion may occur after the dispatcher returns.
    fn handle_root_task_conds(
        conds: Vec<Expression>,
        mut root: RootTask,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        stats_context: TaskStatsContext,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> RootTask {
        if conds.is_empty() {
            return root;
        }
        let Some(plan) = root.take_plan() else {
            return root;
        };
        let selectivity = stats_context
            .root_filter_stats
            .as_deref()
            .or_else(|| plan.stats_info())
            .and_then(|stats| {
                // Go `cardinality.Selectivity`; an estimation error is
                // `cost.SelectionFactor`.
                let mv_index: &dyn tidb_planner_cardinality::selectivity::MvIndexFilters =
                    match stats_context.mv_index_filters {
                        Some(filters) => filters,
                        None => &tidb_planner_cardinality::selectivity::NoMvIndexFilters,
                    };
                let hist_coll = stats.hist_coll()?;
                Some(
                    tidb_planner_cardinality::selectivity::selectivity(
                        &tidb_planner_cardinality::selectivity::SelectivityContext::new(
                            &stats_context.estimator_options,
                            evaluate,
                            mv_index,
                        ),
                        hist_coll,
                        &conds,
                        None,
                    )
                    .unwrap_or(tidb_planner_core_cost::SELECTION_FACTOR),
                )
            })
            .unwrap_or(tidb_planner_core_cost::SELECTION_FACTOR);
        let mut base = crate::BasePhysicalPlan::new(
            allocator,
            "Selection",
            plan.query_block_offset(),
        );
        base.base.set_stats(
            plan.stats_info()
                .map(|stats| stats.scale(selectivity, stats_context.scale_ndv_skew_ratio)),
        );
        base.base.set_schema(plan.schema().cloned());
        base.set_children(vec![plan]);
        let selection = PhysicalPlan::Selection(crate::PhysicalSelection {
            base,
            conditions: conds,
            from_data_source: true,
        });
        root.set_plan(selection);
        root
    }

    /// Go `BuildIndexLookUpTask` (`physical_indexlookup_reader.go:284`): the
    /// double-read cop task becomes a root task holding a
    /// `PhysicalIndexLookUpReader` whose schema and stats are the TABLE
    /// side's (`Init`, `:205`). The `NeedExtraProj` projection restores
    /// `OriginSchema`. Go skips that projection when the table side
    /// already holds a pushed partial aggregate.
    fn build_index_look_up_task(
        mut self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        let mut index_plan = self
            .index_plan
            .take()
            .ok_or_else(|| PlanError::internal("BuildIndexLookUpTask without an index half"))?;
        let mut table_plan = self
            .table_plan
            .take()
            .ok_or_else(|| PlanError::internal("BuildIndexLookUpTask without a table half"))?;
        let agg_pushed_down = matches!(
            &*table_plan,
            PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_)
        );
        let mut base = crate::BasePhysicalPlan::new(
            allocator,
            "IndexLookUp",
            table_plan.query_block_offset(),
        );
        base.base.set_stats(table_plan.stats_info().cloned());
        base.base.set_schema(table_plan.schema().cloned());
        let mut index_lookup_push_down = if self.index_lookup_push_down_by
            == tidb_planner_util::path::IndexLookupPushDownBy::None
        {
            false
        } else if self.keep_order {
            if self.index_lookup_push_down_by == tidb_planner_util::path::IndexLookupPushDownBy::Hint {
                self.warnings.append_warning(
                    "hint INDEX_LOOKUP_PUSHDOWN is inapplicable, keep order is not supported.",
                );
            }
            false
        } else {
            true
        };
        if index_lookup_push_down {
            fn reset_plan_ids(
                plan: &mut PhysicalPlan,
                allocator: &tidb_planner_core_base::PlanIdAllocator,
            ) {
                plan.base_mut().base.set_id(allocator.alloc());
                for child in plan.base_mut().children_mut() {
                    reset_plan_ids(child, allocator);
                }
            }

            fn attach_local_lookup(
                mut table_subtree: PhysicalPlan,
                index_subtree: PhysicalPlan,
                index_handle_offsets: Vec<u32>,
                allocator: &tidb_planner_core_base::PlanIdAllocator,
                query_block_offset: i32,
            ) -> Result<PhysicalPlan, PlanError> {
                if matches!(table_subtree, PhysicalPlan::TableScan(_)) {
                    let stats = table_subtree.stats_info().cloned();
                    let schema = table_subtree.schema().cloned();
                    let mut base = crate::BasePhysicalPlan::new(
                        allocator,
                        "LocalIndexLookUp",
                        query_block_offset,
                    );
                    base.base.set_stats(stats);
                    base.base.set_schema(schema);
                    base.set_children(vec![index_subtree, table_subtree]);
                    return Ok(PhysicalPlan::LocalIndexLookUp(
                        crate::PhysicalLocalIndexLookUp {
                            base,
                            index_handle_offsets,
                        },
                    ));
                }
                if table_subtree.children().len() != 1 {
                    return Err(PlanError::internal(
                        "buildPushDownIndexLookUpPlan requires a unary table plan ending in TableScan",
                    ));
                }
                let child = table_subtree
                    .base_mut()
                    .take_children()
                    .pop()
                    .expect("the unary table subtree has one child");
                let replacement = attach_local_lookup(
                    child,
                    index_subtree,
                    index_handle_offsets,
                    allocator,
                    query_block_offset,
                )?;
                table_subtree.set_children(vec![replacement]);
                Ok(table_subtree)
            }

            fn zero_stats(plan: &mut PhysicalPlan) {
                let stats = plan.stats_info().map(|stats| stats.scale(0.0, 1.0));
                plan.base_mut().base.set_stats(stats);
                for child in plan.base_mut().children_mut() {
                    zero_stats(child);
                }
            }

            let rewrite = (|| {
                let mut cloned_table_plan = table_plan.deep_clone();
                reset_plan_ids(&mut cloned_table_plan, allocator);
                let index_handle_offsets = if self.common_handle_cols.is_empty() {
                    let schema = index_plan.schema().ok_or_else(|| {
                        PlanError::internal("buildPushDownIndexLookUpPlan requires an index schema")
                    })?;
                    let offset = schema
                        .columns
                        .iter()
                        .rposition(|column| {
                            column.id >= 0
                                || column.id == tidb_planner_core_operator_logicalop::data_source::EXTRA_HANDLE_ID
                        })
                        .ok_or_else(|| {
                            PlanError::internal("cannot find handle column in index schema")
                        })?;
                    vec![u32::try_from(offset).expect("a schema offset fits in u32")]
                } else {
                    Vec::new()
                };
                attach_local_lookup(
                    cloned_table_plan,
                    index_plan.deep_clone(),
                    index_handle_offsets,
                    allocator,
                    table_plan.query_block_offset(),
                )
            })();
            if let Ok(rewritten) = rewrite {
                index_plan = Box::new(rewritten);
                zero_stats(&mut table_plan);
                base.base.set_stats(index_plan.stats_info().cloned());
            } else {
                // Go treats this as an internal assertion/log entry and keeps
                // the ordinary two-phase lookup plan user-visible.
                index_lookup_push_down = false;
            }
        }
        let table_plans = crate::flatten_list_push_down_plan(&table_plan);
        let (index_plans, index_plans_un_natural_orders) =
            crate::flatten_tree_push_down_plan(&index_plan);
        let reader = PhysicalPlan::IndexLookUpReader(crate::PhysicalIndexLookUpReader {
            base,
            index_plan: Some(index_plan),
            table_plan: Some(table_plan),
            index_plans,
            index_plans_un_natural_orders,
            table_plans,
            common_handle_cols: self.common_handle_cols,
            index_lookup_push_down,
            keep_order: self.keep_order,
            expect_cnt: self.expect_cnt,
            paging: false,
            pushed_limit: None,
            plan_part_info: self.phys_plan_part_info.clone(),
        });
        let reader = if self.need_extra_proj && !agg_pushed_down {
            Self::project_origin_schema(reader, self.origin_schema.take(), allocator)?
        } else {
            reader
        };
        let mut root = RootTask::default();
        root.set_plan(reader);
        root.index_join_info = self.index_join_info.take();
        if self.warnings.warning_count() > 0 {
            root.warnings.copy_of(&self.warnings);
        }
        Ok(Task::Root(root))
    }

    pub fn convert_to_root_task_impl(
        self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        self.convert_to_root_task_impl_in(allocator, &tidb_ranger::points::evaluate_static)
    }

    fn convert_to_root_task_impl_in(
        mut self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> Result<Task, PlanError> {
        // Go calls `FinishIndexPlan` first, for every branch: a double read
        // converted right after a Limit attached to its open index half
        // gives the table scan that Limit's estimate.
        self.finish_index_plan();
        let origin = self
            .table_plan
            .as_deref()
            .and_then(PhysicalPlan::schema)
            .cloned();
        if let Some(plan) = self.table_plan.as_deref_mut() {
            virtual_columns::expand(plan)?;
        }
        let expanded = origin.as_ref().is_some_and(|origin| {
            self.table_plan
                .as_deref()
                .and_then(PhysicalPlan::schema)
                .is_some_and(|schema| schema.len() != origin.len())
        });
        if expanded && !self.need_extra_proj {
            self.need_extra_proj = true;
            self.origin_schema = origin;
        }
        self.convert_expanded_to_root_task(allocator, evaluate)
    }

    fn project_origin_schema(
        plan: PhysicalPlan,
        schema: Option<tidb_expr::schema::Schema>,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
    ) -> Result<PhysicalPlan, PlanError> {
        let schema = schema.ok_or_else(|| {
            PlanError::internal("NeedExtraProj requires the cop task's OriginSchema")
        })?;
        let mut base = crate::BasePhysicalPlan::new(
            allocator,
            "Projection",
            plan.query_block_offset(),
        );
        base.base.set_stats(plan.stats_info().cloned());
        base.base.set_schema(Some(schema.clone()));
        base.set_children(vec![plan]);
        Ok(PhysicalPlan::Projection(
            crate::PhysicalProjection {
                base,
                exprs: schema.columns.into_iter().map(Expression::Column).collect(),
                calculate_no_delay: false,
                avoid_column_evaluator: false,
            },
        ))
    }

    fn convert_expanded_to_root_task(
        mut self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> Result<Task, PlanError> {
        let stats_context = self.stats_context.clone();
        if !self.idx_merge_part_plans.is_empty() {
            let partial_plans_raw = std::mem::take(&mut self.idx_merge_part_plans);
            let first = partial_plans_raw
                .first()
                .expect("the index-merge branch requires a partial plan");
            let mut base = crate::BasePhysicalPlan::new(
                allocator,
                "IndexMerge",
                first.query_block_offset(),
            );
            let table_plan = self.table_plan.take();
            let receipt = table_plan.as_deref().unwrap_or(first);
            base.base.set_stats(receipt.stats_info().cloned());
            base.base.set_schema(receipt.schema().cloned());
            let reader =
                PhysicalPlan::IndexMergeReader(crate::PhysicalIndexMergeReader {
                    base,
                    partial_plans_raw,
                    table_plan,
                    is_intersection_type: self.idx_merge_is_intersection,
                    access_mv_index: self.idx_merge_access_mv_index,
                    pushed_limit: None,
                    by_items: if self.keep_order {
                        self.idx_merge_order
                            .take()
                            .map_or_else(Vec::new, |order| order.by_items)
                    } else {
                        Vec::new()
                    },
                    keep_order: self.keep_order,
                    plan_part_info: self.phys_plan_part_info.clone(),
                });
            let reader = if self.need_extra_proj {
                Self::project_origin_schema(reader, self.origin_schema.take(), allocator)?
            } else {
                reader
            };
            let mut root = RootTask::default();
            root.set_plan(reader);
            root.index_join_info = self.index_join_info.take();
            if self.warnings.warning_count() > 0 {
                root.warnings.copy_of(&self.warnings);
            }
            let conds = std::mem::take(&mut self.root_task_conds);
            return Ok(Task::Root(Self::handle_root_task_conds(
                conds,
                root,
                allocator,
                stats_context,
                evaluate,
            )));
        }
        if self.index_plan.is_some() && self.table_plan.is_some() {
            let conds = std::mem::take(&mut self.root_task_conds);
            return self
                .build_index_look_up_task(allocator)
                .map(|task| match task {
                    Task::Root(root) => Task::Root(Self::handle_root_task_conds(
                        conds,
                        root,
                        allocator,
                        stats_context,
                        evaluate,
                    )),
                    other => other,
                });
        }
        if let Some(index_plan) = self.index_plan.take() {
            // Go's index branch (`task_base.go:563`): wrap the pushed-down
            // index plan in a PhysicalIndexReader carrying its stats, at its
            // query-block offset. The reader reuses the pushed plan's id —
            // the same named narrowing as the table branch.
            let mut base = crate::BasePhysicalPlan::new(
                allocator,
                "IndexReader",
                index_plan.query_block_offset(),
            );
            base.base.set_stats(index_plan.stats_info().cloned());
            // Go PhysicalIndexReader.SetSchema keeps aggregate/projection
            // output, otherwise it exposes the scan's DataSourceSchema.
            // Other unary pushed operators retain the leaf datasource output.
            let output_schema = if matches!(
                &*index_plan,
                PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_) | PhysicalPlan::Projection(_)
            ) {
                index_plan.schema()
            } else {
                let mut scan = &*index_plan;
                while let [child] = scan.children() {
                    scan = child;
                }
                match scan {
                    PhysicalPlan::IndexScan(scan) => {
                        Some(scan.data_source_schema.as_deref().ok_or_else(|| {
                            PlanError::internal(
                                "IndexReader requires the index scan's DataSourceSchema",
                            )
                        })?)
                    }
                    _ => index_plan.schema(),
                }
            };
            base.base.set_schema(output_schema.cloned());
            let output_columns = output_schema
                .map(|schema| schema.columns.clone())
                .unwrap_or_default();
            let reader = PhysicalPlan::IndexReader(crate::PhysicalIndexReader {
                base,
                index_plan: Some(index_plan),
                output_columns,
                plan_part_info: self.phys_plan_part_info.clone(),
            });
            let mut root = RootTask::default();
            root.set_plan(reader);
            root.index_join_info = self.index_join_info.take();
            if self.warnings.warning_count() > 0 {
                root.warnings.copy_of(&self.warnings);
            }
            let conds = std::mem::take(&mut self.root_task_conds);
            return Ok(Task::Root(Self::handle_root_task_conds(
                conds,
                root,
                allocator,
                stats_context,
                evaluate,
            )));
        }
        self.finish_index_plan();
        let Some(table_plan) = self.table_plan.take() else {
            return Err(PlanError::internal(
                "convertToRootTaskImpl: a cop task with neither half",
            ));
        };
        let mut bottom = &*table_plan;
        while let Some(child) = bottom.children().first() {
            bottom = child;
        }
        let PhysicalPlan::TableScan(scan) = bottom else {
            return Err(PlanError::internal(format!(
                "convertToRootTaskImpl: the bottom of the table half is a {}, \
                 not a PhysicalTableScan — Go type-asserts here",
                bottom.tp()
            )));
        };
        let store_type = scan.store_type;
        let projection_stats = scan.base.base.stats_info().cloned();
        let agg_pushed_down = matches!(
            &*table_plan,
            PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_)
        );
        let is_common_handle = scan
            .resolved_is_common_handle()
            .unwrap_or(!self.common_handle_cols.is_empty());
        // Go `adjustReadReqType` (`physical_table_reader.go:299`): a TiFlash
        // store whose table plan is an `PhysicalExchangeSender` is an MPP
        // reader; Go's root conversion for a TiFlash mpp task wraps the
        // fragment in the PassThrough sender
        // (`GenerateRootMPPTasks`, `fragment.go:167`). This port keeps that
        // shape for the single-fragment table scan; multi-fragment MPP
        // planning stays a documented gap.
        let (table_plan, read_req_type) =
            if store_type == tidb_kv::store_type::StoreType::TiFlash {
                let mut sender_base = crate::BasePhysicalPlan::new(
                    allocator,
                    "ExchangeSender",
                    table_plan.query_block_offset(),
                );
                sender_base.base.set_stats(table_plan.stats_info().cloned());
                sender_base.base.set_schema(table_plan.schema().cloned());
                sender_base.set_children(vec![*table_plan]);
                (
                    Box::new(PhysicalPlan::ExchangeSender(
                        crate::PhysicalExchangeSender {
                            base: sender_base,
                            exchange_type: crate::ExchangeType::PassThrough,
                            hash_cols: Vec::new(),
                        },
                    )),
                    crate::physical_table_reader::ReadReqType::Mpp,
                )
            } else {
                (table_plan, crate::physical_table_reader::ReadReqType::Cop)
            };
        let mut base = crate::BasePhysicalPlan::new(
            allocator,
            "TableReader",
            table_plan.query_block_offset(),
        );
        base.base.set_stats(table_plan.stats_info().cloned());
        base.base.set_schema(table_plan.schema().cloned());
        let reader = PhysicalPlan::TableReader(crate::PhysicalTableReader {
            base,
            table_plan: Some(table_plan),
            store_type,
            is_common_handle,
            read_req_type,
            plan_part_info: self.phys_plan_part_info.clone(),
        });
        let reader = if self.need_extra_proj && !agg_pushed_down {
            let mut projection =
                Self::project_origin_schema(reader, self.origin_schema.take(), allocator)?;
            projection.base_mut().base.set_stats(projection_stats);
            projection
        } else {
            reader
        };
        let mut root = RootTask::default();
        root.set_plan(reader);
        root.index_join_info = self.index_join_info.take();
        if self.warnings.warning_count() > 0 {
            root.warnings.copy_of(&self.warnings);
        }
        let conds = std::mem::take(&mut self.root_task_conds);
        Ok(Task::Root(Self::handle_root_task_conds(
            conds,
            root,
            allocator,
            stats_context,
            evaluate,
        )))
    }

    /// Go `GetStoreType` (`task.go:84-96`): walk the table half; more than
    /// one child anywhere means TiFlash, a `PhysicalTableScan` leaf answers
    /// with its own store, anything else is TiKV.
    #[must_use]
    pub fn get_store_type(&self) -> StoreType {
        let Some(mut plan) = self.table_plan.as_deref() else {
            return StoreType::TiKv;
        };
        while !plan.children().is_empty() {
            if plan.children().len() > 1 {
                return StoreType::TiFlash;
            }
            plan = &plan.children()[0];
        }
        if let PhysicalPlan::TableScan(scan) = plan {
            return scan.store_type;
        }
        StoreType::TiKv
    }
}

/// Go `base.Task` (`base/task_base.go:27-44`): the closed set of task kinds,
/// asserted closed by Go itself.
#[derive(Clone, Debug)]
pub enum Task {
    /// `physicalop.RootTask`.
    Root(RootTask),
    /// `physicalop.CopTask`.
    Cop(CopTask),
    /// `physicalop.MppTask`.
    Mpp(MppTask),
}

impl Task {
    /// Go `base.InvalidTask`: "core's empty RootTask", the shared invalid
    /// singleton — here a constructor, since a valueless empty task needs no
    /// global.
    #[must_use]
    pub fn invalid_task() -> Task {
        Task::Root(RootTask::default())
    }

    /// Go `Count()`.
    #[must_use]
    pub fn count(&self) -> f64 {
        match self {
            // Go: `t.p.StatsInfo().RowCount`, nil-deref on an invalid task.
            Task::Root(task) => task.get_plan().base().base.stats_info().map_or_else(
                || panic!("RootTask.Count with no stats: Go nil-derefs here"),
                tidb_planner_property::stats_info::StatsInfo::row_count,
            ),
            Task::Cop(task) => task.count(),
            Task::Mpp(task) => task
                .plan
                .as_deref()
                .and_then(|plan| plan.base().base.stats_info())
                .map_or_else(
                    || panic!("MppTask.Count with no stats: Go nil-derefs here"),
                    tidb_planner_property::stats_info::StatsInfo::row_count,
                ),
        }
    }

    /// Go `Copy()`: a shallow task copy whose warnings slice is its own.
    #[must_use]
    pub fn copy(&self) -> Task {
        match self {
            Task::Root(task) => Task::Root(task.copy()),
            Task::Cop(task) => Task::Cop(task.copy()),
            Task::Mpp(task) => Task::Mpp(task.copy()),
        }
    }

    /// Go `Plan()`. `None` only for the shapes whose Go receiver would be
    /// nil-adjacent: an invalid root/MPP task, or a cop task with neither
    /// half.
    #[must_use]
    pub fn plan(&self) -> Option<&PhysicalPlan> {
        match self {
            Task::Root(task) => task.plan.as_deref(),
            Task::Cop(task) => task.plan(),
            Task::Mpp(task) => task.plan.as_deref(),
        }
    }

    /// Mutable counterpart of [`Self::plan`], used for the physical cost
    /// side effects Go writes onto the selected plan tree.
    pub fn plan_mut(&mut self) -> Option<&mut PhysicalPlan> {
        match self {
            Task::Root(task) => task.plan.as_mut().map(Arc::make_mut),
            Task::Cop(task) => {
                if task.index_plan_finished {
                    task.table_plan.as_deref_mut()
                } else {
                    task.index_plan.as_deref_mut()
                }
            }
            Task::Mpp(task) => task.plan.as_deref_mut(),
        }
    }

    /// Go `Invalid()`.
    #[must_use]
    pub fn invalid(&self) -> bool {
        match self {
            Task::Root(task) => task.plan.is_none(),
            Task::Cop(task) => task.invalid(),
            Task::Mpp(task) => task.plan.is_none(),
        }
    }

    /// Go `ConvertToRootTask(ctx)` for a borrowed task. A retained candidate
    /// keeps its definition; callers that already own the task can move it.
    pub fn convert_to_root_task(
        &self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        self.copy().into_root_task(allocator)
    }

    /// Borrowed conversion using the current statement evaluator.
    pub fn convert_to_root_task_in(
        &self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> Result<Task, PlanError> {
        self.copy().into_root_task_in(allocator, evaluate)
    }

    /// Owned conversion using the current statement evaluator.
    pub fn into_root_task_in(
        self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> Result<Task, PlanError> {
        match self {
            Task::Root(task) => Ok(Task::Root(task)),
            Task::Cop(task) => task.convert_to_root_task_impl_in(allocator, evaluate),
            Task::Mpp(task) => task.into_root_task_in(allocator, evaluate),
        }
    }

    /// Owned form of conversion: no extra plan-tree copy at task attachment.
    pub fn into_root_task(
        self,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        self.into_root_task_in(allocator, &tidb_ranger::points::evaluate_static)
    }

    /// Go `AppendWarning(err)`.
    pub fn append_warning(&mut self, message: impl Into<String>) {
        match self {
            Task::Root(task) => task.warnings.append_warning(message),
            Task::Cop(task) => task.warnings.append_warning(message),
            Task::Mpp(task) => task.warnings.append_warning(message),
        }
    }

    /// Go `Warnings`: the warnings this task and its children recorded.
    #[must_use]
    pub const fn warnings(&self) -> &SimpleWarnings {
        match self {
            Task::Root(task) => &task.warnings,
            Task::Cop(task) => &task.warnings,
            Task::Mpp(task) => &task.warnings,
        }
    }
}

