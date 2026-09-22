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

use crate::physical::PhysicalPlan;
use crate::physical_property::MppPartitionType;
use crate::physical_table_reader::{ReadReqType, StoreType};
use crate::plan_base::PlanError;
use std::sync::Arc;
use tidb_expr::expression::Expression;

mod virtual_columns;

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
    pub ranges: crate::ranger::types::Ranges,
    /// Go `IndexJoinInfo.IdxColLens`.
    pub idx_col_lens: Vec<i64>,
    /// Go `IndexJoinInfo.KeyOff2IdxOff`; `-1` means the selected access cannot
    /// use that logical equality as a lookup key.
    pub key_off2_idx_off: Vec<i64>,
    /// Chosen range predicates used by Go to construct the scan RangeInfo.
    /// Retained as expressions for the executor renderer, not row evaluation.
    pub access_conditions: Vec<tidb_expr::expression::Expression>,
    /// Static template inputs for Go mutableIndexJoinRange on cache reuse.
    pub range_rebuild: Option<crate::physical_plan_cache::PointRangeRebuild>,
    /// Go `IndexJoinInfo.CompareFilters`.
    pub compare_filters: Option<crate::physical::IndexJoinCompareFilters>,
}

/// Go `context.WarnLevelWarning` / `WarnLevelNote` — the two levels this
/// file writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarnLevel {
    /// `WarnLevelWarning`.
    Warning,
    /// `WarnLevelNote`.
    Note,
}

/// Go `context.SQLWarn`, narrowed: the `error` becomes its rendered message.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlWarn {
    /// The warning level.
    pub level: WarnLevel,
    /// The rendered message of Go's wrapped `error`.
    pub message: String,
}

/// Go `physicalop.SimpleWarnings` (`task_base.go:47-107`): the per-task
/// warning slice, copied — never shared — between task instances.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SimpleWarnings {
    warnings: Vec<SqlWarn>,
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
                level: WarnLevel::Warning,
                message: message.into(),
            });
        }
    }

    /// Go `AppendNote`, under the same cap.
    pub fn append_note(&mut self, message: impl Into<String>) {
        if self.warnings.len() < usize::from(u16::MAX) {
            self.warnings.push(SqlWarn {
                level: WarnLevel::Note,
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
    plan: Option<Box<PhysicalPlan>>,
    /// Go's private `partTp`.
    part_tp: MppPartitionType,
    /// Go `HashCols`, retained so exchange enforcement can compare and carry
    /// the current partitioning contract.
    pub hash_cols: Vec<crate::physical_property::MppPartitionColumn>,
    /// Go `RootTaskConds`: TableScan filters TiFlash cannot take, executed
    /// in a TiDB-side Selection when the task converts to root.
    pub root_task_conds: Vec<Expression>,
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
        hash_cols: Vec<crate::physical_property::MppPartitionColumn>,
        warnings: impl IntoIterator<Item = SimpleWarnings>,
    ) -> MppTask {
        let mut task = MppTask {
            plan: Some(Box::new(plan)),
            part_tp,
            hash_cols,
            root_task_conds: Vec::new(),
            warnings: SimpleWarnings::default(),
        };
        let sources: Vec<SimpleWarnings> = warnings.into_iter().collect();
        task.warnings.copy_from(sources.iter());
        task
    }

    /// Go `GetPartitionType`.
    #[must_use]
    pub fn partition_type(&self) -> MppPartitionType {
        self.part_tp
    }

    /// Go `GetHashCols`.
    #[must_use]
    pub fn hash_cols(&self) -> &[crate::physical_property::MppPartitionColumn] {
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
        mut self,
        allocator: &crate::plan_base::PlanIdAllocator,
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

        let mut sender_base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "ExchangeSender",
            plan.query_block_offset(),
        );
        sender_base.base.set_stats(plan.stats_info().cloned());
        sender_base.base.set_schema(plan.schema().cloned());
        sender_base.set_children(vec![*plan]);
        let sender = PhysicalPlan::ExchangeSender(crate::physical::PhysicalExchangeSender {
            base: sender_base,
            exchange_type: crate::physical::ExchangeType::PassThrough,
            hash_cols: Vec::new(),
        });

        let mut reader_base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "TableReader",
            sender.query_block_offset(),
        );
        reader_base.base.set_stats(sender.stats_info().cloned());
        reader_base.base.set_schema(sender.schema().cloned());
        let reader = PhysicalPlan::TableReader(crate::physical::PhysicalTableReader {
            base: reader_base,
            table_plan: Some(Box::new(sender)),
            store_type: StoreType::TiFlash,
            is_common_handle: false,
            read_req_type: ReadReqType::Mpp,
        });

        let mut root = RootTask::default();
        root.set_plan(reader);
        if self.warnings.warning_count() > 0 {
            root.warnings.copy_of(&self.warnings);
        }
        let conds = std::mem::take(&mut self.root_task_conds);
        Ok(Task::Root(CopTask::handle_root_task_conds(
            conds, root, allocator,
        )))
    }
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
    pub index_lookup_push_down_by: crate::access_path::IndexLookupPushDownBy,
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
    /// Go `RootTaskConds`: selections carrying virtual columns, which cannot
    /// push to TiKV.
    pub root_task_conds: Vec<Expression>,
    /// Go `ExpectCnt`: the upper task's expected row count, `0` for
    /// unlimited; decides paging distsql.
    pub expect_cnt: u64,
    /// Go `CopTask.IndexJoinInfo`, produced only while this data source is an
    /// index-join inner child.
    pub index_join_info: Option<IndexJoinInfo>,
    /// Go `PartialOrderMatchResult`, set when a prefix index supplies the
    /// partial order requested by a TopN child property.
    pub partial_order_match_result: Option<crate::physical_property::PartialOrderMatchResult>,
    // boundary: `ExtraHandleCol`,
    // `TblColHists`, `TblCols`,
    // `IdxMergeMatchWithAdvisorySortItems`, `IdxMergePartPlansMatchResults`,
    // `PhysPlanPartInfo`,
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
                crate::stats_info::StatsInfo::row_count,
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
            // Go calls `FinishIndexPlan` before adding table filters. Rust's
            // DataSource conversion builds the same tree in one pass, so a
            // table-side Selection may already be present: the index stats
            // still belong to the bottom row-ID scan, not to that filter.
            fn set_bottom_stats(
                plan: &mut PhysicalPlan,
                stats: Option<crate::stats_info::StatsInfo>,
            ) {
                let Some(child) = plan.base_mut().children_mut().first_mut() else {
                    plan.base_mut().base.set_stats(stats);
                    return;
                };
                set_bottom_stats(child, stats);
            }
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
    /// The child plan's profile carries that HistColl, so the same call runs
    /// here through the Selectivity port; Go's own error fallback
    /// (`cost.SelectionFactor`) applies only when the estimate cannot be
    /// computed. The skew ratio is Go's default 1.0.
    fn handle_root_task_conds(
        conds: Vec<Expression>,
        mut root: RootTask,
        allocator: &crate::plan_base::PlanIdAllocator,
    ) -> RootTask {
        if conds.is_empty() {
            return root;
        }
        let Some(plan) = root.take_plan() else {
            return root;
        };
        let selectivity = plan
            .stats_info()
            .and_then(|stats| crate::logical::rewrite::analyzed_filter_selectivity(stats, &conds))
            .filter(|value| *value > 0.0)
            .unwrap_or(crate::cost_factors::SELECTION_FACTOR);
        let mut base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "Selection",
            plan.query_block_offset(),
        );
        base.base
            .set_stats(plan.stats_info().map(|stats| stats.scale(selectivity, 1.0)));
        base.base.set_schema(plan.schema().cloned());
        base.set_children(vec![plan]);
        let selection = PhysicalPlan::Selection(crate::physical::PhysicalSelection {
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
        allocator: &crate::plan_base::PlanIdAllocator,
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
        let mut base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "IndexLookUp",
            table_plan.query_block_offset(),
        );
        base.base.set_stats(table_plan.stats_info().cloned());
        base.base.set_schema(table_plan.schema().cloned());
        let mut index_lookup_push_down = if self.index_lookup_push_down_by
            == crate::access_path::IndexLookupPushDownBy::None
        {
            false
        } else if self.keep_order {
            if self.index_lookup_push_down_by == crate::access_path::IndexLookupPushDownBy::Hint {
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
                allocator: &crate::plan_base::PlanIdAllocator,
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
                allocator: &crate::plan_base::PlanIdAllocator,
                query_block_offset: i32,
            ) -> Result<PhysicalPlan, PlanError> {
                if matches!(table_subtree, PhysicalPlan::TableScan(_)) {
                    let stats = table_subtree.stats_info().cloned();
                    let schema = table_subtree.schema().cloned();
                    let mut base = crate::physical::BasePhysicalPlan::new(
                        allocator,
                        "LocalIndexLookUp",
                        query_block_offset,
                    );
                    base.base.set_stats(stats);
                    base.base.set_schema(schema);
                    base.set_children(vec![index_subtree, table_subtree]);
                    return Ok(PhysicalPlan::LocalIndexLookUp(
                        crate::physical::PhysicalLocalIndexLookUp {
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
                                || column.id == crate::logical::data_source::EXTRA_HANDLE_ID
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
        let table_plans = crate::physical::flatten_list_push_down_plan(&table_plan);
        let (index_plans, index_plans_un_natural_orders) =
            crate::physical::flatten_tree_push_down_plan(&index_plan);
        let reader = PhysicalPlan::IndexLookUpReader(crate::physical::PhysicalIndexLookUpReader {
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
        mut self,
        allocator: &crate::plan_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
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
        self.convert_expanded_to_root_task(allocator)
    }

    fn project_origin_schema(
        plan: PhysicalPlan,
        schema: Option<tidb_expr::schema::Schema>,
        allocator: &crate::plan_base::PlanIdAllocator,
    ) -> Result<PhysicalPlan, PlanError> {
        let schema = schema.ok_or_else(|| {
            PlanError::internal("NeedExtraProj requires the cop task's OriginSchema")
        })?;
        let mut base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "Projection",
            plan.query_block_offset(),
        );
        base.base.set_stats(plan.stats_info().cloned());
        base.base.set_schema(Some(schema.clone()));
        base.set_children(vec![plan]);
        Ok(PhysicalPlan::Projection(
            crate::physical::PhysicalProjection {
                base,
                exprs: schema.columns.into_iter().map(Expression::Column).collect(),
                calculate_no_delay: false,
                avoid_column_evaluator: false,
            },
        ))
    }

    fn convert_expanded_to_root_task(
        mut self,
        allocator: &crate::plan_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        if !self.idx_merge_part_plans.is_empty() {
            let partial_plans_raw = std::mem::take(&mut self.idx_merge_part_plans);
            let first = partial_plans_raw
                .first()
                .expect("the index-merge branch requires a partial plan");
            let mut base = crate::physical::BasePhysicalPlan::new(
                allocator,
                "IndexMerge",
                first.query_block_offset(),
            );
            let table_plan = self.table_plan.take();
            let receipt = table_plan.as_deref().unwrap_or(first);
            base.base.set_stats(receipt.stats_info().cloned());
            base.base.set_schema(receipt.schema().cloned());
            let reader =
                PhysicalPlan::IndexMergeReader(crate::physical::PhysicalIndexMergeReader {
                    base,
                    partial_plans_raw,
                    table_plan,
                    is_intersection_type: self.idx_merge_is_intersection,
                    access_mv_index: self.idx_merge_access_mv_index,
                    pushed_limit: None,
                    by_items: Vec::new(),
                    keep_order: self.keep_order,
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
                conds, root, allocator,
            )));
        }
        if self.index_plan.is_some() && self.table_plan.is_some() {
            let conds = std::mem::take(&mut self.root_task_conds);
            return self
                .build_index_look_up_task(allocator)
                .map(|task| match task {
                    Task::Root(root) => {
                        Task::Root(Self::handle_root_task_conds(conds, root, allocator))
                    }
                    other => other,
                });
        }
        if let Some(index_plan) = self.index_plan.take() {
            // Go's index branch (`task_base.go:563`): wrap the pushed-down
            // index plan in a PhysicalIndexReader carrying its stats, at its
            // query-block offset. The reader reuses the pushed plan's id —
            // the same named narrowing as the table branch.
            let mut base = crate::physical::BasePhysicalPlan::new(
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
            let reader = PhysicalPlan::IndexReader(crate::physical::PhysicalIndexReader {
                base,
                index_plan: Some(index_plan),
                output_columns,
            });
            let mut root = RootTask::default();
            root.set_plan(reader);
            root.index_join_info = self.index_join_info.take();
            if self.warnings.warning_count() > 0 {
                root.warnings.copy_of(&self.warnings);
            }
            let conds = std::mem::take(&mut self.root_task_conds);
            return Ok(Task::Root(Self::handle_root_task_conds(
                conds, root, allocator,
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
            if store_type == crate::physical_table_reader::StoreType::TiFlash {
                let mut sender_base = crate::physical::BasePhysicalPlan::new(
                    allocator,
                    "ExchangeSender",
                    table_plan.query_block_offset(),
                );
                sender_base.base.set_stats(table_plan.stats_info().cloned());
                sender_base.base.set_schema(table_plan.schema().cloned());
                sender_base.set_children(vec![*table_plan]);
                (
                    Box::new(PhysicalPlan::ExchangeSender(
                        crate::physical::PhysicalExchangeSender {
                            base: sender_base,
                            exchange_type: crate::physical::ExchangeType::PassThrough,
                            hash_cols: Vec::new(),
                        },
                    )),
                    crate::physical_table_reader::ReadReqType::Mpp,
                )
            } else {
                (table_plan, crate::physical_table_reader::ReadReqType::Cop)
            };
        let mut base = crate::physical::BasePhysicalPlan::new(
            allocator,
            "TableReader",
            table_plan.query_block_offset(),
        );
        base.base.set_stats(table_plan.stats_info().cloned());
        base.base.set_schema(table_plan.schema().cloned());
        let reader = PhysicalPlan::TableReader(crate::physical::PhysicalTableReader {
            base,
            table_plan: Some(table_plan),
            store_type,
            is_common_handle,
            read_req_type,
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
            conds, root, allocator,
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
                crate::stats_info::StatsInfo::row_count,
            ),
            Task::Cop(task) => task.count(),
            Task::Mpp(task) => task
                .plan
                .as_deref()
                .and_then(|plan| plan.base().base.stats_info())
                .map_or_else(
                    || panic!("MppTask.Count with no stats: Go nil-derefs here"),
                    crate::stats_info::StatsInfo::row_count,
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
        allocator: &crate::plan_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        self.copy().into_root_task(allocator)
    }

    /// Owned form of conversion: no extra plan-tree copy at task attachment.
    pub fn into_root_task(
        self,
        allocator: &crate::plan_base::PlanIdAllocator,
    ) -> Result<Task, PlanError> {
        match self {
            Task::Root(task) => Ok(Task::Root(task)),
            Task::Cop(task) => task.convert_to_root_task_impl(allocator),
            Task::Mpp(task) => task.into_root_task(allocator),
        }
    }

    /// Go `AppendWarning(err)`.
    pub fn append_warning(&mut self, message: impl Into<String>) {
        match self {
            Task::Root(task) => task.warnings.append_warning(message),
            Task::Cop(task) => task.warnings.append_warning(message),
            Task::Mpp(task) => task.warnings.append_warning(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::PhysicalPlan;
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::StatsInfo;
    // All WRITTEN: Go's coverage of the task layer is exercised through
    // planner integration suites; `task_base.go` has no unit tests of its
    // own beside them.

    fn dual_with_rows(rows: f64) -> PhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = crate::physical::BasePhysicalPlan::default();
        base.base =
            crate::plan_base::BasePlan::new(&allocator, crate::logical::LogicalTableDual::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        base.base
            .set_schema(Some(tidb_expr::schema::Schema::new(vec![
                tidb_expr::column::Column::new(
                    1,
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
            ])));
        PhysicalPlan::TableDual(crate::physical::PhysicalTableDual { base, row_count: 0 })
    }

    fn table_scan_with_rows(rows: f64) -> PhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = crate::physical::BasePhysicalPlan::default();
        base.base = crate::plan_base::BasePlan::new(&allocator, "TableScan", 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
            base,
            table_id: 1,
            table_as_name: None,
            dynamic_partition_access: None,
            cost_columns: Vec::new(),
            store_type: crate::physical_table_reader::StoreType::TiKv,
            keep_order: false,
            desc: false,
            ranges: crate::ranger::types::Ranges::new(),
            ..Default::default()
        })
    }

    #[test]
    fn mpp_task_converts_to_tiflash_reader_with_passthrough_sender() {
        let task = Task::Mpp(MppTask::new(
            table_scan_with_rows(7.0),
            MppPartitionType::Any,
            [],
        ));
        let converted = task
            .into_root_task(&PlanIdAllocator::new())
            .expect("the MPP fragment converts at the root boundary");
        let Task::Root(root) = converted else {
            panic!("MPP conversion must produce a root task");
        };
        let PhysicalPlan::TableReader(reader) = root.get_plan() else {
            panic!("MPP conversion must build a table reader");
        };
        assert_eq!(reader.store_type, StoreType::TiFlash);
        assert_eq!(reader.read_req_type, ReadReqType::Mpp);
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the table reader must own the pass-through sender");
        };
        assert_eq!(
            sender.exchange_type,
            crate::physical::ExchangeType::PassThrough
        );
        assert!(sender.hash_cols.is_empty());
        assert!(matches!(
            sender.base.children(),
            [PhysicalPlan::TableScan(_)]
        ));

        let mut filtered = MppTask::new(table_scan_with_rows(7.0), MppPartitionType::Any, []);
        filtered.root_task_conds = vec![Expression::Column(column_with_id(1))];
        let Task::Root(filtered_root) = Task::Mpp(filtered)
            .into_root_task(&PlanIdAllocator::new())
            .expect("a scan-root condition becomes a root selection")
        else {
            panic!("filtered MPP conversion must produce a root task");
        };
        assert!(matches!(
            filtered_root.get_plan(),
            PhysicalPlan::Selection(_)
        ));

        let mut misplaced = MppTask::new(
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual::default()),
            MppPartitionType::Any,
            [],
        );
        misplaced.root_task_conds = vec![Expression::Column(column_with_id(1))];
        let converted = Task::Mpp(misplaced)
            .into_root_task(&PlanIdAllocator::new())
            .expect("Go returns an invalid task for misplaced root conditions");
        assert!(converted.invalid());
    }

    fn column_with_id(id: i64) -> tidb_expr::column::Column {
        let mut column = tidb_expr::column::Column::new(
            id,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        column.id = id;
        column
    }

    fn scan_with_schema(
        allocator: &PlanIdAllocator,
        tp: &str,
        schema: tidb_expr::schema::Schema,
        rows: f64,
    ) -> PhysicalPlan {
        let mut base = crate::physical::BasePhysicalPlan::new(allocator, tp, 10);
        base.base.set_schema(Some(schema));
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        match tp {
            "TableScan" => PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                base,
                table_id: 1,
                store_type: crate::physical_table_reader::StoreType::TiKv,
                ..crate::physical::PhysicalTableScan::default()
            }),
            "IndexScan" => PhysicalPlan::IndexScan(crate::physical::PhysicalIndexScan {
                base,
                table_id: 1,
                index_id: 1,
                ..crate::physical::PhysicalIndexScan::default()
            }),
            _ => unreachable!("only scan operators are built by this helper"),
        }
    }

    #[test]
    fn index_lookup_pushdown_builds_and_flattens_the_go_plan_shape() {
        fn check(reader: &crate::physical::PhysicalIndexLookUpReader, complex: bool) {
            assert!(reader.index_lookup_push_down);
            assert!(reader.table_plans.iter().all(|plan| {
                plan.stats_info()
                    .is_some_and(|stats| stats.row_count() == 0.0)
            }));

            let mut ids = std::collections::BTreeSet::new();
            for plan in reader.table_plans.iter().chain(&reader.index_plans) {
                assert!(ids.insert(plan.id()), "duplicated plan id {}", plan.id());
            }

            if complex {
                assert_eq!(
                    reader
                        .table_plans
                        .iter()
                        .map(PhysicalPlan::tp)
                        .collect::<Vec<_>>(),
                    ["TableScan", "Selection", "Projection"]
                );
                assert_eq!(
                    reader
                        .index_plans
                        .iter()
                        .map(PhysicalPlan::tp)
                        .collect::<Vec<_>>(),
                    [
                        "IndexScan",
                        "Limit",
                        "TableScan",
                        "LocalIndexLookUp",
                        "Selection",
                        "Projection"
                    ]
                );
                assert_eq!(
                    reader.index_plans_un_natural_orders,
                    std::collections::BTreeMap::from([(1, 3)])
                );
            } else {
                assert_eq!(reader.table_plans[0].tp(), "TableScan");
                assert_eq!(
                    reader
                        .index_plans
                        .iter()
                        .map(PhysicalPlan::tp)
                        .collect::<Vec<_>>(),
                    ["IndexScan", "TableScan", "LocalIndexLookUp"]
                );
                assert_eq!(
                    reader.index_plans_un_natural_orders,
                    std::collections::BTreeMap::from([(0, 2)])
                );
            }

            let local = reader
                .index_plans
                .iter()
                .find_map(|plan| match plan {
                    PhysicalPlan::LocalIndexLookUp(local) => Some(local),
                    _ => None,
                })
                .expect("the flattened local lookup");
            assert_eq!(local.index_handle_offsets, [1]);
            assert_eq!(local.base.base.query_block_offset(), 10);
            assert_eq!(local.base.base.schema().expect("lookup schema").len(), 3);
        }

        let allocator = PlanIdAllocator::new();
        let table_schema = tidb_expr::schema::Schema::new(vec![
            column_with_id(1),
            column_with_id(2),
            column_with_id(3),
        ]);
        let index_schema =
            tidb_expr::schema::Schema::new(vec![column_with_id(2), column_with_id(1)]);

        let simple = Task::Cop(CopTask {
            index_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "IndexScan",
                index_schema.clone(),
                1_000.0,
            ))),
            table_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "TableScan",
                table_schema.clone(),
                1_000.0,
            ))),
            index_lookup_push_down_by: crate::access_path::IndexLookupPushDownBy::Hint,
            ..CopTask::default()
        })
        .convert_to_root_task(&allocator)
        .expect("the simple lookup converts");
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = simple.plan() else {
            panic!("an index lookup reader");
        };
        check(reader, false);
        let cloned = simple.plan().expect("reader").deep_clone();
        let PhysicalPlan::IndexLookUpReader(cloned) = cloned else {
            panic!("the clone remains a reader");
        };
        check(&cloned, false);

        let common_handle = Task::Cop(CopTask {
            index_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "IndexScan",
                index_schema.clone(),
                1_000.0,
            ))),
            table_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "TableScan",
                table_schema.clone(),
                1_000.0,
            ))),
            common_handle_cols: vec![column_with_id(1), column_with_id(2)],
            index_lookup_push_down_by: crate::access_path::IndexLookupPushDownBy::Hint,
            ..CopTask::default()
        })
        .convert_to_root_task(&allocator)
        .expect("the common-handle lookup converts");
        let Some(PhysicalPlan::IndexLookUpReader(common_handle)) = common_handle.plan() else {
            panic!("an index lookup reader");
        };
        let PhysicalPlan::LocalIndexLookUp(local) = &common_handle.index_plans[2] else {
            panic!("the local lookup");
        };
        assert!(local.index_handle_offsets.is_empty());

        let mut table_scan = scan_with_schema(&allocator, "TableScan", table_schema, 500.0);
        let mut selection_base =
            crate::physical::BasePhysicalPlan::new(&allocator, "Selection", 10);
        selection_base.base.set_schema(table_scan.schema().cloned());
        selection_base
            .base
            .set_stats(Some(StatsInfo::new(200.0, [])));
        selection_base.set_children(vec![table_scan]);
        let selection = PhysicalPlan::Selection(crate::physical::PhysicalSelection {
            base: selection_base,
            conditions: Vec::new(),
            from_data_source: true,
        });
        let mut projection_base =
            crate::physical::BasePhysicalPlan::new(&allocator, "Projection", 10);
        projection_base
            .base
            .set_schema(Some(tidb_expr::schema::Schema::new(vec![column_with_id(
                2,
            )])));
        projection_base
            .base
            .set_stats(Some(StatsInfo::new(200.0, [])));
        projection_base.set_children(vec![selection]);
        table_scan = PhysicalPlan::Projection(crate::physical::PhysicalProjection {
            base: projection_base,
            exprs: Vec::new(),
            calculate_no_delay: false,
            avoid_column_evaluator: false,
        });

        let index_scan = scan_with_schema(&allocator, "IndexScan", index_schema, 1_000.0);
        let mut limit_base = crate::physical::BasePhysicalPlan::new(&allocator, "Limit", 10);
        limit_base.base.set_schema(index_scan.schema().cloned());
        limit_base.base.set_stats(Some(StatsInfo::new(1_000.0, [])));
        limit_base.set_children(vec![index_scan]);
        let limit = PhysicalPlan::Limit(crate::physical::PhysicalLimit {
            base: limit_base,
            ..crate::physical::PhysicalLimit::default()
        });
        let complex = Task::Cop(CopTask {
            index_plan: Some(Box::new(limit)),
            table_plan: Some(Box::new(table_scan)),
            index_lookup_push_down_by: crate::access_path::IndexLookupPushDownBy::Hint,
            ..CopTask::default()
        })
        .convert_to_root_task(&allocator)
        .expect("the complex lookup converts");
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = complex.plan() else {
            panic!("an index lookup reader");
        };
        check(reader, true);
    }

    #[test]
    fn a_table_only_cop_task_converts_into_a_table_reader() {
        // `convertToRootTaskImpl`'s table branch (`task_base.go:571`): the
        // pushed-down plan hangs off the reader's TablePlan field, the
        // store type is read off the bottom scan, and the task's warnings
        // ride the deferred copy onto the fresh root task.
        let allocator = PlanIdAllocator::new();
        let mut base = crate::physical::BasePhysicalPlan::default();
        base.base = crate::plan_base::BasePlan::new(&allocator, "TableScan", 0);
        base.base.set_stats(Some(StatsInfo::new(42.0, [])));
        let scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
            base,
            table_id: 9,
            table_as_name: None,
            dynamic_partition_access: None,
            cost_columns: Vec::new(),
            store_type: crate::physical_table_reader::StoreType::TiKv,
            keep_order: false,
            desc: false,
            ranges: crate::ranger::types::Ranges::new(),
            ..Default::default()
        });
        let mut cop = CopTask {
            table_plan: Some(Box::new(scan)),
            index_plan_finished: true,
            ..CopTask::default()
        };
        cop.warnings.append_warning("carried");
        let task = Task::Cop(cop)
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("converts");
        let Task::Root(root) = &task else {
            panic!("a root task");
        };
        assert_eq!(root.warnings.warning_count(), 1, "the deferred copy");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        assert_eq!(
            reader.store_type,
            crate::physical_table_reader::StoreType::TiKv
        );
        assert!(
            matches!(
                reader.table_plan.as_deref(),
                Some(PhysicalPlan::TableScan(_))
            ),
            "the pushed-down side hangs off TablePlan, not the child list"
        );
        assert!(reader.base.children().is_empty());
        assert!(
            (task
                .plan()
                .expect("plan")
                .stats_info()
                .expect("stats")
                .row_count()
                - 42.0)
                .abs()
                < f64::EPSILON,
            "the reader carries the table plan's stats"
        );
    }

    #[test]
    fn cop_pushdown_wraps_uint64_row_bounds_like_go() {
        for is_topn in [false, true] {
            let allocator = PlanIdAllocator::new();
            let child = Task::Cop(CopTask {
                table_plan: Some(Box::new(table_scan_with_rows(100.0))),
                index_plan_finished: true,
                ..CopTask::default()
            });
            let base = crate::physical::BasePhysicalPlan::with_id(9, "Limit", 0);
            let plan = if is_topn {
                PhysicalPlan::TopN(crate::physical::PhysicalTopN {
                    by_items: vec![tidb_expr::aggregation::ByItems::new(
                        Expression::Column(column_with_id(1)),
                        false,
                    )],
                    base,
                    offset: u64::MAX,
                    count: 8,
                    ..Default::default()
                })
            } else {
                PhysicalPlan::Limit(crate::physical::PhysicalLimit {
                    base,
                    offset: u64::MAX,
                    count: 8,
                    ..Default::default()
                })
            };
            let task = attach2_task(plan, vec![child], None, &allocator).unwrap();
            let root = task.plan().unwrap();
            let bound = |plan: &PhysicalPlan| match plan {
                PhysicalPlan::Limit(p) => (p.offset, p.count),
                PhysicalPlan::TopN(p) => (p.offset, p.count),
                _ => panic!("expected row bound: {plan:?}"),
            };
            assert_eq!(bound(root), (u64::MAX, 8));
            let PhysicalPlan::TableReader(reader) = &root.children()[0] else {
                panic!("expected table reader");
            };
            assert_eq!(bound(reader.table_plan.as_deref().unwrap()), (0, 7));
        }
    }

    #[test]
    fn partial_order_topn_pushes_prefix_limit_and_keeps_root_topn() {
        let allocator = PlanIdAllocator::new();
        let schema = tidb_expr::schema::Schema::new(vec![column_with_id(1)]);
        let cop = Task::Cop(CopTask {
            index_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "IndexScan",
                schema.clone(),
                100.0,
            ))),
            table_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "TableScan",
                schema.clone(),
                100.0,
            ))),
            partial_order_match_result: Some(
                crate::physical_property::PartialOrderMatchResult {
                    matched: true,
                    prefix_col: Some(column_with_id(1)),
                    prefix_len: 4,
                },
            ),
            ..CopTask::default()
        });
        let mut topn_base = crate::physical::BasePhysicalPlan::new(&allocator, "TopN", 0);
        topn_base.base.set_schema(Some(schema));
        topn_base.base.set_stats(Some(StatsInfo::new(5.0, [])));
        let topn = PhysicalPlan::TopN(crate::physical::PhysicalTopN {
            base: topn_base,
            by_items: vec![tidb_expr::aggregation::ByItems::new(
                Expression::Column(column_with_id(1)),
                false,
            )],
            offset: 2,
            count: 3,
            ..Default::default()
        });

        let task = attach2_task(topn, vec![cop], None, &allocator).expect("attaches");
        let Some(PhysicalPlan::TopN(root_topn)) = task.plan() else {
            panic!("partial-order TopN stays at the root: {:?}", task.plan());
        };
        assert_eq!((root_topn.offset, root_topn.count), (2, 3));
        assert_eq!(root_topn.prefix_col, Some(1));
        assert_eq!(root_topn.prefix_len, 4);
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = root_topn.base.children().first()
        else {
            panic!("the cop task converts to an index lookup reader");
        };
        let Some(PhysicalPlan::Limit(limit)) = reader.index_plan.as_deref() else {
            panic!("the prefix limit is pushed to the index plan");
        };
        assert_eq!((limit.offset, limit.count), (0, 5));
        assert_eq!(limit.prefix_col, Some(1));
        assert_eq!(limit.prefix_len, 4);
        assert!(matches!(
            limit.base.children().first(),
            Some(PhysicalPlan::IndexScan(_))
        ));
    }

    #[test]
    fn a_limit_sinks_into_the_index_lookup_reader() {
        // `sinkIntoIndexLookUp` (`task.go:733`): a root Limit over a
        // converted double read becomes the reader's PushedLimit — no root
        // Limit operator survives — and the reader plus its bare table scan
        // adopt the limit's stats.
        let double = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(100.0))),
            table_plan: Some(Box::new(table_scan_with_rows(100.0))),
            keep_order: true,
            ..CopTask::default()
        });
        let mut base = crate::physical::BasePhysicalPlan::with_id(9, "Limit", 0);
        base.base
            .set_stats(Some(crate::stats_info::StatsInfo::new(5.0, [])));
        let limit = PhysicalPlan::Limit(crate::physical::PhysicalLimit {
            base,
            partition_by: Vec::new(),
            offset: 2,
            count: 3,
            prefix_col: None,
            prefix_len: 0,
        });
        let task =
            attach2_task(limit, vec![double], None, &PlanIdAllocator::new()).expect("attaches");
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = task.plan() else {
            panic!(
                "the limit sinks, leaving the reader on top: {:?}",
                task.plan()
            );
        };
        let pushed = reader.pushed_limit.expect("the sunk limit");
        assert_eq!((pushed.offset, pushed.count), (2, 3));
        assert!(
            (reader.base.base.stats_info().expect("stats").row_count() - 5.0).abs() < f64::EPSILON
        );
        let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
            panic!("the bare table side");
        };
        assert!(
            (scan.base.base.stats_info().expect("stats").row_count() - 5.0).abs() < f64::EPSILON,
            "the table side adopts the smaller stats"
        );
    }

    #[test]
    fn need_extra_proj_restores_origin_before_root_conditions_for_all_readers() {
        use tidb_expr::schema::Schema;

        for kind in ["table", "lookup", "merge"] {
            let allocator = PlanIdAllocator::new();
            let origin = Schema::new(vec![column_with_id(11)]);
            let expanded = Schema::new(vec![column_with_id(11), column_with_id(12)]);
            let mut cop = CopTask {
                table_plan: Some(Box::new(scan_with_schema(
                    &allocator,
                    "TableScan",
                    expanded.clone(),
                    20.0,
                ))),
                need_extra_proj: true,
                origin_schema: Some(origin),
                root_task_conds: vec![Expression::Column(column_with_id(11))],
                ..Default::default()
            };
            if kind == "lookup" {
                cop.index_plan = Some(Box::new(scan_with_schema(
                    &allocator,
                    "IndexScan",
                    expanded.clone(),
                    20.0,
                )));
            } else if kind == "merge" {
                cop.idx_merge_part_plans.push(scan_with_schema(
                    &allocator,
                    "IndexScan",
                    expanded,
                    20.0,
                ));
            }
            let task = cop.convert_to_root_task_impl(&allocator).expect(kind);
            let PhysicalPlan::Selection(selection) = task.plan().unwrap() else {
                panic!("root conditions must remain above the projection");
            };
            let PhysicalPlan::Projection(projection) = &selection.base.children()[0] else {
                panic!("{kind}: extra columns must be projected away");
            };
            assert_eq!(projection.base.base.schema().unwrap().len(), 1);
            let [Expression::Column(column)] = projection.exprs.as_slice() else {
                panic!("origin columns must be passed through");
            };
            assert_eq!(column.unique_id, 11);
            let reader = &projection.base.children()[0];
            assert_eq!(reader.schema().unwrap().len(), 2);
            assert_eq!(projection.base.base.stats_info().unwrap().row_count(), 20.0);
            assert_eq!(
                projection.base.base.query_block_offset(),
                reader.query_block_offset()
            );
            assert!(matches!(
                (kind, reader),
                ("table", PhysicalPlan::TableReader(_))
                    | ("lookup", PhysicalPlan::IndexLookUpReader(_))
                    | ("merge", PhysicalPlan::IndexMergeReader(_))
            ));
        }
    }

    #[test]
    fn need_extra_proj_does_not_replace_pushed_aggregate_schema() {
        use crate::physical::{BasePhysicalPlan, PhysicalHashAgg, PhysicalStreamAgg};
        use tidb_expr::schema::Schema;
        for double_read in [false, true] {
            for hash in [false, true] {
                let allocator = PlanIdAllocator::new();
                let input = Schema::new(vec![column_with_id(11), column_with_id(12)]);
                let mut base = BasePhysicalPlan::new(&allocator, "Aggregation", 10);
                base.base
                    .set_schema(Some(Schema::new(vec![column_with_id(21)])));
                base.base.set_stats(Some(StatsInfo::new(5.0, [])));
                base.set_children(vec![scan_with_schema(
                    &allocator,
                    "TableScan",
                    input.clone(),
                    20.0,
                )]);
                let aggregate = if hash {
                    PhysicalPlan::HashAgg(PhysicalHashAgg {
                        base,
                        ..Default::default()
                    })
                } else {
                    PhysicalPlan::StreamAgg(PhysicalStreamAgg {
                        base,
                        ..Default::default()
                    })
                };
                let cop = CopTask {
                    table_plan: Some(Box::new(aggregate)),
                    index_plan: double_read
                        .then(|| Box::new(scan_with_schema(&allocator, "IndexScan", input, 20.0))),
                    need_extra_proj: true,
                    origin_schema: Some(Schema::new(vec![column_with_id(11)])),
                    ..Default::default()
                };
                let task = cop.convert_to_root_task_impl(&allocator).unwrap();
                let plan = task.plan().unwrap();
                assert!(matches!(
                    plan,
                    PhysicalPlan::TableReader(_) | PhysicalPlan::IndexLookUpReader(_)
                ));
                assert_eq!(plan.schema().unwrap().columns[0].unique_id, 21);
            }
        }
    }

    #[test]
    fn virtual_expansion_keeps_root_conditions_above_origin_projection() {
        use tidb_expr::schema::Schema;
        let allocator = PlanIdAllocator::new();
        let mut generated = column_with_id(12);
        generated.virtual_expr = Some(Box::new(Expression::Column(column_with_id(11))));
        let cop = CopTask {
            table_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "TableScan",
                Schema::new(vec![generated.clone()]),
                20.0,
            ))),
            root_task_conds: vec![Expression::Column(generated)],
            ..Default::default()
        };
        let task = cop.convert_to_root_task_impl(&allocator).unwrap();
        let PhysicalPlan::Selection(selection) = task.plan().unwrap() else {
            panic!("Go applies root conditions after constructing the origin projection");
        };
        let PhysicalPlan::Projection(projection) = &selection.base.children()[0] else {
            panic!("virtual dependency expansion requires a projection");
        };
        assert_eq!(projection.base.base.schema().unwrap().len(), 1);
        assert_eq!(projection.base.children()[0].schema().unwrap().len(), 2);
    }

    #[test]
    fn root_task_conds_land_as_a_selection_above_the_reader() {
        // `handleRootTaskConds` (`physicalop/task.go:47`): the unpushable
        // conditions become a FromDataSource Selection above the converted
        // reader, stats scaled by Go's own histogram-miss fallback
        // (`cost.SelectionFactor` = 0.8).
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;

        let cond = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let task = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(10.0))),
            root_task_conds: vec![cond],
            ..CopTask::default()
        });
        let converted = task
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("converts");
        let Some(PhysicalPlan::Selection(selection)) = converted.plan() else {
            panic!("a Selection above the reader, got {:?}", converted.plan());
        };
        assert!(selection.from_data_source);
        assert_eq!(selection.conditions.len(), 1);
        assert!(
            (selection.base.base.stats_info().expect("stats").row_count() - 8.0).abs()
                < f64::EPSILON,
            "10 rows * SelectionFactor 0.8"
        );
        assert!(matches!(
            selection.base.children().first(),
            Some(PhysicalPlan::IndexReader(_))
        ));
    }

    #[test]
    fn an_index_only_cop_task_converts_into_an_index_reader() {
        // `convertToRootTaskImpl`'s index branch (`task_base.go:563`): the
        // pushed-down index plan wraps in a PhysicalIndexReader carrying its
        // stats; the DOUBLE-READ shape (both halves) now builds
        // `BuildIndexLookUpTask`'s reader, schema and stats from the TABLE
        // side (`Init`, `physical_indexlookup_reader.go:205`).
        let task = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(4.0))),
            ..CopTask::default()
        });
        let converted = task
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("converts");
        let Some(PhysicalPlan::IndexReader(reader)) = converted.plan() else {
            panic!("an IndexReader, got {:?}", converted.plan());
        };
        assert!(reader.index_plan.is_some());
        assert_eq!(reader.base.base.schema().expect("reader schema").len(), 1);
        assert!(
            (converted
                .plan()
                .expect("plan")
                .stats_info()
                .expect("stats")
                .row_count()
                - 4.0)
                .abs()
                < f64::EPSILON
        );

        let double = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(1.0))),
            table_plan: Some(Box::new(dual_with_rows(3.0))),
            keep_order: true,
            ..CopTask::default()
        });
        let converted = double
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("builds");
        let Some(PhysicalPlan::IndexLookUpReader(lookup)) = converted.plan() else {
            panic!("an IndexLookUpReader, got {:?}", converted.plan());
        };
        assert!(lookup.keep_order);
        // The reader's stats are the TABLE side's.
        assert!(
            (converted
                .plan()
                .expect("plan")
                .stats_info()
                .expect("stats")
                .row_count()
                - 3.0)
                .abs()
                < f64::EPSILON
        );
    }

    /// Go `RootTask.Copy` keeps the plan POINTER (`p: t.p`) and copies only
    /// the warnings. A copy must still behave as its own value: a write
    /// through one task cannot reach the other's plan, and a task that owns
    /// its plan alone hands it over without copying.
    #[test]
    fn a_task_copy_shares_the_plan_and_writes_do_not_cross() {
        let mut original = RootTask::default();
        original.set_plan(dual_with_rows(3.0));
        let rows = |task: &RootTask| task.get_plan().stats_info().expect("stats").row_count();

        let copied = original.copy();
        assert!((rows(&copied) - 3.0).abs() < f64::EPSILON);

        let mut written = Task::Root(copied);
        written
            .plan_mut()
            .expect("plan")
            .set_stats(Some(StatsInfo::new(9.0, [])));
        assert!(
            (rows(&original) - 3.0).abs() < f64::EPSILON,
            "the write must not reach the task it was copied from"
        );
        let Task::Root(mut written) = written else {
            panic!("root task");
        };
        assert!((rows(&written) - 9.0).abs() < f64::EPSILON);

        // The warnings are copied, not shared, exactly as Go's Copy does.
        original.warnings.append_warning("one");
        assert_eq!(written.warnings.warning_count(), 0);

        // Taking the plan out of its only owner yields the same value.
        let taken = written.take_plan().expect("plan");
        assert!((taken.stats_info().expect("stats").row_count() - 9.0).abs() < f64::EPSILON);
        assert!(written.take_plan().is_none());
    }

    #[test]
    fn an_empty_root_task_is_gos_invalid_task() {
        // `base.InvalidTask` is "core's empty RootTask".
        let task = Task::invalid_task();
        assert!(task.invalid());
        assert!(task.plan().is_none());
    }

    #[test]
    fn lookup_pushdown_is_applied_only_without_keep_order() {
        let build = |keep_order| {
            Task::Cop(CopTask {
                index_plan: Some(Box::new(dual_with_rows(1.0))),
                table_plan: Some(Box::new(dual_with_rows(1.0))),
                index_lookup_push_down_by: crate::access_path::IndexLookupPushDownBy::Hint,
                keep_order,
                ..CopTask::default()
            })
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("lookup task builds")
        };

        let unordered = build(false);
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = unordered.plan() else {
            panic!("an IndexLookUpReader");
        };
        // Go's `detachRootTableScanPlan` asserts the leaf IS a TableScan
        // (`physical_indexlookup.go:155`): a Dual table plan can never be
        // pushed down, so the reader keeps the ordinary two-phase lookup and
        // the flag stays off.
        assert!(!reader.index_lookup_push_down);

        let ordered = build(true);
        let Task::Root(root) = &ordered else {
            panic!("a root task");
        };
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = ordered.plan() else {
            panic!("an IndexLookUpReader");
        };
        assert!(!reader.index_lookup_push_down);
        assert_eq!(root.warnings.warning_count(), 1);
        assert_eq!(
            root.warnings.warnings[0].message,
            "hint INDEX_LOOKUP_PUSHDOWN is inapplicable, keep order is not supported."
        );
    }

    #[test]
    fn count_reads_whichever_half_the_cop_task_has_open() {
        // `CopTask.Count` (`task_base.go:455-461`): the index half before
        // FinishIndexPlan, the table half after.
        let mut task = CopTask {
            index_plan: Some(Box::new(dual_with_rows(7.0))),
            table_plan: Some(Box::new(dual_with_rows(99.0))),
            ..CopTask::default()
        };
        assert!((task.count() - 7.0).abs() < f64::EPSILON);
        task.finish_index_plan();
        // FinishIndexPlan moved the INDEX stats onto the table half: the
        // table read sees exactly the rows the index found.
        assert!((task.count() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn copies_never_share_a_warnings_slice() {
        // The whole point of `SimpleWarnings.Copy` per Go's comments.
        let mut original = Task::Root(RootTask::default());
        original.append_warning("first");
        let mut copied = original.copy();
        copied.append_warning("second");
        let Task::Root(original) = &original else {
            unreachable!()
        };
        let Task::Root(copied) = &copied else {
            unreachable!()
        };
        assert_eq!(original.warnings.warning_count(), 1);
        assert_eq!(copied.warnings.warning_count(), 2);
    }

    #[test]
    fn the_warning_cap_is_gos_max_uint16() {
        let mut warnings = SimpleWarnings::default();
        for i in 0..u32::from(u16::MAX) + 10 {
            warnings.append_warning(format!("w{i}"));
        }
        assert_eq!(warnings.warning_count(), usize::from(u16::MAX));
    }

    #[test]
    fn converting_a_non_root_task_refuses_by_name() {
        let cop = Task::Cop(CopTask::default());
        let error = cop
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect_err("refuses");
        assert!(format!("{error:?}").contains("convertToRootTaskImpl"));
        let mpp = Task::Mpp(MppTask::default());
        let error = mpp
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect_err("refuses");
        assert!(format!("{error:?}").contains("ConvertToRootTaskImpl"));
    }

    #[test]
    fn a_root_task_converts_by_copying() {
        let mut root = RootTask::default();
        root.set_plan(dual_with_rows(3.0));
        let task = Task::Root(root);
        let converted = task
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("root -> root");
        assert!(!converted.invalid());
        assert!((converted.count() - 3.0).abs() < f64::EPSILON);
    }
}

// ---------------------------------------------------------------------------
// Go `Attach2Task`: how a physical operator composes onto a child task.
// ---------------------------------------------------------------------------

/// Go `attachPlan2Task` (`core/task.go`): wrap the task's plan with `plan`.
///
/// * a root task: `p.SetChildren(v.GetPlan()); v.SetPlan(p)`;
/// * an MPP task: the same wrap on its plan;
/// * a cop task: the plan attaches to whichever half is still OPEN — the
///   index half before `FinishIndexPlan`, the table half after.
///
/// Go's `inheritStatsFromBottomTaskForIndexJoinInner` hook runs first. An
/// operator above a dynamic index-join probe must describe one probe result,
/// not retain the cardinality derived for its ordinary full-input subtree.
#[must_use]
pub fn attach_plan_to_task(mut plan: PhysicalPlan, mut task: Task) -> Task {
    let feedback = match &task {
        Task::Root(root) => root.index_join_info.is_some(),
        Task::Cop(cop) => cop.index_join_info.is_some(),
        Task::Mpp(_) => false,
    };
    inherit_index_join_stats(&mut plan, feedback, task.plan());
    match &mut task {
        Task::Root(root) => {
            let child = root.take_plan();
            plan.base_mut().set_children(child.into_iter().collect());
            root.set_plan(plan);
        }
        Task::Mpp(mpp) => {
            let child = mpp.plan.take().map(|boxed| *boxed);
            plan.base_mut().set_children(child.into_iter().collect());
            mpp.plan = Some(Box::new(plan));
        }
        Task::Cop(cop) => {
            if cop.index_plan_finished {
                let child = cop.table_plan.take().map(|boxed| *boxed);
                plan.base_mut().set_children(child.into_iter().collect());
                cop.table_plan = Some(Box::new(plan));
            } else {
                let child = cop.index_plan.take().map(|boxed| *boxed);
                plan.base_mut().set_children(child.into_iter().collect());
                cop.index_plan = Some(Box::new(plan));
            }
        }
    }
    task
}

fn inherit_index_join_stats(
    plan: &mut PhysicalPlan,
    has_feedback: bool,
    child: Option<&PhysicalPlan>,
) {
    if has_feedback && !matches!(plan, PhysicalPlan::IndexJoin(_)) {
        plan.base_mut().base.set_stats(
            child
                .and_then(PhysicalPlan::stats_info)
                .map(|stats| stats.scale(1.0, 1.0)),
        );
    }
}

/// Go `attach2TaskForMpp` for a physical HashAgg.  The MPP candidate carries
/// the requested run mode; this helper performs the partial/final split and
/// places the exchange between the two phases where the Go planner does.
fn attach_hash_agg_to_mpp(
    mut plan: PhysicalPlan,
    mpp: MppTask,
    column_ids: Option<&crate::expression_rewriter::ColumnIdAllocator>,
    allocator: &crate::plan_base::PlanIdAllocator,
) -> Result<Task, PlanError> {
    let PhysicalPlan::HashAgg(hash_agg) = &plan else {
        unreachable!("MPP HashAgg attachment received a non-HashAgg plan");
    };
    let mode = hash_agg.mpp_run_mode;
    let requested_partition_cols = hash_agg.mpp_partition_cols.clone();
    let enable_3_stage_distinct_agg = hash_agg.enable_3_stage_distinct_agg;
    let enable_3_stage_multi_distinct_agg = hash_agg.enable_3_stage_multi_distinct_agg;
    let tiflash_pre_agg_mode = hash_agg.tiflash_pre_agg_mode.clone();
    let expr_ctx = tidb_expr::ZonedNoColumns(tidb_expr::SessionTimeZone::utc());
    let multi_distinct_grouping_sets = if mode == crate::physical::AggMppRunMode::MppScalar {
        let PhysicalPlan::HashAgg(hash_agg) = &mut plan else {
            unreachable!("MPP scalar aggregate is a HashAgg");
        };
        let grouping_sets = crate::final_mode_agg::can_use_three_stage_multi_distinct(
            &hash_agg.agg_funcs,
            &hash_agg.group_by_items,
            enable_3_stage_distinct_agg,
            enable_3_stage_multi_distinct_agg,
        );
        if let Some(grouping_sets) = &grouping_sets {
            if !crate::final_mode_agg::mark_three_stage_grouping_ids(
                &mut hash_agg.agg_funcs,
                grouping_sets,
            ) {
                return Ok(Task::invalid_task());
            }
        }
        grouping_sets
    } else {
        None
    };
    let mut avg_projection = None;
    if matches!(
        mode,
        crate::physical::AggMppRunMode::Mpp1Phase
            | crate::physical::AggMppRunMode::Mpp2Phase
            | crate::physical::AggMppRunMode::MppScalar
    ) {
        if let Some(column_ids) = column_ids {
            avg_projection =
                crate::final_mode_agg::convert_avg_for_mpp(&mut plan, column_ids, allocator)?;
        }
    }

    match mode {
        crate::physical::AggMppRunMode::NoMpp
        | crate::physical::AggMppRunMode::Mpp1Phase => {
            let task = attach_plan_to_task(plan, Task::Mpp(mpp));
            if let Some(projection) = avg_projection.take() {
                Ok(attach_plan_to_task(PhysicalPlan::Projection(projection), task))
            } else {
                Ok(task)
            }
        }
        crate::physical::AggMppRunMode::Mpp2Phase => {
            let Some(column_ids) = column_ids else {
                return Err(PlanError::internal(
                    "MPP HashAgg partial construction needs a column allocator",
                ));
            };
            let (Some(partial), final_agg) = crate::final_mode_agg::new_partial_aggregate_mpp(
                &expr_ctx,
                column_ids,
                plan,
                allocator,
            )? else {
                return Ok(Task::invalid_task());
            };
            let mut partial = partial;
            if let PhysicalPlan::HashAgg(partial_hash_agg) = &mut partial {
                partial_hash_agg.tiflash_pre_agg_mode = tiflash_pre_agg_mode.clone();
            }
            let task = attach_plan_to_task(partial, Task::Mpp(mpp));
            let Task::Mpp(mpp) = task else {
                unreachable!("partial HashAgg attachment must retain MPP task");
            };
            let partition_cols = if requested_partition_cols.is_empty() {
                let PhysicalPlan::HashAgg(final_hash_agg) = &final_agg else {
                    return Ok(Task::invalid_task());
                };
                final_hash_agg
                    .group_by_items
                    .iter()
                    .map(|item| match item {
                        Expression::Column(column) => Ok(
                            crate::physical_property::MppPartitionColumn {
                                collate_id: column
                                    .get_static_type()
                                    .map(|field_type| {
                                        crate::physical_property::collate_id_for_partition(
                                            field_type.collation_name(),
                                        )
                                    })
                                    .unwrap_or(-1),
                                col: column.clone(),
                            },
                        ),
                        _ => Err(PlanError::internal(
                            "MPP HashAgg partition key must be a column",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                requested_partition_cols
            };
            if partition_cols.is_empty() {
                return Ok(Task::invalid_task());
            }
            let required = crate::physical_property::PhysicalProperty {
                task_tp: crate::task_type::TaskType::Mpp,
                expected_cnt: f64::MAX,
                mpp_partition_tp: MppPartitionType::Hash,
                mpp_partition_cols: partition_cols.clone(),
                ..crate::physical_property::PhysicalProperty::default()
            };
            let mpp = mpp.enforce_exchanger_impl(&required, allocator)?;
            let task = attach_plan_to_task(final_agg, Task::Mpp(mpp));
            if let Some(projection) = avg_projection.take() {
                Ok(attach_plan_to_task(PhysicalPlan::Projection(projection), task))
            } else {
                Ok(task)
            }
        }
        crate::physical::AggMppRunMode::MppTiDB => {
            let Some(column_ids) = column_ids else {
                return Err(PlanError::internal(
                    "MPP HashAgg partial construction needs a column allocator",
                ));
            };
            let (partial, final_agg) = crate::final_mode_agg::new_partial_aggregate_mpp_tidb(
                &expr_ctx,
                column_ids,
                plan,
                allocator,
            )?;
            let task = if let Some(partial) = partial {
                attach_plan_to_task(partial, Task::Mpp(mpp))
            } else {
                Task::Mpp(mpp)
            };
            let task = task.into_root_task(allocator)?;
            let task = attach_plan_to_task(final_agg, task);
            if let Some(projection) = avg_projection.take() {
                Ok(attach_plan_to_task(PhysicalPlan::Projection(projection), task))
            } else {
                Ok(task)
            }
        }
        crate::physical::AggMppRunMode::MppScalar => {
            let can_use_three_stage_single = match &plan {
                PhysicalPlan::HashAgg(hash_agg) => {
                    crate::final_mode_agg::can_use_three_stage_single_distinct(
                        &hash_agg.agg_funcs,
                        &hash_agg.group_by_items,
                    )
                }
                _ => false,
            };
            let can_use_three_stage =
                can_use_three_stage_single || multi_distinct_grouping_sets.is_some();
            let original_schema = plan.schema().cloned();
            let required = crate::physical_property::PhysicalProperty {
                task_tp: crate::task_type::TaskType::Mpp,
                expected_cnt: f64::MAX,
                mpp_partition_tp: MppPartitionType::SinglePartition,
                ..crate::physical_property::PhysicalProperty::default()
            };
            if !crate::physical_property::need_enforce_exchanger(
                mpp.partition_type(),
                mpp.hash_cols(),
                &required,
                None,
            ) {
                let task = attach_plan_to_task(plan, Task::Mpp(mpp));
                if let Some(projection) = avg_projection.take() {
                    return Ok(attach_plan_to_task(PhysicalPlan::Projection(projection), task));
                }
                return Ok(task);
            }
            let Some(column_ids) = column_ids else {
                return Err(PlanError::internal(
                    "MPP HashAgg partial construction needs a column allocator",
                ));
            };
            let (partial, final_agg) = crate::final_mode_agg::new_partial_aggregate_mpp(
                &expr_ctx,
                column_ids,
                plan,
                allocator,
            )?;
            if can_use_three_stage {
                let Some(partial) = partial else {
                    return Ok(Task::invalid_task());
                };
                if let Some(grouping_sets) = multi_distinct_grouping_sets {
                    let child = mpp.plan.as_deref().ok_or_else(|| {
                        PlanError::internal("scalar MPP aggregate has no child plan")
                    })?;
                    let Some(split) = crate::final_mode_agg::adjust_three_stage_multi_distinct(
                        partial,
                        final_agg,
                        child,
                        &grouping_sets,
                        column_ids,
                        allocator,
                    )?
                    else {
                        return Ok(Task::invalid_task());
                    };
                    let task = attach_plan_to_task(split.expand, Task::Mpp(mpp));
                    let task = attach_plan_to_task(split.partial_projection, task);
                    let mut partial = split.partial;
                    if let PhysicalPlan::HashAgg(partial_hash_agg) = &mut partial {
                        partial_hash_agg.tiflash_pre_agg_mode = tiflash_pre_agg_mode.clone();
                    }
                    let task = attach_plan_to_task(partial, task);
                    let Task::Mpp(mpp) = task else {
                        unreachable!("multi-distinct partial HashAgg must retain MPP task");
                    };
                    let hash_required = crate::physical_property::PhysicalProperty {
                        task_tp: crate::task_type::TaskType::Mpp,
                        expected_cnt: f64::MAX,
                        mpp_partition_tp: MppPartitionType::Hash,
                        mpp_partition_cols: split.partition_cols,
                        ..crate::physical_property::PhysicalProperty::default()
                    };
                    let mpp = mpp.enforce_exchanger(&hash_required, allocator)?;
                    let task = attach_plan_to_task(split.middle, Task::Mpp(mpp));
                    let Task::Mpp(mpp) = task else {
                        unreachable!("multi-distinct middle HashAgg must retain MPP task");
                    };
                    let mpp = mpp.enforce_exchanger(&required, allocator)?;
                    let task = attach_plan_to_task(split.final_agg, Task::Mpp(mpp));
                    return finish_scalar_aggregate_output(
                        task,
                        avg_projection,
                        original_schema.as_ref(),
                        allocator,
                    );
                }
                let Some(split) = crate::final_mode_agg::adjust_three_stage_single_distinct(
                    partial,
                    final_agg,
                    column_ids,
                    allocator,
                )?
                else {
                    return Ok(Task::invalid_task());
                };
                let task = attach_plan_to_task(split.partial, Task::Mpp(mpp));
                let Task::Mpp(mpp) = task else {
                    unreachable!("three-stage partial HashAgg must retain MPP task");
                };
                let hash_required = crate::physical_property::PhysicalProperty {
                    task_tp: crate::task_type::TaskType::Mpp,
                    expected_cnt: f64::MAX,
                    mpp_partition_tp: MppPartitionType::Hash,
                    mpp_partition_cols: split.partition_cols,
                    ..crate::physical_property::PhysicalProperty::default()
                };
                let mpp = mpp.enforce_exchanger(&hash_required, allocator)?;
                let task = attach_plan_to_task(split.middle, Task::Mpp(mpp));
                let Task::Mpp(mpp) = task else {
                    unreachable!("three-stage middle HashAgg must retain MPP task");
                };
                let mpp = mpp.enforce_exchanger(&required, allocator)?;
                let task = attach_plan_to_task(split.final_agg, Task::Mpp(mpp));
                return finish_scalar_aggregate_output(
                    task,
                    avg_projection,
                    original_schema.as_ref(),
                    allocator,
                );
            }
            let mpp = if let Some(partial) = partial {
                let task = attach_plan_to_task(partial, Task::Mpp(mpp));
                let Task::Mpp(mpp) = task else {
                    unreachable!("scalar partial HashAgg must retain MPP task");
                };
                mpp
            } else {
                mpp
            };
            let mpp = mpp.enforce_exchanger(&required, allocator)?;
            let task = attach_plan_to_task(final_agg, Task::Mpp(mpp));
            finish_scalar_aggregate_output(
                task,
                avg_projection,
                original_schema.as_ref(),
                allocator,
            )
        }
    }
}

/// Go's scalar MPP path always restores the original aggregate schema with a
/// projection after the single-partition final stage. AVG supplies its own
/// CASE/DIV projection; all other descriptors use this identity projection.
fn finish_scalar_aggregate_output(
    task: Task,
    avg_projection: Option<crate::physical::PhysicalProjection>,
    original_schema: Option<&tidb_expr::schema::Schema>,
    allocator: &crate::plan_base::PlanIdAllocator,
) -> Result<Task, PlanError> {
    if let Some(projection) = avg_projection {
        return Ok(attach_plan_to_task(PhysicalPlan::Projection(projection), task));
    }
    let Some(schema) = original_schema else {
        return Ok(task);
    };
    let child = task
        .plan()
        .ok_or_else(|| PlanError::internal("scalar MPP aggregate has no final plan"))?;
    let mut base = crate::physical::BasePhysicalPlan::new(
        allocator,
        "Projection",
        child.query_block_offset(),
    );
    base.base.set_stats(child.stats_info().cloned());
    base.base.set_schema(Some(schema.clone()));
    base.set_children_req_props(vec![Some(
        crate::physical_property::PhysicalProperty::default(),
    )]);
    let projection = PhysicalPlan::Projection(crate::physical::PhysicalProjection {
        base,
        exprs: schema
            .columns
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect(),
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    });
    Ok(attach_plan_to_task(projection, task))
}

/// Go `Attach2Task` per operator — the ROOT-TASK slice.
///
/// Every ported arm reproduces its Go body exactly for a root child task.
/// Remaining cop and MPP gaps are kept explicit: cop conversion still needs
/// `convertToRootTaskImpl`, while heavy-function TopN and scalar multi-distinct
/// aggregation still need their Go counterparts. A wrong push-down
/// is a silent wrong plan; a refusal is a loud gap.
/// Go `sinkIntoIndexLookUp` (`task.go:733`): after conversion, a root Limit
/// SINKS into the `PhysicalIndexLookUpReader` (directly or under one
/// Projection) as its `PushedLimit`, cutting the double read short — but
/// only when the table side is a bare `PhysicalTableScan`. The table scan
/// and reader adopt the limit's stats when smaller (Go's `StatsVersion`
/// carry-over has no counterpart field here). The schema-mending extra
/// projection (issue 14428's inlined-Projection shape) compares schema
/// LENGTHS; schemas absent on both sides compare equal.
fn sink_into_index_look_up(
    limit: &crate::physical::PhysicalLimit,
    task: &mut Task,
    allocator: &crate::plan_base::PlanIdAllocator,
) -> bool {
    let Task::Root(root) = task else {
        return false;
    };
    let Some(mut plan) = root.take_plan() else {
        return false;
    };
    let limit_schema_len = limit.base.base.schema().map_or(0, |schema| schema.len());
    let limit_stats = limit.base.base.stats_info().cloned();
    let sunk = {
        let (reader, via_proj) = match &mut plan {
            PhysicalPlan::IndexLookUpReader(reader) => (Some(reader), false),
            PhysicalPlan::Projection(_) => {
                let is_lookup = matches!(
                    plan.children().first(),
                    Some(PhysicalPlan::IndexLookUpReader(_))
                );
                if is_lookup {
                    let PhysicalPlan::Projection(proj) = &mut plan else {
                        unreachable!("the arm matched Projection");
                    };
                    let Some(PhysicalPlan::IndexLookUpReader(reader)) =
                        proj.base.children_mut().first_mut()
                    else {
                        unreachable!("checked above");
                    };
                    (Some(reader), true)
                } else {
                    (None, false)
                }
            }
            _ => (None, false),
        };
        match reader {
            None => false,
            Some(reader) => {
                // Only a bare table scan admits the sink: a Selection on the
                // table side must see every row.
                let ts_stats = match reader.table_plan.as_deref() {
                    Some(PhysicalPlan::TableScan(scan)) => scan.base.base.stats_info().cloned(),
                    _ => {
                        root.set_plan(plan);
                        return false;
                    }
                };
                let reader_schema_len = reader.base.base.schema().map_or(0, |schema| schema.len());
                reader.pushed_limit = Some(crate::physical::PushedDownLimit {
                    offset: limit.offset,
                    count: limit.count,
                });
                let limit_rows = limit_stats.as_ref().map_or(0.0, |stats| stats.row_count());
                if let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref_mut() {
                    if ts_stats
                        .as_ref()
                        .is_some_and(|stats| stats.row_count() >= limit_rows)
                    {
                        scan.base.base.set_stats(limit_stats.clone());
                    }
                }
                reader.base.base.set_stats(limit_stats.clone());
                if via_proj {
                    if let PhysicalPlan::Projection(proj) = &mut plan {
                        proj.base.base.set_stats(limit_stats.clone());
                    }
                }
                // The schema-mending projection above the reader.
                if limit_schema_len != reader_schema_len {
                    let mut base = crate::physical::BasePhysicalPlan::new(
                        allocator,
                        "Projection",
                        limit.base.base.query_block_offset(),
                    );
                    base.base.set_stats(limit_stats);
                    base.base.set_schema(limit.base.base.schema().cloned());
                    let exprs = limit.base.base.schema().map_or_else(Vec::new, |schema| {
                        schema
                            .columns
                            .iter()
                            .cloned()
                            .map(tidb_expr::expression::Expression::Column)
                            .collect()
                    });
                    base.set_children(vec![plan]);
                    plan = PhysicalPlan::Projection(crate::physical::PhysicalProjection {
                        base,
                        exprs,
                        avoid_column_evaluator: false,
                        calculate_no_delay: false,
                    });
                }
                true
            }
        }
    };
    root.set_plan(plan);
    sunk
}

/// Go `canPushToIndexPlan` (`core/task.go`): every referenced column must be
/// supplied by the open index half as a full, non-prefix column.
fn can_push_to_index_plan(
    index_plan: Option<&PhysicalPlan>,
    columns: &[tidb_expr::column::Column],
) -> bool {
    let Some(schema) = index_plan.and_then(PhysicalPlan::schema) else {
        return false;
    };
    columns.iter().all(|column| {
        let position = schema.column_index(column);
        position >= 0 && !schema.columns[position as usize].is_prefix
    })
}

/// The shared cop body of Go `attach2Task4PhysicalStreamAgg` and
/// `...HashAgg` after their gates: split via `NewPartialAggregate`, hang the
/// partial half on the cop task's live side, convert, and attach the final
/// half at root, preserving the inner lookup's per-outer-row statistics.
fn attach_agg_over_cop(
    plan: PhysicalPlan,
    mut cop: CopTask,
    column_ids: Option<&crate::expression_rewriter::ColumnIdAllocator>,
    allocator: &crate::plan_base::PlanIdAllocator,
) -> Result<Task, PlanError> {
    let Some(column_ids) = column_ids else {
        return Err(PlanError::internal(
            "the aggregate cop push needs the caller's column id allocator",
        ));
    };
    // Go's expression context is consulted only for argument TYPES during
    // the split's TypeInfer; a column-free context is exact for that.
    let ctx = tidb_expr::ZonedNoColumns(tidb_expr::SessionTimeZone::utc());
    let (partial, final_plan) =
        crate::final_mode_agg::new_partial_aggregate(&ctx, column_ids, plan, allocator)?;
    if let Some(mut partial) = partial {
        if let Some(table_plan) = cop.table_plan.take() {
            cop.finish_index_plan();
            inherit_index_join_stats(
                &mut partial,
                cop.index_join_info.is_some(),
                Some(&table_plan),
            );
            partial.base_mut().set_children(vec![*table_plan]);
            cop.table_plan = Some(Box::new(partial));
            // Go: the pushed agg's schema replaces the extra projection a
            // double read would otherwise re-add above the reader.
            cop.need_extra_proj = false;
        } else if let Some(index_plan) = cop.index_plan.take() {
            inherit_index_join_stats(
                &mut partial,
                cop.index_join_info.is_some(),
                Some(&index_plan),
            );
            partial.base_mut().set_children(vec![*index_plan]);
            cop.index_plan = Some(Box::new(partial));
        } else {
            return Err(PlanError::internal(
                "an aggregate cop push over a task with neither half",
            ));
        }
    }
    let t = Task::Cop(cop).into_root_task(allocator)?;
    Ok(attach_plan_to_task(final_plan, t))
}

fn index_join_range_rebuild(
    plan: &PhysicalPlan,
) -> Option<crate::physical_plan_cache::PointRangeRebuild> {
    use crate::physical_plan_cache::PointRangeRebuild;
    match plan {
        PhysicalPlan::TableScan(scan) => scan.range_rebuild.clone().map(PointRangeRebuild::Table),
        PhysicalPlan::IndexScan(scan) => scan.range_rebuild.clone().map(PointRangeRebuild::Index),
        PhysicalPlan::TableReader(reader) => reader
            .table_plan
            .as_deref()
            .and_then(index_join_range_rebuild),
        PhysicalPlan::IndexReader(reader) => reader
            .index_plan
            .as_deref()
            .and_then(index_join_range_rebuild),
        PhysicalPlan::IndexLookUpReader(reader) => reader
            .index_plan
            .as_deref()
            .and_then(index_join_range_rebuild)
            .or_else(|| {
                reader
                    .table_plan
                    .as_deref()
                    .and_then(index_join_range_rebuild)
            }),
        _ => plan.children().iter().find_map(index_join_range_rebuild),
    }
}

/// Go `completePhysicalIndexJoin`: consume the inner task's access feedback,
/// retain only lookup-capable equalities as join keys, and move every unused
/// equality into the residual condition list.
fn complete_physical_index_join(
    join: &mut crate::physical::PhysicalIndexJoin,
    info: IndexJoinInfo,
    inner_plan: &PhysicalPlan,
    outer_plan: &PhysicalPlan,
) -> Result<(), PlanError> {
    if info.key_off2_idx_off.len() != join.inner_join_keys.len()
        || join.inner_join_keys.len() != join.outer_join_keys.len()
        || join.inner_join_keys.len() != join.is_null_eq.len()
    {
        return Err(PlanError::internal(
            "completePhysicalIndexJoin received misaligned lookup-key feedback",
        ));
    }
    let mut new_inner_keys = Vec::with_capacity(join.inner_join_keys.len());
    let mut new_outer_keys = Vec::with_capacity(join.outer_join_keys.len());
    let mut new_is_null_eq = Vec::with_capacity(join.is_null_eq.len());
    let mut new_key_off = Vec::with_capacity(info.key_off2_idx_off.len());
    let mut other_conditions = std::mem::take(&mut join.other_conditions);
    for (key_off, idx_off) in info.key_off2_idx_off.iter().copied().enumerate() {
        if idx_off < 0 {
            let equality = join.equal_conditions.get(key_off).cloned().ok_or_else(|| {
                PlanError::internal("completePhysicalIndexJoin cannot restore an unused equality")
            })?;
            other_conditions.push(tidb_expr::expression::Expression::ScalarFunction(equality));
            continue;
        }
        new_inner_keys.push(join.inner_join_keys[key_off].clone());
        new_outer_keys.push(join.outer_join_keys[key_off].clone());
        new_is_null_eq.push(join.is_null_eq[key_off]);
        new_key_off.push(idx_off);
    }

    let mut outer_hash_keys = new_outer_keys.clone();
    let mut inner_hash_keys = new_inner_keys.clone();
    // Go's `completePhysicalIndexJoin` runs this extraction with
    // `extractOtherEQ` hardcoded `true` at BOTH of its call sites that reach
    // it -- `indexJoinAttach2Task` and `indexHashJoinAttach2Task`
    // (`task.go:158,178`) -- so a plain `IndexJoin` batches its outer rows
    // into a hash map for lookup dedup exactly as `IndexHashJoin` does, and
    // reads `OuterHashKeys`/`InnerHashKeys` too. `attach2Task4PhysicalIndexMergeJoin`
    // (`task.go:141`) is the one index-join attach path that never calls
    // `completePhysicalIndexJoin` at all -- its inner plan is already fixed
    // before `Attach2Task` runs, and the merge executor has no hash table to
    // key -- so `IndexMergeJoin` alone keeps skipping this extraction.
    if join.kind != crate::plan_cost_ver2::IndexJoinKind::IndexMergeJoin {
        let outer_schema = outer_plan.schema().ok_or_else(|| {
            PlanError::internal("completePhysicalIndexJoin outer child has no schema")
        })?;
        let inner_schema = inner_plan.schema().ok_or_else(|| {
            PlanError::internal("completePhysicalIndexJoin inner child has no schema")
        })?;
        for index in (0..other_conditions.len()).rev() {
            let tidb_expr::expression::Expression::ScalarFunction(function) =
                &other_conditions[index]
            else {
                continue;
            };
            if function.func_name.lowercase() != "eq" {
                continue;
            }
            let Some((left, right)) = tidb_expr::expr_util::is_col_op_col(function) else {
                continue;
            };
            if left.in_operand || right.in_operand {
                continue;
            }
            if outer_schema.contains(left) && inner_schema.contains(right) {
                outer_hash_keys.push(left.clone());
                inner_hash_keys.push(right.clone());
            } else if inner_schema.contains(left) && outer_schema.contains(right) {
                outer_hash_keys.push(right.clone());
                inner_hash_keys.push(left.clone());
            }
            other_conditions.remove(index);
        }
    }

    join.inner_access_table_id = Some(info.table_id);
    join.inner_access_index_id = info.index_id;
    join.inner_access_conditions = info.access_conditions;
    join.ranges = info.ranges;
    join.idx_col_lens = info.idx_col_lens;
    join.compare_filters = info.compare_filters;
    join.key_off2_idx_off = new_key_off;
    join.inner_join_keys = new_inner_keys;
    join.outer_join_keys = new_outer_keys;
    join.is_null_eq = new_is_null_eq;
    join.other_conditions = other_conditions;
    join.outer_hash_keys = outer_hash_keys;
    join.inner_hash_keys = inner_hash_keys;
    join.equal_conditions.clear();
    join.range_rebuild = info
        .range_rebuild
        .or_else(|| index_join_range_rebuild(inner_plan));
    let (left, right) = if join.inner_child_idx == 0 {
        (&join.inner_join_keys, &join.outer_join_keys)
    } else {
        (&join.outer_join_keys, &join.inner_join_keys)
    };
    join.left_join_keys.clone_from(left);
    join.right_join_keys.clone_from(right);
    Ok(())
}

/// Go `containVirtualColumn` (`core/task.go:1125-1142`): a pushdown
/// expression may refer to a generated column only when the MPP child schema
/// does not identify that column as virtual.
fn contains_virtual_column_in_plan(
    expressions: &[tidb_expr::expression::Expression],
    plan: &PhysicalPlan,
) -> bool {
    let Some(schema) = plan.schema() else {
        return false;
    };
    let virtual_ids: std::collections::HashSet<i64> = schema
        .columns
        .iter()
        .filter(|column| column.unique_id > 0 && column.virtual_expr.is_some())
        .map(|column| column.unique_id)
        .collect();
    tidb_expr::simple_expr::extract_columns_from_expressions(expressions, None)
        .iter()
        .any(|column| virtual_ids.contains(&column.unique_id))
}

/// Go `HeavyFunctionNameMap` (`core/task.go:46-57`). These expressions are
/// expensive enough that evaluating them once in a pushed-down projection is
/// preferable to evaluating them again in the global TopN.
const HEAVY_TOPN_FUNCTIONS: &[&str] = &[
    "vec_cosine_distance",
    "vec_l1_distance",
    "vec_l2_distance",
    "vec_negative_inner_product",
    "vec_dims",
    "vec_l2_norm",
    "fts_match_word",
];

/// Go `ContainHeavyFunction` (`core/task.go:1083-1092`).
fn contains_heavy_topn_function(expression: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expression else {
        return false;
    };
    if HEAVY_TOPN_FUNCTIONS.contains(&function.func_name.lowercase()) {
        return true;
    }
    function.args.iter().any(contains_heavy_topn_function)
}

/// The plans returned by Go `getPushedDownTopN` when at least one by-item is a
/// heavy function: a bottom projection materializes each heavy expression,
/// a pushed TopN orders by those fresh columns, and a global TopN reuses the
/// same columns after the task returns to TiDB.
fn heavy_topn_split(
    topn: &crate::physical::PhysicalTopN,
    child_plan: &PhysicalPlan,
    store_type: StoreType,
    column_ids: Option<&crate::expression_rewriter::ColumnIdAllocator>,
    allocator: &crate::plan_base::PlanIdAllocator,
) -> Option<(
    crate::physical::PhysicalProjection,
    crate::physical::PhysicalTopN,
    crate::physical::PhysicalTopN,
)> {
    if !topn.heavy_function_optimize
        || (store_type == StoreType::TiKv && !topn.allow_projection_push_down)
    {
        return None;
    }
    let heavy_indexes: Vec<usize> = topn
        .by_items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            contains_heavy_topn_function(&item.expr).then_some(index)
        })
        .collect();
    if heavy_indexes.is_empty() {
        return None;
    }
    let column_ids = column_ids?;
    let output_schema = topn.base.base.schema()?.clone();
    let mut projection_exprs = output_schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect::<Vec<_>>();
    let mut projection_columns = output_schema.columns.clone();
    let mut distance_columns = Vec::with_capacity(heavy_indexes.len());
    for index in heavy_indexes.iter().copied() {
        let expression = topn.by_items[index].expr.clone();
        let ret_type = expression.static_type()?.clone();
        projection_exprs.push(expression);
        let mut distance = tidb_expr::column::Column::new(column_ids.alloc(), ret_type);
        distance.index = (projection_exprs.len() - 1) as i64;
        projection_columns.push(distance.clone());
        distance_columns.push((index, distance));
    }

    let child_stats = child_plan.stats_info().cloned();
    let new_count = topn.offset.wrapping_add(topn.count);
    let pushed_stats = child_stats.map(|stats| stats.derive_limit_stats(new_count as f64));
    let projection_schema = tidb_expr::schema::Schema::new(projection_columns);
    let mut projection_base = crate::physical::BasePhysicalPlan::new(
        allocator,
        "Projection",
        topn.base.base.query_block_offset(),
    );
    projection_base.base.set_stats(pushed_stats.clone());
    projection_base
        .base
        .set_schema(Some(projection_schema.clone()));
    let projection = crate::physical::PhysicalProjection {
        base: projection_base,
        exprs: projection_exprs,
        ..Default::default()
    };

    let mut pushed = topn.clone();
    pushed.base.set_children(Vec::new());
    pushed.base.base.set_stats(pushed_stats);
    pushed
        .base
        .base
        .set_schema(Some(projection_schema));
    pushed.offset = 0;
    pushed.count = new_count;
    for (index, distance) in &distance_columns {
        pushed.by_items[*index].expr = Expression::Column(distance.clone());
    }

    let mut global = topn.clone();
    global.base.set_children(Vec::new());
    for (index, distance) in distance_columns {
        global.by_items[index].expr = Expression::Column(distance);
    }
    Some((projection, pushed, global))
}

pub fn attach2_task(
    plan: PhysicalPlan,
    mut tasks: Vec<Task>,
    column_ids: Option<&crate::expression_rewriter::ColumnIdAllocator>,
    allocator: &crate::plan_base::PlanIdAllocator,
) -> Result<Task, PlanError> {
    // The caller hands over owned candidate tasks. Go copies task headers
    // while sharing plan pointers; moving these tasks avoids cloning Rust's
    // owned plan trees again. A caller retaining a candidate uses Task::copy.
    let mut first = tasks
        .drain(..1)
        .next()
        .ok_or_else(|| PlanError::internal("attach2_task with no child task"))?;
    match &plan {
        // `attach2Task4PhysicalSort` (`task.go:843`): copy, attach. No
        // conversion — findBestTask only asks a Sort under a root property.
        PhysicalPlan::Sort(_) => Ok(attach_plan_to_task(plan, first)),
        // `attach2Task4PhysicalSelection` (`task.go:1598`): Go has NO cop
        // push at attach — a cop child CONVERTS and the selection lands at
        // root (pushed filters ride the DataSource's PushedDownConds
        // instead). The MPP arm pushes when TiFlash's
        // `CanExprsPushDown` admits every condition, and converts otherwise.
        PhysicalPlan::Selection(_) => match &first {
            Task::Root(_) | Task::Cop(_) => {
                let converted = first.into_root_task(allocator)?;
                Ok(attach_plan_to_task(plan, converted))
            }
            Task::Mpp(_) => {
                let PhysicalPlan::Selection(selection) = &plan else {
                    unreachable!("the arm matched Selection");
                };
                if crate::pushdown::can_exprs_push_down_tiflash(&selection.conditions) {
                    Ok(attach_plan_to_task(plan, first))
                } else {
                    let converted = first.into_root_task(allocator)?;
                    Ok(attach_plan_to_task(plan, converted))
                }
            }
        },
        // `attach2Task4PhysicalWindow` (`task.go:2230`): convert the child to
        // a root task and attach. The TiFlash MPP arm is absent with that
        // tier.
        PhysicalPlan::Window(_) | PhysicalPlan::Shuffle(_) | PhysicalPlan::ShuffleReceiver(_) => {
            let converted = first.copy().convert_to_root_task(allocator)?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // Go attach2Task4PhysicalExpand retains an MPP child on TiFlash;
        // all other children finish their remote reads before root expansion.
        PhysicalPlan::Expand(_) => {
            let task = match first.copy() {
                mpp @ Task::Mpp(_) => mpp,
                other => other.convert_to_root_task(allocator)?,
            };
            Ok(attach_plan_to_task(plan, task))
        }
        // `attach2Task4PhysicalProjection` (`task.go:1506`): the cop arm
        // pushes the projection onto the cop task — staying a COP task —
        // when there are no root conds, no index-merge parts, and every
        // expr passes the TiKV gate (`crate::pushdown`); an unfinished
        // index half finishes first (the conservative arm of
        // `canPushToIndexPlan`'s column check). Otherwise, and for a root
        // child, convert-then-attach. The MPP arm uses the TiFlash admission
        // gate and converts when a projection cannot be pushed.
        PhysicalPlan::Projection(_) => match &first {
            Task::Root(_) => {
                let converted = first.into_root_task(allocator)?;
                Ok(attach_plan_to_task(plan, converted))
            }
            Task::Cop(cop_ref) => {
                let PhysicalPlan::Projection(projection) = &plan else {
                    unreachable!("the arm matched Projection");
                };
                let pushable = cop_ref.root_task_conds.is_empty()
                    && cop_ref.idx_merge_part_plans.is_empty()
                    && crate::pushdown::can_exprs_push_down_tikv(&projection.exprs);
                if pushable {
                    let Task::Cop(mut cop) = first else {
                        unreachable!("the arm matched Cop");
                    };
                    let columns = tidb_expr::simple_expr::extract_columns_from_expressions(
                        &projection.exprs,
                        None,
                    );
                    if !cop.index_plan_finished
                        && !can_push_to_index_plan(cop.index_plan.as_deref(), &columns)
                    {
                        cop.finish_index_plan();
                    }
                    Ok(attach_plan_to_task(plan, Task::Cop(cop)))
                } else {
                    let converted = first.into_root_task(allocator)?;
                    Ok(attach_plan_to_task(plan, converted))
                }
            }
            Task::Mpp(_) => {
                let PhysicalPlan::Projection(projection) = &plan else {
                    unreachable!("the arm matched Projection");
                };
                if crate::pushdown::can_exprs_push_down_tiflash(&projection.exprs) {
                    Ok(attach_plan_to_task(plan, first))
                } else {
                    let converted = first.into_root_task(allocator)?;
                    Ok(attach_plan_to_task(plan, converted))
                }
            }
        },
        // `attach2Task4PhysicalLimit` (`task.go:619`): the SINGLE-READ cop
        // branch pushes a partial limit — `Count = Offset + Count`, offset
        // removed, `DeriveLimitStats` over the open half's profile, the
        // child's schema shared — onto the cop task, converts, and attaches
        // the ROOT limit above (unless a partition-by skips it: "a derived
        // topN and window function will take care of the filter").
        // `sinkIntoIndexLookUp` and the index-merge/MPP arms narrow with
        // their readers, named here.
        PhysicalPlan::Limit(_) => {
            let PhysicalPlan::Limit(limit) = &plan else {
                unreachable!("the arm matched Limit");
            };
            let t = match first {
                Task::Root(_) => first.into_root_task(allocator)?,
                Task::Cop(_) => {
                    let Task::Cop(mut cop) = first else {
                        unreachable!("the arm matched Cop");
                    };
                    let pushable =
                        (!cop.keep_order || !cop.index_plan_finished || cop.index_plan.is_none())
                            && cop.root_task_conds.is_empty();
                    if pushable {
                        let new_count = limit.offset.wrapping_add(limit.count);
                        let stats = cop
                            .plan()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = crate::physical::BasePhysicalPlan::new(
                            allocator,
                            "Limit",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        // "Don't use clone() so that Limit and its children
                        // share the same schema": the pushed limit reports
                        // the open half's schema.
                        base.base
                            .set_schema(cop.plan().and_then(PhysicalPlan::schema).cloned());
                        let pushed = PhysicalPlan::Limit(crate::physical::PhysicalLimit {
                            base,
                            partition_by: limit.partition_by.clone(),
                            offset: 0,
                            count: new_count,
                            prefix_col: None,
                            prefix_len: 0,
                        });
                        let Task::Cop(pushed_cop) = attach_plan_to_task(pushed, Task::Cop(cop))
                        else {
                            unreachable!("attaching onto a cop task answers a cop task");
                        };
                        cop = pushed_cop;
                    }
                    let mut t = Task::Cop(cop).into_root_task(allocator)?;
                    // `sunk = sinkIntoIndexLookUp(p, t)`: a converted double
                    // read absorbs the limit itself.
                    if sink_into_index_look_up(limit, &mut t, allocator) {
                        return Ok(t);
                    }
                    t
                }
                Task::Mpp(mpp) => {
                    let new_count = limit.offset.wrapping_add(limit.count);
                    let stats = mpp
                        .plan
                        .as_deref()
                        .and_then(PhysicalPlan::stats_info)
                        .map(|profile| profile.derive_limit_stats(new_count as f64));
                    let mut base = crate::physical::BasePhysicalPlan::new(
                        allocator,
                        "Limit",
                        plan.query_block_offset(),
                    );
                    base.base.set_stats(stats);
                    base.base
                        .set_schema(mpp.plan.as_deref().and_then(PhysicalPlan::schema).cloned());
                    let pushed = PhysicalPlan::Limit(crate::physical::PhysicalLimit {
                        base,
                        partition_by: limit.partition_by.clone(),
                        offset: 0,
                        count: new_count,
                        prefix_col: None,
                        prefix_len: 0,
                    });
                    let mut pushed_task = attach_plan_to_task(pushed, Task::Mpp(mpp));
                    // Go sets the pushed Limit schema from its child after
                    // attachment so generated columns retain the exact child
                    // schema object.
                    if let Some(PhysicalPlan::Limit(pushed_limit)) = pushed_task.plan_mut() {
                        let child_schema = pushed_limit
                            .base
                            .children()
                            .first()
                            .and_then(PhysicalPlan::schema)
                            .cloned();
                        pushed_limit.base.base.set_schema(child_schema);
                    }
                    pushed_task.into_root_task(allocator)?
                }
            };
            // "Skip limit with partition on the root."
            if !limit.partition_by.is_empty() {
                return Ok(t);
            }
            Ok(attach_plan_to_task(plan, t))
        }
        // `PhysicalMaxOneRow` has no override: `BasePhysicalPlan.Attach2Task`
        // (`base_physical_plan.go:202`) is convert-to-root then attach, on
        // ANY task kind — a cop/MPP child propagates
        // `convert_to_root_task`'s reader-building refusal.
        PhysicalPlan::MaxOneRow(_) => {
            let converted = first.into_root_task(allocator)?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // An ExchangeSender is born at root conversion (and at MPP fragment
        // generation), never as an attach parent.
        PhysicalPlan::ExchangeSender(_) => Err(PlanError::internal(
            "attach2_task: an ExchangeSender is never an attach parent",
        )),
        PhysicalPlan::ExchangeReceiver(_) => Ok(attach_plan_to_task(plan, first)),
        // `PhysicalLock` has no override: the default convert-then-attach
        // body, exactly as `PhysicalMaxOneRow`'s arm above.
        PhysicalPlan::Lock(_) => {
            let converted = first.into_root_task(allocator)?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // `attach2Task4PhysicalUnionAll` (`task.go:1573`): convert EVERY
        // child task to root and wire the multi-child plan into a fresh
        // RootTask. Go's MPP arm: a PartitionUnion over any MPP child is the
        // invalid task outright ("PartitionUnion cannot pushdown to
        // tiflash"); a plain union over MPP children needs
        // `attach2MppTasks4PhysicalUnionAll`, which creates one MPP task over
        // the child fragment plans.
        PhysicalPlan::UnionAll(_) => {
            let mut tasks = {
                let mut all = vec![first];
                all.append(&mut tasks);
                all
            };
            if tasks.iter().any(|task| matches!(task, Task::Mpp(_))) {
                if plan.base().base.tp() == "PartitionUnion" {
                    return Ok(Task::invalid_task());
                }
                let mut child_plans = Vec::with_capacity(tasks.len());
                for task in tasks {
                    let Task::Mpp(mut mpp) = task else {
                        return Ok(Task::invalid_task());
                    };
                    let Some(child) = mpp.plan.take() else {
                        return Ok(Task::invalid_task());
                    };
                    child_plans.push(*child);
                }
                if child_plans.is_empty() {
                    return Ok(Task::invalid_task());
                }
                let mut mpp_plan = plan;
                mpp_plan.base_mut().set_children(child_plans);
                return Ok(Task::Mpp(MppTask::new(
                    mpp_plan,
                    crate::physical_property::MppPartitionType::Any,
                    [],
                )));
            }
            let mut plan = plan;
            let mut children = Vec::with_capacity(tasks.len());
            for task in tasks.drain(..) {
                let Task::Root(mut converted) = task.into_root_task(allocator)? else {
                    return Err(PlanError::internal(
                        "convert_to_root_task answered a non-root task",
                    ));
                };
                let Some(child) = converted.take_plan() else {
                    return Err(PlanError::internal(
                        "attach2Task4PhysicalUnionAll: a child task has no plan",
                    ));
                };
                children.push(child);
            }
            plan.base_mut().set_children(children);
            let mut root = RootTask::default();
            root.set_plan(plan);
            Ok(Task::Root(root))
        }
        // `attach2Task4PhysicalApply` (`task.go:127`): convert both children
        // to root, wire them in, build the join schema
        // (`BuildPhysicalJoinSchema`), and inherit BOTH children's warnings.
        PhysicalPlan::Apply(_) => {
            let second = tasks
                .drain(..1)
                .next()
                .ok_or_else(|| PlanError::internal("attach2Task4PhysicalApply needs two tasks"))?;
            let Task::Root(mut left) = first.into_root_task(allocator)? else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let Task::Root(mut right) = second.into_root_task(allocator)? else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let (Some(left_plan), Some(right_plan)) = (left.take_plan(), right.take_plan()) else {
                return Err(PlanError::internal(
                    "attach2Task4PhysicalApply: a child task has no plan",
                ));
            };
            let mut plan = plan;
            plan.base_mut().set_children(vec![left_plan, right_plan]);
            let join_type = match &plan {
                PhysicalPlan::Apply(apply) => apply.hash_join.join_type,
                _ => unreachable!("the arm matched Apply"),
            };
            let schema = crate::physical::build_physical_join_schema(join_type, &plan);
            plan.base_mut().base.set_schema(schema);
            let mut root = RootTask::default();
            root.set_plan(plan);
            root.warnings.copy_from([&left.warnings, &right.warnings]);
            Ok(Task::Root(root))
        }

        // `attach2Task4NominalSort` (`task.go:851`): an only-column nominal
        // sort returns the child task ITSELF — not even a copy — and
        // otherwise it is copy-then-attach with no conversion, like Sort.
        PhysicalPlan::NominalSort(op) => {
            if op.only_column {
                return Ok(first);
            }
            Ok(attach_plan_to_task(plan, first))
        }
        // `attach2Task4PhysicalSequence` (`task.go:2259`): when ANY child
        // task is not MPP, the sequence VANISHES — the last child's task is
        // returned unchanged and every producer's task is discarded. That is
        // Go's own body ("if !isMpp { return tasks[len(tasks)-1] }"), a
        // quirk reproduced rather than fixed: on a root-tier plan the CTE
        // producers are wired elsewhere, not through this attach. The
        // all-MPP arm builds an MppTask over the last child's partition type
        // and carries its hash-column contract into the composed fragment.
        PhysicalPlan::Sequence(_) => {
            let mut all = vec![first];
            all.append(&mut tasks);
            if all.iter().any(|task| !matches!(task, Task::Mpp(_))) {
                return Ok(all.pop().expect("the vec was built with at least one task"));
            }
            let Some(Task::Mpp(last)) = all.last() else {
                unreachable!("the all-MPP branch proved every task is MPP");
            };
            let partition_type = last.partition_type();
            let hash_cols = last.hash_cols.clone();
            let mut child_plans = Vec::with_capacity(all.len());
            let mut warnings = Vec::with_capacity(all.len());
            for task in all {
                let Task::Mpp(mut mpp) = task else {
                    unreachable!("the all-MPP branch proved every task is MPP");
                };
                let Some(child) = mpp.plan.take() else {
                    return Ok(Task::invalid_task());
                };
                child_plans.push(*child);
                warnings.push(mpp.warnings);
            }
            let mut sequence = plan;
            sequence.base_mut().set_children(child_plans);
            Ok(Task::Mpp(MppTask::new_with_hash_cols(
                sequence,
                partition_type,
                hash_cols,
                warnings,
            )))
        }
        // `attach2Task4PhysicalTopN` (`task.go:1249`), the SIMPLE path:
        // when the by-items carry columns, pass the TiKV gate, and the cop
        // task has no root conds, push a partial TopN — ByItems cloned,
        // `Count = Offset + Count`, offset removed, `DeriveLimitStats` over
        // the open half (`getPushedDownTopN`'s non-heavy half) — then
        // convert and attach the ROOT TopN above (partition-by root skip).
        // The heavy-function rewrite, partial-order, TiDB-cop, and
        // index-merge-advisory arms narrow by name.
        PhysicalPlan::TopN(_) => {
            let PhysicalPlan::TopN(topn) = &plan else {
                unreachable!("the arm matched TopN");
            };
            // Go `handlePartialOrderTopN`: a prefix-index match is carried
            // by the CopTask so the root TopN can retain the partial-order
            // metadata while a special Limit short-circuits the index scan.
            if let Task::Cop(mut cop) = first {
                if let Some(match_result) = cop
                    .partial_order_match_result
                    .take()
                    .filter(|result| result.matched)
                {
                    let Some(prefix_col) = match_result.prefix_col.as_ref() else {
                        return Ok(Task::invalid_task());
                    };
                    let Some(topn_schema) = topn.base.base.schema() else {
                        return Ok(Task::invalid_task());
                    };
                    let Some(prefix_col) = topn_schema
                        .columns
                        .iter()
                        .find(|column| column.unique_id == prefix_col.unique_id)
                        .cloned()
                    else {
                        return Ok(Task::invalid_task());
                    };
                    let mut partial_topn = topn.clone();
                    partial_topn.prefix_col = Some(prefix_col.unique_id);
                    partial_topn.prefix_len = match_result.prefix_len;

                    let can_push_limit = cop.idx_merge_part_plans.is_empty()
                        && !cop.index_plan_finished
                        && cop.root_task_conds.is_empty()
                        && cop.index_plan.is_some();
                    if can_push_limit {
                        let new_count = partial_topn.offset.wrapping_add(partial_topn.count);
                        let child = cop
                            .index_plan
                            .take()
                            .expect("partial-order CopTask has an index plan");
                        let stats = child
                            .stats_info()
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = crate::physical::BasePhysicalPlan::new(
                            allocator,
                            "Limit",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        base.base.set_schema(child.schema().cloned());
                        base.set_children(vec![*child]);
                        cop.index_plan = Some(Box::new(PhysicalPlan::Limit(
                            crate::physical::PhysicalLimit {
                                base,
                                partition_by: partial_topn.partition_by.clone(),
                                offset: 0,
                                count: new_count,
                                prefix_col: Some(prefix_col.unique_id),
                                prefix_len: match_result.prefix_len,
                            },
                        )));
                    }

                    let converted = Task::Cop(cop).into_root_task(allocator)?;
                    if !partial_topn.partition_by.is_empty() {
                        return Ok(converted);
                    }
                    return Ok(attach_plan_to_task(
                        PhysicalPlan::TopN(partial_topn),
                        converted,
                    ));
                }
                first = Task::Cop(cop);
            }
            let t = match first {
                Task::Root(_) => first.into_root_task(allocator)?,
                Task::Cop(_) => {
                    let Task::Cop(mut cop) = first else {
                        unreachable!("the arm matched Cop");
                    };
                    let by_exprs: Vec<tidb_expr::expression::Expression> =
                        topn.by_items.iter().map(|item| item.expr.clone()).collect();
                    let columns =
                        tidb_expr::simple_expr::extract_columns_from_expressions(&by_exprs, None);
                    let need_push_down = !columns.is_empty();
                    let pushable = need_push_down
                        && crate::pushdown::can_exprs_push_down_tikv(&by_exprs)
                        && cop.root_task_conds.is_empty();
                    if pushable {
                        if !cop.index_plan_finished
                            && !can_push_to_index_plan(cop.index_plan.as_deref(), &columns)
                        {
                            cop.finish_index_plan();
                        }
                        if cop.plan().is_none() {
                            let converted = Task::Cop(cop).convert_to_root_task(allocator)?;
                            if !topn.partition_by.is_empty() {
                                return Ok(converted);
                            }
                            return Ok(attach_plan_to_task(plan, converted));
                        }
                        if let Some((projection, pushed, global)) = heavy_topn_split(
                            topn,
                            cop.plan().expect("cop plan was checked above"),
                            cop.get_store_type(),
                            column_ids,
                            allocator,
                        ) {
                            let pushed_task = attach_plan_to_task(
                                PhysicalPlan::TopN(pushed),
                                attach_plan_to_task(
                                    PhysicalPlan::Projection(projection),
                                    Task::Cop(cop),
                                ),
                            );
                            if !topn.partition_by.is_empty() {
                                return Ok(pushed_task);
                            }
                            let converted = pushed_task.into_root_task(allocator)?;
                            return Ok(attach_plan_to_task(
                                PhysicalPlan::TopN(global),
                                converted,
                            ));
                        }
                        let new_count = topn.offset.wrapping_add(topn.count);
                        let stats = cop
                            .plan()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = crate::physical::BasePhysicalPlan::new(
                            allocator,
                            "TopN",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        base.base
                            .set_schema(cop.plan().and_then(PhysicalPlan::schema).cloned());
                        let pushed = PhysicalPlan::TopN(crate::physical::PhysicalTopN {
                            base,
                            by_items: topn.by_items.clone(),
                            partition_by: topn.partition_by.clone(),
                            offset: 0,
                            count: new_count,
                            prefix_col: topn.prefix_col,
                            prefix_len: topn.prefix_len,
                            allow_projection_push_down: topn.allow_projection_push_down,
                            heavy_function_optimize: topn.heavy_function_optimize,
                        });
                        let Task::Cop(pushed_cop) = attach_plan_to_task(pushed, Task::Cop(cop))
                        else {
                            unreachable!("attaching onto a cop task answers a cop task");
                        };
                        cop = pushed_cop;
                    }
                    Task::Cop(cop).into_root_task(allocator)?
                }
                Task::Mpp(mpp) => {
                    let by_exprs: Vec<tidb_expr::expression::Expression> =
                        topn.by_items.iter().map(|item| item.expr.clone()).collect();
                    let columns =
                        tidb_expr::simple_expr::extract_columns_from_expressions(&by_exprs, None);
                    let need_push_down = !columns.is_empty();
                    let pushable = need_push_down
                        && crate::pushdown::can_exprs_push_down_tiflash(&by_exprs)
                        && !contains_virtual_column_in_plan(
                            &by_exprs,
                            mpp.plan.as_deref().ok_or_else(|| {
                                PlanError::internal("MPP TopN attachment received an empty task")
                            })?,
                    );
                    if pushable {
                        if let Some((projection, pushed, global)) = heavy_topn_split(
                            topn,
                            mpp.plan.as_deref().ok_or_else(|| {
                                PlanError::internal("MPP TopN attachment received an empty task")
                            })?,
                            StoreType::TiFlash,
                            column_ids,
                            allocator,
                        ) {
                            let pushed_task = attach_plan_to_task(
                                PhysicalPlan::TopN(pushed),
                                attach_plan_to_task(
                                    PhysicalPlan::Projection(projection),
                                    Task::Mpp(mpp),
                                ),
                            );
                            if !topn.partition_by.is_empty() {
                                return Ok(pushed_task);
                            }
                            let converted = pushed_task.into_root_task(allocator)?;
                            return Ok(attach_plan_to_task(
                                PhysicalPlan::TopN(global),
                                converted,
                            ));
                        }
                        let new_count = topn.offset.wrapping_add(topn.count);
                        let stats = mpp
                            .plan
                            .as_deref()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = crate::physical::BasePhysicalPlan::new(
                            allocator,
                            "TopN",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        base.base.set_schema(
                            mpp.plan.as_deref().and_then(PhysicalPlan::schema).cloned(),
                        );
                        let pushed = PhysicalPlan::TopN(crate::physical::PhysicalTopN {
                            base,
                            by_items: topn.by_items.clone(),
                            partition_by: topn.partition_by.clone(),
                            offset: 0,
                            count: new_count,
                            prefix_col: topn.prefix_col,
                            prefix_len: topn.prefix_len,
                            allow_projection_push_down: topn.allow_projection_push_down,
                            heavy_function_optimize: topn.heavy_function_optimize,
                        });
                        let mut pushed_task = attach_plan_to_task(pushed, Task::Mpp(mpp));
                        if let Some(PhysicalPlan::TopN(pushed_topn)) = pushed_task.plan_mut() {
                            let child_schema = pushed_topn
                                .base
                                .children()
                                .first()
                                .and_then(PhysicalPlan::schema)
                                .cloned();
                            pushed_topn.base.base.set_schema(child_schema);
                        }
                        pushed_task.into_root_task(allocator)?
                    } else {
                        Task::Mpp(mpp).into_root_task(allocator)?
                    }
                }
            };
            if !topn.partition_by.is_empty() {
                return Ok(t);
            }
            Ok(attach_plan_to_task(plan, t))
        }
        // `attach2Task4PhysicalStreamAgg` (`task.go:1653`): a cop child
        // takes the partial/final split UNLESS the aggregate must not cross
        // the boundary — an order-keeping double read, root-side filters, or
        // an index merge. The TiFlash stream-agg refusal is unreachable on
        // this port's TiKV-only cop tasks.
        PhysicalPlan::StreamAgg(_) => match first {
            Task::Cop(cop) => {
                if (cop.index_plan.is_some() && cop.table_plan.is_some() && cop.keep_order)
                    || !cop.root_task_conds.is_empty()
                    || !cop.idx_merge_part_plans.is_empty()
                {
                    let t = Task::Cop(cop).into_root_task(allocator)?;
                    Ok(attach_plan_to_task(plan, t))
                } else {
                    attach_agg_over_cop(plan, cop, column_ids, allocator)
                }
            }
            Task::Mpp(mpp) => {
                let converted = Task::Mpp(mpp).into_root_task(allocator)?;
                Ok(attach_plan_to_task(plan, converted))
            }
            root @ Task::Root(_) => Ok(attach_plan_to_task(plan, root.into_root_task(allocator)?)),
        },
        // `attach2Task4PhysicalHashAgg` (`task.go:2162`): same split, gated
        // only on root-side filters and index merge.
        PhysicalPlan::HashAgg(_) => match first {
            Task::Cop(cop) => {
                if cop.root_task_conds.is_empty() && cop.idx_merge_part_plans.is_empty() {
                    attach_agg_over_cop(plan, cop, column_ids, allocator)
                } else {
                    let t = Task::Cop(cop).into_root_task(allocator)?;
                    Ok(attach_plan_to_task(plan, t))
                }
            }
            Task::Mpp(mpp) => attach_hash_agg_to_mpp(plan, mpp, column_ids, allocator),
            root @ Task::Root(_) => Ok(attach_plan_to_task(plan, root.into_root_task(allocator)?)),
        },
        // `attach2Task4PhysicalHashJoin` (`task.go:211`): convert BOTH
        // children — Go converts the RIGHT one first — wire them in, and
        // concatenate warnings right-before-left, which is the order SHOW
        // WARNINGS replays them in. `StoreTp == kv.TiFlash` routes to the
        // TiFlash attach in Go; the enum's hash join carries no store type,
        // with no TiFlash tier to route to. HashJoin carries its lookup
        // child's feedback upward; MergeJoin does not.
        PhysicalPlan::HashJoin(_) | PhysicalPlan::MergeJoin(_) => {
            let second = tasks
                .drain(..1)
                .next()
                .ok_or_else(|| PlanError::internal("physical join needs two tasks"))?;
            // Go merge converts left first; hash converts right first.
            let (left, right) = if matches!(plan, PhysicalPlan::MergeJoin(_)) {
                let left = first.into_root_task(allocator)?;
                (left, second.into_root_task(allocator)?)
            } else {
                let right = second.into_root_task(allocator)?;
                (first.into_root_task(allocator)?, right)
            };
            let Task::Root(mut right) = right else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let Task::Root(mut left) = left else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let (Some(left_plan), Some(right_plan)) = (left.take_plan(), right.take_plan()) else {
                return Err(PlanError::internal(
                    "physical join: a child task has no plan",
                ));
            };
            let mut plan = plan;
            let propagated_index_join_info = if matches!(plan, PhysicalPlan::HashJoin(_)) {
                right.index_join_info.take().or(left.index_join_info.take())
            } else {
                None
            };
            plan.base_mut().set_children(vec![left_plan, right_plan]);
            let mut root = RootTask::default();
            root.set_plan(plan);
            root.index_join_info = propagated_index_join_info;
            root.warnings.copy_from([&right.warnings, &left.warnings]);
            Ok(Task::Root(root))
        }
        PhysicalPlan::IndexJoin(_) => {
            let second = tasks
                .into_iter()
                .next()
                .ok_or_else(|| PlanError::internal("IndexJoin requires two tasks"))?;
            let PhysicalPlan::IndexJoin(mut join) = plan else {
                unreachable!()
            };
            let (outer, inner) = match join.inner_child_idx {
                0 => (second, first),
                1 => (first, second),
                _ => return Err(PlanError::internal("invalid IndexJoin inner child index")),
            };
            // Go converts outer first and replays its warnings first.
            let Task::Root(mut outer) = outer.into_root_task(allocator)? else {
                unreachable!()
            };
            let Task::Root(mut inner) = inner.into_root_task(allocator)? else {
                unreachable!()
            };
            let info = inner.index_join_info.take().ok_or_else(|| {
                PlanError::internal("IndexJoin inner task has no access-path feedback")
            })?;
            complete_physical_index_join(&mut join, info, inner.get_plan(), outer.get_plan())?;
            let outer_plan = outer.take_plan().expect("read above");
            let inner_plan = inner.take_plan().expect("read above");
            join.base.set_children(if join.inner_child_idx == 1 {
                vec![outer_plan, inner_plan]
            } else {
                vec![inner_plan, outer_plan]
            });
            let mut root = RootTask::default();
            root.set_plan(PhysicalPlan::IndexJoin(join));
            root.warnings.copy_from([&outer.warnings, &inner.warnings]);
            Ok(Task::Root(root))
        }
        PhysicalPlan::TableScan(_) => Err(PlanError::internal(
            "a PhysicalTableScan is born inside a cop task by findBestTask, \
             never attached (convertToTableScan, find_best_task.go)",
        )),
        PhysicalPlan::TableSample(_) => Err(PlanError::internal(
            "a PhysicalTableSample is born inside its own root task by \
             convertToSampleTable (find_best_task.go), never attached",
        )),
        PhysicalPlan::IndexLookUpReader(_) => Err(PlanError::internal(
            "a PhysicalIndexLookUpReader is born by BuildIndexLookUpTask at \
             cop-to-root conversion, never attached",
        )),
        PhysicalPlan::TableDual(_) => Err(PlanError::internal(
            "a PhysicalTableDual is born inside its own root task by \
             findBestTask (logical_table_dual.go), never attached",
        )),
        PhysicalPlan::TableReader(_) => Err(PlanError::internal(
            "a PhysicalTableReader is born by convertToRootTaskImpl \
             (task_base.go:571), never attached",
        )),
        PhysicalPlan::IndexScan(_) => Err(PlanError::internal(
            "a PhysicalIndexScan is born inside a cop task by findBestTask \
             (convertToIndexScan), never attached",
        )),
        PhysicalPlan::IndexReader(_) => Err(PlanError::internal(
            "a PhysicalIndexReader is born by convertToRootTaskImpl \
             (task_base.go:563), never attached",
        )),
        PhysicalPlan::PointGet(_) | PhysicalPlan::BatchPointGet(_) => Err(PlanError::internal(
            "PointGet and BatchPointGet plans are complete root plans, never attached",
        )),
        PhysicalPlan::IndexMergeReader(_) => Err(PlanError::internal(
            "a PhysicalIndexMergeReader is born by index-merge task conversion, never attached",
        )),
        PhysicalPlan::LocalIndexLookUp(_) => Err(PlanError::internal(
            "a PhysicalLocalIndexLookUp is born inside an IndexLookUpReader, never attached",
        )),
        PhysicalPlan::Dml(_) => Err(PlanError::internal(
            "a physical DML root owns its select plan and is never attached as a relational child",
        )),
        PhysicalPlan::CTE(_) => Err(PlanError::internal(
            "a PhysicalCTE is born inside its own root task by \
             findBestTask4LogicalCTE (physical_cte.go), never attached",
        )),
        PhysicalPlan::CTETable(_) => Err(PlanError::internal(
            "a PhysicalCTETable is born inside its own root task by \
             findBestTask4LogicalCTETable (physical_cte_table.go), never \
             attached",
        )),
        PhysicalPlan::MemTable(_) => Err(PlanError::internal(
            "a PhysicalMemTable is born inside its own root task by \
             findBestTask4LogicalMemTable (physical_mem_table.go), never attached",
        )),
        PhysicalPlan::Show(_) | PhysicalPlan::ShowDDLJobs(_) => Err(PlanError::internal(
            "a PhysicalShow/PhysicalShowDDLJobs is born inside its own root \
             task by findBestTask4LogicalShow{,DDLJobs} (physical_show.go), \
             never attached",
        )),
    }
}

#[cfg(test)]
mod attach_tests {
    use super::*;
    use crate::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalSelection, PhysicalSort};
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::StatsInfo;
    use tidb_expr::schema::Schema;

    // All WRITTEN: Go's Attach2Task coverage is planner-integration bound.

    fn op_with_stats(tp: &str, rows: f64) -> BasePhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = BasePhysicalPlan::default();
        base.base = crate::plan_base::BasePlan::new(&allocator, tp, 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        base
    }

    fn root_task_over(rows: f64) -> Task {
        let mut root = RootTask::default();
        root.set_plan(PhysicalPlan::TableDual(
            crate::physical::PhysicalTableDual {
                base: op_with_stats("Dual", rows),
                row_count: 0,
            },
        ));
        Task::Root(root)
    }

    fn mpp_task_over(rows: f64) -> Task {
        Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base: op_with_stats("Dual", rows),
                row_count: 0,
            }),
            MppPartitionType::Any,
            [],
        ))
    }

    #[test]
    fn mpp_unary_attachment_follows_tiflash_pushdown_and_root_fallback() {
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let pushed = attach2_task(
            selection,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("an empty Selection is TiFlash-pushable");
        assert!(matches!(pushed, Task::Mpp(_)));
        assert!(matches!(pushed.plan(), Some(PhysicalPlan::Selection(_))));

        let mut unsupported = tidb_expr::scalar_function::ScalarFunction::default();
        unsupported.func_name = tidb_ast::CiString::new("tan");
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            conditions: vec![tidb_expr::expression::Expression::ScalarFunction(
                unsupported,
            )],
            ..PhysicalSelection::default()
        });
        let converted = attach2_task(
            selection,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("an unsupported Selection converts to root");
        assert!(matches!(converted, Task::Root(_)));
        assert!(matches!(converted.plan(), Some(PhysicalPlan::Selection(_))));

        let projection = PhysicalPlan::Projection(crate::physical::PhysicalProjection {
            base: op_with_stats("Projection", 8.0),
            ..crate::physical::PhysicalProjection::default()
        });
        let projected = attach2_task(
            projection,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("an empty Projection is TiFlash-pushable");
        assert!(matches!(projected, Task::Mpp(_)));
        assert!(matches!(
            projected.plan(),
            Some(PhysicalPlan::Projection(_))
        ));
    }

    #[test]
    fn mpp_limit_and_topn_keep_the_partial_operator_inside_the_reader() {
        let limit = PhysicalPlan::Limit(crate::physical::PhysicalLimit {
            base: op_with_stats("Limit", 5.0),
            offset: 2,
            count: 3,
            ..crate::physical::PhysicalLimit::default()
        });
        let limited = attach2_task(
            limit,
            vec![mpp_task_over(20.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP Limit attaches");
        let Some(PhysicalPlan::Limit(root_limit)) = limited.plan() else {
            panic!("the original Limit remains at root");
        };
        let Some(PhysicalPlan::TableReader(reader)) = root_limit.base.children().first() else {
            panic!("root Limit reads through a TableReader");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the MPP reader owns the pass-through sender");
        };
        let Some(PhysicalPlan::Limit(pushed_limit)) = sender.base.children().first() else {
            panic!("the partial Limit is below the sender");
        };
        assert_eq!(pushed_limit.offset, 0);
        assert_eq!(pushed_limit.count, 5);

        let by_item = tidb_expr::aggregation::ByItems::new(
            tidb_expr::expression::Expression::Column(tidb_expr::column::Column::new(
                1,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            )),
            false,
        );
        let topn = PhysicalPlan::TopN(crate::physical::PhysicalTopN {
            base: op_with_stats("TopN", 5.0),
            by_items: vec![by_item],
            offset: 4,
            count: 6,
            ..crate::physical::PhysicalTopN::default()
        });
        let topned = attach2_task(
            topn,
            vec![mpp_task_over(20.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP TopN attaches");
        let Some(PhysicalPlan::TopN(root_topn)) = topned.plan() else {
            panic!("the original TopN remains at root");
        };
        let Some(PhysicalPlan::TableReader(reader)) = root_topn.base.children().first() else {
            panic!("root TopN reads through a TableReader");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the MPP reader owns the pass-through sender");
        };
        let Some(PhysicalPlan::TopN(pushed_topn)) = sender.base.children().first() else {
            panic!("the partial TopN is below the sender");
        };
        assert_eq!(pushed_topn.offset, 0);
        assert_eq!(pushed_topn.count, 10);
    }

    #[test]
    fn mpp_topn_materializes_heavy_by_items_once() {
        use tidb_ast::CiString;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode, VectorFloat32};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::scalar_function::ScalarFunction;

        let id = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        let score = Column::new(2, FieldType::new(FieldTypeCode::Double));
        let vector = Column::new(3, FieldType::new(FieldTypeCode::VectorFloat32));
        let child_schema = Schema::new(vec![id.clone(), score.clone(), vector.clone()]);
        let mut child_base = op_with_stats("Dual", 20.0);
        child_base.base.set_schema(Some(child_schema));
        let child = PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
            base: child_base,
            row_count: 0,
        });
        let child_task = Task::Mpp(MppTask::new(child, MppPartitionType::Any, []));

        let query_vector = Constant::new(
            Datum::new_vector_float32(VectorFloat32::must_create(vec![1.0, 2.0])),
            FieldType::new(FieldTypeCode::VectorFloat32),
        );
        let heavy = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("vec_l2_distance"),
            FieldType::new(FieldTypeCode::Double),
            vec![Expression::Column(vector), Expression::Constant(query_vector)],
        ));
        let topn_schema = Schema::new(vec![id.clone(), score.clone()]);
        let mut topn_base = op_with_stats("TopN", 10.0);
        topn_base.base.set_schema(Some(topn_schema));
        let topn = PhysicalPlan::TopN(crate::physical::PhysicalTopN {
            base: topn_base,
            by_items: vec![
                tidb_expr::aggregation::ByItems::new(Expression::Column(score), false),
                tidb_expr::aggregation::ByItems::new(heavy, false),
            ],
            offset: 4,
            count: 6,
            ..crate::physical::PhysicalTopN::default()
        });
        let PhysicalPlan::TopN(topn_ref) = &topn else {
            unreachable!();
        };
        assert!(crate::pushdown::can_exprs_push_down_tiflash(
            &topn_ref
                .by_items
                .iter()
                .map(|item| item.expr.clone())
                .collect::<Vec<_>>()
        ));
        assert!(contains_heavy_topn_function(&topn_ref.by_items[1].expr));
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let attached = attach2_task(
            topn,
            vec![child_task],
            Some(&column_ids),
            &PlanIdAllocator::new(),
        )
        .expect("heavy MPP TopN attaches");
        let Some(PhysicalPlan::TopN(global)) = attached.plan() else {
            panic!("the global TopN remains at root");
        };
        let Expression::Column(global_distance) = &global.by_items[1].expr else {
            panic!("global heavy by-item was not replaced by a distance column");
        };
        assert_eq!(global_distance.index, 2);
        let Some(PhysicalPlan::TableReader(reader)) = global.base.children().first() else {
            panic!("global TopN reads through a TableReader");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the MPP reader owns the pass-through sender");
        };
        let Some(PhysicalPlan::TopN(pushed)) = sender.base.children().first() else {
            panic!("the partial TopN is below the sender");
        };
        assert_eq!(pushed.count, 10);
        let Expression::Column(pushed_distance) = &pushed.by_items[1].expr else {
            panic!("pushed heavy by-item was not replaced by a distance column");
        };
        assert_eq!(pushed_distance.unique_id, global_distance.unique_id);
        let Some(PhysicalPlan::Projection(projection)) = pushed.base.children().first() else {
            panic!("the distance projection is below the pushed TopN");
        };
        assert_eq!(projection.exprs.len(), 3);
    }

    #[test]
    fn mpp_union_sequence_and_stream_agg_preserve_go_task_shapes() {
        let union = PhysicalPlan::UnionAll(crate::physical::PhysicalUnionAll {
            base: op_with_stats("Union", 20.0),
            mpp: true,
        });
        let union_task = attach2_task(
            union,
            vec![mpp_task_over(10.0), mpp_task_over(11.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP UnionAll attaches");
        assert!(matches!(union_task, Task::Mpp(_)));
        assert_eq!(union_task.plan().expect("union plan").children().len(), 2);

        let sequence = PhysicalPlan::Sequence(crate::physical::PhysicalSequence {
            base: op_with_stats("Sequence", 20.0),
        });
        let sequence_task = attach2_task(
            sequence,
            vec![mpp_task_over(10.0), mpp_task_over(11.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP Sequence attaches");
        assert!(matches!(sequence_task, Task::Mpp(_)));
        assert_eq!(
            sequence_task
                .plan()
                .expect("sequence plan")
                .children()
                .len(),
            2
        );

        let stream_agg = PhysicalPlan::StreamAgg(crate::physical::PhysicalStreamAgg {
            base: op_with_stats("StreamAgg", 2.0),
            ..crate::physical::PhysicalStreamAgg::default()
        });
        let stream_task = attach2_task(
            stream_agg,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP StreamAgg converts to root");
        assert!(matches!(stream_task, Task::Root(_)));
        assert!(matches!(
            stream_task.plan(),
            Some(PhysicalPlan::StreamAgg(_))
        ));
    }

    #[test]
    fn mpp_two_phase_hash_agg_attaches_partial_exchange_and_final() {
        use crate::expression_rewriter::ColumnIdAllocator;
        use crate::physical::{AggMppRunMode, PhysicalHashAgg};
        use crate::physical_property::MppPartitionColumn;
        use tidb_expr::aggregation::{names, AggFuncDesc};
        use tidb_expr::{SessionTimeZone, ZonedNoColumns};

        let plan_allocator = PlanIdAllocator::new();
        let column_allocator = ColumnIdAllocator::new();
        let group = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let value = tidb_expr::column::Column::new(
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let count = AggFuncDesc::new(
            &ZonedNoColumns(SessionTimeZone::utc()),
            names::COUNT,
            vec![Expression::Column(value)],
            false,
        )
        .expect("count descriptor");
        let mut base = op_with_stats("HashAgg", 4.0);
        base.base.set_schema(Some(Schema::new(vec![
            tidb_expr::column::Column::new(
                3,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            group.clone(),
        ])));
        let hash = PhysicalPlan::HashAgg(PhysicalHashAgg {
            base,
            agg_funcs: vec![count],
            group_by_items: vec![Expression::Column(group.clone())],
            mpp_run_mode: AggMppRunMode::Mpp2Phase,
            mpp_partition_cols: vec![MppPartitionColumn {
                col: group.clone(),
                collate_id: -1,
            }],
            ..PhysicalHashAgg::default()
        });
        let mut child_base = op_with_stats("Dual", 12.0);
        child_base.base.set_schema(Some(Schema::new(vec![
            group.clone(),
            tidb_expr::column::Column::new(
                2,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
        ])));
        let child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base: child_base,
                row_count: 12,
            }),
            MppPartitionType::Any,
            [],
        ));
        let attached = attach2_task(
            hash,
            vec![child],
            Some(&column_allocator),
            &plan_allocator,
        )
        .expect("MPP HashAgg attaches");
        let Task::Mpp(mpp) = attached else {
            panic!("two-phase MPP HashAgg remains an MPP task");
        };
        let Some(PhysicalPlan::HashAgg(final_agg)) = mpp.plan() else {
            panic!("final HashAgg is the MPP task root");
        };
        let Some(PhysicalPlan::ExchangeReceiver(receiver)) = final_agg.base.children().first()
        else {
            panic!("hash exchange sits between partial and final aggregation");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = receiver.base.children().first() else {
            panic!("receiver owns the exchange sender");
        };
        assert_eq!(sender.hash_cols[0].col.unique_id, group.unique_id);
        assert!(matches!(
            sender.base.children().first(),
            Some(PhysicalPlan::HashAgg(partial))
                if partial.agg_funcs[0].mode == tidb_expr::aggregation::AggFunctionMode::Partial1
        ));
    }

    #[test]
    fn mpp_scalar_single_distinct_builds_three_aggregation_stages() {
        use crate::expression_rewriter::ColumnIdAllocator;
        use crate::physical::{AggMppRunMode, PhysicalHashAgg};
        use tidb_expr::aggregation::{names, AggFuncDesc};
        use tidb_expr::{SessionTimeZone, ZonedNoColumns};

        let plan_allocator = PlanIdAllocator::new();
        let column_allocator = ColumnIdAllocator::new();
        let ctx = ZonedNoColumns(SessionTimeZone::utc());
        let distinct_col = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let ordinary_col = tidb_expr::column::Column::new(
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let distinct = AggFuncDesc::new(
            &ctx,
            names::COUNT,
            vec![Expression::Column(distinct_col.clone())],
            true,
        )
        .expect("distinct count descriptor");
        let ordinary = AggFuncDesc::new(
            &ctx,
            names::COUNT,
            vec![Expression::Column(ordinary_col.clone())],
            false,
        )
        .expect("ordinary count descriptor");
        let mut base = op_with_stats("HashAgg", 4.0);
        base.base.set_schema(Some(Schema::new(vec![
            tidb_expr::column::Column::new(
                3,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            tidb_expr::column::Column::new(
                4,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
        ])));
        let hash = PhysicalPlan::HashAgg(PhysicalHashAgg {
            base,
            agg_funcs: vec![distinct, ordinary],
            group_by_items: Vec::new(),
            mpp_run_mode: AggMppRunMode::MppScalar,
            ..PhysicalHashAgg::default()
        });
        let mut child_base = op_with_stats("Dual", 12.0);
        child_base.base.set_schema(Some(Schema::new(vec![
            distinct_col,
            ordinary_col,
        ])));
        let child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base: child_base,
                row_count: 12,
            }),
            MppPartitionType::Any,
            [],
        ));
        let attached = attach2_task(
            hash,
            vec![child],
            Some(&column_allocator),
            &plan_allocator,
        )
        .expect("three-stage scalar MPP HashAgg attaches");
        let Task::Mpp(mpp) = attached else {
            panic!("scalar MPP HashAgg remains an MPP task");
        };
        let Some(PhysicalPlan::Projection(projection)) = mpp.plan() else {
            panic!("scalar MPP output restores the original schema");
        };
        let Some(PhysicalPlan::HashAgg(final_agg)) = projection.base.children().first() else {
            panic!("projection is above the final HashAgg");
        };
        assert_eq!(final_agg.agg_funcs[0].name(), names::SUM);
        let Some(PhysicalPlan::ExchangeReceiver(single_receiver)) =
            final_agg.base.children().first()
        else {
            panic!("final aggregation is fed by a single-partition exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(single_sender)) =
            single_receiver.base.children().first()
        else {
            panic!("the single-partition receiver owns its sender");
        };
        let Some(PhysicalPlan::HashAgg(middle)) = single_sender.base.children().first() else {
            panic!("the single-partition exchange feeds the middle HashAgg");
        };
        assert_eq!(middle.agg_funcs[0].mode, tidb_expr::aggregation::AggFunctionMode::Partial1);
        let Some(PhysicalPlan::ExchangeReceiver(hash_receiver)) =
            middle.base.children().first()
        else {
            panic!("the middle aggregation is fed by a hash exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = hash_receiver.base.children().first() else {
            panic!("the hash receiver owns its sender");
        };
        assert_eq!(sender.hash_cols.len(), 1);
        assert!(matches!(
            sender.base.children().first(),
            Some(PhysicalPlan::HashAgg(partial))
                if partial.group_by_items.len() == 1
                    && partial.agg_funcs.len() == 1
                    && partial.agg_funcs[0].name() == names::COUNT
        ));
    }

    #[test]
    fn mpp_scalar_multi_distinct_builds_expand_and_three_aggregation_stages() {
        use crate::expression_rewriter::ColumnIdAllocator;
        use crate::physical::{AggMppRunMode, PhysicalHashAgg};
        use tidb_expr::aggregation::{names, AggFuncDesc};
        use tidb_expr::{SessionTimeZone, ZonedNoColumns};

        let plan_allocator = PlanIdAllocator::new();
        let column_allocator = ColumnIdAllocator::new();
        let ctx = ZonedNoColumns(SessionTimeZone::utc());
        let distinct_a = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let distinct_b = tidb_expr::column::Column::new(
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let ordinary_c = tidb_expr::column::Column::new(
            3,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let count_distinct = |column: tidb_expr::column::Column| {
            AggFuncDesc::new(&ctx, names::COUNT, vec![Expression::Column(column)], true)
                .expect("distinct count descriptor")
        };
        let ordinary = AggFuncDesc::new(
            &ctx,
            names::COUNT,
            vec![Expression::Column(ordinary_c.clone())],
            false,
        )
        .expect("ordinary count descriptor");
        let mut base = op_with_stats("HashAgg", 4.0);
        base.base.set_schema(Some(Schema::new(vec![
            tidb_expr::column::Column::new(
                4,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            tidb_expr::column::Column::new(
                5,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            tidb_expr::column::Column::new(
                6,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
        ])));
        let hash = PhysicalPlan::HashAgg(PhysicalHashAgg {
            base,
            agg_funcs: vec![count_distinct(distinct_a.clone()), count_distinct(distinct_b.clone()), ordinary],
            group_by_items: Vec::new(),
            mpp_run_mode: AggMppRunMode::MppScalar,
            enable_3_stage_distinct_agg: true,
            enable_3_stage_multi_distinct_agg: true,
            ..PhysicalHashAgg::default()
        });
        let mut child_base = op_with_stats("Dual", 12.0);
        child_base.base.set_schema(Some(Schema::new(vec![
            distinct_a.clone(),
            distinct_b.clone(),
            ordinary_c,
        ])));
        let child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base: child_base,
                row_count: 12,
            }),
            MppPartitionType::Any,
            [],
        ));
        let attached = attach2_task(
            hash,
            vec![child],
            Some(&column_allocator),
            &plan_allocator,
        )
        .expect("multi-distinct scalar MPP HashAgg attaches");
        let Task::Mpp(mpp) = attached else {
            panic!("multi-distinct scalar MPP HashAgg remains an MPP task");
        };
        let Some(PhysicalPlan::Projection(projection)) = mpp.plan() else {
            panic!("scalar MPP output restores the original schema");
        };
        let Some(PhysicalPlan::HashAgg(final_agg)) = projection.base.children().first() else {
            panic!("projection is above the final HashAgg");
        };
        let Some(PhysicalPlan::ExchangeReceiver(single_receiver)) =
            final_agg.base.children().first()
        else {
            panic!("final aggregation is fed by a single-partition exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(single_sender)) =
            single_receiver.base.children().first()
        else {
            panic!("the single-partition receiver owns its sender");
        };
        let Some(PhysicalPlan::HashAgg(middle)) = single_sender.base.children().first() else {
            panic!("the single-partition exchange feeds the middle HashAgg");
        };
        assert!(middle.group_by_items.is_empty());
        let Some(PhysicalPlan::ExchangeReceiver(hash_receiver)) = middle.base.children().first()
        else {
            panic!("the middle aggregation is fed by a hash exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = hash_receiver.base.children().first() else {
            panic!("the hash receiver owns its sender");
        };
        assert_eq!(sender.hash_cols.len(), 3, "a, b, and grouping ID partition the middle stage");
        let Some(PhysicalPlan::HashAgg(partial)) = sender.base.children().first() else {
            panic!("the hash exchange feeds the partial HashAgg");
        };
        assert_eq!(partial.group_by_items.len(), 3);
        let Some(PhysicalPlan::Projection(projection)) = partial.base.children().first() else {
            panic!("ordinary aggregates are guarded below the partial HashAgg");
        };
        assert_eq!(projection.exprs.len(), 5);
        let Some(PhysicalPlan::Expand(expand)) = projection.base.children().first() else {
            panic!("the partial projection is above the grouping-set Expand");
        };
        assert_eq!(expand.level_exprs.len(), 2);
        assert_eq!(expand.base.base.schema().map(Schema::len), Some(4));
    }

    #[test]
    fn a_selection_wraps_the_root_plan_and_keeps_its_own_stats() {
        // `attachPlan2Task`'s root arm: the old plan becomes the child, the
        // new plan becomes the task's, and Count reads the NEW top's stats.
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let task = attach2_task(
            selection,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("root attaches");
        assert!((task.count() - 8.0).abs() < f64::EPSILON);
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert_eq!(plan.children().len(), 1, "the old plan became the child");
    }

    #[test]
    fn an_index_join_inner_operator_inherits_probe_stats_and_receipt() {
        let mut child = root_task_over(3.0);
        let Task::Root(root) = &mut child else {
            unreachable!("the helper builds a root task");
        };
        root.index_join_info = Some(IndexJoinInfo {
            table_id: 42,
            ..IndexJoinInfo::default()
        });
        let projection = PhysicalPlan::Projection(crate::physical::PhysicalProjection {
            base: op_with_stats("Projection", 30_000.0),
            ..crate::physical::PhysicalProjection::default()
        });

        let task =
            attach2_task(projection, vec![child], None, &PlanIdAllocator::new()).expect("attaches");

        assert_eq!(task.count(), 3.0, "one dynamic probe, not the full input");
        let Task::Root(root) = task else {
            panic!("the projection remains a root task");
        };
        assert_eq!(
            root.index_join_info.as_ref().map(|info| info.table_id),
            Some(42),
            "the owning index join still receives the chosen access"
        );
    }

    #[test]
    fn index_join_completion_moves_unusable_equalities_to_residual_conditions() {
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let column = |id| Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let outer_keys = vec![column(1), column(2)];
        let inner_keys = vec![column(11), column(12)];
        let equality = |left: &Column, right: &Column| {
            ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                FieldType::new(FieldTypeCode::Tiny),
                vec![
                    Expression::Column(left.clone()),
                    Expression::Column(right.clone()),
                ],
            )
        };
        let child = |columns: Vec<Column>| {
            let mut base = op_with_stats("TableDual", 1.0);
            base.base.set_schema(Some(Schema::new(columns)));
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual { base, row_count: 1 })
        };
        let outer = child(outer_keys.clone());
        let inner = child(inner_keys.clone());
        let mut join = crate::physical::PhysicalIndexJoin {
            inner_child_idx: 1,
            left_join_keys: outer_keys.clone(),
            right_join_keys: inner_keys.clone(),
            outer_join_keys: outer_keys.clone(),
            inner_join_keys: inner_keys.clone(),
            is_null_eq: vec![false, false],
            equal_conditions: vec![
                equality(&outer_keys[0], &inner_keys[0]),
                equality(&outer_keys[1], &inner_keys[1]),
            ],
            // `PhysicalIndexJoin::default()`'s `kind` is the PLAIN
            // `IndexJoin` variant, not `IndexHashJoin` -- proving Go's
            // `extractOtherEQ=true` (`task.go:158,178`) applies to both.
            ..crate::physical::PhysicalIndexJoin::default()
        };
        assert_eq!(join.kind, crate::plan_cost_ver2::IndexJoinKind::IndexJoin);

        complete_physical_index_join(
            &mut join,
            IndexJoinInfo {
                table_id: 7,
                index_id: Some(8),
                ranges: crate::ranger::types::Ranges::new(),
                idx_col_lens: vec![tidb_datatype::UNSPECIFIED_LENGTH],
                key_off2_idx_off: vec![0, -1],
                access_conditions: vec![],
                range_rebuild: None,
                compare_filters: None,
            },
            &inner,
            &outer,
        )
        .expect("completes");

        assert_eq!(
            join.outer_join_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            join.inner_join_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![11]
        );
        assert_eq!(join.key_off2_idx_off, vec![0]);
        assert!(join.equal_conditions.is_empty());
        // The equality unusable as a lookup key (`outer_keys[1]` /
        // `inner_keys[1]`) is restored to `other_conditions` first, but it
        // is a bare `col <eq> col` with each side in its own child's
        // schema, so Go's unconditional `extractOtherEQ` immediately
        // promotes it back out into the hash keys instead of leaving it
        // stuck as a residual filter -- for THIS plain `IndexJoin`, not
        // only for `IndexHashJoin`.
        assert!(
            join.other_conditions.is_empty(),
            "the residual equality is a plain IndexJoin's hash key too"
        );
        assert_eq!(
            join.outer_hash_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![outer_keys[0].unique_id, outer_keys[1].unique_id]
        );
        assert_eq!(
            join.inner_hash_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![inner_keys[0].unique_id, inner_keys[1].unique_id]
        );
    }

    #[test]
    fn index_join_completion_leaves_a_non_column_residual_condition_alone() {
        // The extraction only promotes a BARE `col <eq> col`
        // (`exhaust_physical_plans.go:410-437`, `expression.IsColOpCol`): a
        // residual equality against a non-column operand (a cast, a
        // constant, ...) stays a filter for every index-join kind,
        // including the plain `IndexJoin` this fix now also extracts for.
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let column = |id| Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let outer_keys = vec![column(1)];
        let inner_keys = vec![column(11)];
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let equality_against_constant = ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(outer_keys[0].clone()),
                Expression::Constant(Constant::new(Datum::Int(5), ty)),
            ],
        );
        let child = |columns: Vec<Column>| {
            let mut base = op_with_stats("TableDual", 1.0);
            base.base.set_schema(Some(Schema::new(columns)));
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual { base, row_count: 1 })
        };
        let outer = child(outer_keys.clone());
        let inner = child(inner_keys.clone());
        let mut join = crate::physical::PhysicalIndexJoin {
            inner_child_idx: 1,
            left_join_keys: outer_keys.clone(),
            right_join_keys: inner_keys.clone(),
            outer_join_keys: outer_keys.clone(),
            inner_join_keys: inner_keys.clone(),
            is_null_eq: vec![false],
            equal_conditions: vec![equality_against_constant],
            ..crate::physical::PhysicalIndexJoin::default()
        };

        complete_physical_index_join(
            &mut join,
            IndexJoinInfo {
                table_id: 7,
                index_id: Some(8),
                ranges: crate::ranger::types::Ranges::new(),
                idx_col_lens: vec![tidb_datatype::UNSPECIFIED_LENGTH],
                key_off2_idx_off: vec![-1],
                access_conditions: vec![],
                range_rebuild: None,
                compare_filters: None,
            },
            &inner,
            &outer,
        )
        .expect("completes");

        assert_eq!(
            join.other_conditions.len(),
            1,
            "a non-column residual equality is never a hash key"
        );
        assert!(join.outer_hash_keys.is_empty());
        assert!(join.inner_hash_keys.is_empty());
    }

    #[test]
    fn a_sort_attaches_without_conversion() {
        // `attach2Task4PhysicalSort` is copy-then-attach, nothing else.
        let sort = PhysicalPlan::Sort(PhysicalSort {
            base: op_with_stats("Sort", 10.0),
            ..PhysicalSort::default()
        });
        let task = attach2_task(
            sort,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        assert!(matches!(task.plan(), Some(PhysicalPlan::Sort(_))));
    }

    #[test]
    fn an_only_column_nominal_sort_returns_the_child_task_itself() {
        // `attach2Task4NominalSort` (`task.go:853`): `if p.OnlyColumn {
        // return tasks[0] }` — the fake operator vanishes.
        let nominal = PhysicalPlan::NominalSort(crate::physical::NominalSort {
            base: op_with_stats("Sort", 10.0),
            only_column: true,
            ..crate::physical::NominalSort::default()
        });
        let task = attach2_task(
            nominal,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("passes through");
        assert!(
            matches!(task.plan(), Some(PhysicalPlan::TableDual(_))),
            "the child plan is still the task's plan"
        );
    }

    #[test]
    fn an_expression_nominal_sort_attaches_without_conversion() {
        // The non-only-column arm is copy-then-attach, exactly Sort's.
        let nominal = PhysicalPlan::NominalSort(crate::physical::NominalSort {
            base: op_with_stats("Sort", 10.0),
            only_column: false,
            ..crate::physical::NominalSort::default()
        });
        let task = attach2_task(
            nominal,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::NominalSort(_)));
        assert_eq!(plan.children().len(), 1, "the old plan became the child");
    }

    #[test]
    fn a_union_all_converts_every_child_and_owns_them() {
        // `attach2Task4PhysicalUnionAll` (`task.go:1573`): each child task
        // converts to root and its plan becomes a child of the union, which
        // sits in a fresh RootTask.
        let union = PhysicalPlan::UnionAll(crate::physical::PhysicalUnionAll {
            base: op_with_stats("Union", 20.0),
            mpp: false,
        });
        let task = attach2_task(
            union,
            vec![root_task_over(10.0), root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::UnionAll(_)));
        assert_eq!(plan.children().len(), 2, "both children wired in");
    }

    #[test]
    fn a_partition_union_over_an_mpp_child_is_invalid() {
        // Go: "PartitionUnion cannot pushdown to tiflash ... return
        // base.InvalidTask immediately".
        let mut base = op_with_stats("Union", 20.0);
        base.base.set_tp("PartitionUnion");
        let union = PhysicalPlan::UnionAll(crate::physical::PhysicalUnionAll { base, mpp: false });
        let mpp_child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base: op_with_stats("Dual", 1.0),
                ..crate::physical::PhysicalTableDual::default()
            }),
            crate::physical_property::MppPartitionType::Any,
            [],
        ));
        let task = attach2_task(
            union,
            vec![root_task_over(10.0), mpp_child],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("invalid, not error");
        assert!(task.invalid());
    }

    #[test]
    fn an_apply_builds_the_join_schema_and_inherits_both_warning_sets() {
        // `attach2Task4PhysicalApply` (`task.go:127`): both children convert,
        // the schema is BuildPhysicalJoinSchema's, and the fresh root task's
        // warnings are the concatenation of both children's.
        use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
        use tidb_expr::column::Column;
        use tidb_expr::schema::Schema;

        let child = |rows: f64, col_id: i64, not_null: bool| {
            let mut base = op_with_stats("Dual", rows);
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            if not_null {
                ft.add_flags(FieldTypeFlags::NOT_NULL);
            }
            let mut schema = Schema::default();
            schema.columns = vec![Column::new(col_id, ft)];
            base.base.set_schema(Some(schema));
            let mut root = RootTask::default();
            root.warnings.append_warning(format!("w{col_id}"));
            root.set_plan(PhysicalPlan::TableDual(
                crate::physical::PhysicalTableDual {
                    base,
                    ..crate::physical::PhysicalTableDual::default()
                },
            ));
            Task::Root(root)
        };

        let apply = PhysicalPlan::Apply(crate::physical::PhysicalApply {
            hash_join: crate::physical::PhysicalHashJoin {
                base: op_with_stats("Apply", 5.0),
                join_type: crate::find_best_task::LogicalJoinType::LeftOuter,
                inner_child_idx: 1,
                ..crate::physical::PhysicalHashJoin::default()
            },
            ..crate::physical::PhysicalApply::default()
        });
        let task = attach2_task(
            apply,
            vec![child(10.0, 1, true), child(3.0, 2, true)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let Task::Root(root) = &task else {
            panic!("a root task");
        };
        assert_eq!(root.warnings.warning_count(), 2, "both children's warnings");
        let plan = task.plan().expect("a plan");
        let schema = plan.base().base.schema().expect("the built join schema");
        assert_eq!(schema.len(), 2, "left + right merged");
        assert!(
            !schema.columns[1]
                .ret_type
                .as_ref()
                .expect("a type")
                .has_flag(FieldTypeFlags::NOT_NULL),
            "LeftOuter resets NOT NULL on the right half"
        );
        assert!(
            schema.columns[0]
                .ret_type
                .as_ref()
                .expect("a type")
                .has_flag(FieldTypeFlags::NOT_NULL),
            "the left half keeps its flag"
        );
    }

    #[test]
    fn a_root_sequence_vanishes_into_its_last_childs_task() {
        // `attach2Task4PhysicalSequence` (`task.go:2259`): any non-MPP child
        // returns tasks[len-1] UNCHANGED — the sequence plan and every
        // producer's task are discarded. Go's own body, reproduced.
        let sequence = PhysicalPlan::Sequence(crate::physical::PhysicalSequence {
            base: op_with_stats("Sequence", 5.0),
        });
        let task = attach2_task(
            sequence,
            vec![
                root_task_over(1.0),
                root_task_over(2.0),
                root_task_over(3.0),
            ],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("passes through");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::TableDual(_)),
            "the LAST child's own plan, no Sequence above it"
        );
        assert!(
            (plan.stats_info().expect("stats").row_count() - 3.0).abs() < f64::EPSILON,
            "specifically the last child"
        );
    }

    #[test]
    fn a_hash_join_concatenates_warnings_right_before_left() {
        // `attach2Task4PhysicalHashJoin` (`task.go:227`):
        // `CopyFrom(&rTask.Warnings, &lTask.Warnings)` — the RIGHT child's
        // warnings come first in the replay order.
        let child = |rows: f64, warning: &str| {
            let mut root = RootTask::default();
            root.warnings.append_warning(warning);
            root.set_plan(PhysicalPlan::TableDual(
                crate::physical::PhysicalTableDual {
                    base: op_with_stats("Dual", rows),
                    ..crate::physical::PhysicalTableDual::default()
                },
            ));
            Task::Root(root)
        };
        let join = PhysicalPlan::HashJoin(crate::physical::PhysicalHashJoin {
            base: op_with_stats("HashJoin", 5.0),
            ..crate::physical::PhysicalHashJoin::default()
        });
        let task = attach2_task(
            join,
            vec![child(1.0, "left"), child(2.0, "right")],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let Task::Root(root) = &task else {
            panic!("a root task");
        };
        let warnings = root.warnings.get_warnings();
        assert_eq!(
            warnings
                .iter()
                .map(|w| w.message.as_str())
                .collect::<Vec<_>>(),
            vec!["right", "left"],
            "Go copies the right child's warnings first"
        );
        assert_eq!(task.plan().expect("plan").children().len(), 2);
    }

    #[test]
    fn a_max_one_row_converts_then_attaches() {
        // No Attach2Task override: `BasePhysicalPlan.Attach2Task`
        // (`base_physical_plan.go:202`) converts to root, then attaches.
        let mor = PhysicalPlan::MaxOneRow(crate::physical::PhysicalMaxOneRow {
            base: op_with_stats("MaxOneRow", 1.0),
        });
        let task = attach2_task(
            mor,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        assert!(matches!(task.plan(), Some(PhysicalPlan::MaxOneRow(_))));
    }

    #[test]
    fn a_max_one_row_on_a_cop_task_propagates_the_conversion_refusal() {
        // The cop child's ConvertToRootTask builds readers
        // (`convertToRootTaskImpl`), which is refused — the default attach
        // body surfaces that refusal rather than skipping the conversion.
        let mor = PhysicalPlan::MaxOneRow(crate::physical::PhysicalMaxOneRow {
            base: op_with_stats("MaxOneRow", 1.0),
        });
        let error = attach2_task(
            mor,
            vec![Task::Cop(CopTask::default())],
            None,
            &PlanIdAllocator::new(),
        )
        .expect_err("refuses");
        assert!(
            format!("{error}").contains("convertToRootTaskImpl"),
            "the refusal names its Go symbol: {error}"
        );
    }

    #[test]
    fn a_selection_over_a_cop_child_converts_and_lands_at_root() {
        // `attach2Task4PhysicalSelection` (`task.go:1598`): Go pushes NO
        // selection at attach time — the cop child converts through its
        // reader and the selection sits above it at root.
        let scan = {
            let allocator = PlanIdAllocator::new();
            let mut base = crate::physical::BasePhysicalPlan::default();
            base.base = crate::plan_base::BasePlan::new(&allocator, "TableScan", 0);
            base.base.set_stats(Some(StatsInfo::new(9.0, [])));
            PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                base,
                table_id: 1,
                table_as_name: None,
                dynamic_partition_access: None,
                cost_columns: Vec::new(),
                store_type: crate::physical_table_reader::StoreType::TiKv,
                keep_order: false,
                desc: false,
                ranges: crate::ranger::types::Ranges::new(),
                ..Default::default()
            })
        };
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let cop = Task::Cop(CopTask {
            table_plan: Some(Box::new(scan)),
            index_plan_finished: true,
            ..CopTask::default()
        });
        let task = attach2_task(selection, vec![cop], None, &PlanIdAllocator::new())
            .expect("converts and attaches");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert!(
            matches!(plan.children().first(), Some(PhysicalPlan::TableReader(_))),
            "the selection sits ABOVE the reader"
        );
    }

    #[test]
    fn an_index_merge_cop_task_converts_to_the_retained_reader_tree() {
        let scan = |id: i32, rows: f64| {
            let mut base = op_with_stats("TableScan", rows);
            base.base.set_id(id);
            PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                base,
                table_id: 42,
                ..crate::physical::PhysicalTableScan::default()
            })
        };
        let task = CopTask {
            table_plan: Some(Box::new(scan(3, 5.0))),
            index_plan_finished: true,
            keep_order: true,
            idx_merge_part_plans: vec![scan(1, 3.0), scan(2, 2.0)],
            idx_merge_is_intersection: true,
            ..CopTask::default()
        }
        .convert_to_root_task_impl(&PlanIdAllocator::new())
        .expect("index merge converts to a root reader");

        let PhysicalPlan::IndexMergeReader(reader) = task.plan().expect("a retained reader") else {
            panic!("index merge reader");
        };
        assert_eq!(reader.partial_plans_raw.len(), 2);
        assert!(reader.table_plan.is_some());
        assert!(reader.is_intersection_type);
        assert!(reader.keep_order);
    }

    #[test]
    fn the_attach_copies_so_the_child_task_survives() {
        // Go's bodies `Copy()` the incoming task; the caller's task must not
        // observe the wrap.
        let original = root_task_over(10.0);
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let _ = attach2_task(
            selection,
            vec![original.copy()],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        assert!(
            matches!(original.plan(), Some(PhysicalPlan::TableDual(_))),
            "the original task keeps its own plan"
        );
    }
}
