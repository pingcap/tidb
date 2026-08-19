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
//! `pkg/planner/core/task.go` (2,319 lines) lands here ARM BY ARM as each
//! operator's dependencies close: [`attach2_task`] carries the ported
//! `attach2Task4PhysicalX` bodies (Sort, Selection, Projection, Limit,
//! MaxOneRow, NominalSort, Lock, UnionAll, Apply, HashJoin, Sequence), and
//! every unported body remains a refusal naming its Go symbol.
//!
//! SEED of `pkg/planner/core`'s task layer: the representation and its own
//! small methods land; the conversions that BUILD plans do not (see the
//! refusals below).
//!
//! # Closed enum, deliberately
//!
//! Go's `base.Task` is an interface with exactly three implementors,
//! asserted at the top of `task_base.go` (`var _ base.Task = &RootTask{}`
//! ...). [`Task`] is therefore a closed enum, the same decision
//! [`crate::logical::LogicalPlan`] and [`crate::physical::PhysicalPlan`]
//! made, for the same reason: no `_ =>` arm can hide an unhandled task kind,
//! and Go's own doc note — "appending the new adding method to the last, for
//! the convenience of easy locating in other implementor" — is the pain of
//! an open set this shape does not have.
//!
//! # Refusals, each naming its Go symbol
//!
//! * `MppTask.ConvertToRootTaskImpl` (`task_base.go:298-355`): builds a
//!   `PhysicalExchangeSender` + `PhysicalTableReader` pair, runs
//!   `cardinality.Selectivity` over `RootTaskConds`, and can fall back to
//!   `base.InvalidTask`. Building plans is `Attach2Task`-batch work;
//!   [`Task::convert_to_root_task`] refuses for an MPP task rather than
//!   fabricating a reader.
//! * `CopTask`'s `ConvertToRootTask` lives in `core/task.go`
//!   (`convertToRootTaskImpl`) and builds `PhysicalTableReader` /
//!   `PhysicalIndexReader` / `PhysicalIndexLookUpReader`; refused the same
//!   way.
//! * `CopTask.handleRootTaskConds` (`task.go`): `cardinality.Selectivity`
//!   plus a built `PhysicalSelection`; not ported, named here.
//!
//! # Narrowings
//!
//! * `context.SQLWarn` carries a Go `error`; [`SqlWarn`] carries the
//!   rendered message. Level and the `math.MaxUint16` cap are Go's.
//! * `physicalop.IndexJoinInfo` (the runtime range info an index join
//!   fetches from the data source) is unported; `RootTask.IndexJoinInfo` and
//!   `CopTask.IndexJoinInfo` are therefore absent, and
//!   [`crate::find_best_task::LeafRole`] remains the crate's stand-in for
//!   the property half of that mechanism.
//! * `statistics.HistColl` (`TblColHists`) is unported; the fields carrying
//!   it are absent. Network/scan-width costing that reads them is cost-model
//!   work, not representation work.
//! * `PhysPlanPartInfo`, `util.IndexLookUpPushDownByType`,
//!   `property.PhysicalPropMatchResult` and `PartialOrderMatchResult` are
//!   unported; their `CopTask` fields are absent by the same rule.
//! * `StatsInfo.StatsVersion` does not exist on the ported profile, so
//!   [`CopTask::finish_index_plan`] cannot preserve it; the stats MOVE is
//!   ported, the version pin is named here.

use crate::physical::PhysicalPlan;
use crate::physical_property::MppPartitionType;
use crate::physical_table_reader::StoreType;
use crate::plan_base::PlanError;
use tidb_expr::expression::Expression;

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
    plan: Option<Box<PhysicalPlan>>,
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
        self.plan = Some(Box::new(plan));
    }

    /// Takes the plan out, for the ownership handoff `attachPlan2Task`'s
    /// `p.SetChildren(v.GetPlan()); v.SetPlan(p)` pair performs.
    pub fn take_plan(&mut self) -> Option<PhysicalPlan> {
        self.plan.take().map(|plan| *plan)
    }

    /// Go `Copy` (`task_base.go:131-142`): same plan, warnings COPIED so the
    /// two instances never share a slice. Go copies the plan POINTER where
    /// this clones the owned tree; observably equal until someone mutates a
    /// shared plan through one task, which Go's planner does not do between
    /// copies.
    #[must_use]
    pub fn copy(&self) -> RootTask {
        let mut copied = RootTask {
            plan: self.plan.clone(),
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
    // boundary: `HashCols []*property.MPPPartitionColumn` — the partition
    // column carries collation alongside the column; unported.
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
        let mut task = MppTask {
            plan: Some(Box::new(plan)),
            part_tp,
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

    /// Go `Copy`: struct copy plus a fresh warnings slice.
    #[must_use]
    pub fn copy(&self) -> MppTask {
        let mut copied = self.clone();
        copied.warnings = SimpleWarnings::default();
        copied.warnings.copy_of(&self.warnings);
        copied
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
    /// Go `IndexPlanFinished`: whether the index half is sealed, which
    /// decides which half [`CopTask::plan`] and [`CopTask::count`] read.
    pub index_plan_finished: bool,
    /// Go `KeepOrder`.
    pub keep_order: bool,
    /// Go `NeedExtraProj`: a double read may output one extra handle column
    /// that must be pruned above.
    pub need_extra_proj: bool,
    /// Go `IdxMergePartPlans`: the real plans of an index-merge reader while
    /// `IndexPlanFinished` is false.
    pub idx_merge_part_plans: Vec<PhysicalPlan>,
    /// Go `IdxMergeIsIntersection`.
    pub idx_merge_is_intersection: bool,
    /// Go `RootTaskConds`: selections carrying virtual columns, which cannot
    /// push to TiKV.
    pub root_task_conds: Vec<Expression>,
    /// Go `ExpectCnt`: the upper task's expected row count, `0` for
    /// unlimited; decides paging distsql.
    pub expect_cnt: u64,
    // boundary: `OriginSchema`, `ExtraHandleCol`, `CommonHandleCols`,
    // `TblColHists`, `TblCols`, `IdxMergeAccessMVIndex`,
    // `IdxMergeMatchWithAdvisorySortItems`, `IdxMergePartPlansMatchResults`,
    // `PhysPlanPartInfo`, `IndexJoinInfo`, `IndexLookUpPushDownBy`,
    // `PartialOrderMatchResult` — each blocked on an unported type named in
    // the module header, absent rather than stubbed.
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
        let mut copied = self.clone();
        copied.warnings = SimpleWarnings::default();
        copied.warnings.copy_of(&self.warnings);
        copied
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
            table.base_mut().base.set_stats(index_stats);
        }
    }

    /// Go `CopTask.convertToRootTaskImpl` (`task_base.go:509`), the
    /// TABLE-ONLY branch: seal the index half, walk to the bottom
    /// `PhysicalTableScan`, and wrap the pushed-down plan in a
    /// `PhysicalTableReader` carrying the scan's store type — with the
    /// task's warnings copied onto the fresh root task, Go's deferred tail.
    ///
    /// The other shapes refuse by name: an index half needs
    /// `PhysicalIndexReader`/`BuildIndexLookUpTask`, index-merge needs
    /// `PhysicalIndexMergeReader`, and `RootTaskConds` need
    /// `handleRootTaskConds` (`cardinality.Selectivity` over a built
    /// Selection). `ExpandVirtualColumn`/`NeedExtraProj` narrow with
    /// virtual columns, which the ported scan does not carry;
    /// `IsCommonHandle` narrows with the unported `table.Table` binding and
    /// stays false. Go's `Init` allocates the reader a FRESH plan id from
    /// the context; this conversion path carries no allocator, so the
    /// reader reuses the pushed-down plan's id — a named narrowing, visible
    /// only in explain-id suffixes.
    /// Go `CopTask.handleRootTaskConds` (`physicalop/task.go:47`): the
    /// conditions that could not push down (virtual columns) become a
    /// `PhysicalSelection` at root, `FromDataSource`, its stats scaled by
    /// the conditions' selectivity. `cardinality.Selectivity` reads the
    /// table's histograms (`TblColHists`), which this port's tasks do not
    /// carry — the scaling therefore takes Go's OWN error fallback,
    /// `cost.SelectionFactor` (0.8), the value Go uses whenever the
    /// histogram read fails. The skew ratio is Go's default 1.0.
    fn handle_root_task_conds(conds: Vec<Expression>, mut root: RootTask) -> RootTask {
        if conds.is_empty() {
            return root;
        }
        let Some(plan) = root.take_plan() else {
            return root;
        };
        let selectivity = crate::cost_factors::SELECTION_FACTOR;
        let mut base = crate::physical::BasePhysicalPlan::with_id(
            plan.id(),
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
    /// side's (`Init`, `:205`). `IndexLookUpPushDownBy` is always
    /// `IndexLookUpPushDownNone` on this port (`tryPushDownLookUp` returns
    /// immediately), and the `NeedExtraProj` projection reads the unported
    /// `OriginSchema` — a task that needs it refuses by name rather than
    /// serving a broken schema. Go skips that projection when the table side
    /// already holds a pushed partial aggregate.
    fn build_index_look_up_task(mut self) -> Result<Task, PlanError> {
        let index_plan = self
            .index_plan
            .take()
            .ok_or_else(|| PlanError::internal("BuildIndexLookUpTask without an index half"))?;
        let table_plan = self
            .table_plan
            .take()
            .ok_or_else(|| PlanError::internal("BuildIndexLookUpTask without a table half"))?;
        let agg_pushed_down = matches!(
            &*table_plan,
            PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_)
        );
        if self.need_extra_proj && !agg_pushed_down {
            return Err(PlanError::internal(
                "BuildIndexLookUpTask: the NeedExtraProj projection reads \
                 OriginSchema, not ported",
            ));
        }
        let mut base = crate::physical::BasePhysicalPlan::with_id(
            table_plan.id(),
            "IndexLookUp",
            table_plan.query_block_offset(),
        );
        base.base.set_stats(table_plan.stats_info().cloned());
        base.base.set_schema(table_plan.schema().cloned());
        let reader =
            PhysicalPlan::IndexLookUpReader(crate::physical::PhysicalIndexLookUpReader {
                base,
                index_plan: Some(index_plan),
                table_plan: Some(table_plan),
                keep_order: self.keep_order,
                expect_cnt: self.expect_cnt,
            });
        let mut root = RootTask::default();
        root.set_plan(reader);
        if self.warnings.warning_count() > 0 {
            root.warnings.copy_of(&self.warnings);
        }
        Ok(Task::Root(root))
    }

    pub fn convert_to_root_task_impl(mut self) -> Result<Task, PlanError> {
        if !self.idx_merge_part_plans.is_empty() {
            return Err(PlanError::internal(
                "convertToRootTaskImpl: the index-merge branch builds \
                 PhysicalIndexMergeReader, not ported",
            ));
        }
        if self.index_plan.is_some() && self.table_plan.is_some() {
            let conds = std::mem::take(&mut self.root_task_conds);
            return self.build_index_look_up_task().map(|task| match task {
                Task::Root(root) => Task::Root(Self::handle_root_task_conds(conds, root)),
                other => other,
            });
        }
        if let Some(index_plan) = self.index_plan.take() {
            // Go's index branch (`task_base.go:563`): wrap the pushed-down
            // index plan in a PhysicalIndexReader carrying its stats, at its
            // query-block offset. The reader reuses the pushed plan's id —
            // the same named narrowing as the table branch.
            let mut base = crate::physical::BasePhysicalPlan::with_id(
                index_plan.id(),
                "IndexReader",
                index_plan.query_block_offset(),
            );
            base.base.set_stats(index_plan.stats_info().cloned());
            let reader = PhysicalPlan::IndexReader(crate::physical::PhysicalIndexReader {
                base,
                index_plan: Some(index_plan),
            });
            let mut root = RootTask::default();
            root.set_plan(reader);
            if self.warnings.warning_count() > 0 {
                root.warnings.copy_of(&self.warnings);
            }
            let conds = std::mem::take(&mut self.root_task_conds);
            return Ok(Task::Root(Self::handle_root_task_conds(conds, root)));
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
        let mut base = crate::physical::BasePhysicalPlan::with_id(
            table_plan.id(),
            "TableReader",
            table_plan.query_block_offset(),
        );
        base.base.set_stats(table_plan.stats_info().cloned());
        let reader = PhysicalPlan::TableReader(crate::physical::PhysicalTableReader {
            base,
            table_plan: Some(Box::new(*table_plan)),
            store_type,
            is_common_handle: false,
        });
        let mut root = RootTask::default();
        root.set_plan(reader);
        if self.warnings.warning_count() > 0 {
            root.warnings.copy_of(&self.warnings);
        }
        let conds = std::mem::take(&mut self.root_task_conds);
        Ok(Task::Root(Self::handle_root_task_conds(conds, root)))
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

    /// Go `Invalid()`.
    #[must_use]
    pub fn invalid(&self) -> bool {
        match self {
            Task::Root(task) => task.plan.is_none(),
            Task::Cop(task) => task.invalid(),
            Task::Mpp(task) => task.plan.is_none(),
        }
    }

    /// Go `ConvertToRootTask(ctx)`.
    ///
    /// A root task converts by copying — Go's own body. The other two BUILD
    /// plan nodes to convert (`MppTask.ConvertToRootTaskImpl` builds an
    /// exchange pair; `CopTask`'s impl in `core/task.go` builds readers);
    /// both refuse here by name rather than fabricating a reader, per the
    /// module header.
    pub fn convert_to_root_task(&self) -> Result<Task, PlanError> {
        match self {
            Task::Root(task) => Ok(Task::Root(task.copy())),
            Task::Cop(task) => task.copy().convert_to_root_task_impl(),
            Task::Mpp(_) => Err(PlanError::internal(
                "MppTask.ConvertToRootTaskImpl (task_base.go:298) is not ported: it \
                 builds a PhysicalExchangeSender + PhysicalTableReader pair",
            )),
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
    use crate::physical_table_dual::PLAN_TYPE as DUAL_TYPE;
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::StatsInfo;

    // All WRITTEN: Go's coverage of the task layer is exercised through
    // planner integration suites; `task_base.go` has no unit tests of its
    // own beside them.

    fn dual_with_rows(rows: f64) -> PhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = crate::physical::BasePhysicalPlan::default();
        base.base = crate::plan_base::BasePlan::new(&allocator, DUAL_TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        PhysicalPlan::TableDual(crate::physical::PhysicalTableDual { base, row_count: 0 })
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
            store_type: crate::physical_table_reader::StoreType::TiKv,
            keep_order: false,
            desc: false,
        });
        let mut cop = CopTask {
            table_plan: Some(Box::new(scan)),
            index_plan_finished: true,
            ..CopTask::default()
        };
        cop.warnings.append_warning("carried");
        let task = Task::Cop(cop).convert_to_root_task().expect("converts");
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
            matches!(reader.table_plan.as_deref(), Some(PhysicalPlan::TableScan(_))),
            "the pushed-down side hangs off TablePlan, not the child list"
        );
        assert!(reader.base.children().is_empty());
        assert!(
            (task.plan().expect("plan").stats_info().expect("stats").row_count() - 42.0).abs()
                < f64::EPSILON,
            "the reader carries the table plan's stats"
        );
    }

    #[test]
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
        let converted = task.convert_to_root_task().expect("converts");
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
        let converted = task.convert_to_root_task().expect("converts");
        let Some(PhysicalPlan::IndexReader(reader)) = converted.plan() else {
            panic!("an IndexReader, got {:?}", converted.plan());
        };
        assert!(reader.index_plan.is_some());
        assert!(
            (converted.plan().expect("plan").stats_info().expect("stats").row_count() - 4.0)
                .abs()
                < f64::EPSILON
        );

        let double = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(1.0))),
            table_plan: Some(Box::new(dual_with_rows(3.0))),
            keep_order: true,
            ..CopTask::default()
        });
        let converted = double.convert_to_root_task().expect("builds");
        let Some(PhysicalPlan::IndexLookUpReader(lookup)) = converted.plan() else {
            panic!("an IndexLookUpReader, got {:?}", converted.plan());
        };
        assert!(lookup.keep_order);
        // The reader's stats are the TABLE side's.
        assert!(
            (converted.plan().expect("plan").stats_info().expect("stats").row_count() - 3.0)
                .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn an_empty_root_task_is_gos_invalid_task() {
        // `base.InvalidTask` is "core's empty RootTask".
        let task = Task::invalid_task();
        assert!(task.invalid());
        assert!(task.plan().is_none());
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
        let error = cop.convert_to_root_task().expect_err("refuses");
        assert!(format!("{error:?}").contains("convertToRootTaskImpl"));
        let mpp = Task::Mpp(MppTask::default());
        let error = mpp.convert_to_root_task().expect_err("refuses");
        assert!(format!("{error:?}").contains("ConvertToRootTaskImpl"));
    }

    #[test]
    fn a_root_task_converts_by_copying() {
        let mut root = RootTask::default();
        root.set_plan(dual_with_rows(3.0));
        let task = Task::Root(root);
        let converted = task.convert_to_root_task().expect("root -> root");
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
/// Go's `inheritStatsFromBottomTaskForIndexJoinInner` hook runs first there;
/// it reads `IndexJoinInfo`, a field this port's tasks do not carry (module
/// header), so the hook is vacuous here and not restated.
#[must_use]
pub fn attach_plan_to_task(mut plan: PhysicalPlan, mut task: Task) -> Task {
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

/// Go `Attach2Task` per operator — the ROOT-TASK slice.
///
/// Every ported arm reproduces its Go body exactly for a root child task.
/// The cop and MPP branches each need machinery that is not here —
/// `expression.CanExprsPushDown` for the push-down decisions,
/// `convertToRootTaskImpl` to finish a cop task into readers, the pushed
/// TopN/Limit constructions — and REFUSE naming those symbols rather than
/// composing something Go would not compose. A wrong push-down is a silent
/// wrong plan; a refusal is a loud gap.
/// The shared cop body of Go `attach2Task4PhysicalStreamAgg` and
/// `...HashAgg` after their gates: split via `NewPartialAggregate`, hang the
/// partial half on the cop task's live side, convert, and attach the final
/// half at root. Go's `inheritStatsFromBottomElemForIndexJoinInner` call is
/// an `IndexJoinInfo` no-op on this port's tasks (module header).
fn attach_agg_over_cop(
    plan: PhysicalPlan,
    mut cop: CopTask,
    column_ids: Option<&crate::expression_rewriter::ColumnIdAllocator>,
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
        crate::final_mode_agg::new_partial_aggregate(&ctx, column_ids, plan)?;
    if let Some(mut partial) = partial {
        if let Some(table_plan) = cop.table_plan.take() {
            cop.finish_index_plan();
            partial.base_mut().set_children(vec![*table_plan]);
            cop.table_plan = Some(Box::new(partial));
            // Go: the pushed agg's schema replaces the extra projection a
            // double read would otherwise re-add above the reader.
            cop.need_extra_proj = false;
        } else if let Some(index_plan) = cop.index_plan.take() {
            partial.base_mut().set_children(vec![*index_plan]);
            cop.index_plan = Some(Box::new(partial));
        } else {
            return Err(PlanError::internal(
                "an aggregate cop push over a task with neither half",
            ));
        }
    }
    let t = Task::Cop(cop).convert_to_root_task()?;
    Ok(attach_plan_to_task(final_plan, t))
}

pub fn attach2_task(
    plan: PhysicalPlan,
    mut tasks: Vec<Task>,
    column_ids: Option<&crate::expression_rewriter::ColumnIdAllocator>,
) -> Result<Task, PlanError> {
    let first = tasks
        .drain(..1)
        .next()
        .ok_or_else(|| PlanError::internal("attach2_task with no child task"))?;
    match &plan {
        // `attach2Task4PhysicalSort` (`task.go:843`): copy, attach. No
        // conversion — findBestTask only asks a Sort under a root property.
        PhysicalPlan::Sort(_) => Ok(attach_plan_to_task(plan, first.copy())),
        // `attach2Task4PhysicalSelection` (`task.go:1598`): Go has NO cop
        // push at attach — a cop child CONVERTS and the selection lands at
        // root (pushed filters ride the DataSource's PushedDownConds
        // instead). Only the MPP arm pushes, gated on the TiFlash
        // `CanExprsPushDown`; refused by name.
        PhysicalPlan::Selection(_) => match &first {
            Task::Root(_) | Task::Cop(_) => {
                let converted = first.convert_to_root_task()?;
                Ok(attach_plan_to_task(plan, converted))
            }
            Task::Mpp(_) => Err(PlanError::internal(
                "attach2Task4PhysicalSelection's MPP arm needs the TiFlash \
                 CanExprsPushDown, not ported",
            )),
        },
        // `attach2Task4PhysicalProjection` (`task.go:1506`): the cop arm
        // pushes the projection onto the cop task — staying a COP task —
        // when there are no root conds, no index-merge parts, and every
        // expr passes the TiKV gate (`crate::pushdown`); an unfinished
        // index half finishes first (the conservative arm of
        // `canPushToIndexPlan`'s column check). Otherwise, and for a root
        // child, convert-then-attach. The MPP arm refuses by name.
        PhysicalPlan::Projection(_) => match &first {
            Task::Root(_) => {
                let converted = first.copy().convert_to_root_task()?;
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
                    let Task::Cop(mut cop) = first.copy() else {
                        unreachable!("the arm matched Cop");
                    };
                    if !cop.index_plan_finished {
                        cop.finish_index_plan();
                    }
                    Ok(attach_plan_to_task(plan, Task::Cop(cop)))
                } else {
                    let converted = first.copy().convert_to_root_task()?;
                    Ok(attach_plan_to_task(plan, converted))
                }
            }
            Task::Mpp(_) => Err(PlanError::internal(
                "attach2Task4PhysicalProjection's MPP arm needs the TiFlash \
                 CanExprsPushDown, not ported",
            )),
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
                Task::Root(_) => first.copy().convert_to_root_task()?,
                Task::Cop(_) => {
                    let Task::Cop(mut cop) = first.copy() else {
                        unreachable!("the arm matched Cop");
                    };
                    let pushable = (!cop.keep_order
                        || !cop.index_plan_finished
                        || cop.index_plan.is_none())
                        && cop.root_task_conds.is_empty();
                    if pushable {
                        let new_count = limit.offset + limit.count;
                        let stats = cop
                            .plan()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = crate::physical::BasePhysicalPlan::with_id(
                            plan.id(),
                            "Limit",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        // "Don't use clone() so that Limit and its children
                        // share the same schema": the pushed limit reports
                        // the open half's schema.
                        base.base.set_schema(
                            cop.plan().and_then(PhysicalPlan::schema).cloned(),
                        );
                        let pushed = PhysicalPlan::Limit(crate::physical::PhysicalLimit {
                            base,
                            partition_by: limit.partition_by.clone(),
                            offset: 0,
                            count: new_count,
                            prefix_col: None,
                            prefix_len: 0,
                        });
                        let Task::Cop(pushed_cop) =
                            attach_plan_to_task(pushed, Task::Cop(cop))
                        else {
                            unreachable!("attaching onto a cop task answers a cop task");
                        };
                        cop = pushed_cop;
                    }
                    Task::Cop(cop).convert_to_root_task()?
                }
                Task::Mpp(_) => {
                    return Err(PlanError::internal(
                        "attach2Task4PhysicalLimit's MPP arm (task.go:713) is \
                         not ported",
                    ))
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
            let converted = first.convert_to_root_task()?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // `PhysicalLock` has no override: the default convert-then-attach
        // body, exactly as `PhysicalMaxOneRow`'s arm above.
        PhysicalPlan::Lock(_) => {
            let converted = first.convert_to_root_task()?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // `attach2Task4PhysicalUnionAll` (`task.go:1573`): convert EVERY
        // child task to root and wire the multi-child plan into a fresh
        // RootTask. Go's MPP arm: a PartitionUnion over any MPP child is the
        // invalid task outright ("PartitionUnion cannot pushdown to
        // tiflash"); a plain union over MPP children needs
        // `attach2MppTasks4PhysicalUnionAll`, refused by name.
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
                return Err(PlanError::internal(
                    "attach2MppTasks4PhysicalUnionAll (task.go) is not ported",
                ));
            }
            let mut plan = plan;
            let mut children = Vec::with_capacity(tasks.len());
            for task in tasks.drain(..) {
                let Task::Root(mut converted) = task.convert_to_root_task()? else {
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
            let Task::Root(mut left) = first.convert_to_root_task()? else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let Task::Root(mut right) = second.convert_to_root_task()? else {
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
            Ok(attach_plan_to_task(plan, first.copy()))
        }
        // `attach2Task4PhysicalSequence` (`task.go:2259`): when ANY child
        // task is not MPP, the sequence VANISHES — the last child's task is
        // returned unchanged and every producer's task is discarded. That is
        // Go's own body ("if !isMpp { return tasks[len(tasks)-1] }"), a
        // quirk reproduced rather than fixed: on a root-tier plan the CTE
        // producers are wired elsewhere, not through this attach. The
        // all-MPP arm builds an MppTask over the last child's partition
        // columns — unported, refused by name.
        PhysicalPlan::Sequence(_) => {
            let mut all = vec![first];
            all.append(&mut tasks);
            if all.iter().any(|task| !matches!(task, Task::Mpp(_))) {
                return Ok(all
                    .pop()
                    .expect("the vec was built with at least one task"));
            }
            Err(PlanError::internal(
                "attach2Task4PhysicalSequence's all-MPP arm (task.go:2269, \
                 NewMppTask over GetHashCols) is not ported",
            ))
        }
        // `attach2Task4PhysicalTopN` (`task.go:1249`), the SIMPLE path:
        // when the by-items carry columns, pass the TiKV gate, and the cop
        // task has no root conds, push a partial TopN — ByItems cloned,
        // `Count = Offset + Count`, offset removed, `DeriveLimitStats` over
        // the open half (`getPushedDownTopN`'s non-heavy half) — then
        // convert and attach the ROOT TopN above (partition-by root skip).
        // The heavy-function rewrite, partial-order, TiDB-cop,
        // index-merge-advisory, and virtual-column arms narrow by name.
        PhysicalPlan::TopN(_) => {
            let PhysicalPlan::TopN(topn) = &plan else {
                unreachable!("the arm matched TopN");
            };
            let t = match first {
                Task::Root(_) => first.copy().convert_to_root_task()?,
                Task::Cop(_) => {
                    let Task::Cop(mut cop) = first.copy() else {
                        unreachable!("the arm matched Cop");
                    };
                    let by_exprs: Vec<tidb_expr::expression::Expression> = topn
                        .by_items
                        .iter()
                        .map(|item| item.expr.clone())
                        .collect();
                    let need_push_down = by_exprs.iter().any(|expr| {
                        !matches!(expr, tidb_expr::expression::Expression::Constant(_))
                    });
                    let pushable = need_push_down
                        && crate::pushdown::can_exprs_push_down_tikv(&by_exprs)
                        && cop.root_task_conds.is_empty();
                    if pushable {
                        let new_count = topn.offset + topn.count;
                        let stats = cop
                            .plan()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = crate::physical::BasePhysicalPlan::with_id(
                            plan.id(),
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
                        });
                        let Task::Cop(pushed_cop) =
                            attach_plan_to_task(pushed, Task::Cop(cop))
                        else {
                            unreachable!("attaching onto a cop task answers a cop task");
                        };
                        cop = pushed_cop;
                    }
                    Task::Cop(cop).convert_to_root_task()?
                }
                Task::Mpp(_) => {
                    return Err(PlanError::internal(
                        "attach2Task4PhysicalTopN's MPP arm is not ported",
                    ))
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
        PhysicalPlan::StreamAgg(_) => match first.copy() {
            Task::Cop(cop) => {
                if (cop.index_plan.is_some() && cop.table_plan.is_some() && cop.keep_order)
                    || !cop.root_task_conds.is_empty()
                    || !cop.idx_merge_part_plans.is_empty()
                {
                    let t = Task::Cop(cop).convert_to_root_task()?;
                    Ok(attach_plan_to_task(plan, t))
                } else {
                    attach_agg_over_cop(plan, cop, column_ids)
                }
            }
            Task::Mpp(_) => Err(PlanError::internal(
                "attach2Task4PhysicalStreamAgg's MPP arm is not ported",
            )),
            root @ Task::Root(_) => {
                Ok(attach_plan_to_task(plan, root.convert_to_root_task()?))
            }
        },
        // `attach2Task4PhysicalHashAgg` (`task.go:2162`): same split, gated
        // only on root-side filters and index merge.
        PhysicalPlan::HashAgg(_) => match first.copy() {
            Task::Cop(cop) => {
                if cop.root_task_conds.is_empty() && cop.idx_merge_part_plans.is_empty() {
                    attach_agg_over_cop(plan, cop, column_ids)
                } else {
                    let t = Task::Cop(cop).convert_to_root_task()?;
                    Ok(attach_plan_to_task(plan, t))
                }
            }
            Task::Mpp(_) => Err(PlanError::internal(
                "attach2Task4PhysicalHashAgg's MPP arm is not ported",
            )),
            root @ Task::Root(_) => {
                Ok(attach_plan_to_task(plan, root.convert_to_root_task()?))
            }
        },
        // `attach2Task4PhysicalHashJoin` (`task.go:211`): convert BOTH
        // children — Go converts the RIGHT one first — wire them in, and
        // concatenate warnings right-before-left, which is the order SHOW
        // WARNINGS replays them in. `StoreTp == kv.TiFlash` routes to the
        // TiFlash attach in Go; the enum's hash join carries no store type,
        // with no TiFlash tier to route to. `IndexJoinInfo` is a named
        // narrowing on this port's tasks (module header).
        PhysicalPlan::HashJoin(_) => {
            let second = tasks.drain(..1).next().ok_or_else(|| {
                PlanError::internal("attach2Task4PhysicalHashJoin needs two tasks")
            })?;
            let Task::Root(mut right) = second.convert_to_root_task()? else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let Task::Root(mut left) = first.convert_to_root_task()? else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let (Some(left_plan), Some(right_plan)) = (left.take_plan(), right.take_plan()) else {
                return Err(PlanError::internal(
                    "attach2Task4PhysicalHashJoin: a child task has no plan",
                ));
            };
            let mut plan = plan;
            plan.base_mut().set_children(vec![left_plan, right_plan]);
            let mut root = RootTask::default();
            root.set_plan(plan);
            root.warnings
                .copy_from([&right.warnings, &left.warnings]);
            Ok(Task::Root(root))
        }
        PhysicalPlan::TableScan(_) => Err(PlanError::internal(
            "a PhysicalTableScan is born inside a cop task by findBestTask, \
             never attached (convertToTableScan, find_best_task.go)",
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
        PhysicalPlan::CTETable(_) => Err(PlanError::internal(
            "a PhysicalCTETable is born inside its own root task by \
             findBestTask4LogicalCTETable (physical_cte_table.go), never \
             attached",
        )),
        PhysicalPlan::Show(_) | PhysicalPlan::ShowDDLJobs(_) => Err(PlanError::internal(
            "a PhysicalShow/PhysicalShowDDLJobs is born inside its own root \
             task by findBestTask4LogicalShow{,DDLJobs} (physical_show.go), \
             never attached",
        )),
        PhysicalPlan::Todo(op) => Err(PlanError::internal(format!(
            "attach2_task: {} is not ported",
            op.go_operator
        ))),
    }
}

#[cfg(test)]
mod attach_tests {
    use super::*;
    use crate::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalSelection, PhysicalSort};
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::StatsInfo;

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

    #[test]
    fn a_selection_wraps_the_root_plan_and_keeps_its_own_stats() {
        // `attachPlan2Task`'s root arm: the old plan becomes the child, the
        // new plan becomes the task's, and Count reads the NEW top's stats.
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let task = attach2_task(selection, vec![root_task_over(10.0)], None).expect("root attaches");
        assert!((task.count() - 8.0).abs() < f64::EPSILON);
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert_eq!(plan.children().len(), 1, "the old plan became the child");
    }

    #[test]
    fn a_sort_attaches_without_conversion() {
        // `attach2Task4PhysicalSort` is copy-then-attach, nothing else.
        let sort = PhysicalPlan::Sort(PhysicalSort {
            base: op_with_stats("Sort", 10.0),
            ..PhysicalSort::default()
        });
        let task = attach2_task(sort, vec![root_task_over(10.0)], None).expect("attaches");
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
        let task = attach2_task(nominal, vec![root_task_over(10.0)], None).expect("passes through");
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
        let task = attach2_task(nominal, vec![root_task_over(10.0)], None).expect("attaches");
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
        let task = attach2_task(union, vec![root_task_over(10.0), root_task_over(10.0)], None)
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
        let union = PhysicalPlan::UnionAll(crate::physical::PhysicalUnionAll {
            base,
            mpp: false,
        });
        let mpp_child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base: op_with_stats("Dual", 1.0),
                ..crate::physical::PhysicalTableDual::default()
            }),
            crate::physical_property::MppPartitionType::Any,
            [],
        ));
        let task =
            attach2_task(union, vec![root_task_over(10.0), mpp_child], None).expect("invalid, not error");
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
            root.set_plan(PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base,
                ..crate::physical::PhysicalTableDual::default()
            }));
            Task::Root(root)
        };

        let apply = PhysicalPlan::Apply(crate::physical::PhysicalApply {
            hash_join: crate::physical::PhysicalHashJoin {
                base: op_with_stats("Apply", 5.0),
                join_type: crate::find_best_task::LogicalJoinType::LeftOuter,
                inner_child_idx: 1,
            },
            ..crate::physical::PhysicalApply::default()
        });
        let task =
            attach2_task(apply, vec![child(10.0, 1, true), child(3.0, 2, true)], None).expect("attaches");
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
            vec![root_task_over(1.0), root_task_over(2.0), root_task_over(3.0)],
            None,
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
            root.set_plan(PhysicalPlan::TableDual(crate::physical::PhysicalTableDual {
                base: op_with_stats("Dual", rows),
                ..crate::physical::PhysicalTableDual::default()
            }));
            Task::Root(root)
        };
        let join = PhysicalPlan::HashJoin(crate::physical::PhysicalHashJoin {
            base: op_with_stats("HashJoin", 5.0),
            ..crate::physical::PhysicalHashJoin::default()
        });
        let task = attach2_task(join, vec![child(1.0, "left"), child(2.0, "right")], None)
            .expect("attaches");
        let Task::Root(root) = &task else {
            panic!("a root task");
        };
        let warnings = root.warnings.get_warnings();
        assert_eq!(
            warnings.iter().map(|w| w.message.as_str()).collect::<Vec<_>>(),
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
        let task = attach2_task(mor, vec![root_task_over(10.0)], None).expect("attaches");
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
        let error =
            attach2_task(mor, vec![Task::Cop(CopTask::default())], None).expect_err("refuses");
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
                store_type: crate::physical_table_reader::StoreType::TiKv,
                keep_order: false,
                desc: false,
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
        let task = attach2_task(selection, vec![cop], None).expect("converts and attaches");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert!(
            matches!(plan.children().first(), Some(PhysicalPlan::TableReader(_))),
            "the selection sits ABOVE the reader"
        );
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
        let _ = attach2_task(selection, vec![original.copy()], None).expect("attaches");
        assert!(
            matches!(original.plan(), Some(PhysicalPlan::TableDual(_))),
            "the original task keeps its own plan"
        );
    }
}
