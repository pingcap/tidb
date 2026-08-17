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
//! NOT here, by name: the 2,319-line `pkg/planner/core/task.go`, which is
//! every operator's `Attach2Task` body — a later batch. The empty
//! `attach2_task` stub in [`crate::physical`] keeps standing for it.
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
            Task::Cop(_) => Err(PlanError::internal(
                "convertToRootTaskImpl (core/task.go) is not ported: converting a \
                 CopTask builds PhysicalTableReader/IndexReader/IndexLookUpReader",
            )),
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
