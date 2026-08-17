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

//! The root of the plan interface tree.
//!
//! Go sources:
//! * `pkg/planner/core/base/plan_base.go` — the `Plan` interface (lines 45-94),
//!   `JoinType` (305-371), `PossiblePropertiesInfo` (383-440).
//! * `pkg/planner/core/operator/baseimpl/plan.go` — the `Plan` struct every
//!   operator embeds (lines 31-166).
//!
//! SEED of `pkg/planner/core`: this file carries the base state and the
//! `base.Plan` member surface. The operator set is a later batch.
//!
//! # Narrowings, by name
//!
//! * `Plan.SCtx()` / `SetSCtx` / `PlanContext`. Go's plan holds a live
//!   `sessionctx`, which is how `NewBasePlan` allocates its id
//!   (`ctx.GetSessionVars().PlanID.Add(1)`) and how `ExplainID` reads
//!   `StmtCtx.IgnoreExplainIDSuffix`. No session owner exists here, so id
//!   allocation is an explicit [`PlanIdAllocator`] argument and the explain
//!   suffix is an explicit `ignore_suffix` parameter. An absent field cannot
//!   be read as a wrong answer.
//! * `Plan.ReplaceExprColumns`. The default Go body is empty and every real
//!   body walks operator-specific expressions, which do not exist yet; the
//!   method is present with its signature and the base no-op body.
//! * `Plan.CloneForPlanCache`. Go returns `(nil, false)` from the base and
//!   relies on generated per-operator clones (`plan_clone_generator.go`).
//!   [`BasePlan::clone_for_plan_cache`] keeps the base contract, returning
//!   `None`.
//! * `PlanSize` / `MemoryUsage` return a source-shaped byte estimate over
//!   Rust's own layout; the absolute number is not Go's, only the shape
//!   (`base size + len(tp)`) is.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};

use tidb_datatype::FieldName;
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;

use crate::stats_info::StatsInfo;

/// Go `ctx.GetSessionVars().PlanID.Add(1)`, without the session.
///
/// `baseimpl.NewBasePlan` takes its id from a per-statement counter on the
/// session variables. This crate has no session, so the counter is passed in
/// explicitly. It is monotonic and starts at zero, so the first allocated id
/// is `1`, exactly as Go's `Add(1)` returns.
#[derive(Debug, Default)]
pub struct PlanIdAllocator {
    next: AtomicI32,
}

impl PlanIdAllocator {
    /// A fresh allocator whose first [`Self::alloc`] returns `1`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: AtomicI32::new(0),
        }
    }

    /// Go `PlanID.Add(1)`: the next plan id.
    pub fn alloc(&self) -> i32 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The last id handed out, without allocating one.
    pub fn current(&self) -> i32 {
        self.next.load(Ordering::Relaxed)
    }
}

/// Go `baseimpl.Plan`: the state every plan operator embeds.
///
/// Field-for-field with the source struct except for `ctx`, which is narrowed
/// out (see the module header), and for `schema`/`output_names`, which Go
/// keeps on `logicalop.LogicalSchemaProducer` and the operator structs rather
/// than on `baseimpl.Plan`. They live here because in Rust the enum variants
/// share exactly one base struct, and a schema-less operator simply leaves
/// them `None`/empty — which is also what Go's `baseimpl.Plan.OutputNames()`
/// returns.
#[derive(Clone, Debug, Default)]
pub struct BasePlan {
    /// Go `id`.
    id: i32,
    /// Go `tp`: the plan-codec operator name, e.g. `"Selection"`.
    tp: String,
    /// Go `qbBlock`: the query-block offset a hint is resolved against.
    query_block_offset: i32,
    /// Go `stats`, `plan-cache-clone:"shallow"`.
    stats: Option<StatsInfo>,
    /// Go `Plan.Schema()`; `None` when the operator derives it from a child.
    schema: Option<Schema>,
    /// Go `types.NameSlice`.
    output_names: Vec<FieldName>,
    /// Go `NoncacheableReason`.
    noncacheable_reason: String,
}

/// Go `PlanSize = unsafe.Sizeof(Plan{})`, over the Rust layout.
pub const PLAN_SIZE: i64 = std::mem::size_of::<BasePlan>() as i64;

impl BasePlan {
    /// Go `baseimpl.NewBasePlan(ctx, tp, qbBlock)`.
    #[must_use]
    pub fn new(
        allocator: &PlanIdAllocator,
        tp: impl Into<String>,
        query_block_offset: i32,
    ) -> Self {
        Self {
            id: allocator.alloc(),
            tp: tp.into(),
            query_block_offset,
            ..Self::default()
        }
    }

    /// Go `baseimpl.NewBasePlan` with a caller-chosen id.
    ///
    /// Tests and re-materialised plans need a fixed id; production callers
    /// should use [`Self::new`] so ids stay statement-unique.
    #[must_use]
    pub fn with_id(id: i32, tp: impl Into<String>, query_block_offset: i32) -> Self {
        Self {
            id,
            tp: tp.into(),
            query_block_offset,
            ..Self::default()
        }
    }

    /// Go `Plan.ID()`.
    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    /// Go `Plan.SetID(id)`.
    pub const fn set_id(&mut self, id: i32) {
        self.id = id;
    }

    /// Go `Plan.TP(...bool)`. The variadic argument selects a normalised name
    /// in a handful of operators and is unread by the base implementation.
    #[must_use]
    pub fn tp(&self) -> &str {
        &self.tp
    }

    /// Go `Plan.SetTP(tp)`.
    pub fn set_tp(&mut self, tp: impl Into<String>) {
        self.tp = tp.into();
    }

    /// Go `Plan.ExplainID()`.
    ///
    /// Go reads `StmtCtx.IgnoreExplainIDSuffix` off the session; that flag is
    /// the `ignore_suffix` argument here.
    #[must_use]
    pub fn explain_id(&self, ignore_suffix: bool) -> String {
        if ignore_suffix {
            self.tp.clone()
        } else {
            format!("{}_{}", self.tp, self.id)
        }
    }

    /// Go `baseimpl.Plan.ExplainInfo()`, whose base body is the literal
    /// `"N/A"`. Both `BaseLogicalPlan` and `BasePhysicalPlan` override it
    /// with `""`; those overrides live on their own base structs.
    #[must_use]
    pub fn explain_info() -> &'static str {
        "N/A"
    }

    /// Go `Plan.ReplaceExprColumns(replace)`. The base body is empty: no
    /// operator-owned expressions exist on `baseimpl.Plan`.
    pub const fn replace_expr_columns(&mut self, _replace: &BTreeMap<String, Column>) {}

    /// Go `Plan.StatsInfo()`.
    #[must_use]
    pub const fn stats_info(&self) -> Option<&StatsInfo> {
        self.stats.as_ref()
    }

    /// Go `Plan.SetStats(s)`.
    pub fn set_stats(&mut self, stats: Option<StatsInfo>) {
        self.stats = stats;
    }

    /// Go `Plan.Schema()`.
    #[must_use]
    pub const fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    /// Sets the operator's own schema.
    pub fn set_schema(&mut self, schema: Option<Schema>) {
        self.schema = schema;
    }

    /// Go `Plan.OutputNames()`.
    #[must_use]
    pub fn output_names(&self) -> &[FieldName] {
        &self.output_names
    }

    /// Go `Plan.SetOutputNames(names)`.
    pub fn set_output_names(&mut self, names: Vec<FieldName>) {
        self.output_names = names;
    }

    /// Go `Plan.QueryBlockOffset()`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Go `Plan.SetNoncacheableReason(reason)`: first writer wins.
    pub fn set_noncacheable_reason(&mut self, reason: impl Into<String>) {
        if self.noncacheable_reason.is_empty() {
            self.noncacheable_reason = reason.into();
        }
    }

    /// Go `Plan.GetNoncacheableReason()`.
    #[must_use]
    pub fn noncacheable_reason(&self) -> &str {
        &self.noncacheable_reason
    }

    /// Go `baseimpl.Plan.CloneForPlanCache`, whose base body returns
    /// `(nil, false)` so that an operator without a generated clone is never
    /// silently shallow-copied into the plan cache.
    #[must_use]
    pub const fn clone_for_plan_cache() -> Option<Self> {
        None
    }

    /// Go `Plan.MemoryUsage()`: `PlanSize + len(tp)`.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        PLAN_SIZE + self.tp.len() as i64
    }

    /// Go `Plan.ReAlloc4Cascades(tp)`: new type, new id, dropped stats; the
    /// context and query block stay.
    pub fn realloc_for_cascades(&mut self, allocator: &PlanIdAllocator, tp: impl Into<String>) {
        self.tp = tp.into();
        self.id = allocator.alloc();
        self.stats = None;
    }
}

/// The `error` half of every fallible plan-interface method.
///
/// Go raises these through `plannererrors.ErrInternal.GenWithStack` and
/// `errors.Errorf`; both are message-only at the interface boundary, and the
/// code mapping belongs to the `plannererrors` catalogue rather than to the
/// plan tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanError {
    message: String,
}

impl PlanError {
    /// Go `plannererrors.ErrInternal.GenWithStack(msg)`.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// The diagnostic text.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PlanError {}

/// Go `base.PossiblePropertiesInfo` (`plan_base.go:383`).
///
/// `Orders` holds `*expression.Column`; the identity Go compares with is
/// `Column.Equals`, so the columns are carried whole rather than reduced to
/// their `UniqueID` — unlike [`crate::physical_property::SortItem`], which
/// only ever needs the id.
#[derive(Clone, Debug, Default)]
pub struct PossiblePropertiesInfo {
    /// Go `Orders`: every order the subtree can produce.
    pub orders: Vec<Vec<Column>>,
    /// Go `HasTiFlash`: a runtime pruning signal, deliberately excluded from
    /// Go's `Hash64`/`Equals`.
    pub has_tiflash: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_id_allocator_matches_go_add_one() {
        let alloc = PlanIdAllocator::new();
        assert_eq!(alloc.current(), 0);
        assert_eq!(alloc.alloc(), 1);
        assert_eq!(alloc.alloc(), 2);
        assert_eq!(alloc.current(), 2);
    }

    #[test]
    fn explain_id_honours_the_ignore_suffix_flag() {
        let alloc = PlanIdAllocator::new();
        let base = BasePlan::new(&alloc, "Selection", 1);
        assert_eq!(base.explain_id(false), "Selection_1");
        assert_eq!(base.explain_id(true), "Selection");
        assert_eq!(base.query_block_offset(), 1);
    }

    #[test]
    fn noncacheable_reason_keeps_the_first_writer() {
        let mut base = BasePlan::with_id(7, "Projection", 0);
        assert_eq!(base.noncacheable_reason(), "");
        base.set_noncacheable_reason("uses now()");
        base.set_noncacheable_reason("second reason");
        assert_eq!(base.noncacheable_reason(), "uses now()");
    }

    #[test]
    fn realloc_for_cascades_renews_id_and_drops_stats() {
        let alloc = PlanIdAllocator::new();
        let mut base = BasePlan::new(&alloc, "Selection", 3);
        base.set_stats(Some(StatsInfo::new(10.0, [(1, 5.0)])));
        assert_eq!(base.id(), 1);

        base.realloc_for_cascades(&alloc, "Projection");
        assert_eq!(base.tp(), "Projection");
        assert_eq!(base.id(), 2);
        assert!(base.stats_info().is_none());
        // The query block is explicitly preserved by the source.
        assert_eq!(base.query_block_offset(), 3);
    }

    #[test]
    fn memory_usage_is_base_size_plus_type_name() {
        let base = BasePlan::with_id(1, "TableReader", 0);
        assert_eq!(base.memory_usage(), PLAN_SIZE + 11);
    }

    #[test]
    fn base_clone_for_plan_cache_refuses() {
        assert!(BasePlan::clone_for_plan_cache().is_none());
    }
}
