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
use std::sync::Arc;

use tidb_datatype::FieldName;
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;
use tidb_exprctx::EvalError;

use tidb_planner_property::stats_info::StatsInfo;

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
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        if std::env::var_os("TIDB_RS_ALLOC_TRACE").is_some() {
            let bt = std::backtrace::Backtrace::force_capture().to_string();
            let interesting: Vec<&str> = bt
                .lines()
                .filter(|line| {
                    (line.contains("tidb_planner") || line.contains("tidb_executor"))
                        && !line.contains("PlanIdAllocator")
                })
                .take(6)
                .collect();
            eprintln!(
                "ALLOC inst={:p} id={id}: {}",
                self as *const Self,
                interesting.join(" | ")
            );
        }
        id
    }

    /// The last id handed out, without allocating one.
    pub fn current(&self) -> i32 {
        self.next.load(Ordering::Relaxed)
    }
}

/// Go `SessionVars.AllocPlanColumnID()`: the source of a plan column's
/// `UniqueID`.
///
/// The plan-id counter already lives on [`PlanIdAllocator`]; column ids are a
/// separate Go counter with a wider type, so they get their own here.
#[derive(Debug, Default)]
pub struct ColumnIdAllocator {
    next: std::sync::atomic::AtomicI64,
}

impl ColumnIdAllocator {
    /// A fresh allocator whose first [`Self::alloc`] returns `1`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: std::sync::atomic::AtomicI64::new(0),
        }
    }

    /// Go `PlanColumnID.Add(1)`.
    pub fn alloc(&self) -> i64 {
        let id = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if std::env::var_os("TIDB_RS_COL_TRACE").is_some() {
            let bt = std::backtrace::Backtrace::force_capture().to_string();
            let interesting: Vec<&str> = bt
                .lines()
                .filter(|line| {
                    (line.contains("tidb_planner") || line.contains("tidb_executor"))
                        && !line.contains("ColumnIdAllocator")
                })
                .take(3)
                .collect();
            eprintln!(
                "COLALLOC inst={:p} id={id}: {}",
                self as *const Self,
                interesting.join(" | ")
            );
        }
        id
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
    ///
    /// Go holds a `*expression.Schema`, and a plan copy or a child-derived
    /// schema shares the pointer; the `Arc` is that sharing, so cloning a
    /// plan tree (join reorder, predicate push-down, the task memo) does not
    /// copy every column of every operator.
    schema: Option<Arc<Schema>>,
    /// Go `types.NameSlice`, shared like the slice header Go copies;
    /// `None` is the empty slice.
    output_names: Option<Arc<Vec<FieldName>>>,
    /// Go `NoncacheableReason`.
    noncacheable_reason: String,
}

/// Go `PlanSize = unsafe.Sizeof(Plan{})` (`baseimpl/plan.go:131`): 72 bytes on
/// 64-bit. The plan cache charges plans by Go's weight, not this port's own
/// layout, so the constant is Go's.
pub const PLAN_SIZE: i64 = 72;

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
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_deref()
    }

    /// The schema as the shared handle Go's `*expression.Schema` is, for a
    /// `SetSchema(child.Schema())` that shares rather than copies.
    #[must_use]
    pub const fn schema_shared(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    /// Sets the operator's own schema.
    pub fn set_schema(&mut self, schema: Option<Schema>) {
        self.schema = schema.map(Arc::new);
    }

    /// Sets the operator's schema to one already shared with another plan.
    pub fn set_schema_shared(&mut self, schema: Option<Arc<Schema>>) {
        self.schema = schema;
    }

    /// Go `Plan.OutputNames()`.
    #[must_use]
    pub fn output_names(&self) -> &[FieldName] {
        self.output_names.as_deref().map_or(&[], Vec::as_slice)
    }

    /// The output names as the shared slice Go's `NameSlice` copies share.
    #[must_use]
    pub const fn output_names_shared(&self) -> Option<&Arc<Vec<FieldName>>> {
        self.output_names.as_ref()
    }

    /// Go `Plan.SetOutputNames(names)`.
    pub fn set_output_names(&mut self, names: Vec<FieldName>) {
        self.output_names = (!names.is_empty()).then(|| Arc::new(names));
    }

    /// Sets the output names to a slice already shared with another plan.
    pub fn set_output_names_shared(&mut self, names: Option<Arc<Vec<FieldName>>>) {
        self.output_names = names.filter(|names| !names.is_empty());
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
/// Go raises most of these through `plannererrors.ErrInternal.GenWithStack`
/// and `errors.Errorf`; those remain message-only at the interface boundary.
/// Expression errors are the deliberate exception: their typed variant is
/// retained so the executor can map client-visible MySQL codes (for example
/// `charset.ErrCollationCharsetMismatch`) after planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanError {
    kind: PlanErrorKind,
    message: String,
}

/// What a planner failure carries across the plan boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanErrorKind {
    /// The error value Go's planner returns, raised as Go raises it.
    Mysql(tidb_terror::MysqlError),
    /// An expression error whose MySQL identity must survive the planner
    /// boundary (for example Go's 1253 collation/charset mismatch). Keeping
    /// the typed error avoids turning it into generic 1105 merely because
    /// plan construction crossed crates.
    Eval(EvalError),
    /// A message-only refusal: what this tier does not plan.
    Internal,
    /// An uncorrelated subquery evaluated during planning
    /// (`EvalSubqueryFirstRow`) scanned a memory table whose rows the session
    /// has yet to retrieve. Not a Go error: the executor tier asks the session
    /// for the rows and plans again.
    MemTableRetrievalNeeded,
}

impl PlanError {
    /// An expression error that must retain its typed client-facing identity.
    #[must_use]
    pub fn eval(error: EvalError) -> Self {
        Self {
            message: format!("{error:?}"),
            kind: PlanErrorKind::Eval(error),
        }
    }

    /// Go `plannererrors.ErrInternal.GenWithStack(msg)`.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: PlanErrorKind::Internal,
            message: message.into(),
        }
    }

    /// A subquery evaluated during planning needs memory-table rows the
    /// session has not retrieved; see [`PlanErrorKind::MemTableRetrievalNeeded`].
    #[must_use]
    pub fn mem_table_retrieval_needed() -> Self {
        Self {
            message: "a memory table was read without its rows being retrieved".to_owned(),
            kind: PlanErrorKind::MemTableRetrievalNeeded,
        }
    }

    /// Go `proto.Equal(err)`: whether this is the Go error `proto` raises.
    #[must_use]
    pub fn equal(&self, proto: &tidb_terror::TerrorError) -> bool {
        matches!(&self.kind, PlanErrorKind::Mysql(error) if error.equal(proto))
    }

    /// The stable error class used by the statement layer.
    #[must_use]
    pub const fn kind(&self) -> &PlanErrorKind {
        &self.kind
    }

    /// The diagnostic text.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<tidb_terror::MysqlError> for PlanError {
    fn from(error: tidb_terror::MysqlError) -> Self {
        Self {
            message: error.message.clone(),
            kind: PlanErrorKind::Mysql(error),
        }
    }
}

impl From<tidb_terror::TerrorError> for PlanError {
    fn from(error: tidb_terror::TerrorError) -> Self {
        tidb_terror::MysqlError::from_terror(&error).into()
    }
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PlanError {}

/// Go `base.JoinType` (`plan_base.go`): the join kinds a logical join and
/// its physical alternatives carry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalJoinType {
    /// `InnerJoin`.
    Inner,
    /// `LeftOuterJoin`.
    LeftOuter,
    /// `RightOuterJoin`.
    RightOuter,
    /// `SemiJoin`.
    Semi,
    /// `AntiSemiJoin`.
    AntiSemi,
    /// `LeftOuterSemiJoin`.
    LeftOuterSemi,
    /// `AntiLeftOuterSemiJoin`.
    AntiLeftOuterSemi,
}

/// Go `base.PossiblePropertiesInfo` (`plan_base.go:383`).
///
/// `Orders` holds `*expression.Column`; the identity Go compares with is
/// `Column.Equals`, so the columns are carried whole rather than reduced to
/// their `UniqueID` — unlike [`tidb_planner_property::SortItem`], which
/// only ever needs the id.
#[derive(Clone, Debug, Default)]
pub struct PossiblePropertiesInfo {
    /// Go `Orders`: every order the subtree can produce.
    pub orders: Vec<Vec<Column>>,
    /// Go `HasTiFlash`: a runtime pruning signal, deliberately excluded from
    /// Go's `Hash64`/`Equals`.
    pub has_tiflash: bool,
}

/// Hash the identity portion of Go `PossiblePropertiesInfo`.
///
/// `HasTiFlash` is deliberately runtime-only and is excluded from Go's
/// generated `Hash64`/`Equals`; only the nested order lists participate. Rust
/// cannot represent Go's nil-vs-empty slice distinction, so the owned vector
/// is encoded as a present list and each column uses the same identity triple
/// used by the planner schema hash.
pub fn hash_possible_properties(
    hasher: &mut impl tidb_planner_cascades_base::Hasher,
    properties: &PossiblePropertiesInfo,
) {
    use tidb_planner_cascades_base::NOT_NIL_FLAG;

    hasher.hash_byte(NOT_NIL_FLAG);
    hasher.hash_int(properties.orders.len() as i64);
    for order in &properties.orders {
        hasher.hash_int(order.len() as i64);
        for column in order {
            hasher.hash_int64(column.id);
            hasher.hash_int64(column.unique_id);
            hasher.hash_int64(column.index);
        }
    }
}

/// Go `PossiblePropertiesInfo.Equals` over the representable Rust shape.
/// `HasTiFlash` is intentionally ignored because it is not plan identity.
#[must_use]
pub fn possible_properties_equal(
    left: &PossiblePropertiesInfo,
    right: &PossiblePropertiesInfo,
) -> bool {
    left.orders.len() == right.orders.len()
        && left.orders.iter().zip(&right.orders).all(|(left, right)| {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| left.equals(right))
        })
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

    /// A plan copy shares its schema and names the way Go copies the
    /// `*Schema` pointer and the `NameSlice` header, and a setter replaces
    /// only this plan's handle.
    #[test]
    fn a_plan_copy_shares_its_schema_and_names() {
        let allocator = PlanIdAllocator::default();
        let mut plan = BasePlan::new(&allocator, "Projection", 0);
        assert!(plan.output_names().is_empty());
        assert!(plan.output_names_shared().is_none());
        plan.set_schema(Some(Schema::new(vec![Column::default()])));
        plan.set_output_names(vec![FieldName::default()]);

        let copy = plan.clone();
        let same_schema = match (plan.schema_shared(), copy.schema_shared()) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        };
        assert!(same_schema, "the copy shares the schema allocation");
        let same_names = match (plan.output_names_shared(), copy.output_names_shared()) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        };
        assert!(same_names, "the copy shares the names allocation");

        plan.set_output_names(Vec::new());
        assert!(plan.output_names().is_empty());
        assert_eq!(copy.output_names().len(), 1, "the copy keeps its names");
        let mut derived = BasePlan::new(&allocator, "Selection", 0);
        derived.set_schema_shared(copy.schema_shared().cloned());
        derived.set_output_names_shared(copy.output_names_shared().cloned());
        assert_eq!(derived.schema().map(|schema| schema.columns.len()), Some(1));
        assert_eq!(derived.output_names().len(), 1);
    }
}

/// An unknown column is Go's typed `plannererrors.ErrUnknownColumn`, not a
/// generic evaluation failure, so it keeps the clause the resolver named.
/// Without this arm the clause survives only inside the debug-formatted
/// message and the executor lifts the error as `Exec(Eval(..))`, losing the
/// typed variant the plan-time resolution path produces.
impl From<EvalError> for PlanError {
    fn from(error: EvalError) -> Self {
        match error {
            EvalError::UnknownColumnInClause(column, clause) => Self::from(
                tidb_plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                    tidb_hack::go_to_lower(column).into(),
                    clause.into(),
                ]),
            ),
            other => Self::eval(other),
        }
    }
}

/// Go `base.MemTablePredicateExtractor`: the extractor a memory table owns,
/// which `LogicalMemTable.PredicatePushDown` hands its predicates to, with
/// Go's optional `MemTableRowLimitHintSetter` / `MemTableDescHintSetter`
/// (`LogicalMemTable.PushDownTopN`); the planner reads it back by its concrete
/// type when it builds the physical scan.
pub trait MemTablePredicateExtractor: std::fmt::Debug + Send + Sync {
    /// Go `Extract(ctx, schema, names, predicates)`: the predicates this
    /// extractor does not claim.
    fn extract(
        &mut self,
        ctx: &dyn tidb_exprctx::Columns,
        schema: &tidb_expr::schema::Schema,
        names: &[tidb_datatype::FieldName],
        predicates: Vec<tidb_expr::expression::Expression>,
    ) -> Vec<tidb_expr::expression::Expression>;

    /// Go `MemTableRowLimitHintSetter.SetRowLimitHint`, for an extractor that
    /// implements it.
    fn set_row_limit_hint(&mut self, _limit: u64) {}

    /// Go `MemTableDescHintSetter.SetDesc`, for an extractor that implements it.
    fn set_desc(&mut self, _desc: bool) {}

    /// A boxed copy, for the plan's clone.
    fn clone_box(&self) -> Box<dyn MemTablePredicateExtractor>;

    /// The concrete extractor, for the planner's type assertion.
    fn as_any(&self) -> &dyn std::any::Any;
}

impl Clone for Box<dyn MemTablePredicateExtractor> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Go `base.ShowPredicateExtractor`: the extractor `buildShow`
/// (`ShowBaseExtractor`) or `LogicalShow.PredicatePushDown`
/// (`ShowStatsMetaPredicateExtractor`) installs on a SHOW, which the executor
/// reads back by its concrete type (Go's type switch).
pub trait ShowPredicateExtractor: std::fmt::Debug + Send + Sync {
    /// A boxed copy, for the plan's clone.
    fn clone_box(&self) -> Box<dyn ShowPredicateExtractor>;

    /// The concrete extractor, for the reader's type switch.
    fn as_any(&self) -> &dyn std::any::Any;
}

impl Clone for Box<dyn ShowPredicateExtractor> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Go `base.PhysicalPlan` as a logical operator holds one: a CTE class keeps
/// its seed and recursive parts' physical plans, whose statistics
/// `LogicalCTE.DeriveStats` reads; the planner that built them reads them back
/// as its own plan type (Go's type assertion).
pub trait PhysicalPlanHandle: std::fmt::Debug {
    /// Go `StatsInfo()`.
    fn stats_info(&self) -> Option<&StatsInfo>;

    /// The concrete plan, for the planner's own type assertion.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Go `PlanContext.SetSkipPlanCache`: the session side effect the expression
/// and rule simplifiers and the estimator perform.
pub trait PlanCacheMarker {
    /// Records the first reason the current statement cannot use plan cache.
    fn set_skip_plan_cache(&self, reason: &str);
}

impl PlanCacheMarker for tidb_sessionctx::StmtContext {
    fn set_skip_plan_cache(&self, reason: &str) {
        tidb_sessionctx::StmtContext::set_skip_plan_cache(self, reason);
    }
}
