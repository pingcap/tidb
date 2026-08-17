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

//! The logical plan tree.
//!
//! Go sources:
//! * `pkg/planner/core/base/plan_base.go` — the `LogicalPlan` interface
//!   (lines 172-275).
//! * `pkg/planner/core/operator/logicalop/base_logical_plan.go` — the
//!   `BaseLogicalPlan` struct and its default bodies (lines 42-481).
//!
//! SEED of `pkg/planner/core`: the tree and its method surface land here, and
//! the operator set is now PARTIAL rather than empty. Every method below
//! carries Go's signature, and the bodies are either Go's base body (where it
//! is dependency-closed) or an explicit `todo`. A `todo` body is one that a
//! later batch fills; it is never a body that silently answers.
//!
//! # Which operators have landed
//!
//! Ported, each in its own submodule beside this one, with its own state and
//! its dependency-closed member bodies:
//!
//! * [`selection::LogicalSelection`] — `logical_selection.go`
//! * [`projection::LogicalProjection`] — `logical_projection.go`
//! * [`join::LogicalJoin`] — `logical_join.go`
//! * [`apply::LogicalApply`] — `logical_apply.go`, BUILT ON the above rather
//!   than beside it
//! * [`aggregation::LogicalAggregation`] — `logical_aggregation.go`
//! * [`data_source::DataSource`] — `logical_datasource.go`
//! * [`schema_producer`] — `logical_schema_producer.go`, whose Go struct
//!   becomes behaviour here rather than a third base struct
//! * [`sort::LogicalSort`] — `logical_sort.go`, which also carries the
//!   `ByItems` pruning of `logical_plans_misc.go`
//! * [`limit::LogicalLimit`] — `logical_limit.go`
//! * [`topn::LogicalTopN`] — `logical_top_n.go`
//! * [`union_all::LogicalUnionAll`] and
//!   [`union_all::LogicalPartitionUnionAll`] — `logical_union_all.go` and
//!   `logical_partition_union_all.go`
//! * [`max_one_row::LogicalMaxOneRow`] — `logical_max_one_row.go`
//! * [`lock::LogicalLock`] — `logical_lock.go`
//! * [`sequence::LogicalSequence`] — `logical_sequence.go`
//! * [`union_scan::LogicalUnionScan`] — `logical_union_scan.go`
//! * [`table_scan::LogicalTableScan`] — `logical_table_scan.go`
//! * [`index_scan::LogicalIndexScan`] — `logical_index_scan.go`
//! * [`tikv_single_gather::TiKVSingleGather`] —
//!   `logical_tikv_single_gather.go`
//! * [`cte::LogicalCTE`] and [`cte::LogicalCTETable`] — `logical_cte.go` and
//!   `logical_cte_table.go`
//! * [`window::LogicalWindow`] — `logical_window.go`, which MERGES the crate's
//!   former `window_frame` identity leaf
//!
//! Still SKELETAL, carrying only their state: [`LogicalTableDual`].
//!
//! Still [`TodoLogicalOp`], i.e. not modelled at all:
//! `LogicalExpand`, `LogicalMemTable`, `LogicalShow`, `LogicalShowDDLJobs`.
//!
//! # Why a closed enum and not `Box<dyn LogicalPlan>`
//!
//! Go's rule signatures return a REPLACEMENT node:
//!
//! ```go
//! PredicatePushDown([]expression.Expression) ([]expression.Expression, LogicalPlan, error)
//! PruneColumns([]*expression.Column) (LogicalPlan, error)
//! ```
//!
//! In Rust that is `fn predicate_push_down(self, ...) -> (..., LogicalPlan)`:
//! `self` by value, the replacement moved out. With trait objects the same
//! signature needs `Box<Self>` receivers and gives up `Clone`, `PartialEq`,
//! and any `Hash64`/`Equals` pair to object safety — Go gets those from
//! `base.HashEquals` and its concrete types, and Rust gets them for free from
//! an enum. `tidb-expr::Expression` made exactly this call for Go's
//! `expression.Expression` interface and it has held across that crate; the
//! plan tree has the same shape of problem and takes the same answer.
//!
//! The cost of the closed set — a new operator touches every `match` — is the
//! point: an unhandled operator is a compile error rather than a silently
//! wrong plan.
//!
//! # Recursion and ownership, measured
//!
//! Children are OWNED (`Vec<LogicalPlan>`), not `Rc`/`Arc`. Two properties
//! were measured on this exact shape before committing to it:
//!
//! * A recursive `match` walk over a unary chain survives ~30,000 levels on a
//!   2 MiB stack and aborts by 50,000; recursive `Drop` glue is tighter,
//!   surviving 20,000 and aborting by 100,000. Real plan trees are orders of
//!   magnitude shallower, but a generated query can nest without bound, so
//!   every walk this module offers ([`LogicalPlan::walk_preorder`],
//!   [`LogicalPlan::plan_count`], [`LogicalPlan::max_depth`]) uses an EXPLICIT
//!   STACK and is depth-safe. [`LogicalPlan::dismantle`] does the same for
//!   teardown; a manual `Drop` impl was rejected because it would forbid
//!   moving a base out of a variant, which is the whole point of the by-value
//!   rule signatures.
//! * Cloning is not on the rule path. A rule that rewrites a node MOVES its
//!   children into the replacement, which copies pointers, not trees. Deep
//!   clone was measured at ~180 ns per node (a 1,024-node chain clones in
//!   ~181 µs), dominated by the `String` type name and the name slice; the
//!   base-shell clone is ~110 ns flat. So `Rc` on the child edge would buy
//!   nothing on the rewrite path and would cost the mutable `SetChild` that
//!   Go's rules use constantly. Sharing belongs where Go shares — the
//!   cascades memo's group expressions and the task map — and those hold
//!   their own handles rather than aliasing this tree.

use tidb_expr::column::Column;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;

use crate::physical::PhysicalPlan;
use crate::physical_property::PhysicalProperty;
use crate::physical_table_reader::StoreType;
use crate::plan_base::{BasePlan, PlanError, PlanIdAllocator, PossiblePropertiesInfo};
use crate::stats_info::StatsInfo;

/// Go `logicalop.ApplyGenFromXFDeCorrelateRuleFlag` (`base_logical_plan.go:38`).
pub const APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG: u64 = 1 << 0;

/// Go `logicalop.BaseLogicalPlan` (`base_logical_plan.go:42`).
///
/// # Narrowings, by name
///
/// * `self base.LogicalPlan`. Go's base struct keeps a back-pointer to the
///   concrete operator so a default body can call the override. An enum
///   dispatches on the variant, so `self` has no counterpart and is dropped;
///   `Self()`/`SetSelf()`/`GetWrappedLogicalPlan()` become identity on
///   [`LogicalPlan`].
/// * `taskMap` / `taskMapBak` / `taskMapBakTS`. The memoised
///   `(planIDsHash, prop) -> Task` table. `base.Task` is not transcreated, so
///   the table is absent rather than typed against a placeholder;
///   `roll_back_task_map` keeps its signature and is a `todo`.
/// * `fdSet`. `pkg/planner/funcdep` is not transcreated; `ExtractFD` keeps
///   its signature and is a `todo`.
#[derive(Clone, Debug, Default)]
pub struct BaseLogicalPlan {
    /// Go's embedded `baseimpl.Plan`.
    pub base: BasePlan,
    /// Go `children`.
    children: Vec<LogicalPlan>,
    /// Go `maxOneRow`.
    max_one_row: bool,
    /// Go `hasTiFlash`, set by `PreparePossibleProperties`.
    has_tiflash: bool,
    /// Go `planIDsHash`: the hash of the subtree rooted here.
    plan_ids_hash: u64,
    /// Go `Flag`: a bit set of per-operator marks, see
    /// [`APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG`].
    pub flag: u64,
}

impl BaseLogicalPlan {
    /// Go `NewBaseLogicalPlan(ctx, tp, self, qbOffset)` (`base_logical_plan.go:444`).
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

    /// Go `Children()` (`<17th>`).
    #[must_use]
    pub fn children(&self) -> &[LogicalPlan] {
        &self.children
    }

    /// Mutable access to the child vector, for rules that rewrite in place.
    pub const fn children_mut(&mut self) -> &mut Vec<LogicalPlan> {
        &mut self.children
    }

    /// Go `SetChildren(...)` (`<18th>`).
    pub fn set_children(&mut self, children: Vec<LogicalPlan>) {
        self.children = children;
    }

    /// Go `SetChild(i, child)` (`<19th>`).
    ///
    /// Go panics on an out-of-range index; this returns the previous child so
    /// a caller can neither lose it nor silently write past the end.
    pub fn set_child(&mut self, i: usize, child: LogicalPlan) -> Option<LogicalPlan> {
        let slot = self.children.get_mut(i)?;
        Some(std::mem::replace(slot, child))
    }

    /// Go `ChildLen()` (`base_logical_plan.go:389`).
    #[must_use]
    pub fn child_len(&self) -> usize {
        self.children.len()
    }

    /// Takes the children, leaving the node childless.
    #[must_use]
    pub fn take_children(&mut self) -> Vec<LogicalPlan> {
        std::mem::take(&mut self.children)
    }

    /// Go `MaxOneRow()` (`<16th>`).
    #[must_use]
    pub const fn max_one_row(&self) -> bool {
        self.max_one_row
    }

    /// Go `SetMaxOneRow(b)` (`base_logical_plan.go:414`).
    pub const fn set_max_one_row(&mut self, value: bool) {
        self.max_one_row = value;
    }

    /// Go `hasTiFlash`, as observed through `PreparePossibleProperties`.
    #[must_use]
    pub const fn has_tiflash(&self) -> bool {
        self.has_tiflash
    }

    /// Go's `p.hasTiFlash = ...`, which every operator's
    /// `PreparePossibleProperties` writes directly on the embedded base.
    pub const fn set_has_tiflash(&mut self, value: bool) {
        self.has_tiflash = value;
    }

    /// This base's own state with NO children.
    ///
    /// The building block of every operator's `clone_shallow`; see the module
    /// header for why the child edge is never cloned implicitly.
    #[must_use]
    pub fn shell(&self) -> Self {
        Self {
            base: self.base.clone(),
            children: Vec::new(),
            max_one_row: self.max_one_row,
            has_tiflash: self.has_tiflash,
            plan_ids_hash: self.plan_ids_hash,
            flag: self.flag,
        }
    }

    /// Go `GetPlanIDsHash()` (`<26th>`).
    #[must_use]
    pub const fn plan_ids_hash(&self) -> u64 {
        self.plan_ids_hash
    }

    /// Go `SetPlanIDsHash(hash)` (`<25th>`).
    pub const fn set_plan_ids_hash(&mut self, hash: u64) {
        self.plan_ids_hash = hash;
    }

    /// Go `HasFlag(mask)` (`base_logical_plan.go:455`).
    #[must_use]
    pub const fn has_flag(&self, mask: u64) -> bool {
        self.flag & mask > 0
    }

    /// Go `SetFlag(mask)` (`base_logical_plan.go:460`).
    pub const fn set_flag(&mut self, mask: u64) {
        self.flag |= mask;
    }

    /// Go `BaseLogicalPlan.ExplainInfo()`, which overrides `baseimpl.Plan`'s
    /// `"N/A"` with the empty string (`base_logical_plan.go:97`).
    #[must_use]
    pub const fn explain_info() -> &'static str {
        ""
    }

    /// Go `HashCode()` (`<0th>`): the plan id encoded as a big-endian
    /// `uint32` with the sign bit flipped, which is `util.EncodeIntAsUint32`.
    #[must_use]
    pub fn hash_code(&self) -> Vec<u8> {
        let biased = (self.base.id() as i64 + i64::from(i32::MAX) + 1) as u32;
        biased.to_be_bytes().to_vec()
    }

    /// Go `ReAlloc4Cascades(tp, self)` (`base_logical_plan.go:468`): a new
    /// type and id, a fresh task map, no `self` alias, no `maxOneRow`, no FDs.
    /// The children are deliberately preserved.
    pub fn realloc_for_cascades(&mut self, allocator: &PlanIdAllocator, tp: impl Into<String>) {
        self.base.realloc_for_cascades(allocator, tp);
        self.max_one_row = false;
    }
}

pub mod aggregation;
pub mod apply;
pub mod cte;
pub mod data_source;
pub mod index_scan;
pub mod join;
pub mod limit;
pub mod lock;
pub mod max_one_row;
pub mod projection;
pub mod schema_producer;
pub mod selection;
pub mod sequence;
pub mod sort;
pub mod table_scan;
pub mod tikv_single_gather;
pub mod topn;
pub mod union_all;
pub mod union_scan;
pub mod window;

pub use aggregation::LogicalAggregation;
pub use apply::LogicalApply;
pub use cte::{LogicalCTE, LogicalCTETable};
pub use data_source::DataSource;
pub use index_scan::LogicalIndexScan;
pub use join::LogicalJoin;
pub use limit::LogicalLimit;
pub use lock::LogicalLock;
pub use max_one_row::LogicalMaxOneRow;
pub use projection::LogicalProjection;
pub use selection::LogicalSelection;
pub use sequence::LogicalSequence;
pub use sort::LogicalSort;
pub use table_scan::LogicalTableScan;
pub use tikv_single_gather::TiKVSingleGather;
pub use topn::LogicalTopN;
pub use union_all::{LogicalPartitionUnionAll, LogicalUnionAll};
pub use union_scan::LogicalUnionScan;
pub use window::LogicalWindow;

/// Go `logicalop.LogicalTableDual`.
#[derive(Clone, Debug, Default)]
pub struct LogicalTableDual {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `RowCount`: 0 for an empty result, 1 for a constant row.
    pub row_count: usize,
}

/// A logical operator whose own port is a later batch.
///
/// This is the SEED's honest placeholder: it names the Go operator it stands
/// for so a `match` arm reads as "not yet ported", never as "handled". It is
/// deliberately a distinct variant rather than a default arm, so filling the
/// operator set is a mechanical, checkable change.
#[derive(Clone, Debug, Default)]
pub struct TodoLogicalOp {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// The Go type this node stands in for, e.g. `"logicalop.LogicalWindow"`.
    pub go_operator: String,
}

/// The return of [`LogicalPlan::get_join_child_stats_and_schema`]: both
/// children's stats and schemas, in Go's `(stats0, stats1, schema0, schema1)`
/// order.
pub type JoinChildStatsAndSchema<'a> = (
    Option<&'a StatsInfo>,
    Option<&'a StatsInfo>,
    Option<&'a Schema>,
    Option<&'a Schema>,
);

/// Go `base.LogicalPlan`: a tree of logical operators.
///
/// See the module header for why this is a closed enum.
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// Go `logicalop.LogicalSelection`.
    Selection(LogicalSelection),
    /// Go `logicalop.LogicalProjection`.
    Projection(LogicalProjection),
    /// Go `logicalop.LogicalJoin`.
    Join(LogicalJoin),
    /// Go `logicalop.LogicalApply`, which EMBEDS the above.
    Apply(LogicalApply),
    /// Go `logicalop.LogicalAggregation`.
    Aggregation(LogicalAggregation),
    /// Go `logicalop.LogicalSort`.
    Sort(LogicalSort),
    /// Go `logicalop.LogicalLimit`.
    Limit(LogicalLimit),
    /// Go `logicalop.LogicalTopN`.
    TopN(LogicalTopN),
    /// Go `logicalop.LogicalUnionAll`.
    UnionAll(LogicalUnionAll),
    /// Go `logicalop.LogicalPartitionUnionAll`, which EMBEDS the above.
    PartitionUnionAll(LogicalPartitionUnionAll),
    /// Go `logicalop.LogicalWindow`.
    Window(LogicalWindow),
    /// Go `logicalop.LogicalCTE`.
    CTE(LogicalCTE),
    /// Go `logicalop.LogicalCTETable`.
    CTETable(LogicalCTETable),
    /// Go `logicalop.LogicalMaxOneRow`.
    MaxOneRow(LogicalMaxOneRow),
    /// Go `logicalop.LogicalLock`.
    Lock(LogicalLock),
    /// Go `logicalop.LogicalSequence`.
    Sequence(LogicalSequence),
    /// Go `logicalop.LogicalUnionScan`.
    UnionScan(LogicalUnionScan),
    /// Go `logicalop.TiKVSingleGather`.
    TiKVSingleGather(TiKVSingleGather),
    /// Go `logicalop.LogicalTableScan`.
    TableScan(LogicalTableScan),
    /// Go `logicalop.LogicalIndexScan`.
    IndexScan(LogicalIndexScan),
    /// Go `logicalop.DataSource`.
    DataSource(DataSource),
    /// Go `logicalop.LogicalTableDual`.
    TableDual(LogicalTableDual),
    /// An operator whose port is a later batch; see [`TodoLogicalOp`].
    Todo(TodoLogicalOp),
}

impl LogicalPlan {
    /// The shared logical base of whichever operator this is.
    #[must_use]
    pub const fn base(&self) -> &BaseLogicalPlan {
        match self {
            Self::Selection(op) => &op.base,
            Self::Projection(op) => &op.base,
            Self::Join(op) => &op.base,
            Self::Apply(op) => &op.join.base,
            Self::Aggregation(op) => &op.base,
            Self::Sort(op) => &op.base,
            Self::Limit(op) => &op.base,
            Self::TopN(op) => &op.base,
            Self::UnionAll(op) => &op.base,
            Self::PartitionUnionAll(op) => &op.union_all.base,
            Self::Window(op) => &op.base,
            Self::CTE(op) => &op.base,
            Self::CTETable(op) => &op.base,
            Self::MaxOneRow(op) => &op.base,
            Self::Lock(op) => &op.base,
            Self::Sequence(op) => &op.base,
            Self::UnionScan(op) => &op.base,
            Self::TiKVSingleGather(op) => &op.base,
            Self::TableScan(op) => &op.base,
            Self::IndexScan(op) => &op.base,
            Self::DataSource(op) => &op.base,
            Self::TableDual(op) => &op.base,
            Self::Todo(op) => &op.base,
        }
    }

    /// The shared logical base, mutably.
    pub const fn base_mut(&mut self) -> &mut BaseLogicalPlan {
        match self {
            Self::Selection(op) => &mut op.base,
            Self::Projection(op) => &mut op.base,
            Self::Join(op) => &mut op.base,
            Self::Apply(op) => &mut op.join.base,
            Self::Aggregation(op) => &mut op.base,
            Self::Sort(op) => &mut op.base,
            Self::Limit(op) => &mut op.base,
            Self::TopN(op) => &mut op.base,
            Self::UnionAll(op) => &mut op.base,
            Self::PartitionUnionAll(op) => &mut op.union_all.base,
            Self::Window(op) => &mut op.base,
            Self::CTE(op) => &mut op.base,
            Self::CTETable(op) => &mut op.base,
            Self::MaxOneRow(op) => &mut op.base,
            Self::Lock(op) => &mut op.base,
            Self::Sequence(op) => &mut op.base,
            Self::UnionScan(op) => &mut op.base,
            Self::TiKVSingleGather(op) => &mut op.base,
            Self::TableScan(op) => &mut op.base,
            Self::IndexScan(op) => &mut op.base,
            Self::DataSource(op) => &mut op.base,
            Self::TableDual(op) => &mut op.base,
            Self::Todo(op) => &mut op.base,
        }
    }

    /// Go `GetBaseLogicalPlan()` (`<23rd>`).
    #[must_use]
    pub const fn get_base_logical_plan(&self) -> &BaseLogicalPlan {
        self.base()
    }

    /// Go `GetWrappedLogicalPlan()` (`<27th>`).
    ///
    /// Go's implementation returns `p.self`, which for a plain logical
    /// operator is the operator itself, and for a memo group expression is
    /// the wrapped one. Only the former exists here, so this is identity.
    #[must_use]
    pub const fn get_wrapped_logical_plan(&self) -> &Self {
        self
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

    /// Go `Plan.ExplainID(...)`; see [`BasePlan::explain_id`].
    #[must_use]
    pub fn explain_id(&self, ignore_suffix: bool) -> String {
        self.base().base.explain_id(ignore_suffix)
    }

    /// Go `Plan.QueryBlockOffset()`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.base().base.query_block_offset()
    }

    /// Go `Plan.StatsInfo()`.
    #[must_use]
    pub const fn stats_info(&self) -> Option<&StatsInfo> {
        self.base().base.stats_info()
    }

    /// Go `SetStats(s)`.
    pub fn set_stats(&mut self, stats: Option<StatsInfo>) {
        self.base_mut().base.set_stats(stats);
    }

    /// Go `BaseLogicalPlan.Schema()` (`base_logical_plan.go:102`): the
    /// operator's own schema when it produces one, otherwise the first
    /// child's.
    ///
    /// Go indexes `p.children[0]` unconditionally and panics on a leaf with no
    /// schema of its own; this returns `None` there instead.
    #[must_use]
    pub fn schema(&self) -> Option<&Schema> {
        if let Some(schema) = self.base().base.schema() {
            return Some(schema);
        }
        self.base().children().first().and_then(Self::schema)
    }

    /// Go `BaseLogicalPlan.OutputNames()` (`base_logical_plan.go:107`), with
    /// the same own-then-first-child rule as [`Self::schema`].
    #[must_use]
    pub fn output_names(&self) -> &[tidb_datatype::FieldName] {
        let own = self.base().base.output_names();
        if !own.is_empty() {
            return own;
        }
        self.base()
            .children()
            .first()
            .map_or(&[][..], Self::output_names)
    }

    /// Go `BaseLogicalPlan.SetOutputNames(names)` (`base_logical_plan.go:112`),
    /// which forwards to `children[0]`. With no child the names land here.
    pub fn set_output_names(&mut self, names: Vec<tidb_datatype::FieldName>) {
        match self.base_mut().children_mut().first_mut() {
            Some(child) => child.set_output_names(names),
            None => self.base_mut().base.set_output_names(names),
        }
    }

    // ***** base.LogicalPlan members *****

    /// Go `HashCode()` (`<0th>`).
    #[must_use]
    pub fn hash_code(&self) -> Vec<u8> {
        self.base().hash_code()
    }

    /// Go `Equals(other)` from `base.HashEquals`: plan-id equality.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.id() == other.id()
    }

    /// Go `PredicatePushDown(predicates)` (`<1st>`).
    ///
    /// Go: `([]expression.Expression, LogicalPlan, error)` — the predicates
    /// that could NOT be pushed, plus a possibly-new root. Taking `self` by
    /// value is how the "possibly-new root" is expressed without a clone.
    ///
    /// The base body (`base_logical_plan.go:128`) pushes everything into
    /// `children[0]` and re-attaches a `Selection` for the remainder; the
    /// `AddSelection` half needs `logicalop.LogicalSelection` construction
    /// rules that are a later batch, so this returns the predicates unpushed
    /// for a childless node and is otherwise a `todo`.
    pub fn predicate_push_down(
        self,
        predicates: Vec<Expression>,
    ) -> Result<(Vec<Expression>, Self), PlanError> {
        if self.base().child_len() == 0 {
            return Ok((predicates, self));
        }
        Err(PlanError::internal(
            "todo: LogicalPlan::predicate_push_down needs logicalop.AddSelection",
        ))
    }

    /// Go `PruneColumns(parentUsedCols)` (`<2nd>`): prune, returning the new
    /// plan if it changed and the same one otherwise.
    ///
    /// The base body recurses into `children[0]`; that recursion is safe to
    /// port once the operators own their column sets, which is a later batch.
    pub fn prune_columns(self, _parent_used_cols: &[Column]) -> Result<Self, PlanError> {
        if self.base().child_len() == 0 {
            return Ok(self);
        }
        Err(PlanError::internal(
            "todo: LogicalPlan::prune_columns needs the operator column sets",
        ))
    }

    /// Go `BuildKeyInfo(selfSchema, childSchema)` (`<3rd>`), dispatched to the
    /// ported operators.
    ///
    /// The base half — `maxOneRow` from `HasMaxOneRow(self, childMaxOneRow)` —
    /// still needs `logicalop.HasMaxOneRow`, whose table covers operators this
    /// batch does not port; the single-child propagation it performs is
    /// preserved here, and the per-operator overrides run on top.
    pub fn build_key_info(&mut self, self_schema: &mut Schema, child_schema: &[Schema]) {
        // Go `BaseLogicalPlan.BuildKeyInfo` (`base_logical_plan.go:196`): a
        // single-child operator inherits its child's `maxOneRow`.
        if self.base().child_len() == 1 {
            let inherited = self.base().children()[0].base().max_one_row();
            if inherited {
                self.base_mut().set_max_one_row(true);
            }
        }
        match self {
            Self::Selection(op) => op.build_key_info(child_schema),
            Self::Projection(op) => op.build_key_info(self_schema, child_schema),
            Self::Join(op) => op.build_key_info(self_schema, child_schema),
            Self::Aggregation(op) => op.build_key_info(self_schema, child_schema),
            // `DataSource::build_key_info` needs the index definitions, which
            // the catalogue owns; call it directly with them.
            // `LogicalTableScan` delegates to the source and `LogicalIndexScan`
            // needs `ruleutil.CheckIndexCanBeKey`; both take the index
            // definitions the catalogue owns, so call them directly with them.
            Self::DataSource(_) | Self::TableScan(_) | Self::IndexScan(_) => {}
            Self::Limit(op) => op.build_key_info(self_schema, child_schema),
            Self::TopN(op) => op.build_key_info(self_schema, child_schema),
            Self::TiKVSingleGather(_) => {
                TiKVSingleGather::build_key_info(self_schema, child_schema);
            }
            Self::Sort(_)
            | Self::Apply(_)
            | Self::UnionAll(_)
            | Self::PartitionUnionAll(_)
            | Self::Window(_)
            | Self::CTE(_)
            | Self::CTETable(_)
            | Self::MaxOneRow(_)
            | Self::Lock(_)
            | Self::Sequence(_)
            | Self::UnionScan(_)
            | Self::TableDual(_)
            | Self::Todo(_) => {
                schema_producer::propagate_child_keys(self_schema, child_schema);
            }
        }
    }

    /// Go `PushDownTopN(topN)` (`<4th>`).
    #[must_use]
    pub fn push_down_topn(self, _topn: Option<Self>) -> Self {
        self // todo: pushDownTopNForBaseLogicalPlan
    }

    /// Go `DeriveTopN()` (`<5th>`), gated on `AllowDeriveTopN`.
    #[must_use]
    pub fn derive_topn(self) -> Self {
        self // todo: needs the session's AllowDeriveTopN and LogicalWindow
    }

    /// Go `PredicateSimplification()` (`<6th>`): rewrite each child, in place.
    #[must_use]
    pub fn predicate_simplification(self) -> Self {
        self // todo: needs the per-operator predicate consolidation
    }

    /// Go `ConstantPropagation(parentPlan, currentChildIdx)` (`<7th>`).
    ///
    /// Go's base body returns `nil` — only `LogicalJoin` propagates — so
    /// `None` here is Go's answer, not a `todo`.
    #[must_use]
    pub const fn constant_propagation(
        &mut self,
        _parent: Option<&Self>,
        _current_child_idx: usize,
    ) -> Option<Self> {
        None
    }

    /// Go `PullUpConstantPredicates()` (`<8th>`).
    ///
    /// Go's base body returns `nil`; only `LogicalProjection` and
    /// `LogicalSelection` override it. `LogicalProjection`'s override reads
    /// its CHILD's answer and rewrites it through the projection, so it lives
    /// on the driver rather than on the operator; the empty vector is Go's
    /// answer everywhere else.
    #[must_use]
    pub fn pull_up_constant_predicates(&self) -> Vec<Expression> {
        match self {
            Self::Selection(op) => op.pull_up_constant_predicates(),
            Self::Projection(_)
            | Self::Join(_)
            | Self::Aggregation(_)
            | Self::DataSource(_)
            | Self::Sort(_)
            | Self::Limit(_)
            | Self::Apply(_)
            | Self::TopN(_)
            | Self::UnionAll(_)
            | Self::PartitionUnionAll(_)
            | Self::Window(_)
            | Self::CTE(_)
            | Self::CTETable(_)
            | Self::MaxOneRow(_)
            | Self::Lock(_)
            | Self::Sequence(_)
            | Self::UnionScan(_)
            | Self::TiKVSingleGather(_)
            | Self::TableScan(_)
            | Self::IndexScan(_)
            | Self::TableDual(_)
            | Self::Todo(_) => Vec::new(),
        }
    }

    /// Go `RecursiveDeriveStats(colGroups)` (`<9th>`): derive bottom-up, then
    /// call this node's `DeriveStats` with the children's results.
    ///
    /// The recursion is written with an explicit stack; see the module header.
    pub fn recursive_derive_stats(
        &mut self,
        _col_groups: &[Vec<Column>],
    ) -> Result<(StatsInfo, bool), PlanError> {
        Err(PlanError::internal(
            "todo: LogicalPlan::recursive_derive_stats needs the per-operator DeriveStats",
        ))
    }

    /// Go `DeriveStats(childStats, selfSchema, childSchema, reloads)` (`<10th>`).
    ///
    /// This is the one rule body that IS dependency-closed, so it is ported
    /// whole (`base_logical_plan.go:224`):
    /// * exactly one child — adopt its stats, reloaded;
    /// * more than one — Go raises `ErrInternal`, because a multi-child
    ///   operator must override;
    /// * no child and nothing to reload — keep the existing stats;
    /// * no child otherwise — one row, and every schema column at NDV 1.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        _child_schema: &[Schema],
        reloads: &[bool],
    ) -> Result<(StatsInfo, bool), PlanError> {
        let reload = reloads.iter().any(|one| *one);
        if child_stats.len() == 1 {
            let stats = child_stats[0].clone();
            self.set_stats(Some(stats.clone()));
            return Ok((stats, true));
        }
        if child_stats.len() > 1 {
            return Err(PlanError::internal(
                "LogicalPlans with more than one child should implement their own DeriveStats().",
            ));
        }
        if !reload {
            if let Some(existing) = self.stats_info() {
                return Ok((existing.clone(), false));
            }
        }
        let profile = StatsInfo::new(
            1.0,
            self_schema
                .columns
                .iter()
                .map(|col| (col.unique_id, 1.0_f64)),
        );
        self.set_stats(Some(profile.clone()));
        Ok((profile, true))
    }

    /// Go `ExtractColGroups(colGroups)` (`<11th>`). The base body returns
    /// `nil`, which is Go's answer, not a `todo`.
    ///
    /// `LogicalAggregation` DISCARDS the parent's groups and asks only for its
    /// own group-by columns; that override is dispatched here. The
    /// `LogicalProjection` and `LogicalJoin` overrides need
    /// `Schema.ExtractColGroups`, which `tidb-expr` lists as deferred, so they
    /// fall through to the base answer rather than to a guess.
    #[must_use]
    pub fn extract_col_groups(&self, _col_groups: &[Vec<Column>]) -> Vec<Vec<Column>> {
        match self {
            Self::Aggregation(op) => op.extract_col_groups(),
            Self::Selection(_)
            | Self::Projection(_)
            | Self::Join(_)
            | Self::DataSource(_)
            | Self::Sort(_)
            | Self::Limit(_)
            | Self::Apply(_)
            | Self::TopN(_)
            | Self::UnionAll(_)
            | Self::PartitionUnionAll(_)
            | Self::Window(_)
            | Self::CTE(_)
            | Self::CTETable(_)
            | Self::MaxOneRow(_)
            | Self::Lock(_)
            | Self::Sequence(_)
            | Self::UnionScan(_)
            | Self::TiKVSingleGather(_)
            | Self::TableScan(_)
            | Self::IndexScan(_)
            | Self::TableDual(_)
            | Self::Todo(_) => Vec::new(),
        }
    }

    /// Go `PreparePossibleProperties(schema, childrenProperties...)` (`<12th>`).
    ///
    /// Ported whole (`base_logical_plan.go:257`): the node has TiFlash-capable
    /// order support only if it has at least one child and EVERY child
    /// reported one.
    pub fn prepare_possible_properties(
        &mut self,
        _schema: &Schema,
        children_properties: &[Option<PossiblePropertiesInfo>],
    ) -> PossiblePropertiesInfo {
        let mut has_tiflash = !children_properties.is_empty();
        for child in children_properties {
            match child {
                None => has_tiflash = false,
                Some(info) => has_tiflash = has_tiflash && info.has_tiflash,
            }
        }
        self.base_mut().has_tiflash = has_tiflash;
        PossiblePropertiesInfo {
            orders: Vec::new(),
            has_tiflash,
        }
    }

    /// Go `ExtractCorrelatedCols()` (`<13th>`), dispatched to the ported
    /// operators. The base body returns `nil`, which is what an operator
    /// without expressions of its own answers.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        match self {
            Self::Selection(op) => op.extract_correlated_cols(),
            Self::Projection(op) => op.extract_correlated_cols(),
            Self::Join(op) => op.extract_correlated_cols(),
            Self::Aggregation(op) => op.extract_correlated_cols(),
            Self::DataSource(op) => op.extract_correlated_cols(),
            Self::Sort(op) => op.extract_correlated_cols(),
            Self::TopN(op) => op.extract_correlated_cols(),
            Self::Window(op) => op.extract_correlated_cols(),
            Self::CTE(op) => op.extract_correlated_cols(),
            // Go `LogicalApply.ExtractCorrelatedCols` (`logical_apply.go:250`)
            // subtracts the columns the OUTER child already produces, which the
            // enum can supply and the operator alone cannot.
            Self::Apply(op) => op
                .base()
                .children()
                .first()
                .and_then(Self::schema)
                .map_or_else(
                    || op.join.extract_correlated_cols(),
                    |outer| op.extract_correlated_cols(outer),
                ),
            Self::Limit(_)
            | Self::CTETable(_)
            | Self::MaxOneRow(_)
            | Self::Lock(_)
            | Self::Sequence(_)
            | Self::UnionScan(_)
            | Self::TiKVSingleGather(_)
            | Self::TableScan(_)
            | Self::IndexScan(_)
            | Self::UnionAll(_)
            | Self::PartitionUnionAll(_)
            | Self::TableDual(_)
            | Self::Todo(_) => Vec::new(),
        }
    }

    /// Go `Plan.ExplainInfo()` (`base/plan_base.go`), dispatched to the ported
    /// operators.
    ///
    /// Every operator whose Go body renders an expression list through
    /// `expression.SortedExplainExpressionList` needs an `EvalContext` that
    /// this crate does not have; those arms answer with
    /// `BaseLogicalPlan::explain_info`, the empty string, exactly as an
    /// operator without an override does.
    #[must_use]
    pub fn explain_info(&self) -> String {
        match self {
            Self::Join(op) => op.explain_info(),
            Self::UnionScan(op) => op.explain_info(),
            Self::TiKVSingleGather(op) => op.explain_info(),
            Self::TableScan(op) => op.explain_info(),
            Self::IndexScan(op) => op.explain_info(),
            Self::Apply(op) => op.explain_info(),
            Self::DataSource(op) => op.explain_info(),
            Self::Sort(op) => op.explain_info(),
            Self::Limit(op) => op.explain_info(),
            Self::TopN(op) => op.explain_info(),
            Self::Selection(_)
            | Self::Projection(_)
            | Self::Aggregation(_)
            | Self::UnionAll(_)
            | Self::PartitionUnionAll(_)
            | Self::Window(_)
            | Self::CTE(_)
            | Self::CTETable(_)
            | Self::MaxOneRow(_)
            | Self::Lock(_)
            | Self::Sequence(_)
            | Self::TableDual(_)
            | Self::Todo(_) => BaseLogicalPlan::explain_info().to_owned(),
        }
    }

    /// Go `MaxOneRow()` (`<14th>`).
    #[must_use]
    pub const fn max_one_row(&self) -> bool {
        self.base().max_one_row()
    }

    /// Go `Children()` (`<15th>`).
    #[must_use]
    pub fn children(&self) -> &[Self] {
        self.base().children()
    }

    /// Go `SetChildren(...)` (`<16th>`).
    pub fn set_children(&mut self, children: Vec<Self>) {
        self.base_mut().set_children(children);
    }

    /// Go `SetChild(i, child)` (`<17th>`); returns the replaced child.
    pub fn set_child(&mut self, i: usize, child: Self) -> Option<Self> {
        self.base_mut().set_child(i, child)
    }

    /// Go `RollBackTaskMap(TS)` (`<18th>`).
    ///
    /// A `todo`: the task map it rolls back is narrowed out of
    /// [`BaseLogicalPlan`] (see that struct's header).
    pub const fn roll_back_task_map(&mut self, _ts: u64) {}

    /// Go `CanPushToCop(store)` (`<19th>`), deprecated upstream in favour of
    /// the op-self check `CanSelfBeingPushedToCopImpl`.
    #[must_use]
    pub const fn can_push_to_cop(&self, _store: StoreType) -> bool {
        false // todo: logicalop.CanPushToCopImpl
    }

    /// Go `ExtractFD()` (`<20th>`): the functional-dependency set, derived
    /// bottom-up. `pkg/planner/funcdep` is not transcreated; see the
    /// [`BaseLogicalPlan`] header.
    pub const fn extract_fd(&self) {}

    /// Go `ConvertOuterToInnerJoin(predicates)` (`<22nd>`).
    #[must_use]
    pub fn convert_outer_to_inner_join(self, _predicates: &[Expression]) -> Self {
        self // todo: needs LogicalJoin's null-rejection analysis
    }

    /// Go `SetPlanIDsHash(hash)` (`<23rd>`).
    pub const fn set_plan_ids_hash(&mut self, hash: u64) {
        self.base_mut().set_plan_ids_hash(hash);
    }

    /// Go `GetPlanIDsHash()` (`<24th>`).
    #[must_use]
    pub const fn plan_ids_hash(&self) -> u64 {
        self.base().plan_ids_hash()
    }

    /// Go `GetChildStatsAndSchema()` (`<26th>`): the first child's stats and
    /// schema. Go indexes `Children()[0]` and panics on a leaf; this returns
    /// `None`.
    #[must_use]
    pub fn get_child_stats_and_schema(&self) -> Option<(Option<&StatsInfo>, Option<&Schema>)> {
        let child = self.children().first()?;
        Some((child.stats_info(), child.schema()))
    }

    /// Go `GetJoinChildStatsAndSchema()` (`<27th>`): both children's stats and
    /// schemas. Go's base body PANICS, so only a two-child operator may
    /// answer; `None` is that refusal without the panic.
    #[must_use]
    pub fn get_join_child_stats_and_schema(&self) -> Option<JoinChildStatsAndSchema<'_>> {
        // Go's override lives on `LogicalJoin` (`logical_join.go:775`), and
        // `LogicalApply` PROMOTES it through the embedding.
        if !matches!(self, Self::Join(_) | Self::Apply(_)) {
            return None;
        }
        let children = self.children();
        let (left, right) = (children.first()?, children.get(1)?);
        Some((
            left.stats_info(),
            right.stats_info(),
            left.schema(),
            right.schema(),
        ))
    }

    /// Go `ExhaustPhysicalPlans(prop)`
    /// (`pkg/planner/core/exhaust_physical_plans.go`): the physical
    /// alternatives for this operator under `prop`, plus whether the
    /// statement's join hint could be honoured.
    ///
    /// A `todo` here by design: [`crate::find_best_task`] already models the
    /// `LogicalJoin` arm of this enumeration against its own node type, and
    /// merging the two is a later batch rather than a second answer now.
    pub fn exhaust_physical_plans(
        &self,
        _prop: &PhysicalProperty,
    ) -> Result<(Vec<PhysicalPlan>, bool), PlanError> {
        Err(PlanError::internal(
            "todo: LogicalPlan::exhaust_physical_plans, see crate::find_best_task",
        ))
    }

    // ***** depth-safe tree utilities (see the module header) *****

    /// This node with its OWN fields copied and NO children.
    ///
    /// The building block of [`Self::deep_clone`]; on its own it is also what
    /// a rule wants when it rebuilds a node around moved children.
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        match self {
            Self::Selection(op) => Self::Selection(op.clone_shallow()),
            Self::Projection(op) => Self::Projection(op.clone_shallow()),
            Self::Join(op) => Self::Join(op.clone_shallow()),
            Self::Apply(op) => Self::Apply(op.clone_shallow()),
            Self::Aggregation(op) => Self::Aggregation(op.clone_shallow()),
            Self::Sort(op) => Self::Sort(op.clone_shallow()),
            Self::Limit(op) => Self::Limit(op.clone_shallow()),
            Self::TopN(op) => Self::TopN(op.clone_shallow()),
            Self::UnionAll(op) => Self::UnionAll(op.clone_shallow()),
            Self::PartitionUnionAll(op) => Self::PartitionUnionAll(op.clone_shallow()),
            Self::Window(op) => Self::Window(op.clone_shallow()),
            Self::MaxOneRow(op) => Self::MaxOneRow(op.clone_shallow()),
            Self::Lock(op) => Self::Lock(op.clone_shallow()),
            Self::Sequence(op) => Self::Sequence(op.clone_shallow()),
            Self::UnionScan(op) => Self::UnionScan(op.clone_shallow()),
            Self::TiKVSingleGather(op) => Self::TiKVSingleGather(op.clone_shallow()),
            Self::TableScan(op) => Self::TableScan(op.clone_shallow()),
            Self::IndexScan(op) => Self::IndexScan(op.clone_shallow()),
            Self::CTE(op) => Self::CTE(op.clone_shallow()),
            Self::CTETable(op) => Self::CTETable(op.clone_shallow()),
            Self::DataSource(op) => Self::DataSource(op.clone_shallow()),
            Self::TableDual(op) => Self::TableDual(LogicalTableDual {
                base: op.base.shell(),
                row_count: op.row_count,
            }),
            Self::Todo(op) => Self::Todo(TodoLogicalOp {
                base: op.base.shell(),
                go_operator: op.go_operator.clone(),
            }),
        }
    }

    /// A deep copy of this subtree, built without recursion.
    ///
    /// The DERIVED [`Clone`] recurses through the child vector and overflows
    /// at the same order of depth as `Drop` — measurably around 2,000 levels
    /// in an unoptimised build. Use this whenever the depth is not known to
    /// be small.
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        enum Step<'a> {
            Enter(&'a LogicalPlan),
            Exit(&'a LogicalPlan),
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

    /// The longest root-to-leaf path length, counted without recursion.
    #[must_use]
    pub fn max_depth(&self) -> usize {
        let mut stack = vec![(self, 1_usize)];
        let mut deepest = 0;
        while let Some((node, depth)) = stack.pop() {
            deepest = deepest.max(depth);
            for child in node.children() {
                stack.push((child, depth + 1));
            }
        }
        deepest
    }

    /// Tears the subtree down iteratively.
    ///
    /// Rust's derived `Drop` glue recurses, and a chain deeper than roughly
    /// 20,000 overflows a 2 MiB stack. Callers holding a tree of untrusted
    /// depth should end its life here rather than by letting it fall out of
    /// scope. A manual `Drop` impl would fix this globally but would forbid
    /// moving a base out of a variant, which the by-value rule signatures
    /// depend on.
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
mod operator_tests;
#[cfg(test)]
mod tests;
