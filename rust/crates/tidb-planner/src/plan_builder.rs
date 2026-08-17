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

//! AST to [`LogicalPlan`]: the SELECT build spine.
//!
//! Go sources:
//! * `pkg/planner/core/planbuilder.go` — `PlanBuilder` (line 219),
//!   `handleColHelper` (375), `GetOptFlag` (455), `getSelectOffset` (465).
//! * `pkg/planner/core/logical_plan_builder.go` — `buildResultSetNode` (434),
//!   `buildSelection` (1343), `buildProjectionField` (1535),
//!   `buildProjection` (1767), `buildSort` (2399), `buildLimit` (2569),
//!   `unfoldWildStar` (4115), `buildSelect` (4254), `buildTableDual` (4658),
//!   `buildDataSource` (4927).
//!
//! This is the piece the rest of `pkg/planner/core` was ported AGAINST: the
//! logical tree (batch 7a), the plan-aware expression rewriter (batch 5) and
//! the access paths all had a shape but no producer. It is a SEED of the two
//! Go files above — the SELECT spine lands, and the boundaries below name
//! every clause that does not.
//!
//! # 1. The catalogue seam
//!
//! [`catalog::TableSource`] is the ONLY way a table reaches this crate.
//! `tidb-planner` has no `tidb-model`/`tidb-meta` dependency and must not gain
//! one; see [`catalog`]'s header for the method set and why each method is
//! there.
//!
//! # 2. The marker scheme
//!
//! Go's eight `map[*ast.XxxExpr]int` side tables cannot be transcreated —
//! their keys are AST node ADDRESSES, and Rust AST values are cloned and
//! moved. [`marker`] ratifies the replacement: the producing pass substitutes
//! a reserved `#kind#index` column INTO the clause, and the reading pass
//! decodes it. Every later batch uses that and no other mechanism; the spec,
//! the collision bound and the instruction to 6c/6e are all in [`marker`]'s
//! header. It is harvested from `tidb-executor`'s
//! `driver/agg_build.rs:787` `substitute_aggregates`, which already shipped
//! on it.
//!
//! Consequently [`PlanBuilder`] carries NO `col_mapper`, `agg_mapper`,
//! `having_map`, `order_map`, `total_map`, `window_agg_map` or
//! `window_mapper` field: each is a [`marker::MarkerKind`]. Go's
//! `correlatedAggMapper` maps to a `*expression.CorrelatedColumn` rather than
//! an index, so its VALUES live in
//! [`PlanBuilder::correlated_agg_columns`] and the key is the
//! [`marker::MarkerKind::CorrelatedAgg`] marker's index into that vector.
//!
//! # 3. THE READ-AFTER-MOVE RULE. Stated once, for every clause builder.
//!
//! Go writes
//!
//! ```text
//! selection.SetChildren(p)
//! selection.SetSchema(p.Schema())          // p is still live
//! ```
//!
//! and `dual.SetOutputNames(p.OutputNames()); dual.SetSchema(p.Schema())` in
//! `buildSelection`'s always-false arm. Here the child is MOVED into the
//! parent's child list, so `p` is gone by the second line.
//!
//! **Rule: snapshot [`LogicalPlan::schema`] and [`LogicalPlan::output_names`]
//! off the child BEFORE moving it.** Both are cheap clones — a `Schema` is a
//! `Vec<Column>` of small structs, and `FieldName` is five short strings — and
//! [`snapshot_schema_and_names`] is the one helper that does it. Every builder
//! in this module calls it as its first statement after it has a child.
//!
//! Do NOT reach for `Rc`, interior mutability or a second traversal to work
//! around this: the logical tree's whole rule surface is
//! `fn(self, ...) -> LogicalPlan` precisely so children can be owned, and a
//! shared handle on a child edge would undo that (see [`crate::logical`]'s
//! header).
//!
//! # 4. `opt_flag`, and where it lives
//!
//! Go accumulates `PlanBuilder.optFlag` as the clauses are built and hands it
//! to `logicalOptimize` beside the plan. [`PlanBuilder::opt_flag`] is that
//! `u64`, over [`crate::logical::rule::flags`]' already-ported bit values, and
//! [`PlanBuilder::build_select`] returns `(LogicalPlan, u64)`.
//!
//! It is deliberately NOT a field on [`BasePlan`](crate::plan_base::BasePlan):
//! that struct's layout is load-bearing (`PLAN_SIZE`, the memo's hashing, the
//! plan-cache clone), the flag is a property of the BUILD and not of any node,
//! and Go itself keeps it on the builder.
//!
//! # 5. One error type
//!
//! [`PlanError`] is the crate's plan-side error and what
//! [`crate::logical::rule`] already returns; [`RewriteError`] is the
//! expression rewriter's. Rather than a third type, `PlanError: From<RewriteError>`
//! (and `From<EvalError>`) lets every builder body use `?` over both. Nothing
//! reads a `RewriteError` variant after it crosses into a builder, so
//! flattening to `PlanError`'s message loses no decision — the variants stay
//! available to callers of the rewriter itself.
//!
//! # Boundaries, by exact Go symbol
//!
//! Each is a symbol whose dependency is genuinely absent, not a body skipped.
//!
//! * `PlanBuilder.Build` and every non-SELECT statement builder
//!   (`planbuilder.go`: `buildInsert`, `buildDDL`, `buildShow`,
//!   `buildExplain`, `buildSimple`, ...). Out of scope for this batch by
//!   construction; the SELECT path is what the logical tree needs.
//! * The UPDATE/DELETE column machinery
//!   (`logical_plan_builder.go:5808-6494`, `6705`) and
//!   `ExtractTableList`/`tableListExtractor` (`:7450-7666`). Explicitly out of
//!   scope; both are DML/privilege surfaces with no logical-tree consumer.
//! * `buildAggregation` (`:212`), `resolveHavingAndOrderBy` (`:2913`),
//!   `buildWindowFunctions` (`:6893`), `buildUnion`/`buildSetOpr` (`:2149`),
//!   `buildJoin` (`:723`), `buildCte` (`:7900`). Batches 6b-6e. Each has its
//!   `Todo` arm here rather than a silent fallthrough.
//! * `expression.EvalBool` on a folded predicate
//!   (`buildSelection`'s always-false arm). The rewriter hands back a folded
//!   [`Constant`](tidb_expr::constant::Constant); reading its truth needs an
//!   `EvalContext`, which [`constant_is_always_false`] takes from the already
//!   materialised [`Datum`] rather than evaluating. A constant that is NOT
//!   materialised (a parameter marker, a non-deterministic builtin) is
//!   conservatively kept as a condition, which is Go's `useCache` arm.
//! * `hint.QBHintHandler` / `hint.PlanHints` / `setPreferredStoreType`.
//!   The hint catalogue is not transcreated; [`PlanBuilder::hints`] carries
//!   the fields the ported bodies read, exactly as
//!   [`crate::expression_rewriter::RewriterHints`] does.
//! * `tablesampler.NewTableSampleInfo`, `tableHasDirtyContent`,
//!   `addExtraPhysTblIDColumn4DS`, `BuildDataSourceFromView`. Table sampling,
//!   the transaction membuffer and the view expander each need a handle this
//!   crate does not hold; `buildDataSource`'s arms for them are marked.
//!
//! # Narrowings, by name
//!
//! * `visitInfo` (`planbuilder.go:220`) — DROPPED, not narrowed. It is the
//!   privilege-check record. There is no privilege model anywhere in the
//!   workspace, so carrying an always-empty vector would be a stub that later
//!   readers would mistake for coverage.
//! * `rewriterPool` / `rewriterCounter` (`:229`) — DROPPED. A free-list of
//!   `*expressionRewriter` to dodge Go's allocator. Rust constructs a rewriter
//!   by value; pooling would be a pessimisation with no semantic content.
//! * `resolveCtx *resolve.Context` (`:337`) — DROPPED. It caches
//!   name-resolution results keyed by AST node pointer, which is the same
//!   unsound key the marker scheme replaces.
//! * `isCreateView` / `capFlag`'s `canExpandAST` (`:267`), `inUpdateStmt` /
//!   `inDeleteStmt` (`:234`), `isSampling` (`:274`) — DROPPED. All four gate
//!   statement kinds this batch does not build. `isSampling`'s only reader is
//!   `GetOptFlag`'s "return 0", which cannot fire when nothing sets it.
//! * `partitionedTable []table.PartitionedTable`, `hintProcessor`,
//!   `renamingViewName`, `nonViableFTSMatch`, `predicateMatchSeen`,
//!   `allowBuildCastArray` — no reader on the SELECT spine; each belongs to a
//!   boundary above.
//! * `outerCTEs []*cteInfo` becomes [`OuterCte`], holding the name, the
//!   recursion flags and the built seed plan's schema/names — what the ported
//!   bodies read. `*cteInfo`'s `storageID`, `cteClass` and the optimizer
//!   handles belong to batch 6d.

pub mod catalog;
pub mod from;
#[cfg(test)]
mod from_tests;
pub mod handle_col_helper;
pub mod marker;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::{Expr, JoinNode, Limit, SelectField, SelectStmt, TableRef};
use tidb_datatype::{
    Datum, FieldName, FieldNameMetadata, FieldType, FieldTypeCode, FieldTypeFlags,
    IdentifierMetadata, SessionTimeZone,
};
use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;

use tidb_expr::expr_util::normal_form::split_cnf_items;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};
use tidb_expr::schema::Schema;
use tidb_expr::{Columns, EvalError};

use crate::expression_rewriter::{
    ClauseCode, ColumnIdAllocator, ExprRewriterPlanCtx, ExpressionRewriter, RewriteError,
    RewriterEnv, RewriterHints, RewriterSessionFlags, SubQueryCtx,
};
use crate::logical::data_source::{DataSource, DataSourceColumn, EXTRA_HANDLE_ID};
use crate::logical::limit::LogicalLimit;
use crate::logical::projection::LogicalProjection;
use crate::logical::rule::flags;
use crate::logical::selection::LogicalSelection;
use crate::logical::sort::LogicalSort;
use crate::logical::table_dual::LogicalTableDual;
use crate::logical::{BaseLogicalPlan, LogicalPlan};
use crate::plan_base::{PlanError, PlanIdAllocator};

use catalog::TableSource;
use handle_col_helper::{HandleColHelper, HandleColMap, PlanHandleCols};
use marker::{MarkerKind, PlanMarker};

/// Go `model.ExtraPhysTblID` (`meta/model/table.go:43`).
pub const EXTRA_PHYS_TBL_ID: i64 = -3;
/// Go `model.ExtraCommitTSID` (`meta/model/table.go:52`).
pub const EXTRA_COMMIT_TS_ID: i64 = -5;
/// Go `model.ExtraHandleName` (`meta/model/table.go:92`).
pub const EXTRA_HANDLE_NAME: &str = "_tidb_rowid";
/// Go `model.ExtraCommitTSName` (`meta/model/table.go:98`).
pub const EXTRA_COMMIT_TS_NAME: &str = "_tidb_commit_ts";

impl From<RewriteError> for PlanError {
    fn from(error: RewriteError) -> Self {
        Self::internal(error.to_string())
    }
}

/// The expression rewriter's own error, which reaches a builder through
/// [`rewrite_expr_resolved`].
impl From<EvalError> for PlanError {
    fn from(error: EvalError) -> Self {
        Self::internal(format!("{error:?}"))
    }
}

/// Go `PlanBuilder.outerCTEs []*cteInfo`, narrowed to the fields the SELECT
/// spine reads; see this module's narrowings.
#[derive(Clone, Debug, Default)]
pub struct OuterCte {
    /// Go `cteInfo.def.Name.L`.
    pub name: String,
    /// Go `cteInfo.isBuilding`.
    pub is_building: bool,
    /// Go `cteInfo.isRecursive`.
    pub is_recursive: bool,
    /// Go `cteInfo.seedLP.Schema()`, snapshotted per rule 3.
    pub seed_schema: Option<Schema>,
    /// Go `cteInfo.seedLP.OutputNames()`, snapshotted per rule 3.
    pub seed_names: Vec<FieldName>,
}

/// One select-list entry after wildcard expansion.
///
/// Go keeps `[]*ast.SelectField` and reads `field.Text()` / `field.AsName` /
/// `field.Auxiliary` off it. `tidb_ast::SelectFieldList` stores the source
/// text BESIDE the field slice, so the two travel together here instead.
#[derive(Clone, Debug)]
pub struct ProjectionField {
    /// The projected expression, with any [`marker`] already substituted in.
    pub expr: Expr,
    /// Go `SelectField.AsName`.
    pub alias: Option<String>,
    /// Go `SelectField.Text()`: the exact source bytes, which name a computed
    /// column. Absent for a wildcard-expanded or builder-appended field.
    pub text: Option<String>,
    /// Go `SelectField.Auxiliary`: a column ORDER BY or HAVING needs but the
    /// select list does not project. Trimmed by `buildSelect`'s `:4640`
    /// trailing projection.
    pub hidden: bool,
}

/// Go `PlanBuilder.currentBlockExpand` / `outerBlockExpand`, narrowed to the
/// grouping-set columns the ported bodies read off a `*LogicalExpand`.
#[derive(Clone, Debug, Default)]
pub struct BlockExpand {
    /// The `Expand`'s `GroupingIDCol`, when one has been allocated.
    pub grouping_id_col: Option<Column>,
    /// The distinct grouping columns the block's `ROLLUP` produced.
    pub distinct_group_by_cols: Vec<Column>,
}

/// Go `schemaTableKey` (`planbuilder.go`), the recursion guard's key.
pub type SchemaTableKey = (String, String);

/// Go `PlanBuilder` (`planbuilder.go:219`), on the SELECT path.
///
/// The dropped fields are listed in this module's narrowings, each with the
/// reason it is absent rather than empty.
pub struct PlanBuilder<'a, S: TableSource, C: Columns> {
    /// Go `b.is infoschema.InfoSchema`, through the seam.
    pub source: &'a S,
    /// Go `b.ctx.GetExprCtx()`: the expression build context.
    pub ctx: &'a C,
    /// Go `PlanID`.
    pub plan_ids: &'a PlanIdAllocator,
    /// Go `PlanColumnID`.
    pub column_ids: &'a ColumnIdAllocator,
    /// Go `ctx.GetSessionVars().Location()`, which every expression rewrite
    /// runs under.
    pub time_zone: SessionTimeZone,

    /// Go `optFlag`; see this module's section 4.
    pub opt_flag: u64,
    /// Go `curClause`.
    pub cur_clause: ClauseCode,
    /// Go `qbOffset`; [`Self::select_offset`] reads its tail.
    pub qb_offset: Vec<i32>,

    /// Go `outerSchemas`, outermost first.
    pub outer_schemas: Vec<Schema>,
    /// Go `outerNames`, index-parallel to [`Self::outer_schemas`].
    pub outer_names: Vec<Vec<FieldName>>,
    /// Go `lateralOuterCount`: how many trailing [`Self::outer_schemas`]
    /// entries `buildJoin` pushed for a `LATERAL` derived table, which a
    /// NON-lateral derived table must not see.
    pub lateral_outer_count: usize,
    /// Go `outerCTEs`.
    pub outer_ctes: Vec<OuterCte>,
    /// Go `outerBlockExpand`.
    pub outer_block_expand: Vec<BlockExpand>,
    /// Go `currentBlockExpand`.
    pub current_block_expand: Option<BlockExpand>,

    /// Go `windowSpecs map[string]*ast.WindowSpec`.
    pub window_specs: BTreeMap<String, tidb_ast::WindowDef>,
    /// Go `inStraightJoin`.
    pub in_straight_join: bool,
    /// Go `handleHelper`.
    pub handle_helper: HandleColHelper,
    /// Go `allNames [][]*types.FieldName`: the output names as they stood
    /// BEFORE each projection, which `evalDefaultExpr` searches.
    pub all_names: Vec<Vec<FieldName>>,
    /// The VALUES of Go `correlatedAggMapper`; the key is the
    /// [`MarkerKind::CorrelatedAgg`] marker's index. See section 2.
    pub correlated_agg_columns: Vec<tidb_expr::column::CorrelatedColumn>,

    /// Go `buildingCTE`.
    pub building_cte: bool,
    /// Go `isCTE`.
    pub is_cte: bool,
    /// Go `buildingRecursivePartForCTE`.
    pub building_recursive_part_for_cte: bool,
    /// Go `nameMapCTE`.
    pub name_map_cte: BTreeSet<String>,
    /// Go `allocIDForCTEStorage`.
    pub alloc_id_for_cte_storage: i32,
    /// Go `buildingViewStack`, the recursive-view guard.
    pub building_view_stack: BTreeSet<SchemaTableKey>,

    /// Go `subQueryCtx`.
    pub sub_query_ctx: SubQueryCtx,
    /// Go `subQueryHintFlags`.
    pub sub_query_hint_flags: u64,
    /// Go `noDecorrelate`.
    pub no_decorrelate: bool,
    /// Go `isForUpdateRead`.
    pub is_for_update_read: bool,

    /// Go `SessionVars`, narrowed exactly as the rewriter narrows it.
    pub flags: RewriterSessionFlags,
    /// Go `b.TableHints()`, narrowed; see this module's boundaries.
    pub hints: RewriterHints,
    /// Go `b.TableHints()`'s JOIN half, which `buildJoin` reads through
    /// `SetPreferredJoinTypeAndOrder`; see [`from::JoinHints`].
    pub join_hints: from::JoinHints,
}

/// The child's schema and output names, taken BEFORE the child is moved.
///
/// This is section 3's rule in one call. Go reads `p.Schema()` after
/// `SetChildren(p)`; here that would be a use-after-move, so every builder
/// starts with this.
#[must_use]
pub fn snapshot_schema_and_names(plan: &LogicalPlan) -> (Schema, Vec<FieldName>) {
    (
        plan.schema().cloned().unwrap_or_default(),
        plan.output_names().to_vec(),
    )
}

/// Go's `expression.EvalBool(ctx, []{con}, chunk.Row{})` on an already-folded
/// constant, reduced to the materialised [`Datum`].
///
/// `None` means "not decidable here" — a parameter marker or a constant whose
/// value the fold left deferred — which is exactly Go's `useCache` arm, where
/// the constant is KEPT as a condition rather than decided at plan time.
#[must_use]
pub fn constant_is_always_false(constant: &Constant) -> Option<bool> {
    if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
        return None;
    }
    match &constant.value {
        // Go: a NULL predicate filters every row, the same as false.
        Datum::Null => Some(true),
        Datum::Int(value) => Some(*value == 0),
        Datum::UInt(value) => Some(*value == 0),
        Datum::Real(value) => Some(*value == 0.0),
        Datum::Float32(value) => Some(*value == 0.0),
        _ => None,
    }
}

/// Resolves a column path against one plan's schema and output names, and
/// decodes the markers a previous pass substituted in.
///
/// This is Go `expressionRewriter.toColumn`'s NAME half
/// (`expression_rewriter.go`'s `Leave` for `*ast.ColumnNameExpr`) plus
/// `expression.FindFieldName`, in the shape [`ColumnResolver`] wants. The
/// SUBQUERY half stays in [`crate::expression_rewriter`], which this does not
/// duplicate.
pub struct PlanScopeResolver<'a> {
    schema: &'a Schema,
    names: &'a [FieldName],
    /// The columns a marker index refers to, per [`MarkerKind`]. A kind absent
    /// from this map has no producer yet in the current build.
    marker_columns: &'a BTreeMap<MarkerKind, Vec<Column>>,
    time_zone: SessionTimeZone,
}

impl<'a> PlanScopeResolver<'a> {
    /// A resolver over one plan's schema and names, with no markers bound.
    #[must_use]
    pub const fn new(
        schema: &'a Schema,
        names: &'a [FieldName],
        marker_columns: &'a BTreeMap<MarkerKind, Vec<Column>>,
        time_zone: SessionTimeZone,
    ) -> Self {
        Self {
            schema,
            names,
            marker_columns,
            time_zone,
        }
    }

    /// Go `expression.FindFieldName(names, astCol)`: the unique index whose
    /// name matches, or an error when several do.
    fn find_field_name(&self, path: &[String]) -> Option<usize> {
        let (db, table, column) = match path {
            [column] => (None, None, column.as_str()),
            [table, column] => (None, Some(table.as_str()), column.as_str()),
            [db, table, column] => (Some(db.as_str()), Some(table.as_str()), column.as_str()),
            _ => return None,
        };
        let mut found = None;
        for (index, name) in self.names.iter().enumerate() {
            if name.hidden || name.not_explicit_usable {
                continue;
            }
            let matches = name.names.column.lower.eq_ignore_ascii_case(column)
                && table.is_none_or(|t| name.names.table.lower.eq_ignore_ascii_case(t))
                && db.is_none_or(|d| name.names.database.lower.eq_ignore_ascii_case(d));
            if matches {
                if found.is_some() {
                    // Go raises ErrAmbiguous. The resolver interface has no
                    // error channel, so an ambiguous name resolves to nothing
                    // and the caller reports the unresolved column.
                    return None;
                }
                found = Some(index);
            }
        }
        found
    }
}

impl ColumnResolver for PlanScopeResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let column = self.resolve_column(path)?;
        Some((
            usize::try_from(column.index).unwrap_or(0),
            column.ret_type.clone()?,
            column.unique_id,
        ))
    }

    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        // A marker is checked FIRST: a producing pass put it there in place of
        // a node this scope's names know nothing about.
        if let [name] = path {
            if let Some(marker) = PlanMarker::decode(name) {
                if let Some(column) = self
                    .marker_columns
                    .get(&marker.kind)
                    .and_then(|columns| columns.get(marker.index))
                {
                    let mut column = column.clone();
                    // Spec rule 4: the marker index IS the producing
                    // operator's schema index, which is also the evaluated
                    // row position Go's `Column.Index` carries.
                    column.index = marker.index as i64;
                    return Some(column);
                }
                // Rule 6 / the collision note: an undecodable-to-a-column
                // marker falls through to ordinary name resolution.
            }
        }
        let index = self.find_field_name(path)?;
        let mut column = self.schema.columns.get(index)?.clone();
        column.index = index as i64;
        Some(column)
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.time_zone.clone()
    }
}

impl<'a, S: TableSource, C: Columns> PlanBuilder<'a, S, C> {
    /// Go `NewPlanBuilder().Init(sctx, is, processor)` (`planbuilder.go:520`),
    /// over this crate's seams.
    #[must_use]
    pub fn new(
        source: &'a S,
        ctx: &'a C,
        plan_ids: &'a PlanIdAllocator,
        column_ids: &'a ColumnIdAllocator,
        time_zone: SessionTimeZone,
    ) -> Self {
        Self {
            source,
            ctx,
            plan_ids,
            column_ids,
            time_zone,
            opt_flag: 0,
            cur_clause: ClauseCode::Unknow,
            qb_offset: Vec::new(),
            outer_schemas: Vec::new(),
            outer_names: Vec::new(),
            lateral_outer_count: 0,
            outer_ctes: Vec::new(),
            outer_block_expand: Vec::new(),
            current_block_expand: None,
            window_specs: BTreeMap::new(),
            in_straight_join: false,
            handle_helper: HandleColHelper::new(),
            all_names: Vec::new(),
            correlated_agg_columns: Vec::new(),
            building_cte: false,
            is_cte: false,
            building_recursive_part_for_cte: false,
            name_map_cte: BTreeSet::new(),
            alloc_id_for_cte_storage: 0,
            building_view_stack: BTreeSet::new(),
            sub_query_ctx: SubQueryCtx::NotHandlingSubquery,
            sub_query_hint_flags: 0,
            no_decorrelate: false,
            is_for_update_read: false,
            flags: RewriterSessionFlags::default(),
            hints: RewriterHints::default(),
            join_hints: from::JoinHints::default(),
        }
    }

    /// Go `getSelectOffset()` (`planbuilder.go:465`): the tail of `qbOffset`,
    /// or `-1`.
    #[must_use]
    pub fn select_offset(&self) -> i32 {
        self.qb_offset.last().copied().unwrap_or(-1)
    }

    /// Go `GetOptFlag()` (`planbuilder.go:455`). `isSampling`'s "return 0" arm
    /// is a dropped narrowing; see this module's header.
    #[must_use]
    pub const fn get_opt_flag(&self) -> u64 {
        self.opt_flag
    }

    /// Go's `b.optFlag |= rule.FlagXxx`, which every clause builder does.
    pub const fn add_opt_flag(&mut self, flag: u64) {
        self.opt_flag |= flag;
    }

    fn base(&self, tp: &str) -> BaseLogicalPlan {
        BaseLogicalPlan::new(self.plan_ids, tp, self.select_offset())
    }

    /// The rewriter environment this builder hands
    /// [`crate::expression_rewriter`], which is what makes that module
    /// callable from here UNCHANGED.
    #[must_use]
    pub fn rewriter_env(&self) -> RewriterEnv<'_, C> {
        RewriterEnv {
            ctx: self.ctx,
            plan_ids: self.plan_ids,
            column_ids: self.column_ids,
            select_offset: self.select_offset(),
            flags: self.flags,
            hints: self.hints,
        }
    }

    /// Go `b.buildSubquery`'s `exprRewriterPlanCtx` fill-in: the outer scopes
    /// and the current clause, which is what makes a reference correlated.
    #[must_use]
    pub fn rewriter_plan_ctx(&self) -> ExprRewriterPlanCtx {
        ExprRewriterPlanCtx {
            cur_clause: self.cur_clause,
            outer_schemas: self.outer_schemas.clone(),
            outer_names: self.outer_names.clone(),
            // `inUpdateStmt || inDeleteStmt`; both are dropped narrowings.
            in_dml_stmt: false,
        }
    }

    /// A plan-aware [`ExpressionRewriter`] positioned at this builder's
    /// scopes. The subquery handlers hang off the returned value.
    #[must_use]
    pub fn expression_rewriter(&self) -> ExpressionRewriter<'_, C> {
        let mut rewriter = ExpressionRewriter::new(self.rewriter_env());
        rewriter.plan_ctx = self.rewriter_plan_ctx();
        rewriter
    }

    /// Rule 5 of the marker spec: clause rewriting runs over a clone the
    /// builder owns, so the caller's AST is never mutated and the build is
    /// repeatable.
    #[must_use]
    pub fn clause_scratch(expr: &Expr) -> Expr {
        expr.clone()
    }

    /// Go `b.rewrite(ctx, expr, p, mapper, asScalar)`, for the SUBQUERY-FREE
    /// case this batch's spine covers.
    ///
    /// The plan-carrying case — where the rewrite REPLACES `p` with an apply —
    /// is [`Self::expression_rewriter`]'s; that is the seam batch 6b-6e widen,
    /// and it needs no change here.
    ///
    /// # Errors
    ///
    /// The expression builder's error, or an unresolved column.
    pub fn rewrite_scalar(
        &self,
        expr: &Expr,
        schema: &Schema,
        names: &[FieldName],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<Expression, PlanError> {
        let resolver = PlanScopeResolver::new(schema, names, markers, self.time_zone.clone());
        Ok(rewrite_expr_resolved(expr, &resolver)?)
    }
}

// ***** the SELECT spine *****

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildTableDual()` (`logical_plan_builder.go:4658`).
    pub fn build_table_dual(&mut self) -> LogicalPlan {
        self.handle_helper.push_empty();
        LogicalPlan::TableDual(LogicalTableDual::new(self.base(LogicalTableDual::TYPE), 1))
    }

    /// Go `buildDataSource(ctx, tn, asName)`
    /// (`logical_plan_builder.go:4927`), over [`TableSource`].
    ///
    /// Ported: the catalogue lookup and its two errors, the column/schema/name
    /// loop (`:5195-5220`), the handle-column decision and the extra handle
    /// (`:5221-5241`), the extra commit-ts column (`:5244-5256`), the
    /// `handleHelper` push (`:5258-5260`), the `FlagGcSubstitute` and
    /// `FlagPartitionProcessor` decisions, and the access-path seeding.
    ///
    /// Marked boundaries in the body: views, sequences, table sampling and the
    /// dirty-content `UnionScan`.
    ///
    /// # Errors
    ///
    /// `ErrBadDB` for an unknown database, `ErrNoSuchTable` for an unknown
    /// table.
    pub fn build_data_source(&mut self, table_ref: &TableRef) -> Result<LogicalPlan, PlanError> {
        let (db_name, table_name) = match table_ref.name.as_slice() {
            [table] => (self.source.current_database().to_owned(), table.clone()),
            [db, table] => (db.clone(), table.clone()),
            _ => {
                return Err(PlanError::internal(format!(
                    "Unknown table '{}'",
                    table_ref.name.join(".")
                )))
            }
        };
        if !self.source.database_exists(&db_name) {
            return Err(PlanError::internal(format!("Unknown database '{db_name}'")));
        }
        let table = self
            .source
            .find_table(&db_name, &table_name)
            .ok_or_else(|| {
                PlanError::internal(format!("Table '{db_name}.{table_name}' doesn't exist"))
            })?;

        // `b.optFlag |= rule.FlagPartitionProcessor` — Go sets it from the
        // partition pruning mode; a table that reports a partition definition
        // needs the processor.
        if !table.partition_definition_names.is_empty() {
            self.opt_flag |= flags::PARTITION_PROCESSOR;
        }
        // `:5102` "Try to substitute generate column only if there is an index
        // on generate column."
        if table.indexes.iter().any(|index| {
            index.is_public
                && index.columns.iter().any(|index_column| {
                    table
                        .column_at(index_column.offset)
                        .is_some_and(|column| column.is_virtual_generated)
                })
        }) {
            self.opt_flag |= flags::GC_SUBSTITUTE;
        }

        let as_name = table_ref.alias.clone();
        let visible_table = as_name.clone().unwrap_or_else(|| table.table_name.clone());

        let mut columns = Vec::with_capacity(table.columns.len() + 2);
        let mut schema_columns = Vec::with_capacity(table.columns.len() + 2);
        let mut names = Vec::with_capacity(table.columns.len() + 2);
        let handle_cols: Vec<Column>;
        let handle_is_int: bool;

        for source_column in &table.columns {
            let name = self.table_field_name(
                &db_name,
                &table.table_name,
                &visible_table,
                &source_column.name,
                !source_column.is_public,
                source_column.is_hidden,
            );
            let mut column = Column::new(self.column_ids.alloc(), source_column.ret_type.clone());
            column.id = source_column.id;
            column.orig_name = name.display_name();
            column.is_hidden = source_column.is_hidden;
            columns.push(DataSourceColumn {
                id: source_column.id,
                name: source_column.name.clone(),
                is_primary_key: source_column.is_primary_key,
            });
            schema_columns.push(column);
            names.push(name);
        }

        // `:5221` "We append an extra handle column to the schema when the
        // handle column is not the primary key."
        if table.handle_col_offsets.is_empty() {
            let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
            ret_type
                .set_flags(ret_type.flags() | FieldTypeFlags::NOT_NULL | FieldTypeFlags::PRI_KEY);
            let mut extra = Column::new(self.column_ids.alloc(), ret_type);
            extra.id = EXTRA_HANDLE_ID;
            let name = self.table_field_name(
                &db_name,
                &table.table_name,
                &visible_table,
                EXTRA_HANDLE_NAME,
                false,
                false,
            );
            extra.orig_name = name.display_name();
            handle_cols = vec![extra.clone()];
            handle_is_int = true;
            columns.push(DataSourceColumn {
                id: EXTRA_HANDLE_ID,
                name: EXTRA_HANDLE_NAME.to_owned(),
                is_primary_key: false,
            });
            schema_columns.push(extra);
            names.push(name);
        } else {
            handle_cols = table
                .handle_col_offsets
                .iter()
                .filter_map(|offset| schema_columns.get(*offset).cloned())
                .collect();
            handle_is_int = table.handle_is_int();
        }

        // `:5244` the extra commit-ts column, appended for every non-cluster
        // table. `unfoldWildStar` (`:4115`) excludes it by column ID, which
        // [`Self::unfold_wild_star`] reproduces.
        let mut commit_ts_type = FieldType::new(FieldTypeCode::LongLong);
        commit_ts_type.set_flags(commit_ts_type.flags() | FieldTypeFlags::UNSIGNED);
        let mut commit_ts = Column::new(self.column_ids.alloc(), commit_ts_type);
        commit_ts.id = EXTRA_COMMIT_TS_ID;
        let commit_ts_name = self.table_field_name(
            &db_name,
            &table.table_name,
            &visible_table,
            EXTRA_COMMIT_TS_NAME,
            false,
            false,
        );
        commit_ts.orig_name = commit_ts_name.display_name();
        columns.push(DataSourceColumn {
            id: EXTRA_COMMIT_TS_ID,
            name: EXTRA_COMMIT_TS_NAME.to_owned(),
            is_primary_key: false,
        });
        schema_columns.push(commit_ts);
        names.push(commit_ts_name);

        let common_handle_cols: Vec<Column> = table
            .common_handle_col_offsets
            .iter()
            .filter_map(|offset| schema_columns.get(*offset).cloned())
            .collect();

        // `:5258` the handle map push, keyed by the LOGICAL table id.
        let mut handle_map = HandleColMap::new();
        handle_map.insert(
            table.table_id,
            vec![self.plan_handle_cols(&handle_cols, handle_is_int)],
        );
        self.handle_helper.push_map(handle_map);

        let mut data_source = DataSource {
            base: self.base(DataSource::TYPE),
            table_id: table.table_id,
            table_name: table.table_name.clone(),
            table_as_name: as_name,
            db_name,
            physical_table_id: table.physical_table_id,
            partition_def_idx: table.partition_def_idx,
            partition_definition_names: table.partition_definition_names.clone(),
            columns,
            pk_is_handle: table.pk_is_handle,
            handle_cols,
            handle_is_int,
            common_handle_cols,
            common_handle_lens: table.common_handle_lens.clone(),
            prefer_store_type: table.prefer_store_type,
            is_for_update_read: self.is_for_update_read,
            ..DataSource::default()
        };
        // boundary: `getPossibleAccessPaths` (`:5042`). Every constructor on
        // [`DataSourceAccessPath`] demands an already-PROVEN input — a
        // `ResolvedTableDescriptor` plus a `TiKvTableScanSpec` for a table
        // path, a `LiveIndexCandidate` with its `CountAfterAccess` for an
        // index path — because that module deliberately fails closed rather
        // than inventing statistics (see [`crate::access_path`]). Those inputs
        // come from ranger and the statistics handle, neither of which is on
        // this seam, so the path list is left EMPTY here rather than seeded
        // with an unproven path the cost model would then trust.
        debug_assert!(data_source.possible_access_paths.is_empty());

        data_source
            .base
            .base
            .set_schema(Some(Schema::new(schema_columns)));
        data_source.base.base.set_output_names(names);

        // boundary: `tableInfo.IsView()` / `IsSequence()` (`:5047`, `:5081`),
        // `tablesampler.NewTableSampleInfo` (`:5269`), `tableHasDirtyContent`
        // and the `LogicalUnionScan` it wraps (`:5312`). Each needs a handle
        // this crate does not hold; see the module boundaries.
        Ok(LogicalPlan::DataSource(data_source))
    }

    fn plan_handle_cols(&self, handle_cols: &[Column], handle_is_int: bool) -> PlanHandleCols {
        use crate::handle_cols::{CommonHandleIdentity, HandleColumnIdentity, IntHandleIdentity};
        let identities: Vec<HandleColumnIdentity> = handle_cols
            .iter()
            .map(|column| HandleColumnIdentity::new(column.id, column.unique_id, column.index))
            .collect();
        if handle_is_int {
            PlanHandleCols::Int(IntHandleIdentity::new(identities.first().cloned()))
        } else {
            PlanHandleCols::Common(CommonHandleIdentity::new(None, None, Some(identities)))
        }
    }

    fn table_field_name(
        &self,
        db_name: &str,
        original_table: &str,
        visible_table: &str,
        column_name: &str,
        not_explicit_usable: bool,
        hidden: bool,
    ) -> FieldName {
        FieldName {
            names: FieldNameMetadata {
                database: IdentifierMetadata::new(db_name),
                table: IdentifierMetadata::new(visible_table),
                original_table: IdentifierMetadata::new(original_table),
                column: IdentifierMetadata::new(column_name),
                original_column: IdentifierMetadata::new(column_name),
            },
            hidden,
            not_explicit_usable,
            redundant: false,
        }
    }

    /// Go `buildResultSetNode(ctx, node, isCTE)`
    /// (`logical_plan_builder.go:434`).
    ///
    /// Ported: the single-table arm and the join-wrapper unwrap. The DERIVED
    /// table arm (`:441-500`) pushes `outerSchemas`, hides
    /// `lateralOuterCount` entries and recurses into `buildSelect`; it is
    /// reachable here only when the subquery is a plain `SELECT`, which is
    /// what this batch builds.
    ///
    /// # Errors
    ///
    /// Any error from the arm that applies, or an unported `FROM` shape.
    pub fn build_result_set_node(&mut self, node: &JoinNode) -> Result<LogicalPlan, PlanError> {
        match node {
            JoinNode::Table(table_ref) => self.build_data_source(table_ref),
            // Both the single-table wrapper (`Join{Right: nil}`) and a real
            // join go to `buildJoin` (`:736`), which unwraps the former
            // itself.
            JoinNode::Join(join) => self.build_join(join),
            JoinNode::Derived {
                subquery,
                alias,
                lateral,
                column_names,
            } => self.build_derived_source(
                subquery.as_ref(),
                alias.as_deref(),
                *lateral,
                column_names,
            ),
        }
    }

    /// Go `buildSelection(ctx, p, where, aggMapper)`
    /// (`logical_plan_builder.go:1343`).
    ///
    /// # Errors
    ///
    /// The expression build error for any conjunct.
    pub fn build_selection(
        &mut self,
        plan: LogicalPlan,
        where_clause: &Expr,
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<LogicalPlan, PlanError> {
        self.opt_flag |= flags::PREDICATE_PUSH_DOWN;
        if self.cur_clause != ClauseCode::Having {
            self.cur_clause = ClauseCode::Where;
        }
        // Rule 3: both snapshots are taken before `plan` moves anywhere.
        let (schema, names) = snapshot_schema_and_names(&plan);

        let mut conditions = Vec::new();
        // Go `splitWhere(where)` splits the AST's top-level `AND` first, then
        // `SplitCNFItems` splits the built expression; the second subsumes the
        // first once every conjunct is built, so one clause is rewritten here
        // and split afterwards.
        let scratch = Self::clause_scratch(where_clause);
        let built = self.rewrite_scalar(&scratch, &schema, &names, markers)?;

        for item in split_cnf_items(&built) {
            if let Expression::Constant(constant) = &item {
                match constant_is_always_false(constant) {
                    // "If there is condition which is always false, return
                    // dual plan directly." (`:1381`)
                    Some(true) => {
                        let mut dual = LogicalTableDual::new(self.base(LogicalTableDual::TYPE), 0);
                        dual.base.base.set_schema(Some(schema));
                        dual.base.base.set_output_names(names);
                        return Ok(LogicalPlan::TableDual(dual));
                    }
                    // An always-true conjunct is dropped.
                    Some(false) => continue,
                    // Not decidable at plan time: keep it. Go's `useCache` arm.
                    None => {}
                }
            }
            conditions.push(item);
        }
        if conditions.is_empty() {
            return Ok(plan);
        }
        let mut selection = LogicalSelection::new(self.base(LogicalSelection::TYPE), conditions);
        selection.base.set_children(vec![plan]);
        Ok(LogicalPlan::Selection(selection))
    }

    /// Go `unfoldWildStar(field, outputName, column)`
    /// (`logical_plan_builder.go:4115`), harvested from `tidb-executor`'s
    /// `driver.rs:2023-2050`.
    ///
    /// A hidden column, and any of the three EXTRA columns, is skipped by ID —
    /// which is why [`Self::build_data_source`] may append them without
    /// widening `SELECT *`.
    #[must_use]
    pub fn unfold_wild_star(
        wildcard: &[String],
        schema: &Schema,
        names: &[FieldName],
    ) -> Vec<ProjectionField> {
        let (db, table) = match wildcard {
            [table] => (None, Some(table.as_str())),
            [db, table] => (Some(db.as_str()), Some(table.as_str())),
            _ => (None, None),
        };
        let mut fields = Vec::new();
        for (index, name) in names.iter().enumerate() {
            let Some(column) = schema.columns.get(index) else {
                continue;
            };
            if column.is_hidden
                || column.id == EXTRA_HANDLE_ID
                || column.id == EXTRA_PHYS_TBL_ID
                || column.id == EXTRA_COMMIT_TS_ID
            {
                continue;
            }
            if db.is_some_and(|d| !name.names.database.lower.eq_ignore_ascii_case(d))
                || table.is_some_and(|t| !name.names.table.lower.eq_ignore_ascii_case(t))
            {
                continue;
            }
            let mut path = Vec::new();
            if !name.names.table.original.is_empty() {
                if !name.names.database.original.is_empty() {
                    path.push(name.names.database.original.clone());
                }
                path.push(name.names.table.original.clone());
            }
            path.push(name.names.column.original.clone());
            fields.push(ProjectionField {
                expr: Expr::Column(path),
                alias: None,
                text: Some(name.names.column.original.clone()),
                hidden: false,
            });
        }
        fields
    }

    /// Go `buildProjectionField(ctx, p, field, expr)`
    /// (`logical_plan_builder.go:1535`): the output NAME one select field
    /// produces.
    fn projection_field_name(
        field: &ProjectionField,
        names: &[FieldName],
        resolved_index: Option<usize>,
    ) -> FieldName {
        // `:1537` "Field is a column reference": the origin names survive, and
        // only `ColName` takes the alias.
        if let (Expr::Column(_), Some(index)) = (&field.expr, resolved_index) {
            if let Some(origin) = names.get(index) {
                let mut name = origin.clone();
                if let Some(alias) = &field.alias {
                    name.names.column = IdentifierMetadata::new(alias);
                }
                name.redundant = false;
                name.hidden = field.hidden;
                return name;
            }
        }
        // `:1560` otherwise the name is the alias, else the field's own SOURCE
        // TEXT (Go `field.Text()`), else the restored expression
        // (`buildProjectionFieldNameFromExpressions`, `:1445`).
        let column_name = field
            .alias
            .clone()
            .or_else(|| field.text.clone())
            .unwrap_or_else(|| field.expr.restore());
        let mut name = FieldName::new(FieldNameMetadata {
            column: IdentifierMetadata::new(column_name),
            ..FieldNameMetadata::default()
        });
        name.hidden = field.hidden;
        name
    }

    /// Go `buildProjection`'s wildcard pass (`:1790`), plus the per-field
    /// source text `buildProjectionFieldNameFromExpressions` reads.
    #[must_use]
    pub fn expand_fields(
        fields: &tidb_ast::SelectFieldList,
        schema: &Schema,
        names: &[FieldName],
    ) -> Vec<ProjectionField> {
        let mut expanded = Vec::with_capacity(fields.fields().len());
        for (index, field) in fields.fields().iter().enumerate() {
            match field {
                SelectField::Wildcard(path) => {
                    expanded.extend(Self::unfold_wild_star(path, schema, names));
                }
                SelectField::Expr { expr, alias } => expanded.push(ProjectionField {
                    expr: expr.clone(),
                    alias: alias.clone(),
                    text: fields
                        .text(index)
                        .and_then(|bytes| std::str::from_utf8(bytes).ok())
                        .map(str::to_owned)
                        .filter(|text| !text.is_empty()),
                    hidden: false,
                }),
            }
        }
        expanded
    }

    /// Go `resolveHavingAndOrderBy`'s ORDER BY half
    /// (`logical_plan_builder.go:2913`, over
    /// `havingWindowAndOrderbyExprResolver` at `:2723`), narrowed to the
    /// clauses this batch builds: no aggregate, no window, no HAVING.
    ///
    /// Harvested from `tidb-executor`'s `driver/clause_resolve.rs`, whose
    /// `resolveFromSelectFields` port already gets the PRECEDENCE right:
    ///
    /// 1. an unqualified name matching a select-list ALIAS wins;
    /// 2. then a select-list field that IS that column;
    /// 3. then the source scope — and a source column ORDER BY names but the
    ///    select list does not project becomes a HIDDEN extra projection
    ///    column, which `buildSelect`'s `:4640` trailing projection trims off.
    ///
    /// Each resolved item is rewritten to a [`marker`] in place of the
    /// resolved node: [`MarkerKind::Column`] for cases 1 and 2 (Go's
    /// `colMapper`), [`MarkerKind::OrderBy`] for case 3 (Go's `orderMap`).
    /// Both index the PROJECTION's schema, so [`Self::build_sort`] binds them
    /// with one map.
    #[must_use]
    pub fn resolve_order_by(
        items: &[tidb_ast::OrderItem],
        fields: &mut Vec<ProjectionField>,
    ) -> Vec<Expr> {
        let old_len = fields.len();
        let mut resolved = Vec::with_capacity(items.len());
        for item in items {
            let mut expr = item.expr.clone();
            // A bare integer is a POSITION, handled by `build_sort` against
            // the projection's own schema; leave it alone here.
            if Self::order_by_position(&expr).is_some() {
                resolved.push(expr);
                continue;
            }
            if let Some(index) = Self::find_in_select_fields(&expr, &fields[..old_len]) {
                marker::substitute(&mut expr, PlanMarker::new(MarkerKind::Column, index));
                resolved.push(expr);
                continue;
            }
            // Case 3: it must come from the source. An identical extra field
            // is reused rather than appended twice, as Go's map key does.
            let extra = match fields[old_len..]
                .iter()
                .position(|field| field.expr == expr)
            {
                Some(position) => position,
                None => {
                    fields.push(ProjectionField {
                        expr: expr.clone(),
                        alias: None,
                        text: None,
                        hidden: true,
                    });
                    fields.len() - 1 - old_len
                }
            };
            marker::substitute(
                &mut expr,
                PlanMarker::new(MarkerKind::OrderBy, old_len + extra),
            );
            resolved.push(expr);
        }
        resolved
    }

    fn find_in_select_fields(expr: &Expr, fields: &[ProjectionField]) -> Option<usize> {
        if let Expr::Column(path) = expr {
            if let [name] = path.as_slice() {
                // 1. the alias.
                if let Some(index) = fields.iter().position(|field| {
                    field
                        .alias
                        .as_deref()
                        .is_some_and(|a| a.eq_ignore_ascii_case(name))
                }) {
                    return Some(index);
                }
                // 2. a select-list field that IS that column.
                if let Some(index) = fields.iter().position(|field| {
                    field.alias.is_none()
                        && matches!(&field.expr, Expr::Column(p) if p.last().is_some_and(|c| c.eq_ignore_ascii_case(name)))
                }) {
                    return Some(index);
                }
                return None;
            }
        }
        // A non-column ORDER BY term that is textually the same expression as
        // a select field reuses that field's column instead of recomputing it.
        fields.iter().position(|field| &field.expr == expr)
    }

    /// Go `buildProjection(ctx, p, fields, mapper, windowMapper, ...)`
    /// (`logical_plan_builder.go:1767`), returning the projection and the
    /// expressions Go's second result carries.
    ///
    /// Narrowed from Go's six-argument form: `considerWindow` and
    /// `expandGenerateColumn` gate batch 6e and the generated-column
    /// substitution respectively. Go's third result `oldLen` is the caller's
    /// here, because it is the caller ([`Self::resolve_order_by`]) that
    /// appends the hidden fields past it.
    ///
    /// # Errors
    ///
    /// The expression build error for any field.
    pub fn build_projection(
        &mut self,
        plan: LogicalPlan,
        fields: &[ProjectionField],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<(LogicalPlan, Vec<Expression>), PlanError> {
        self.opt_flag |= flags::ELIMINATE_PROJECTION;
        self.cur_clause = ClauseCode::FieldList;
        let (schema, names) = snapshot_schema_and_names(&plan);
        // Go `b.allNames = append(b.allNames, p.OutputNames())` (`:1782`),
        // which `evalDefaultExpr` later searches.
        self.all_names.push(names.clone());

        let mut exprs = Vec::with_capacity(fields.len());
        let mut projection_columns = Vec::with_capacity(fields.len());
        let mut projection_names = Vec::with_capacity(fields.len());
        for field in fields {
            let scratch = Self::clause_scratch(&field.expr);
            let built = self.rewrite_scalar(&scratch, &schema, &names, markers)?;
            let resolved_index = match &built {
                Expression::Column(column) => usize::try_from(column.index).ok(),
                _ => None,
            };
            let name = Self::projection_field_name(field, &names, resolved_index);
            let ret_type = built
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            let mut output = Column::new(self.column_ids.alloc(), ret_type);
            output.orig_name = name.display_name();
            output.is_hidden = field.hidden;
            projection_columns.push(output);
            projection_names.push(name);
            exprs.push(built);
        }

        let mut projection =
            LogicalProjection::new(self.base(LogicalProjection::TYPE), exprs.clone());
        projection.base.set_children(vec![plan]);
        projection
            .base
            .base
            .set_schema(Some(Schema::new(projection_columns)));
        projection.base.base.set_output_names(projection_names);
        Ok((LogicalPlan::Projection(projection), exprs))
    }

    /// Go `buildSelect`'s trailing projection (`logical_plan_builder.go:4640`):
    /// "if oldLen != p.Schema().Len()", trim the hidden ORDER BY / HAVING
    /// columns back off the output.
    fn build_trim_projection(&mut self, plan: LogicalPlan, old_len: usize) -> LogicalPlan {
        let (schema, names) = snapshot_schema_and_names(&plan);
        let exprs: Vec<Expression> = schema
            .columns
            .iter()
            .take(old_len)
            .enumerate()
            .map(|(index, column)| {
                let mut column = column.clone();
                column.index = index as i64;
                Expression::Column(column)
            })
            .collect();
        let kept_columns: Vec<Column> = schema.columns.into_iter().take(old_len).collect();
        let kept_names: Vec<FieldName> = names.into_iter().take(old_len).collect();
        let mut projection = LogicalProjection::new(self.base(LogicalProjection::TYPE), exprs);
        projection.base.set_children(vec![plan]);
        projection
            .base
            .base
            .set_schema(Some(Schema::new(kept_columns)));
        projection.base.base.set_output_names(kept_names);
        LogicalPlan::Projection(projection)
    }

    /// Go `buildSort(ctx, p, byItems, aggMapper, windowMapper)`
    /// (`logical_plan_builder.go:2399`).
    ///
    /// `itemTransformer` (`:2380`) turns a bare integer into a select-list
    /// POSITION; that is ported here as [`Self::order_by_position`], since a
    /// positional ORDER BY is otherwise silently a constant.
    ///
    /// # Errors
    ///
    /// The expression build error for any item, or an out-of-range position.
    pub fn build_sort(
        &mut self,
        plan: LogicalPlan,
        items: &[tidb_ast::OrderItem],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<LogicalPlan, PlanError> {
        self.cur_clause = ClauseCode::OrderBy;
        let (schema, names) = snapshot_schema_and_names(&plan);

        let mut by_items = Vec::with_capacity(items.len());
        for item in items {
            let scratch = Self::clause_scratch(&item.expr);
            let built = match Self::order_by_position(&scratch) {
                Some(position) => {
                    let column = schema.columns.get(position - 1).ok_or_else(|| {
                        PlanError::internal(format!(
                            "Unknown column '{position}' in 'order clause'"
                        ))
                    })?;
                    let mut column = column.clone();
                    column.index = position as i64 - 1;
                    Expression::Column(column)
                }
                None => self.rewrite_scalar(&scratch, &schema, &names, markers)?,
            };
            by_items.push(ByItems::new(built, item.desc));
        }

        let mut sort = LogicalSort::new(self.base(LogicalSort::TYPE), by_items);
        sort.base.set_children(vec![plan]);
        Ok(LogicalPlan::Sort(sort))
    }

    /// Go `itemTransformer.Leave` (`logical_plan_builder.go:2380`): a bare
    /// integer literal in ORDER BY is a 1-based select-list position.
    #[must_use]
    pub fn order_by_position(expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Int(digits) => digits.parse().ok(),
            _ => None,
        }
    }

    /// Go `buildLimit(src, limit)` (`logical_plan_builder.go:2569`).
    ///
    /// # Errors
    ///
    /// A `LIMIT` whose operand is not a non-negative integer literal — Go's
    /// `extractLimitCountOffset` raises `ErrWrongArguments` there, and a
    /// parameter marker needs the execute-time binding this crate has no
    /// access to.
    pub fn build_limit(
        &mut self,
        plan: LogicalPlan,
        limit: &Limit,
    ) -> Result<LogicalPlan, PlanError> {
        self.opt_flag |= flags::PUSH_DOWN_TOPN;
        let offset = match &limit.offset {
            Some(expr) => Self::limit_value(expr)?,
            None => 0,
        };
        let mut count = Self::limit_value(&limit.count)?;
        // `:2582` "If `offset+count` overflows uint64, we should use the max
        // value."
        if count > u64::MAX - offset {
            count = u64::MAX - offset;
        }
        if offset.saturating_add(count) == 0 {
            // `:2588` an empty limit becomes a zero-row dual carrying the
            // source's schema. Rule 3 applies.
            let (schema, names) = snapshot_schema_and_names(&plan);
            let mut dual = LogicalTableDual::new(self.base(LogicalTableDual::TYPE), 0);
            dual.base.base.set_schema(Some(schema));
            dual.base.base.set_output_names(names);
            return Ok(LogicalPlan::TableDual(dual));
        }
        let mut logical_limit = LogicalLimit::new(self.base(LogicalLimit::TYPE), offset, count);
        logical_limit.base.set_children(vec![plan]);
        Ok(LogicalPlan::Limit(logical_limit))
    }

    fn limit_value(expr: &Expr) -> Result<u64, PlanError> {
        match expr {
            Expr::Int(digits) => digits
                .parse()
                .map_err(|_| PlanError::internal("Incorrect arguments to LIMIT")),
            _ => Err(PlanError::internal(
                "Incorrect arguments to LIMIT: only an integer literal is ported",
            )),
        }
    }

    /// Go `buildSelect(ctx, sel)` (`logical_plan_builder.go:4254`), on the
    /// FROM / WHERE / SELECT / ORDER BY / LIMIT spine.
    ///
    /// Returns the plan and [`Self::get_opt_flag`], which is what
    /// `logicalOptimize` takes beside the plan; see this module's section 4.
    ///
    /// The clause ORDER is Go's, and is load-bearing: the projection is built
    /// BEFORE the sort, so `ORDER BY` sees the projection's output names — the
    /// three-clause precedence `tidb-executor`'s `driver/clause_resolve.rs`
    /// gets right and which 6c must port in full together with HAVING.
    ///
    /// # Errors
    ///
    /// Any clause's error, or an unported clause (GROUP BY, HAVING, WINDOW,
    /// DISTINCT, locking, `INTO OUTFILE`), each naming its Go symbol.
    pub fn build_select(&mut self, select: &SelectStmt) -> Result<(LogicalPlan, u64), PlanError> {
        for (clause, present, go_symbol) in [
            (
                "GROUP BY",
                !select.group_by.is_empty(),
                "buildAggregation (logical_plan_builder.go:212)",
            ),
            (
                "HAVING",
                select.having.is_some(),
                "resolveHavingAndOrderBy (logical_plan_builder.go:2913)",
            ),
            (
                "WINDOW",
                !select.windows.is_empty(),
                "buildWindowFunctions (logical_plan_builder.go:6893)",
            ),
            (
                "DISTINCT",
                select.distinct,
                "buildDistinct (logical_plan_builder.go:1966)",
            ),
            (
                "WITH",
                select.with.is_some(),
                "buildWith (logical_plan_builder.go:7900)",
            ),
        ] {
            if present {
                return Err(PlanError::internal(format!(
                    "{clause} is a later batch: {go_symbol}"
                )));
            }
        }

        // `:4356` FROM.
        // `buildTableRefs` (`:420`) is the FROM clause's own entry point; the
        // `None` arm is its `buildTableDual`.
        let mut plan = self.build_table_refs(select.from.as_ref())?;
        // `:4394` WHERE. No marker is bound yet: the passes that PRODUCE one
        // (aggregation, window) are later batches, and ORDER BY's markers are
        // produced below, after the select list is known.
        let no_markers = BTreeMap::new();
        if let Some(where_clause) = &select.where_clause {
            plan = self.build_selection(plan, where_clause, &no_markers)?;
        }

        // `:4460` resolveHavingAndOrderBy, then `:4520` the select list.
        let (source_schema, source_names) = snapshot_schema_and_names(&plan);
        let mut fields = Self::expand_fields(&select.fields, &source_schema, &source_names);
        let old_len = fields.len();
        let order_by = Self::resolve_order_by(&select.order_by, &mut fields);

        let (projected, _) = self.build_projection(plan, &fields, &no_markers)?;
        plan = projected;

        // Both ORDER BY marker kinds index the projection's schema, so one map
        // binds them; see [`Self::resolve_order_by`].
        let projection_columns = plan
            .schema()
            .map(|schema| schema.columns.clone())
            .unwrap_or_default();
        let mut markers = BTreeMap::new();
        markers.insert(MarkerKind::Column, projection_columns.clone());
        markers.insert(MarkerKind::OrderBy, projection_columns);

        // `:4600` ORDER BY, then `:4620` LIMIT.
        if !order_by.is_empty() {
            let items: Vec<tidb_ast::OrderItem> = order_by
                .into_iter()
                .zip(&select.order_by)
                .map(|(expr, original)| tidb_ast::OrderItem {
                    expr,
                    desc: original.desc,
                })
                .collect();
            plan = self.build_sort(plan, &items, &markers)?;
        }
        if let Some(limit) = &select.limit {
            plan = self.build_limit(plan, limit)?;
        }
        // `:4640` trim the hidden ORDER BY columns back off.
        if fields.len() != old_len {
            plan = self.build_trim_projection(plan, old_len);
        }
        Ok((plan, self.get_opt_flag()))
    }
}
