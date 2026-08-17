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

//! Go `pkg/planner/core/operator/logicalop/logical_apply.go`: `LogicalApply`,
//! the correlated join that "gets one row from outer executor and gets one row
//! from inner executor according to outer row".
//!
//! SEED of `pkg/planner/core`. `LogicalApply` was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! # Built ON [`LogicalJoin`], not beside it
//!
//! Go writes `LogicalApply { LogicalJoin; CorCols; NoDecorrelate; IsLateral }`
//! and INHERITS the whole join surface: `ExplainInfo` is
//! `la.LogicalJoin.ExplainInfo()`, `ExtractCorrelatedCols` starts from
//! `la.LogicalJoin.ExtractCorrelatedCols()`, `PruneColumns` calls the join's
//! `ExtractUsedCols` and `MergeSchema`, and `CanPullUpAgg` reads the join's
//! four condition buckets. That embedding is reproduced by
//! [`LogicalApply::join`]; nothing from `logical_join.go` is restated here.
//!
//! # Narrowings, by name
//!
//! * `PruneColumns`' apply-elimination is gated on
//!   `fixcontrol.GetBoolWithDefault(..., fixcontrol.Fix45822, true)`, which
//!   needs the session's fix-control map. The gate is a PARAMETER of
//!   [`LogicalApply::can_eliminate_apply`]; this crate's
//!   [`crate::fix_control`] carries the fix id but not the session map.
//! * `PruneColumns` also calls
//!   `coreusage.ExtractCorColumnsBySchema4LogicalPlan(inner, outerSchema)`,
//!   which walks the whole inner subtree; that walk is the driver's, and the
//!   local half is [`LogicalApply::prune_columns_local`].
//! * `DeriveStats`' two LATERAL estimates are
//!   `cardinality.EstimateFullJoinRowCount` and
//!   `cardinality.EstimateColsNDVWithMatchedLen`, both of which need the
//!   session and both histogram collections. The caller resolves them; see
//!   [`LogicalApply::needs_lateral_row_count_estimate`], which reports when a
//!   caller MUST supply one rather than letting the Cartesian fallback answer.
//! * `ExtractColGroups` needs `Schema.ExtractColGroups`, which `tidb-expr`
//!   lists as deferred; the join-type gate that decides whether ANY group can
//!   be extracted is ported as [`LogicalApply::col_groups_outer_side`].
//! * `ExtractFD` needs `pkg/planner/funcdep`; see the
//!   [`crate::logical::BaseLogicalPlan`] header.
//! * `ReplaceExprColumns` needs `ruleutil.ResolveExprAndReplace`.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::{Column, CorrelatedColumn};
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;

use crate::find_best_task::LogicalJoinType;
use crate::logical::join::LogicalJoin;
use crate::logical::selection::SELECTION_FACTOR;
use crate::logical::{BaseLogicalPlan, LogicalPlan};
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalApply` (`logical_apply.go:37`).
#[derive(Clone, Debug, Default)]
pub struct LogicalApply {
    /// Go's embedded `LogicalJoin`; see this module's header.
    pub join: LogicalJoin,
    /// Go `CorCols`: the outer columns the inner subtree reads, recomputed by
    /// `PruneColumns` from the inner plan against the outer schema.
    pub cor_cols: Vec<CorrelatedColumn>,
    /// Go `NoDecorrelate`, from the `/*+ no_decorrelate() */` hint.
    pub no_decorrelate: bool,
    /// Go `IsLateral`: this apply came from a `LATERAL` join rather than from a
    /// scalar correlated subquery.
    ///
    /// The distinction is load-bearing twice over, both times because a
    /// `LATERAL` inner may return MANY rows per outer row while a scalar
    /// subquery returns at most one: it forbids apply elimination
    /// ([`Self::can_eliminate_apply`]) and it changes the row-count estimate
    /// ([`Self::derive_stats`]).
    pub is_lateral: bool,
}

impl LogicalApply {
    /// Go `plancodec.TypeApply`.
    pub const TYPE: &'static str = "Apply";

    /// Go `LogicalApply.Init(ctx, offset)` (`logical_apply.go:50`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, join_type: LogicalJoinType) -> Self {
        Self {
            join: LogicalJoin::new(base, join_type),
            cor_cols: Vec::new(),
            no_decorrelate: false,
            is_lateral: false,
        }
    }

    /// The shared logical base, reached through the embedded join.
    #[must_use]
    pub const fn base(&self) -> &BaseLogicalPlan {
        &self.join.base
    }

    /// The shared logical base, mutably.
    pub const fn base_mut(&mut self) -> &mut BaseLogicalPlan {
        &mut self.join.base
    }

    /// Go `LogicalApply.ExplainInfo()` (`logical_apply.go:57`): the join's,
    /// verbatim.
    #[must_use]
    pub fn explain_info(&self) -> String {
        self.join.explain_info()
    }

    /// Go `LogicalApply.ExtractCorrelatedCols()` (`logical_apply.go:250`): the
    /// join's correlated columns MINUS the ones the outer child already
    /// produces.
    ///
    /// A correlated column that the outer side supplies is resolved BY this
    /// apply, so it is not correlated as far as this apply's own parent is
    /// concerned; only the ones that reach further out survive.
    #[must_use]
    pub fn extract_correlated_cols(&self, outer_schema: &Schema) -> Vec<CorrelatedColumn> {
        let mut cor_cols = self.join.extract_correlated_cols();
        cor_cols.retain(|col| !outer_schema.contains(&col.column));
        cor_cols
    }

    /// Go's apply-elimination test inside `PruneColumns`
    /// (`logical_apply.go:110`): a `LEFT OUTER` apply whose INNER side
    /// contributes no column at all is just its outer child.
    ///
    /// Go's own emphasis, kept because getting it wrong is a wrong answer and
    /// not a slow one: this holds only for a scalar correlated subquery, which
    /// carries a max-one-row guarantee. A `LATERAL` inner may return many rows
    /// per outer row, so eliminating the apply would change the MULTIPLICITY
    /// of the result — a wrong `COUNT(*)`, a wrong aggregate.
    ///
    /// `allow_eliminate_apply` is `fixcontrol.Fix45822`, default true; see this
    /// module's header.
    #[must_use]
    pub fn can_eliminate_apply(&self, allow_eliminate_apply: bool, right_cols_empty: bool) -> bool {
        allow_eliminate_apply
            && !self.is_lateral
            && right_cols_empty
            && matches!(self.join.join_type, LogicalJoinType::LeftOuter)
    }

    /// Go `LogicalApply.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_apply.go:103`): split the parent's set across the two
    /// children, which is the join's `ExtractUsedCols`.
    ///
    /// The driver then prunes the INNER child with `right_cols`, recomputes
    /// [`Self::cor_cols`] against the outer schema, appends each correlated
    /// column to `left_cols`, prunes the OUTER child, and re-merges the schema
    /// — in that order, because the inner's pruning is what determines which
    /// outer columns are still correlated.
    #[must_use]
    pub fn prune_columns_local(
        &self,
        parent_used_cols: &[Column],
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> (Vec<Column>, Vec<Column>) {
        self.join
            .extract_used_cols(parent_used_cols, left_schema, right_schema)
    }

    /// Go's `for _, col := range la.CorCols { leftCols = append(...) }`
    /// (`logical_apply.go:130`): every correlated column is also a column the
    /// OUTER child must still produce.
    #[must_use]
    pub fn widen_outer_used_cols(&self, left_cols: &mut Vec<Column>) -> usize {
        let before = left_cols.len();
        left_cols.extend(self.cor_cols.iter().map(|cor| cor.column.clone()));
        left_cols.len() - before
    }

    /// Go `LogicalApply.DeriveStats(childStats, selfSchema, childSchema,
    /// reloads)` (`logical_apply.go:157`).
    ///
    /// The row count is the OUTER child's, because a scalar correlated
    /// subquery yields at most one inner row per outer row. Two things move
    /// off that baseline:
    /// * a semi or anti-semi apply — an `EXISTS`/`NOT EXISTS` that could not be
    ///   decorrelated — is scaled by [`SELECTION_FACTOR`], matching
    ///   `LogicalJoin::derive_stats`;
    /// * a `LATERAL` inner or outer apply takes `lateral_row_count`, floored at
    ///   the outer count for a left outer join.
    ///
    /// The NDVs are the outer child's, plus one entry per column the INNER side
    /// contributes: `2.0` for the marker of a left-outer-semi apply, and the
    /// row count for every other inner column.
    ///
    /// # Blocked
    ///
    /// `lateral_row_count` stands for Go's first two `IsLateral` branches; see
    /// this module's header and [`Self::needs_lateral_row_count_estimate`].
    /// `None` selects Go's THIRD branch, `leftProfile.RowCount *
    /// rightProfile.RowCount` — the Cartesian bound Go itself takes when the
    /// apply has neither join keys nor correlated columns, because
    /// decorrelation will turn it into a plain cross join.
    ///
    /// `getGroupNDVs` is vacuous: [`StatsInfo`] has no `GroupNDVs` field.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        outer_schema_len: usize,
        lateral_row_count: Option<f64>,
        reloads: &[bool],
    ) -> Option<(StatsInfo, bool)> {
        let reload = reloads.iter().any(|one| *one);
        if !reload {
            if let Some(existing) = self.join.base.base.stats_info() {
                return Some((existing.clone(), false));
            }
        }
        let left = child_stats.first()?;
        let right = child_stats.get(1)?;
        let mut row_count = left.row_count();
        if self.is_lateral
            && matches!(
                self.join.join_type,
                LogicalJoinType::Inner | LogicalJoinType::LeftOuter
            )
        {
            row_count = lateral_row_count.unwrap_or_else(|| left.row_count() * right.row_count());
            if matches!(self.join.join_type, LogicalJoinType::LeftOuter) {
                row_count = row_count.max(left.row_count());
            }
        } else if matches!(
            self.join.join_type,
            LogicalJoinType::Semi | LogicalJoinType::AntiSemi
        ) {
            row_count *= SELECTION_FACTOR;
        }
        let mut ndvs: Vec<(i64, f64)> = left
            .col_ndvs()
            .iter()
            .map(|(id, ndv)| (*id, *ndv))
            .collect();
        if matches!(
            self.join.join_type,
            LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi
        ) {
            if let Some(marker) = self_schema.columns.last() {
                ndvs.retain(|(id, _)| *id != marker.unique_id);
                ndvs.push((marker.unique_id, 2.0));
            }
        } else {
            for column in self_schema.columns.iter().skip(outer_schema_len) {
                ndvs.retain(|(id, _)| *id != column.unique_id);
                ndvs.push((column.unique_id, row_count));
            }
        }
        let stats = StatsInfo::new(row_count, ndvs);
        self.join.base.base.set_stats(Some(stats.clone()));
        Some((stats, true))
    }

    /// Whether [`Self::derive_stats`] MUST be given a `lateral_row_count`.
    ///
    /// True exactly when Go takes one of the two estimator branches — explicit
    /// `ON`-clause join keys, or a correlated inner — rather than the
    /// dependency-closed Cartesian fallback. A caller that cannot supply the
    /// estimate is choosing an over-estimate, and this says so out loud.
    #[must_use]
    pub fn needs_lateral_row_count_estimate(&self) -> bool {
        if !self.is_lateral
            || !matches!(
                self.join.join_type,
                LogicalJoinType::Inner | LogicalJoinType::LeftOuter
            )
        {
            return false;
        }
        !self.join.equal_conditions.is_empty() || !self.cor_cols.is_empty()
    }

    /// Go's join-type gate inside `ExtractColGroups`
    /// (`logical_apply.go:228`): only an apply that PRESERVES its outer side
    /// can pass a column group down, and the group is always resolved against
    /// the OUTER child.
    ///
    /// Go's comment: "Apply doesn't have RightOuterJoin." Returning `false`
    /// here is Go returning `nil`.
    #[must_use]
    pub const fn col_groups_outer_side(&self) -> bool {
        matches!(
            self.join.join_type,
            LogicalJoinType::LeftOuter
                | LogicalJoinType::LeftOuterSemi
                | LogicalJoinType::AntiLeftOuterSemi
        )
    }

    /// Go `LogicalApply.CanPullUpAgg()` (`logical_apply.go:305`): an
    /// aggregation may be pulled above this apply only when the apply is a
    /// plain inner or left-outer join with NO conditions at all and the outer
    /// side has a key.
    ///
    /// The key is what makes the pull-up sound: without it, the outer rows the
    /// aggregation would group by are not distinguishable.
    #[must_use]
    pub fn can_pull_up_agg(&self, outer_schema: &Schema) -> bool {
        if !matches!(
            self.join.join_type,
            LogicalJoinType::Inner | LogicalJoinType::LeftOuter
        ) {
            return false;
        }
        let conditions = self.join.equal_conditions.len()
            + self.join.left_conditions.len()
            + self.join.right_conditions.len()
            + self.join.other_conditions.len();
        if conditions > 0 {
            return false;
        }
        !outer_schema.pk_or_uk.is_empty()
    }

    /// Go `LogicalApply.DeCorColFromEqExpr(expr)` (`logical_apply.go:316`):
    /// rewrite `col = correlated col` (in either argument order) into a plain
    /// `correlated = col` equality once the correlated side resolves against
    /// this apply's own schema.
    ///
    /// The result is normalised so the LEFT argument is the join's LEFT key,
    /// which is what makes it usable as an equal condition. `None` means the
    /// expression is not of that form, or the correlated column did NOT
    /// resolve — Go's `if _, ok := ret.(*expression.CorrelatedColumn); ok`
    /// escape, which fires when `Decorrelate` handed the column straight back.
    ///
    /// # Narrowing
    ///
    /// Go builds the result with
    /// `expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), ret, col)`,
    /// which folds constants and infers the type through an `ExprContext`.
    /// There is none here, so the node is built directly with the same `Tiny`
    /// result type Go asks for; no folding happens, which can only leave the
    /// expression less simplified, never differently shaped.
    #[must_use]
    pub fn de_cor_col_from_eq_expr(
        &self,
        expr: &Expression,
        schema: &Schema,
    ) -> Option<Expression> {
        let Expression::ScalarFunction(function) = expr else {
            return None;
        };
        if function.func_name.lowercase() != "eq" {
            return None;
        }
        let args = function.get_args();
        let (column, cor_col) = match (args.first()?, args.get(1)?) {
            (Expression::Column(column), Expression::CorrelatedColumn(cor)) => (column, cor),
            (Expression::CorrelatedColumn(cor), Expression::Column(column)) => (column, cor),
            _ => return None,
        };
        let decorrelated = cor_col.decorrelate(schema);
        if matches!(decorrelated, Expression::CorrelatedColumn(_)) {
            return None;
        }
        Some(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![decorrelated, Expression::Column(column.clone())],
        )))
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            join: self.join.clone_shallow(),
            cor_cols: self.cor_cols.clone(),
            no_decorrelate: self.no_decorrelate,
            is_lateral: self.is_lateral,
        }
    }
}

/// Go `findChildFullSchema(p)` (`logical_apply.go:84`): the `FullSchema` of the
/// nearest `LogicalJoin` or `LogicalApply` below `plan`, seeing through the
/// `LogicalSelection`s an `ON` clause leaves behind.
///
/// `PruneColumns` needs this because a `USING`/`NATURAL` join HIDES its
/// redundant columns from `Schema()`; without the full schema, a `LATERAL` over
/// such a join would fail to see that the inner references one of them and
/// would lose the correlation.
///
/// Written as a loop, not a recursion — see the [`crate::logical`] header.
#[must_use]
pub fn find_child_full_schema(plan: &LogicalPlan) -> Option<&Schema> {
    let mut current = plan;
    loop {
        match current {
            LogicalPlan::Join(join) => return join.full_schema.as_ref(),
            LogicalPlan::Apply(apply) => return apply.join.full_schema.as_ref(),
            LogicalPlan::Selection(_) => {
                let children = current.children();
                if children.len() != 1 {
                    return None;
                }
                current = &children[0];
            }
            // Go's `default:` arm. Spelled out rather than as `_`, because the
            // keystone's rule is that a NEW operator must be a compile error
            // here and not a silent `nil`.
            LogicalPlan::Projection(_)
            | LogicalPlan::Aggregation(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::TopN(_)
            | LogicalPlan::UnionAll(_)
            | LogicalPlan::PartitionUnionAll(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::CTE(_)
            | LogicalPlan::CTETable(_)
            | LogicalPlan::MaxOneRow(_)
            | LogicalPlan::Lock(_)
            | LogicalPlan::Sequence(_)
            | LogicalPlan::UnionScan(_)
            | LogicalPlan::TiKVSingleGather(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::IndexScan(_)
            | LogicalPlan::DataSource(_)
            | LogicalPlan::TableDual(_)
            | LogicalPlan::Todo(_) => return None,
        }
    }
}
