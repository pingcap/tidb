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

//! Go `pkg/planner/core/operator/logicalop/logical_join.go`: `LogicalJoin`,
//! the two-child operator that carries the whole `ON`/`USING` condition
//! classification.
//!
//! SEED of `pkg/planner/core`: the operator state, its condition
//! classification (`ExtractOnCondition`, `AttachOnConds`, `AppendJoinConds`),
//! its key derivation, its statistics, and its column bookkeeping land here.
//! The reorder rules, the hint resolution against `utilhint.PlanHints`, the
//! outer-join simplification, and the functional-dependency derivation do not;
//! each is named at its call site.
//!
//! # Narrowings, by name
//!
//! * `HintInfo` / `InternalHintInfo` are `*utilhint.PlanHints`, which is not
//!   transcreated. The DECISIONS those hints produce are kept as the
//!   [`LogicalJoin::prefer_join_type`] bit set and the order flags, so
//!   `PreferAny` works; `SetPreferredJoinTypeAndOrder` does not.
//! * `allJoinLeaf` is rebuilt by `PredicatePushDown` on every call from the
//!   subtree; it is a cache of a walk, not state, so it is not a field here.
//! * `DefaultValues []types.Datum` is kept, since aggregation push-down writes
//!   it and physical join reads it.

use std::collections::BTreeMap;

use tidb_datatype::{Datum, FieldName, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::expr_util::extract::is_col_op_col;
use tidb_expr::expr_util::normal_form::{
    derive_relaxed_filters_from_dnf, extract_filters_from_dnfs,
};
use tidb_expr::expr_util::predicates::is_mutable_effects_expr;
use tidb_expr::expr_util::substitute::{build_not_null_expr, SubstituteOptions};
use tidb_expr::expression::{is_null_rejected, CorrelatedColumn, Expression};
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};

use crate::find_best_task::LogicalJoinType;
use crate::hash_equaler::{new_hash_equaler, Hasher};
use crate::logical::schema_producer;
use crate::logical::selection::SELECTION_FACTOR;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalJoin` (`logical_join.go:45`).
#[derive(Clone, Debug)]
pub struct LogicalJoin {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `JoinType`, reusing the port in [`crate::find_best_task`] rather
    /// than introducing a second copy of `base.JoinType`.
    pub join_type: LogicalJoinType,
    /// Go `Reordered`.
    pub reordered: bool,
    /// Go `StraightJoin`.
    pub straight_join: bool,
    /// Go `PreferJoinType`: the resolved algorithm-hint bit set.
    pub prefer_join_type: u32,
    /// Go `PreferJoinOrder`.
    pub prefer_join_order: bool,
    /// Go `InternalPreferJoinOrder`.
    pub internal_prefer_join_order: bool,
    /// Go `LeftPreferJoinType`.
    pub left_prefer_join_type: u32,
    /// Go `RightPreferJoinType`.
    pub right_prefer_join_type: u32,
    /// Go `EqualConditions`: `col = col` across the two children, which is
    /// what a hash or merge join keys on. Kept as [`ScalarFunction`] exactly
    /// as Go does, so a join key never has to be re-proved.
    pub equal_conditions: Vec<ScalarFunction>,
    /// Go `NAEQConditions`: null-aware equal conditions, for null-aware semi
    /// joins.
    pub na_eq_conditions: Vec<ScalarFunction>,
    /// Go `LeftConditions`: conditions over the left child alone.
    pub left_conditions: Vec<Expression>,
    /// Go `RightConditions`.
    pub right_conditions: Vec<Expression>,
    /// Go `OtherConditions`: everything that spans both children.
    pub other_conditions: Vec<Expression>,
    /// Go `LeftProperties`, filled by `PreparePossibleProperties`.
    pub left_properties: Vec<Vec<Column>>,
    /// Go `RightProperties`.
    pub right_properties: Vec<Vec<Column>>,
    /// Go `DefaultValues`: the inner row an outer join emits when nothing
    /// matched. Empty means "a slice of NULL", which is Go's nil.
    pub default_values: Vec<Datum>,
    /// Go `FullSchema`: every column the join CAN output, ordered
    /// `[outer..., inner...]`, including the `USING`/`NATURAL` redundant ones
    /// that `Schema()` hides.
    pub full_schema: Option<Schema>,
    /// Go `FullNames`.
    pub full_names: Vec<FieldName>,
    /// Go `RedundantColsToOutputIdx`: a redundant column's `UniqueID` to the
    /// canonical visible output position. Built once and then immutable.
    pub redundant_cols_to_output_idx: BTreeMap<i64, usize>,
    /// Go `PreferCorrelate`.
    pub prefer_correlate: bool,
    /// Go `EqualCondOutCnt`: the estimated row count after `EqualConditions`.
    pub equal_cond_out_cnt: f64,
    /// Go `FromDecorrelatedApply`.
    pub from_decorrelated_apply: bool,
}

impl Default for LogicalJoin {
    fn default() -> Self {
        Self {
            base: BaseLogicalPlan::default(),
            join_type: LogicalJoinType::Inner,
            reordered: false,
            straight_join: false,
            prefer_join_type: 0,
            prefer_join_order: false,
            internal_prefer_join_order: false,
            left_prefer_join_type: 0,
            right_prefer_join_type: 0,
            equal_conditions: Vec::new(),
            na_eq_conditions: Vec::new(),
            left_conditions: Vec::new(),
            right_conditions: Vec::new(),
            other_conditions: Vec::new(),
            left_properties: Vec::new(),
            right_properties: Vec::new(),
            default_values: Vec::new(),
            full_schema: None,
            full_names: Vec::new(),
            redundant_cols_to_output_idx: BTreeMap::new(),
            prefer_correlate: false,
            equal_cond_out_cnt: 0.0,
            from_decorrelated_apply: false,
        }
    }
}

/// Go `LogicalJoin.PredicatePushDown`'s four locals, as
/// [`LogicalJoin::predicate_push_down_local`] hands them to the driver.
#[derive(Clone, Debug, Default)]
pub struct JoinPredicatePushDown {
    /// Go's `ret`: what stays ABOVE the join.
    pub ret: Vec<Expression>,
    /// Go's `leftCond`: what the LEFT child is asked to push.
    pub left_cond: Vec<Expression>,
    /// Go's `rightCond`: what the RIGHT child is asked to push.
    pub right_cond: Vec<Expression>,
    /// The condition list Go hands to `Conds2TableDual` at this point, when it
    /// calls it at all. `None` means Go does not test for a dual here.
    pub dual_conditions: Option<Vec<Expression>>,
}

/// Go `expression.ScalarFuncs2Exprs(funcs)`.
fn scalar_funcs_to_exprs(funcs: &[ScalarFunction]) -> Vec<Expression> {
    funcs
        .iter()
        .cloned()
        .map(Expression::ScalarFunction)
        .collect()
}

/// Go `expression.RemoveDupExprs(exprs)`: keep the first of each distinct
/// `HashCode`.
fn remove_dup_exprs(exprs: Vec<Expression>) -> Vec<Expression> {
    let mut seen: Vec<Vec<u8>> = Vec::with_capacity(exprs.len());
    let mut kept = Vec::with_capacity(exprs.len());
    for mut expr in exprs {
        let code = expr.hash_code().to_vec();
        if seen.iter().any(|other| other == &code) {
            continue;
        }
        seen.push(code);
        kept.push(expr);
    }
    kept
}

/// The four condition buckets `ExtractOnCondition` sorts an `ON` clause into,
/// in Go's return order.
#[derive(Clone, Debug, Default)]
pub struct OnConditionSplit {
    /// Go `eqCond`.
    pub equal: Vec<ScalarFunction>,
    /// Go `leftCond`.
    pub left: Vec<Expression>,
    /// Go `rightCond`.
    pub right: Vec<Expression>,
    /// Go `otherCond`.
    pub other: Vec<Expression>,
}

impl LogicalJoin {
    /// Go `plancodec.TypeJoin`.
    pub const TYPE: &'static str = "Join";

    /// Go `LogicalJoin.Init(ctx, offset)` (`logical_join.go:119`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, join_type: LogicalJoinType) -> Self {
        Self {
            base,
            join_type,
            ..Self::default()
        }
    }

    /// Go `base.JoinType.String()` (`base/plan_base.go:353`), which is the
    /// first field of [`Self::explain_info`].
    #[must_use]
    pub const fn join_type_name(join_type: LogicalJoinType) -> &'static str {
        match join_type {
            LogicalJoinType::Inner => "inner join",
            LogicalJoinType::LeftOuter => "left outer join",
            LogicalJoinType::RightOuter => "right outer join",
            LogicalJoinType::Semi => "semi join",
            LogicalJoinType::AntiSemi => "anti semi join",
            LogicalJoinType::LeftOuterSemi => "left outer semi join",
            LogicalJoinType::AntiLeftOuterSemi => "anti left outer semi join",
        }
    }

    /// Go `LogicalJoin.IsNAAJ()` (`logical_join.go:784`): a null-aware
    /// anti/semi join, i.e. one with null-aware equal conditions.
    #[must_use]
    pub fn is_naaj(&self) -> bool {
        !self.na_eq_conditions.is_empty()
    }

    /// Go `LogicalJoin.PreferAny(joinFlags...)` (`logical_join.go:1384`).
    #[must_use]
    pub fn prefer_any(&self, join_flags: &[u32]) -> bool {
        join_flags
            .iter()
            .any(|flag| self.prefer_join_type & flag > 0)
    }

    /// Go `LogicalJoin.ExplainInfo()` (`logical_join.go:127`): the join type
    /// followed by each non-empty condition bucket.
    ///
    /// # Blocked
    ///
    /// Go renders the three non-equal buckets with
    /// `expression.SortedExplainExpressionList(evalCtx, conds)`, which needs an
    /// `EvalContext` to call `Expression.StringWithCtx`; neither exists in this
    /// crate. Only the dependency-closed prefix is produced, and the presence
    /// of each bucket is reported by its COUNT so the string is never silently
    /// missing a condition.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let mut buffer = String::from(Self::join_type_name(self.join_type));
        for (label, count) in [
            ("equal", self.equal_conditions.len()),
            ("left cond", self.left_conditions.len()),
            ("right cond", self.right_conditions.len()),
            ("other cond", self.other_conditions.len()),
        ] {
            if count > 0 {
                buffer.push_str(&format!(", {label}:{count} exprs"));
            }
        }
        buffer
    }

    /// Go `LogicalJoin.ExtractCorrelatedCols()` (`logical_join.go:675`): every
    /// correlated column across all four buckets, in Go's order.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::new();
        for function in &self.equal_conditions {
            cor_cols.extend(extract_cor_columns(&Expression::ScalarFunction(
                function.clone(),
            )));
        }
        for bucket in [
            &self.left_conditions,
            &self.right_conditions,
            &self.other_conditions,
        ] {
            for cond in bucket {
                cor_cols.extend(extract_cor_columns(cond));
            }
        }
        cor_cols
    }

    /// Go `LogicalJoin.GetJoinKeys()` (`logical_join.go:1011`): the left and
    /// right key columns of every equal condition, plus which are `<=>`.
    #[must_use]
    pub fn get_join_keys(&self) -> (Vec<Column>, Vec<Column>, Vec<bool>, bool) {
        let mut left_keys = Vec::with_capacity(self.equal_conditions.len());
        let mut right_keys = Vec::with_capacity(self.equal_conditions.len());
        let mut is_null_eq = Vec::with_capacity(self.equal_conditions.len());
        let mut has_null_eq = false;
        for function in &self.equal_conditions {
            let Some((left, right)) = is_col_op_col(function) else {
                continue;
            };
            left_keys.push(left.clone());
            right_keys.push(right.clone());
            let null_eq = function.func_name.lowercase() == "nulleq";
            is_null_eq.push(null_eq);
            has_null_eq = has_null_eq || null_eq;
        }
        (left_keys, right_keys, is_null_eq, has_null_eq)
    }

    /// Go `LogicalJoin.GetNAJoinKeys()` (`logical_join.go:1023`).
    #[must_use]
    pub fn get_na_join_keys(&self) -> (Vec<Column>, Vec<Column>) {
        let mut left_keys = Vec::with_capacity(self.na_eq_conditions.len());
        let mut right_keys = Vec::with_capacity(self.na_eq_conditions.len());
        for function in &self.na_eq_conditions {
            if let Some((left, right)) = is_col_op_col(function) {
                left_keys.push(left.clone());
                right_keys.push(right.clone());
            }
        }
        (left_keys, right_keys)
    }

    /// Go `LogicalJoin.ExtractJoinKeys(childIdx)` (`logical_join.go:1203`):
    /// one child's key columns, as a schema.
    #[must_use]
    pub fn extract_join_keys(&self, child_idx: usize) -> Schema {
        let columns = self
            .equal_conditions
            .iter()
            .filter_map(|function| match function.get_args().get(child_idx) {
                Some(Expression::Column(column)) => Some(column.clone()),
                _ => None,
            })
            .collect();
        Schema::new(columns)
    }

    /// Go `LogicalJoin.AppendJoinConds(eq, left, right, other)`
    /// (`logical_join.go:1148`): the new conditions go in FRONT of the
    /// existing ones in every bucket.
    pub fn append_join_conds(&mut self, split: OnConditionSplit) {
        prepend(&mut self.equal_conditions, split.equal);
        prepend(&mut self.left_conditions, split.left);
        prepend(&mut self.right_conditions, split.right);
        prepend(&mut self.other_conditions, split.other);
    }

    /// Go `LogicalJoin.AttachOnConds(onConds)` (`logical_join.go:1142`):
    /// classify an `ON` clause against the two children's schemas and prepend
    /// the result.
    ///
    /// Go reads the schemas from `p.Children()`; the children live on the base
    /// and their schemas are passed in, so this method does not have to walk
    /// the tree.
    pub fn attach_on_conds(
        &mut self,
        on_conds: &[Expression],
        left_schema: &Schema,
        right_schema: &Schema,
        opts: &SubstituteOptions<'_>,
    ) {
        let split =
            self.extract_on_condition(on_conds, left_schema, right_schema, false, false, opts);
        self.append_join_conds(split);
    }

    /// Go `LogicalJoin.ExtractOnCondition(conditions, leftSchema, rightSchema,
    /// deriveLeft, deriveRight)` (`logical_join.go:1448`).
    ///
    /// The classification, in the source's order:
    /// 1. an `=` rewritten from `IN (subq)` goes to `other`, so the join stays
    ///    "empty aware" — see the source comment and TiDB PR #9051;
    /// 2. a `col op col` spanning the two children becomes an EQUAL condition
    ///    when the operator is `=` or `<=>`, and derives a not-null filter on
    ///    each side that `deriveLeft`/`deriveRight` asks for and that the
    ///    predicate rejects nulls on;
    /// 3. a condition with NO columns is routed by
    ///    [`Self::push_down_const_expr`], unless it has mutable effects;
    /// 4. a condition whose columns all come from one side goes to that side;
    /// 5. anything else goes to `other`, optionally relaxed to a per-side
    ///    superset through `DeriveRelaxedFiltersFromDNF`.
    ///
    /// # Narrowing
    ///
    /// Go rebuilds the equal condition with
    /// `expression.NewFunctionInternal(ctx, name, TypeTiny, arg0, arg1)` so the
    /// left argument is always the LEFT child's column. That rebuild is done
    /// through `opts.builder`; if the builder refuses, the original condition
    /// is classified into `other` rather than dropped.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn extract_on_condition(
        &mut self,
        conditions: &[Expression],
        left_schema: &Schema,
        right_schema: &Schema,
        derive_left: bool,
        derive_right: bool,
        opts: &SubstituteOptions<'_>,
    ) -> OnConditionSplit {
        let mut split = OnConditionSplit::default();
        for expr in conditions {
            if is_eq_cond_from_in(expr) {
                split.other.push(expr.clone());
                continue;
            }
            if let Expression::ScalarFunction(binop) = expr {
                if binop.get_args().len() == 2 {
                    if let Some((arg0, arg1)) = is_col_op_col(binop) {
                        let (mut arg0, mut arg1) = (arg0.clone(), arg1.clone());
                        let mut left_col = left_schema.retrieve_column(&arg0).cloned();
                        let mut right_col = right_schema.retrieve_column(&arg1).cloned();
                        if left_col.is_none() || right_col.is_none() {
                            left_col = left_schema.retrieve_column(&arg1).cloned();
                            right_col = right_schema.retrieve_column(&arg0).cloned();
                            std::mem::swap(&mut arg0, &mut arg1);
                        }
                        if let (Some(left_col), Some(right_col)) = (left_col, right_col) {
                            if derive_left {
                                if let Some(cond) =
                                    derive_not_null(&left_col, left_schema, expr, opts)
                                {
                                    split.left.push(cond);
                                }
                            }
                            if derive_right {
                                if let Some(cond) =
                                    derive_not_null(&right_col, right_schema, expr, opts)
                                {
                                    split.right.push(cond);
                                }
                            }
                            let name = binop.func_name.lowercase();
                            if name == "eq" || name == "nulleq" {
                                if let Ok(Expression::ScalarFunction(rebuilt)) =
                                    opts.builder.new_function(
                                        name,
                                        Some(tidb_datatype::FieldType::parser(
                                            tidb_datatype::FieldTypeCode::Tiny,
                                        )),
                                        vec![Expression::Column(arg0), Expression::Column(arg1)],
                                    )
                                {
                                    split.equal.push(rebuilt);
                                } else {
                                    split.other.push(expr.clone());
                                }
                                continue;
                            }
                        }
                    }
                }
            }
            let columns = extract_columns(expr);
            if columns.is_empty() {
                if is_mutable_effects_expr(expr) {
                    split.other.push(expr.clone());
                    continue;
                }
                self.push_down_const_expr(
                    expr,
                    &mut split.left,
                    &mut split.right,
                    derive_left || derive_right,
                );
                continue;
            }
            if is_mutable_effects_expr(expr) {
                split.other.push(expr.clone());
                continue;
            }
            let all_from_left = columns.iter().all(|column| left_schema.contains(column));
            let all_from_right = columns.iter().all(|column| right_schema.contains(column));
            if all_from_right {
                split.right.push(expr.clone());
            } else if all_from_left {
                split.left.push(expr.clone());
            } else {
                if derive_left {
                    if let Some(relaxed) = derive_relaxed_filters_from_dnf(expr, left_schema) {
                        split.left.push(relaxed);
                    }
                }
                if derive_right {
                    if let Some(relaxed) = derive_relaxed_filters_from_dnf(expr, right_schema) {
                        split.right.push(relaxed);
                    }
                }
                split.other.push(expr.clone());
            }
        }
        split
    }

    /// Go `LogicalJoin.pushDownConstExpr(expr, leftCond, rightCond,
    /// filterCond)` (`logical_join.go:1555`).
    ///
    /// A column-free condition from a FILTER (`filter_cond`) may go to the
    /// preserved side; one from the `ON` clause may only go to the side that
    /// nulls can be manufactured for. The two writes into
    /// `self.right_conditions` / `self.left_conditions` are Go's, and they are
    /// what lets the condition keep travelling downward later.
    pub fn push_down_const_expr(
        &mut self,
        expr: &Expression,
        left_cond: &mut Vec<Expression>,
        right_cond: &mut Vec<Expression>,
        filter_cond: bool,
    ) {
        match self.join_type {
            LogicalJoinType::LeftOuter
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiLeftOuterSemi => {
                if filter_cond {
                    left_cond.push(expr.clone());
                    self.right_conditions.push(expr.clone());
                } else {
                    right_cond.push(expr.clone());
                }
            }
            LogicalJoinType::RightOuter => {
                if filter_cond {
                    right_cond.push(expr.clone());
                    self.left_conditions.push(expr.clone());
                } else {
                    left_cond.push(expr.clone());
                }
            }
            LogicalJoinType::Semi | LogicalJoinType::Inner => {
                left_cond.push(expr.clone());
                right_cond.push(expr.clone());
            }
            LogicalJoinType::AntiSemi => {
                if filter_cond {
                    left_cond.push(expr.clone());
                }
                right_cond.push(expr.clone());
            }
        }
    }

    /// Go `LogicalJoin.PredicatePushDown(predicates)`'s LOCAL half
    /// (`logical_join.go:171`): the whole per-join-type attribution, without
    /// the recursion into the children.
    ///
    /// `simplify` is `ruleutil.ApplyPredicateSimplification` /
    /// `ApplyPredicateSimplificationForJoin`, injected because it is a
    /// function POINTER in Go too — `rule/util/misc.go:214`, filled in by
    /// `rule/rule_init.go`'s `init()` to break a package cycle.
    ///
    /// # What is ported
    ///
    /// The per-`JoinType` attribution, which is the rule: which conditions
    /// become `EqualConditions`/`OtherConditions`, which travel to the left
    /// child, which to the right, and which stay above the join (`ret`). For
    /// the inner/semi case that is Go's full body: every bucket plus the
    /// incoming predicates are gathered into one `tempCond`, DNF filters are
    /// extracted, the whole set is simplified, and the result is re-split with
    /// `deriveLeft` and `deriveRight` both set — so an `IS NOT NULL` derived
    /// from an equality on one side becomes a filter on the other. That
    /// attribution is the part the `tidb-executor` driver's
    /// `predicate_push_down` prototype carried, and it is folded in here.
    ///
    /// # Narrowings, by exact blocking Go symbol
    ///
    /// * `simplifyOuterJoin(p, predicates)` (`logical_join.go:300`), which
    ///   turns a left/right outer join into an inner join when a predicate is
    ///   null-rejecting on the inner side. Blocked on `util.IsNullRejected`'s
    ///   session-dependent half; `tidb_expr::expression::is_null_rejected`
    ///   exists but Go's caller needs `p.SCtx()` for the plan-cache guard.
    /// * `p.outerJoinPropConst(predicates, filter)` (`logical_join.go:1024`)
    ///   and therefore `expression.PropagateConstantForJoin`. This is the
    ///   `propagateConstant` half of the simplification hook; see
    ///   [`crate::logical::rule::apply_predicate_simplification`].
    /// * `DeriveOtherConditions(p, leftSchema, rightSchema, deriveLeft,
    ///   deriveRight)` (`logical_join.go:1247`), which manufactures the
    ///   `IS NOT NULL` filters an OUTER join may push to its inner side.
    /// * `p.updateEQCond()` (`logical_join.go:920`) and `p.SemiJoinRewrite()`,
    ///   which run after the children have been pushed into.
    /// * `getAllJoinLeaf(p)` / `p.allJoinLeaf`, which only feeds the two
    ///   `isVaildConstantPropagationExpression*` filters above.
    ///
    /// Every one of those only ever pushes MORE down or narrows a join type
    /// further, so omitting them leaves conditions higher in the tree than Go
    /// would — never lower.
    #[must_use]
    pub fn predicate_push_down_local(
        &mut self,
        predicates: Vec<Expression>,
        left_schema: &Schema,
        right_schema: &Schema,
        opts: &SubstituteOptions<'_>,
        simplify: impl Fn(Vec<Expression>) -> Vec<Expression>,
    ) -> JoinPredicatePushDown {
        // Go's leading `switch p.JoinType`: for everything but the semi/inner
        // and outer-semi families, `OtherConditions` is simplified in place so
        // an obvious logical constant cannot hide a join key.
        match self.join_type {
            LogicalJoinType::AntiLeftOuterSemi
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiSemi
            | LogicalJoinType::Semi
            | LogicalJoinType::Inner => {}
            LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
                let other = std::mem::take(&mut self.other_conditions);
                self.other_conditions = simplify(other);
            }
        }

        let mut result = JoinPredicatePushDown::default();
        match self.join_type {
            LogicalJoinType::LeftOuter
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiLeftOuterSemi => {
                let predicates = simplify(predicates);
                if !predicates.is_empty() {
                    result.dual_conditions = Some(predicates.clone());
                }
                let predicates = extract_filters_from_dnfs(predicates);
                // Only the LEFT where-condition may be derived: a filter on the
                // null-supplying side would change which rows are preserved.
                let split = self.extract_on_condition(
                    &predicates,
                    left_schema,
                    right_schema,
                    true,
                    false,
                    opts,
                );
                let right_cond = std::mem::take(&mut self.right_conditions);
                result.left_cond = split.left;
                result.right_cond = right_cond;
                result.ret = scalar_funcs_to_exprs(&split.equal);
                result.ret.extend(split.other);
                result.ret.extend(split.right);
            }
            LogicalJoinType::RightOuter => {
                let predicates = simplify(predicates);
                if !predicates.is_empty() {
                    result.dual_conditions = Some(predicates.clone());
                }
                let predicates = extract_filters_from_dnfs(predicates);
                let split = self.extract_on_condition(
                    &predicates,
                    left_schema,
                    right_schema,
                    false,
                    true,
                    opts,
                );
                let left_cond = std::mem::take(&mut self.left_conditions);
                result.right_cond = split.right;
                result.left_cond = left_cond;
                result.ret = scalar_funcs_to_exprs(&split.equal);
                result.ret.extend(split.other);
                result.ret.extend(split.left);
            }
            LogicalJoinType::Semi | LogicalJoinType::Inner => {
                let mut temp_cond = Vec::with_capacity(
                    self.left_conditions.len()
                        + self.right_conditions.len()
                        + self.equal_conditions.len()
                        + self.other_conditions.len()
                        + predicates.len(),
                );
                temp_cond.extend(self.left_conditions.iter().cloned());
                temp_cond.extend(self.right_conditions.iter().cloned());
                temp_cond.extend(scalar_funcs_to_exprs(&self.equal_conditions));
                temp_cond.extend(self.other_conditions.iter().cloned());
                temp_cond.extend(predicates);
                let temp_cond = simplify(extract_filters_from_dnfs(temp_cond));
                if !temp_cond.is_empty() {
                    result.dual_conditions = Some(temp_cond.clone());
                }
                let split = self.extract_on_condition(
                    &temp_cond,
                    left_schema,
                    right_schema,
                    true,
                    true,
                    opts,
                );
                self.left_conditions = Vec::new();
                self.right_conditions = Vec::new();
                self.equal_conditions = split.equal;
                self.other_conditions = split.other;
                result.left_cond = split.left;
                result.right_cond = split.right;
            }
            LogicalJoinType::AntiSemi => {
                let predicates = simplify(predicates);
                if !predicates.is_empty() {
                    result.dual_conditions = Some(predicates.clone());
                }
                let split = self.extract_on_condition(
                    &predicates,
                    left_schema,
                    right_schema,
                    true,
                    true,
                    opts,
                );
                // Go: do NOT derive `is not null` for an anti join; see the
                // three counterexamples at `logical_join.go:273`.
                result.left_cond = split.left;
                let mut right_cond = std::mem::take(&mut self.right_conditions);
                right_cond.extend(split.right);
                result.right_cond = right_cond;
            }
        }
        result.left_cond = remove_dup_exprs(result.left_cond);
        result.right_cond = remove_dup_exprs(result.right_cond);
        result
    }

    /// Go `LogicalJoin.ExtractUsedCols(parentUsedCols)`
    /// (`logical_join.go:1212`): every column the join itself reads, added to
    /// the parent's, then split by which child can resolve it.
    #[must_use]
    pub fn extract_used_cols(
        &self,
        parent_used_cols: &[Column],
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> (Vec<Column>, Vec<Column>) {
        let mut used = parent_used_cols.to_vec();
        for function in self.equal_conditions.iter().chain(&self.na_eq_conditions) {
            used.extend(extract_columns(&Expression::ScalarFunction(
                function.clone(),
            )));
        }
        for bucket in [
            &self.left_conditions,
            &self.right_conditions,
            &self.other_conditions,
        ] {
            for cond in bucket {
                used.extend(extract_columns(cond));
            }
        }
        let mut left_cols = Vec::new();
        let mut right_cols = Vec::new();
        for column in used {
            if left_schema.contains(&column) {
                left_cols.push(column);
            } else if right_schema.contains(&column) {
                right_cols.push(column);
            }
        }
        (left_cols, right_cols)
    }

    /// Go `LogicalJoin.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_join.go:365`).
    ///
    /// A semi join outputs left rows, so it keeps the left child's keys. An
    /// inner or one-sided outer join keeps a child's keys when the OTHER
    /// child's join keys cover one of its own keys — the join then matches at
    /// most one row from that other side. The null-filling side is excluded,
    /// because a manufactured NULL row destroys uniqueness.
    pub fn build_key_info(&self, self_schema: &mut Schema, child_schema: &[Schema]) {
        schema_producer::propagate_child_keys(self_schema, child_schema);
        match self.join_type {
            LogicalJoinType::Semi
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiSemi
            | LogicalJoinType::AntiLeftOuterSemi => {
                if let Some(left) = child_schema.first() {
                    self_schema.pk_or_uk = left.pk_or_uk.clone();
                }
            }
            LogicalJoinType::Inner | LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
                if self.equal_conditions.is_empty() {
                    return;
                }
                let (left, right) = (child_schema.first(), child_schema.get(1));
                let (Some(left), Some(right)) = (left, right) else {
                    return;
                };
                let (left_cols, right_cols, _, _) = self.get_join_keys();
                let left_ok = covers_a_key(&left_cols, &left.pk_or_uk);
                let right_ok = covers_a_key(&right_cols, &right.pk_or_uk);
                if left_ok && self.join_type != LogicalJoinType::LeftOuter {
                    self_schema.pk_or_uk.extend(right.pk_or_uk.iter().cloned());
                }
                if right_ok && self.join_type != LogicalJoinType::RightOuter {
                    self_schema.pk_or_uk.extend(left.pk_or_uk.iter().cloned());
                }
            }
        }
    }

    /// Go `LogicalJoin.PreparePossibleProperties(_, childrenProperties)`
    /// (`logical_join.go:646`): both children's orders survive, except that
    /// the null-filled side of an outer join loses its own.
    pub fn prepare_possible_properties(
        &mut self,
        left: &PossiblePropertiesInfo,
        right: &PossiblePropertiesInfo,
    ) -> PossiblePropertiesInfo {
        let has_tiflash = left.has_tiflash && right.has_tiflash;
        self.base.set_has_tiflash(has_tiflash);
        self.left_properties = left.orders.clone();
        self.right_properties = right.orders.clone();
        let mut orders = Vec::new();
        if !matches!(self.join_type, LogicalJoinType::RightOuter) {
            orders.extend(left.orders.iter().cloned());
        }
        if !matches!(
            self.join_type,
            LogicalJoinType::LeftOuter | LogicalJoinType::LeftOuterSemi
        ) {
            orders.extend(right.orders.iter().cloned());
        }
        PossiblePropertiesInfo {
            orders,
            has_tiflash,
        }
    }

    /// Go `LogicalJoin.DeriveStats(childStats, selfSchema, childSchema,
    /// reloads)` (`logical_join.go:560`).
    ///
    /// * a semi/anti-semi join keeps the left profile scaled by the selection
    ///   factor;
    /// * a left-outer-semi join keeps the left profile plus a two-valued
    ///   marker column;
    /// * everything else takes the equal-condition estimate, floored at the
    ///   preserved side's row count for an outer join, and caps every column
    ///   NDV at it.
    ///
    /// # Blocked
    ///
    /// `EqualCondOutCnt` is Go's `cardinality.EstimateFullJoinRowCount(...)`,
    /// which needs the session and both histogram collections. The caller
    /// supplies it here as `equal_cond_out_cnt`; nothing is guessed.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        equal_cond_out_cnt: f64,
        reloads: &[bool],
    ) -> Option<(StatsInfo, bool)> {
        let reload = reloads.iter().any(|one| *one);
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return Some((existing.clone(), false));
            }
        }
        let left = child_stats.first()?;
        let right = child_stats.get(1)?;
        self.equal_cond_out_cnt = equal_cond_out_cnt;
        let stats = match self.join_type {
            LogicalJoinType::Semi | LogicalJoinType::AntiSemi => StatsInfo::new(
                left.row_count() * SELECTION_FACTOR,
                left.col_ndvs()
                    .iter()
                    .map(|(id, ndv)| (*id, ndv * SELECTION_FACTOR)),
            ),
            LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi => {
                let mut ndvs: Vec<(i64, f64)> = left
                    .col_ndvs()
                    .iter()
                    .map(|(id, ndv)| (*id, *ndv))
                    .collect();
                if let Some(marker) = self_schema.columns.last() {
                    ndvs.retain(|(id, _)| *id != marker.unique_id);
                    ndvs.push((marker.unique_id, 2.0));
                }
                StatsInfo::new(left.row_count(), ndvs)
            }
            LogicalJoinType::Inner | LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
                let mut count = equal_cond_out_cnt;
                match self.join_type {
                    LogicalJoinType::LeftOuter => count = count.max(left.row_count()),
                    LogicalJoinType::RightOuter => count = count.max(right.row_count()),
                    _ => {}
                }
                let ndvs = left
                    .col_ndvs()
                    .iter()
                    .chain(right.col_ndvs())
                    .map(|(id, ndv)| (*id, ndv.min(count)));
                StatsInfo::new(count, ndvs)
            }
        };
        self.base.base.set_stats(Some(stats.clone()));
        Some((stats, true))
    }

    /// Go `LogicalJoin.ResolveRedundantColumn(col)`
    /// (`logical_join.go:818`): map a `USING`/`NATURAL JOIN` redundant column
    /// to the canonical visible output column and its name.
    #[must_use]
    pub fn resolve_redundant_column<'a>(
        &self,
        col: &Column,
        self_schema: &'a Schema,
        output_names: &'a [FieldName],
    ) -> Option<(&'a Column, Option<&'a FieldName>)> {
        let index = *self.redundant_cols_to_output_idx.get(&col.unique_id)?;
        let column = self_schema.columns.get(index)?;
        Some((column, output_names.get(index)))
    }

    /// Go `LogicalJoin.RegisterRedundantColumnMapping(redundantCol,
    /// visibleCol)` (`logical_join.go:796`), recorded against the visible
    /// column's position in the output schema.
    ///
    /// The registration is refused when the two columns' result types differ,
    /// which is Go's `redundantColumnRemapTypesMatch` guard: remapping across
    /// types would change the value the user reads back.
    pub fn register_redundant_column_mapping(
        &mut self,
        redundant_col: &Column,
        visible_col: &Column,
        self_schema: &Schema,
    ) -> bool {
        if !redundant_column_remap_types_match(redundant_col, visible_col) {
            return false;
        }
        let index = self_schema.column_index(visible_col);
        if index < 0 {
            return false;
        }
        self.redundant_cols_to_output_idx
            .insert(redundant_col.unique_id, index as usize);
        true
    }

    /// Go `LogicalJoin.Hash64(h)`
    /// (`logicalop/hash64_equals_generated.go`).
    #[must_use]
    pub fn hash64(&self, schema: Option<&Schema>) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_uint64(schema_producer::schema_hash64(schema));
        hasher.hash_int(self.join_type as i64);
        for bucket in [&self.equal_conditions, &self.na_eq_conditions] {
            hasher.hash_int(bucket.len() as i64);
            for function in bucket {
                let mut expr = Expression::ScalarFunction(function.clone());
                hasher.hash_bytes(expr.hash_code());
            }
        }
        for bucket in [
            &self.left_conditions,
            &self.right_conditions,
            &self.other_conditions,
        ] {
            hasher.hash_int(bucket.len() as i64);
            for cond in bucket {
                let mut cond = cond.clone();
                hasher.hash_bytes(cond.hash_code());
            }
        }
        hasher.sum64()
    }

    /// Go `LogicalJoin.Equals(other)`.
    #[must_use]
    pub fn equals(
        &self,
        self_schema: Option<&Schema>,
        other: &Self,
        other_schema: Option<&Schema>,
    ) -> bool {
        schema_producer::schema_equals(self_schema, other_schema)
            && self.join_type == other.join_type
            && scalar_lists_equal(&self.equal_conditions, &other.equal_conditions)
            && scalar_lists_equal(&self.na_eq_conditions, &other.na_eq_conditions)
            && expr_lists_equal(&self.left_conditions, &other.left_conditions)
            && expr_lists_equal(&self.right_conditions, &other.right_conditions)
            && expr_lists_equal(&self.other_conditions, &other.other_conditions)
    }
}

/// Go `expression.IsEQCondFromIn(expr)` (`pkg/expression/expression.go:325`):
/// an `=` whose arguments carry a column marked `InOperand`, i.e. one produced
/// by rewriting `IN (subq)`.
#[must_use]
pub fn is_eq_cond_from_in(expr: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expr else {
        return false;
    };
    if function.func_name.lowercase() != "eq" {
        return false;
    }
    function
        .get_args()
        .iter()
        .flat_map(extract_columns)
        .any(|column| column.in_operand)
}

/// Go `redundantColumnRemapTypesMatch(redundantCol, visibleCol)`
/// (`logical_join.go:810`).
#[must_use]
pub fn redundant_column_remap_types_match(redundant_col: &Column, visible_col: &Column) -> bool {
    match (&redundant_col.ret_type, &visible_col.ret_type) {
        (Some(left), Some(right)) => {
            left.code() == right.code()
                && left.flen() == right.flen()
                && left.decimal() == right.decimal()
        }
        (None, None) => true,
        _ => false,
    }
}

/// The `deriveLeft`/`deriveRight` half of `ExtractOnCondition`: a not-null
/// filter on a nullable join-key column whose predicate rejects nulls.
fn derive_not_null(
    column: &Column,
    schema: &Schema,
    predicate: &Expression,
    opts: &SubstituteOptions<'_>,
) -> Option<Expression> {
    let inner_ids: Vec<i64> = schema.columns.iter().map(|c| c.unique_id).collect();
    if !is_null_rejected(&inner_ids, predicate) {
        return None;
    }
    let already_not_null = column
        .ret_type
        .as_ref()
        .is_some_and(|ty| ty.flags() & FieldTypeFlags::NOT_NULL != 0);
    if already_not_null {
        return None;
    }
    build_not_null_expr(Expression::Column(column.clone()), opts).ok()
}

/// The `checkColumnsMatchPKOrUK` closure of `LogicalJoin.BuildKeyInfo`: some
/// key of `keys` is wholly contained in `cols`.
fn covers_a_key(cols: &[Column], keys: &[Vec<Column>]) -> bool {
    if keys.is_empty() {
        return false;
    }
    keys.iter().any(|key| {
        key.iter().all(|key_column| {
            cols.iter()
                .any(|column| column.unique_id == key_column.unique_id)
        })
    })
}

fn prepend<T>(existing: &mut Vec<T>, mut incoming: Vec<T>) {
    incoming.append(existing);
    *existing = incoming;
}

fn scalar_lists_equal(left: &[ScalarFunction], right: &[ScalarFunction]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(a, b)| {
            schema_producer::expressions_equal(
                &Expression::ScalarFunction(a.clone()),
                &Expression::ScalarFunction(b.clone()),
            )
        })
}

fn expr_lists_equal(left: &[Expression], right: &[Expression]) -> bool {
    schema_producer::expression_lists_equal(left, right)
}

impl LogicalJoin {
    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            join_type: self.join_type,
            reordered: self.reordered,
            straight_join: self.straight_join,
            prefer_join_type: self.prefer_join_type,
            prefer_join_order: self.prefer_join_order,
            internal_prefer_join_order: self.internal_prefer_join_order,
            left_prefer_join_type: self.left_prefer_join_type,
            right_prefer_join_type: self.right_prefer_join_type,
            equal_conditions: self.equal_conditions.clone(),
            na_eq_conditions: self.na_eq_conditions.clone(),
            left_conditions: self.left_conditions.clone(),
            right_conditions: self.right_conditions.clone(),
            other_conditions: self.other_conditions.clone(),
            left_properties: self.left_properties.clone(),
            right_properties: self.right_properties.clone(),
            default_values: self.default_values.clone(),
            full_schema: self.full_schema.clone(),
            full_names: self.full_names.clone(),
            redundant_cols_to_output_idx: self.redundant_cols_to_output_idx.clone(),
            prefer_correlate: self.prefer_correlate,
            equal_cond_out_cnt: self.equal_cond_out_cnt,
            from_decorrelated_apply: self.from_decorrelated_apply,
        }
    }
}
