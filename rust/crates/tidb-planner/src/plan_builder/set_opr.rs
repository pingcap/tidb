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

//! `UNION` / `INTERSECT` / `EXCEPT`: the set-operation half of the logical
//! plan builder.
//!
//! Go source, all `pkg/planner/core/logical_plan_builder.go`:
//!
//! | Here | Go |
//! | --- | --- |
//! | [`union_join_field_type`] | `unionJoinFieldType` (:2001) |
//! | [`PlanBuilder::set_union_flen`] | `setUnionFlen` (:2035) |
//! | [`PlanBuilder::build_projection4_union`] | `buildProjection4Union` (:2053) |
//! | [`PlanBuilder::build_set_opr`] | `buildSetOpr` (:2108) |
//! | [`PlanBuilder::build_semi_join_for_set_operator`] | `buildSemiJoinForSetOperator` (:2201) |
//! | [`PlanBuilder::build_intersect`] | `buildIntersect` (:2234) |
//! | [`PlanBuilder::build_except`] | `buildExcept` (:2286) |
//! | [`PlanBuilder::build_union`] | `buildUnion` (:2317) |
//! | [`divide_union_select_plans`] | `divideUnionSelectPlans` (:2355) |
//! | [`PlanBuilder::build_union_all`] | `buildUnionAll` (:2373) |
//!
//! This module LANDS AS A COMPLETE PACKAGE for the ten symbols above: every
//! production body is present, and the two places Go itself refuses
//! (`INTERSECT ALL`, `EXCEPT ALL`) are refused here with Go's own message.
//! The narrowings below are dependencies that do not exist in this workspace,
//! each named by its exact Go symbol; none is a body left out.
//!
//! # 1. The AST shapes line up, but not one-to-one
//!
//! Go models a set operation as `ast.SetOprStmt{SelectList: *ast.SetOprSelectList}`
//! whose `Selects []ast.Node` holds either an `*ast.SelectStmt` or a NESTED
//! `*ast.SetOprSelectList`, and puts the joining operator on the ELEMENT as
//! `AfterSetOperator`. [`tidb_ast::SetOprStmt`] holds
//! `terms: Vec<`[`SetOprTerm`]`>`, each with its own `op` — which IS
//! `AfterSetOperator` — and a body that is
//! [`SetOprTermBody::Select`] or [`SetOprTermBody::Nested`].
//! [`SetOprTermBody::Nested`] is Go's nested `*ast.SetOprSelectList`, except
//! that it already carries the `With`/`Limit`/`OrderBy` that Go's
//! `buildIntersect` has to re-wrap into a synthetic `ast.SetOprStmt`; here the
//! nested value IS that statement, so the arm is a direct
//! [`PlanBuilder::build_set_opr`] call.
//!
//! One consequence is spelled out in [`PlanBuilder::build_set_opr`]: Go's
//! `breakIteration` inspects the nested node's `Limit`/`OrderBy`, and this
//! port ALSO inspects `outer_limit`/`outer_order_by`, because a tail written
//! outside the term's own parentheses lands in those fields here while Go's
//! parser folds it into the same `SetOprSelectList`.
//!
//! # 2. `UNION` is the only place a column's type is JOINED
//!
//! [`union_join_field_type`] is the rule, and section 2 of
//! [`PlanBuilder::build_projection4_union`] is where it is applied across the
//! branches and turned into a CAST. Read those two together; a wrong result
//! type here is a silent wrong answer, and Go's own comment on the decimal
//! sign rule ("This logic will be intelligible when it is associated with the
//! buildProjection4Union logic") says as much.
//!
//! # Narrowings, by exact Go symbol
//!
//! * `expression.BuildCastFunction4Union(ctx, srcCol, dstType)`. The cast
//!   itself is [`tidb_expr::aggregation::wrap_cast::build_cast_to`], which is
//!   Go's `BuildCastFunction`. What the `4Union` spelling adds is
//!   `inUnion = true` on the built `builtinCastXXXSig`, an EVALUATION flag
//!   that turns a signed-to-unsigned overflow into `0` instead of an error
//!   (`expression/builtin_cast.go`'s `inUnion` field). [`tidb_expr`]'s
//!   `ScalarFunction` has no per-signature state to carry it, so the flag is
//!   dropped and the cast is the ordinary one. That is a RUNTIME difference on
//!   an out-of-range value only; the plan SHAPE and every result type are
//!   identical.
//! * `types.NameSlice` / `expression.Column.CleanHashCode`. Both are Go
//!   memory-management details with no observable counterpart.
//! * `b.ctx.GetSessionVars().StmtCtx`'s warning channel. `setUnionFlen` and
//!   `buildProjection4Union` do not warn, so nothing is lost here; the CTE
//!   half names its own uses.

use tidb_ast::{SetOp, SetOprStmt, SetOprTerm, SetOprTermBody};
use tidb_datatype::{
    agg_field_type, Charset, FieldName, FieldType, FieldTypeCode, FieldTypeFlags,
    UNSPECIFIED_LENGTH,
};
use tidb_expr::collation_derive::check_and_derive_collation_from_exprs;
use tidb_expr::column::Column;
use tidb_expr::expr_collation::Coercibility;
use tidb_expr::expr_util::extract::is_col_op_col;
use tidb_expr::expr_util::{FunctionBuilder, RealFunctionBuilder};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use crate::find_best_task::LogicalJoinType;
use crate::logical::join::LogicalJoin;
use crate::logical::projection::LogicalProjection;
use crate::logical::rule::flags;
use crate::logical::union_all::LogicalUnionAll;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanError;

use super::catalog::TableSource;
use super::{snapshot_schema_and_names, PlanBuilder};

/// Go `mysql.MaxIntWidth` (`parser/mysql/type.go`).
const MAX_INT_WIDTH: i64 = 20;

/// Go `types.TryToFixFlenOfDatetime` (`types/field_type.go`).
///
/// Transcribed rather than imported: `tidb_expr`'s copy lives inside
/// `rewriter::control_type` and is private there. Both are the same four
/// lines, and Go likewise has one function called from several packages.
fn try_to_fix_flen_of_datetime(ft: &mut FieldType) {
    /// Go `mysql.MaxDatetimeWidthNoFsp`.
    const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;
    if ft.code() == FieldTypeCode::Datetime {
        let decimal = ft.decimal();
        ft.set_flen(MAX_DATETIME_WIDTH_NO_FSP + if decimal > 0 { decimal + 1 } else { 0 });
    }
}

/// Go `expression.SetBinFlagOrBinStr(b, resultTp)`
/// (`expression/util.go`): a BINARY-charset source makes the target binary
/// too, and a source that merely carries the binary FLAG passes just the flag.
fn set_bin_flag_or_bin_str(source: &FieldType, target: &mut FieldType) {
    if source.charset_name() == Charset::Binary.name() {
        // Go `types.SetBinChsClnFlag(target)`.
        target.set_charset_name(Charset::Binary.name());
        target.set_collation_name(Charset::Binary.default_collation().name());
        target.add_flags(FieldTypeFlags::BINARY);
    } else if source.flags() & FieldTypeFlags::BINARY != 0 {
        target.add_flags(FieldTypeFlags::BINARY);
    } else {
        target.del_flags(FieldTypeFlags::BINARY);
    }
}

/// Go `unionJoinFieldType(a, b)` (`logical_plan_builder.go:2001`): "finds the
/// type which can carry the given types in Union".
///
/// Go's own note applies unchanged: this does NOT handle charset and
/// collation, and [`PlanBuilder::build_projection4_union`] is the caller that
/// does.
#[must_use]
pub fn union_join_field_type(a: &FieldType, b: &FieldType) -> FieldType {
    // "We ignore the pure NULL type."
    if a.code() == FieldTypeCode::Null {
        return b.clone();
    } else if b.code() == FieldTypeCode::Null {
        return a.clone();
    }
    let mut result = agg_field_type(&[a.clone(), b.clone()]);
    if result.code() == FieldTypeCode::NewDecimal {
        // "The decimal result type will be unsigned only when all the decimals
        // to be united are unsigned." Go's `AndFlag(f)` is `flag &= f`, so
        // every OTHER flag `AggFieldType` carried is cleared here too — which
        // is why this is a `set_flags` of the masked value and not a
        // `del_flags(UNSIGNED)`.
        result.set_flags(result.flags() & (b.flags() & FieldTypeFlags::UNSIGNED));
    } else {
        // "Non-decimal results will be unsigned when a,b both unsigned."
        result.add_flags(
            (a.flags() & FieldTypeFlags::UNSIGNED) & (b.flags() & FieldTypeFlags::UNSIGNED),
        );
    }
    result.set_decimal_under_limit(a.decimal().max(b.decimal()));
    // "`flen - decimal` is the fraction before '.'"
    if a.flen() == UNSPECIFIED_LENGTH || b.flen() == UNSPECIFIED_LENGTH {
        result.set_flen_under_limit(UNSPECIFIED_LENGTH);
    } else {
        result.set_flen_under_limit(
            (a.flen() - a.decimal()).max(b.flen() - b.decimal()) + result.decimal(),
        );
    }
    try_to_fix_flen_of_datetime(&mut result);
    if result.eval_type() != tidb_datatype::EvalType::Int
        && (a.eval_type() == tidb_datatype::EvalType::Int
            || b.eval_type() == tidb_datatype::EvalType::Int)
        && (result.flen() < MAX_INT_WIDTH && result.flen() != UNSPECIFIED_LENGTH)
    {
        result.set_flen(MAX_INT_WIDTH);
    }
    set_bin_flag_or_bin_str(b, &mut result);
    result
}

/// Go `divideUnionSelectPlans(_, selects, setOprTypes)`
/// (`logical_plan_builder.go:2355`): splits the branches into the
/// `UNION DISTINCT` prefix and the `UNION ALL` suffix.
///
/// Go's own rule, quoted: "Mixed UNION types are treated such that a DISTINCT
/// union overrides any ALL union to its left."
///
/// # Errors
///
/// `ErrWrongNumberOfColumnsInSelect` when a branch has a different arity.
fn divide_union_select_plans(
    selects: Vec<LogicalPlan>,
    set_opr_types: &[Option<SetOp>],
) -> Result<(Vec<LogicalPlan>, Vec<LogicalPlan>), PlanError> {
    let mut first_union_all_idx = 0;
    let column_nums = schema_len(&selects[0]);
    for index in (1..selects.len()).rev() {
        // Go dereferences `*setOprTypes[i]`; the `None` a caller leaves at a
        // position it never reads is treated as "not UNION ALL", which is the
        // arm Go's non-nil operators take for everything except `UNION ALL`.
        let is_union_all = matches!(
            set_opr_types.get(index),
            Some(Some(SetOp::Union { all: true }))
        );
        if first_union_all_idx == 0 && !is_union_all {
            first_union_all_idx = index + 1;
        }
        if schema_len(&selects[index]) != column_nums {
            return Err(wrong_number_of_columns());
        }
    }
    let mut selects = selects;
    let all_selects = selects.split_off(first_union_all_idx);
    Ok((selects, all_selects))
}

/// Go `plannererrors.ErrWrongNumberOfColumnsInSelect` (MySQL 1222).
fn wrong_number_of_columns() -> PlanError {
    PlanError::internal("The used SELECT statements have a different number of columns")
}

/// Go `*expression.Column.RetType`, which is never nil for a column a builder
/// produced. The `Unspecified` fallback is Go's zero `types.FieldType`.
fn ret_type_of(column: &Column) -> FieldType {
    column
        .ret_type
        .clone()
        .unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified))
}

fn schema_len(plan: &LogicalPlan) -> usize {
    plan.schema().map_or(0, |schema| schema.columns.len())
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `setUnionFlen(resultTp, cols)` (`logical_plan_builder.go:2035`):
    /// "Set the flen of the union column using the max flen in children."
    ///
    /// A BINARY result counts each child's flen in BYTES, so a `utf8mb4`
    /// child's character length is multiplied by its charset's `Maxlen`; a
    /// charset this workspace does not know keeps Go's `ok == false` arm,
    /// which leaves the multiplier at 1.
    pub fn set_union_flen(result_tp: &mut FieldType, cols: &[Expression]) {
        if result_tp.flen() == UNSPECIFIED_LENGTH {
            return;
        }
        let is_binary = result_tp.charset_name() == Charset::Binary.name();
        for col in cols {
            let Some(child_tp) = col.static_type() else {
                continue;
            };
            let mut char_len = 1;
            if is_binary {
                if let Some(charset) = Charset::from_name(child_tp.charset_name()) {
                    char_len = charset.maxlen();
                }
            }
            result_tp.set_flen(result_tp.flen().max(char_len * child_tp.flen()));
        }
    }

    /// Go `buildProjection4Union(_, u)` (`logical_plan_builder.go:2053`).
    ///
    /// Go MUTATES the `*LogicalUnionAll` in place — it sets the union's schema
    /// and names, then replaces each child with a projection over it. Children
    /// are OWNED here (see [`super`]'s section 3), so the children arrive and
    /// leave as a `Vec` and the union's schema and names are RETURNED rather
    /// than written through a handle. The order of operations is Go's.
    ///
    /// # Errors
    ///
    /// `ErrIllegalMixCollation` when the branches' collations cannot be
    /// reconciled, or a cast-construction error.
    pub fn build_projection4_union(
        &mut self,
        children: Vec<LogicalPlan>,
    ) -> Result<(Vec<LogicalPlan>, Schema, Vec<FieldName>), PlanError> {
        let snapshots: Vec<(Schema, Vec<FieldName>)> =
            children.iter().map(snapshot_schema_and_names).collect();
        let (first_schema, first_names) = &snapshots[0];

        // 1. "Infer union result types by its children's schema."
        let mut union_cols = Vec::with_capacity(first_schema.columns.len());
        let mut names = Vec::with_capacity(first_schema.columns.len());
        for (index, column) in first_schema.columns.iter().enumerate() {
            let mut tmp_exprs = vec![Expression::Column(column.clone())];
            let mut result_tp = ret_type_of(column);
            for (schema, _) in &snapshots[1..] {
                let Some(child_col) = schema.columns.get(index) else {
                    return Err(wrong_number_of_columns());
                };
                tmp_exprs.push(Expression::Column(child_col.clone()));
                let child_tp = ret_type_of(child_col);
                result_tp = union_join_field_type(&result_tp, &child_tp);
            }
            // 2. The collation of the joined type. Go treats an error and a
            // `CoercibilityNone` result the SAME way, so both arms raise
            // `ErrIllegalMixCollation`.
            let collation =
                check_and_derive_collation_from_exprs("UNION", result_tp.eval_type(), &tmp_exprs)
                    .map_err(|_| illegal_mix_collation())?;
            if collation.coer == Coercibility::NONE {
                return Err(illegal_mix_collation());
            }
            result_tp.set_charset_name(collation.charset);
            result_tp.set_collation_name(collation.collation);
            Self::set_union_flen(&mut result_tp, &tmp_exprs);

            // Go builds `&types.FieldName{ColName: ...}` — the DB, table and
            // original-column spellings are deliberately dropped, so a union's
            // output column is unqualified.
            let mut name = FieldName::default();
            name.names.column = first_names
                .get(index)
                .map(|first| first.names.column.clone())
                .unwrap_or_default();
            names.push(name);
            union_cols.push(Column::new(self.column_ids.alloc(), result_tp));
        }

        // 3. "Process each child and add a projection above original child. So
        // the schema of `UnionAll` can be the same with its children's."
        self.opt_flag |= flags::ELIMINATE_PROJECTION;
        let mut projected = Vec::with_capacity(children.len());
        for (child, (child_schema, _)) in children.into_iter().zip(&snapshots) {
            let mut exprs = Vec::with_capacity(child_schema.columns.len());
            for (index, src_col) in child_schema.columns.iter().enumerate() {
                let dst_type = union_cols[index]
                    .ret_type
                    .clone()
                    .expect("a union column is built with a type");
                let src_type = ret_type_of(src_col);
                if src_type.equal(&dst_type) {
                    exprs.push(Expression::Column(src_col.clone()));
                } else {
                    // boundary: `expression.BuildCastFunction4Union`'s
                    // `inUnion` flag; see this module's narrowings.
                    exprs.push(
                        tidb_expr::aggregation::wrap_cast::build_cast_to(
                            Expression::Column(src_col.clone()),
                            dst_type,
                        )
                        .map_err(|error| PlanError::internal(format!("{error:?}")))?,
                    );
                }
            }
            // `proj.SetSchema(u.Schema().Clone())`, then "reset the schema type
            // to make the 'not null' flag right" — the projection's columns
            // keep the union's unique IDs and take each expression's OWN type,
            // which is how a NOT NULL branch stays NOT NULL under the union's
            // nullable joined type.
            let mut proj_columns = union_cols.clone();
            for (column, expr) in proj_columns.iter_mut().zip(&exprs) {
                column.ret_type = expr.static_type().cloned();
            }
            let mut projection = LogicalProjection::new(self.base(LogicalProjection::TYPE), exprs);
            projection.base.set_children(vec![child]);
            projection
                .base
                .base
                .set_schema(Some(Schema::new(proj_columns)));
            projected.push(LogicalPlan::Projection(projection));
        }

        Ok((projected, Schema::new(union_cols), names))
    }

    /// Go `buildUnionAll(ctx, subPlan)` (`logical_plan_builder.go:2373`).
    ///
    /// `None` is Go's `nil, nil` for an empty branch list, which
    /// [`Self::build_union`] tests for.
    ///
    /// # Errors
    ///
    /// [`Self::build_projection4_union`]'s errors.
    pub fn build_union_all(
        &mut self,
        sub_plan: Vec<LogicalPlan>,
    ) -> Result<Option<LogicalPlan>, PlanError> {
        if sub_plan.is_empty() {
            return Ok(None);
        }
        self.opt_flag |= flags::ELIMINATE_UNION_ALL_DUAL_ITEM;
        let (children, schema, names) = self.build_projection4_union(sub_plan)?;
        let mut union = LogicalUnionAll::new(self.base(LogicalUnionAll::TYPE));
        union.base.set_children(children);
        union.base.base.set_schema(Some(schema));
        union.base.base.set_output_names(names);
        Ok(Some(LogicalPlan::UnionAll(union)))
    }

    /// Go `buildUnion(ctx, selects, afterSetOpts)`
    /// (`logical_plan_builder.go:2317`).
    ///
    /// The DISTINCT prefix becomes one `UnionAll` under an aggregation
    /// ([`super::aggregation::PlanBuilder::build_distinct`], which is Go's
    /// `buildDistinct`); the `ALL` suffix becomes a second `UnionAll` with
    /// that aggregation as its FIRST child. Go's comment on that order —
    /// "Can't change the statements order in order to get the correct column
    /// info" — is the reason the de-duplicated half leads.
    ///
    /// # Errors
    ///
    /// `ErrWrongNumberOfColumnsInSelect`, or any branch's own error.
    pub fn build_union(
        &mut self,
        mut selects: Vec<LogicalPlan>,
        after_set_opts: &[Option<SetOp>],
    ) -> Result<LogicalPlan, PlanError> {
        if selects.len() == 1 {
            return Ok(selects.pop().expect("length is one"));
        }
        let (distinct_select_plans, mut all_select_plans) =
            divide_union_select_plans(selects, after_set_opts)?;
        let mut union_distinct_plan = self.build_union_all(distinct_select_plans)?;
        if let Some(plan) = union_distinct_plan.take() {
            let length = schema_len(&plan);
            let distinct = self.build_distinct(plan, length)?;
            if all_select_plans.is_empty() {
                union_distinct_plan = Some(distinct);
            } else {
                all_select_plans.insert(0, distinct);
            }
        }

        let union_all_plan = self.build_union_all(all_select_plans)?;
        match union_all_plan {
            Some(plan) => Ok(plan),
            // Go returns `unionDistinctPlan` here; a caller that reaches this
            // line with neither half would have `nil` in Go, which
            // `buildExcept`/`buildSetOpr` immediately dereference. Both halves
            // empty means `selects` was empty, and the `len == 1` guard above
            // plus `buildSetOpr`'s "at least one term" grammar make that
            // unreachable.
            None => union_distinct_plan.ok_or_else(|| {
                PlanError::internal("buildUnion was given no branch to build (unreachable)")
            }),
        }
    }

    /// Go `buildSemiJoinForSetOperator(leftOriginPlan, rightPlan, joinType)`
    /// (`logical_plan_builder.go:2201`): `INTERSECT` as a `SemiJoin` and
    /// `EXCEPT` as an `AntiSemiJoin`, both over a DE-DUPLICATED left side.
    ///
    /// The left side is de-duplicated FIRST, which is what makes `INTERSECT`
    /// and `EXCEPT` set operations rather than row-multiplicity ones — Go has
    /// no `INTERSECT ALL`/`EXCEPT ALL` precisely because this shape cannot
    /// express them.
    ///
    /// Every column pair is matched with `<=>` (`nulleq`), so two NULLs match:
    /// `SELECT NULL INTERSECT SELECT NULL` has one row.
    ///
    /// # Errors
    ///
    /// `build_distinct`'s error, or a `nulleq` construction error.
    pub fn build_semi_join_for_set_operator(
        &mut self,
        left_origin_plan: LogicalPlan,
        right_plan: LogicalPlan,
        join_type: LogicalJoinType,
    ) -> Result<LogicalPlan, PlanError> {
        let length = schema_len(&left_origin_plan);
        let left_plan = self.build_distinct(left_origin_plan, length)?;

        let (left_schema, left_names) = snapshot_schema_and_names(&left_plan);
        let (right_schema, _) = snapshot_schema_and_names(&right_plan);

        let mut join = LogicalJoin::new(self.base(LogicalJoin::TYPE), join_type);
        let builder = RealFunctionBuilder::new(self.ctx);
        for index in 0..right_schema.columns.len() {
            let (Some(left_col), Some(right_col)) = (
                left_schema.columns.get(index),
                right_schema.columns.get(index),
            ) else {
                return Err(wrong_number_of_columns());
            };
            let eq_cond = builder
                .new_function(
                    "nulleq",
                    Some(FieldType::new(FieldTypeCode::Tiny)),
                    vec![
                        Expression::Column(left_col.clone()),
                        Expression::Column(right_col.clone()),
                    ],
                )
                .map_err(|error| PlanError::internal(format!("{error:?}")))?;
            let Expression::ScalarFunction(sf) = eq_cond else {
                // Go asserts the result IS a `*ScalarFunction`. A folded
                // result cannot be a join KEY, so it goes to the general
                // conditions — the safe side.
                join.other_conditions.push(eq_cond);
                continue;
            };
            let same_code = left_col
                .ret_type
                .as_ref()
                .map(FieldType::code)
                .zip(right_col.ret_type.as_ref().map(FieldType::code))
                .is_some_and(|(left, right)| left == right);
            if !same_code || is_col_op_col(&sf).is_none() {
                join.other_conditions.push(Expression::ScalarFunction(sf));
            } else {
                join.equal_conditions.push(sf);
            }
        }

        join.base.set_children(vec![left_plan, right_plan]);
        join.base.base.set_schema(Some(left_schema));
        join.base.base.set_output_names(left_names);
        Ok(LogicalPlan::Join(join))
    }

    /// Go `buildIntersect(ctx, selects)` (`logical_plan_builder.go:2234`):
    /// "It is called before buildExcept and buildUnion because of its higher
    /// precedence."
    ///
    /// Returns the group's plan and the operator that joins the GROUP to what
    /// precedes it — Go's `selects[0].AfterSetOperator`, which
    /// [`Self::build_except`] then reads.
    ///
    /// # Errors
    ///
    /// `INTERSECT ALL` (Go's own refusal), `ErrWrongNumberOfColumnsInSelect`,
    /// or a branch's error.
    pub fn build_intersect(
        &mut self,
        terms: &[&SetOprTerm],
    ) -> Result<(LogicalPlan, Option<SetOp>), PlanError> {
        let after_set_operator = terms[0].op;
        let mut left_plan = self.build_set_opr_term(&terms[0].body)?;
        if terms.len() == 1 {
            return Ok((left_plan, after_set_operator));
        }

        let column_nums = schema_len(&left_plan);
        for term in &terms[1..] {
            if matches!(term.op, Some(SetOp::Intersect { all: true })) {
                // Go: "TODO: support intersect all".
                return Err(PlanError::internal("TiDB do not support intersect all"));
            }
            let right_plan = self.build_set_opr_term(&term.body)?;
            if schema_len(&right_plan) != column_nums {
                return Err(wrong_number_of_columns());
            }
            left_plan = self.build_semi_join_for_set_operator(
                left_plan,
                right_plan,
                LogicalJoinType::Semi,
            )?;
        }
        Ok((left_plan, after_set_operator))
    }

    /// Go's per-element `switch x := selects[i].(type)` in `buildIntersect`
    /// (`:2239` and `:2258`).
    ///
    /// Go's `*ast.SetOprSelectList` arm re-wraps the nested list into
    /// `&ast.SetOprStmt{SelectList: x, With: x.With, Limit: x.Limit,
    /// OrderBy: x.OrderBy}`; [`SetOprTermBody::Nested`] already IS that
    /// statement, so the wrap has no counterpart here.
    ///
    /// # Errors
    ///
    /// The branch's own build error.
    pub(super) fn build_set_opr_term(
        &mut self,
        body: &SetOprTermBody,
    ) -> Result<LogicalPlan, PlanError> {
        match body {
            SetOprTermBody::Select(select) => self.build_select(select).map(|(plan, _)| plan),
            SetOprTermBody::Nested(nested) => self.build_set_opr(nested),
        }
    }

    /// Go `buildExcept(ctx, selects, afterSetOpts)`
    /// (`logical_plan_builder.go:2286`): "in this function, it calls buildUnion
    /// at the same time. Because Union and except has the same precedence."
    ///
    /// The accumulator is a RUN of union branches. An `EXCEPT` closes the run —
    /// everything to its left is unioned first and becomes the anti-semi
    /// join's left side — and anything else extends it.
    ///
    /// # Errors
    ///
    /// `EXCEPT ALL` (Go's own refusal), `ErrWrongNumberOfColumnsInSelect`, or
    /// a nested build's error.
    pub fn build_except(
        &mut self,
        selects: Vec<LogicalPlan>,
        after_set_opts: &[Option<SetOp>],
    ) -> Result<LogicalPlan, PlanError> {
        let mut selects = selects.into_iter();
        let first = selects
            .next()
            .ok_or_else(|| PlanError::internal("buildExcept was given no branch"))?;
        let column_nums = schema_len(&first);
        let mut union_plans = vec![first];
        let mut tmp_after_set_opts: Vec<Option<SetOp>> = vec![None];

        for (index, right_plan) in selects.enumerate().map(|(i, plan)| (i + 1, plan)) {
            if schema_len(&right_plan) != column_nums {
                return Err(wrong_number_of_columns());
            }
            match after_set_opts.get(index).copied().flatten() {
                Some(SetOp::Except { all: false }) => {
                    let left_plan =
                        self.build_union(std::mem::take(&mut union_plans), &tmp_after_set_opts)?;
                    let left_plan = self.build_semi_join_for_set_operator(
                        left_plan,
                        right_plan,
                        LogicalJoinType::AntiSemi,
                    )?;
                    union_plans = vec![left_plan];
                    tmp_after_set_opts = vec![None];
                }
                Some(SetOp::Except { all: true }) => {
                    // Go: "TODO: support except all".
                    return Err(PlanError::internal("TiDB do not support except all"));
                }
                other => {
                    union_plans.push(right_plan);
                    tmp_after_set_opts.push(other);
                }
            }
        }
        self.build_union(union_plans, &tmp_after_set_opts)
    }

    /// Go `buildSetOpr(ctx, setOpr)` (`logical_plan_builder.go:2108`): the
    /// whole statement.
    ///
    /// The stage order is Go's, and the first stage is the reason this
    /// function is not just "fold the terms left to right": INTERSECT binds
    /// TIGHTER than UNION and EXCEPT, so the terms are first cut into
    /// intersect GROUPS, each group built by [`Self::build_intersect`], and
    /// only then does [`Self::build_except`] fold the groups.
    ///
    /// `a UNION b INTERSECT c` is therefore `a UNION (b INTERSECT c)`, and the
    /// operator that reaches `buildExcept` for the second group is the one
    /// written before the group's FIRST term — the `UNION`, not the
    /// `INTERSECT`.
    ///
    /// # Errors
    ///
    /// Any stage's error.
    pub fn build_set_opr(&mut self, set_opr: &SetOprStmt) -> Result<LogicalPlan, PlanError> {
        // boundary: `buildSelectLock` (`planbuilder.go:1610`). 6b ported it
        // for `buildSelect`; a set operation's own `FOR UPDATE` is Go's
        // `ast.SetOprStmt`-level lock, which `buildSetOpr` does NOT read — it
        // is applied by `PlanBuilder.Build`'s statement wrapper, which is not
        // in this crate. Refusing keeps a locking read from silently becoming
        // a non-locking one.
        if set_opr.lock.is_some() || set_opr.outer_lock.is_some() {
            return Err(PlanError::internal(
                "a locking clause on a set operation needs buildSelectLock's statement wrapper (planbuilder.go:1610)",
            ));
        }
        // `:2110` the WITH clause. `outerCTEs` is truncated back on EVERY exit
        // path, which is Go's `defer`.
        let outer_cte_depth = self.outer_ctes.len();
        let current_layer_ctes = match &set_opr.with {
            Some(with) => match self.build_with(with) {
                Ok(ctes) => ctes,
                Err(error) => {
                    self.outer_ctes.truncate(outer_cte_depth);
                    return Err(error);
                }
            },
            None => Vec::new(),
        };
        let result = self.build_set_opr_body(set_opr, &current_layer_ctes);
        self.outer_ctes.truncate(outer_cte_depth);
        result
    }

    /// [`Self::build_set_opr`] past its `WITH` prologue, so that the
    /// `outerCTEs` truncation is one `defer`-shaped wrapper rather than a
    /// restore before each `return`.
    fn build_set_opr_body(
        &mut self,
        set_opr: &SetOprStmt,
        current_layer_ctes: &[usize],
    ) -> Result<LogicalPlan, PlanError> {
        // `:2123` "Because INTERSECT has higher precedence than UNION and
        // EXCEPT. We build it first."
        let terms = &set_opr.terms;
        let mut select_plans = Vec::with_capacity(terms.len());
        let mut after_set_oprs = Vec::with_capacity(terms.len());
        let mut index = 0;
        while index < terms.len() {
            let start = index;
            while index + 1 < terms.len() && !breaks_intersect_run(&terms[index + 1]) {
                index += 1;
            }
            let group: Vec<&SetOprTerm> = terms[start..=index].iter().collect();
            let (select_plan, after_set_opr) = self.build_intersect(&group)?;
            select_plans.push(select_plan);
            after_set_oprs.push(after_set_opr);
            index += 1;
        }
        let mut set_opr_plan = self.build_except(select_plans, &after_set_oprs)?;

        let old_len = schema_len(&set_opr_plan);

        // `:2160` one `handleHelper` entry per WRITTEN term is popped, and one
        // empty entry stands for the set operation itself.
        for _ in terms {
            self.handle_helper.pop_map();
        }
        self.handle_helper.push_empty();

        // `:2166` ORDER BY, then `:2173` LIMIT.
        //
        // Go has ONE `OrderBy`/`Limit` pair, because its parser folds a
        // parenthesized statement into a nested `ast.SetOprSelectList` and
        // attaches the outer tail to the enclosing `ast.SetOprStmt`.
        // [`tidb_ast::SetOprStmt`] keeps one node and splits the tail into
        // `order_by`/`limit` (inside the braces) and
        // `outer_order_by`/`outer_limit` (outside them), so the OUTER pair is
        // applied as a second layer above the inner one. That is the same
        // operator sequence Go's nesting produces, over an AST that spells the
        // nesting differently.
        for (items, limit) in [
            (&set_opr.order_by, set_opr.limit.as_ref()),
            (&set_opr.outer_order_by, set_opr.outer_limit.as_ref()),
        ] {
            if !items.is_empty() {
                set_opr_plan = self.build_sort(set_opr_plan, items, &Default::default())?;
            }
            if let Some(limit) = limit {
                set_opr_plan = self.build_limit(set_opr_plan, limit)?;
            }
        }

        // `:2183` "Fix issue #8189. If there are extra expressions generated
        // from `ORDER BY` clause, generate a `Projection` to remove them."
        if old_len != schema_len(&set_opr_plan) {
            set_opr_plan = self.build_set_opr_trim_projection(set_opr_plan, old_len);
        }
        Ok(self.try_to_build_sequence(current_layer_ctes, set_opr_plan))
    }

    /// `buildSetOpr`'s own trailing projection (`logical_plan_builder.go:2183`).
    ///
    /// Deliberately NOT [`PlanBuilder::build_trim_projection`], which is
    /// `buildSelect`'s (`:4640`): this one RE-ALLOCATES every surviving
    /// column's unique ID, because the columns it trims back to are the
    /// union's own and a second reference to them under a different operator
    /// must not share their identity.
    fn build_set_opr_trim_projection(&mut self, plan: LogicalPlan, old_len: usize) -> LogicalPlan {
        let (schema, names) = snapshot_schema_and_names(&plan);
        let exprs: Vec<Expression> = schema
            .columns
            .iter()
            .take(old_len)
            .cloned()
            .map(Expression::Column)
            .collect();
        let mut kept_columns: Vec<Column> = schema.columns.into_iter().take(old_len).collect();
        for column in &mut kept_columns {
            column.unique_id = self.column_ids.alloc();
        }
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
}

/// Go `buildSetOpr`'s `breakIteration` (`logical_plan_builder.go:2130`): does
/// this term END the intersect run that precedes it?
///
/// Go's two arms:
/// * `*ast.SelectStmt` — break unless the operator is `INTERSECT` /
///   `INTERSECT ALL`.
/// * `*ast.SetOprSelectList` — the same, PLUS "when SetOprSelectList's limit
///   and order-by is not nil, it means itself is converted from an independent
///   ast.SetOprStmt in parser, its data should be evaluated first, and ordered
///   by given items and conduct a limit on it, then it can only be integrated
///   with other brothers."
///
/// [`tidb_ast::SetOprStmt`] splits that tail across `limit`/`order_by` (written
/// inside the term's own parentheses) and `outer_limit`/`outer_order_by`
/// (written outside them). Both spellings are the same independent evaluation
/// Go's comment describes, so both break the run.
fn breaks_intersect_run(term: &SetOprTerm) -> bool {
    if !matches!(term.op, Some(SetOp::Intersect { .. })) {
        return true;
    }
    match &term.body {
        SetOprTermBody::Select(_) => false,
        SetOprTermBody::Nested(nested) => {
            nested.limit.is_some()
                || !nested.order_by.is_empty()
                || nested.outer_limit.is_some()
                || !nested.outer_order_by.is_empty()
        }
    }
}

/// Go `collate.ErrIllegalMixCollation.GenWithStackByArgs("UNION")` (MySQL 1271).
fn illegal_mix_collation() -> PlanError {
    PlanError::internal("Illegal mix of collations for operation 'UNION'")
}
