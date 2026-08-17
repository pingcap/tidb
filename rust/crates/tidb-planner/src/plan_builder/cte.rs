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

//! `WITH` and `WITH RECURSIVE`: the common-table-expression half of the
//! logical plan builder.
//!
//! Go source, all `pkg/planner/core/logical_plan_builder.go`:
//!
//! | Here | Go |
//! | --- | --- |
//! | [`PlanBuilder::build_with`] | `buildWith` (:7994) |
//! | [`PlanBuilder::build_cte`] | `buildCte` (:7714) |
//! | [`PlanBuilder::build_recursive_cte`] | `buildRecursiveCTE` (:7750) |
//! | [`PlanBuilder::adjust_cte_plan_output_name`] | `adjustCTEPlanOutputName` (:7916) |
//! | [`PlanBuilder::build_projection4_cte_union`] | `buildProjection4CTEUnion` (:8029) |
//! | [`PlanBuilder::get_result_cte_schema`] | `getResultCTESchema` (:8049) |
//! | [`PlanBuilder::try_build_cte`] | `tryBuildCTE` (:4739) |
//! | [`PlanBuilder::compute_cte_inline_flag`] | `computeCTEInlineFlag` (:4875) |
//! | [`PlanBuilder::build_data_source_from_cte_merge`] | `buildDataSourceFromCTEMerge` (:4903) |
//! | [`PlanBuilder::try_to_build_sequence`] | `tryToBuildSequence` (:4624) |
//! | [`PlanBuilder::prepare_cte_check_for_subquery`] | `prepareCTECheckForSubQuery` (:7948) |
//! | [`PlanBuilder::reset_cte_check_for_subquery`] | `resetCTECheckForSubQuery` (:7961) |
//! | [`PlanBuilder::gen_cte_table_name_for_error`] | `genCTETableNameForError` (:7969) |
//! | [`PlanBuilder::build_query_stmt`] | `buildResultSetNode`'s `*ast.SelectStmt` / `*ast.SetOprStmt` arms (:577-580) |
//!
//! This module LANDS AS A COMPLETE PACKAGE for the fourteen symbols above,
//! **except** for the two narrowings marked NARROWED below, which are
//! dependencies absent from this workspace rather than bodies left out. The
//! recursive path in particular is ported WHOLE — the seed/recursive split,
//! the rebuild, the `handleHelper` repair, and every one of Go's five refusals.
//!
//! # 1. The `cteInfo` bookkeeping is on the builder, addressed by INDEX
//!
//! Go's `buildWith` returns `[]*cteInfo` and hands the same pointers to
//! `tryToBuildSequence`; `tryBuildCTE` mutates `b.outerCTEs[i]` through
//! another alias while a build is in flight. A `&mut` alias into
//! [`PlanBuilder::outer_ctes`] cannot coexist with the `&mut self` every
//! builder call needs, so every function here takes an INDEX into that vector
//! and re-borrows for each field access. [`PlanBuilder::build_with`] therefore
//! returns `Vec<usize>` where Go returns `[]*cteInfo`.
//!
//! That is a mechanical change, not a semantic one: the vector is only ever
//! appended to and truncated at a scope boundary, so an index is stable for
//! exactly as long as Go's pointer is live.
//!
//! # 2. How the recursive split is DISCOVERED
//!
//! Go does not parse out the recursive term. It BUILDS each term in order and
//! watches [`super::OuterCte::use_recursive`], which
//! [`PlanBuilder::try_build_cte`] sets when the term referenced the CTE being
//! built. The first term that sets it is the recursive part, and at that
//! moment the seed has not been built yet — so Go re-enters
//! `buildSetOpr` over the terms BEFORE it, stores that as the seed, and then
//! builds the same term AGAIN, this time with
//! [`PlanBuilder::building_recursive_part_for_cte`] set so that the reference
//! resolves to a [`LogicalCTETable`].
//!
//! The failed first attempt is why the `handleHelper` repair loop exists:
//! `buildSelect` promises exactly one pushed entry and does not keep that
//! promise when it fails halfway, so the stack is unwound by DEPTH rather than
//! by one `pop`.
//!
//! # 3. Every refusal in the recursive path, and why each is load-bearing
//!
//! These are Go's, and each one prevents a plan that would silently compute
//! the wrong fixpoint:
//!
//! * `ErrCTERecursiveRequiresNonRecursiveFirst` — the FIRST term referenced
//!   the CTE, so there is no seed to start from.
//! * `ErrCTERecursiveRequiresUnion` — the body is not a set operation at all,
//!   so there is no recursive term to iterate.
//! * `ErrNotSupportedYet("ORDER BY over UNION in recursive ...")` — an
//!   ordering over an iteration that has no defined order.
//! * `ErrNotSupportedYet("<op> between seed part and recursive part ...")` and
//!   the same between recursive terms — only `UNION` / `UNION ALL` define the
//!   fixpoint; `INTERSECT`/`EXCEPT` do not.
//! * `ErrInvalidRequiresSingleReference` — more than one reference to the CTE
//!   inside its own recursive term, or a reference from inside a subquery.
//!
//! # Narrowings, by exact Go symbol
//!
//! * NARROWED: `ast.CommonTableExpression.ConsumerCount`, written by
//!   `UpdateCTEConsumerCount` in the PREPROCESS phase.
//!   [`tidb_ast::Cte`] has no such field and this crate has no preprocess
//!   pass, so [`PlanBuilder::compute_cte_inline_flag`] reads a
//!   [`super::OuterCte::consumer_count`] that is always `0`. Go's own comment
//!   covers exactly that case ("Case the consumer count = 0 (issue #56582) ...
//!   we can not use it to determine whether CTE can be inlined") and takes the
//!   NOT-inlined arm, which is the safe one: the CTE is materialised into its
//!   own storage rather than textually merged. The rest of the function —
//!   including the `forceInlineByHintOrVar` override, which
//!   `EnableForceInlineCTE` still drives — is present and reachable.
//! * NARROWED: `cteInfo.limitLP base.LogicalPlan` becomes
//!   [`super::OuterCte::limit_bounds`], a `(beg, end)` pair.
//!   `tryBuildCTE` reads NOTHING else off that plan — its whole `switch` exists
//!   to extract `LimitBeg`/`LimitEnd` — and building it in Go's own way would
//!   have to move the seed plan into a `LogicalLimit` and take it back out
//!   again. The `LIMIT 0` arm (Go's `*logicalop.LogicalTableDual`, "Beg and
//!   End will both be 0") is reproduced by the pair `(0, 0)`.
//! * `b.ctx.GetSessionVars().StmtCtx.SetHintWarning` / `AppendWarning` in
//!   `computeCTEInlineFlag`. There is no statement-context warning channel in
//!   this crate; the DECISION each warning accompanies is unchanged.
//! * `cteInfo.cteClass.ColumnMap`'s `HashCode` keys are filled here exactly as
//!   Go fills them, through [`Column::hash_code`].
//! * DECLINED: `setIsInApplyForCTE(p, apply)` (`logical_plan_builder.go:5763`),
//!   which walks a just-built subtree marking every `LogicalCTE`'s
//!   `IsInApply`. It belongs to `buildLateralJoin` (6b) rather than to any
//!   symbol in this batch, its only reader is the physical CTE executor, and
//!   the walk it needs is an OWNED rewrite through
//!   [`crate::logical::fold`] rather than the recursive descent Go writes.
//!   [`crate::logical::cte::CteClass::is_in_apply`] therefore stays `false`;
//!   it is named here because 6b's header pointed at this batch for it.
//! * `b.buildingLateralSubquery`. `buildSelect`'s recursive-part guard reads
//!   it to allow `ORDER BY`/`LIMIT` inside a `LATERAL` body; 6b's
//!   `build_lateral_join` does not set such a flag, so the guard here takes
//!   the strict arm — which REFUSES more than Go, never less.

use std::cell::RefCell;
use std::rc::Rc;

use tidb_ast::{Cte, QueryStmt, SetOp, SetOprStmt, WithClause};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags, IdentifierMetadata};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use crate::logical::cte::{CteClass, LogicalCTE, LogicalCTETable};
use crate::logical::rule::flags;
use crate::logical::sequence::LogicalSequence;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanError;
use crate::stats_info::StatsInfo;

use super::catalog::TableSource;
use super::PlanBuilder;

/// Go `SetOutputNames` as `LogicalSchemaProducer` OVERRIDES it
/// (`logical_schema_producer.go:44`), rather than as `BaseLogicalPlan`
/// forwards it (`base_logical_plan.go:112`).
///
/// [`LogicalPlan::set_output_names`] models only the forwarding half, so on a
/// `Projection` — which is what a CTE body always ends in — it would write the
/// names onto the CHILD while [`LogicalPlan::output_names`] keeps reading the
/// projection's own. Go's `*LogicalProjection` embeds a schema producer and
/// therefore takes the OWNING arm; this helper picks the same arm by the same
/// test `output_names` uses.
fn set_own_output_names(plan: &mut LogicalPlan, names: Vec<tidb_datatype::FieldName>) {
    if plan.base().base.output_names().is_empty() {
        plan.set_output_names(names);
    } else {
        plan.base_mut().base.set_output_names(names);
    }
}

/// Go `plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst` (MySQL 3577).
fn err_recursive_requires_non_recursive_first(name: &str) -> PlanError {
    PlanError::internal(format!(
        "Recursive Common Table Expression '{name}' should have one or more non-recursive query blocks followed by one or more recursive ones"
    ))
}

/// Go `plannererrors.ErrCTERecursiveRequiresUnion` (MySQL 3574).
fn err_recursive_requires_union(name: &str) -> PlanError {
    PlanError::internal(format!(
        "Recursive Common Table Expression '{name}' can contain neither aggregation nor window functions in recursive query block"
    ))
}

/// Go `plannererrors.ErrInvalidRequiresSingleReference` (MySQL 3575).
fn err_invalid_requires_single_reference(name: &str) -> PlanError {
    PlanError::internal(format!(
        "In recursive query block of Recursive Common Table Expression '{name}', the recursive table must be referenced only once, and not in any subquery"
    ))
}

/// Go `plannererrors.ErrNotSupportedYet` (MySQL 1235).
fn err_not_supported_yet(what: &str) -> PlanError {
    PlanError::internal(format!("This version of TiDB doesn't yet support '{what}'"))
}

/// Go `plannererrors.ErrNonUniqTable` (MySQL 1066).
fn err_non_uniq_table() -> PlanError {
    PlanError::internal("Not unique table/alias")
}

/// Go `dbterror.ErrViewWrongList` (MySQL 1353).
fn err_view_wrong_list() -> PlanError {
    PlanError::internal("View's SELECT and view's field list have different column counts")
}

/// Whether `op` is one of the two operators a recursive CTE's fixpoint is
/// defined for; Go writes `*afterOpr != ast.Union && *afterOpr != ast.UnionAll`.
const fn is_union_operator(op: SetOp) -> bool {
    matches!(op, SetOp::Union { .. })
}

/// Go `ast.SetOprType.String()` for the message the two refusals interpolate.
const fn set_op_name(op: SetOp) -> &'static str {
    match op {
        SetOp::Union { all: false } => "UNION",
        SetOp::Union { all: true } => "UNION ALL",
        SetOp::Except { all: false } => "EXCEPT",
        SetOp::Except { all: true } => "EXCEPT ALL",
        SetOp::Intersect { all: false } => "INTERSECT",
        SetOp::Intersect { all: true } => "INTERSECT ALL",
    }
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildResultSetNode`'s two statement arms (`:577-580`), which is how
    /// a CTE body, a derived table and a set-operation term all reach a plan.
    ///
    /// Go sets `b.isCTE = isCTE` at the TOP of `buildResultSetNode`, before the
    /// switch, so the flag is set here too.
    ///
    /// # Errors
    ///
    /// The statement's own build error.
    pub fn build_query_stmt(
        &mut self,
        query: &QueryStmt,
        is_cte: bool,
    ) -> Result<LogicalPlan, PlanError> {
        self.is_cte = is_cte;
        match query {
            QueryStmt::Select(select) => self.build_select(select).map(|(plan, _)| plan),
            QueryStmt::SetOpr(set_opr) => self.build_set_opr(set_opr),
        }
    }

    /// Go `buildWith(ctx, w)` (`logical_plan_builder.go:7994`).
    ///
    /// Returns the INDEX of each CTE in [`Self::outer_ctes`]; see this
    /// module's section 1 for why an index and not a handle.
    ///
    /// Note what Go does with `optFlag` around each CTE: it is RESET to
    /// `FlagPruneColumns` for the duration ("Init the flag to flagPrunColumns,
    /// otherwise it's missing"), captured onto the `cteInfo`, and the outer
    /// flag restored. A CTE is optimised as its own plan, so its flags travel
    /// with it rather than with the statement that references it.
    ///
    /// # Errors
    ///
    /// `ErrNonUniqTable` for a duplicated name, or any CTE body's error.
    pub fn build_with(&mut self, with: &WithClause) -> Result<Vec<usize>, PlanError> {
        // "Check CTE name must be unique."
        self.name_map_cte.clear();
        for cte in &with.ctes {
            if !self.name_map_cte.insert(cte.name.to_lowercase()) {
                return Err(err_non_uniq_table());
            }
        }
        let mut ctes = Vec::with_capacity(with.ctes.len());
        for cte in &with.ctes {
            let index = self.outer_ctes.len();
            self.outer_ctes.push(super::OuterCte {
                name: cte.name.to_lowercase(),
                name_original: cte.name.clone(),
                col_name_list: cte.columns.clone(),
                definition: Some((*cte.query).clone()),
                non_recursive: !with.recursive,
                is_building: true,
                storage_id: self.alloc_id_for_cte_storage,
                seed_stat: Rc::new(RefCell::new(StatsInfo::new(0.0, []))),
                force_inline_by_hint_or_var: self.enable_force_inline_cte,
                ..super::OuterCte::default()
            });
            self.alloc_id_for_cte_storage += 1;
            let save_flag = self.opt_flag;
            self.opt_flag = flags::PRUNE_COLUMNS;
            let result = self.build_cte(cte, with.recursive, index);
            self.outer_ctes[index].opt_flag = self.opt_flag;
            self.outer_ctes[index].is_building = false;
            self.opt_flag = save_flag;
            result?;
            // "buildCte() will push one entry into handleHelper. ... building
            // CTE should not affect the handleColHelper, so we pop it out
            // here, then buildWith() as a whole will not modify the
            // handleColHelper."
            self.handle_helper.pop_map();
            ctes.push(index);
        }
        Ok(ctes)
    }

    /// Go `buildCte(ctx, cte, isRecursive)` (`logical_plan_builder.go:7714`):
    /// "It works together with buildWith(). It will push one entry into
    /// b.handleHelper."
    ///
    /// Go returns `(nil, nil)` unconditionally — the built plan is stored on
    /// the `cteInfo`, never returned — so this returns `()`.
    ///
    /// # Errors
    ///
    /// The body's own error.
    fn build_cte(&mut self, cte: &Cte, is_recursive: bool, index: usize) -> Result<(), PlanError> {
        let save_building_cte = self.building_cte;
        self.building_cte = true;
        let result = if is_recursive {
            // "buildingRecursivePartForCTE likes a stack. ... We need a stack
            // because we need to handle the nested recursive CTE, and
            // buildingRecursivePartForCTE indicates the innermost CTE."
            let save_check = self.building_recursive_part_for_cte;
            self.building_recursive_part_for_cte = false;
            let result = self.build_recursive_cte(&cte.query, index);
            self.building_recursive_part_for_cte = save_check;
            result
        } else {
            self.build_query_stmt(&cte.query, true)
                .and_then(|mut plan| {
                    self.adjust_cte_plan_output_name(&mut plan, index)?;
                    self.outer_ctes[index].seed_lp = Some(Box::new(plan));
                    Ok(())
                })
        };
        self.building_cte = save_building_cte;
        result
    }

    /// Go `buildRecursiveCTE(ctx, cte)` (`logical_plan_builder.go:7750`):
    /// "handles the with clause `with recursive xxx as xx`".
    ///
    /// See this module's sections 2 and 3 — the discovery loop and every
    /// refusal it makes — before changing anything here.
    ///
    /// # Errors
    ///
    /// Any of section 3's five refusals, or a term's own error.
    fn build_recursive_cte(&mut self, query: &QueryStmt, index: usize) -> Result<(), PlanError> {
        self.is_cte = true;
        let QueryStmt::SetOpr(set_opr) = query else {
            // Go's `default` arm: a non-set-operation body is not recursive,
            // and its `ErrCTERecursiveRequiresNonRecursiveFirst` is REFINED
            // into `ErrCTERecursiveRequiresUnion` — the body has no `UNION` to
            // put a recursive term in.
            let name = self.outer_ctes[index].name_original.clone();
            let mut plan = self.build_query_stmt(query, true).map_err(|error| {
                if error == err_recursive_requires_non_recursive_first(&name) {
                    err_recursive_requires_union(&name)
                } else {
                    error
                }
            })?;
            self.adjust_cte_plan_output_name(&mut plan, index)?;
            self.outer_ctes[index].seed_lp = Some(Box::new(plan));
            return Ok(());
        };

        // 1. "Handle the WITH clause if exists." Go additionally sets
        // `x.With = nil` so the seed rebuild below does not build it twice,
        // and restores it in a `defer`; the AST is not mutated here, so the
        // working statement simply drops it.
        let outer_depth = self.outer_ctes.len();
        let mut stmt = SetOprStmt {
            with: None,
            ..(**set_opr).clone()
        };
        if let Some(with) = &set_opr.with {
            if let Err(error) = self.build_with(with) {
                self.outer_ctes.truncate(outer_depth);
                return Err(error);
            }
        }
        let result = self.build_recursive_cte_terms(&mut stmt, index);
        self.outer_ctes.truncate(outer_depth);
        result
    }

    /// [`Self::build_recursive_cte`]'s step 2 onwards, so that the nested
    /// `WITH`'s scope truncation is one `defer`-shaped wrapper.
    fn build_recursive_cte_terms(
        &mut self,
        stmt: &mut SetOprStmt,
        index: usize,
    ) -> Result<(), PlanError> {
        let name = self.outer_ctes[index].name_original.clone();
        // 2. "Build plans for each part of SetOprStmt."
        let mut recursive: Vec<LogicalPlan> = Vec::new();
        // Go seeds this with a single `nil` and appends one entry per
        // recursive part, so it ends up ONE longer than `recursive`; the
        // shift is Go's own and `divideUnionSelectPlans` reads it as-is.
        let mut tmp_after_set_opts_for_recur: Vec<Option<SetOp>> = vec![None];

        let mut expect_seed = true;
        let mut term_index = 0;
        while term_index < stmt.terms.len() {
            let original_depth = self.handle_helper.depth();
            let after_opr = stmt.terms[term_index].op;
            let built = self.build_set_opr_term(&stmt.terms[term_index].body);

            // "This is for maintain b.handleHelper instead of normal error
            // handling. Since one error is expected if expectSeed &&
            // cInfo.useRecursive ..."
            let (plan, error) = match built {
                Ok(plan) => {
                    self.handle_helper.pop_map();
                    (Some(plan), None)
                }
                Err(error) => {
                    // "Be careful with this tricky case. ... This violates the
                    // semantic of buildSelect() and buildSetOpr(), which should
                    // only push exactly one entry into b.handleHelper."
                    while self.handle_helper.depth() > original_depth {
                        self.handle_helper.pop_map();
                    }
                    (None, Some(error))
                }
            };

            if expect_seed {
                if self.outer_ctes[index].use_recursive {
                    if let Some(plan) = plan {
                        plan.dismantle();
                    }
                    // 3. "If it fail to build a plan, it may be the recursive
                    // part. Then we build the seed part plan, and rebuild it."
                    if term_index == 0 {
                        return Err(err_recursive_requires_non_recursive_first(&name));
                    }
                    if !stmt.order_by.is_empty() || !stmt.outer_order_by.is_empty() {
                        return Err(err_not_supported_yet(
                            "ORDER BY over UNION in recursive Common Table Expression",
                        ));
                    }
                    // "Limit clause is for the whole CTE instead of only for
                    // the seed part."
                    let ori_limit = stmt.limit.take();
                    let ori_outer_limit = stmt.outer_limit.take();

                    // "Check union type."
                    if let Some(op) = after_opr {
                        if !is_union_operator(op) {
                            return Err(err_not_supported_yet(&format!(
                                "{} between seed part and recursive part, hint: The operator between seed part and recursive part must bu UNION[DISTINCT] or UNION ALL",
                                set_op_name(op)
                            )));
                        }
                        self.outer_ctes[index].is_distinct = op == SetOp::Union { all: false };
                    }

                    expect_seed = false;
                    self.outer_ctes[index].use_recursive = false;

                    // "Build seed part plan." Go slices `x.SelectList.Selects`
                    // down to `[:i]`, builds, and restores the slice.
                    let seed_stmt = SetOprStmt {
                        with: None,
                        terms: stmt.terms[..term_index].to_vec(),
                        limit: None,
                        outer_limit: None,
                        ..stmt.clone()
                    };
                    let mut seed = self.build_set_opr(&seed_stmt)?;
                    self.handle_helper.pop_map();
                    self.adjust_cte_plan_output_name(&mut seed, index)?;
                    if let Some(previous) = self.outer_ctes[index].seed_lp.take() {
                        previous.dismantle();
                    }
                    self.outer_ctes[index].seed_lp = Some(Box::new(seed));

                    // "Rebuild the plan." Go's `i--; continue` re-enters the
                    // SAME term, now with the seed in place.
                    self.building_recursive_part_for_cte = true;
                    stmt.limit = ori_limit;
                    stmt.outer_limit = ori_outer_limit;
                    continue;
                }
                if let Some(error) = error {
                    return Err(error);
                }
                // Go does NOT keep this plan: the seed is rebuilt WHOLE from
                // `x.SelectList.Selects[:i]` once the recursive part is found,
                // or from the whole statement when there is none.
                if let Some(plan) = plan {
                    plan.dismantle();
                }
            } else {
                if let Some(error) = error {
                    return Err(error);
                }
                if let Some(op) = after_opr {
                    if !is_union_operator(op) {
                        return Err(err_not_supported_yet(&format!(
                            "{} between recursive part's selects, hint: The operator between recursive part's selects must bu UNION[DISTINCT] or UNION ALL",
                            set_op_name(op)
                        )));
                    }
                }
                if !self.outer_ctes[index].use_recursive {
                    return Err(err_recursive_requires_non_recursive_first(&name));
                }
                self.outer_ctes[index].use_recursive = false;
                if let Some(plan) = plan {
                    recursive.push(plan);
                }
                tmp_after_set_opts_for_recur.push(after_opr);
            }
            term_index += 1;
        }

        if recursive.is_empty() {
            // "In this case, even if SQL specifies 'WITH RECURSIVE', the CTE
            // is non-recursive."
            let whole = SetOprStmt {
                with: None,
                ..stmt.clone()
            };
            let mut plan = self.build_set_opr(&whole)?;
            self.adjust_cte_plan_output_name(&mut plan, index)?;
            if let Some(previous) = self.outer_ctes[index].seed_lp.take() {
                previous.dismantle();
            }
            self.outer_ctes[index].seed_lp = Some(Box::new(plan));
            return Ok(());
        }

        // "Build the recursive part's logical plan."
        let recur_part = self.build_union(recursive, &tmp_after_set_opts_for_recur)?;
        let seed_schema = self.outer_ctes[index]
            .seed_lp
            .as_ref()
            .and_then(|plan| plan.schema().cloned())
            .ok_or_else(|| err_recursive_requires_non_recursive_first(&name))?;
        let recur_part = self.build_projection4_cte_union(&seed_schema, recur_part)?;
        // 4. "Finally, we get the seed part plan and recursive part plan."
        self.outer_ctes[index].recur_lp = Some(Box::new(recur_part));
        // "Only need to handle limit if x is SetOprStmt." Go builds a
        // `LogicalLimit` over the seed and then DETACHES its child, keeping
        // the node only for its `Offset`/`Count`; see this module's
        // `limitLP` narrowing.
        let limit = stmt.limit.as_ref().or(stmt.outer_limit.as_ref());
        if let Some(limit) = limit {
            let offset = match &limit.offset {
                Some(expr) => Self::limit_value(expr)?,
                None => 0,
            };
            let mut count = Self::limit_value(&limit.count)?;
            if count > u64::MAX - offset {
                count = u64::MAX - offset;
            }
            self.outer_ctes[index].limit_bounds = Some(if offset.saturating_add(count) == 0 {
                (0, 0)
            } else {
                (offset, offset + count)
            });
        }
        self.handle_helper.push_empty();
        Ok(())
    }

    /// Go `adjustCTEPlanOutputName(p, def)` (`logical_plan_builder.go:7916`).
    ///
    /// Go clones the name slice first, with the comment "Clone output names to
    /// avoid mutating shared structs (important for LATERAL subqueries)"; the
    /// names are already an owned `Vec` here, so the clone is the read itself.
    ///
    /// # Errors
    ///
    /// `ErrViewWrongList` when the `WITH name(a, b)` column list does not match
    /// the body's arity.
    fn adjust_cte_plan_output_name(
        &mut self,
        plan: &mut LogicalPlan,
        index: usize,
    ) -> Result<(), PlanError> {
        let cte_name = self.outer_ctes[index].name_original.clone();
        let col_name_list = self.outer_ctes[index].col_name_list.clone();
        let current_db = self.source.current_database().to_owned();
        let mut names = plan.output_names().to_vec();
        for name in &mut names {
            name.names.table = IdentifierMetadata::new(&cte_name);
            if name.names.database.original.is_empty() {
                name.names.database = IdentifierMetadata::new(&current_db);
            }
        }
        if !col_name_list.is_empty() {
            if col_name_list.len() != names.len() {
                return Err(err_view_wrong_list());
            }
            for (name, new_name) in names.iter_mut().zip(&col_name_list) {
                name.names.column = IdentifierMetadata::new(new_name);
                name.names.original_column = IdentifierMetadata::new(new_name);
            }
        }
        set_own_output_names(plan, names);
        Ok(())
    }

    /// Go `getResultCTESchema(seedSchema, svar)`
    /// (`logical_plan_builder.go:8049`): "The recursive part/CTE's schema is
    /// nullable, and the UID should be unique."
    ///
    /// Both halves matter. A recursive term may produce NULL where the seed
    /// could not, so the NOT NULL flag comes off; and a second reference to
    /// the same CTE must not share column identities with the first, so every
    /// unique ID is re-allocated.
    #[must_use]
    pub fn get_result_cte_schema(&self, seed_schema: &Schema) -> Schema {
        let mut columns = seed_schema.columns.clone();
        for column in &mut columns {
            column.unique_id = self.column_ids.alloc();
            if let Some(ret_type) = column.ret_type.as_mut() {
                ret_type.del_flags(FieldTypeFlags::NOT_NULL);
            }
            // Go `col.CleanHashCode()`: the cached hash is stale once the
            // unique ID changed. `Column::new` starts with an empty cache, so
            // the rebuild below is that call.
            let mut fresh = Column::new(
                column.unique_id,
                column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified)),
            );
            fresh.id = column.id;
            fresh.index = column.index;
            fresh.orig_name = column.orig_name.clone();
            fresh.is_hidden = column.is_hidden;
            fresh.is_prefix = column.is_prefix;
            fresh.in_operand = column.in_operand;
            fresh.collation = column.collation.clone();
            fresh.correlated_col_unique_id = column.correlated_col_unique_id;
            fresh.virtual_expr = column.virtual_expr.clone();
            *column = fresh;
        }
        Schema::new(columns)
    }

    /// Go `buildProjection4CTEUnion(_, seed, recur)`
    /// (`logical_plan_builder.go:8029`): the recursive part is CAST to the
    /// CTE's result schema.
    ///
    /// This is the CTE's counterpart to
    /// [`super::set_opr::PlanBuilder::build_projection4_union`], and it is
    /// deliberately NOT the same rule: a `UNION`'s result type is JOINED
    /// across its branches, while a recursive CTE's is the SEED's alone (made
    /// nullable), because the seed is what the iteration starts from.
    ///
    /// # Errors
    ///
    /// `ErrWrongNumberOfColumnsInSelect` when the two arities differ, or a
    /// cast-construction error.
    pub fn build_projection4_cte_union(
        &mut self,
        seed_schema: &Schema,
        recur: LogicalPlan,
    ) -> Result<LogicalPlan, PlanError> {
        let recur_schema = recur.schema().cloned().unwrap_or_default();
        if seed_schema.columns.len() != recur_schema.columns.len() {
            return Err(PlanError::internal(
                "The used SELECT statements have a different number of columns",
            ));
        }
        let res_schema = self.get_result_cte_schema(seed_schema);
        let mut exprs = Vec::with_capacity(recur_schema.columns.len());
        for (index, column) in recur_schema.columns.iter().enumerate() {
            let target = res_schema.columns[index]
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified));
            let source = column
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified));
            if target.equal(&source) {
                exprs.push(tidb_expr::expression::Expression::Column(column.clone()));
            } else {
                // boundary: `expression.BuildCastFunction4Union`'s `inUnion`
                // flag; see [`super::set_opr`]'s narrowings.
                exprs.push(
                    tidb_expr::aggregation::wrap_cast::build_cast_to(
                        tidb_expr::expression::Expression::Column(column.clone()),
                        target,
                    )
                    .map_err(|error| PlanError::internal(format!("{error:?}")))?,
                );
            }
        }
        self.opt_flag |= flags::ELIMINATE_PROJECTION;
        let mut projection = crate::logical::projection::LogicalProjection::new(
            self.base(crate::logical::projection::LogicalProjection::TYPE),
            exprs,
        );
        projection.base.set_children(vec![recur]);
        projection.base.base.set_schema(Some(res_schema));
        Ok(LogicalPlan::Projection(projection))
    }

    /// Go `tryBuildCTE(ctx, tn, asName)` (`logical_plan_builder.go:4739`):
    /// resolve an unqualified table name against the CTEs in scope.
    ///
    /// `Ok(None)` is Go's `nil, nil` — no CTE of that name is visible, so the
    /// caller goes on to look the name up as a real table.
    ///
    /// The search runs from the INNERMOST scope outwards, and a NON-recursive
    /// CTE that is still building is SKIPPED rather than matched ("Can't see
    /// this CTE, try outer definition") — which is what makes
    /// `WITH c AS (SELECT * FROM c)` resolve to an outer `c` or a real table
    /// instead of to itself.
    ///
    /// # Errors
    ///
    /// `ErrCTERecursiveRequiresNonRecursiveFirst` or
    /// `ErrInvalidRequiresSingleReference`; see this module's section 3.
    pub fn try_build_cte(
        &mut self,
        table_name: &str,
        as_name: Option<&str>,
    ) -> Result<Option<LogicalPlan>, PlanError> {
        let lower = table_name.to_lowercase();
        for index in (0..self.outer_ctes.len()).rev() {
            if self.outer_ctes[index].name != lower {
                continue;
            }
            if self.outer_ctes[index].is_building {
                if self.outer_ctes[index].non_recursive {
                    // "Can't see this CTE, try outer definition."
                    continue;
                }
                // "Building the recursive part."
                self.outer_ctes[index].use_recursive = true;
                let Some(seed_schema) = self.outer_ctes[index]
                    .seed_lp
                    .as_ref()
                    .and_then(|plan| plan.schema().cloned())
                else {
                    return Err(err_recursive_requires_non_recursive_first(table_name));
                };
                if self.outer_ctes[index].enter_subquery || self.outer_ctes[index].recursive_ref {
                    return Err(err_invalid_requires_single_reference(table_name));
                }
                self.outer_ctes[index].recursive_ref = true;
                let seed_names = self.outer_ctes[index]
                    .seed_lp
                    .as_ref()
                    .map(|plan| plan.output_names().to_vec())
                    .unwrap_or_default();
                let mut cte_table = LogicalCTETable::new(
                    self.base(LogicalCTETable::TYPE),
                    Rc::clone(&self.outer_ctes[index].seed_stat),
                );
                cte_table.name = self.outer_ctes[index].name_original.clone();
                cte_table.id_for_storage = self.outer_ctes[index].storage_id;
                cte_table.seed_schema = Some(seed_schema.clone());
                let result_schema = self.get_result_cte_schema(&seed_schema);
                cte_table.base.base.set_schema(Some(result_schema));
                cte_table.base.base.set_output_names(seed_names);
                self.handle_helper.push_empty();
                return Ok(Some(LogicalPlan::CTETable(cte_table)));
            }

            self.handle_helper.push_empty();

            // "If current CTE query contain another CTE which
            // 'containRecursiveForbiddenOperator' is true, current CTE
            // 'containRecursiveForbiddenOperator' will be true."
            if self.building_cte {
                let inherited = self.outer_ctes[index].contain_recursive_forbidden_operator;
                if let Some(last) = self.outer_ctes.last_mut() {
                    last.contain_recursive_forbidden_operator |= inherited;
                }
            }
            self.compute_cte_inline_flag(index);

            if self.outer_ctes[index].recur_lp.is_none() && self.outer_ctes[index].is_inline {
                // Go hides every CTE from `i` onwards for the duration, so the
                // merged body cannot see itself, and restores them after.
                let saved = self.outer_ctes.split_off(index);
                let saved_building = self.building_cte;
                self.building_cte = false;
                let result = self.build_data_source_from_cte_merge(&saved[0]);
                self.building_cte = saved_building;
                self.outer_ctes.extend(saved);
                return result.map(Some);
            }

            return self.build_logical_cte_reference(index, as_name).map(Some);
        }
        Ok(None)
    }

    /// `tryBuildCTE`'s materialised-reference tail (`:4799-4857`): the
    /// `CTEClass` (created ONCE per CTE and shared by every reference) and the
    /// `LogicalCTE` node that points at it.
    fn build_logical_cte_reference(
        &mut self,
        index: usize,
        as_name: Option<&str>,
    ) -> Result<LogicalPlan, PlanError> {
        let (has_limit, limit_beg, limit_end) = match self.outer_ctes[index].limit_bounds {
            Some((beg, end)) => (true, beg, end),
            None => (false, 0, 0),
        };
        if self.outer_ctes[index].cte_class.is_none() {
            let seed = self.outer_ctes[index].seed_lp.take();
            let recur = self.outer_ctes[index].recur_lp.take();
            let class = CteClass {
                is_distinct: self.outer_ctes[index].is_distinct,
                seed_part_logical_plan: seed,
                recursive_part_logical_plan: recur,
                id_for_storage: self.outer_ctes[index].storage_id,
                opt_flag: self.outer_ctes[index].opt_flag,
                has_limit,
                limit_beg,
                limit_end,
                ..CteClass::default()
            };
            self.outer_ctes[index].cte_class = Some(Rc::new(RefCell::new(class)));
        }
        let class = Rc::clone(
            self.outer_ctes[index]
                .cte_class
                .as_ref()
                .expect("just created"),
        );

        // "Use cteClass.SeedPartLogicalPlan.Schema() (not cte.seedLP.Schema())
        // to ensure all references to the same CTE use a consistent schema.
        // When a CTE is referenced multiple times, cteClass is shared and
        // contains the schema from when the CTE was first processed."
        let (prev_schema, seed_names) = {
            let borrowed = class.borrow();
            let seed = borrowed
                .seed_part_logical_plan
                .as_ref()
                .ok_or_else(|| PlanError::internal("a CTE reference has no seed plan"))?;
            (
                seed.schema().cloned().unwrap_or_default(),
                seed.output_names().to_vec(),
            )
        };
        let result_schema = self.get_result_cte_schema(&prev_schema);

        let mut lp = LogicalCTE::new(self.base(LogicalCTE::TYPE), Rc::clone(&class));
        lp.cte_as_name = self.outer_ctes[index].name_original.clone();
        lp.cte_name = self.outer_ctes[index].name_original.clone();
        lp.seed_stat = Some(Rc::clone(&self.outer_ctes[index].seed_stat));
        {
            let mut borrowed = class.borrow_mut();
            for (column, prev) in result_schema.columns.iter().zip(&prev_schema.columns) {
                borrowed
                    .column_map
                    .insert(column.clone().hash_code().to_vec(), prev.clone());
            }
        }
        lp.base.base.set_schema(Some(result_schema));

        let mut names = seed_names;
        if let Some(as_name) = as_name.filter(|as_name| !as_name.is_empty()) {
            lp.cte_as_name = as_name.to_owned();
            for name in &mut names {
                name.names.table = IdentifierMetadata::new(as_name);
            }
        }
        lp.base.base.set_output_names(names);
        Ok(LogicalPlan::CTE(lp))
    }

    /// Go `computeCTEInlineFlag(cte)` (`logical_plan_builder.go:4875`):
    /// "Combine the declaration of CTE and the use of CTE to jointly determine
    /// **whether a CTE can be inlined**".
    ///
    /// Go's own three rules, unchanged: a RECURSIVE CTE never inlines; one
    /// that contains an operator forbidden in a recursive part never inlines
    /// while such a part is being built; and one with a consumer count other
    /// than exactly 1 inlines only under an explicit hint or session variable.
    ///
    /// See this module's `ConsumerCount` narrowing for what the third rule
    /// reads here.
    fn compute_cte_inline_flag(&mut self, index: usize) {
        let building_recursive = self.building_recursive_part_for_cte;
        let cte = &mut self.outer_ctes[index];
        if cte.recur_lp.is_some() {
            // Go warns "Recursive CTE %s can not be inlined by merge() or
            // tidb_opt_force_inline_cte." here; no warning channel, same
            // decision.
            cte.is_inline = false;
        } else if cte.contain_recursive_forbidden_operator && building_recursive {
            // Go warns `ErrCTERecursiveForbidsAggregation` here.
            cte.is_inline = false;
        } else if cte.consumer_count != 1 {
            cte.is_inline = cte.force_inline_by_hint_or_var;
        } else {
            cte.is_inline = true;
        }
    }

    /// Go `buildDataSourceFromCTEMerge(ctx, cte)`
    /// (`logical_plan_builder.go:4903`): an INLINED CTE, whose body is built
    /// afresh at the reference site rather than materialised.
    ///
    /// # Errors
    ///
    /// The body's own error, or Go's "CTE columns length is not consistent".
    fn build_data_source_from_cte_merge(
        &mut self,
        cte: &super::OuterCte,
    ) -> Result<LogicalPlan, PlanError> {
        let query = cte
            .definition
            .as_ref()
            .ok_or_else(|| PlanError::internal("an inlined CTE has no recorded body"))?;
        let mut plan = self.build_query_stmt(query, true)?;
        self.handle_helper.pop_map();
        let current_db = self.source.current_database().to_owned();
        let mut names = plan.output_names().to_vec();
        for name in &mut names {
            name.names.table = IdentifierMetadata::new(&cte.name_original);
            name.names.database = IdentifierMetadata::new(&current_db);
        }
        if !cte.col_name_list.is_empty() {
            if cte.col_name_list.len() != names.len() {
                return Err(PlanError::internal("CTE columns length is not consistent"));
            }
            for (name, new_name) in names.iter_mut().zip(&cte.col_name_list) {
                name.names.column = IdentifierMetadata::new(new_name);
            }
        }
        set_own_output_names(&mut plan, names);
        Ok(plan)
    }

    /// Go `tryToBuildSequence(ctes, p)` (`logical_plan_builder.go:4624`).
    ///
    /// A `LogicalSequence` puts the CTE producers BEFORE the main query as
    /// earlier children, so that MPP can share one materialisation across
    /// references. It is built only under `EnableMPPSharedCTEExecution`, and
    /// only when EVERY CTE in the layer is non-recursive and materialised —
    /// Go returns `p` untouched the moment it meets a recursive one, without
    /// looking at the rest.
    #[must_use]
    pub fn try_to_build_sequence(&mut self, ctes: &[usize], plan: LogicalPlan) -> LogicalPlan {
        if !self.enable_mpp_shared_cte_execution {
            return plan;
        }
        let mut kept: Vec<usize> = ctes.to_vec();
        for position in (0..kept.len()).rev() {
            let index = kept[position];
            if !self.outer_ctes[index].non_recursive {
                return plan;
            }
            if self.outer_ctes[index].is_inline || self.outer_ctes[index].cte_class.is_none() {
                kept.remove(position);
            }
        }
        if kept.is_empty() {
            return plan;
        }
        let names = plan.output_names().to_vec();
        let mut children = Vec::with_capacity(kept.len() + 1);
        for index in kept {
            let class = Rc::clone(
                self.outer_ctes[index]
                    .cte_class
                    .as_ref()
                    .expect("filtered above"),
            );
            let seed_schema = class
                .borrow()
                .seed_part_logical_plan
                .as_ref()
                .and_then(|seed| seed.schema().cloned())
                .unwrap_or_default();
            let mut lcte = LogicalCTE::new(self.base(LogicalCTE::TYPE), class);
            lcte.cte_as_name = self.outer_ctes[index].name_original.clone();
            lcte.cte_name = self.outer_ctes[index].name_original.clone();
            lcte.seed_stat = Some(Rc::clone(&self.outer_ctes[index].seed_stat));
            lcte.only_used_as_storage = true;
            let schema = self.get_result_cte_schema(&seed_schema);
            lcte.base.base.set_schema(Some(schema));
            children.push(LogicalPlan::CTE(lcte));
        }
        self.opt_flag |= flags::PUSH_DOWN_SEQUENCE;
        children.push(plan);
        let mut sequence = LogicalSequence::new(self.base(LogicalSequence::TYPE));
        sequence.base.set_children(children);
        sequence.base.base.set_output_names(names);
        LogicalPlan::Sequence(sequence)
    }

    /// Go `prepareCTECheckForSubQuery()` (`logical_plan_builder.go:7948`):
    /// "prepares the check that the recursive CTE can't be referenced in
    /// subQuery", e.g.
    /// `with recursive cte(n) as (select 1 union select * from (select * from cte) c1) select * from cte`.
    ///
    /// Returns the indices whose flag this call SET, which is what
    /// [`Self::reset_cte_check_for_subquery`] clears again.
    pub fn prepare_cte_check_for_subquery(&mut self) -> Vec<usize> {
        let mut modified = Vec::new();
        for index in 0..self.outer_ctes.len() {
            let cte = &mut self.outer_ctes[index];
            if cte.is_building && !cte.enter_subquery {
                cte.enter_subquery = true;
                modified.push(index);
            }
        }
        modified
    }

    /// Go `resetCTECheckForSubQuery(ci)` (`logical_plan_builder.go:7961`).
    pub fn reset_cte_check_for_subquery(&mut self, indices: &[usize]) {
        for &index in indices {
            self.outer_ctes[index].enter_subquery = false;
        }
    }

    /// Go `genCTETableNameForError()` (`logical_plan_builder.go:7969`): "find
    /// the nearest CTE name".
    #[must_use]
    pub fn gen_cte_table_name_for_error(&self) -> String {
        self.outer_ctes
            .iter()
            .rev()
            .find(|cte| cte.is_building)
            .map(|cte| cte.name_original.clone())
            .unwrap_or_default()
    }
}
