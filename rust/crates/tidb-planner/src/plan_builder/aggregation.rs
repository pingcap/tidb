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

//! `GROUP BY`, aggregation, `HAVING` and `DISTINCT`.
//!
//! Go sources, by symbol:
//!
//! | Rust | Go `logical_plan_builder.go` |
//! | --- | --- |
//! | [`PlanBuilder::build_aggregation`] | `buildAggregation` (:255) |
//! | [`PlanBuilder::build_distinct`] | `buildDistinct` (:1966) |
//! | [`PlanBuilder::resolve_gby_exprs`] | `resolveGbyExprs` (:4030) + `gbyResolver.Enter`/`Leave` (:3379/:3401) |
//! | [`PlanBuilder::rewrite_gby_exprs`] | `rewriteGbyExprs` (:4066) |
//! | [`PlanBuilder::extract_agg_funcs_in_exprs`] | `extractAggFuncsInExprs` (:2982) |
//! | [`PlanBuilder::extract_agg_funcs_in_select_fields`] | `extractAggFuncsInSelectFields` (:2996) |
//! | [`PlanBuilder::extract_agg_funcs_in_by_items`] | `extractAggFuncsInByItems` (:3011) |
//! | [`PlanBuilder::extract_correlated_agg_funcs`] | `extractCorrelatedAggFuncs` (:3021) |
//! | [`PlanBuilder::resolve_correlated_aggregates`] | `resolveCorrelatedAggregates` (:3306) + `correlatedAggregateResolver` (:3137-3305) |
//! | [`PlanBuilder::resolve_having_and_order_by`] | `resolveHavingAndOrderBy` (:2905) + `havingWindowAndOrderbyExprResolver` (:2661/:2686/:2784) |
//! | [`resolve_from_select_fields`] | `resolveFromSelectFields` (:2607) |
//! | [`PlanBuilder::build_sort_with_check`] | `buildSortWithCheck` (:2403) |
//! | [`PlanBuilder::add_alias_name`] | `addAliasName` (:4141) |
//!
//! `checkOnlyFullGroupBy` and `checkOrderByInDistinct` are in
//! [`super::only_full_group_by`]; `buildExpand` and the `ROLLUP` machinery are
//! in [`super::expand`].
//!
//! # 1. THE MARKER SCHEME, as this batch's main consumer
//!
//! [`super::marker`]'s header names 6c as the batch that must use the ratified
//! kinds and no new side table. It does. The binding, kind by kind:
//!
//! | Go map | [`MarkerKind`] | index means | bound by |
//! | --- | --- | --- | --- |
//! | `aggMapper` / `totalMap` | [`Agg`](MarkerKind::Agg) | position in the extracted aggregate list | [`agg_marker_columns`] |
//! | `havingMap` | [`Having`](MarkerKind::Having) | position in the extracted aggregate list | [`agg_marker_columns`] |
//! | `colMapper` | [`Column`](MarkerKind::Column) | select-list field index | the projection's schema |
//! | `orderMap` | [`OrderBy`](MarkerKind::OrderBy) | select-list field index | the projection's schema |
//! | `correlatedAggMapper` | [`CorrelatedAgg`](MarkerKind::CorrelatedAgg) | index into [`PlanBuilder::correlated_agg_columns`] | that vector |
//!
//! The `Agg`/`Having` binding is the one that needs a word. Go's map value
//! starts as the aggregate's position in `aggFuncList` and is REMAPPED after
//! the aggregation is built:
//!
//! ```text
//! p, aggIndexMap, err = b.buildAggregation(...)
//! for agg, idx := range totalMap { totalMap[agg] = aggIndexMap[idx] }   // :4514
//! ```
//!
//! because `buildAggregation` COMBINES identical aggregates onto one output
//! column. [`PlanBuilder::build_aggregation`] returns that same
//! `aggIndexMap` as a `Vec<usize>`, and [`agg_marker_columns`] composes it
//! with the aggregation's schema to give the column vector a marker index
//! looks up — which is spec rule 4 with Go's own remap folded in, not a
//! second scheme.
//!
//! # 2. HAVING is a Selection ABOVE the Projection
//!
//! Go's `buildSelect` (`:4533`) builds the projection and only then
//!
//! ```text
//! if sel.Having != nil {
//!     b.curClause = havingClause
//!     p, err = b.buildSelection(ctx, p, sel.Having.Expr, havingMap)
//! }
//! ```
//!
//! so the filter reads the PROJECTION's output, which is what makes
//! `select a+1 as b from t having b > 0` resolve `b` at all. That shape is
//! reproduced literally: [`super::PlanBuilder::build_selection`] is called
//! with the projection as its child and [`MarkerKind::Having`] bound. This is
//! also the exact point at which `tidb-executor`'s `driver/having.rs` diverges
//! — its own header says it evaluates the projection LAST, over source rows —
//! so that file is deliberately NOT harvested; only its name resolution
//! transfers, and that already lives in `driver/clause_resolve.rs`, which 6a
//! took.
//!
//! # 3. Narrowings, by exact blocking Go symbol
//!
//! * `aggOrderByResolver` (`:301`), which resolves a `GROUP_CONCAT`'s
//!   `ORDER BY` positions against the call's OWN argument list. `tidb_ast`
//!   already parses that clause into [`tidb_ast::Expr::GroupConcat`]'s
//!   `order_by`, and the positional arm is ported inline in
//!   [`PlanBuilder::build_aggregation`]; the `*ast.PositionExpr` /
//!   `ParamMarkerExpr` arms of the resolver are not, because neither node has
//!   a `tidb_ast` counterpart on this path.
//! * `findJoinFullSchema(p)` (`:645`) in `resolveGbyExprs` (`:4037`) and in
//!   `buildAggregation`'s second `firstrow` loop (`:379`). 6b landed it as
//!   [`super::from::find_join_full_schema`] and both call sites use it.
//! * `driver.ParamMarkerExpr` and `expression.ConstructPositionExpr`
//!   (`gbyResolver.Enter`, `:3383`). A parameter marker as a top-level
//!   GROUP BY item is a POSITION when its value is a small uint, which needs
//!   the execute-time binding this crate has no access to — the same boundary
//!   [`super::PlanBuilder::build_limit`] already names. A bare integer
//!   literal IS handled, since that is the written form.
//! * `expression.ExtractCorColumns` over a subquery's built plan
//!   (`extractCorrelatedAggFuncs`, `:3030`) needs the plan-carrying rewrite,
//!   which is [`super::PlanBuilder::expression_rewriter`]'s seam and not
//!   [`super::PlanBuilder::rewrite_scalar`]'s.
//!   [`PlanBuilder::extract_correlated_agg_funcs`] therefore decides
//!   correlation SYNTACTICALLY: an aggregate every one of whose column
//!   arguments fails to resolve in the current scope but resolves in an outer
//!   one is correlated, which is Go's `len(corCols) > 0 && len(cols) == 0`
//!   over the same two scopes.
//! * `sel.AsViewSchema` and the anonymous-field dedup loop
//!   (`addAliasName`, `:4193`). `canExpandAST` is a dropped narrowing of
//!   [`super`] — the view builder is a boundary — so
//!   [`PlanBuilder::add_alias_name`] ports the aliasing half and says so.

use std::collections::BTreeMap;

use tidb_ast::{Expr, GroupByItem, OrderItem, SelectStmt};
use tidb_datatype::FieldName;
use tidb_expr::aggregation::{names as agg_names, AggFuncDesc, ByItems};
use tidb_expr::column::Column;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};
use tidb_expr::Columns;

use super::catalog::TableSource;
use super::marker::{self, MarkerKind, PlanMarker};
use super::{find_field_name, snapshot_schema_and_names, PlanBuilder, ProjectionField};
use crate::expression_rewriter::ClauseCode;
use crate::logical::aggregation::LogicalAggregation;
use crate::logical::rule::flags;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanError;

// ***** the generic AST traversal every resolver below is written over *****

/// Visits every [`Expr`] of a clause, outermost first, with `f` free to
/// REPLACE the node it is given; returning `true` from `f` skips that node's
/// children.
///
/// Go's resolvers are `ast.Visitor`s over a pointer graph, so a `Leave` that
/// returns a different node rewrites the parent's field in place. This is the
/// value-typed equivalent, and it is what lets every marker substitution below
/// be a single in-place write rather than a rebuild of the enclosing
/// expression.
pub fn visit_exprs(expr: &mut Expr, f: &mut impl FnMut(&mut Expr) -> bool) {
    struct Walker<'f, F> {
        f: &'f mut F,
    }
    impl<F: FnMut(&mut Expr) -> bool> tidb_ast::Visitor for Walker<'_, F> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            node.downcast_mut::<Expr>()
                .is_some_and(|expr| (self.f)(expr))
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut walker = Walker { f };
    tidb_ast::Visitable::accept(expr, &mut walker);
}

/// [`visit_exprs`] without the mutation, over a scratch clone.
pub fn walk_exprs(expr: &Expr, f: &mut impl FnMut(&Expr) -> bool) {
    let mut owned = expr.clone();
    visit_exprs(&mut owned, &mut |node| f(node));
}

/// Whether `expr` IS an aggregate call — Go's `*ast.AggregateFuncExpr` test.
#[must_use]
pub fn is_aggregate_call(expr: &Expr) -> bool {
    matches!(expr, Expr::Aggregate { .. } | Expr::GroupConcat { .. })
}

/// Whether `expr` contains a window call anywhere — Go `ast.HasWindowFlag`.
#[must_use]
pub fn has_window_flag(expr: &Expr) -> bool {
    let mut found = false;
    walk_exprs(expr, &mut |node| {
        if matches!(node, Expr::Window { .. }) {
            found = true;
            return true;
        }
        false
    });
    found
}

// ***** the aggregate extractors *****

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `AggregateFuncExtractor` as `extractAggFuncsInExprs` drives it
    /// (`logical_plan_builder.go:2982`): collect every aggregate call, and
    /// substitute a [`MarkerKind::Agg`] marker for it.
    ///
    /// Go's extractor SKIPS an aggregate already claimed by
    /// `b.correlatedAggMapper` (`skipAggMap`), because that one belongs to an
    /// OUTER query block and was built there. Here such a node already carries
    /// a [`MarkerKind::CorrelatedAgg`] marker and so is no longer an
    /// `Expr::Aggregate` at all, which is the same skip by construction.
    ///
    /// Nesting: Go's extractor returns `true` from `Enter` on an aggregate, so
    /// an aggregate INSIDE an aggregate is never separately collected. The
    /// `true` returned below is that.
    pub fn extract_agg_funcs_in_exprs(&self, exprs: &mut [Expr]) -> Vec<Expr> {
        let mut found = Vec::new();
        for expr in exprs {
            visit_exprs(expr, &mut |node| {
                if !is_aggregate_call(node) {
                    return false;
                }
                let replaced =
                    marker::substitute(node, PlanMarker::new(MarkerKind::Agg, found.len()));
                found.push(replaced);
                true
            });
        }
        found
    }

    /// Go `extractAggFuncsInSelectFields(fields)`
    /// (`logical_plan_builder.go:2996`).
    pub fn extract_agg_funcs_in_select_fields(&self, fields: &mut [ProjectionField]) -> Vec<Expr> {
        let mut exprs: Vec<Expr> = fields.iter().map(|field| field.expr.clone()).collect();
        let found = self.extract_agg_funcs_in_exprs(&mut exprs);
        for (field, expr) in fields.iter_mut().zip(exprs) {
            field.expr = expr;
        }
        found
    }

    /// Go `extractAggFuncsInByItems(byItems)`
    /// (`logical_plan_builder.go:3011`).
    pub fn extract_agg_funcs_in_by_items(&self, items: &mut [OrderItem]) -> Vec<Expr> {
        let mut exprs: Vec<Expr> = items.iter().map(|item| item.expr.clone()).collect();
        let found = self.extract_agg_funcs_in_exprs(&mut exprs);
        for (item, expr) in items.iter_mut().zip(exprs) {
            item.expr = expr;
        }
        found
    }

    /// Go `extractCorrelatedAggFuncs(ctx, p, aggFuncs)`
    /// (`logical_plan_builder.go:3021`): the aggregates whose arguments read
    /// ONLY outer columns, which therefore belong to the outer query block.
    ///
    /// Go's test is `len(corCols) > 0 && len(cols) == 0` over the BUILT
    /// arguments. See this module's narrowing for why the same test is made
    /// against the two name scopes rather than against a built plan.
    #[must_use]
    pub fn extract_correlated_agg_funcs(
        &self,
        agg_funcs: &[Expr],
        names: &[FieldName],
    ) -> Vec<usize> {
        // "If decorrelation is disabled, don't extract correlated aggregates"
        // (`:3035`).
        if self.no_decorrelate {
            return Vec::new();
        }
        let mut outer = Vec::new();
        for (position, agg) in agg_funcs.iter().enumerate() {
            let mut has_inner = false;
            let mut has_outer = false;
            walk_exprs(agg, &mut |node| {
                let Expr::Column(path) = node else {
                    return false;
                };
                if find_field_name(names, path).is_some() {
                    has_inner = true;
                } else if self
                    .outer_names
                    .iter()
                    .any(|scope| find_field_name(scope, path).is_some())
                {
                    has_outer = true;
                }
                true
            });
            if has_outer && !has_inner {
                outer.push(position);
            }
        }
        outer
    }

    /// Go `resolveCorrelatedAggregates(ctx, sel, p)`
    /// (`logical_plan_builder.go:3306`), over `correlatedAggregateResolver`
    /// (`:3137-3305`).
    ///
    /// An aggregate written inside a SUBQUERY but reading only THIS block's
    /// columns must be evaluated HERE — `select (select count(a)) from t`
    /// counts `t`'s rows. Go's resolver descends into every subquery of the
    /// select list, the HAVING and the ORDER BY, lifts such aggregates into
    /// the select list as auxiliary fields, and records the field index in
    /// `correlatedAggMap` so the subquery's own build finds the already-built
    /// column.
    ///
    /// Here the lift is the same, and the record is a
    /// [`MarkerKind::CorrelatedAgg`] marker substituted for the aggregate
    /// INSIDE the subquery, indexing [`PlanBuilder::correlated_agg_columns`].
    /// The auxiliary field carries Go's `sel_subq_agg_<n>` alias.
    ///
    /// # Errors
    ///
    /// None on this path; the signature matches its Go sibling so the caller's
    /// `?` chain is uniform.
    pub fn resolve_correlated_aggregates(
        &mut self,
        fields: &mut Vec<ProjectionField>,
        having: Option<&mut Expr>,
        order_by: &mut [OrderItem],
        names: &[FieldName],
    ) -> Result<Vec<Expr>, PlanError> {
        let mut lifted = Vec::new();
        let mut clauses: Vec<&mut Expr> = Vec::new();
        // Go visits the select list, then HAVING, then ORDER BY, in that
        // order; the marker indices below follow it.
        let mut field_exprs: Vec<Expr> = fields.iter().map(|field| field.expr.clone()).collect();
        for expr in &mut field_exprs {
            clauses.push(expr);
        }
        if let Some(having) = having {
            clauses.push(having);
        }
        for item in order_by.iter_mut() {
            clauses.push(&mut item.expr);
        }

        for clause in clauses {
            visit_exprs(clause, &mut |node| {
                // Only a SUBQUERY's interior is Go's concern here: an
                // aggregate written directly in this block is the ordinary
                // extractor's.
                let Expr::Subquery(subquery) = node else {
                    return false;
                };
                let mut inner = (**subquery).clone();
                lift_correlated_aggregates(&mut inner, names, &mut lifted);
                **subquery = inner;
                true
            });
        }
        for (index, expr) in field_exprs.into_iter().enumerate() {
            fields[index].expr = expr;
        }

        // `:3350` each lifted aggregate becomes an auxiliary select field, and
        // its marker index is the position in `correlated_agg_columns` the
        // aggregation will later fill.
        let mut appended = Vec::with_capacity(lifted.len());
        for agg in lifted {
            let position = fields.len();
            fields.push(ProjectionField {
                expr: agg.clone(),
                alias: Some(format!("sel_subq_agg_{position}")),
                text: None,
                hidden: true,
            });
            self.correlated_agg_columns
                .push(CorrelatedColumn::default());
            appended.push(agg);
        }
        Ok(appended)
    }
}

/// The interior half of `correlatedAggregateResolver.Enter` (`:3163`): inside
/// `subquery`, replace every aggregate that reads only OUTER (i.e. the current
/// block's) columns with a [`MarkerKind::CorrelatedAgg`] marker, and hand the
/// aggregate back to be lifted.
fn lift_correlated_aggregates(
    subquery: &mut tidb_ast::QueryStmt,
    outer_names: &[FieldName],
    lifted: &mut Vec<Expr>,
) {
    let tidb_ast::QueryStmt::Select(select) = subquery else {
        return;
    };
    let mut visit = |expr: &mut Expr| {
        visit_exprs(expr, &mut |node| {
            if !is_aggregate_call(node) {
                return false;
            }
            // Go's condition, restated over names: the aggregate reads a
            // column of the OUTER block and none of the subquery's own. The
            // subquery's own FROM has not been built at this point, so the
            // test is "every column it reads is an outer one".
            let mut reads_outer = false;
            let mut reads_only_outer = true;
            walk_exprs(node, &mut |inner| {
                let Expr::Column(path) = inner else {
                    return false;
                };
                if find_field_name(outer_names, path).is_some() {
                    reads_outer = true;
                } else {
                    reads_only_outer = false;
                }
                true
            });
            if !(reads_outer && reads_only_outer) {
                return true;
            }
            let replaced = marker::substitute(
                node,
                PlanMarker::new(MarkerKind::CorrelatedAgg, lifted.len()),
            );
            lifted.push(replaced);
            true
        });
    };
    for field in select.fields.fields_mut() {
        if let tidb_ast::SelectField::Expr { expr, .. } = field {
            visit(expr);
        }
    }
    if let Some(having) = select.having.as_mut() {
        visit(having);
    }
}

// ***** GROUP BY resolution *****

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `resolveGbyExprs(p, gby, fields)`
    /// (`logical_plan_builder.go:4030`), over `gbyResolver.Enter`/`Leave`
    /// (`:3379`/`:3401`).
    ///
    /// A GROUP BY item is resolved against the SOURCE scope first and the
    /// select list second — the reverse of ORDER BY. Go's rule, restated:
    ///
    /// * a bare integer is a 1-based select-list POSITION (`:3436`), and it is
    ///   an error for that field to contain an aggregate or a window call;
    /// * an unqualified column that the source scope knows, or that appears
    ///   INSIDE a larger expression (`inExpr`), stays a column;
    /// * otherwise a select-list alias wins and the item BECOMES that field's
    ///   expression — which is why `select a+1 as b from t group by b` groups
    ///   by `a+1` and not by an output column.
    ///
    /// # Errors
    ///
    /// `ErrWrongGroupField` for a position naming an aggregate or window
    /// field, `ErrUnknownColumn` for a position out of range, and
    /// `ErrIllegalReference` for an alias naming an aggregate or window field.
    pub fn resolve_gby_exprs(
        &mut self,
        group_by: &[GroupByItem],
        fields: &[ProjectionField],
        names: &[FieldName],
    ) -> Result<Vec<Expr>, PlanError> {
        self.cur_clause = ClauseCode::GroupBy;
        let mut resolved = Vec::with_capacity(group_by.len());
        for item in group_by {
            let mut expr = Self::clause_scratch(&item.expr);
            // `gbyResolver.Enter`'s `exprDepth == 1` test: only a TOP-LEVEL
            // integer is a position.
            if let Expr::Int(digits) = &expr {
                let position: usize = digits
                    .parse()
                    .map_err(|_| PlanError::internal("Unknown column in 'group statement'"))?;
                let field = fields.get(position.wrapping_sub(1)).ok_or_else(|| {
                    PlanError::internal(format!("Unknown column '{position}' in 'group statement'"))
                })?;
                if aggregate_anywhere(&field.expr) || has_window_flag(&field.expr) {
                    let label = field
                        .alias
                        .clone()
                        .or_else(|| field.text.clone())
                        .unwrap_or_default();
                    return Err(PlanError::internal(format!("Can't group on '{label}'")));
                }
                resolved.push(field.expr.clone());
                continue;
            }
            // `gbyResolver.Leave`'s `*ast.ColumnNameExpr` arm. `in_expr` is
            // Go's flag for "this column is nested inside a larger
            // expression", which suppresses the select-list fallback.
            let mut error = None;
            visit_exprs(&mut expr, &mut |node| {
                let Expr::Column(path) = node else {
                    // Go sets `inExpr` on any node that is not a value, a
                    // column, or parentheses; a nested column below such a
                    // node is resolved against the source only.
                    return false;
                };
                if find_field_name(names, path).is_some() {
                    return true;
                }
                let Some(index) = resolve_from_select_fields(path, fields, false) else {
                    return true;
                };
                let field = &fields[index];
                if aggregate_anywhere(&field.expr) {
                    error = Some(PlanError::internal(format!(
                        "Reference '{}' not supported (reference to group function)",
                        path.last().cloned().unwrap_or_default()
                    )));
                } else if has_window_flag(&field.expr) {
                    error = Some(PlanError::internal(format!(
                        "Reference '{}' not supported (reference to window function)",
                        path.last().cloned().unwrap_or_default()
                    )));
                } else {
                    *node = field.expr.clone();
                }
                true
            });
            if let Some(error) = error {
                return Err(error);
            }
            resolved.push(expr);
        }
        Ok(resolved)
    }

    /// Go `rewriteGbyExprs(ctx, p, gby, items)`
    /// (`logical_plan_builder.go:4066`): build each resolved item against the
    /// child plan.
    ///
    /// # Errors
    ///
    /// The expression build error for any item.
    pub fn rewrite_gby_exprs(
        &mut self,
        items: &[Expr],
        schema: &Schema,
        names: &[FieldName],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<Vec<Expression>, PlanError> {
        items
            .iter()
            .map(|item| self.rewrite_scalar(item, schema, names, markers))
            .collect()
    }
}

/// Go `resolveFromSelectFields(v, fields, ignoreAsName)`
/// (`logical_plan_builder.go:2607`): the select-list index an unqualified name
/// resolves to.
///
/// Go's precedence, and the one `tidb-executor`'s `driver/clause_resolve.rs`
/// already got right: an ALIAS match wins outright; among fields that ARE the
/// same column, the first wins and a genuinely different column with the same
/// name is ambiguous (Go raises `ErrAmbiguous`, which here is `None` — the
/// caller falls through to source resolution and reports the column).
/// An AUXILIARY field is never matched, which is Go's `field.Auxiliary`
/// continue and is what [`ProjectionField::hidden`] carries.
#[must_use]
pub fn resolve_from_select_fields(
    path: &[String],
    fields: &[ProjectionField],
    ignore_as_name: bool,
) -> Option<usize> {
    let [name] = path else {
        return None;
    };
    let mut matched: Option<usize> = None;
    for (index, field) in fields.iter().enumerate() {
        if field.hidden {
            continue;
        }
        let matches = if ignore_as_name {
            matches!(&field.expr, Expr::Column(p)
                if p.last().is_some_and(|last| last.eq_ignore_ascii_case(name)))
        } else {
            match &field.alias {
                Some(alias) => alias.eq_ignore_ascii_case(name),
                None => matches!(&field.expr, Expr::Column(p)
                    if p.last().is_some_and(|last| last.eq_ignore_ascii_case(name))),
            }
        };
        if !matches {
            continue;
        }
        // A field that is NOT a column resolves immediately; Go returns `i`
        // without the ambiguity bookkeeping.
        let Expr::Column(current) = &field.expr else {
            return Some(index);
        };
        match matched {
            None => matched = Some(index),
            Some(previous) => {
                let Expr::Column(earlier) = &fields[previous].expr else {
                    continue;
                };
                // Go: ambiguous unless one name is a PREFIX-qualified form of
                // the other (`Name.Match`).
                if !column_paths_match(earlier, current) {
                    return None;
                }
            }
        }
    }
    matched
}

/// Go `ast.ColumnName.Match`: two written names denote the same column when
/// every qualifier both of them carry agrees.
fn column_paths_match(left: &[String], right: &[String]) -> bool {
    let mut pairs = left.iter().rev().zip(right.iter().rev());
    pairs.all(|(l, r)| l.eq_ignore_ascii_case(r))
}

/// Whether `expr` contains an aggregate call anywhere.
#[must_use]
pub fn aggregate_anywhere(expr: &Expr) -> bool {
    let mut found = false;
    walk_exprs(expr, &mut |node| {
        if is_aggregate_call(node) {
            found = true;
            return true;
        }
        false
    });
    found
}

// ***** HAVING and ORDER BY resolution *****

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `resolveHavingAndOrderBy(ctx, sel, p)`
    /// (`logical_plan_builder.go:2905`)'s HAVING half, over
    /// `havingWindowAndOrderbyExprResolver.Leave` (`:2784`). 6a already landed
    /// the ORDER BY half as [`PlanBuilder::resolve_order_by`].
    ///
    /// Two things happen to the HAVING clause, and both are marker
    /// substitutions:
    ///
    /// * every AGGREGATE call becomes an auxiliary select field named
    ///   `sel_agg_<n>` and is replaced by a [`MarkerKind::Having`] marker —
    ///   Go's `aggMapper[v] = len(a.selectFields)`. It must become a field
    ///   because `having sum(b) < 0` over `select a+1 as b` has to build
    ///   `sum(a+1)`, which is only possible before the projection exists;
    /// * every COLUMN reference resolves SELECT-LIST FIRST (`resolveFieldsFirst`
    ///   is `true` for HAVING outside an aggregate) and becomes a
    ///   [`MarkerKind::Column`] marker on that field's index.
    ///
    /// A column inside a HAVING aggregate is the opposite: Go replaces the
    /// node with `a.selectFields[index].Expr` outright (`:2896`), because the
    /// aggregate is going to be built over SOURCE columns. That arm is here
    /// too.
    ///
    /// Returns the aggregates lifted out of HAVING, in the order their markers
    /// index.
    ///
    /// # Errors
    ///
    /// `ErrWindowInvalidWindowFuncUse` for a window call written in HAVING,
    /// and `ErrUnknownColumn` for a name no scope resolves.
    pub fn resolve_having_and_order_by(
        &mut self,
        having: &mut Expr,
        fields: &mut Vec<ProjectionField>,
        names: &[FieldName],
    ) -> Result<Vec<Expr>, PlanError> {
        self.cur_clause = ClauseCode::Having;
        let mut aggregates = Vec::new();
        let mut error = None;

        // Pass 1: the aggregates. Done first so that pass 2's select-list
        // resolution sees the auxiliary fields Go's single traversal appends
        // as it goes.
        visit_exprs(having, &mut |node| {
            if matches!(node, Expr::Window { .. }) {
                error = Some(PlanError::internal(
                    "Window function is not allowed in HAVING clause",
                ));
                return true;
            }
            if !is_aggregate_call(node) {
                return false;
            }
            let mut agg =
                marker::substitute(node, PlanMarker::new(MarkerKind::Having, aggregates.len()));
            // `:2896` inside an aggregate, a select-list name stands for that
            // field's EXPRESSION, not for its output column.
            visit_exprs(&mut agg, &mut |inner| {
                let Expr::Column(path) = inner else {
                    return false;
                };
                if find_field_name(names, path).is_some() {
                    return true;
                }
                if let Some(index) = resolve_from_select_fields(path, fields, false) {
                    *inner = fields[index].expr.clone();
                }
                true
            });
            aggregates.push(agg);
            true
        });
        if let Some(error) = error {
            return Err(error);
        }

        // Pass 2: the bare columns.
        let old_len = fields.len();
        visit_exprs(having, &mut |node| {
            if !matches!(node, Expr::Column(_)) {
                return false;
            }
            if PlanMarker::from_expr(node).is_some() {
                return true;
            }
            let Expr::Column(path) = &*node else {
                return true;
            };
            let path = path.clone();
            // `resolveFieldsFirst`: HAVING resolves the select list first.
            if let Some(index) = resolve_from_select_fields(&path, &fields[..old_len], false) {
                marker::substitute(node, PlanMarker::new(MarkerKind::Column, index));
                return true;
            }
            // `:2841` "For SQLs like: select a from t b having b.a" — a
            // QUALIFIED name falls back to the source plan.
            if find_field_name(names, &path).is_some() {
                // The column is not projected, so it becomes a hidden extra
                // field the trailing projection trims off, exactly as an
                // unprojected ORDER BY column does.
                let extra = fields[old_len..]
                    .iter()
                    .position(|field| field.expr == *node)
                    .unwrap_or_else(|| {
                        fields.push(ProjectionField {
                            expr: node.clone(),
                            alias: None,
                            text: None,
                            hidden: true,
                        });
                        fields.len() - 1 - old_len
                    });
                marker::substitute(node, PlanMarker::new(MarkerKind::Column, old_len + extra));
                return true;
            }
            // `:2887` a name no scope knows may still be a CORRELATED column,
            // which the rewriter resolves against `outer_names`; only a name
            // NO scope knows is an error.
            if !self
                .outer_names
                .iter()
                .any(|scope| find_field_name(scope, &path).is_some())
            {
                error = Some(PlanError::internal(format!(
                    "Unknown column '{}' in 'having clause'",
                    path.last().cloned().unwrap_or_default()
                )));
            }
            true
        });
        match error {
            Some(error) => Err(error),
            None => Ok(aggregates),
        }
    }
}

// ***** buildAggregation *****

/// The [`MarkerKind::Agg`] / [`MarkerKind::Having`] column vector: Go's
/// `for agg, idx := range totalMap { totalMap[agg] = aggIndexMap[idx] }`
/// (`logical_plan_builder.go:4514`) composed with the aggregation's schema.
///
/// Entry `i` is the output column of the `i`-th EXTRACTED aggregate, which is
/// the schema column `agg_index_map[i]` — a different index whenever
/// [`PlanBuilder::build_aggregation`] combined two identical calls.
#[must_use]
pub fn agg_marker_columns(agg_index_map: &[usize], schema: &Schema) -> Vec<Column> {
    agg_index_map
        .iter()
        .filter_map(|position| {
            let mut column = schema.columns.get(*position)?.clone();
            column.index = *position as i64;
            Some(column)
        })
        .collect()
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildAggregation(ctx, p, aggFuncList, gbyItems, correlatedAggMap)`
    /// (`logical_plan_builder.go:255`).
    ///
    /// Returns the aggregation and Go's `aggIndexMap`: for each entry of
    /// `agg_funcs`, the index of the output column it landed on. Feed it to
    /// [`agg_marker_columns`] to bind [`MarkerKind::Agg`].
    ///
    /// The schema is Go's, in Go's order: one column per DISTINCT aggregate
    /// first, then one `firstrow()` per child column. That second half is what
    /// makes `select a, count(*) from t group by a` able to report `a` at all,
    /// and it is why the aggregation's schema is WIDER than the select list.
    ///
    /// # Errors
    ///
    /// The argument build error, or the aggregate's own type-inference error
    /// (`aggregation.NewAggFuncDesc`).
    pub fn build_aggregation(
        &mut self,
        plan: LogicalPlan,
        agg_funcs: &[Expr],
        group_by_items: Vec<Expression>,
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
    ) -> Result<(LogicalPlan, Vec<usize>), PlanError> {
        self.opt_flag |= flags::BUILD_KEY_INFO
            | flags::PUSH_DOWN_AGG
            | flags::MAX_MIN_ELIMINATE
            | flags::PUSH_DOWN_TOPN
            | flags::PREDICATE_PUSH_DOWN
            | flags::ELIMINATE_AGG
            | flags::ELIMINATE_PROJECTION;
        if self.enable_skew_distinct_agg {
            self.opt_flag |= flags::SKEW_DISTINCT_AGG;
        }
        // Rule 3 of [`super`]: both snapshots precede every move of `plan`.
        let (schema, names) = snapshot_schema_and_names(&plan);
        // `:280` a `ROLLUP` block's Expand supplies the extra group keys.
        let rollup_expand = self.current_block_expand.clone();

        let mut descriptors: Vec<AggFuncDesc> = Vec::with_capacity(agg_funcs.len());
        let mut schema_columns: Vec<Column> = Vec::with_capacity(agg_funcs.len());
        let mut output_names: Vec<FieldName> = Vec::with_capacity(agg_funcs.len());
        let mut agg_index_map: Vec<usize> = Vec::with_capacity(agg_funcs.len());
        let mut all_aggs_first_row = true;

        for agg in agg_funcs {
            let (name, args, distinct, order_by) = decompose_aggregate(agg)?;
            let mut built_args = Vec::with_capacity(args.len());
            for arg in args {
                built_args.push(self.rewrite_scalar(arg, &schema, &names, markers)?);
            }
            let mut descriptor = AggFuncDesc::new(self.ctx, &name, built_args, distinct)
                .map_err(|error| PlanError::internal(format!("{error}")))?;
            if descriptor.name() != agg_names::FIRST_ROW {
                all_aggs_first_row = false;
            }
            // `:294` the aggregate's own ORDER BY (`GROUP_CONCAT`), whose
            // positional items index the call's OWN argument list.
            for item in order_by {
                let resolved = match &item.expr {
                    Expr::Int(digits) => digits
                        .parse::<usize>()
                        .ok()
                        .and_then(|position| args.get(position.wrapping_sub(1)))
                        .cloned()
                        .unwrap_or_else(|| item.expr.clone()),
                    other => other.clone(),
                };
                let built = self.rewrite_scalar(&resolved, &schema, &names, markers)?;
                descriptor
                    .order_by_items
                    .push(ByItems::new(built, item.desc));
            }

            // `:322` "combine identical aggregate functions".
            match descriptors
                .iter()
                .position(|existing| existing.equal(&descriptor))
            {
                Some(position) => agg_index_map.push(position),
                None => {
                    agg_index_map.push(descriptors.len());
                    let mut column =
                        Column::new(self.column_ids.alloc(), descriptor.ret_type().clone());
                    column.index = schema_columns.len() as i64;
                    schema_columns.push(column);
                    // Go appends `types.EmptyName`.
                    output_names.push(FieldName::default());
                    descriptors.push(descriptor);
                }
            }
        }

        // `:366` one `firstrow()` per child column, so the aggregate can still
        // report every column the child had.
        for (index, column) in schema.columns.iter().enumerate() {
            let descriptor = AggFuncDesc::new(
                self.ctx,
                agg_names::FIRST_ROW,
                vec![Expression::Column(column.clone())],
                false,
            )
            .map_err(|error| PlanError::internal(format!("{error}")))?;
            let mut output = column.clone();
            output.ret_type = Some(descriptor.ret_type().clone());
            output.index = schema_columns.len() as i64;
            schema_columns.push(output);
            output_names.push(names.get(index).cloned().unwrap_or_default());
            descriptors.push(descriptor);
        }

        // `:401` `UpdateNotNullFlag4RetType`: an aggregate over a possibly
        // EMPTY group answers NULL, so the NOT NULL flag has to come off
        // unless a GROUP BY guarantees the group is non-empty.
        let has_group_by = !group_by_items.is_empty();
        for (index, descriptor) in descriptors.iter_mut().enumerate() {
            descriptor
                .update_not_null_flag_4_ret_type(has_group_by, all_aggs_first_row)
                .map_err(|error| PlanError::internal(format!("{error}")))?;
            if let Some(column) = schema_columns.get_mut(index) {
                column.ret_type = Some(descriptor.ret_type().clone());
            }
        }

        // `:414` a ROLLUP block groups additionally by the Expand's generated
        // columns, which is what separates the super-aggregate rows.
        let mut group_by_items = group_by_items;
        if let Some(expand) = &rollup_expand {
            if let Some(gid) = &expand.grouping_id_col {
                group_by_items.push(Expression::Column(gid.clone()));
            }
            if let Some(gpos) = &expand.grouping_pos_col {
                group_by_items.push(Expression::Column(gpos.clone()));
            }
        }

        let mut aggregation = LogicalAggregation::new(
            self.base(LogicalAggregation::TYPE),
            descriptors,
            group_by_items,
        );
        aggregation.prefer_agg_type = self.hints.prefer_agg_type;
        aggregation.prefer_agg_to_cop = self.hints.prefer_agg_to_cop;
        aggregation.base.set_children(vec![plan]);
        aggregation
            .base
            .base
            .set_schema(Some(Schema::new(schema_columns)));
        aggregation.base.base.set_output_names(output_names);
        Ok((LogicalPlan::Aggregation(aggregation), agg_index_map))
    }

    /// Go `buildDistinct(child, length)` (`logical_plan_builder.go:1966`): a
    /// `LogicalAggregation` that groups by the first `length` child columns
    /// and reports every child column through `firstrow()`.
    ///
    /// `length` is `oldLen` — the select list WITHOUT the auxiliary ORDER BY /
    /// HAVING columns — so `select distinct a from t order by b` de-duplicates
    /// on `a` alone while `b` still reaches the sort.
    ///
    /// A survey of the crate confirmed nothing prior to this batch built one:
    /// 6a's `build_select` refuses `DISTINCT` with this exact Go symbol, and
    /// `tidb-executor`'s driver de-duplicates ROWS at execution rather than
    /// building an operator. So this is written here rather than reused.
    ///
    /// # Errors
    ///
    /// The `firstrow()` type-inference error.
    pub fn build_distinct(
        &mut self,
        child: LogicalPlan,
        length: usize,
    ) -> Result<LogicalPlan, PlanError> {
        self.opt_flag |= flags::BUILD_KEY_INFO | flags::PUSH_DOWN_AGG;
        let (schema, names) = snapshot_schema_and_names(&child);

        let group_by_items = schema
            .columns
            .iter()
            .take(length)
            .cloned()
            .map(Expression::Column)
            .collect();
        let mut descriptors = Vec::with_capacity(schema.columns.len());
        let mut columns = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let descriptor = AggFuncDesc::new(
                self.ctx,
                agg_names::FIRST_ROW,
                vec![Expression::Column(column.clone())],
                false,
            )
            .map_err(|error| PlanError::internal(format!("{error}")))?;
            // "Distinct will be rewritten as first_row, we reset the type here
            // since the return type of first_row is not always the same as the
            // column arg of first_row." (`:1994`)
            let mut output = column.clone();
            output.ret_type = Some(descriptor.ret_type().clone());
            columns.push(output);
            descriptors.push(descriptor);
        }

        let mut aggregation = LogicalAggregation::new(
            self.base(LogicalAggregation::TYPE),
            descriptors,
            group_by_items,
        );
        aggregation.prefer_agg_type = self.hints.prefer_agg_type;
        aggregation.prefer_agg_to_cop = self.hints.prefer_agg_to_cop;
        aggregation.base.set_children(vec![child]);
        aggregation.base.base.set_schema(Some(Schema::new(columns)));
        aggregation.base.base.set_output_names(names);
        Ok(LogicalPlan::Aggregation(aggregation))
    }

    /// Go `buildSortWithCheck(ctx, p, byItems, aggMapper, windowMapper,
    /// projExprs, oldLen, hasDistinct)` (`logical_plan_builder.go:2403`).
    ///
    /// Go's own body is `buildSort` plus two extra steps per item:
    /// `b.replaceGroupingFunc(it)` and, under `DISTINCT`,
    /// `b.checkOrderByInDistinct(...)`. Both are here, the second as
    /// [`PlanBuilder::check_order_by_in_distinct_ast`], which the caller runs
    /// over the whole clause before this — see that method for why the check
    /// is on the AST.
    ///
    /// # Errors
    ///
    /// [`PlanBuilder::build_sort`]'s errors, plus 3065 / 3066 from the
    /// `DISTINCT` check.
    pub fn build_sort_with_check(
        &mut self,
        plan: LogicalPlan,
        items: &[OrderItem],
        markers: &BTreeMap<MarkerKind, Vec<Column>>,
        select: &SelectStmt,
        names: &[FieldName],
    ) -> Result<LogicalPlan, PlanError> {
        self.check_order_by_in_distinct_ast(select, names)?;
        let sorted = self.build_sort(plan, items, markers)?;
        Ok(self.replace_grouping_func_in_sort(sorted))
    }

    /// Go `addAliasName(ctx, selectStmt, p)`
    /// (`logical_plan_builder.go:4141`): every select field gains an explicit
    /// alias, so that a view's column names are fixed at creation.
    ///
    /// A field that IS a column takes that column's written name; anything
    /// else takes the name [`super::PlanBuilder::projection_field_name`]
    /// would give it. The `AsViewSchema` dedup half is a narrowing; see this
    /// module's header.
    pub fn add_alias_name(fields: &mut [ProjectionField], names: &[FieldName]) {
        for field in fields.iter_mut() {
            if field.alias.is_some() {
                continue;
            }
            field.alias = Some(match &field.expr {
                Expr::Column(path) => path.last().cloned().unwrap_or_default(),
                other => {
                    let resolved = match other {
                        Expr::Column(path) => find_field_name(names, path),
                        _ => None,
                    };
                    match resolved.and_then(|index| names.get(index)) {
                        Some(name) => name.names.column.original.clone(),
                        None => field.text.clone().unwrap_or_else(|| field.expr.restore()),
                    }
                }
            });
        }
    }
}

/// The `(name, args, distinct, order_by)` of an aggregate call node, in the
/// shape `aggregation.NewAggFuncDesc` takes.
///
/// Go reads `aggFunc.F`, `aggFunc.Args`, `aggFunc.Distinct` and
/// `aggFunc.Order` off one `*ast.AggregateFuncExpr`; `tidb_ast` splits
/// `GROUP_CONCAT` into its own variant because its arity and separator differ,
/// so the two are recombined here.
///
/// `GROUP_CONCAT`'s separator is Go's LAST argument — which is exactly why
/// `buildAggregation` writes `trueArgs := aggFunc.Args[:len(aggFunc.Args)-1]`
/// before resolving the `ORDER BY` positions (`:295`).
type DecomposedAggregate<'a> = (String, &'a [Expr], bool, &'a [OrderItem]);

fn decompose_aggregate(agg: &Expr) -> Result<DecomposedAggregate<'_>, PlanError> {
    match agg {
        Expr::Aggregate {
            name,
            distinct,
            args,
        } => Ok((name.to_ascii_lowercase(), args, *distinct, &[])),
        Expr::GroupConcat {
            distinct,
            args,
            order_by,
            ..
        } => Ok((
            agg_names::GROUP_CONCAT.to_owned(),
            args,
            *distinct,
            order_by,
        )),
        other => Err(PlanError::internal(format!(
            "not an aggregate function: {}",
            other.restore()
        ))),
    }
}

/// Go's `expression.ExtractColumns` / `ExtractCorColumns` pair as the
/// aggregation's own consumers use them; re-exported here so a caller need not
/// reach into `tidb_expr` for the two halves separately.
#[must_use]
pub fn columns_and_correlated(expr: &Expression) -> (Vec<Column>, Vec<CorrelatedColumn>) {
    (extract_columns(expr), extract_cor_columns(expr))
}
