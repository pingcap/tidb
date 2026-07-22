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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Row-wise projection, `GROUP BY`/aggregation, `HAVING`, window functions
//! combined with `GROUP BY` (via `crate::window::compute_window`, over the
//! HAVING-surviving groups — see `Database::aggregate`'s own doc for the
//! exact sequencing), `ONLY_FULL_GROUP_BY` column-scope validation
//! (`check_group_by_scope`), folding an aggregate hidden anywhere inside a
//! non-`Binary`/`Unary`/`Case` expression shape (`fold_group_aggregates`),
//! and folding already-resolved values by one aggregate function's own
//! semantics (`fold_aggregate_values`, shared with `crate::window`'s
//! window-aggregate shape). Called from `crate::select`.

use std::cmp::Ordering;

use tidb_ast::{
    BinaryOp, CastExpr, Expr, OrderItem, SelectField, SelectStmt, WindowDef, WindowOver,
};
use tidb_datatype::Datum;
use tidb_expr::{
    apply_binary, apply_binary_with_div_precision, apply_unary, eval_in, truthy_of, Columns,
    EvalError,
};
use tidb_planner::aggregation_descriptor::AggregateKind;

use crate::catalog::Column;
/// Tuple DISTINCT identity shared by the aggregate functions and the prepared
/// read's `SELECT DISTINCT` dedup.
pub mod aggregate_distinct {
    include!("aggregate_distinct.rs");
}
pub mod runtime;
use crate::literal::value_to_literal;
use crate::order::{cmp_keys, positional, resolve_order_keys};
use crate::select::{is_truthy, wildcard_indices};
use crate::session::RelResolver;
use crate::session_settings::NoopFunctionsMode;
use crate::subquery::is_subquery_free;
use crate::{Database, ExecError, Row};
use aggregate_distinct::DistinctChecker;

impl Database {
    /// Projects one relation row through the select list (no aggregation).
    /// Subqueries in the select list are resolved per row (an uncorrelated one
    /// just recomputes the same value each time; a correlated one sees `outer`
    /// as its enclosing scope), the same one-code-path approach `WHERE` uses.
    pub(crate) fn project_row(
        &self,
        fields: &[SelectField],
        cols: &[Column],
        row: &Row,
        outer: Option<&dyn Columns>,
    ) -> Result<Row, ExecError> {
        let resolver = RelResolver::with_outer(cols, row, outer, self.session_state());
        let mut out = Row::new();
        for field in fields {
            match field {
                SelectField::Wildcard(qualifier) => {
                    for i in wildcard_indices(cols, qualifier)? {
                        out.push(row[i].clone());
                    }
                }
                SelectField::Expr { expr, .. } => {
                    if is_subquery_free(expr) {
                        out.push(eval_in(expr, &resolver)?);
                    } else {
                        let folded = self.resolve_subqueries(expr, &resolver)?;
                        out.push(eval_in(&folded, &resolver)?);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Groups the passing rows by the `GROUP BY` keys and evaluates the select
    /// list per group. `GROUP BY` may itself reference a select-list alias
    /// (`SELECT dept AS x, COUNT(*) FROM t GROUP BY x`, confirmed via
    /// `gorun`) — resolved via `crate::order::resolve_alias` BEFORE grouping,
    /// so `HAVING`/`ORDER BY`/the select list may then reference EITHER the
    /// alias or the underlying real column (`HAVING dept = 'a'` works even
    /// when `GROUP BY` was written as `x`). A query with aggregates but no
    /// `GROUP BY` forms a single group over all rows (even when empty). A
    /// `HAVING` predicate filters whole groups; a window function (see
    /// `crate::window`'s own doc) then computes over the SURVIVING groups as
    /// post-aggregation "virtual rows," one per group (confirmed via `gorun`,
    /// not assumed). Subqueries in the select list, `HAVING`, or a grouped
    /// `ORDER BY` are resolved against the group's first row (all rows in a
    /// group share the same `GROUP BY` key, so this is consistent regardless
    /// of which row is picked), with `outer` as their enclosing scope for a
    /// correlated query.
    pub(crate) fn aggregate(
        &self,
        sel: &SelectStmt,
        cols: &[Column],
        passing: &[&Row],
        outer: Option<&dyn Columns>,
    ) -> Result<Vec<Row>, ExecError> {
        // Real MySQL/TiDB rejects ANY explicit `GROUP BY` direction
        // (`ASC` or `DESC`) at EXECUTION time by default — confirmed via
        // `gorun`: `[expression:1235] function GROUP BY expr ASC|DESC
        // has only noop implementation in tidb now, use
        // tidb_enable_noop_functions to enable these functions`. The
        // direction is a compatibility-only no-op when that session switch
        // is enabled, so grouping still follows the normal key path below.
        // See `tidb_ast::GroupByItem`'s own doc for why an explicit `ASC`
        // needs the exact same gate as `DESC`, even though it restores
        // identically to no direction at all.
        if self.noop_functions_mode == NoopFunctionsMode::Off
            && sel.group_by.iter().any(|item| item.desc.is_some())
        {
            return Err(ExecError::Unsupported("GROUP BY expr ASC|DESC"));
        }
        // `WITH ROLLUP` has a REAL, multi-level semantic effect on real
        // TiDB's own result rows (super-aggregate rows at every `GROUP BY`
        // prefix length, confirmed via `gorun`) — this crate deliberately
        // does NOT replicate that (a clean `Unsupported` rather than a
        // partial/incorrect implementation), see `tidb_ast::SelectStmt::
        // rollup`'s own doc.
        if sel.rollup {
            return Err(ExecError::Unsupported("GROUP BY WITH ROLLUP"));
        }
        // `GROUP BY` may reference a select-list alias OR a positional
        // ordinal (`GROUP BY 1`, or even `GROUP BY true` — MySQL/TiDB
        // treats a boolean literal here as `1`/`0` too, confirmed via
        // `gorun`) — resolved once here, upfront via the SAME whole-item
        // resolution `ORDER BY` itself uses (`crate::order::resolve_by_item`),
        // so grouping, the `ONLY_FULL_GROUP_BY` "pinned" derivation below,
        // and the "exact `GROUP BY` expression" structural match all
        // agree on the SAME (real-column) resolved shape — `HAVING dept =
        // 'a'` and `ORDER BY dept` both work even though `GROUP BY` was
        // written as `x`, confirmed via `gorun`.
        let group_by: Vec<&Expr> = sel
            .group_by
            .iter()
            .map(|item| crate::order::resolve_by_item(&item.expr, &sel.fields))
            .collect::<Result<_, _>>()?;

        let mut groups: Vec<Vec<&Row>> = if group_by.is_empty() {
            vec![passing.to_vec()]
        } else {
            let mut groups: Vec<(Vec<Datum>, Vec<&Row>)> = Vec::new();
            let session = self.session_state();
            for &row in passing {
                let resolver = RelResolver::new(cols, row, session.clone());
                let keyvals = group_by
                    .iter()
                    .map(|e| eval_in(e, &resolver))
                    .collect::<Result<Vec<_>, _>>()?;
                match groups.iter_mut().find(|(k, _)| *k == keyvals) {
                    Some((_, rows)) => rows.push(row),
                    None => groups.push((keyvals, vec![row])),
                }
            }
            groups.into_iter().map(|(_, rows)| rows).collect()
        };

        let order_keys = resolve_order_keys(&sel.order_by, &sel.fields)?;
        // `HAVING` may reference a select-list alias ANYWHERE in its
        // expression (confirmed via `gorun`: `HAVING c + 1 > 3` where `c`
        // aliases `COUNT(*)`), unlike the select list itself, where a later
        // field referencing an earlier field's own alias is a genuine
        // `ERR` — resolved once here, upfront, so both the scope check
        // below and every group's evaluation see the same rewritten
        // expression.
        let having = sel
            .having
            .as_ref()
            .map(|h| crate::order::resolve_having_aliases(h, &sel.fields));
        check_group_by_scope(sel, &group_by, &order_keys, having.as_ref())?;

        // `HAVING` filters whole groups BEFORE window computation (confirmed
        // via `gorun`: a `HAVING`-excluded group is invisible to e.g.
        // `ROW_NUMBER() OVER (...)` too, not just to the final output) —
        // moved ahead of where the previous (pre-window-function) version of
        // this function applied it, inline in the final per-group
        // projection loop AFTER the `ORDER BY` sort below. This reordering
        // does not change behavior for a query with no window function:
        // filtering a sequence and then sorting the survivors gives the
        // exact same relative order as sorting everything and then
        // filtering, since sorting is deterministic on the retained values
        // and filtering does not itself reorder anything.
        if let Some(having) = &having {
            let mut filtered = Vec::with_capacity(groups.len());
            for group in groups {
                if is_truthy(self.eval_group(having, &group, cols, outer)?)? {
                    filtered.push(group);
                }
            }
            groups = filtered;
        }

        // Window functions: collect every DISTINCT window call across the
        // select list / ORDER BY keys, and compute each one's value for
        // every (HAVING-surviving) group now, in `groups`'s current
        // (pre-sort) order — a no-op when the query has none, so the
        // ordinary path below is unaffected.
        let mut window_exprs: Vec<Expr> = Vec::new();
        for field in &sel.fields {
            if let SelectField::Expr { expr, .. } = field {
                crate::window::collect_windows_in(expr, &mut window_exprs);
            }
        }
        for (e, _) in &order_keys {
            crate::window::collect_windows_in(e, &mut window_exprs);
        }
        let windows = window_exprs
            .into_iter()
            .map(|w| {
                let Expr::Window { name, args, over } = &w else {
                    unreachable!("collect_windows_in only ever pushes Expr::Window")
                };
                let WindowOver::Def(WindowDef { spec, .. }) = over else {
                    unreachable!("resolve_named_windows already normalized every OVER clause")
                };
                self.compute_window(name, args, spec, &groups, cols, outer)
                    .map(|vals| (w.clone(), vals))
            })
            .collect::<Result<Vec<_>, ExecError>>()?;

        // `orig_idx[i]` tracks `groups[i]`'s index into `windows`' value
        // vectors (computed against the pre-sort order) as groups get
        // reordered by `ORDER BY` below — a window value must follow its
        // own group through the sort, exactly like `crate::select`'s
        // row-wise path does for individual rows.
        let mut orig_idx: Vec<usize> = (0..groups.len()).collect();
        if !order_keys.is_empty() {
            let descs: Vec<bool> = order_keys.iter().map(|(_, d)| *d).collect();
            let mut keyed = Vec::with_capacity(groups.len());
            for (i, group) in groups.into_iter().enumerate() {
                let kv = order_keys
                    .iter()
                    .map(|(e, _)| {
                        let resolved = crate::window::resolve_windows(e, &windows, i);
                        self.eval_group(&resolved, &group, cols, outer)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                keyed.push((kv, i, group));
            }
            keyed.sort_by(|a, b| cmp_keys(&a.0, &b.0, &descs));
            orig_idx = keyed.iter().map(|(_, i, _)| *i).collect();
            groups = keyed.into_iter().map(|(_, _, g)| g).collect();
        }

        let mut out = Vec::new();
        for (group, &i) in groups.iter().zip(&orig_idx) {
            let mut result_row = Row::new();
            for field in &sel.fields {
                let SelectField::Expr { expr, .. } = field else {
                    return Err(ExecError::Wildcard); // `*` with GROUP BY unsupported
                };
                let resolved = crate::window::resolve_windows(expr, &windows, i);
                result_row.push(self.eval_group(&resolved, group, cols, outer)?);
            }
            out.push(result_row);
        }
        Ok(out)
    }

    /// Evaluates an expression for one group. An aggregate folds over the
    /// group's rows; operators recurse (so `COUNT(*) + 1` and `SUM(v) > 100`
    /// work); any aggregate-free leaf — a `GROUP BY` key, a constant, or a
    /// (possibly correlated) subquery — is resolved and evaluated against the
    /// group's first row. This is shared by select-list projection, `HAVING`,
    /// grouped `ORDER BY`, and — for a single-row "group" — `crate::window`'s
    /// window-function evaluation (a one-element group's first row is that
    /// row itself, making the row-wise and grouped cases the same code path).
    pub(crate) fn eval_group(
        &self,
        expr: &Expr,
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Datum, ExecError> {
        match expr {
            Expr::Aggregate {
                name,
                distinct,
                args,
            } => self.compute_aggregate(name, *distinct, args, group, cols, outer),
            Expr::GroupConcat {
                distinct,
                args,
                order_by,
                separator,
            } => {
                self.compute_group_concat(*distinct, args, order_by, separator, group, cols, outer)
            }
            Expr::Paren(inner) => self.eval_group(inner, group, cols, outer),
            Expr::Unary(op, inner) if expr_has_aggregate(inner) => Ok(apply_unary(
                *op,
                self.eval_group(inner, group, cols, outer)?,
            )?),
            Expr::Binary(op, left, right)
                if expr_has_aggregate(left) || expr_has_aggregate(right) =>
            {
                let l = self.eval_group(left, group, cols, outer)?;
                let r = self.eval_group(right, group, cols, outer)?;
                Ok(apply_binary_with_div_precision(
                    *op,
                    l,
                    r,
                    u32::from(self.div_precision_increment.value()),
                )?)
            }
            // `CASE WHEN COUNT(*) > 5 THEN dept.name ELSE 'other' END`:
            // the SAME lazy short-circuit selection `tidb_expr::eval_in`
            // implements for `Expr::Case`, mirrored here so an aggregate
            // anywhere in the branches folds correctly — but recursing
            // via `self.eval_group` (not `eval_in`) for each condition/
            // result, so a mix of aggregate and non-aggregate branches
            // both work (a non-aggregate sub-expression falls through
            // `eval_group`'s own leaf case below, evaluated against
            // `group.first()` exactly as it would outside a CASE).
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } if expr_has_aggregate(expr) => {
                let taken = match value {
                    Some(value_expr) => {
                        let v = self.eval_group(value_expr, group, cols, outer)?;
                        let mut taken = None;
                        for (cond, result) in when_clauses {
                            let w = self.eval_group(cond, group, cols, outer)?;
                            if apply_binary(BinaryOp::Eq, v.clone(), w)? == Datum::Int(1) {
                                taken = Some(result);
                                break;
                            }
                        }
                        taken
                    }
                    None => {
                        let mut taken = None;
                        for (cond, result) in when_clauses {
                            if truthy_of(&self.eval_group(cond, group, cols, outer)?)? == Some(true)
                            {
                                taken = Some(result);
                                break;
                            }
                        }
                        taken
                    }
                };
                match taken {
                    Some(result) => self.eval_group(result, group, cols, outer),
                    None => match else_clause {
                        Some(e) => self.eval_group(e, group, cols, outer),
                        None => Ok(Datum::Null),
                    },
                }
            }
            // Every other shape that can hide an aggregate somewhere within
            // it (`IF(cond, COUNT(*), 0)`, `HAVING COUNT(*) BETWEEN 1 AND
            // 5`, `HAVING COUNT(*) IN (1, 2)`, ...): fold every
            // `Aggregate`/`GroupConcat` leaf down to its own group-scoped
            // literal first ([`Database::fold_group_aggregates`]), then
            // recurse — the rebuilt, now aggregate-free expression falls
            // through to the ordinary per-row path below.
            _ if expr_has_aggregate(expr) => {
                let folded = self.fold_group_aggregates(expr, group, cols, outer)?;
                self.eval_group(&folded, group, cols, outer)
            }
            _ => match group.first() {
                Some(row) => {
                    let cur = RelResolver::with_outer(cols, row, outer, self.session_state());
                    let folded = self.resolve_subqueries(expr, &cur)?;
                    Ok(eval_in(&folded, &cur)?)
                }
                None => Ok(Datum::Null), // empty single group: bare columns are NULL
            },
        }
    }

    /// Folds every `Aggregate`/`GroupConcat` LEAF within `expr` down to its
    /// own group-scoped value (spliced back as a literal via
    /// [`crate::literal::value_to_literal`]), leaving every other node's shape
    /// otherwise intact, so the caller can defer the rebuilt, now
    /// aggregate-free expression to the ordinary per-row evaluator.
    /// Mirrors [`check_columns_pinned`]'s own traversal shape: a nested
    /// subquery's OWN body is left completely untouched (resolved
    /// separately, later, by `resolve_subqueries` against the group's
    /// representative row), but the outer-scope operand of `InSubquery`/
    /// `CompareSubquery` IS folded, since it evaluates in THIS query's own
    /// scope, not the subquery's. `Expr::Case` is deliberately NOT handled
    /// here — it has its own dedicated lazy-evaluation arm in
    /// [`Database::eval_group`] (only the TAKEN branch is ever folded,
    /// matching real `CASE`'s short-circuit semantics; eagerly folding
    /// every branch here would break that), so this is only ever reached
    /// for shapes that arm's own guard doesn't already claim.
    fn fold_group_aggregates(
        &self,
        expr: &Expr,
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Expr, ExecError> {
        if !expr_has_aggregate(expr) {
            return Ok(expr.clone());
        }
        Ok(match expr {
            Expr::Aggregate { .. } | Expr::GroupConcat { .. } => {
                value_to_literal(self.eval_group(expr, group, cols, outer)?)
            }
            Expr::Unary(op, inner) => Expr::Unary(
                *op,
                Box::new(self.fold_group_aggregates(inner, group, cols, outer)?),
            ),
            Expr::Paren(inner) => Expr::Paren(Box::new(
                self.fold_group_aggregates(inner, group, cols, outer)?,
            )),
            Expr::Assign { name, value } => Expr::Assign {
                name: name.clone(),
                value: Box::new(self.fold_group_aggregates(value, group, cols, outer)?),
            },
            Expr::Trim {
                expr,
                remstr,
                direction,
            } => Expr::Trim {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                remstr: remstr
                    .as_deref()
                    .map(|r| self.fold_group_aggregates(r, group, cols, outer))
                    .transpose()?
                    .map(Box::new),
                direction: *direction,
            },
            Expr::Position { substr, str } => Expr::Position {
                substr: Box::new(self.fold_group_aggregates(substr, group, cols, outer)?),
                str: Box::new(self.fold_group_aggregates(str, group, cols, outer)?),
            },
            Expr::WeightString { expr, as_type } => Expr::WeightString {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                as_type: *as_type,
            },
            Expr::Binary(op, left, right) => Expr::Binary(
                *op,
                Box::new(self.fold_group_aggregates(left, group, cols, outer)?),
                Box::new(self.fold_group_aggregates(right, group, cols, outer)?),
            ),
            Expr::Func { name, args } => Expr::Func {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| self.fold_group_aggregates(a, group, cols, outer))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            Expr::GenericFuncCall { schema, name, args } => Expr::GenericFuncCall {
                schema: schema.clone(),
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| self.fold_group_aggregates(a, group, cols, outer))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            Expr::Row(values) => Expr::Row(
                values
                    .iter()
                    .map(|v| self.fold_group_aggregates(v, group, cols, outer))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Expr::Interval { value, unit } => Expr::Interval {
                value: Box::new(self.fold_group_aggregates(value, group, cols, outer)?),
                unit: unit.clone(),
            },
            Expr::Extract { unit, value } => Expr::Extract {
                unit: unit.clone(),
                value: Box::new(self.fold_group_aggregates(value, group, cols, outer)?),
            },
            Expr::TimestampAdd {
                unit,
                interval,
                expr,
            } => Expr::TimestampAdd {
                unit: unit.clone(),
                interval: Box::new(self.fold_group_aggregates(interval, group, cols, outer)?),
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
            },
            Expr::TimestampDiff { unit, expr1, expr2 } => Expr::TimestampDiff {
                unit: unit.clone(),
                expr1: Box::new(self.fold_group_aggregates(expr1, group, cols, outer)?),
                expr2: Box::new(self.fold_group_aggregates(expr2, group, cols, outer)?),
            },
            Expr::GetFormat { selector, expr } => Expr::GetFormat {
                selector: *selector,
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
            },
            Expr::In { expr, list, not } => Expr::In {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                list: list
                    .iter()
                    .map(|e| self.fold_group_aggregates(e, group, cols, outer))
                    .collect::<Result<Vec<_>, _>>()?,
                not: *not,
            },
            Expr::Between {
                expr,
                low,
                high,
                not,
            } => Expr::Between {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                low: Box::new(self.fold_group_aggregates(low, group, cols, outer)?),
                high: Box::new(self.fold_group_aggregates(high, group, cols, outer)?),
                not: *not,
            },
            Expr::Like {
                expr,
                pattern,
                not,
                escape,
            } => Expr::Like {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                pattern: Box::new(self.fold_group_aggregates(pattern, group, cols, outer)?),
                not: *not,
                escape: *escape,
            },
            Expr::Regexp { expr, pattern, not } => Expr::Regexp {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                pattern: Box::new(self.fold_group_aggregates(pattern, group, cols, outer)?),
                not: *not,
            },
            Expr::Collate { expr, collation } => Expr::Collate {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                collation: collation.clone(),
            },
            Expr::Cast(cast) => Expr::Cast(CastExpr {
                expr: Box::new(self.fold_group_aggregates(&cast.expr, group, cols, outer)?),
                cast_type: cast.cast_type.clone(),
                style: cast.style,
                array: cast.array,
            }),
            Expr::ConvertUsing { expr, charset } => Expr::ConvertUsing {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                charset: charset.clone(),
            },
            Expr::MatchAgainst {
                columns,
                against,
                modifier,
            } => Expr::MatchAgainst {
                columns: columns.clone(),
                against: Box::new(self.fold_group_aggregates(against, group, cols, outer)?),
                modifier: *modifier,
            },
            Expr::MemberOf { expr, array } => Expr::MemberOf {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                array: Box::new(self.fold_group_aggregates(array, group, cols, outer)?),
            },
            Expr::Is { expr, target, not } => Expr::Is {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                target: *target,
                not: *not,
            },
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => Expr::InSubquery {
                expr: Box::new(self.fold_group_aggregates(expr, group, cols, outer)?),
                subquery: subquery.clone(),
                not: *not,
            },
            Expr::CompareSubquery {
                op,
                left,
                all,
                subquery,
            } => Expr::CompareSubquery {
                op: *op,
                left: Box::new(self.fold_group_aggregates(left, group, cols, outer)?),
                all: *all,
                subquery: subquery.clone(),
            },
            // Anything else with `expr_has_aggregate(expr) == true` is
            // `Expr::Case` (its own dedicated arm in `eval_group` claims
            // it first, so this is unreachable in practice) — returned
            // unchanged rather than panicking, since eliminating this
            // arm entirely would require `expr_has_aggregate` and this
            // match to be proven exhaustively in sync, which isn't worth
            // the fragility for a genuinely unreachable case.
            other => other.clone(),
        })
    }

    /// Computes the canonical runtime aggregate families over a group's rows:
    /// `COUNT`/`SUM`/`AVG`/`MAX`/`MIN`, variance/standard deviation,
    /// and `BIT_*`. An internal `FIRST_ROW` state also exists in the shared
    /// runtime, but the SQL parser does not expose it. The argument is resolved for
    /// subqueries per row (so `SUM((SELECT …))` and a correlated
    /// `COUNT((SELECT … WHERE t.k = outer.k))` both work, the same
    /// one-code-path approach as everywhere else). `NULL`s are ignored;
    /// `DISTINCT` folds duplicates first. Also reused, unchanged, by
    /// `crate::window::compute_window` for supported frame-based window
    /// aggregates (`SUM(x) OVER (...)`, including variance/stddev) — `group` there is the window's
    /// own default FRAME rather than a `GROUP BY` group, but the fold
    /// logic itself doesn't need to know the difference.
    ///
    /// `args.len() > 1` is only reachable for `COUNT(DISTINCT a, b, ...)`
    /// (`tidb_parser::parse_aggregate` rejects every other multi-arg shape
    /// except `APPROX_COUNT_DISTINCT`, which isn't in this value domain
    /// either) — dispatched to [`Self::compute_count_distinct_tuple`],
    /// whose per-row NULL/dedup rule was confirmed via `gorun` against
    /// real TiDB's own `pkg/executor/aggfuncs/func_count_distinct.go`
    /// (`baseCountDistinct4MultiArgs.UpdatePartialResult`): a row is
    /// skipped entirely if ANY argument is `NULL`, otherwise the full
    /// tuple is deduped.
    pub(crate) fn compute_aggregate(
        &self,
        name: &str,
        distinct: bool,
        args: &[Expr],
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Datum, ExecError> {
        let kind = AggregateKind::from_name(name).ok_or(ExecError::Eval(
            EvalError::Unsupported("aggregate function"),
        ))?;
        if args.len() > 1 {
            if kind == AggregateKind::Count {
                return self.compute_count_distinct_tuple(args, group, cols, outer);
            }
            // `APPROX_COUNT_DISTINCT(a, b, ...)` — parse/restore only, same
            // scope-cut as its own single-arg form (no arm in
            // `fold_aggregate_values` below).
            return Err(ExecError::Eval(EvalError::Unsupported(
                "aggregate function",
            )));
        }
        // `COUNT(*)` uses the literal `1`, which is never NULL.
        let arg = &args[0];
        let session = self.session_state();
        let vals: Vec<Datum> = group
            .iter()
            .map(|row| {
                let cur = RelResolver::with_outer(cols, row, outer, session.clone());
                let folded = self.resolve_subqueries(arg, &cur)?;
                Ok(eval_in(&folded, &cur)?)
            })
            .collect::<Result<_, ExecError>>()?;
        fold_aggregate_values(
            kind,
            distinct,
            &vals,
            u32::from(self.div_precision_increment.value()),
        )
    }

    /// `COUNT(DISTINCT a, b, ...)` — counts distinct non-NULL `(a, b, ...)`
    /// tuples: a row is skipped entirely the instant any one argument
    /// evaluates to `NULL` (matching real TiDB's own short-circuit order,
    /// see [`Self::compute_aggregate`]'s own doc), otherwise the full
    /// tuple is deduped through the same source-shaped checker used by every
    /// other aggregate DISTINCT path.
    fn compute_count_distinct_tuple(
        &self,
        args: &[Expr],
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Datum, ExecError> {
        let session = self.session_state();
        let mut distinct = DistinctChecker::new();
        let mut count = 0_i64;
        for row in group {
            let cur = RelResolver::with_outer(cols, row, outer, session.clone());
            let mut tuple = Vec::with_capacity(args.len());
            let mut has_null = false;
            for arg in args {
                let folded = self.resolve_subqueries(arg, &cur)?;
                let v = eval_in(&folded, &cur)?;
                if v == Datum::Null {
                    has_null = true;
                    break;
                }
                tuple.push(v);
            }
            if !has_null && distinct.check(&tuple) {
                count += 1;
            }
        }
        Ok(Datum::Int(count))
    }

    /// `GROUP_CONCAT`: each row's arguments concatenate exactly like
    /// `CONCAT(args...)` — reusing `tidb-expr`'s own NULL-propagation and
    /// int/string coercion rather than reimplementing them — and a row whose
    /// concatenation is `NULL` (any argument `NULL`, matching `CONCAT`)
    /// contributes nothing to the group. `order_by`, if non-empty, sorts the
    /// GROUP's OWN rows first (see [`Self::group_concat_order`]'s own doc)
    /// — confirmed via `gorun` that `DISTINCT` then dedupes in THAT sorted
    /// order, keeping the first post-sort occurrence. Equal ORDER BY keys have
    /// no promised relative order, matching Go's heap plus unstable sort. The
    /// non-`NULL` per-row results are then joined
    /// with `separator`; an empty result (an empty or all-`NULL` group) is
    /// `NULL`, same as `SUM`.
    #[allow(clippy::too_many_arguments)]
    fn compute_group_concat(
        &self,
        distinct: bool,
        args: &[Expr],
        order_by: &[OrderItem],
        separator: &str,
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Datum, ExecError> {
        let ordered: Vec<&Row>;
        let rows: &[&Row] = if order_by.is_empty() {
            group
        } else {
            ordered = self.group_concat_order(args, order_by, group, cols, outer)?;
            &ordered
        };
        let mut evaluated = Vec::with_capacity(rows.len());
        let session = self.session_state();
        for row in rows {
            let cur = RelResolver::with_outer(cols, row, outer, session.clone());
            let mut tuple = Vec::with_capacity(args.len());
            for arg in args {
                let folded = self.resolve_subqueries(arg, &cur)?;
                let value = eval_in(&folded, &cur)?;
                if value == Datum::Null {
                    tuple.clear();
                    break;
                }
                tuple.push(value);
            }
            if tuple.is_empty() {
                continue;
            }
            let value = tidb_expr::concat_values(&tuple)?;
            let rendered = value
                .as_raw_bytes()
                .ok_or(EvalError::Unsupported("GROUP_CONCAT argument rendering"))?
                .to_vec();
            evaluated.push((tuple, rendered));
        }

        let result = if !order_by.is_empty() {
            // `group_concat_order` has already applied the full Datum/collation
            // comparator. Feed monotonic keys to the canonical bounded top-N
            // state so eviction, row truncation, and separator truncation are
            // still owned by the translated GROUP_CONCAT runtime.
            let mut state = if distinct {
                crate::group_concat::OrderedGroupConcatState::new_distinct(
                    separator,
                    0,
                    vec![false],
                )
            } else {
                crate::group_concat::OrderedGroupConcatState::new(separator, 0, vec![false])
            };
            for (position, (tuple, rendered)) in evaluated.into_iter().enumerate() {
                let order_key = vec![(position as u64).to_be_bytes().to_vec()];
                if distinct {
                    state.update_distinct(group_concat_distinct_key(&tuple)?, order_key, rendered);
                } else {
                    state.update(order_key, rendered);
                }
            }
            state.finalize();
            state.finish().map(|value| value.to_vec())
        } else if distinct {
            let mut state = crate::group_concat::DistinctGroupConcatState::new(separator, 0);
            for (tuple, rendered) in evaluated {
                state.update(&group_concat_distinct_key(&tuple)?, &rendered);
            }
            state.finalize();
            state.finish().map(|value| value.to_vec())
        } else {
            let mut state = crate::group_concat::GroupConcatState::new(separator, 0);
            for (_, rendered) in evaluated {
                state.update_bytes(&[Some(&rendered)]);
            }
            state.finish().map(|value| value.to_vec())
        };
        Ok(result.map_or(Datum::Null, Datum::new_string))
    }

    /// Sorts `GROUP_CONCAT`'s own group of rows by `order_by`. Equal keys have
    /// no relative-order guarantee, matching Go's `heap` plus `sort.Sort`
    /// implementation rather than inventing scan-order stability. An
    /// `order_by` item is resolved
    /// POSITIONALLY against `args` (`GROUP_CONCAT(a, b ORDER BY 1)` sorts
    /// by `a`, the SAME `N`-refers-to-a-position rule
    /// [`crate::order::positional`] already implements for the outer
    /// query's own `ORDER BY`/`GROUP BY`, just against a DIFFERENT list —
    /// `GROUP_CONCAT`'s own args, not the select list, confirmed via
    /// `gorun`: the positional item's VALUE used for sorting is the
    /// referenced arg's own value, not affected by how many OTHER args
    /// are concatenated alongside it); any other item is evaluated as its
    /// own raw expression against the row directly (real MySQL allows
    /// ordering by an expression unrelated to the concatenated args too,
    /// e.g. `GROUP_CONCAT(a ORDER BY b)`).
    fn group_concat_order<'a>(
        &self,
        args: &[Expr],
        order_by: &[OrderItem],
        group: &[&'a Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Vec<&'a Row>, ExecError> {
        let session = self.session_state();
        let mut keyed: Vec<(Vec<Datum>, &Row)> = Vec::with_capacity(group.len());
        for &row in group {
            let cur = RelResolver::with_outer(cols, row, outer, session.clone());
            let mut key = Vec::with_capacity(order_by.len());
            for item in order_by {
                let resolved = match positional(&item.expr)? {
                    Some(pos) => args
                        .get(pos)
                        .ok_or(ExecError::Unsupported("GROUP_CONCAT ORDER BY position"))?,
                    None => &item.expr,
                };
                let folded = self.resolve_subqueries(resolved, &cur)?;
                key.push(eval_in(&folded, &cur)?);
            }
            keyed.push((key, row));
        }
        let descs: Vec<bool> = order_by.iter().map(|item| item.desc).collect();
        keyed.sort_unstable_by(|a, b| cmp_keys(&a.0, &b.0, &descs));
        Ok(keyed.into_iter().map(|(_, row)| row).collect())
    }
}

/// Encodes the GROUP_CONCAT DISTINCT argument tuple after each argument's
/// EvalString conversion and immutable collation key, matching Go's
/// `codec.EncodeBytes(collator.ImmutableKey(v))` ownership boundary.
fn group_concat_distinct_key(tuple: &[Datum]) -> Result<Vec<u8>, ExecError> {
    let mut encoded = Vec::new();
    for value in tuple {
        let rendered = tidb_expr::concat_values(std::slice::from_ref(value))?;
        let bytes = rendered.as_raw_bytes().ok_or(EvalError::Unsupported(
            "GROUP_CONCAT DISTINCT key rendering",
        ))?;
        let key = value
            .collation()
            .unwrap_or(tidb_datatype::Collation::Binary)
            .key(bytes);
        encoded.extend_from_slice(&(key.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&key);
    }
    Ok(encoded)
}

/// Folds already-resolved scalar values by one aggregate function's own
/// semantics. Split out of [`Database::compute_aggregate`] so
/// `crate::window`'s window-aggregate shape (`SUM(...) OVER (...)` etc.,
/// combined with `GROUP BY`) can fold one value PER GROUP obtained via
/// [`Database::eval_group`] — which, unlike `compute_aggregate`'s own
/// row-resolution step (`eval_in`), can itself evaluate a nested aggregate
/// argument such as `SUM(SUM(salary)) OVER (...)` (confirmed via `gorun`) —
/// rather than needing to flatten and re-derive values from raw rows.
pub(crate) fn fold_aggregate_values(
    kind: AggregateKind,
    distinct: bool,
    vals: &[Datum],
    div_precision_increment: u32,
) -> Result<Datum, ExecError> {
    runtime::fold_values(kind, distinct, vals, div_precision_increment)
}

/// Reports whether an expression contains an aggregate anywhere in its tree
/// (outside of a nested subquery's own scope — see below), so a select list
/// like `COUNT(*) + 1`, or an aggregate hidden inside an ordinary function
/// call's argument like `IF(1=1, COUNT(*), 0)`, is recognized as aggregated.
/// Also called from `crate::select::select_rows`, to decide whether the
/// aggregation path is needed at all — confirmed via `gorun`, not assumed,
/// that missing a nested case here is a REAL, observable bug, not a
/// theoretical one: before this function recursed into `Expr::Func`'s own
/// arguments, `SELECT IF(1=1, COUNT(*), 0) FROM t` took the WRONG (row-wise)
/// path and failed with `Eval(Unsupported(...))` instead of the correct
/// single-group result real TiDB returns.
///
/// Does NOT recurse into a nested subquery's own body (`Expr::Subquery`/
/// `Exists`/the `subquery` field of `InSubquery`/`CompareSubquery`) — an
/// aggregate inside a subquery's OWN select list belongs to THAT subquery's
/// own scope, not this one (confirmed via `gorun`: `SELECT id, (SELECT
/// COUNT(*) FROM t2) FROM t` stays a per-row query, not grouped). The
/// OUTER-scope operand of `InSubquery`/`CompareSubquery` (`expr`/`left`) is
/// still recursed into, since it's evaluated in THIS query's scope.
pub(crate) fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } | Expr::GroupConcat { .. } => true,
        Expr::Unary(_, inner) | Expr::Paren(inner) => expr_has_aggregate(inner),
        Expr::Assign { value, .. } => expr_has_aggregate(value),
        Expr::Trim { expr, remstr, .. } => {
            expr_has_aggregate(expr) || remstr.as_deref().is_some_and(expr_has_aggregate)
        }
        Expr::Position { substr, str } => expr_has_aggregate(substr) || expr_has_aggregate(str),
        Expr::WeightString { expr, .. } => expr_has_aggregate(expr),
        Expr::Binary(_, left, right) => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::Func { args, .. } | Expr::GenericFuncCall { args, .. } => {
            args.iter().any(expr_has_aggregate)
        }
        Expr::Row(values) => values.iter().any(expr_has_aggregate),
        Expr::Interval { value, .. } => expr_has_aggregate(value),
        Expr::Extract { value, .. } => expr_has_aggregate(value),
        Expr::TimestampAdd { interval, expr, .. } => {
            expr_has_aggregate(interval) || expr_has_aggregate(expr)
        }
        Expr::TimestampDiff { expr1, expr2, .. } => {
            expr_has_aggregate(expr1) || expr_has_aggregate(expr2)
        }
        Expr::GetFormat { expr, .. } => expr_has_aggregate(expr),
        Expr::In { expr, list, .. } => {
            expr_has_aggregate(expr) || list.iter().any(expr_has_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_has_aggregate(expr) || expr_has_aggregate(low) || expr_has_aggregate(high),
        Expr::Like { expr, pattern, .. } => expr_has_aggregate(expr) || expr_has_aggregate(pattern),
        Expr::Regexp { expr, pattern, .. } => {
            expr_has_aggregate(expr) || expr_has_aggregate(pattern)
        }
        Expr::Collate { expr, .. } => expr_has_aggregate(expr),
        Expr::Cast(cast) => expr_has_aggregate(&cast.expr),
        Expr::ConvertUsing { expr, .. } => expr_has_aggregate(expr),
        Expr::MatchAgainst { against, .. } => expr_has_aggregate(against),
        Expr::MemberOf { expr, array } => expr_has_aggregate(expr) || expr_has_aggregate(array),
        Expr::Is { expr, .. } => expr_has_aggregate(expr),
        Expr::InSubquery { expr, .. } => expr_has_aggregate(expr),
        Expr::CompareSubquery { left, .. } => expr_has_aggregate(left),
        // `CASE WHEN COUNT(*) > 5 THEN ... END` needs the SAME recursion —
        // an aggregate can hide in the compare value, any WHEN condition
        // or THEN result, or the ELSE clause.
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().is_some_and(expr_has_aggregate)
                || when_clauses
                    .iter()
                    .any(|(cond, result)| expr_has_aggregate(cond) || expr_has_aggregate(result))
                || else_clause.as_deref().is_some_and(expr_has_aggregate)
        }
        _ => false,
    }
}

/// Enforces `ONLY_FULL_GROUP_BY` (real MySQL/TiDB's default `sql_mode`,
/// confirmed via `gorun`, not assumed): every column reference in the
/// select list, `HAVING`, or `ORDER BY` of a `GROUP BY`/aggregate query
/// that is not nested inside an aggregate call must be "safe" —
/// [`check_columns_pinned`] does the actual per-expression walk; this
/// function only assembles `pinned` (the bare `GROUP BY` columns) and
/// drives it across the three checked positions. `group_by` is the
/// ALREADY-alias-resolved `GROUP BY` list (see
/// `crate::order::resolve_alias`, `Database::aggregate`'s own caller); `
/// order_keys` is the ALREADY-position-resolved `ORDER BY` list (`ORDER
/// BY 2` etc. resolved to the underlying select-list expression) the
/// caller already computed, reused here rather than re-resolving;
/// `having` is likewise the ALREADY-alias-resolved predicate (see
/// `crate::order::resolve_having_aliases`), so a select-list alias
/// referenced in any of these three positions is checked as its own
/// resolved (real-column) expression, not as an ungrouped bare column.
pub(crate) fn check_group_by_scope(
    sel: &SelectStmt,
    group_by: &[&Expr],
    order_keys: &[(&Expr, bool)],
    having: Option<&Expr>,
) -> Result<(), ExecError> {
    let pinned: Vec<String> = group_by
        .iter()
        .filter_map(|e| match e {
            Expr::Column(path) => path.last().cloned(),
            _ => None,
        })
        .collect();
    for field in &sel.fields {
        if let SelectField::Expr { expr, .. } = field {
            check_group_safe(expr, group_by, &pinned)?;
        }
    }
    if let Some(having) = having {
        check_group_safe(having, group_by, &pinned)?;
    }
    for (expr, _) in order_keys {
        check_group_safe(expr, group_by, &pinned)?;
    }
    Ok(())
}

/// A TOP-LEVEL checked expression (one select-list field, the whole
/// `HAVING` predicate, or one `ORDER BY` key) is safe if it exactly
/// matches one of the `GROUP BY` expressions AS A WHOLE — confirmed via
/// `gorun` that this match is purely STRUCTURAL and NOT applied
/// recursively at every nesting level: `SELECT id+1+1 ... GROUP BY id+1`
/// still errors even though `id+1` appears nested inside the checked
/// expression, so only ONE top-level check happens here, before falling
/// through to [`check_columns_pinned`]'s leaf-only recursion.
fn check_group_safe(expr: &Expr, group_by: &[&Expr], pinned: &[String]) -> Result<(), ExecError> {
    if group_by.contains(&expr) {
        return Ok(());
    }
    check_columns_pinned(expr, pinned)
}

/// Recursively checks that every bare column leaf in `expr` (skipping
/// aggregate arguments and nested-subquery bodies, which are their own
/// separate scopes) is one of `pinned`'s bare `GROUP BY` columns.
/// Confirmed via `gorun` that this is a NAME-level check, not a full
/// functional-dependency proof: `SELECT id ... GROUP BY id+1` still
/// errors even though `id` is mathematically recoverable from `id+1` —
/// MySQL's own real check is this same simpler syntactic rule, not true
/// functional-dependency reasoning.
fn check_columns_pinned(expr: &Expr, pinned: &[String]) -> Result<(), ExecError> {
    match expr {
        Expr::Aggregate { .. } | Expr::GroupConcat { .. } => Ok(()),
        Expr::Column(path) => {
            let name = path.last().expect("column path is non-empty");
            if pinned.iter().any(|p| p.eq_ignore_ascii_case(name)) {
                Ok(())
            } else {
                Err(ExecError::UngroupedColumn(name.clone()))
            }
        }
        Expr::Unary(_, inner) | Expr::Paren(inner) => check_columns_pinned(inner, pinned),
        Expr::Assign { value, .. } => check_columns_pinned(value, pinned),
        Expr::Trim { expr, remstr, .. } => {
            check_columns_pinned(expr, pinned)?;
            match remstr {
                Some(r) => check_columns_pinned(r, pinned),
                None => Ok(()),
            }
        }
        Expr::Position { substr, str } => {
            check_columns_pinned(substr, pinned)?;
            check_columns_pinned(str, pinned)
        }
        Expr::WeightString { expr, .. } => check_columns_pinned(expr, pinned),
        Expr::Binary(_, left, right) => {
            check_columns_pinned(left, pinned)?;
            check_columns_pinned(right, pinned)
        }
        Expr::Func { args, .. } | Expr::GenericFuncCall { args, .. } => args
            .iter()
            .try_for_each(|a| check_columns_pinned(a, pinned)),
        Expr::Row(values) => values
            .iter()
            .try_for_each(|v| check_columns_pinned(v, pinned)),
        Expr::Interval { value, .. } => check_columns_pinned(value, pinned),
        Expr::Extract { value, .. } => check_columns_pinned(value, pinned),
        Expr::TimestampAdd { interval, expr, .. } => {
            check_columns_pinned(interval, pinned)?;
            check_columns_pinned(expr, pinned)
        }
        Expr::TimestampDiff { expr1, expr2, .. } => {
            check_columns_pinned(expr1, pinned)?;
            check_columns_pinned(expr2, pinned)
        }
        Expr::GetFormat { expr, .. } => check_columns_pinned(expr, pinned),
        Expr::In { expr, list, .. } => {
            check_columns_pinned(expr, pinned)?;
            list.iter()
                .try_for_each(|e| check_columns_pinned(e, pinned))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            check_columns_pinned(expr, pinned)?;
            check_columns_pinned(low, pinned)?;
            check_columns_pinned(high, pinned)
        }
        Expr::Like { expr, pattern, .. } => {
            check_columns_pinned(expr, pinned)?;
            check_columns_pinned(pattern, pinned)
        }
        Expr::Regexp { expr, pattern, .. } => {
            check_columns_pinned(expr, pinned)?;
            check_columns_pinned(pattern, pinned)
        }
        Expr::Collate { expr, .. } => check_columns_pinned(expr, pinned),
        Expr::Cast(cast) => check_columns_pinned(&cast.expr, pinned),
        Expr::ConvertUsing { expr, .. } => check_columns_pinned(expr, pinned),
        Expr::MatchAgainst { against, .. } => check_columns_pinned(against, pinned),
        Expr::MemberOf { expr, array } => {
            check_columns_pinned(expr, pinned)?;
            check_columns_pinned(array, pinned)
        }
        Expr::Is { expr, .. } => check_columns_pinned(expr, pinned),
        // The subquery's own body is a separate scope, not walked into
        // (confirmed via `gorun`: a scalar subquery referencing an
        // ungrouped outer column is permitted) — but the OUTER-scope
        // operand (`expr`/`left`) IS still checked, since it's evaluated
        // in THIS query's scope, not the subquery's (also confirmed via
        // `gorun`: `HAVING v IN (SELECT 1)` still errors on `v`).
        Expr::InSubquery { expr, .. } => check_columns_pinned(expr, pinned),
        Expr::CompareSubquery { left, .. } => check_columns_pinned(left, pinned),
        Expr::Subquery(_) | Expr::Exists { .. } => Ok(()),
        // A window's own args/PARTITION BY/ORDER BY are evaluated in THIS
        // query's scope (confirmed via `gorun`: `ROW_NUMBER() OVER (ORDER
        // BY salary)` under `GROUP BY dept` errors on the ungrouped
        // `salary` exactly like a bare SELECT-list column would, while
        // `ORDER BY SUM(salary)` is fine) — so every sub-expression is
        // checked the same as any other operand, not skipped like a
        // subquery body.
        Expr::Window { args, over, .. } => {
            let WindowOver::Def(WindowDef { spec, .. }) = over else {
                unreachable!("resolve_named_windows already normalized every OVER clause")
            };
            args.iter()
                .try_for_each(|a| check_columns_pinned(a, pinned))?;
            spec.partition_by
                .iter()
                .try_for_each(|e| check_columns_pinned(e, pinned))?;
            spec.order_by
                .iter()
                .try_for_each(|o| check_columns_pinned(&o.expr, pinned))
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            if let Some(v) = value {
                check_columns_pinned(v, pinned)?;
            }
            for (cond, result) in when_clauses {
                check_columns_pinned(cond, pinned)?;
                check_columns_pinned(result, pinned)?;
            }
            if let Some(e) = else_clause {
                check_columns_pinned(e, pinned)?;
            }
            Ok(())
        }
        // Every other variant is a literal or variable reference, with no
        // column to check.
        _ => Ok(()),
    }
}

/// Compares two non-NULL values from one resolved MAX/MIN evaluator domain.
///
/// Go selects one typed evaluator from the aggregate descriptor before values
/// reach the partial state. This seed executor does not carry that descriptor
/// yet, so rejecting mixed domains is safer than silently treating them as
/// equal. String payloads likewise must agree on their typed collation.
pub(super) fn value_cmp(a: &Datum, b: &Datum) -> Result<Ordering, ExecError> {
    match (a, b) {
        (Datum::Int(x), Datum::Int(y)) => Ok(x.cmp(y)),
        (Datum::UInt(x), Datum::UInt(y)) => Ok(x.cmp(y)),
        (Datum::String(x), Datum::String(y)) if x.collation() == y.collation() => {
            Ok(x.collation().compare(x.bytes(), y.bytes()))
        }
        (Datum::String(_), Datum::String(_)) => {
            Err(ExecError::Unsupported("MAX/MIN string collation mismatch"))
        }
        (Datum::Bytes(x), Datum::Bytes(y)) => Ok(x.cmp(y)),
        (Datum::Decimal(x), Datum::Decimal(y)) => Ok(x.cmp(y)),
        (Datum::Real(x), Datum::Real(y)) => x
            .partial_cmp(y)
            .ok_or(ExecError::Unsupported("MAX/MIN unordered real")),
        (Datum::Null, _) | (_, Datum::Null) => {
            Err(ExecError::Unsupported("MAX/MIN NULL comparison"))
        }
        (Datum::MinNotNull | Datum::MaxValue, _) | (_, Datum::MinNotNull | Datum::MaxValue) => {
            Err(ExecError::Unsupported("MAX/MIN range sentinel input"))
        }
        _ => Err(ExecError::Unsupported("MAX/MIN mixed value domains")),
    }
}
