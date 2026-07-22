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

//! Window function evaluation: the ranking functions `ROW_NUMBER`/`RANK`/
//! `DENSE_RANK`; the frame-based window AGGREGATES `COUNT`/`SUM`/`AVG`/
//! `MAX`/`MIN`; the "value function" family `FIRST_VALUE`/`LAST_VALUE`/
//! `NTH_VALUE`/`LAG`/`LEAD`; and the "distribution function" family
//! `NTILE`/`PERCENT_RANK`/`CUME_DIST` — all `OVER (PARTITION BY ...
//! ORDER BY ...)`. Called from both `crate::select` (the row-wise path) and
//! `crate::aggregate` (`GROUP BY`/aggregate queries).
//!
//! Design: [`collect_windows_in`] finds every DISTINCT `Expr::Window` call
//! appearing in the select list / (already position-resolved) `ORDER BY`
//! keys, in first-encountered order — deduplicated by FULL structural
//! equality (name, arguments, AND spec all together; two window calls
//! sharing a name and spec but with DIFFERENT arguments, e.g. `SUM(a) OVER
//! (...)` and `SUM(b) OVER (...)`, are two separate entries, not merged).
//! `Database::compute_window` then computes each one's value for every
//! GROUP in the current relation ONCE — stable-partitioning, a stable
//! per-partition sort, then one of several per-name shapes (see
//! `Database::compute_window`'s own doc for exactly which groups/how each
//! one is computed) — producing a `Vec<Datum>` aligned with the relation's
//! group order at the time of computation (BEFORE any `ORDER BY`
//! reordering, since window computation is independent of the final
//! result order — confirmed via `gorun`: output preserves original scan/
//! group-discovery order unless the query has its own top-level
//! `ORDER BY`). A row-wise (non-`GROUP BY`) query is the degenerate case
//! of one row per group — `crate::select`'s row-wise caller wraps each
//! passing row as its own single-element group before calling
//! `Database::compute_window`, so the two callers share every line of
//! this file's own logic. [`resolve_windows`] then walks an expression and
//! replaces each `Expr::Window` node with its OWN group's precomputed
//! value, spliced back as a literal via [`crate::literal::value_to_literal`] — the
//! SAME "precompute once, then splice a literal back into the tree"
//! technique `crate::subquery::resolve_subqueries` already uses for
//! subqueries — so the caller can defer entirely to the ordinary
//! `eval_in`/`Database::eval_group` path for the rewritten, now
//! window-free expression, with no new evaluation logic needed at all.
//!
//! `GROUP BY` integration (confirmed via `gorun`, not assumed): a window
//! function combined with `GROUP BY` computes over the POST-aggregation
//! "virtual rows," one per group — `HAVING` filters groups BEFORE window
//! computation runs (an excluded group is invisible to the window, not
//! just to the final output; see `crate::aggregate::aggregate`'s own doc
//! for the exact sequencing). A window's own `PARTITION BY`/`ORDER BY`/
//! arguments may reference an aggregate expression directly (`RANK() OVER
//! (ORDER BY SUM(salary))`) — evaluated once per group via
//! `Database::eval_group`, the SAME method that folds aggregates for
//! `HAVING`/the select list — but may NOT reference a select-list alias
//! (`SUM(c) OVER (...)` where `c` aliases `COUNT(*)` is a genuine `ERR`,
//! the same as any other ungrouped bare column). A window AGGREGATE's own
//! argument, when itself an aggregate expression (`SUM(SUM(salary)) OVER
//! (...)`), is evaluated ONCE PER GROUP via `eval_group` and THEN folded
//! across the frame via `crate::aggregate::fold_aggregate_values` — NOT by
//! flattening the frame's raw rows and re-running
//! `Database::compute_aggregate`, which cannot evaluate a nested aggregate
//! argument at all (its row-resolution step uses `eval_in`, which has no
//! notion of `Expr::Aggregate`).
//!
//! An explicit `ROWS`/`RANGE` frame clause (`spec.frame`, see
//! `tidb_ast::WindowFrame`'s own doc for the two kinds) restricts the
//! window-AGGREGATE and `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` shapes to a
//! per-row range, computed by `for_each_default_frame`'s sibling
//! `for_each_explicit_frame` — which dispatches to `for_each_rows_frame`
//! (a PHYSICAL row-offset range, NOT peer-group-aware: confirmed via
//! `gorun`, two rows TIED on `ORDER BY` still get their own DISTINCT
//! `ROWS`-frame value) or `for_each_range_frame` (a VALUE-distance range
//! against the single `ORDER BY` key, via `range_bound_satisfied` — see
//! that function's own doc for the exact `ASC`/`DESC` sign rule; a
//! `RANGE` frame with no explicit clause at all is the implicit default
//! frame, itself byte-identical to `RANGE BETWEEN UNBOUNDED PRECEDING AND
//! CURRENT ROW`, confirmed via `gorun`). A frame clause parses on EVERY
//! window function but has NO effect on the ranking/`NTILE`/
//! `PERCENT_RANK`/`CUME_DIST`/`LAG`/`LEAD` shapes (confirmed via `gorun`)
//! — those match arms simply never reference `spec.frame` at all, needing
//! no extra validation to reject it.
//!
//! Scope: a window function's own `PARTITION BY`/`ORDER BY` expressions may
//! reference ordinary columns and aggregates only — a subquery there is not
//! resolved (a deliberate scope boundary, matching how `GROUP BY` itself
//! doesn't support subqueries either); every window call's own ARGUMENTS
//! (the aggregate's arg, `LAG`/`LEAD`'s value/offset/default, a frame
//! bound's own offset, ...) DO get subquery resolution, for free, since
//! `Database::eval_group`'s own fallback already handles that. A window
//! function inside `HAVING` itself is rejected (confirmed via `gorun`) —
//! naturally, since `HAVING` is evaluated before any window resolution
//! runs, so a raw `Expr::Window` node reaches `eval_in`, which has no
//! notion of it. `DISTINCT` in a window aggregate (`MAX(DISTINCT x) OVER
//! (...)`) and `IGNORE NULLS`/`FROM LAST` on `LAG`/`LEAD`/etc. (real ANSI
//! SQL grammar, but confirmed via `gorun` that real TiDB itself rejects
//! both unconditionally too) are rejected at PARSE time (see
//! `tidb-parser`), so neither ever reaches here.
//!
//! **Named windows** (`OVER w`, `OVER (w ...)`, and the `WINDOW name AS
//! (...), ...` clause that defines them) are resolved entirely BEFORE
//! any of the above runs: [`resolve_named_windows`] rewrites `sel`'s
//! `fields`/`order_by` (the SAME two places [`collect_windows_in`] is
//! ever applied — a window function structurally cannot appear in
//! `WHERE`/`GROUP BY`, and one inside `HAVING` is a genuine,
//! pre-existing `EvalError::Unsupported` `eval_in` catch-all this
//! rewrite does not need to touch), replacing every `Expr::Window`'s
//! [`tidb_ast::WindowOver`] with an already-merged, fully INLINE
//! `WindowDef` — the SAME "rewrite once, leave every downstream consumer
//! unaware of the surface syntax" precedent `crate::cte::desugar_ctes`
//! and `crate::order::resolve_alias`'s callers already use. This is why
//! neither `collect_windows_in`/`resolve_windows` above, NOR
//! `Database::compute_window`, NOR
//! `crate::aggregate::check_columns_pinned`'s own `Expr::Window` arm
//! need to know anything about named windows at all — they only ever
//! see the resolved, inline shape. [`resolve_window_over`] does the
//! actual merge: an extension (`base: Some(name)`) may NEVER add
//! `PARTITION BY`; may add `ORDER BY` only if the named base doesn't
//! already have one; may add a frame only if the named base doesn't
//! already have one either — all confirmed via `gorun` to be real
//! EXECUTION-time restrictions, not parse-time ones (the grammar accepts
//! any combination syntactically). A named window may itself extend an
//! EARLIER-OR-LATER one (`gorun` confirms `WINDOW` clause order doesn't
//! matter for resolution), chaining the same rules transitively — a
//! self-referencing or circular chain is a genuine error, guarded by
//! `resolve_named_window`'s own `chain` argument.

use std::cmp::Ordering;

use tidb_ast::{
    BinaryOp, CastExpr, CastStyle, CastType, Expr, FrameBound, FrameKind, SelectField, SelectStmt,
    UnaryOp, WindowDef, WindowFrame, WindowOver, WindowSpec,
};
use tidb_datatype::Datum;
use tidb_expr::{apply_binary, truthy_of, Columns};
use tidb_planner::aggregation_descriptor::AggregateKind;

use crate::aggregate::fold_aggregate_values;
use crate::catalog::Column;
use crate::lead_lag::{LeadLagDefault, LeadLagDirection, LeadLagSelection};
use crate::literal::value_to_literal;
use crate::order::cmp_keys;
use crate::{Database, ExecError, Row};

pub(crate) mod ranking_runtime;

use ranking_runtime::WindowPartitionRuntime;

pub(crate) fn validate_window_modifiers(
    distinct: bool,
    ignore_nulls: bool,
    from_last: bool,
) -> Result<(), ExecError> {
    if distinct {
        return Err(ExecError::Unsupported("DISTINCT window function"));
    }
    if ignore_nulls {
        return Err(ExecError::Unsupported("IGNORE NULLS window function"));
    }
    if from_last {
        return Err(ExecError::Unsupported("FROM LAST window function"));
    }
    Ok(())
}

/// Collects every DISTINCT `Expr::Window` node appearing in `expr`'s tree
/// into `out` (deduplicated by full structural equality — name, argument,
/// AND spec together — in first-encountered order). Does not recurse into
/// a nested subquery's own body (a window function there belongs to that
/// subquery's own separate scope) — mirrors
/// `crate::aggregate::check_columns_pinned`'s own traversal shape and
/// subquery boundary. Does NOT recurse into a window call's OWN argument
/// (a window function nested inside another window function's argument is
/// not valid SQL to begin with, and `Database::compute_aggregate` — which
/// evaluates that argument — has no notion of a nested window value
/// either, so it would surface as an honest evaluation error rather than
/// something this collector needs to pre-empt).
pub(crate) fn collect_windows_in(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Window { .. } => {
            if !out.contains(expr) {
                out.push(expr.clone());
            }
        }
        Expr::Unary(_, inner) | Expr::Paren(inner) => collect_windows_in(inner, out),
        Expr::Assign { value, .. } => collect_windows_in(value, out),
        Expr::Trim { expr, remstr, .. } => {
            collect_windows_in(expr, out);
            if let Some(r) = remstr {
                collect_windows_in(r, out);
            }
        }
        Expr::Position { substr, str } => {
            collect_windows_in(substr, out);
            collect_windows_in(str, out);
        }
        Expr::WeightString { expr, .. } => collect_windows_in(expr, out),
        Expr::Binary(_, left, right) => {
            collect_windows_in(left, out);
            collect_windows_in(right, out);
        }
        Expr::Func { args, .. } | Expr::GenericFuncCall { args, .. } => {
            args.iter().for_each(|a| collect_windows_in(a, out))
        }
        Expr::Row(values) => values.iter().for_each(|v| collect_windows_in(v, out)),
        Expr::Interval { value, .. } => collect_windows_in(value, out),
        Expr::Extract { value, .. } => collect_windows_in(value, out),
        Expr::TimestampAdd { interval, expr, .. } => {
            collect_windows_in(interval, out);
            collect_windows_in(expr, out);
        }
        Expr::TimestampDiff { expr1, expr2, .. } => {
            collect_windows_in(expr1, out);
            collect_windows_in(expr2, out);
        }
        Expr::GetFormat { expr, .. } => collect_windows_in(expr, out),
        Expr::In { expr, list, .. } => {
            collect_windows_in(expr, out);
            list.iter().for_each(|e| collect_windows_in(e, out));
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_windows_in(expr, out);
            collect_windows_in(low, out);
            collect_windows_in(high, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_windows_in(expr, out);
            collect_windows_in(pattern, out);
        }
        Expr::Regexp { expr, pattern, .. } => {
            collect_windows_in(expr, out);
            collect_windows_in(pattern, out);
        }
        Expr::Collate { expr, .. } => collect_windows_in(expr, out),
        Expr::Cast(cast) => collect_windows_in(&cast.expr, out),
        Expr::ConvertUsing { expr, .. } => collect_windows_in(expr, out),
        Expr::MatchAgainst { against, .. } => collect_windows_in(against, out),
        Expr::MemberOf { expr, array } => {
            collect_windows_in(expr, out);
            collect_windows_in(array, out);
        }
        Expr::Is { expr, .. } => collect_windows_in(expr, out),
        Expr::InSubquery { expr, .. } => collect_windows_in(expr, out),
        Expr::CompareSubquery { left, .. } => collect_windows_in(left, out),
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            if let Some(v) = value {
                collect_windows_in(v, out);
            }
            for (cond, result) in when_clauses {
                collect_windows_in(cond, out);
                collect_windows_in(result, out);
            }
            if let Some(e) = else_clause {
                collect_windows_in(e, out);
            }
        }
        _ => {}
    }
}

/// Replaces every `Expr::Window` node in `expr` with the value looked up
/// (by full structural equality against `windows`) for `row_idx` — spliced
/// back as a literal via [`crate::literal::value_to_literal`]. Mirrors
/// [`collect_windows_in`]'s own traversal shape. A no-op deep-clone when
/// `expr` contains no window function at all.
pub(crate) fn resolve_windows(expr: &Expr, windows: &[(Expr, Vec<Datum>)], row_idx: usize) -> Expr {
    match expr {
        Expr::Window { .. } => {
            let value = windows
                .iter()
                .find(|(w, _)| w == expr)
                .map(|(_, vals)| vals[row_idx].clone())
                // Unreachable in practice: every `Expr::Window` node
                // reachable from here was already found by
                // `collect_windows_in` over the SAME expression tree.
                .unwrap_or(Datum::Null);
            // An unadorned positive Expr::Int evaluates back to signed i64
            // while it fits, so preserve a precomputed unsigned window
            // result with the same typed CAST representation SQL uses.
            // NTILE is the current source of this domain.
            if matches!(&value, Datum::UInt(_)) {
                Expr::Cast(CastExpr {
                    expr: Box::new(value_to_literal(value)),
                    cast_type: CastType::Unsigned,
                    style: CastStyle::Cast,
                    array: false,
                })
            } else {
                value_to_literal(value)
            }
        }
        Expr::Unary(op, inner) => {
            Expr::Unary(*op, Box::new(resolve_windows(inner, windows, row_idx)))
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(resolve_windows(inner, windows, row_idx))),
        Expr::Assign { name, value } => Expr::Assign {
            name: name.clone(),
            value: Box::new(resolve_windows(value, windows, row_idx)),
        },
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => Expr::Trim {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            remstr: remstr
                .as_deref()
                .map(|r| Box::new(resolve_windows(r, windows, row_idx))),
            direction: *direction,
        },
        Expr::Position { substr, str } => Expr::Position {
            substr: Box::new(resolve_windows(substr, windows, row_idx)),
            str: Box::new(resolve_windows(str, windows, row_idx)),
        },
        Expr::WeightString { expr, as_type } => Expr::WeightString {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            as_type: *as_type,
        },
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(resolve_windows(left, windows, row_idx)),
            Box::new(resolve_windows(right, windows, row_idx)),
        ),
        Expr::Func { name, args } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_windows(a, windows, row_idx))
                .collect(),
        },
        Expr::GenericFuncCall { schema, name, args } => Expr::GenericFuncCall {
            schema: schema.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_windows(a, windows, row_idx))
                .collect(),
        },
        Expr::Row(values) => Expr::Row(
            values
                .iter()
                .map(|v| resolve_windows(v, windows, row_idx))
                .collect(),
        ),
        Expr::Interval { value, unit } => Expr::Interval {
            value: Box::new(resolve_windows(value, windows, row_idx)),
            unit: unit.clone(),
        },
        Expr::Extract { unit, value } => Expr::Extract {
            unit: unit.clone(),
            value: Box::new(resolve_windows(value, windows, row_idx)),
        },
        Expr::TimestampAdd {
            unit,
            interval,
            expr,
        } => Expr::TimestampAdd {
            unit: unit.clone(),
            interval: Box::new(resolve_windows(interval, windows, row_idx)),
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
        },
        Expr::TimestampDiff { unit, expr1, expr2 } => Expr::TimestampDiff {
            unit: unit.clone(),
            expr1: Box::new(resolve_windows(expr1, windows, row_idx)),
            expr2: Box::new(resolve_windows(expr2, windows, row_idx)),
        },
        Expr::GetFormat { selector, expr } => Expr::GetFormat {
            selector: *selector,
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            list: list
                .iter()
                .map(|e| resolve_windows(e, windows, row_idx))
                .collect(),
            not: *not,
        },
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => Expr::Between {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            low: Box::new(resolve_windows(low, windows, row_idx)),
            high: Box::new(resolve_windows(high, windows, row_idx)),
            not: *not,
        },
        Expr::Like {
            expr,
            pattern,
            not,
            escape,
        } => Expr::Like {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            pattern: Box::new(resolve_windows(pattern, windows, row_idx)),
            not: *not,
            escape: *escape,
        },
        Expr::Regexp { expr, pattern, not } => Expr::Regexp {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            pattern: Box::new(resolve_windows(pattern, windows, row_idx)),
            not: *not,
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            collation: collation.clone(),
        },
        Expr::Cast(cast) => Expr::Cast(CastExpr {
            expr: Box::new(resolve_windows(&cast.expr, windows, row_idx)),
            cast_type: cast.cast_type.clone(),
            style: cast.style,
            array: cast.array,
        }),
        Expr::ConvertUsing { expr, charset } => Expr::ConvertUsing {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            charset: charset.clone(),
        },
        Expr::MatchAgainst {
            columns,
            against,
            modifier,
        } => Expr::MatchAgainst {
            columns: columns.clone(),
            against: Box::new(resolve_windows(against, windows, row_idx)),
            modifier: *modifier,
        },
        Expr::MemberOf { expr, array } => Expr::MemberOf {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            array: Box::new(resolve_windows(array, windows, row_idx)),
        },
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
            target: *target,
            not: *not,
        },
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => Expr::InSubquery {
            expr: Box::new(resolve_windows(expr, windows, row_idx)),
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
            left: Box::new(resolve_windows(left, windows, row_idx)),
            all: *all,
            subquery: subquery.clone(),
        },
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => Expr::Case {
            value: value
                .as_ref()
                .map(|v| Box::new(resolve_windows(v, windows, row_idx))),
            when_clauses: when_clauses
                .iter()
                .map(|(cond, result)| {
                    (
                        resolve_windows(cond, windows, row_idx),
                        resolve_windows(result, windows, row_idx),
                    )
                })
                .collect(),
            else_clause: else_clause
                .as_ref()
                .map(|e| Box::new(resolve_windows(e, windows, row_idx))),
        },
        other => other.clone(),
    }
}

/// Resolves every window function's `OVER` clause against `sel.windows`
/// into an already-merged, fully INLINE form — see this module's own doc
/// for why this runs as one early rewrite pass rather than threading
/// `sel.windows` through every downstream consumer. A no-op deep-clone
/// when the query has neither a `WINDOW` clause nor any named/extended
/// `OVER` reference, so the ordinary (still overwhelmingly common) case
/// pays only a cheap clone, not a real cost.
pub(crate) fn resolve_named_windows(sel: &SelectStmt) -> Result<SelectStmt, ExecError> {
    let mut sel = sel.clone();
    let windows = std::mem::take(&mut sel.windows);
    for field in &mut sel.fields {
        if let SelectField::Expr { expr, .. } = field {
            *expr = rewrite_window_over(expr, &windows)?;
        }
    }
    for item in &mut sel.order_by {
        item.expr = rewrite_window_over(&item.expr, &windows)?;
    }
    Ok(sel)
}

/// Rewrites every `Expr::Window` node in `expr`'s tree, replacing its
/// `over` with the fully-merged, inline form [`resolve_window_over`]
/// produces — mirrors [`collect_windows_in`]'s own traversal shape
/// exactly (same scope boundaries: does not recurse into a nested
/// subquery's own body, nor into a window call's OWN argument).
fn rewrite_window_over(expr: &Expr, windows: &[(String, WindowDef)]) -> Result<Expr, ExecError> {
    Ok(match expr {
        Expr::Window {
            name,
            args,
            distinct,
            ignore_nulls,
            from_last,
            over,
        } => Expr::Window {
            name: name.clone(),
            args: args.clone(),
            distinct: *distinct,
            ignore_nulls: *ignore_nulls,
            from_last: *from_last,
            over: WindowOver::Def(WindowDef {
                base: None,
                spec: resolve_window_over(over, windows)?,
            }),
        },
        Expr::Unary(op, inner) => Expr::Unary(*op, Box::new(rewrite_window_over(inner, windows)?)),
        Expr::Paren(inner) => Expr::Paren(Box::new(rewrite_window_over(inner, windows)?)),
        Expr::Assign { name, value } => Expr::Assign {
            name: name.clone(),
            value: Box::new(rewrite_window_over(value, windows)?),
        },
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => Expr::Trim {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            remstr: remstr
                .as_deref()
                .map(|r| rewrite_window_over(r, windows))
                .transpose()?
                .map(Box::new),
            direction: *direction,
        },
        Expr::Position { substr, str } => Expr::Position {
            substr: Box::new(rewrite_window_over(substr, windows)?),
            str: Box::new(rewrite_window_over(str, windows)?),
        },
        Expr::WeightString { expr, as_type } => Expr::WeightString {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            as_type: *as_type,
        },
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(rewrite_window_over(left, windows)?),
            Box::new(rewrite_window_over(right, windows)?),
        ),
        Expr::Func { name, args } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_window_over(a, windows))
                .collect::<Result<_, _>>()?,
        },
        Expr::GenericFuncCall { schema, name, args } => Expr::GenericFuncCall {
            schema: schema.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_window_over(a, windows))
                .collect::<Result<_, _>>()?,
        },
        Expr::Row(values) => Expr::Row(
            values
                .iter()
                .map(|v| rewrite_window_over(v, windows))
                .collect::<Result<_, _>>()?,
        ),
        Expr::Interval { value, unit } => Expr::Interval {
            value: Box::new(rewrite_window_over(value, windows)?),
            unit: unit.clone(),
        },
        Expr::Extract { unit, value } => Expr::Extract {
            unit: unit.clone(),
            value: Box::new(rewrite_window_over(value, windows)?),
        },
        Expr::TimestampAdd {
            unit,
            interval,
            expr,
        } => Expr::TimestampAdd {
            unit: unit.clone(),
            interval: Box::new(rewrite_window_over(interval, windows)?),
            expr: Box::new(rewrite_window_over(expr, windows)?),
        },
        Expr::TimestampDiff { unit, expr1, expr2 } => Expr::TimestampDiff {
            unit: unit.clone(),
            expr1: Box::new(rewrite_window_over(expr1, windows)?),
            expr2: Box::new(rewrite_window_over(expr2, windows)?),
        },
        Expr::GetFormat { selector, expr } => Expr::GetFormat {
            selector: *selector,
            expr: Box::new(rewrite_window_over(expr, windows)?),
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            list: list
                .iter()
                .map(|e| rewrite_window_over(e, windows))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => Expr::Between {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            low: Box::new(rewrite_window_over(low, windows)?),
            high: Box::new(rewrite_window_over(high, windows)?),
            not: *not,
        },
        Expr::Like {
            expr,
            pattern,
            not,
            escape,
        } => Expr::Like {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            pattern: Box::new(rewrite_window_over(pattern, windows)?),
            not: *not,
            escape: *escape,
        },
        Expr::Regexp { expr, pattern, not } => Expr::Regexp {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            pattern: Box::new(rewrite_window_over(pattern, windows)?),
            not: *not,
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            collation: collation.clone(),
        },
        Expr::Cast(cast) => Expr::Cast(CastExpr {
            expr: Box::new(rewrite_window_over(&cast.expr, windows)?),
            cast_type: cast.cast_type.clone(),
            style: cast.style,
            array: cast.array,
        }),
        Expr::ConvertUsing { expr, charset } => Expr::ConvertUsing {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            charset: charset.clone(),
        },
        Expr::MatchAgainst {
            columns,
            against,
            modifier,
        } => Expr::MatchAgainst {
            columns: columns.clone(),
            against: Box::new(rewrite_window_over(against, windows)?),
            modifier: *modifier,
        },
        Expr::MemberOf { expr, array } => Expr::MemberOf {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            array: Box::new(rewrite_window_over(array, windows)?),
        },
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(rewrite_window_over(expr, windows)?),
            target: *target,
            not: *not,
        },
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => Expr::InSubquery {
            expr: Box::new(rewrite_window_over(expr, windows)?),
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
            left: Box::new(rewrite_window_over(left, windows)?),
            all: *all,
            subquery: subquery.clone(),
        },
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => Expr::Case {
            value: value
                .as_ref()
                .map(|v| rewrite_window_over(v, windows))
                .transpose()?
                .map(Box::new),
            when_clauses: when_clauses
                .iter()
                .map(|(cond, result)| {
                    Ok((
                        rewrite_window_over(cond, windows)?,
                        rewrite_window_over(result, windows)?,
                    ))
                })
                .collect::<Result<_, ExecError>>()?,
            else_clause: else_clause
                .as_ref()
                .map(|e| rewrite_window_over(e, windows))
                .transpose()?
                .map(Box::new),
        },
        other => other.clone(),
    })
}

/// Resolves ONE window function's `over` clause to its fully-merged
/// `WindowSpec` — see this module's own doc for the merge rules.
fn resolve_window_over(
    over: &WindowOver,
    windows: &[(String, WindowDef)],
) -> Result<WindowSpec, ExecError> {
    match over {
        WindowOver::Name(name) => resolve_named_window(name, windows, &mut Vec::new()),
        WindowOver::Def(WindowDef { base: None, spec }) => Ok(spec.clone()),
        WindowOver::Def(WindowDef {
            base: Some(name),
            spec,
        }) => {
            let base_spec = resolve_named_window(name, windows, &mut Vec::new())?;
            merge_window_extension(base_spec, spec)
        }
    }
}

/// Resolves ONE named window (looked up by `name` in `windows`,
/// case-insensitively) to its own fully-merged `WindowSpec`, recursively
/// resolving whatever it itself extends — `chain` guards against a
/// self-referencing or circular definition (confirmed via `gorun` to be
/// a genuine error, not silently accepted).
fn resolve_named_window(
    name: &str,
    windows: &[(String, WindowDef)],
    chain: &mut Vec<String>,
) -> Result<WindowSpec, ExecError> {
    if chain.iter().any(|n| n.eq_ignore_ascii_case(name)) {
        return Err(ExecError::Unsupported("circular WINDOW definition"));
    }
    let def = windows
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, d)| d)
        .ok_or(ExecError::Unsupported("undefined window name"))?;
    chain.push(name.to_string());
    let result = match &def.base {
        None => Ok(def.spec.clone()),
        Some(base_name) => {
            let base_spec = resolve_named_window(base_name, windows, chain)?;
            merge_window_extension(base_spec, &def.spec)
        }
    };
    chain.pop();
    result
}

/// Merges an extension's own `spec` onto the NAMED window it extends —
/// see this module's own doc for the exact restriction (never
/// `PARTITION BY`; `ORDER BY`/a frame only if `base` doesn't already
/// have one).
fn merge_window_extension(
    base: WindowSpec,
    extension: &WindowSpec,
) -> Result<WindowSpec, ExecError> {
    if !extension.partition_by.is_empty() {
        return Err(ExecError::Unsupported(
            "PARTITION BY not allowed when extending a named window",
        ));
    }
    let order_by = if extension.order_by.is_empty() {
        base.order_by
    } else if base.order_by.is_empty() {
        extension.order_by.clone()
    } else {
        return Err(ExecError::Unsupported(
            "ORDER BY not allowed when extending a window that already has one",
        ));
    };
    let frame = match (&base.frame, &extension.frame) {
        (None, f) => f.clone(),
        (Some(_), None) => base.frame.clone(),
        (Some(_), Some(_)) => {
            return Err(ExecError::Unsupported(
                "frame not allowed when extending a window that already has one",
            ))
        }
    };
    Ok(WindowSpec {
        partition_by: base.partition_by,
        order_by,
        frame,
    })
}

impl Database {
    /// Computes one window function's value for every group in `groups` (in
    /// `groups`'s own order), producing a `Vec<Datum>` aligned 1:1. A
    /// row-wise (non-`GROUP BY`) caller passes one row per group; a
    /// `GROUP BY` caller passes each aggregated group's own rows.
    /// Partitioning is a stable linear grouping (groups sharing a
    /// `PARTITION BY` key stay in relative scan order — confirmed via
    /// `gorun`, not assumed, including the no-`ORDER BY` case: every group
    /// in a partition then ties, so `RANK`/`DENSE_RANK` are `1` for all of
    /// them while `ROW_NUMBER` still assigns sequential scan-order
    /// positions). Each partition is then given a STABLE sort by its
    /// `ORDER BY` keys (ties preserve scan order, confirmed via `gorun`,
    /// reusing `crate::order::cmp_keys` — the exact same total order,
    /// including `NULL`-sorts-first-ascending, the top-level `ORDER BY`
    /// already uses).
    ///
    /// `args` and `name` select one of the following shapes: the ranking
    /// functions (`ROW_NUMBER`/`RANK`/`DENSE_RANK`/`PERCENT_RANK`/
    /// `CUME_DIST`, no arguments), all derived from one peer geometry;
    /// `NTILE` (one descriptor-time integer literal), driven by the same
    /// physical partition runtime; a
    /// window AGGREGATE (`COUNT`/`SUM`/`AVG`/`MAX`/`MIN` plus the
    /// variance and standard-deviation family, `args[0]`
    /// resolved once per group via `Database::eval_group` and folded via
    /// `crate::aggregate::fold_aggregate_values`) over the default FRAME;
    /// `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` (`args[0]`, an optional
    /// `args[1]` 1-based position for `NTH_VALUE`), which pick one GROUP
    /// from that SAME default frame and evaluate `args[0]` against it; and
    /// `LAG`/`LEAD` (`args[0]`, an optional `args[1]` offset — `1` if
    /// unwritten — and an optional `args[2]` out-of-range default), which
    /// use PHYSICAL (`ROWS`-style) adjacency within the sorted partition
    /// instead of the frame — confirmed via `gorun`, not assumed, that
    /// this is a genuinely different rule from the frame functions: two
    /// groups TIED on `ORDER BY` still get their own DISTINCT physical
    /// predecessor/successor value from `LAG`/`LEAD`, unlike `LAST_VALUE`,
    /// which gives both tied groups the SAME peer-group value.
    ///
    /// The default FRAME (shared by the aggregate and `FIRST_VALUE`/
    /// `LAST_VALUE`/`NTH_VALUE` shapes) — confirmed via `gorun`, not
    /// assumed: with no `ORDER BY`, the frame is the WHOLE partition, so
    /// every group in it sees the SAME value; with an `ORDER BY`, the
    /// default frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
    /// ROW`, where for `RANGE` (unlike `ROWS`) "CURRENT ROW" means every
    /// group sharing the SAME `ORDER BY` key as this one (a whole peer/tie
    /// group), not just the single physical group — confirmed with two
    /// TIED groups, both showing the identical cumulative/peer-group
    /// value.
    pub(crate) fn compute_window(
        &self,
        name: &str,
        args: &[Expr],
        spec: &WindowSpec,
        groups: &[Vec<&Row>],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Vec<Datum>, ExecError> {
        // This executor currently accepts one syntactic integer-literal
        // offset and stores it once for LAG/LEAD. Go's broader Constant
        // authority also covers prepared/deferred values; that descriptor
        // boundary remains outside this runtime.
        let lead_lag_offset = if matches!(name, "LAG" | "LEAD") {
            Some(Self::resolve_lead_lag_offset(args.get(1))?)
        } else {
            None
        };
        // Resolve NTILE's descriptor-time constant once, before any
        // partition is visited. The Go builder likewise stores `n` on the
        // aggregate function rather than evaluating the argument per row.
        let ntile_divisor = if name == "NTILE" {
            Some(Self::resolve_ntile_divisor(args.first())?)
        } else {
            None
        };

        let mut partitions: Vec<(Vec<Datum>, Vec<usize>)> = Vec::new();
        for (i, group) in groups.iter().enumerate() {
            let key = spec
                .partition_by
                .iter()
                .map(|e| self.eval_group(e, group, cols, outer))
                .collect::<Result<Vec<_>, _>>()?;
            match partitions.iter_mut().find(|(k, _)| *k == key) {
                Some((_, idxs)) => idxs.push(i),
                None => partitions.push((key, vec![i])),
            }
        }

        let mut out = vec![Datum::Null; groups.len()];
        for (_, idxs) in &partitions {
            let mut keyed: Vec<(Vec<Datum>, usize)> = Vec::with_capacity(idxs.len());
            for &i in idxs {
                let kv = spec
                    .order_by
                    .iter()
                    .map(|item| self.eval_group(&item.expr, &groups[i], cols, outer))
                    .collect::<Result<Vec<_>, _>>()?;
                keyed.push((kv, i));
            }
            let descs: Vec<bool> = spec.order_by.iter().map(|item| item.desc).collect();
            keyed.sort_by(|a, b| cmp_keys(&a.0, &b.0, &descs));

            // Ranking, distribution, bucket, and row-offset functions all
            // consume this one stable-sorted partition authority. Constructing
            // it at the partition boundary co-locates the row buffer, peer
            // geometry, physical cursor, and reset lifecycle.
            let mut partition_runtime = matches!(
                name,
                "ROW_NUMBER"
                    | "RANK"
                    | "DENSE_RANK"
                    | "PERCENT_RANK"
                    | "CUME_DIST"
                    | "NTILE"
                    | "LAG"
                    | "LEAD"
            )
            .then(|| {
                WindowPartitionRuntime::from_sorted_rows(
                    keyed.iter().map(|(_, row_i)| *row_i),
                    |prev, curr| {
                        cmp_keys(&keyed[prev].0, &keyed[curr].0, &descs) == Ordering::Equal
                    },
                )
            });

            match name {
                "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" => {
                    // Peer identity follows the exact comparator used by the
                    // stable ORDER BY sort, rather than Datum's tagged enum
                    // equality. Build it once and derive all five ranking
                    // outputs from the same peer starts/ends.
                    let runtime = partition_runtime
                        .as_mut()
                        .expect("ranking initializes the partition runtime");
                    runtime.reset_cursor();
                    while let Some((pos, row_i)) = runtime.next_physical() {
                        let peer = runtime.peer_position(pos);
                        out[row_i] = match name {
                            "ROW_NUMBER" => {
                                Datum::Int(WindowPartitionRuntime::row_number(pos) as i64)
                            }
                            "RANK" => Datum::Int(peer.rank() as i64),
                            "DENSE_RANK" => Datum::Int(peer.dense_rank() as i64),
                            "PERCENT_RANK" => Datum::Real(peer.percent_rank(runtime.len())),
                            "CUME_DIST" => Datum::Real(peer.cume_dist(runtime.len())),
                            _ => unreachable!(),
                        };
                    }
                }
                "NTILE" => {
                    // Physical-position-based bucket assignment (NOT
                    // peer-group aware, matching `ROW_NUMBER`'s own
                    // physical nature -- confirmed via `gorun`: two TIED
                    // rows can land in DIFFERENT buckets). MySQL's own
                    // algorithm: with `len` rows split into `n` buckets,
                    // `len = q*n + r`; the first `r` buckets get `q+1`
                    // rows each, the rest get `q` rows each.
                    let runtime = partition_runtime
                        .as_mut()
                        .expect("NTILE initializes the partition runtime");
                    let divisor = ntile_divisor.expect("NTILE resolves its divisor");
                    runtime.reset_cursor();
                    while let Some((pos, row_i)) = runtime.next_physical() {
                        out[row_i] = runtime
                            .ntile_bucket(pos, divisor)
                            // Go's `typeInfer4Ntile` marks the BIGINT result
                            // unsigned and `AppendFinalResult2Chunk` appends a
                            // uint64. Preserve that domain even though a
                            // materialized partition cannot practically emit
                            // a bucket larger than `i64::MAX` here.
                            .map_or(Datum::Null, Datum::UInt);
                    }
                }
                "COUNT" | "SUM" | "AVG" | "MAX" | "MIN" | "VAR_POP" | "VARIANCE" | "VAR_SAMP"
                | "STDDEV_POP" | "STDDEV" | "STD" | "STDDEV_SAMP" => {
                    let kind = AggregateKind::from_name(name)
                        .ok_or(ExecError::Unsupported("window aggregate function"))?;
                    // Each group in the frame contributes ONE resolved value
                    // (via `eval_group`, which folds an aggregate argument
                    // over that group's own rows), THEN those per-group
                    // values are folded across the frame — NOT by
                    // flattening the frame's raw rows and re-running
                    // `compute_aggregate`, which would be unable to
                    // evaluate a nested aggregate argument (`eval_in` has
                    // no notion of `Expr::Aggregate`) and, confirmed via
                    // `gorun`, is the wrong shape in general even where it
                    // happens to coincide numerically (e.g. `SUM(SUM(x))`).
                    // Row-wise callers pass one-row-per-group, where this
                    // is exactly the prior per-row behavior. An explicit
                    // `ROWS` frame (`spec.frame`) uses the SAME fold, just
                    // sourced from each row's own physical-offset frame
                    // instead of the implicit default one — an empty
                    // frame folds zero values (`NULL` for every shape but
                    // `COUNT`, which is legitimately `0`).
                    let arg = &args[0];
                    let results = match &spec.frame {
                        Some(frame) => self.for_each_explicit_frame(
                            &keyed,
                            frame,
                            &descs,
                            groups,
                            cols,
                            outer,
                            |frame_idxs| {
                                let vals: Vec<Datum> = frame_idxs
                                    .unwrap_or(&[])
                                    .iter()
                                    .map(|&i| self.eval_group(arg, &groups[i], cols, outer))
                                    .collect::<Result<_, ExecError>>()?;
                                fold_aggregate_values(
                                    kind,
                                    false,
                                    &vals,
                                    u32::from(self.div_precision_increment.value()),
                                )
                            },
                        )?,
                        None => self.for_each_default_frame(&keyed, spec, |frame_idxs| {
                            let vals: Vec<Datum> = frame_idxs
                                .iter()
                                .map(|&i| self.eval_group(arg, &groups[i], cols, outer))
                                .collect::<Result<_, ExecError>>()?;
                            fold_aggregate_values(
                                kind,
                                false,
                                &vals,
                                u32::from(self.div_precision_increment.value()),
                            )
                        })?,
                    };
                    for (i, v) in results {
                        out[i] = v;
                    }
                }
                "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                    // `NTH_VALUE`'s position argument is only evaluated
                    // when the frame is non-empty (against the frame's
                    // own first row, matching the default-frame case) —
                    // an empty explicit `ROWS` frame yields `NULL`
                    // directly (confirmed via `gorun`) without needing
                    // any row context to evaluate the position against.
                    let value_arg = &args[0];
                    let pick = |frame_idxs: &[usize]| -> Result<Datum, ExecError> {
                        let target = match name {
                            "FIRST_VALUE" => frame_idxs.first().copied(),
                            "LAST_VALUE" => frame_idxs.last().copied(),
                            "NTH_VALUE" => match frame_idxs.first() {
                                Some(&first) => {
                                    match self.eval_group(&args[1], &groups[first], cols, outer)? {
                                        Datum::Int(n) if n >= 1 => {
                                            frame_idxs.get((n - 1) as usize).copied()
                                        }
                                        _ => {
                                            return Err(ExecError::Unsupported(
                                                "NTH_VALUE position must be a positive integer",
                                            ))
                                        }
                                    }
                                }
                                None => None,
                            },
                            _ => unreachable!(),
                        };
                        match target {
                            Some(i) => self.eval_group(value_arg, &groups[i], cols, outer),
                            None => Ok(Datum::Null),
                        }
                    };
                    let results = match &spec.frame {
                        Some(frame) => self.for_each_explicit_frame(
                            &keyed,
                            frame,
                            &descs,
                            groups,
                            cols,
                            outer,
                            |frame_idxs| pick(frame_idxs.unwrap_or(&[])),
                        )?,
                        None => self.for_each_default_frame(&keyed, spec, pick)?,
                    };
                    for (i, v) in results {
                        out[i] = v;
                    }
                }
                "LAG" | "LEAD" => {
                    let value_arg = &args[0];
                    let runtime = partition_runtime
                        .as_mut()
                        .expect("LAG/LEAD initializes the partition runtime");
                    // Reset the cursor at the partition boundary before
                    // AppendFinalResult-style advancement over the already
                    // buffered stable-sorted physical group handles.
                    runtime.reset_cursor();
                    let direction = if name == "LAG" {
                        LeadLagDirection::Lag
                    } else {
                        LeadLagDirection::Lead
                    };
                    let default = if args.get(2).is_some() {
                        LeadLagDefault::CurrentRow
                    } else {
                        LeadLagDefault::Null
                    };
                    let offset = lead_lag_offset.expect("LAG/LEAD resolves its offset");
                    while let Some((row_i, selection)) =
                        runtime.next_lead_lag_selection(direction, offset, default)
                    {
                        let value = match selection {
                            LeadLagSelection::Source(target_i) => {
                                self.eval_group(value_arg, &groups[target_i], cols, outer)?
                            }
                            LeadLagSelection::Default(current_i) => {
                                let default_arg = args
                                    .get(2)
                                    .expect("default selection requires a third argument");
                                debug_assert_eq!(current_i, row_i);
                                self.eval_group(default_arg, &groups[current_i], cols, outer)?
                            }
                            LeadLagSelection::Null => Datum::Null,
                        };
                        out[row_i] = value;
                    }
                }
                _ => return Err(ExecError::Unsupported("window function")),
            }
        }
        Ok(out)
    }

    fn resolve_lead_lag_offset(offset_arg: Option<&Expr>) -> Result<u64, ExecError> {
        let Some(offset_arg) = offset_arg else {
            return Ok(1);
        };
        match offset_arg {
            Expr::Int(value) => value
                .parse::<u64>()
                .map_err(|_| ExecError::Unsupported("LAG/LEAD offset must be an integer")),
            Expr::Paren(inner) | Expr::Unary(UnaryOp::Plus, inner) => {
                Self::resolve_lead_lag_offset(Some(inner))
            }
            Expr::Unary(UnaryOp::Minus, inner) => {
                if Self::resolve_lead_lag_offset(Some(inner))? == 0 {
                    Ok(0)
                } else {
                    Err(ExecError::Unsupported(
                        "LAG/LEAD offset must not be negative",
                    ))
                }
            }
            // Do not approximate literal syntax with equal values observed at
            // runtime: a column is not a literal even when every current row
            // happens to carry the same value. Go's broader Constant,
            // prepared-parameter, and deferred-expression authority remains
            // outside this executor.
            _ => Err(ExecError::Unsupported("LAG/LEAD offset must be constant")),
        }
    }

    fn resolve_ntile_divisor(divisor_arg: Option<&Expr>) -> Result<u64, ExecError> {
        let Some(divisor_arg) = divisor_arg else {
            return Err(ExecError::Unsupported("NTILE requires one argument"));
        };
        match divisor_arg {
            // Go represents a NULL constant as n == 0 and the runtime emits
            // NULL for every row. Numeric zero is rejected by the descriptor.
            Expr::Null => Ok(0),
            Expr::Int(value) => {
                value
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value != 0)
                    .ok_or(ExecError::Unsupported(
                        "NTILE argument must be a positive integer literal",
                    ))
            }
            // TiDB's TRUE/FALSE literals are Constant values with an Int64
            // Datum, so GetUint64FromConstant accepts TRUE as 1 and rejects
            // FALSE as the non-NULL numeric zero.
            Expr::Bool(true) => Ok(1),
            Expr::Bool(false) => Err(ExecError::Unsupported(
                "NTILE argument must be a positive integer literal",
            )),
            Expr::Paren(inner) | Expr::Unary(UnaryOp::Plus, inner) => {
                Self::resolve_ntile_divisor(Some(inner))
            }
            Expr::Unary(UnaryOp::Minus, _) => Err(ExecError::Unsupported(
                "NTILE argument must be a positive integer literal",
            )),
            // The Go descriptor also accepts prepared/deferred Constant
            // forms. This AST has no bound-parameter constant node at this
            // stage, so reject rather than approximating constancy from row
            // values.
            _ => Err(ExecError::Unsupported(
                "NTILE argument must be a constant integer literal",
            )),
        }
    }

    /// Walks one partition's ALREADY sorted groups (`keyed`) and calls `f`
    /// once per DISTINCT default frame (see `Database::compute_window`'s
    /// own doc for the exact frame rule), passing the frame's group indices
    /// (into the outer `groups` slice, in sorted order) — `f`'s result is
    /// then assigned to every group sharing that frame. Shared by the
    /// frame-aggregate and `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` shapes,
    /// which differ only in what they compute FROM the frame, not in how
    /// the frame itself is determined.
    fn for_each_default_frame(
        &self,
        keyed: &[(Vec<Datum>, usize)],
        spec: &WindowSpec,
        mut f: impl FnMut(&[usize]) -> Result<Datum, ExecError>,
    ) -> Result<Vec<(usize, Datum)>, ExecError> {
        let mut out = Vec::with_capacity(keyed.len());
        if spec.order_by.is_empty() {
            let frame_idxs: Vec<usize> = keyed.iter().map(|(_, i)| *i).collect();
            let value = f(&frame_idxs)?;
            for &i in &frame_idxs {
                out.push((i, value.clone()));
            }
            return Ok(out);
        }
        let mut frame_idxs: Vec<usize> = Vec::new();
        let mut pos = 0;
        while pos < keyed.len() {
            let mut end = pos;
            while end + 1 < keyed.len() && keyed[end + 1].0 == keyed[pos].0 {
                end += 1;
            }
            for (_, i) in &keyed[pos..=end] {
                frame_idxs.push(*i);
            }
            let value = f(&frame_idxs)?;
            for (_, i) in &keyed[pos..=end] {
                out.push((*i, value.clone()));
            }
            pos = end + 1;
        }
        Ok(out)
    }

    /// Dispatches an explicit frame clause to its own `FrameKind`
    /// implementation, after the ONE check shared by both kinds: a frame
    /// whose `start` bound ranks AFTER its `end` bound
    /// (`UnboundedPreceding < Preceding < CurrentRow < Following <
    /// UnboundedFollowing`) is a genuine execution error regardless of
    /// any row's own position or the frame's own kind, confirmed via
    /// `gorun`: `ROWS`/`RANGE BETWEEN CURRENT ROW AND 1 PRECEDING` both
    /// error even though both bounds are individually valid.
    #[allow(clippy::too_many_arguments)]
    fn for_each_explicit_frame(
        &self,
        keyed: &[(Vec<Datum>, usize)],
        frame: &WindowFrame,
        descs: &[bool],
        groups: &[Vec<&Row>],
        cols: &[Column],
        outer: Option<&dyn Columns>,
        f: impl FnMut(Option<&[usize]>) -> Result<Datum, ExecError>,
    ) -> Result<Vec<(usize, Datum)>, ExecError> {
        if frame_bound_rank(&frame.start) > frame_bound_rank(&frame.end) {
            return Err(ExecError::Unsupported(
                "window frame start is after its own end",
            ));
        }
        match frame.kind {
            FrameKind::Rows => self.for_each_rows_frame(keyed, frame, groups, cols, outer, f),
            FrameKind::Range => {
                self.for_each_range_frame(keyed, frame, descs, groups, cols, outer, f)
            }
        }
    }

    /// Walks one partition's ALREADY sorted groups (`keyed`) and calls `f`
    /// once per ROW with that row's OWN explicit `ROWS`-frame group
    /// indices (into the outer `groups` slice, physical-position order,
    /// `None` if that row's own frame is empty) — unlike
    /// `for_each_default_frame`, no two rows can share one batched call in
    /// general, since an explicit `ROWS` frame is NOT peer-group-aware
    /// (confirmed via `gorun`: two rows TIED on `ORDER BY` get their own
    /// DISTINCT `ROWS`-frame value, unlike the implicit default frame).
    /// A frame is empty when its clamped `[start, end]` range is
    /// backwards or falls entirely outside `[0, len)` — confirmed via
    /// `gorun` this is silent (`f(None)`), not an error, REGARDLESS of
    /// why (an inverted same-kind bound pair like `2 FOLLOWING AND 1
    /// FOLLOWING`, or a bound pair that's simply out of the partition's
    /// range).
    fn for_each_rows_frame(
        &self,
        keyed: &[(Vec<Datum>, usize)],
        frame: &WindowFrame,
        groups: &[Vec<&Row>],
        cols: &[Column],
        outer: Option<&dyn Columns>,
        mut f: impl FnMut(Option<&[usize]>) -> Result<Datum, ExecError>,
    ) -> Result<Vec<(usize, Datum)>, ExecError> {
        let len = keyed.len() as i64;
        let idxs: Vec<usize> = keyed.iter().map(|(_, i)| *i).collect();
        let mut out = Vec::with_capacity(keyed.len());
        for pos in 0..keyed.len() {
            let group = &groups[keyed[pos].1];
            let start = self.resolve_rows_offset(&frame.start, pos, group, cols, outer)?;
            let end = self.resolve_rows_offset(&frame.end, pos, group, cols, outer)?;
            let clamped_start = start.max(0);
            let clamped_end = end.min(len - 1);
            let value = if clamped_start > clamped_end {
                f(None)?
            } else {
                f(Some(&idxs[clamped_start as usize..=clamped_end as usize]))?
            };
            out.push((keyed[pos].1, value));
        }
        Ok(out)
    }

    /// Resolves one `ROWS` frame bound to a physical target position
    /// (0-based, relative to the partition — may be negative or `>= len`,
    /// clamped by the caller). `UNBOUNDED PRECEDING`/`FOLLOWING` use a
    /// sufficiently-extreme sentinel rather than the partition's own
    /// length, since `for_each_rows_frame` clamps against the ACTUAL
    /// length anyway and this keeps the arithmetic overflow-free
    /// regardless of `pos`.
    fn resolve_rows_offset(
        &self,
        bound: &FrameBound,
        pos: usize,
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<i64, ExecError> {
        Ok(match bound {
            FrameBound::UnboundedPreceding => i64::MIN / 2,
            FrameBound::CurrentRow => pos as i64,
            FrameBound::UnboundedFollowing => i64::MAX / 2,
            FrameBound::Preceding(n) => {
                pos as i64 - self.rows_bound_offset(n, group, cols, outer)?
            }
            FrameBound::Following(n) => {
                pos as i64 + self.rows_bound_offset(n, group, cols, outer)?
            }
        })
    }

    /// Evaluates a `ROWS` bound's own `PRECEDING`/`FOLLOWING` offset
    /// expression against `group`'s own row context — a non-negative
    /// integer, the same requirement `NTILE`'s bucket count and
    /// `LAG`/`LEAD`'s offset argument already enforce.
    fn rows_bound_offset(
        &self,
        n: &Expr,
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<i64, ExecError> {
        match self.eval_group(n, group, cols, outer)? {
            Datum::Int(n) if n >= 0 => Ok(n),
            _ => Err(ExecError::Unsupported(
                "window frame offset must be a non-negative integer",
            )),
        }
    }

    /// Walks one partition's ALREADY sorted groups (`keyed`) and calls `f`
    /// once per ROW with that row's OWN explicit `RANGE`-frame group
    /// indices — VALUE-based rather than position-based (see
    /// [`tidb_ast::FrameKind::Range`]'s own doc): a numeric
    /// `PRECEDING`/`FOLLOWING` offset bound requires EXACTLY one `ORDER
    /// BY` column (confirmed via `gorun`: real TiDB rejects it outright
    /// with zero or multiple columns), checked ONCE up front — an
    /// `UnboundedPreceding`/`CurrentRow`/`UnboundedFollowing`-only frame
    /// needs no such column, matching `ROWS`'s own unrestricted arity.
    /// Every row is tested against every OTHER row via
    /// [`Self::range_bound_satisfied`] — quadratic in partition size, but
    /// this is a differential-testing reference engine, not a
    /// performance-tuned one, and every other window shape here is
    /// equally unoptimized.
    #[allow(clippy::too_many_arguments)]
    fn for_each_range_frame(
        &self,
        keyed: &[(Vec<Datum>, usize)],
        frame: &WindowFrame,
        descs: &[bool],
        groups: &[Vec<&Row>],
        cols: &[Column],
        outer: Option<&dyn Columns>,
        mut f: impl FnMut(Option<&[usize]>) -> Result<Datum, ExecError>,
    ) -> Result<Vec<(usize, Datum)>, ExecError> {
        let needs_offset = matches!(
            frame.start,
            FrameBound::Preceding(_) | FrameBound::Following(_)
        ) || matches!(
            frame.end,
            FrameBound::Preceding(_) | FrameBound::Following(_)
        );
        if needs_offset && descs.len() != 1 {
            return Err(ExecError::Unsupported(
                "RANGE frame with a PRECEDING/FOLLOWING offset requires exactly one ORDER BY column",
            ));
        }
        let idxs: Vec<usize> = keyed.iter().map(|(_, i)| *i).collect();
        let mut out = Vec::with_capacity(keyed.len());
        for pos in 0..keyed.len() {
            let current_key = &keyed[pos].0;
            let group = &groups[keyed[pos].1];
            let mut frame_idxs = Vec::new();
            for (other, (other_key, _)) in keyed.iter().enumerate() {
                let in_start = self.range_bound_satisfied(
                    &frame.start,
                    true,
                    descs,
                    current_key,
                    other_key,
                    group,
                    cols,
                    outer,
                )?;
                let in_end = self.range_bound_satisfied(
                    &frame.end,
                    false,
                    descs,
                    current_key,
                    other_key,
                    group,
                    cols,
                    outer,
                )?;
                if in_start && in_end {
                    frame_idxs.push(idxs[other]);
                }
            }
            let value = if frame_idxs.is_empty() {
                f(None)?
            } else {
                f(Some(&frame_idxs))?
            };
            out.push((keyed[pos].1, value));
        }
        Ok(out)
    }

    /// Tests whether `other_key` (a candidate row) lies on the included
    /// side of `bound` relative to `current_key` (the row the frame is
    /// being computed for). `is_start` says whether `bound` is
    /// `frame.start` (the boundary nearer `UnboundedPreceding`) or
    /// `frame.end` (nearer `UnboundedFollowing`).
    ///
    /// `CurrentRow` compares the FULL `ORDER BY` key (via [`cmp_keys`],
    /// which already honors each column's own `DESC` — works for zero,
    /// one, or many columns alike) rather than a single scalar value, so
    /// it naturally includes every row TIED with `current_key` with no
    /// extra case — this is exactly why `RANGE BETWEEN UNBOUNDED
    /// PRECEDING AND CURRENT ROW` is byte-identical to the implicit
    /// default frame (confirmed via `gorun`), and why a zero-column
    /// `ORDER BY` (every row a "tie") folds the whole partition into one
    /// frame with no special-cased base case.
    ///
    /// A `Preceding`/`Following` bound instead computes a VALUE threshold
    /// against the single `ORDER BY` column (the caller already checked
    /// there is exactly one): `current_value - offset` for `Preceding`
    /// under `ASC`, `current_value + offset` under `DESC` — and the
    /// mirror image for `Following` — since `PRECEDING`/`FOLLOWING` are
    /// defined relative to SCAN order, not raw value order, so `DESC`
    /// flips which side of `current_value` each bound lands on (confirmed
    /// via `gorun` using an asymmetric-offset probe specifically chosen
    /// so `ASC` vs `DESC` results differ observably). The comparison
    /// operator against that threshold flips the same way: `is_start`
    /// means `>=` under `ASC`/`<=` under `DESC`; `frame.end` is the
    /// mirror.
    #[allow(clippy::too_many_arguments)]
    fn range_bound_satisfied(
        &self,
        bound: &FrameBound,
        is_start: bool,
        descs: &[bool],
        current_key: &[Datum],
        other_key: &[Datum],
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<bool, ExecError> {
        match bound {
            FrameBound::UnboundedPreceding | FrameBound::UnboundedFollowing => Ok(true),
            FrameBound::CurrentRow => {
                let ord = cmp_keys(other_key, current_key, descs);
                Ok(if is_start {
                    ord != Ordering::Less
                } else {
                    ord != Ordering::Greater
                })
            }
            FrameBound::Preceding(n) | FrameBound::Following(n) => {
                let desc = descs[0];
                let add = matches!(bound, FrameBound::Following(_)) != desc;
                let offset = self.range_bound_offset(n, group, cols, outer)?;
                let current_value = current_key[0].clone();
                let threshold = if add {
                    apply_binary(BinaryOp::Plus, current_value, offset)
                } else {
                    apply_binary(BinaryOp::Minus, current_value, offset)
                }?;
                let ge = is_start != desc;
                let cmp_op = if ge { BinaryOp::Ge } else { BinaryOp::Le };
                let result = apply_binary(cmp_op, other_key[0].clone(), threshold)?;
                Ok(truthy_of(&result)?.unwrap_or(false))
            }
        }
    }

    /// Evaluates a `RANGE` bound's own `PRECEDING`/`FOLLOWING` offset
    /// expression against `group`'s own row context — any numeric value
    /// (`Int`/`Decimal`/`Float`, matching whichever type flows through
    /// `apply_binary`'s own promotion rules against the `ORDER BY`
    /// column's own value, confirmed via `gorun` that `Decimal` works),
    /// but never negative (confirmed via `gorun`: `RANGE ... -5
    /// PRECEDING` errors, matching `ROWS`'s own non-negative requirement
    /// for `rows_bound_offset`).
    fn range_bound_offset(
        &self,
        n: &Expr,
        group: &[&Row],
        cols: &[Column],
        outer: Option<&dyn Columns>,
    ) -> Result<Datum, ExecError> {
        let v = self.eval_group(n, group, cols, outer)?;
        let negative = apply_binary(BinaryOp::Lt, v.clone(), Datum::Int(0))?;
        if truthy_of(&negative)?.unwrap_or(false) {
            return Err(ExecError::Unsupported(
                "window frame offset must not be negative",
            ));
        }
        Ok(v)
    }
}

/// Ranks a frame bound's KIND (not its `N` value) so a frame's `start` can
/// be checked against its `end`: `UnboundedPreceding < Preceding <
/// CurrentRow < Following < UnboundedFollowing`.
fn frame_bound_rank(bound: &FrameBound) -> u8 {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(_) => 1,
        FrameBound::CurrentRow => 2,
        FrameBound::Following(_) => 3,
        FrameBound::UnboundedFollowing => 4,
    }
}
