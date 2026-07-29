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

//! Column pruning for a single-table scan: Go's `rule_column_pruning.go`,
//! restricted to the one plan shape where it can be proved correct here.
//!
//! # Why a narrow slice, and what "narrow" means
//!
//! Go prunes on the logical plan, where every column reference is already a
//! `*expression.Column` carrying a stable unique id, so renumbering a schema
//! cannot lose a reference. This tier resolves column references *by
//! position* into the `FROM` scope, and roughly seven thousand lines of
//! driver index rows by that position. A renumbering pass over an
//! already-built plan would therefore have to rewrite every one of those
//! sites, and a missed site is a silently wrong answer rather than an error.
//!
//! This module avoids the renumbering entirely. It runs **before** any
//! expression is built: it decides the kept column set from the statement
//! text, narrows the [`FromScope`] to exactly those columns, and lets the
//! ordinary resolver assign the compact offsets. Every expression the driver
//! then builds is built against the narrow scope from the start, so there is
//! no index to remap and no site that can be missed. The scan is narrowed by
//! the same set, in the same order, so scope offset *i* is still scan output
//! offset *i*.
//!
//! # The gate
//!
//! [`prunable_columns`] is the single answer to "is this statement in the
//! narrow slice?". It returns `None` -- take the unchanged full-width path --
//! for every shape below, and `None` is also what any expression form it does
//! not explicitly understand produces:
//!
//! * a `FROM` that is not exactly one base KV table (join, derived table,
//!   lateral, view, memory table, no `FROM` at all);
//! * `SELECT *` or `SELECT t.*` (a wildcard names every column);
//! * a `WITH` clause, `WITH ROLLUP`, a `WINDOW` clause, `VALUES`,
//!   `INTO OUTFILE`, or a locking clause;
//! * any subquery, `EXISTS`, `IN (subquery)`, or quantified comparison
//!   anywhere in the statement -- which is also how a correlated reference is
//!   refused, since a correlated reference can only occur inside one;
//! * any window function call;
//! * `DEFAULT(col)`, `MATCH ... AGAINST`, an unbound `?` marker, or an inline
//!   `@v := ...` assignment;
//! * a column reference the scope cannot resolve (unknown or ambiguous): the
//!   full-width path raises the proper MySQL error for it.
//!
//! [`collect_expr_columns`] matches [`tidb_ast::Expr`] **exhaustively, with no
//! wildcard arm**, so a new expression variant is a compile error here rather
//! than a silently unvisited subtree.
//!
//! # What the scan promises
//!
//! [`Executor::accept_column_prune`] is opt-in and defaults to refusing, the
//! same shape [`Executor::accept_scan_filter`] uses. A source returns `true`
//! only when it will emit exactly the requested columns, in the requested
//! order, for every row -- and [`TableScanExec`] additionally asks the table
//! to decode only those columns' ids, which is what makes pruning save work
//! rather than merely narrow the output.
//!
//! Pruning is offered *before* predicate push-down, so a pushed conjunct's
//! `column_offset` is computed against the already-narrowed scope; the kept
//! set contains the `WHERE`'s columns by construction.
//!
//! [`Executor::accept_column_prune`]: crate::executor::Executor::accept_column_prune
//! [`Executor::accept_scan_filter`]: crate::executor::Executor::accept_scan_filter
//! [`TableScanExec`]: crate::kv_table::TableScanExec

use std::collections::BTreeSet;

use tidb_ast::{Expr, SelectField, SelectStmt};

use tidb_expr::rewriter::ColumnResolver;

use crate::driver::{FromScope, FromTable};

/// The scan-output offsets a statement actually reads, when the statement is
/// in the narrow slice this module can prune. `None` means "not eligible":
/// the caller must keep the full-width path unchanged.
///
/// The returned offsets are sorted and unique, so they preserve the table's
/// column order -- the pruned row is the full row with holes removed, never
/// reordered.
pub(crate) fn prunable_columns(select: &SelectStmt, scope: &FromScope) -> Option<Vec<usize>> {
    // One base table, and nothing whose meaning depends on the columns the
    // scope does not name. Each of these is a *shape* refusal, listed here
    // rather than discovered halfway through the walk.
    if scope.tables.len() != 1 {
        return None;
    }
    if select.with.is_some()
        || select.rollup
        || !select.windows.is_empty()
        || !select.values.is_empty()
        || select.into_outfile.is_some()
        || select.lock.is_some()
    {
        return None;
    }
    // The FROM must be the single-table wrapper: `right` set is a join, and
    // a non-`Table` node is a derived/lateral source.
    let join = select.from.as_ref()?;
    if join.right.is_some() || join.on.is_some() || !join.using.is_empty() || join.natural {
        return None;
    }
    if !matches!(&join.left, tidb_ast::JoinNode::Table(_)) {
        return None;
    }

    let resolver = crate::driver::scope_resolver(scope);
    let mut wanted = BTreeSet::new();
    for field in select.fields.fields() {
        match field {
            // A wildcard names every column of the scope, so there is
            // nothing to prune and no expression to walk.
            SelectField::Wildcard(_) => return None,
            SelectField::Expr { expr, alias: _ } => {
                collect_expr_columns(expr, &resolver, &mut wanted)?;
            }
        }
    }
    for predicate in [select.where_clause.as_ref(), select.having.as_ref()]
        .into_iter()
        .flatten()
    {
        collect_expr_columns(predicate, &resolver, &mut wanted)?;
    }
    for item in &select.group_by {
        collect_expr_columns(&item.expr, &resolver, &mut wanted)?;
    }
    for item in &select.order_by {
        collect_expr_columns(&item.expr, &resolver, &mut wanted)?;
    }
    if let Some(limit) = &select.limit {
        collect_expr_columns(&limit.count, &resolver, &mut wanted)?;
        if let Some(offset) = &limit.offset {
            collect_expr_columns(offset, &resolver, &mut wanted)?;
        }
    }
    Some(wanted.into_iter().collect())
}

/// `scope` narrowed to `keep`, whose offsets index the single table's column
/// list. Column order is preserved, so the pruned scope's offset *i* names
/// the same column as pruned scan output offset *i*.
pub(crate) fn pruned_scope(scope: &FromScope, keep: &[usize]) -> FromScope {
    let table = &scope.tables[0];
    FromScope {
        tables: vec![FromTable {
            name: table.name.clone(),
            database: table.database.clone(),
            columns: keep
                .iter()
                .map(|offset| table.columns[*offset].clone())
                .collect(),
            offset: 0,
        }],
    }
}

/// Adds every scope column `expr` reads to `wanted`, or returns `None` when
/// the expression is outside the narrow slice.
///
/// The match is exhaustive on purpose: no `_` arm, so adding an
/// [`Expr`] variant fails to compile until it is classified as a leaf, a
/// recursion, or a refusal.
fn collect_expr_columns(
    expr: &Expr,
    resolver: &dyn ColumnResolver,
    wanted: &mut BTreeSet<usize>,
) -> Option<()> {
    match expr {
        Expr::Column(path) => {
            // An unresolvable or ambiguous name is left to the full-width
            // path, which raises the MySQL error for it.
            let (offset, _, _) = resolver.resolve(path)?;
            wanted.insert(offset);
        }

        // Leaves: no column can hide inside them.
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::CharsetString { .. }
        | Expr::Null
        | Expr::Bool(_)
        | Expr::UserVar(_)
        | Expr::SysVar { .. } => {}

        // Refused shapes. Each either names a column outside the walk's
        // reach, or puts a whole second query plan inside this one -- which
        // is where a correlated reference to this scope would live.
        Expr::ParamMarker { .. }
        | Expr::Default(_)
        | Expr::Assign { .. }
        | Expr::Window { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::MatchAgainst { .. } => return None,

        // Recursions.
        Expr::Paren(inner)
        | Expr::Unary(_, inner)
        | Expr::CharsetBinary { value: inner, .. }
        | Expr::Interval { value: inner, .. }
        | Expr::Extract { value: inner, .. }
        | Expr::WeightString { expr: inner, .. }
        | Expr::GetFormat { expr: inner, .. }
        | Expr::Is { expr: inner, .. }
        | Expr::ConvertUsing { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => {
            collect_expr_columns(inner, resolver, wanted)?;
        }
        Expr::Binary(_, left, right)
        | Expr::Position {
            substr: left,
            str: right,
        }
        | Expr::TimestampAdd {
            interval: left,
            expr: right,
            ..
        }
        | Expr::TimestampDiff {
            expr1: left,
            expr2: right,
            ..
        }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::Regexp {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::MemberOf {
            expr: left,
            array: right,
        } => {
            collect_expr_columns(left, resolver, wanted)?;
            collect_expr_columns(right, resolver, wanted)?;
        }
        Expr::Trim {
            expr,
            remstr,
            direction: _,
        } => {
            collect_expr_columns(expr, resolver, wanted)?;
            if let Some(remstr) = remstr {
                collect_expr_columns(remstr, resolver, wanted)?;
            }
        }
        Expr::Row(items)
        | Expr::Func { args: items, .. }
        | Expr::GenericFuncCall { args: items, .. }
        | Expr::Aggregate { args: items, .. } => {
            for item in items {
                collect_expr_columns(item, resolver, wanted)?;
            }
        }
        Expr::GroupConcat { args, order_by, .. } => {
            for arg in args {
                collect_expr_columns(arg, resolver, wanted)?;
            }
            for item in order_by {
                collect_expr_columns(&item.expr, resolver, wanted)?;
            }
        }
        Expr::In { expr, list, not: _ } => {
            collect_expr_columns(expr, resolver, wanted)?;
            for item in list {
                collect_expr_columns(item, resolver, wanted)?;
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            not: _,
        } => {
            collect_expr_columns(expr, resolver, wanted)?;
            collect_expr_columns(low, resolver, wanted)?;
            collect_expr_columns(high, resolver, wanted)?;
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            if let Some(value) = value {
                collect_expr_columns(value, resolver, wanted)?;
            }
            for (condition, result) in when_clauses {
                collect_expr_columns(condition, resolver, wanted)?;
                collect_expr_columns(result, resolver, wanted)?;
            }
            if let Some(else_clause) = else_clause {
                collect_expr_columns(else_clause, resolver, wanted)?;
            }
        }
        Expr::Cast(cast) => collect_expr_columns(&cast.expr, resolver, wanted)?,
    }
    Some(())
}
