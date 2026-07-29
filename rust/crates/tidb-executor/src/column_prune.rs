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

//! Column pruning for a single-table scan and for a two-base-table join:
//! Go's `rule_column_pruning.go`, restricted to the plan shapes where it can
//! be proved correct here.
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
//! * a `FROM` that is not exactly one base KV table (derived table, lateral,
//!   view, memory table, no `FROM` at all) -- with the single widening of
//!   [`prunable_join_columns`] below;
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
//! # The join widening
//!
//! [`prunable_join_columns`] adds exactly one shape: a `FROM` of **two base
//! tables**, joined by an ordinary `ON` (or by nothing, with the predicate in
//! the `WHERE`). Every statement-level refusal above still applies, and the
//! join's own `ON` is walked alongside the statement's clauses -- an `ON`
//! column dropped from a side would be a silently wrong join.
//!
//! The prune-before-expressions rule is what fixes *where* that gate runs. A
//! join's `ON` is built from the concatenated scope inside `build_join`, so
//! the gate runs at the one moment when both sides exist as executors and no
//! expression has been rewritten yet. Each side is then handed offsets in its
//! own local space and answers independently; the concatenated scope is
//! rebuilt afterwards from whatever widths the two sides settled on, and only
//! then is the `ON` rewritten. Nothing is renumbered, because at the moment
//! of narrowing no offset yet exists.
//!
//! Three or more tables stay refused, and the reason is structural rather
//! than cautious: `build_join` recurses, so an inner join's sides would be
//! offered a prune while the enclosing join's `ON` -- which may name their
//! columns -- is nowhere in view. `prune` is therefore passed only to a
//! query's outermost `FROM` node.
//!
//! # What the scan promises
//!
//! [`TableAccess::accept_column_prune`] is opt-in and defaults to refusing, the
//! same shape [`TableAccess::accept_scan_filter`] uses. A source returns `true`
//! only when it will emit exactly the requested columns, in the requested
//! order, for every row -- and [`TableScanExec`] additionally asks the table
//! to decode only those columns' ids, which is what makes pruning save work
//! rather than merely narrow the output.
//!
//! Pruning is offered *before* predicate push-down, so a pushed conjunct's
//! `column_offset` is computed against the already-narrowed scope; the kept
//! set contains the `WHERE`'s columns by construction.
//!
//! [`TableAccess::accept_column_prune`]: crate::table_access::TableAccess::accept_column_prune
//! [`TableAccess::accept_scan_filter`]: crate::table_access::TableAccess::accept_scan_filter
//! [`TableScanExec`]: crate::kv_table::TableScanExec

use std::collections::BTreeSet;

use tidb_ast::{Expr, SelectField, SelectStmt};

use tidb_datatype::FieldType;
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
    collect_statement_columns(select, &resolver, &mut wanted)?;
    Some(wanted.into_iter().collect())
}

/// The scope offsets a two-base-table join reads, when the join is in the
/// widened slice; `None` keeps both sides full width.
///
/// # Why joins need their own gate, and why it stops at two tables
///
/// The single-table gate runs on an already-built source, because nothing has
/// been built from the scope yet at that point. A join is different: its `ON`
/// condition is an expression built from the *concatenated* scope, inside
/// [`crate::driver::from::build_join`]. So the prune-before-expressions rule
/// puts this gate at the one moment when both sides exist as executors but the
/// `ON` has not been rewritten yet -- and the kept set must therefore include
/// the `ON`'s own columns, which is why [`Join::on`] is walked here alongside
/// every clause the single-table gate walks.
///
/// Both sides must be plain base tables (`JoinNode::Table`). A nested join is
/// refused for a reason that is not merely caution: `build_join` recurses, so
/// an inner join's sides would be offered a prune while the *outer* join's
/// `ON` -- which can name an inner table's column -- is nowhere in view. There
/// is no scope in which those references can be seen from inside, so the only
/// safe answer is to refuse the whole shape. `a JOIN b JOIN c` therefore keeps
/// the unchanged full-width path throughout.
///
/// [`Join::on`]: tidb_ast::Join::on
fn prunable_join_columns(
    select: &SelectStmt,
    join: &tidb_ast::Join,
    scope: &FromScope,
) -> Option<Vec<usize>> {
    // Exactly two base tables, joined by an ordinary ON (or nothing). The
    // scope width check is the one that matters -- it is what makes
    // `tables[0]`/`tables[1]` below name the two sides -- and the node checks
    // are what make each side a relation this walk can reason about.
    if scope.tables.len() != 2 {
        return None;
    }
    if !matches!(peel_relation(&join.left), tidb_ast::JoinNode::Table(_)) {
        return None;
    }
    match &join.right {
        Some(right) if matches!(peel_relation(right), tidb_ast::JoinNode::Table(_)) => {}
        _ => return None,
    }
    // `USING` and `NATURAL` name columns implicitly -- by the two sides'
    // shared names rather than by any expression this walk can reach.
    if join.natural || !join.using.is_empty() {
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

    let resolver = crate::driver::scope_resolver(scope);
    let mut wanted = BTreeSet::new();
    collect_statement_columns(select, &resolver, &mut wanted)?;
    if let Some(on) = &join.on {
        collect_expr_columns(on, &resolver, &mut wanted)?;
    }
    Some(wanted.into_iter().collect())
}

/// One join side's `(name, type)` column list, in row order -- the same shape
/// [`FromTable::columns`] holds.
type JoinSideColumns = Vec<(String, FieldType)>;

/// Offers each side of a two-table join the columns that side actually
/// contributes, and reports the narrowed `(left, right)` column lists when a
/// side took the offer.
///
/// # How the concatenated offsets stay consistent
///
/// The kept set is split at `left_width`, so each side is handed offsets in
/// its own local space -- exactly what [`TableAccess::accept_column_prune`]
/// expects. Each side then answers independently and fail-closed, so a
/// refusal on one side leaves that side full width without touching the
/// other. Nothing is renumbered here: the caller rebuilds the concatenated
/// scope from whatever widths the two sides ended up with, and every
/// expression in the statement -- the `ON` included -- is rewritten against
/// that final scope afterwards. There is therefore no offset in existence at
/// the moment of narrowing that could go stale.
///
/// A side whose kept set is empty stays full width: the scan refuses an empty
/// prune (a zero-column row is not a shape this tier's sources emit), so the
/// cross-join side that no clause mentions is simply left alone.
///
/// [`TableAccess::accept_column_prune`]: crate::table_access::TableAccess::accept_column_prune
pub(crate) fn prune_join_sides(
    select: &SelectStmt,
    join: &tidb_ast::Join,
    scope: &FromScope,
    left: &mut Box<dyn crate::executor::Executor>,
    right: &mut Box<dyn crate::executor::Executor>,
) -> Option<(JoinSideColumns, JoinSideColumns)> {
    let keep = prunable_join_columns(select, join, scope)?;
    let left_columns = &scope.tables[0].columns;
    let right_columns = &scope.tables[1].columns;
    let left_width = left_columns.len();

    let left_keep: Vec<usize> = keep.iter().copied().filter(|o| *o < left_width).collect();
    let right_keep: Vec<usize> = keep
        .iter()
        .filter(|o| **o >= left_width)
        .map(|o| o - left_width)
        .collect();

    let left_pruned = left_keep.len() < left_width
        && left
            .table_access()
            .is_some_and(|access| access.accept_column_prune(&left_keep));
    let right_pruned = right_keep.len() < right_columns.len()
        && right
            .table_access()
            .is_some_and(|access| access.accept_column_prune(&right_keep));
    if !left_pruned && !right_pruned {
        return None;
    }
    Some((
        narrowed(left_columns, &left_keep, left_pruned),
        narrowed(right_columns, &right_keep, right_pruned),
    ))
}

/// `columns` narrowed to `keep` when the side took the prune, unchanged
/// otherwise.
fn narrowed(columns: &[(String, FieldType)], keep: &[usize], pruned: bool) -> JoinSideColumns {
    if !pruned {
        return columns.to_vec();
    }
    keep.iter().map(|offset| columns[*offset].clone()).collect()
}

/// A `FROM` node with the parser's single-relation wrappers peeled off.
///
/// `FROM a, b` parses as `Join { left: Join { left: a }, right: b }`: the
/// left side is a real relation wearing a wrapper, not a nested join. Peeling
/// makes the comma and `JOIN ... ON` spellings the same shape to the gate,
/// and a genuine nested join -- which has a `right` -- is left alone and so
/// still fails the `Table` check above.
fn peel_relation(node: &tidb_ast::JoinNode) -> &tidb_ast::JoinNode {
    let mut node = node;
    while let tidb_ast::JoinNode::Join(inner) = node {
        if inner.right.is_some() || inner.on.is_some() || !inner.using.is_empty() || inner.natural {
            break;
        }
        node = &inner.left;
    }
    node
}

/// Adds every scope column the statement's clauses read to `wanted`.
///
/// This is the whole statement *except* a `FROM`-local expression: the
/// single-table shape has none, and the join shape adds its `ON` separately.
fn collect_statement_columns(
    select: &SelectStmt,
    resolver: &dyn ColumnResolver,
    wanted: &mut BTreeSet<usize>,
) -> Option<()> {
    for field in select.fields.fields() {
        match field {
            // A wildcard names every column of the scope, so there is
            // nothing to prune and no expression to walk.
            SelectField::Wildcard(_) => return None,
            SelectField::Expr { expr, alias: _ } => {
                collect_expr_columns(expr, resolver, wanted)?;
            }
        }
    }
    for predicate in [select.where_clause.as_ref(), select.having.as_ref()]
        .into_iter()
        .flatten()
    {
        collect_expr_columns(predicate, resolver, wanted)?;
    }
    for item in &select.group_by {
        collect_expr_columns(&item.expr, resolver, wanted)?;
    }
    for item in &select.order_by {
        collect_expr_columns(&item.expr, resolver, wanted)?;
    }
    if let Some(limit) = &select.limit {
        collect_expr_columns(&limit.count, resolver, wanted)?;
        if let Some(offset) = &limit.offset {
            collect_expr_columns(offset, resolver, wanted)?;
        }
    }
    Some(())
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
