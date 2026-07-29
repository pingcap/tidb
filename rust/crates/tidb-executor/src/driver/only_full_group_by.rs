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

//! Go `PlanBuilder.checkOnlyFullGroupBy`: the `sql_mode=ONLY_FULL_GROUP_BY`
//! rule that a grouped query may only report values a group actually has.
//!
//! A `FIRST_ROW` carrier makes ANY ungrouped column readable -- it answers
//! with whichever row the aggregation happened to see first -- so without this
//! check the engine invents a value rather than refusing the query. Go runs
//! the check in the planner, BEFORE the `GROUP BY` expressions are rewritten
//! (the rewrite can change a field's type), which is why it works on the
//! written AST here too.
//!
//! The rule is SYNTACTIC, not semantic: a column is justified by being NAMED
//! in `GROUP BY`, by being pinned to a single value by an equality in `WHERE`,
//! by appearing only inside an aggregate, or by belonging to a table whose
//! candidate key `GROUP BY` already pins. Determining a column is not enough
//! -- `GROUP BY k+0` fixes `k` for every group and Go still rejects a bare
//! `k`, because `k+0` is not `k`.

use super::*;
use std::collections::HashSet;

/// The clause an offending expression was written in, as Go names it in the
/// error and as it decides which error is reported.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Clause {
    Select,
    OrderBy,
}

impl Clause {
    fn label(self) -> &'static str {
        match self {
            Clause::Select => "SELECT list",
            Clause::OrderBy => "ORDER BY",
        }
    }
}

/// An expression that reports a column the grouping does not justify.
struct Offender {
    /// The column's position in the joined `FROM` scope.
    offset: usize,
    /// The column's bare written name, which the no-`GROUP BY` error quotes.
    name: String,
    /// The offending expression's 1-based position in its clause.
    position: usize,
    clause: Clause,
}

/// Rejects a grouped query that reports a value its groups do not determine.
///
/// A no-op unless `ONLY_FULL_GROUP_BY` is in the session's `sql_mode`, which
/// TiDB's default `sql_mode` carries; clearing the mode restores the
/// permissive `FIRST_ROW` behavior for every case below.
///
/// DEFERRED (documented, matching what has been captured): Go additionally
/// derives functional dependencies ACROSS a join, from an equality in `WHERE`
/// or in an `ON` (`buildWhereFuncDepend` / `buildJoinFuncDepend`), so grouping
/// by one side's key can justify the other side's columns. This checks each
/// table's own candidate keys only, which is strictly the narrower rule --
/// it never permits what Go rejects.
pub(crate) fn check_only_full_group_by(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if !ctx.only_full_group_by() || select.from.is_none() {
        return Ok(());
    }
    let resolver = ScopeResolver { scope };
    let offset_of = |expr: &tidb_ast::Expr| match strip(expr) {
        tidb_ast::Expr::Column(path) => resolver.resolve(path).map(|(offset, _, _)| offset),
        _ => None,
    };

    // Every column the grouping justifies by name: the `GROUP BY` items that
    // ARE a column, plus the columns an equality in `WHERE` fixes to one
    // value for the whole query (Go's `extractSingeValueColNamesFromWhere`,
    // which MySQL documents as a legal ONLY_FULL_GROUP_BY relaxation).
    let mut pinned: HashSet<usize> = HashSet::new();
    if let Some(where_clause) = &select.where_clause {
        for column in single_valued_columns(where_clause) {
            pinned.extend(offset_of(column));
        }
    }
    // A `GROUP BY` item that is NOT a column stands on its own: an expression
    // in a checked clause is justified by being written IDENTICALLY here.
    let mut group_exprs: Vec<&tidb_ast::Expr> = Vec::new();
    for item in &select.group_by {
        let resolved = resolve_group_by_position(&item.expr, select.fields.fields())?;
        // The borrow must outlive the loop body, so a position that resolved
        // to a select field is re-read from the field list rather than from
        // the temporary `Cow`.
        let resolved: &tidb_ast::Expr = match resolved {
            std::borrow::Cow::Borrowed(expr) => expr,
            std::borrow::Cow::Owned(_) => &item.expr,
        };
        match offset_of(resolved) {
            Some(offset) => {
                pinned.insert(offset);
            }
            None => group_exprs.push(strip(resolved)),
        }
    }

    let mut offenders: Vec<Offender> = Vec::new();
    let mut has_aggregate = false;
    for (index, field) in select.fields.fields().iter().enumerate() {
        let tidb_ast::SelectField::Expr { expr, .. } = field else {
            continue;
        };
        has_aggregate |= aggregates_anywhere(expr);
        collect_offenders(
            expr,
            index + 1,
            Clause::Select,
            &group_exprs,
            &pinned,
            &resolver,
            &mut offenders,
        );
    }
    // `ORDER BY` is checked only against an explicit `GROUP BY`: Go's
    // no-`GROUP BY` path reaches the clause only to widen an error it already
    // has, and every captured case there is reported against the SELECT list.
    // An item that merely names a select field was checked as that field.
    if !select.group_by.is_empty() {
        for (index, item) in select.order_by.iter().enumerate() {
            if names_a_select_field(&item.expr, select.fields.fields()) {
                continue;
            }
            collect_offenders(
                &item.expr,
                index + 1,
                Clause::OrderBy,
                &group_exprs,
                &pinned,
                &resolver,
                &mut offenders,
            );
        }
    }

    // A query with no aggregate and no `GROUP BY` is not a grouped query at
    // all, so nothing needs justifying.
    if select.group_by.is_empty() && !has_aggregate {
        return Ok(());
    }
    for offender in offenders {
        if determined_by_a_pinned_key(offender.offset, scope, &pinned) {
            continue;
        }
        return Err(if select.group_by.is_empty() {
            DriverError::FieldNotInAggregatedQuery {
                position: offender.position,
                column: offender.name,
            }
        } else {
            DriverError::FieldNotInGroupBy {
                position: offender.position,
                clause: offender.clause.label(),
                column: qualified_name(offender.offset, scope),
            }
        });
    }
    Ok(())
}

/// Records every column `expr` reports that the grouping does not justify.
///
/// Go's `checkExprInGroupByOrIsSingleValue`: an aggregate is exempt outright,
/// so are `ANY_VALUE()` (MySQL's documented escape hatch) and `GROUPING()`
/// (validated by its own rule later); a non-column expression written exactly
/// as a `GROUP BY` item is exempt as a whole; otherwise every column the
/// expression reads OUTSIDE an aggregate must be pinned.
fn collect_offenders(
    expr: &tidb_ast::Expr,
    position: usize,
    clause: Clause,
    group_exprs: &[&tidb_ast::Expr],
    pinned: &HashSet<usize>,
    resolver: &ScopeResolver<'_>,
    out: &mut Vec<Offender>,
) {
    let expr = strip(expr);
    if is_exempt(expr) {
        return;
    }
    if !matches!(expr, tidb_ast::Expr::Column(_)) && group_exprs.contains(&expr) {
        return;
    }
    for path in bare_columns(expr) {
        let Some((offset, _, _)) = resolver.resolve(&path) else {
            // An unresolvable name is reported by the ordinary column
            // resolver, with its own error; this rule has nothing to say.
            continue;
        };
        if pinned.contains(&offset) {
            continue;
        }
        out.push(Offender {
            offset,
            name: path.last().cloned().unwrap_or_default(),
            position,
            clause,
        });
    }
}

/// Whether the column at `offset` belongs to a source one of whose candidate
/// keys is entirely pinned, which makes every column of that source a single
/// value per group (Go's `checkColFuncDepend`).
fn determined_by_a_pinned_key(offset: usize, scope: &FromScope, pinned: &HashSet<usize>) -> bool {
    let Some(table) = table_at(offset, scope) else {
        return false;
    };
    table.determinants.iter().any(|key| {
        key.iter().all(|name| {
            table
                .columns
                .iter()
                .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .is_some_and(|index| pinned.contains(&(table.offset + index)))
        })
    })
}

/// The source the joined scope's column at `offset` came from.
fn table_at(offset: usize, scope: &FromScope) -> Option<&FromTable> {
    scope
        .tables
        .iter()
        .find(|table| (table.offset..table.offset + table.columns.len()).contains(&offset))
}

/// The column at `offset` as Go's `ErrFieldNotInGroupBy` qualifies it:
/// `db.tbl.col`, dropping the parts the source does not have (an aliased
/// table answers to no schema).
fn qualified_name(offset: usize, scope: &FromScope) -> String {
    let Some(table) = table_at(offset, scope) else {
        return String::new();
    };
    let column = table
        .columns
        .get(offset - table.offset)
        .map(|(name, _)| name.as_str())
        .unwrap_or_default();
    [
        table.database.as_deref(),
        Some(table.name.as_str()),
        Some(column),
    ]
    .into_iter()
    .flatten()
    .filter(|part| !part.is_empty())
    .collect::<Vec<_>>()
    .join(".")
}

/// Go `getInnerFromParenthesesAndUnaryPlus`: parentheses and a unary `+` are
/// pure notation, so `(k)` and `+k` are the column `k` on both sides of the
/// comparison this rule makes.
fn strip(expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
    match expr {
        tidb_ast::Expr::Paren(inner) => strip(inner),
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Plus, inner) => strip(inner),
        other => other,
    }
}

/// Whether the expression carries no obligation of its own: an aggregate
/// collapses its whole group, `ANY_VALUE()` says the caller accepts an
/// arbitrary row, and `GROUPING()` is checked by its own rule.
fn is_exempt(expr: &tidb_ast::Expr) -> bool {
    match expr {
        tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. } => true,
        tidb_ast::Expr::Func { name, .. } => {
            name.eq_ignore_ascii_case("any_value") || name.eq_ignore_ascii_case("grouping")
        }
        _ => false,
    }
}

/// Whether `expr` contains an aggregate anywhere, which is what makes a
/// `GROUP BY`-less query a grouped one (Go's `hasAggFuncOrAnyValue`).
fn aggregates_anywhere(expr: &tidb_ast::Expr) -> bool {
    struct Found(bool);
    impl tidb_ast::Visitor for Found {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() {
                if is_exempt(expr) {
                    self.0 = true;
                    return true;
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut found = Found(false);
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut found);
    found.0
}

/// Every column `expr` reads OUTSIDE an aggregate, a window call or an
/// `ANY_VALUE()`, which are the subtrees that answer for their own columns.
pub(super) fn bare_columns(expr: &tidb_ast::Expr) -> Vec<Vec<String>> {
    struct Collector {
        found: Vec<Vec<String>>,
    }
    impl tidb_ast::Visitor for Collector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() else {
                return false;
            };
            match expr {
                tidb_ast::Expr::Column(path) => {
                    self.found.push(path.clone());
                    true
                }
                // A window call's own frame reads whole partitions, so its
                // columns are not this expression's to justify.
                tidb_ast::Expr::Window { .. } => true,
                // A subquery has its OWN scope: `s.k` in `(SELECT ... WHERE
                // s.k = t.g)` names the subquery's table, not this one. The
                // outer names it correlates to (`t.g`) go unchecked here,
                // which is the permissive side -- Go reaches them through
                // `collect_outer_columns`, a widening this rule has no
                // captured case for.
                tidb_ast::Expr::Subquery(_) | tidb_ast::Expr::Exists { .. } => true,
                tidb_ast::Expr::InSubquery { expr, .. } => {
                    self.found.extend(bare_columns(expr));
                    true
                }
                tidb_ast::Expr::CompareSubquery { left, .. } => {
                    self.found.extend(bare_columns(left));
                    true
                }
                other => is_exempt(other),
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collector = Collector { found: Vec::new() };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut collector);
    collector.found
}

/// Whether an `ORDER BY` item merely names a select field, which was already
/// checked in its own right (Go skips such an item explicitly).
fn names_a_select_field(expr: &tidb_ast::Expr, fields: &[tidb_ast::SelectField]) -> bool {
    let tidb_ast::Expr::Column(path) = strip(expr) else {
        return false;
    };
    let [name] = path.as_slice() else {
        return false;
    };
    fields.iter().any(|field| match field {
        tidb_ast::SelectField::Expr { expr, alias } => match alias {
            Some(alias) => alias.eq_ignore_ascii_case(name),
            None => matches!(expr, tidb_ast::Expr::Column(path)
                if path.last().is_some_and(|last| last.eq_ignore_ascii_case(name))),
        },
        tidb_ast::SelectField::Wildcard(_) => false,
    })
}

/// The columns a `WHERE` fixes to one value for the whole query, which MySQL
/// documents as justifying an ungrouped reference to them.
///
/// Go's `extractSingeValueColNamesFromWhere` reads only the top-level `AND`
/// chain and only the shape `col = <literal>` (or a negated literal) in
/// either operand order -- a deliberately shallow rule, not a constant folder.
fn single_valued_columns(where_clause: &tidb_ast::Expr) -> Vec<&tidb_ast::Expr> {
    let mut found = Vec::new();
    let mut stack = vec![where_clause];
    while let Some(expr) = stack.pop() {
        match expr {
            tidb_ast::Expr::Paren(inner) => stack.push(inner),
            tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) => {
                stack.push(lhs);
                stack.push(rhs);
            }
            tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, lhs, rhs) => {
                if is_literal(rhs) {
                    found.push(&**lhs);
                }
                if is_literal(lhs) {
                    found.push(&**rhs);
                }
            }
            _ => {}
        }
    }
    found
}

/// A literal, or a literal behind a unary sign -- the two shapes Go's
/// single-value extraction accepts on the other side of the equality.
fn is_literal(expr: &tidb_ast::Expr) -> bool {
    match expr {
        tidb_ast::Expr::Unary(_, inner) => {
            matches!(
                &**inner,
                tidb_ast::Expr::Int(_)
                    | tidb_ast::Expr::Float(_)
                    | tidb_ast::Expr::Decimal(_)
                    | tidb_ast::Expr::String(_)
                    | tidb_ast::Expr::Bool(_)
            )
        }
        tidb_ast::Expr::Int(_)
        | tidb_ast::Expr::Float(_)
        | tidb_ast::Expr::Decimal(_)
        | tidb_ast::Expr::String(_)
        | tidb_ast::Expr::Bool(_)
        | tidb_ast::Expr::Null => true,
        _ => false,
    }
}
