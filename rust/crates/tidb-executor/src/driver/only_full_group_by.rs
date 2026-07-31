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
    // A `GROUP BY` item is checked as Go RESOLVED it: a position and a
    // select-list alias both stand for the field's expression, so `GROUP BY x`
    // over `SELECT dept AS x` pins `dept` and the select list is justified.
    let resolved_items: Vec<tidb_ast::Expr> = select
        .group_by
        .iter()
        .map(|item| {
            resolve_group_by_item(&item.expr, &select.fields, &resolver)
                .map(|resolved| resolved.into_owned())
        })
        .collect::<Result<_, _>>()?;
    let mut group_exprs: Vec<&tidb_ast::Expr> = Vec::new();
    for resolved in &resolved_items {
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

    // Go `checkOnlyFullGroupByWithOutGroupClause`: an `ORDER BY` aggregate over
    // a query whose select list reads a bare column is 3029, reported BEFORE
    // 8123 and regardless of whether the select list aggregates at all
    // (`SELECT id FROM t ORDER BY COUNT(*)` is illegal). Go's `nonAggCols`
    // knows nothing of the WHERE-clause pinning applied above -- captured:
    // `SELECT id FROM ha WHERE id = 1 ORDER BY COUNT(v)` is 3029 too -- so the
    // test is over the select list with nothing pinned.
    if select.group_by.is_empty() {
        let mut bare_columns: Vec<Offender> = Vec::new();
        for (index, field) in select.fields.fields().iter().enumerate() {
            if let tidb_ast::SelectField::Expr { expr, .. } = field {
                collect_offenders(
                    expr,
                    index + 1,
                    Clause::Select,
                    &[],
                    &HashSet::new(),
                    &resolver,
                    &mut bare_columns,
                );
            }
        }
        if !bare_columns.is_empty() {
            if let Some(index) = select
                .order_by
                .iter()
                .position(|item| aggregates_anywhere(&item.expr))
            {
                return Err(DriverError::AggregateOrderNonAggQuery {
                    position: index + 1,
                });
            }
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

/// Go `PlanBuilder.checkOrderByInDistinct`: a `SELECT DISTINCT` may only order
/// by something its own result carries.
///
/// `DISTINCT` collapses rows the select list cannot tell apart, so ordering by
/// a column the select list does NOT report asks for an order over values that
/// no longer exist -- MySQL's #12442. The test is membership in the select
/// list, in the two forms Go compares against: the field's own expression
/// (`originalExprs`) and the field's output column (`p.Schema().Columns`,
/// which is what an alias names). An item passes whole, or else every column
/// it reads outside an aggregate must itself be a reported field.
///
/// This rule is NOT the functional-dependency rule and does not get to use it.
/// Captured: `SELECT DISTINCT id, v FROM pk ORDER BY w` over `pk(id INT
/// PRIMARY KEY, v INT, w INT)` is 3065, even though the primary key in the
/// select list determines `w` -- because after `DISTINCT` the query is not
/// asking for `w` at all. So the rule needs nothing this tier lacks.
///
/// Runs only under `ONLY_FULL_GROUP_BY` (Go gates the whole
/// `buildSortWithCheck` call on `SQLMode.HasOnlyFullGroupBy()`), and always
/// AFTER [`check_only_full_group_by`], which is where `SELECT DISTINCT a FROM
/// t ORDER BY sum(b)` gets its 3029 rather than a 3066 from here.
pub(crate) fn check_order_by_in_distinct(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if !ctx.only_full_group_by() || !select.distinct || select.order_by.is_empty() {
        return Ok(());
    }
    let fields = select.fields.fields();
    // A wildcard's expansion is not written in the select list, so a field
    // list containing one cannot be compared item by item here. Skipping is
    // the permissive side and matches every captured case: `SELECT DISTINCT *
    // FROM t ORDER BY c` and `SELECT DISTINCT t.* FROM t ORDER BY c` are both
    // accepted by TiDB.
    if fields
        .iter()
        .any(|field| matches!(field, tidb_ast::SelectField::Wildcard(_)))
    {
        return Ok(());
    }
    let resolver = ScopeResolver { scope };
    let offset_of = |expr: &tidb_ast::Expr| match strip(expr) {
        tidb_ast::Expr::Column(path) => resolver.resolve(path).map(|(offset, _, _)| offset),
        _ => None,
    };

    // The select list in the three forms an ORDER BY item can name it: the
    // scope offset of a field that IS a column, the field's written
    // expression, and the field's output name (its alias, else its column).
    let mut reported_offsets: HashSet<usize> = HashSet::new();
    let mut reported_exprs: Vec<&tidb_ast::Expr> = Vec::new();
    let mut reported_names: Vec<&str> = Vec::new();
    for field in fields {
        let tidb_ast::SelectField::Expr { expr, alias } = field else {
            continue;
        };
        reported_offsets.extend(offset_of(expr));
        reported_exprs.push(strip(expr));
        match alias {
            Some(alias) => reported_names.push(alias),
            None => {
                if let tidb_ast::Expr::Column(path) = strip(expr) {
                    reported_names.extend(path.last().map(String::as_str));
                }
            }
        }
    }
    // A select-list alias is visible in `ORDER BY` only as the WHOLE item: Go
    // resolves a bare `ORDER BY v` to the field aliased `v`, and `ORDER BY
    // v+1` to the TABLE's `v`. Captured over `SELECT DISTINCT k AS v FROM gg`
    // -- `ORDER BY v` answers `1;2`, `ORDER BY v+1` is 3065 naming
    // `test.gg.v` -- which is why this is not consulted per column below.
    let names_a_reported_field = |path: &[String]| {
        matches!(path, [name]
            if reported_names.iter().any(|reported| reported.eq_ignore_ascii_case(name)))
    };

    for (index, item) in select.order_by.iter().enumerate() {
        let expr = strip(&item.expr);
        // `ORDER BY <n>` names a select field by ordinal, so it is reported by
        // construction.
        if matches!(expr, tidb_ast::Expr::Int(_)) {
            continue;
        }
        let whole_item_is_reported = match expr {
            tidb_ast::Expr::Column(path) => {
                offset_of(expr).is_some_and(|offset| reported_offsets.contains(&offset))
                    || names_a_reported_field(path)
            }
            other => reported_exprs.contains(&other),
        };
        if whole_item_is_reported {
            continue;
        }
        for path in bare_columns(expr) {
            // A name this scope cannot resolve is the column resolver's to
            // report, with its own error.
            let Some((offset, _, _)) = resolver.resolve(&path) else {
                continue;
            };
            if reported_offsets.contains(&offset) {
                continue;
            }
            return Err(DriverError::FieldInOrderNotSelect {
                position: index + 1,
                column: qualified_name(offset, scope),
            });
        }
        // An aggregate reads no bare column of its own, so it reaches here
        // with nothing collected: Go compares the aggregation's OUTPUT column
        // against the select list, which holds it only if the same call is
        // written there. A nested aggregate (`ORDER BY sum(a)+1`) is left
        // alone -- the permissive side, and no captured case reaches it.
        if is_exempt(expr) {
            return Err(DriverError::AggregateInOrderNotSelect { position: index + 1 });
        }
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
