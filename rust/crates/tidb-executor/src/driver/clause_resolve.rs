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

//! Go `havingWindowAndOrderbyExprResolver` and `gbyResolver`: resolving an
//! `ORDER BY`, `HAVING` or `GROUP BY` item against the SELECT list, so a
//! select alias and a 1-based output position both name a projected
//! expression.
//!
//! One module because it is one rule with three clause-specific answers, and
//! the differences are the point:
//!
//! - `ORDER BY`: an alias SHADOWS a real column of the same name.
//! - `GROUP BY`: the REAL COLUMN WINS, matching `gbyResolver.Leave`, which
//!   substitutes only when `FindFieldName` found nothing.
//! - `HAVING`: a name resolves against the aggregation's own output first and
//!   reaches the select list only for a name that output lacks.
//!
//! A bare integer is a position only at the TOP of an item -- `ORDER BY twice
//! + 0` is arithmetic, not position 1 -- and what an unusable position REPORTS
//! is also the clause's choice ([`PositionalError`]).

use super::*;
/// Go `havingWindowAndOrderbyExprResolver`: an `ORDER BY` item is resolved
/// against the SELECT list first, so a select alias and an output position
/// both name a projected expression.
///
/// Go rewrites the reference into the projected expression itself, which is
/// what this does -- the sort then runs over the source rows with no plan
/// reshuffle, and an expression BUILT on an alias (`ORDER BY twice + 0`)
/// falls out for free.
///
/// Captured from TiDB: an alias SHADOWS a real column of the same name
/// (`SELECT b AS a FROM t ORDER BY a` sorts by `b`); a bare integer is a
/// 1-based output position, and only at the top level (`ORDER BY twice + 0`
/// is arithmetic, not position 1); an out-of-range position and an unknown
/// name are both `ErrUnknownColumn` naming the `order clause`.
pub(crate) fn substitute_output_aliases(
    expr: &tidb_ast::Expr,
    fields: &[SelectField],
    top_level: bool,
) -> Result<tidb_ast::Expr, DriverError> {
    substitute_output_aliases_where(expr, fields, top_level, &|_| false)
}

/// Go resolves a positional `ORDER BY` item before any optimizer rule runs:
/// the parser builds a bare integer item as an `ast.PositionExpr` and
/// `positionToScalarFunc` (`expression_rewriter.go:1935`) rewrites it to the
/// projected column of the built plan. Go's planner therefore never sees a
/// literal sort key -- every rule downstream (aggregate pushdown included)
/// compares resolved columns. The driver keeps clauses as written until each
/// consumer resolves them, which is why a positional item still looked like a
/// literal to the grouped partial-aggregate plans' order/group match.
///
/// This is that one early resolution: every positional ORDER BY item becomes
/// its projected expression, so a positional sort is planned exactly like the
/// equivalent named-column sort. Items that name no field keep their written
/// form; the ORDER BY stage's own resolution reports them where it did
/// before.
pub(crate) fn resolve_positional_order_by(
    select: &tidb_ast::SelectStmt,
) -> Option<tidb_ast::SelectStmt> {
    let positional = select
        .order_by
        .iter()
        .any(|item| is_positional_field(&item.expr));
    if !positional {
        return None;
    }
    let mut resolved = select.clone();
    for item in &mut resolved.order_by {
        if !is_positional_field(&item.expr) {
            continue;
        }
        // A position naming a BARE-LITERAL field (`SELECT 42 ORDER BY 1`)
        // resolves to another bare integer -- and unlike Go, whose resolved
        // form is a dedicated `PositionExpr` node consumed exactly once, a
        // second top-level resolution here would read that integer back as a
        // NEW position. Such an item keeps its written form; the ORDER BY
        // stage's own substitution handles it as it did before.
        if let Ok(expr) = substitute_output_aliases(&item.expr, select.fields.fields(), true) {
            if positional_field_index(&expr).is_none() {
                item.expr = expr;
            }
        }
    }
    Some(resolved)
}

/// [`substitute_output_aliases`], with the names that already resolve where
/// the caller is standing held back.
///
/// `HAVING` needs that and `ORDER BY` does not, which IS Go's difference
/// between the two: `havingWindowAndOrderbyExprResolver` resolves a `HAVING`
/// name against the aggregation's own output FIRST and reaches the select
/// list only for a name that output lacks, while an `ORDER BY` alias shadows
/// a source column outright.
pub(crate) fn substitute_output_aliases_where(
    expr: &tidb_ast::Expr,
    fields: &[SelectField],
    top_level: bool,
    resolves_already: &dyn Fn(&str) -> bool,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // A bare integer at the top of an ORDER BY item is an output position.
    if top_level {
        if let Some((text, index)) = positional_field_index(expr) {
            let index = index.map_err(|_| unknown_order_column(text))?;
            let projected = fields
                .iter()
                .filter_map(|field| match field {
                    SelectField::Expr { expr, .. } => Some(expr),
                    SelectField::Wildcard(_) => None,
                })
                .nth(index)
                .ok_or_else(|| unknown_order_column(text))?;
            return Ok(projected.clone());
        }
    }
    Ok(match expr {
        // A one-segment name may be a select alias; a qualified one
        // (`t.a`) always addresses the source.
        Expr::Column(path) if path.len() == 1 && !resolves_already(&path[0]) => {
            let alias = fields.iter().find_map(|field| match field {
                SelectField::Expr {
                    expr,
                    alias: Some(alias),
                } if alias.eq_ignore_ascii_case(&path[0]) => Some(expr),
                _ => None,
            });
            match alias {
                Some(expr) => expr.clone(),
                None => expr.clone(),
            }
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(substitute_output_aliases_where(
            inner,
            fields,
            false,
            resolves_already,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_output_aliases_where(
                inner,
                fields,
                false,
                resolves_already,
            )?),
        ),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(substitute_output_aliases_where(
                left,
                fields,
                false,
                resolves_already,
            )?),
            Box::new(substitute_output_aliases_where(
                right,
                fields,
                false,
                resolves_already,
            )?),
        ),
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| substitute_output_aliases_where(arg, fields, false, resolves_already))
                .collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        other => other.clone(),
    })
}

/// Go `havingWindowAndOrderbyExprResolver` for a NON-aggregate `SELECT`: the
/// `HAVING` expression with every name it may use replaced by the select
/// field it names, and `ErrUnknownColumn` for every name it may not.
///
/// Go builds `HAVING` as a `LogicalSelection` ABOVE the select list's
/// `Projection` (`buildSelect`: `buildProjection` then `if sel.Having != nil {
/// buildSelection(...) }`), so the clause sees the PROJECTION's output and
/// nothing else. `resolveHavingAndOrderBy` runs first and is what enforces it:
/// with no `GROUP BY` items to match, `resolveFieldsFirst` stays true, and the
/// `havingClause` branch of `resolveFromPlan` returns `-1` for every name the
/// select list lacks -- so a source column that is merely IN SCOPE is
/// `ErrUnknownColumn` naming the `having clause`, in every `sql_mode`.
///
/// Captured from TiDB on `ht(a, b)`:
///
/// ```text
/// select a from ht having b > 0        -- 1054 Unknown column 'b'
/// select a from ht having ht.b > 0     -- 1054 Unknown column 'ht.b'
/// select a from ht having b is null    -- 1054 (the shape, not the operator)
/// select a, b from ht having b > 0     -- 1|10 2|20   (b IS projected)
/// select a from ht having a > 0        -- 1;2
/// select count(*) c from ht having c>0 -- the aggregate path, still legal
/// select b as a from ht having a > 15  -- 20   (the ALIAS wins over ht.a)
/// select b as a from ht having ht.a>1  -- 1054 Unknown column 'ht.a'
/// select a+1 as a from ht having a > 2 -- 3
/// select a from ht t1 having t1.a > 1  -- 2    (the FROM alias resolves)
/// select a from ht t1 having ht.a > 1  -- 1054 Unknown column 'ht.a'
/// select * from ht having b > 15       -- 2|20 (the star is unfolded first)
/// ```
///
/// `fields` is the select list AS PROJECTED -- `*` already unfolded, which is
/// Go's order (`unfoldWildStar` runs before `resolveHavingAndOrderBy`) and
/// what makes the last capture work.
///
/// Subquery bodies are NOT walked: `Enter` returns `skipChildren` for
/// `*ast.SubqueryExpr`/`*ast.ExistsSubqueryExpr`, so a correlated name inside
/// one is resolved later, against the projection -- see
/// [`check_having_subquery_correlations`].
pub(crate) fn resolve_having_names(
    having: &tidb_ast::Expr,
    fields: &[(Option<String>, tidb_ast::Expr)],
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    struct Rewriter<'a, 'b> {
        fields: &'a [(Option<String>, tidb_ast::Expr)],
        resolver: &'a ScopeResolver<'b>,
        error: Option<DriverError>,
    }
    impl Rewriter<'_, '_> {
        fn visit(&mut self, expr: &mut tidb_ast::Expr) {
            tidb_ast::Visitable::accept(expr, self);
        }
    }
    impl tidb_ast::Visitor for Rewriter<'_, '_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if self.error.is_some() {
                return true;
            }
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            match expr {
                // Go enters a new context for a subquery and skips it.
                tidb_ast::Expr::Subquery(_) | tidb_ast::Expr::Exists { .. } => true,
                // `x IN (subquery)` / `x > ANY (subquery)`: the node itself is
                // visited and only its `SubqueryExpr` child is skipped, so the
                // operand BESIDE the subquery answers to this rule.
                tidb_ast::Expr::InSubquery { expr: operand, .. }
                | tidb_ast::Expr::CompareSubquery { left: operand, .. } => {
                    let mut left = (**operand).clone();
                    self.visit(&mut left);
                    **operand = left;
                    true
                }
                tidb_ast::Expr::Column(path) => {
                    match resolve_having_column(path, self.fields, self.resolver) {
                        Ok(replacement) => *expr = replacement,
                        Err(error) => self.error = Some(error),
                    }
                    true
                }
                _ => false,
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut rewritten = having.clone();
    let mut visitor = Rewriter {
        fields,
        resolver,
        error: None,
    };
    visitor.visit(&mut rewritten);
    match visitor.error {
        Some(error) => Err(error),
        None => Ok(rewritten),
    }
}

/// One `HAVING` name, resolved the way `resolveFromSelectFields` +
/// `resolveFromPlan`'s `havingClause` branch resolve it.
///
/// Unqualified: Go's two `resolveFromSelectFields` passes, an alias-respecting
/// one and an alias-IGNORING one, in that order -- which is why an alias
/// SHADOWS a real column (`SELECT b AS a ... HAVING a` reads `b`) while an
/// aliased field is still reachable by its own column name.
///
/// Qualified: Go canonicalizes the name against the FROM scope first
/// (`resolveFromPlan`) and then requires a select field whose WRITTEN name
/// matches the canonical one (`ColumnName.Match`: an empty qualifier matches
/// anything, a written one must be equal). It never appends an auxiliary
/// field for `HAVING`, so a canonical name no field matches is 1054.
fn resolve_having_column(
    path: &[String],
    fields: &[(Option<String>, tidb_ast::Expr)],
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    let written = path.join(".");
    let column_of = |expr: &tidb_ast::Expr| match expr {
        tidb_ast::Expr::Column(p) => Some(p.clone()),
        _ => None,
    };
    if let [name] = path {
        // Pass 1, `ignoreAsName = false`: an aliased field answers only to its
        // alias, an unaliased column field to its column name.
        let matched = fields.iter().find_map(|(alias, expr)| match alias {
            Some(alias) => alias.eq_ignore_ascii_case(name).then(|| expr.clone()),
            None => column_of(expr)
                .filter(|p| p.last().is_some_and(|c| c.eq_ignore_ascii_case(name)))
                .map(|_| expr.clone()),
        });
        // Pass 2, `ignoreAsName = true`: any column field, alias or not.
        let matched = matched.or_else(|| {
            fields.iter().find_map(|(_, expr)| {
                column_of(expr)
                    .filter(|p| p.last().is_some_and(|c| c.eq_ignore_ascii_case(name)))
                    .map(|_| expr.clone())
            })
        });
        return matched.ok_or_else(|| unknown_having_column(&written));
    }
    let (index, _, _) = resolver
        .resolve(path)
        .ok_or_else(|| unknown_having_column(&written))?;
    let (table, column) = resolver
        .scope
        .tables
        .iter()
        .find(|table| (table.offset..table.offset + table.columns.len()).contains(&index))
        .and_then(|table| {
            Some((
                table.name.clone(),
                table.columns.get(index - table.offset)?.0.clone(),
            ))
        })
        .ok_or_else(|| unknown_having_column(&written))?;
    fields
        .iter()
        .find_map(|(_, expr)| {
            let field = column_of(expr)?;
            let (field_column, field_table) = match field.as_slice() {
                [name] => (name, None),
                [.., qualifier, name] => (name, Some(qualifier)),
                [] => return None,
            };
            let matches = field_column.eq_ignore_ascii_case(&column)
                && field_table.is_none_or(|q| q.eq_ignore_ascii_case(&table));
            matches.then(|| expr.clone())
        })
        .ok_or_else(|| unknown_having_column(&written))
}

/// One column of the select list as the `HAVING` clause's SUBQUERIES see it:
/// a name, the table that name is still qualified by, and the SOURCE-row
/// expression it reads.
///
/// This is Go's `FieldName` on the `Projection` `HAVING` sits above -- and it
/// is NOT the same thing [`resolve_having_column`] matches against. A field
/// written `b AS bb` has `ColName = bb` and `TblName = ht`, so the projection
/// answers to `bb` and to `ht.bb` but NOT to `ht.b`, while the select-field
/// rule above still matches `ht.b` by the field's WRITTEN name. Both were
/// captured from TiDB, and the pair is the whole reason this is a second
/// rule:
///
/// ```text
/// select b as bb from ht having (select y from hs where hs.x = bb) > 0
///   -- 10
/// select b as bb from ht having (select y from hs where hs.x = ht.b) > 0
///   -- 1054 Unknown column 'ht.b' in 'having clause'
/// select a from ht having (select y from hs where hs.x = ht.b) > 0
///   -- 1054 Unknown column 'ht.b' in 'having clause'
/// select a from ht having (select y from hs where hs.x = ht.a) > 0
///   -- (no rows: the correlation is legal, nothing matches)
/// select a, b from ht having (select y from hs where hs.x = ht.b) > 0
///   -- 1|10
/// ```
pub(crate) struct HavingOutput {
    /// The output column's name: the written alias, or the column's own name.
    pub(crate) name: String,
    /// The table the output is still qualified by -- present only for a plain
    /// column field, which is the only shape Go gives an `OrigTblName`.
    pub(crate) table: Option<String>,
}

/// Whether `path` names one of the projection's outputs, and which.
///
/// Go's `expression.FindFieldName` over the `Projection`'s names: an
/// unqualified path matches by column name, a qualified one must also match
/// the output's table.
pub(crate) fn find_having_output<'a>(
    path: &[String],
    outputs: &'a [HavingOutput],
) -> Option<&'a HavingOutput> {
    let (name, qualifier) = match path {
        [name] => (name, None),
        [.., qualifier, name] => (name, Some(qualifier)),
        [] => return None,
    };
    outputs.iter().find(|output| {
        output.name.eq_ignore_ascii_case(name)
            && qualifier.is_none_or(|q| {
                output
                    .table
                    .as_deref()
                    .is_some_and(|table| table.eq_ignore_ascii_case(q))
            })
    })
}

/// Go `ErrUnknownColumn` naming the `having clause`.
pub(crate) fn unknown_having_column(name: &str) -> DriverError {
    DriverError::UnknownColumnInClause {
        column: name.to_owned(),
        clause: "having clause".to_owned(),
    }
}

/// Go `gbyResolver`, whole: a `GROUP BY` item's positions AND its select-list
/// aliases, resolved the way that resolver does.
///
/// The two rules are one pass because they are one clause: `GROUP BY 1` and
/// `GROUP BY x` both end up naming a select field's expression, and every
/// consumer below (the aggregation's keys, `ONLY_FULL_GROUP_BY`, `GROUPING`)
/// then reads the RESOLVED item and needs no notion of either.
///
/// The alias rule is not `ORDER BY`'s. Captured from TiDB, and this is the
/// difference: in `ORDER BY` an alias SHADOWS a real column of the same name,
/// while in `GROUP BY` the REAL COLUMN WINS -- `SELECT y AS x FROM t GROUP BY
/// x` groups by `t.x`, not by `y`, and then rejects the select list under
/// `ONLY_FULL_GROUP_BY` because `y` is not determined by `t.x`. Go's
/// `gbyResolver.Leave` is where that falls out: it substitutes only when
/// `FindFieldName` found nothing.
pub(crate) fn resolve_group_by_item<'a>(
    expr: &'a tidb_ast::Expr,
    fields: &'a SelectFieldList,
    resolver: &ScopeResolver<'_>,
) -> Result<std::borrow::Cow<'a, tidb_ast::Expr>, DriverError> {
    if positional_field_index(expr).is_some() {
        return resolve_group_by_position(expr, fields);
    }
    Ok(std::borrow::Cow::Owned(substitute_group_by_aliases(
        expr, fields, resolver,
    )?))
}

/// One node of [`resolve_group_by_item`]'s alias substitution.
///
/// Go carries a `gbyResolver.inExpr` flag that says whether the name sits at
/// the TOP of the item or inside a larger expression. It changes nothing
/// here, and deliberately has no counterpart: both of Go's branches keep a
/// name the `FROM` scope has and substitute one it lacks, so the flag only
/// ever selects between two paths that agree. `GROUP BY x + 0` over `SELECT
/// dept AS x` therefore groups by `dept + 0`, which is what TiDB does.
fn substitute_group_by_aliases(
    expr: &tidb_ast::Expr,
    fields: &SelectFieldList,
    resolver: &ScopeResolver<'_>,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    let recurse = |inner: &Expr| substitute_group_by_aliases(inner, fields, resolver);
    Ok(match expr {
        Expr::Column(path) if path.len() == 1 => {
            if resolver.resolve(path).is_some() {
                // A real column of the `FROM` scope always wins.
                return Ok(expr.clone());
            }
            let alias = fields.fields().iter().find_map(|field| match field {
                SelectField::Expr {
                    expr,
                    alias: Some(alias),
                } if alias.eq_ignore_ascii_case(&path[0]) => Some(expr),
                _ => None,
            });
            let Some(target) = alias else {
                // Not a column and not an alias: the ordinary resolver
                // reports it, with its own error.
                return Ok(expr.clone());
            };
            // Grouping happens BEFORE aggregates and window functions have a
            // value, so an alias naming one is Go's ErrIllegalReference.
            let reason = if aggregates_in(target) {
                Some("reference to group function")
            } else if !crate::window::windows_in(target).is_empty() {
                Some("reference to window function")
            } else {
                None
            };
            if let Some(reason) = reason {
                return Err(DriverError::IllegalReference {
                    name: path[0].clone(),
                    reason,
                });
            }
            target.clone()
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(recurse(inner)?)),
        Expr::Unary(op, inner) => Expr::Unary(*op, Box::new(recurse(inner)?)),
        Expr::Binary(op, left, right) => {
            Expr::Binary(*op, Box::new(recurse(left)?), Box::new(recurse(right)?))
        }
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args.iter().map(recurse).collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        other => other.clone(),
    })
}

/// Whether `expr` calls an aggregate anywhere, which is what makes a `GROUP
/// BY` alias reference to it illegal.
fn aggregates_in(expr: &tidb_ast::Expr) -> bool {
    match expr {
        tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. } => true,
        tidb_ast::Expr::Paren(inner) | tidb_ast::Expr::Unary(_, inner) => aggregates_in(inner),
        tidb_ast::Expr::Binary(_, left, right) => aggregates_in(left) || aggregates_in(right),
        tidb_ast::Expr::Func { args, .. } => args.iter().any(aggregates_in),
        _ => false,
    }
}

/// Go `gbyResolver`: a bare integer at the top of a `GROUP BY` item is a
/// 1-based output position resolved against the SELECT list.
///
/// Captured from TiDB: an out-of-range position is `ErrUnknownColumn` naming
/// the `group statement`; a position landing on an aggregate or
/// window-function select field is `ErrWrongGroupField` ("Can't group on
/// '<name>'"), naming the field's alias if it has one and its written text
/// otherwise.
pub(crate) fn resolve_group_by_position<'a>(
    expr: &'a tidb_ast::Expr,
    fields: &'a SelectFieldList,
) -> Result<std::borrow::Cow<'a, tidb_ast::Expr>, DriverError> {
    let Some((text, index)) = positional_field_index(expr) else {
        return Ok(std::borrow::Cow::Borrowed(expr));
    };
    let index = index.map_err(|_| unknown_group_position(text))?;
    let (target, alias, field_index) = fields
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(field_index, field)| match field {
            SelectField::Expr { expr, alias } => Some((expr, alias, field_index)),
            SelectField::Wildcard(_) => None,
        })
        .nth(index)
        .ok_or_else(|| unknown_group_position(text))?;
    if matches!(
        target,
        tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. }
    ) || !crate::window::windows_in(target).is_empty()
    {
        let name = alias
            .clone()
            .unwrap_or_else(|| default_field_display_name(fields, field_index, target));
        return Err(DriverError::WrongGroupField(name));
    }
    Ok(std::borrow::Cow::Borrowed(target))
}

/// Why a bare-integer clause item is not a usable output position.
///
/// The clause decides what this REPORTS: `ORDER BY` and `GROUP BY` raise
/// `ErrUnknownColumn` naming their own clause, and the DML tier refuses the
/// statement outright. The rule itself -- which integers are positions and
/// which index they name -- is the same everywhere, so it lives once in
/// [`positional_field_index`].
pub(crate) enum PositionalError {
    /// The digits do not fit a `usize` (Go's `strconv.ParseUint` failure).
    Malformed,
    /// Position `0`, which MySQL numbers from 1 and so never names a field.
    Zero,
}

/// Go's shared "bare integer is a 1-based output position" rule, as it applies
/// in `ORDER BY`, `GROUP BY` and the DML tier's own `ORDER BY`.
///
/// Returns `None` when `expr` is not a bare integer at all -- the item is then
/// an ordinary expression and every caller falls through to its usual
/// resolution. Otherwise it yields the integer AS WRITTEN (which the callers'
/// errors quote verbatim, as MySQL does) together with the ZERO-based field
/// index it names, or why it names none.
///
/// `TRUE`/`FALSE` are positions too: Go's parser builds them with
/// `ast.NewValueExpr(bool)`, and `types.Datum` has no boolean kind, so they
/// reach the clause as the plain integers `1`/`0` and the position rule sees
/// nothing else. Captured from TiDB: `GROUP BY TRUE` groups by the first
/// select field exactly like `GROUP BY 1`, and `GROUP BY FALSE` reports the
/// same "Unknown column '0' in 'group statement'" `GROUP BY 0` does.
pub(crate) fn positional_field_index(
    expr: &tidb_ast::Expr,
) -> Option<(&str, Result<usize, PositionalError>)> {
    let text = match expr {
        tidb_ast::Expr::Int(text) => text.as_str(),
        tidb_ast::Expr::Bool(true) => "1",
        tidb_ast::Expr::Bool(false) => "0",
        _ => return None,
    };
    let index = match text.parse::<usize>() {
        Err(_) => Err(PositionalError::Malformed),
        Ok(0) => Err(PositionalError::Zero),
        Ok(position) => Ok(position - 1),
    };
    Some((text, index))
}

/// Whether a clause item is the bare-integer output position form, without
/// resolving it -- see [`positional_field_index`].
pub(crate) fn is_positional_field(expr: &tidb_ast::Expr) -> bool {
    positional_field_index(expr).is_some()
}

/// Go `ErrUnknownColumn` naming the `group statement`, for a `GROUP BY`
/// position that is zero or past the end of the SELECT list.
fn unknown_group_position(text: &str) -> DriverError {
    DriverError::UnknownColumnInClause {
        column: text.to_owned(),
        clause: "group statement".to_owned(),
    }
}

/// The `ErrUnknownColumn` an unresolvable `ORDER BY` item reports, when the
/// item is a plain name -- anything else keeps the rewriter's own error.
pub(crate) fn order_by_column_error(expr: &tidb_ast::Expr) -> Option<DriverError> {
    match expr {
        tidb_ast::Expr::Column(path) => Some(unknown_order_column(&path.join("."))),
        _ => None,
    }
}

/// Go `ErrUnknownColumn` naming the `order clause`.
fn unknown_order_column(name: &str) -> DriverError {
    DriverError::UnknownColumnInClause {
        column: name.to_owned(),
        clause: "order clause".to_owned(),
    }
}
