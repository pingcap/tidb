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

//! Subqueries: Go's `expressionRewriter` subquery handling plus the Apply the
//! correlated ones need, reduced to the one shape this tier plans.
//!
//! Two paths, split by whether the subquery reads an outer column. An
//! UNCORRELATED subquery is planned and run once, and its result folds back
//! into the enclosing AST as a literal ([`fold_select_subqueries`]) -- Go's own
//! constant-folding of a scalar subquery, reached here through the AST because
//! this tier executes SQL text. A CORRELATED one cannot be folded, so it is
//! LIFTED out of the expression ([`extract_correlated_subquery`]) and re-run
//! per outer row with the outer columns bound ([`run_correlated_subquery`]),
//! which is Go's Apply operator with a nested loop for its join.
//!
//! [`extract_and_hoist_subquery`] is the aggregate-query variant of the same
//! lift: an aggregate query's Apply column has to be appended AFTER the
//! aggregation, so the select field records where it reads from
//! ([`OutputSlot`]) instead of being rewritten in place.

use std::borrow::Cow;

use super::*;
/// The type of the column an Apply appends for a correlated scalar subquery.
///
/// Go infers it statically from the subquery's select field, where a
/// correlated reference is a `CorrelatedColumn` carrying the OUTER column's
/// own `RetType`. Here the query is planned once with every correlated column
/// bound to a stand-in value, which reaches the same field type without
/// depending on any outer row -- and it must, because the appended column's
/// width is fixed before the first inner run (a `SUM` is a 40-byte decimal,
/// not an 8-byte integer).
///
/// The stand-in is [`probe_datum`] of the outer column's own type, NOT a bare
/// NULL, for the reason `build_lateral_join` already states for the `LATERAL`
/// shape: a NULL erases the type it stood for, so `select t.a from t t1 limit
/// 1` infers `NULL`, whose chunk column is variable-length, and the first
/// inner run then appends an 8-byte integer into a zero-width cell and panics.
/// Both Apply shapes now settle their inner column the one way.
///
/// `outer` is the scope the correlated columns bind against. Falling back to
/// `LongLong` matches what the rest of the seed does for an uninferred
/// expression.
pub(crate) fn subquery_result_type(
    correlated: &CorrelatedSubquery,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<FieldType> {
    let resolver = ScopeResolver { scope: outer };
    let probes: Vec<(Vec<String>, Datum)> = correlated
        .columns
        .iter()
        .map(|path| {
            let datum = resolver.resolve(path).map_or(Datum::Null, |(_, ft, _)| {
                crate::driver::from::probe_datum(&ft)
            });
            (path.clone(), datum)
        })
        .collect();
    let typed = bind_subquery_columns(&correlated.select, &probes).ok()?;
    run_select_stmt(&typed, catalog, current_db, ctx)
        .ok()
        .and_then(|(columns, _)| columns.first().map(|(_, ft)| ft.clone()))
}

/// A short description of a driver error, for the executor-level error the
/// apply callback must return.
///
/// Borrowed when the reason is fixed text, owned when the refusal built it per
/// call -- so a refusal that names what it saw keeps saying so here instead of
/// being flattened to the generic phrase.
pub(crate) fn driver_error_text(error: &DriverError) -> Cow<'static, str> {
    match error {
        DriverError::SubqueryReturnsMoreThanOneRow => {
            Cow::Borrowed("Subquery returns more than 1 row")
        }
        DriverError::Unsupported(text) => text.clone(),
        _ => Cow::Borrowed("the correlated subquery failed"),
    }
}

/// What the outer expression asks of a correlated subquery's result.
///
/// Go builds a different plan for each: `handleScalarSubquery` for a scalar
/// read, and a semi join (`LogicalJoin` with `SemiJoin`/`AntiSemiJoin`/
/// `LeftOuterSemiJoin`) for `EXISTS`, `IN` and `ANY`/`ALL`. Here they all ride
/// one Apply, because the join's answer for one outer row is exactly what
/// running the inner query for that row and folding the result yields.
pub(crate) enum SubqueryKind {
    /// A scalar read: the one value the subquery selects, NULL if no row.
    Scalar,
    /// `[NOT] EXISTS`.
    Exists { not: bool },
    /// `lhs [NOT] IN (subquery)`. `lhs` belongs to the OUTER scope and is
    /// evaluated per outer row against that row's inner result.
    In { lhs: tidb_ast::Expr, not: bool },
    /// `lhs <op> ANY|ALL (subquery)`.
    Compare {
        op: tidb_ast::BinaryOp,
        lhs: tidb_ast::Expr,
        all: bool,
    },
}

/// A correlated subquery found in an outer expression: the subquery itself and
/// what its result is asked for.
pub(crate) struct CorrelatedSubquery {
    pub(crate) select: tidb_ast::SelectStmt,
    pub(crate) kind: SubqueryKind,
    pub(crate) columns: Vec<Vec<String>>,
}

/// Whether `expr` references a column of the OUTER scope, which is what makes
/// a subquery correlated (Go's `ExtractCorrelatedCols4LogicalPlan`).
///
/// A reference is correlated when the inner query's own `FROM` cannot resolve
/// it but the outer scope can -- the same two-scope test Go's name resolver
/// applies when it binds a column to an outer plan's schema.
/// [`collect_correlated_columns`], widened to a `QueryStmt`: a set operation's
/// correlated columns are the union of what each of its terms references,
/// since every term is re-run per outer row exactly like a lone `SELECT` is.
/// A statement-level `ORDER BY`/`LIMIT` names an output column or position
/// (see `sort_rows_by_output`), never an outer one, so it contributes nothing
/// here.
pub(crate) fn collect_correlated_columns_query(
    query: &QueryStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    found: &mut Vec<Vec<String>>,
    ctx: &crate::StmtContext,
) {
    match query {
        QueryStmt::Select(select) => {
            collect_correlated_columns(select, outer, catalog, current_db, found, ctx)
        }
        QueryStmt::SetOpr(set_opr) => {
            for term in &set_opr.terms {
                match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => {
                        collect_correlated_columns(select, outer, catalog, current_db, found, ctx)
                    }
                    tidb_ast::SetOprTermBody::Nested(nested) => collect_correlated_columns_query(
                        &QueryStmt::SetOpr(nested.clone()),
                        outer,
                        catalog,
                        current_db,
                        found,
                        ctx,
                    ),
                }
            }
        }
    }
}

fn collect_correlated_columns(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    found: &mut Vec<Vec<String>>,
    ctx: &crate::StmtContext,
) {
    let inner = match &select.from {
        None => FromScope::default(),
        Some(join) => match build_join(
            join,
            catalog,
            current_db,
            ctx,
            None,
            None,
            crate::driver::leaf_demand::FromDemand::none(),
            &tidb_planner::physical_property::PhysicalProperty::default(),
        ) {
            Ok((_, scope, _)) => scope,
            // An unresolvable inner FROM is reported by the inner run itself.
            Err(_) => FromScope::default(),
        },
    };
    let mut visit = |expr: &tidb_ast::Expr| {
        collect_outer_columns(expr, &inner, outer, found);
    };
    for field in select.fields.fields() {
        if let SelectField::Expr { expr, .. } = field {
            visit(expr);
        }
    }
    if let Some(where_clause) = &select.where_clause {
        visit(where_clause);
    }
    if let Some(having) = &select.having {
        visit(having);
    }
    for item in &select.group_by {
        visit(&item.expr);
    }
    for item in &select.order_by {
        visit(&item.expr);
    }
}

/// Records every column reference in `expr` that the inner scope cannot
/// resolve but the outer scope can.
fn collect_outer_columns(
    expr: &tidb_ast::Expr,
    inner: &FromScope,
    outer: &FromScope,
    found: &mut Vec<Vec<String>>,
) {
    use tidb_ast::Expr;
    match expr {
        Expr::Column(path) => {
            let inner_resolver = ScopeResolver { scope: inner };
            let outer_resolver = ScopeResolver { scope: outer };
            if inner_resolver.resolve(path).is_none()
                && outer_resolver.resolve(path).is_some()
                && !found.contains(path)
            {
                found.push(path.clone());
            }
        }
        Expr::Paren(inner_expr)
        | Expr::Unary(_, inner_expr)
        | Expr::Is {
            expr: inner_expr, ..
        } => {
            collect_outer_columns(inner_expr, inner, outer, found);
        }
        Expr::Binary(_, lhs, rhs) => {
            collect_outer_columns(lhs, inner, outer, found);
            collect_outer_columns(rhs, inner, outer, found);
        }
        Expr::In { expr, list, .. } => {
            collect_outer_columns(expr, inner, outer, found);
            for item in list {
                collect_outer_columns(item, inner, outer, found);
            }
        }
        Expr::Aggregate { args, .. } => {
            for arg in args {
                collect_outer_columns(arg, inner, outer, found);
            }
        }
        _ => {}
    }
}

/// Replaces each correlated column reference with the literal for the outer
/// row's value, which is this port's equivalent of Go's apply loop writing
/// `*col.Data` before re-running the inner plan.
fn bind_correlated_columns(
    expr: &tidb_ast::Expr,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match expr {
        Expr::Column(path) => match bindings.iter().find(|(bound, _)| paths_match(bound, path)) {
            Some((_, value)) => datum_to_literal(value)?,
            None => expr.clone(),
        },
        Expr::Paren(inner) => Expr::Paren(Box::new(bind_correlated_columns(inner, bindings)?)),
        Expr::Unary(op, inner) => {
            Expr::Unary(*op, Box::new(bind_correlated_columns(inner, bindings)?))
        }
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(bind_correlated_columns(expr, bindings)?),
            target: *target,
            not: *not,
        },
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(bind_correlated_columns(lhs, bindings)?),
            Box::new(bind_correlated_columns(rhs, bindings)?),
        ),
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(bind_correlated_columns(expr, bindings)?),
            list: list
                .iter()
                .map(|item| bind_correlated_columns(item, bindings))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        Expr::Aggregate {
            name,
            distinct,
            args,
        } => Expr::Aggregate {
            name: name.clone(),
            distinct: *distinct,
            args: args
                .iter()
                .map(|arg| bind_correlated_columns(arg, bindings))
                .collect::<Result<_, _>>()?,
        },
        other => other.clone(),
    })
}

/// Whether a bound path and a reference name the same column.
///
/// The comparison is the WHOLE path, because every binding was recorded from
/// the very references it is substituted into ([`collect_outer_columns`] pushes
/// each path as written), so an unqualified outer reference is bound under its
/// unqualified path and needs no suffix rule. Matching on the last name alone
/// would instead bind the INNER query's own same-named column: in
/// `SELECT id FROM emp WHERE emp.dept_id = dept.id AND emp.id = 10`, the outer
/// binding for `dept.id` would swallow `emp.id` and the selected `id`, and the
/// subquery would return NULL for every outer row.
fn paths_match(bound: &[String], candidate: &[String]) -> bool {
    bound.len() == candidate.len()
        && bound
            .iter()
            .zip(candidate)
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Substitutes `bindings` for the correlated column references in every clause
/// of `select`.
fn bind_subquery_columns(
    select: &tidb_ast::SelectStmt,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut bound = select.clone();
    for field in bound.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = bind_correlated_columns(expr, bindings)?;
        }
    }
    if let Some(where_clause) = &bound.where_clause {
        bound.where_clause = Some(bind_correlated_columns(where_clause, bindings)?);
    }
    if let Some(having) = &bound.having {
        bound.having = Some(bind_correlated_columns(having, bindings)?);
    }
    for item in &mut bound.group_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    for item in &mut bound.order_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    Ok(bound)
}

/// [`bind_subquery_columns`], widened to a `QueryStmt`: every term of a set
/// operation gets the same substitution, since each is re-run per outer row.
pub(crate) fn bind_subquery_columns_query(
    query: &QueryStmt,
    bindings: &[(Vec<String>, Datum)],
) -> Result<QueryStmt, DriverError> {
    Ok(match query {
        QueryStmt::Select(select) => {
            QueryStmt::Select(Box::new(bind_subquery_columns(select, bindings)?))
        }
        QueryStmt::SetOpr(set_opr) => {
            let mut bound = (**set_opr).clone();
            for term in &mut bound.terms {
                term.body = match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => tidb_ast::SetOprTermBody::Select(
                        Box::new(bind_subquery_columns(select, bindings)?),
                    ),
                    tidb_ast::SetOprTermBody::Nested(nested) => {
                        let QueryStmt::SetOpr(nested) = bind_subquery_columns_query(
                            &QueryStmt::SetOpr(nested.clone()),
                            bindings,
                        )?
                        else {
                            unreachable!("SetOpr input binds to SetOpr output")
                        };
                        tidb_ast::SetOprTermBody::Nested(nested)
                    }
                };
            }
            QueryStmt::SetOpr(Box::new(bound))
        }
    })
}

/// Resolves the outer-row indexes represented by correlated column paths.
pub(crate) fn correlated_path_indices(
    paths: &[Vec<String>],
    outer_scope: &FromScope,
) -> Result<Vec<usize>, DriverError> {
    paths
        .iter()
        .map(|path| {
            let resolver = ScopeResolver { scope: outer_scope };
            let (index, _, _) = resolver
                .resolve(path)
                .ok_or(DriverError::unsupported("unresolved correlated column"))?;
            Ok(index)
        })
        .collect()
}

/// Resolves the cache-key columns for a correlated subquery.
pub(crate) fn correlated_column_indices(
    correlated: &CorrelatedSubquery,
    outer_scope: &FromScope,
) -> Result<Vec<usize>, DriverError> {
    correlated
        .columns
        .iter()
        .map(|path| {
            let resolver = ScopeResolver { scope: outer_scope };
            // Aggregation output is one flat row with no table qualifiers,
            // so a qualified correlated reference falls back to its final
            // name there. Plain source scopes resolve the qualified path.
            let (index, _, _) = resolver
                .resolve(path)
                .or_else(|| {
                    let name = path.last()?;
                    resolver.resolve(std::slice::from_ref(name))
                })
                .ok_or(DriverError::unsupported("unresolved correlated column"))?;
            Ok(index)
        })
        .collect()
}

/// Binds every correlated column in `select` and runs it for one outer row.
pub(crate) fn run_correlated_subquery(
    correlated: &CorrelatedSubquery,
    outer_values: &[Datum],
    outer_scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let indices = correlated_column_indices(correlated, outer_scope)?;
    let mut bindings = Vec::with_capacity(correlated.columns.len());
    for (path, index) in correlated.columns.iter().zip(indices) {
        let value = outer_values
            .get(index)
            .cloned()
            .ok_or(DriverError::unsupported("correlated column out of range"))?;
        bindings.push((path.clone(), value));
    }

    let bound = bind_subquery_columns(&correlated.select, &bindings)?;
    let (_, rows) = run_select_stmt(&bound, catalog, current_db, ctx)?;
    match &correlated.kind {
        // EXISTS folds to 1/0 per outer row.
        SubqueryKind::Exists { not } => Ok(Datum::Int(i64::from(!rows.is_empty() != *not))),
        SubqueryKind::Scalar => match rows.len() {
            0 => Ok(Datum::Null),
            1 => {
                let [value] = rows[0].as_slice() else {
                    return Err(DriverError::unsupported(
                        "a scalar subquery selecting several columns is not supported yet",
                    ));
                };
                Ok(value.clone())
            }
            _ => Err(DriverError::SubqueryReturnsMoreThanOneRow),
        },
        // The semi-join shapes: this outer row's inner result becomes a value
        // list, and the test is evaluated over it exactly as the uncorrelated
        // fold evaluates its own folded list -- same `IN`, same comparisons,
        // so the same three-valued answers.
        SubqueryKind::In { lhs, not } => {
            let list = subquery_value_list(
                &rows,
                "an IN subquery selecting several columns is not supported yet",
            )?;
            let test = in_list_expr(lhs.clone(), list, *not);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
        SubqueryKind::Compare { op, lhs, all } => {
            let list = subquery_value_list(
                &rows,
                "an ANY/ALL subquery selecting several columns is not supported yet",
            )?;
            let test = any_all_expr(*op, lhs.clone(), *all, list);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
    }
}

/// A subquery result's single column, as the literals a value list needs.
fn subquery_value_list(
    rows: &[Vec<Datum>],
    several_columns: &'static str,
) -> Result<Vec<tidb_ast::Expr>, DriverError> {
    let mut list = Vec::with_capacity(rows.len());
    for row in rows {
        let [value] = row.as_slice() else {
            return Err(DriverError::unsupported(several_columns));
        };
        list.push(datum_to_literal(value)?);
    }
    Ok(list)
}

/// `lhs [NOT] IN (list)`, with the empty list written as the constant it is.
///
/// `x IN ()` is not sayable in SQL: an empty subquery result makes `IN` false
/// and `NOT IN` true for every x INCLUDING NULL, because MySQL evaluates the
/// semi join, which finds no row to match. The non-empty case keeps the
/// ordinary `IN`, whose NULL rules are the three-valued ones (an unmatched x
/// against a list holding NULL is NULL, not false).
fn in_list_expr(lhs: tidb_ast::Expr, list: Vec<tidb_ast::Expr>, not: bool) -> tidb_ast::Expr {
    if list.is_empty() {
        return tidb_ast::Expr::Int(i64::from(not).to_string());
    }
    tidb_ast::Expr::In {
        expr: Box::new(lhs),
        list,
        not,
    }
}

/// `lhs <op> ANY|ALL (list)` as the OR/AND chain it is defined to be.
///
/// Go's `buildSemiApply` for a comparison subquery builds the same disjunction
/// (`ANY`) or conjunction (`ALL`) of per-value comparisons, which is where the
/// three-valued behaviour comes from: `20 > ANY (25, NULL)` is
/// `false OR NULL` = NULL, while `20 > ALL (25, NULL)` is `false AND NULL` =
/// false. An empty list has no comparison at all, so `ALL` is vacuously TRUE
/// and `ANY` is FALSE -- both for a NULL `lhs` too.
fn any_all_expr(
    op: tidb_ast::BinaryOp,
    lhs: tidb_ast::Expr,
    all: bool,
    list: Vec<tidb_ast::Expr>,
) -> tidb_ast::Expr {
    use tidb_ast::{BinaryOp, Expr};
    let compare = |value: Expr| Expr::Binary(op, Box::new(lhs.clone()), Box::new(value));
    let mut values = list.into_iter();
    let Some(first) = values.next() else {
        return Expr::Int(i64::from(all).to_string());
    };
    let combine = if all {
        BinaryOp::LogicAnd
    } else {
        BinaryOp::LogicOr
    };
    values.fold(compare(first), |acc, value| {
        Expr::Binary(combine, Box::new(acc), Box::new(compare(value)))
    })
}

/// Evaluates an expression over the OUTER scope's columns for one outer row.
///
/// The semi-join folds keep their left operand in the outer scope rather than
/// binding it to a literal, so the comparison runs through the very same
/// expression evaluator the uncorrelated path uses.
fn eval_expr_on_row(
    expr: &tidb_ast::Expr,
    scope: &FromScope,
    values: &[Datum],
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let types: Vec<FieldType> = scope.column_list().into_iter().map(|(_, ft)| ft).collect();
    let rewritten = rewrite_expr_resolved(expr, &ScopeResolver { scope })
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    let chunk = row_chunk(values, &types)?;
    rewritten
        .eval(ctx, chunk.get_row(0))
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))
}

/// Finds the one correlated subquery in `expr`, replacing it with a reference
/// to the column an [`ApplyExec`] will append at `index`.
///
/// Go's rewriter does the same substitution: after building the Apply, the
/// subquery expression becomes the Apply schema's last column.
pub(crate) fn extract_correlated_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    index: usize,
    found: &mut Option<CorrelatedSubquery>,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // The synthetic name the appended column answers to.
    let placeholder = |index: usize| Expr::Column(vec![format!("__apply_{index}")]);
    Ok(match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                return Err(DriverError::unsupported(
                    "set-operation subqueries are not supported yet",
                ));
            };
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                // Uncorrelated: the folding pass handles it.
                return Ok(expr.clone());
            }
            if found.is_some() {
                return Err(DriverError::unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            let kind = match expr {
                Expr::Exists { not, .. } => SubqueryKind::Exists { not: *not },
                _ => SubqueryKind::Scalar,
            };
            *found = Some(CorrelatedSubquery {
                select: (**select).clone(),
                kind,
                columns,
            });
            placeholder(index)
        }
        // Go turns a correlated IN / ANY / ALL into a semi join; the Apply
        // here answers the same question one outer row at a time, with the
        // tested left operand staying in the outer expression.
        Expr::InSubquery {
            expr: lhs,
            subquery,
            not,
        } => {
            let select = subquery_select(subquery)?;
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                return Ok(expr.clone());
            }
            if found.is_some() || expr_has_subquery(lhs) {
                return Err(DriverError::unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            *found = Some(CorrelatedSubquery {
                select: select.clone(),
                kind: SubqueryKind::In {
                    lhs: (**lhs).clone(),
                    not: *not,
                },
                columns,
            });
            placeholder(index)
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } => {
            let select = subquery_select(subquery)?;
            let mut columns = Vec::new();
            collect_correlated_columns(select, outer, catalog, current_db, &mut columns, ctx);
            if columns.is_empty() {
                return Ok(expr.clone());
            }
            if found.is_some() || expr_has_subquery(left) {
                return Err(DriverError::unsupported(
                    "more than one correlated subquery in an expression is not supported yet",
                ));
            }
            *found = Some(CorrelatedSubquery {
                select: select.clone(),
                kind: SubqueryKind::Compare {
                    op: *op,
                    lhs: (**left).clone(),
                    all: *all,
                },
                columns,
            });
            placeholder(index)
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(extract_correlated_subquery(
            inner, outer, catalog, current_db, index, found, ctx,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(extract_correlated_subquery(
                inner, outer, catalog, current_db, index, found, ctx,
            )?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(extract_correlated_subquery(
                expr, outer, catalog, current_db, index, found, ctx,
            )?),
            target: *target,
            not: *not,
        },
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(extract_correlated_subquery(
                lhs, outer, catalog, current_db, index, found, ctx,
            )?),
            Box::new(extract_correlated_subquery(
                rhs, outer, catalog, current_db, index, found, ctx,
            )?),
        ),
        other => other.clone(),
    })
}

/// The `SELECT` a subquery carries, rejecting the set-operation body.
fn subquery_select(query: &tidb_ast::QueryStmt) -> Result<&tidb_ast::SelectStmt, DriverError> {
    match query {
        tidb_ast::QueryStmt::Select(select) => Ok(select),
        _ => Err(DriverError::unsupported(
            "set-operation subqueries are not supported yet",
        )),
    }
}

/// The scope a subquery inside `select` sees as its OUTER scope: `select`'s
/// own `FROM` tables. An unresolvable `FROM` yields an empty scope, and the
/// error surfaces when the query itself is built.
pub(crate) fn select_outer_scope(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> FromScope {
    let empty = || FromScope {
        zone: ctx.session_zone(),
        tidb_info_len: ctx.tidb_info_len(),
        ..FromScope::default()
    };
    match &select.from {
        None => empty(),
        Some(join) => match build_join(
            join,
            catalog,
            current_db,
            ctx,
            None,
            None,
            crate::driver::leaf_demand::FromDemand::none(),
            &tidb_planner::physical_property::PhysicalProperty::default(),
        ) {
            Ok((_, scope, _)) => scope,
            Err(_) => empty(),
        },
    }
}

/// Whether any clause of `select` contains a subquery the folding pass should
/// run on. A correlated subquery in the `WHERE` is left for the Apply path.
pub(crate) fn select_has_uncorrelated_subquery(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    if let Some(where_clause) = &select.where_clause {
        if expr_has_subquery(where_clause) {
            let outer = select_outer_scope(select, catalog, current_db, ctx);
            let mut found = None;
            // A correlated WHERE subquery is the Apply path's job.
            if extract_correlated_subquery(
                where_clause,
                &outer,
                catalog,
                current_db,
                0,
                &mut found,
                ctx,
            )
            .is_ok()
                && found.is_some()
            {
                return false;
            }
        }
    }
    select_has_subquery(select)
}

/// Whether any clause of `select` contains a subquery, so the fold pass runs
/// only when it has something to do.
fn select_has_subquery(select: &tidb_ast::SelectStmt) -> bool {
    let fields = select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => expr_has_subquery(expr),
        SelectField::Wildcard(_) => false,
    });
    fields
        || select.where_clause.as_ref().is_some_and(expr_has_subquery)
        || select.having.as_ref().is_some_and(expr_has_subquery)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
        || select
            .group_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
}

/// Whether `expr` contains a subquery in a position the fold pass walks.
pub(crate) fn expr_has_subquery(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            expr_has_subquery(inner)
        }
        Expr::Binary(_, lhs, rhs) => expr_has_subquery(lhs) || expr_has_subquery(rhs),
        Expr::In { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        // The forms below must stay in step with `fold_subqueries`' own walk:
        // this predicate is the GATE that decides whether the fold pass runs
        // at all, so a form the fold could handle but this cannot is a
        // subquery that never gets folded and fails to plan instead.
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().is_some_and(expr_has_subquery)
                || when_clauses.iter().any(|(condition, result)| {
                    expr_has_subquery(condition) || expr_has_subquery(result)
                })
                || else_clause.as_deref().is_some_and(expr_has_subquery)
        }
        Expr::Func { args, .. } | Expr::Aggregate { args, .. } => {
            args.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}

/// Folds every subquery in `select`'s clauses, returning the rewritten copy.
pub(crate) fn fold_select_subqueries(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut folded = select.clone();
    for field in folded.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = fold_subqueries(expr, outer, catalog, current_db, ctx)?;
        }
    }
    if let Some(where_clause) = &folded.where_clause {
        folded.where_clause = Some(fold_subqueries(
            where_clause,
            outer,
            catalog,
            current_db,
            ctx,
        )?);
    }
    if let Some(having) = &folded.having {
        folded.having = Some(fold_subqueries(having, outer, catalog, current_db, ctx)?);
    }
    for item in &mut folded.order_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    for item in &mut folded.group_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    Ok(folded)
}

/// Replaces every uncorrelated subquery in `expr` with the value it produces.
///
/// This is Go's `handleScalarSubquery` path for a subquery with no correlated
/// columns: the subquery is planned and run on the spot
/// (`EvalSubqueryFirstRow`) and its result folded into a `Constant`, so the
/// outer statement plans against ordinary literals. Go's `buildMaxOneRow`
/// wrapper is the "more than one row" check below; a subquery producing no
/// rows yields NULL.
///
/// `EXISTS` folds to 1 or 0, `x IN (subquery)` folds to `x IN (values)` and
/// `x <op> ANY|ALL (subquery)` to the OR/AND chain of comparisons, all of
/// which evaluate identically for an uncorrelated subquery -- including the
/// NULL rules, since the folded list is compared by the same code.
///
/// DEFERRED (documented): CORRELATED subqueries, which Go turns into an Apply
/// operator rather than folding, and which this leaves for the Apply path
/// rather than silently evaluating the inner query against the wrong row; and
/// row constructors (a subquery selecting several columns).
fn fold_subqueries(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // A subquery reading the OUTER query's columns has no single value to fold
    // to: it is the Apply path's job, one run per outer row.
    if let Expr::Subquery(query)
    | Expr::Exists {
        subquery: query, ..
    }
    | Expr::InSubquery {
        subquery: query, ..
    }
    | Expr::CompareSubquery {
        subquery: query, ..
    } = expr
    {
        let mut columns = Vec::new();
        collect_correlated_columns_query(query, outer, catalog, current_db, &mut columns, ctx);
        if !columns.is_empty() {
            return Ok(expr.clone());
        }
    }
    Ok(match expr {
        Expr::Subquery(query) => {
            let rows = run_subquery(query, catalog, current_db, ctx)?;
            match rows.len() {
                // Go: a scalar subquery with no rows is NULL.
                0 => Expr::Null,
                1 => {
                    let row = &rows[0];
                    let [value] = row.as_slice() else {
                        return Err(DriverError::unsupported(
                            "a scalar subquery selecting several columns is not supported yet",
                        ));
                    };
                    datum_to_literal(value)?
                }
                // Go's buildMaxOneRow raises ER_SUBQUERY_NO_1_ROW here.
                _ => return Err(DriverError::SubqueryReturnsMoreThanOneRow),
            }
        }
        Expr::Exists { subquery, not } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let exists = !rows.is_empty();
            Expr::Int(i64::from(exists != *not).to_string())
        }
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(
                &rows,
                "an IN subquery selecting several columns is not supported yet",
            )?;
            in_list_expr(
                fold_subqueries(expr, outer, catalog, current_db, ctx)?,
                list,
                *not,
            )
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(
                &rows,
                "an ANY/ALL subquery selecting several columns is not supported yet",
            )?;
            any_all_expr(
                *op,
                fold_subqueries(left, outer, catalog, current_db, ctx)?,
                *all,
                list,
            )
        }
        // Walk the child-bearing forms. A form missing from this list carries
        // its subquery past the fold and into the rewriter, which knows no
        // subqueries at all: the statement then FAILS to plan (never answers
        // wrongly), which is what makes adding a form here purely a matter of
        // reach rather than of correctness.
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => Expr::Case {
            value: match value {
                Some(value) => Some(Box::new(fold_subqueries(
                    value, outer, catalog, current_db, ctx,
                )?)),
                None => None,
            },
            when_clauses: when_clauses
                .iter()
                .map(|(condition, result)| {
                    Ok((
                        fold_subqueries(condition, outer, catalog, current_db, ctx)?,
                        fold_subqueries(result, outer, catalog, current_db, ctx)?,
                    ))
                })
                .collect::<Result<_, DriverError>>()?,
            else_clause: match else_clause {
                Some(else_clause) => Some(Box::new(fold_subqueries(
                    else_clause,
                    outer,
                    catalog,
                    current_db,
                    ctx,
                )?)),
                None => None,
            },
        },
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| fold_subqueries(arg, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        // An aggregate's own argument: an UNCORRELATED subquery in it is a
        // constant and folds here, which is the only reason `SUM((SELECT MAX(id)
        // FROM d))` can run at all -- a CORRELATED one has to run once per
        // SOURCE row, below the aggregation, and `driver::agg_build` refuses it
        // by name after this fold has had its chance.
        Expr::Aggregate {
            name,
            distinct,
            args,
        } => Expr::Aggregate {
            name: name.clone(),
            distinct: *distinct,
            args: args
                .iter()
                .map(|arg| fold_subqueries(arg, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
        },
        Expr::Paren(inner) => Expr::Paren(Box::new(fold_subqueries(
            inner, outer, catalog, current_db, ctx,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(fold_subqueries(inner, outer, catalog, current_db, ctx)?),
        ),
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(fold_subqueries(lhs, outer, catalog, current_db, ctx)?),
            Box::new(fold_subqueries(rhs, outer, catalog, current_db, ctx)?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            target: *target,
            not: *not,
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            list: list
                .iter()
                .map(|item| fold_subqueries(item, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        other => other.clone(),
    })
}

/// Runs an uncorrelated subquery against the catalog.
///
/// A correlated subquery references a column of the OUTER query, which this
/// resolver cannot see -- so it fails to resolve here and the error surfaces
/// rather than the subquery being evaluated against the wrong scope.
fn run_subquery(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_query_stmt(query, catalog, current_db, ctx).map(|(_, rows)| rows)
}

/// Where one select field of an aggregate query reads its value from.
pub(crate) enum OutputSlot {
    /// An aggregation output column, by index.
    Agg(usize),
    /// An expression over the aggregation's (+ Apply's) output columns, by
    /// index into `post_agg_exprs` -- a select field that CONTAINS a
    /// correlated subquery alongside aggregates/columns, e.g.
    /// `SUM(v) + (SELECT ...)`.
    Expr(usize),
    /// The column the n-th window call appends above the aggregation.
    Window(usize),
}

/// Extracts the one correlated subquery in a post-aggregation expression (a
/// select field, `HAVING`, or an `ORDER BY` item), hoists any aggregate calls
/// left in the remainder into `agg_funcs`/`names`/`types`, and returns the
/// resulting expression: aggregates and grouped columns become output column
/// references (Go's `havingWindowAndOrderbyExprResolver`), and the subquery
/// becomes a `__apply_N` placeholder column reference that the caller's
/// Apply (built once every correlated subquery in the statement is known)
/// makes real. `EXISTS`, `IN` and `ANY`/`ALL` ride the same placeholder,
/// because the Apply appends whatever [`run_correlated_subquery`] folds.
///
/// Returns `(expr, true)` when a correlated subquery was found and hoisted,
/// `(expr, false)` otherwise (uncorrelated, or no subquery at all).
#[allow(clippy::too_many_arguments)]
pub(crate) fn extract_and_hoist_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    applies: &mut Vec<(CorrelatedSubquery, String, FieldType)>,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_exprs: &[String],
    resolver: &ScopeResolver<'_>,
    ctx: &crate::StmtContext,
) -> Result<(tidb_ast::Expr, bool), DriverError> {
    // No subquery anywhere in the expression, so there is nothing to hoist
    // out of the way of a per-group Apply: the caller decides how (or
    // whether) to run the aggregate hoist itself, exactly as it did before
    // this function existed.
    if !expr_has_subquery(expr) {
        return Ok((expr.clone(), false));
    }
    let index = applies.len();
    let mut found = None;
    let rewritten =
        extract_correlated_subquery(expr, outer, catalog, current_db, index, &mut found, ctx)?;
    let Some(correlated) = found else {
        // Uncorrelated, or no subquery reachable through this expression
        // shape: left for the caller / the fold pass / the rewriter's own
        // error.
        return Ok((rewritten, false));
    };
    let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
        subquery_result_type(&correlated, outer, catalog, current_db, ctx)
            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
    } else {
        FieldType::new(FieldTypeCode::LongLong)
    };
    let hoisted = substitute_aggregates(
        &rewritten,
        agg_funcs,
        names,
        types,
        grouping_specs,
        group_by_exprs,
        resolver,
        tidb_expr::Columns::div_precision_increment(ctx),
    )?;
    applies.push((correlated, format!("__apply_{index}"), value_type));
    Ok((hoisted, true))
}
