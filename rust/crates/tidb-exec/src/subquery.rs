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

//! Subquery resolution: rewrites every subquery in an expression tree to the
//! literal it evaluates to, so the caller (WHERE, projection, HAVING, an
//! aggregate's own argument) can evaluate the resulting subquery-free
//! expression normally. Called from `crate::select` and `crate::aggregate`.

use tidb_ast::{BinaryOp, CastExpr, Expr, OrderItem, QueryStmt, SelectStmt};
use tidb_datatype::Datum;
use tidb_expr::Columns;

use crate::literal::value_to_literal;
use crate::{Database, ExecError, Row};

/// Returns true when an expression is provably free of subqueries. Callers may
/// then evaluate the original parsed expression rather than rebuilding it for
/// every input row, preserving source-AST identity for stateful builtins such
/// as constant `RAND(N)`. Unknown forms intentionally return false and keep
/// the existing resolver path: a conservative false costs a clone, while a
/// false positive would skip required subquery work.
pub(crate) fn is_subquery_free(e: &Expr) -> bool {
    match e {
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::Null
        | Expr::Bool(_)
        | Expr::Column(_)
        | Expr::UserVar(_)
        | Expr::SysVar { .. } => true,
        Expr::Unary(_, expr) | Expr::Paren(expr) => is_subquery_free(expr),
        Expr::Binary(_, left, right) => is_subquery_free(left) && is_subquery_free(right),
        Expr::Assign { value, .. }
        | Expr::WeightString { expr: value, .. }
        | Expr::Collate { expr: value, .. }
        | Expr::ConvertUsing { expr: value, .. }
        | Expr::Interval { value, .. }
        | Expr::Extract { value, .. }
        | Expr::GetFormat { expr: value, .. }
        | Expr::Is { expr: value, .. } => is_subquery_free(value),
        Expr::Trim { expr, remstr, .. } => {
            is_subquery_free(expr) && remstr.as_deref().is_none_or(is_subquery_free)
        }
        Expr::Position { substr, str }
        | Expr::Regexp {
            expr: substr,
            pattern: str,
            ..
        } => is_subquery_free(substr) && is_subquery_free(str),
        Expr::Func { args, .. }
        | Expr::GenericFuncCall { args, .. }
        | Expr::Row(args)
        | Expr::Aggregate { args, .. } => args.iter().all(is_subquery_free),
        Expr::GroupConcat { args, order_by, .. } => {
            args.iter().all(is_subquery_free)
                && order_by.iter().all(|item| is_subquery_free(&item.expr))
        }
        Expr::In { expr, list, .. } => is_subquery_free(expr) && list.iter().all(is_subquery_free),
        Expr::Between {
            expr, low, high, ..
        } => is_subquery_free(expr) && is_subquery_free(low) && is_subquery_free(high),
        Expr::Like { expr, pattern, .. }
        | Expr::MemberOf {
            expr,
            array: pattern,
        } => is_subquery_free(expr) && is_subquery_free(pattern),
        Expr::Cast(cast) => is_subquery_free(&cast.expr),
        Expr::MatchAgainst { against, .. } => is_subquery_free(against),
        Expr::TimestampAdd { interval, expr, .. } => {
            is_subquery_free(interval) && is_subquery_free(expr)
        }
        Expr::TimestampDiff { expr1, expr2, .. } => {
            is_subquery_free(expr1) && is_subquery_free(expr2)
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().is_none_or(is_subquery_free)
                && when_clauses
                    .iter()
                    .all(|(when, then)| is_subquery_free(when) && is_subquery_free(then))
                && else_clause.as_deref().is_none_or(is_subquery_free)
        }
        _ => false,
    }
}

impl Database {
    /// Rewrites an expression for the current outer row, replacing every
    /// subquery with the literal it evaluates to: a scalar `(SELECT ...)` becomes
    /// its single value, `x IN (SELECT ...)` becomes `x IN (<values>)`, and
    /// `[NOT] EXISTS (...)` becomes `1`/`0`. Non-subquery nodes are rebuilt
    /// structurally so nested subqueries are also resolved. `outer` is passed to
    /// each subquery so a correlated one resolves its outer-column references
    /// against the current row; this runs per row (see `select_rows`).
    pub(crate) fn resolve_subqueries(
        &self,
        e: &Expr,
        outer: &dyn Columns,
    ) -> Result<Expr, ExecError> {
        let rec = |x: &Expr| self.resolve_subqueries(x, outer);
        Ok(match e {
            Expr::Subquery(sel) => value_to_literal(self.scalar_subquery(sel, Some(outer))?),
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => {
                let resolved_expr = rec(expr)?;
                // A row-value operand (`(a, b) IN (subquery)`) needs
                // every subquery row's FULL column set, not just the
                // first, so each becomes its own `Expr::Row` list item
                // — `crate::func::eval_in_list` (via `tidb_expr::row`)
                // then does the element-wise comparison, the SAME
                // logic a literal row-value list (`(a,b) IN ((1,2))`)
                // already uses.
                let list = if matches!(resolved_expr, Expr::Row(_)) {
                    self.in_subquery_rows(subquery, Some(outer))?
                        .into_iter()
                        .map(|row| Expr::Row(row.into_iter().map(value_to_literal).collect()))
                        .collect()
                } else {
                    self.in_subquery_column(subquery, Some(outer))?
                        .into_iter()
                        .map(value_to_literal)
                        .collect()
                };
                Expr::In {
                    expr: Box::new(resolved_expr),
                    list,
                    not: *not,
                }
            }
            Expr::Exists { subquery, not } => {
                let exists = !self.in_subquery_rows(subquery, Some(outer))?.is_empty();
                value_to_literal(Datum::Int(i64::from(exists ^ *not)))
            }
            // Structural rebuild so nested subqueries are resolved too.
            Expr::Unary(op, x) => Expr::Unary(*op, Box::new(rec(x)?)),
            Expr::Binary(op, l, r) => Expr::Binary(*op, Box::new(rec(l)?), Box::new(rec(r)?)),
            Expr::Paren(x) => Expr::Paren(Box::new(rec(x)?)),
            Expr::Assign { name, value } => Expr::Assign {
                name: name.clone(),
                value: Box::new(rec(value)?),
            },
            Expr::Trim {
                expr,
                remstr,
                direction,
            } => Expr::Trim {
                expr: Box::new(rec(expr)?),
                remstr: remstr.as_deref().map(rec).transpose()?.map(Box::new),
                direction: *direction,
            },
            Expr::Position { substr, str } => Expr::Position {
                substr: Box::new(rec(substr)?),
                str: Box::new(rec(str)?),
            },
            Expr::WeightString { expr, as_type } => Expr::WeightString {
                expr: Box::new(rec(expr)?),
                as_type: *as_type,
            },
            Expr::Func { name, args } => Expr::Func {
                name: name.clone(),
                args: args.iter().map(rec).collect::<Result<_, _>>()?,
            },
            Expr::GenericFuncCall { schema, name, args } => Expr::GenericFuncCall {
                schema: schema.clone(),
                name: name.clone(),
                args: args.iter().map(rec).collect::<Result<_, _>>()?,
            },
            Expr::Row(values) => Expr::Row(values.iter().map(rec).collect::<Result<_, _>>()?),
            Expr::Aggregate {
                name,
                distinct,
                args,
            } => Expr::Aggregate {
                name: name.clone(),
                distinct: *distinct,
                args: args.iter().map(rec).collect::<Result<_, _>>()?,
            },
            Expr::GroupConcat {
                distinct,
                args,
                order_by,
                separator,
            } => Expr::GroupConcat {
                distinct: *distinct,
                args: args.iter().map(rec).collect::<Result<_, _>>()?,
                order_by: order_by
                    .iter()
                    .map(|item| {
                        Ok(OrderItem {
                            expr: rec(&item.expr)?,
                            desc: item.desc,
                        })
                    })
                    .collect::<Result<_, ExecError>>()?,
                separator: separator.clone(),
            },
            Expr::In { expr, list, not } => Expr::In {
                expr: Box::new(rec(expr)?),
                list: list.iter().map(rec).collect::<Result<_, _>>()?,
                not: *not,
            },
            Expr::Between {
                expr,
                low,
                high,
                not,
            } => Expr::Between {
                expr: Box::new(rec(expr)?),
                low: Box::new(rec(low)?),
                high: Box::new(rec(high)?),
                not: *not,
            },
            Expr::Like {
                expr,
                pattern,
                not,
                escape,
            } => Expr::Like {
                expr: Box::new(rec(expr)?),
                pattern: Box::new(rec(pattern)?),
                not: *not,
                escape: *escape,
            },
            Expr::Regexp { expr, pattern, not } => Expr::Regexp {
                expr: Box::new(rec(expr)?),
                pattern: Box::new(rec(pattern)?),
                not: *not,
            },
            Expr::Collate { expr, collation } => Expr::Collate {
                expr: Box::new(rec(expr)?),
                collation: collation.clone(),
            },
            Expr::Cast(cast) => Expr::Cast(CastExpr {
                expr: Box::new(rec(&cast.expr)?),
                cast_type: cast.cast_type.clone(),
                style: cast.style,
                array: cast.array,
            }),
            Expr::ConvertUsing { expr, charset } => Expr::ConvertUsing {
                expr: Box::new(rec(expr)?),
                charset: charset.clone(),
            },
            Expr::MatchAgainst {
                columns,
                against,
                modifier,
            } => Expr::MatchAgainst {
                columns: columns.clone(),
                against: Box::new(rec(against)?),
                modifier: *modifier,
            },
            Expr::MemberOf { expr, array } => Expr::MemberOf {
                expr: Box::new(rec(expr)?),
                array: Box::new(rec(array)?),
            },
            // `Expr::Interval`/`Expr::Extract` were ALSO missing from this
            // function (a genuine pre-existing gap, not introduced this
            // turn — found while adding the same-shaped `TimestampAdd`/
            // `TimestampDiff`/`GetFormat` below, per this project's own
            // "draw inferences about other cases from one instance"
            // practice): a subquery nested in either's own value position
            // (`EXTRACT(YEAR FROM (SELECT ...))`) would stay unresolved
            // and fail at evaluation.
            Expr::Interval { value, unit } => Expr::Interval {
                value: Box::new(rec(value)?),
                unit: unit.clone(),
            },
            Expr::Extract { unit, value } => Expr::Extract {
                unit: unit.clone(),
                value: Box::new(rec(value)?),
            },
            Expr::TimestampAdd {
                unit,
                interval,
                expr,
            } => Expr::TimestampAdd {
                unit: unit.clone(),
                interval: Box::new(rec(interval)?),
                expr: Box::new(rec(expr)?),
            },
            Expr::TimestampDiff { unit, expr1, expr2 } => Expr::TimestampDiff {
                unit: unit.clone(),
                expr1: Box::new(rec(expr1)?),
                expr2: Box::new(rec(expr2)?),
            },
            Expr::GetFormat { selector, expr } => Expr::GetFormat {
                selector: *selector,
                expr: Box::new(rec(expr)?),
            },
            Expr::Is { expr, target, not } => Expr::Is {
                expr: Box::new(rec(expr)?),
                target: *target,
                not: *not,
            },
            // `x <op> ALL (subquery)` desugars to `x<op>v1 AND x<op>v2 AND ...`
            // (empty list: vacuously TRUE); `ANY`/`SOME` to the `OR` chain
            // (empty list: FALSE). The three-valued fold, including a NULL
            // short-circuiting to NULL unless a FALSE (for ALL) or TRUE (for
            // ANY) already decided it, then falls out of the normal `AND`/`OR`
            // evaluation `eval_in` already implements — no new logic needed.
            Expr::CompareSubquery {
                op,
                left,
                all,
                subquery,
            } => {
                let l = rec(left)?;
                let fold_op = if *all {
                    BinaryOp::LogicAnd
                } else {
                    BinaryOp::LogicOr
                };
                let mut acc = Expr::Bool(*all);
                for v in self.subquery_column(subquery, Some(outer))? {
                    let cmp = Expr::Binary(*op, Box::new(l.clone()), Box::new(value_to_literal(v)));
                    acc = Expr::Binary(fold_op, Box::new(acc), Box::new(cmp));
                }
                acc
            }
            // Structural rebuild so a subquery anywhere inside a CASE
            // (the compare value, any WHEN condition or THEN result, or
            // the ELSE clause) is resolved too — otherwise it would fall
            // to the `other => other.clone()` leaf below, leaving nested
            // `Expr::Subquery`/`Expr::Exists`/... nodes unresolved and
            // unevaluable. Every branch is resolved regardless of which
            // one `eval_in`'s own lazy CASE evaluation ultimately takes
            // (this structural walk runs BEFORE that short-circuit
            // decision, so a subquery in a never-taken branch is still
            // executed here — a narrow, deliberate divergence from real
            // MySQL's true laziness, not attempted; see `tidb-expr`'s own
            // `Expr::Case` evaluation doc for the corresponding
            // short-circuit-vs-static-typing scope note).
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => Expr::Case {
                value: value.as_deref().map(rec).transpose()?.map(Box::new),
                when_clauses: when_clauses
                    .iter()
                    .map(|(cond, result)| Ok((rec(cond)?, rec(result)?)))
                    .collect::<Result<_, ExecError>>()?,
                else_clause: else_clause.as_deref().map(rec).transpose()?.map(Box::new),
            },
            // Leaves (no nested expression): clone as-is.
            other => other.clone(),
        })
    }

    /// Executes a scalar subquery: 0 rows is `NULL`, 1 row / 1 column is that
    /// value; more rows or columns is an error (as it is in MySQL). The query
    /// may be a plain SELECT or a set operation, matching the parser's
    /// source-owned `QueryStmt` envelope. `outer` carries the enclosing row
    /// for correlated references.
    fn scalar_subquery(
        &self,
        query: &QueryStmt,
        outer: Option<&dyn Columns>,
    ) -> Result<Datum, ExecError> {
        let rs = match query {
            QueryStmt::Select(sel) => self.select(sel, outer)?,
            QueryStmt::SetOpr(setopr) => self.setopr(setopr, outer)?,
        };
        match rs.rows.as_slice() {
            [] => Ok(Datum::Null),
            [row] if row.len() == 1 => Ok(row[0].clone()),
            [_] => Err(ExecError::Unsupported(
                "scalar subquery with multiple columns",
            )),
            _ => Err(ExecError::Unsupported(
                "scalar subquery returned multiple rows",
            )),
        }
    }

    /// Executes a subquery for `x <op> ANY|ALL (...)`, collecting its
    /// first column's values.
    fn subquery_column(
        &self,
        sel: &SelectStmt,
        outer: Option<&dyn Columns>,
    ) -> Result<Vec<Datum>, ExecError> {
        Self::first_column(self.select(sel, outer)?.rows)
    }

    /// Like [`Database::subquery_column`], but for `IN`'s own subquery,
    /// which — unlike every OTHER subquery position here — may ALSO be
    /// `UNION`/`EXCEPT`/`INTERSECT`-bodied (see
    /// `tidb_ast::Expr::InSubquery`'s own doc), hence the full `QueryStmt`
    /// rather than always a plain `SelectStmt`. `parse_predicate` only
    /// always constructs a `QueryStmt::Select` or `QueryStmt::SetOpr` here
    /// (mirroring `Parser::parse_select_or_setopr`'s own contract), so
    /// those are the only two reachable arms.
    fn in_subquery_column(
        &self,
        stmt: &QueryStmt,
        outer: Option<&dyn Columns>,
    ) -> Result<Vec<Datum>, ExecError> {
        Self::first_column(self.in_subquery_rows(stmt, outer)?)
    }

    /// Like [`Database::in_subquery_column`], but for a ROW-VALUE `IN`
    /// operand (`(a, b) IN (subquery)`) — captures every column of
    /// each subquery row, not just the first, so the caller can build
    /// a full `Expr::Row` list item per row (see
    /// `Database::resolve_subqueries`'s own `Expr::InSubquery` arm).
    /// Unlike `first_column`, this does NOT reject an empty row — an
    /// arity check happens later, at comparison time
    /// (`tidb_expr::row::row_compare`).
    fn in_subquery_rows(
        &self,
        stmt: &QueryStmt,
        outer: Option<&dyn Columns>,
    ) -> Result<Vec<Row>, ExecError> {
        Ok(match stmt {
            QueryStmt::Select(select) => self.select(select, outer)?.rows,
            QueryStmt::SetOpr(setopr) => self.setopr(setopr, outer)?.rows,
        })
    }

    fn first_column(rows: Vec<Row>) -> Result<Vec<Datum>, ExecError> {
        rows.iter()
            .map(|r| {
                r.first()
                    .cloned()
                    .ok_or(ExecError::Unsupported("empty subquery row"))
            })
            .collect()
    }
}
