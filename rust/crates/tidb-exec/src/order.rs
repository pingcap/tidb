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

//! `ORDER BY` / `LIMIT` support: resolving order items to sort keys, the
//! total order used to compare them, and truncating to a `LIMIT` window.
//! Free functions only — no `Database` methods — called from
//! `crate::select`, `crate::aggregate`, and `crate::setopr`.

use std::cmp::Ordering;

use tidb_ast::{CastExpr, Expr, Limit, OrderItem, SelectField};
use tidb_datatype::Datum;
use tidb_expr::eval;

use crate::{ExecError, Row};

/// If the expression is a positive integer literal `N` (or the boolean
/// literals `TRUE`/`FALSE`, which MySQL/TiDB treat as `1`/`0` in this
/// position too — confirmed via `gorun`: `GROUP BY true` groups by the
/// first select-list column exactly like `GROUP BY 1`), returns its 0-based
/// output-column index (`N-1`); any other expression returns `None`; a zero
/// position is an error. Used to resolve positional `ORDER BY`/`GROUP BY`
/// (against the select-list) AND `GROUP_CONCAT(... ORDER BY N ...)`'s own
/// positional item (against ITS OWN arg list instead — see
/// `crate::aggregate::Database::compute_group_concat`'s own doc), the SAME
/// `N`-referring-a-position semantics in a different list.
pub(crate) fn positional(expr: &Expr) -> Result<Option<usize>, ExecError> {
    let n: usize = match expr {
        Expr::Int(digits) => digits
            .parse()
            .map_err(|_| ExecError::Unsupported("GROUP BY/ORDER BY position"))?,
        Expr::Bool(b) => usize::from(*b),
        _ => return Ok(None),
    };
    n.checked_sub(1)
        .ok_or(ExecError::Unsupported("GROUP BY/ORDER BY position 0"))
        .map(Some)
}

/// Resolves a single `GROUP BY`/`ORDER BY` item's expression: a positional
/// item (see [`positional`]) resolves to the corresponding select-list
/// expression; a bare identifier matching exactly one select-list alias
/// resolves to that field's own expression (see [`resolve_alias`]); any
/// other item keeps its own expression (so ordering/grouping by a
/// non-selected column works).
pub(crate) fn resolve_by_item<'a>(
    expr: &'a Expr,
    fields: &'a [SelectField],
) -> Result<&'a Expr, ExecError> {
    match positional(expr)? {
        Some(pos) => match fields.get(pos) {
            Some(SelectField::Expr { expr, .. }) => Ok(expr),
            Some(SelectField::Wildcard(_)) => {
                Err(ExecError::Unsupported("GROUP BY/ORDER BY position over *"))
            }
            None => Err(ExecError::Unsupported(
                "GROUP BY/ORDER BY position out of range",
            )),
        },
        None => Ok(resolve_alias(expr, fields)),
    }
}

/// Resolves `ORDER BY` items to (expression, DESC) pairs for evaluation in the
/// input context. Used by the scan and aggregation paths.
pub(crate) fn resolve_order_keys<'a>(
    order_by: &'a [OrderItem],
    fields: &'a [SelectField],
) -> Result<Vec<(&'a Expr, bool)>, ExecError> {
    order_by
        .iter()
        .map(|item| Ok((resolve_by_item(&item.expr, fields)?, item.desc)))
        .collect()
}

/// Resolves a bare, unqualified identifier to the ONE select-list field
/// that explicitly aliases it (`expr AS name`, matched case-insensitively,
/// MySQL's default identifier collation) — confirmed via `gorun`: `SELECT
/// id AS x FROM t ORDER BY x`, `SELECT dept, COUNT(*) c ... GROUP BY dept
/// ORDER BY c` (including a window-function alias, e.g. `RANK() OVER (...)
/// rnk ... ORDER BY rnk`), and `SELECT dept AS x, COUNT(*) FROM t GROUP BY
/// x` (`crate::aggregate::Database::aggregate`'s own use, resolving the
/// `GROUP BY` clause itself) all resolve this way. A `GROUP BY` item that
/// resolves to an AGGREGATE expression (`SELECT dept, COUNT(*) AS c FROM t
/// GROUP BY c`) is a genuine `ERR`, confirmed via `gorun` — no special
/// rejection needed here, since grouping's own per-row `eval_in` call
/// naturally has no notion of `Expr::Aggregate` either. Only EXPLICIT
/// aliases are matched — an unaliased field's own bare column name is not
/// a candidate, and two-or-more fields sharing the same alias name fall
/// through unresolved (surfacing as an ordinary column-not-found error
/// downstream, which is still the correct ERR shape even if not by the same
/// path MySQL rejects it through). This is a deliberately narrower rule than
/// MySQL's full output-column-name resolution: `SELECT id, dept AS id FROM
/// t ORDER BY id` is a genuine `ERR` in real TiDB (ambiguous between the
/// real `id` column and the aliased one), which this simpler rule does not
/// reproduce — that would require tracking every field's IMPLICIT display
/// name too, not just explicit `AS` aliases.
pub(crate) fn resolve_alias<'a>(item: &'a Expr, fields: &'a [SelectField]) -> &'a Expr {
    let Expr::Column(path) = item else {
        return item;
    };
    let [name] = path.as_slice() else {
        return item;
    };
    let mut matches = fields.iter().filter_map(|f| match f {
        SelectField::Expr {
            expr,
            alias: Some(a),
        } if a.eq_ignore_ascii_case(name) => Some(expr),
        _ => None,
    });
    match (matches.next(), matches.next()) {
        (Some(expr), None) => expr,
        _ => item,
    }
}

/// Recursively resolves every bare, unqualified column reference anywhere in
/// `expr`'s tree to a matching select-list alias (see [`resolve_alias`]),
/// spliced back in as an owned clone. Used for `HAVING`, which — confirmed
/// via `gorun` — may reference a select-list alias ANYWHERE in its
/// expression (`HAVING c + 1 > 3`), unlike `ORDER BY`'s own items, which are
/// only ever resolved as a WHOLE top-level bare identifier
/// ([`resolve_order_keys`]); also unlike the `SELECT` list itself, where a
/// later field referencing an earlier field's own alias is a genuine `ERR`,
/// so no equivalent resolution is needed there. Does not recurse into an
/// aggregate's own argument (`Expr::Aggregate`/`Expr::GroupConcat`) or a
/// nested subquery's own body — the same scope boundary
/// `crate::aggregate::check_columns_pinned` already draws for those, left
/// alone here rather than resolved.
pub(crate) fn resolve_having_aliases(expr: &Expr, fields: &[SelectField]) -> Expr {
    match expr {
        Expr::Column(_) => resolve_alias(expr, fields).clone(),
        Expr::Unary(op, inner) => Expr::Unary(*op, Box::new(resolve_having_aliases(inner, fields))),
        Expr::Paren(inner) => Expr::Paren(Box::new(resolve_having_aliases(inner, fields))),
        Expr::Assign { name, value } => Expr::Assign {
            name: name.clone(),
            value: Box::new(resolve_having_aliases(value, fields)),
        },
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => Expr::Trim {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            remstr: remstr
                .as_deref()
                .map(|r| Box::new(resolve_having_aliases(r, fields))),
            direction: *direction,
        },
        Expr::Position { substr, str } => Expr::Position {
            substr: Box::new(resolve_having_aliases(substr, fields)),
            str: Box::new(resolve_having_aliases(str, fields)),
        },
        Expr::WeightString { expr, as_type } => Expr::WeightString {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            as_type: *as_type,
        },
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(resolve_having_aliases(left, fields)),
            Box::new(resolve_having_aliases(right, fields)),
        ),
        Expr::Func { name, args } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_having_aliases(a, fields))
                .collect(),
        },
        Expr::GenericFuncCall { schema, name, args } => Expr::GenericFuncCall {
            schema: schema.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_having_aliases(a, fields))
                .collect(),
        },
        Expr::Row(values) => Expr::Row(
            values
                .iter()
                .map(|v| resolve_having_aliases(v, fields))
                .collect(),
        ),
        Expr::Interval { value, unit } => Expr::Interval {
            value: Box::new(resolve_having_aliases(value, fields)),
            unit: unit.clone(),
        },
        Expr::Extract { unit, value } => Expr::Extract {
            unit: unit.clone(),
            value: Box::new(resolve_having_aliases(value, fields)),
        },
        Expr::TimestampAdd {
            unit,
            interval,
            expr,
        } => Expr::TimestampAdd {
            unit: unit.clone(),
            interval: Box::new(resolve_having_aliases(interval, fields)),
            expr: Box::new(resolve_having_aliases(expr, fields)),
        },
        Expr::TimestampDiff { unit, expr1, expr2 } => Expr::TimestampDiff {
            unit: unit.clone(),
            expr1: Box::new(resolve_having_aliases(expr1, fields)),
            expr2: Box::new(resolve_having_aliases(expr2, fields)),
        },
        Expr::GetFormat { selector, expr } => Expr::GetFormat {
            selector: *selector,
            expr: Box::new(resolve_having_aliases(expr, fields)),
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            list: list
                .iter()
                .map(|e| resolve_having_aliases(e, fields))
                .collect(),
            not: *not,
        },
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => Expr::Between {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            low: Box::new(resolve_having_aliases(low, fields)),
            high: Box::new(resolve_having_aliases(high, fields)),
            not: *not,
        },
        Expr::Like {
            expr,
            pattern,
            not,
            escape,
        } => Expr::Like {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            pattern: Box::new(resolve_having_aliases(pattern, fields)),
            not: *not,
            escape: *escape,
        },
        Expr::Regexp { expr, pattern, not } => Expr::Regexp {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            pattern: Box::new(resolve_having_aliases(pattern, fields)),
            not: *not,
        },
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            collation: collation.clone(),
        },
        Expr::Cast(cast) => Expr::Cast(CastExpr {
            expr: Box::new(resolve_having_aliases(&cast.expr, fields)),
            cast_type: cast.cast_type.clone(),
            style: cast.style,
            array: cast.array,
        }),
        Expr::ConvertUsing { expr, charset } => Expr::ConvertUsing {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            charset: charset.clone(),
        },
        Expr::MatchAgainst {
            columns,
            against,
            modifier,
        } => Expr::MatchAgainst {
            columns: columns.clone(),
            against: Box::new(resolve_having_aliases(against, fields)),
            modifier: *modifier,
        },
        Expr::MemberOf { expr, array } => Expr::MemberOf {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            array: Box::new(resolve_having_aliases(array, fields)),
        },
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            target: *target,
            not: *not,
        },
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => Expr::InSubquery {
            expr: Box::new(resolve_having_aliases(expr, fields)),
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
            left: Box::new(resolve_having_aliases(left, fields)),
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
                .map(|v| Box::new(resolve_having_aliases(v, fields))),
            when_clauses: when_clauses
                .iter()
                .map(|(cond, result)| {
                    (
                        resolve_having_aliases(cond, fields),
                        resolve_having_aliases(result, fields),
                    )
                })
                .collect(),
            else_clause: else_clause
                .as_ref()
                .map(|e| Box::new(resolve_having_aliases(e, fields))),
        },
        other => other.clone(),
    }
}

/// Resolves an `ORDER BY` item to an output-column index for already-projected
/// rows (used by set operations, whose rows carry no input columns): a
/// positional item, a bare identifier matching exactly one select-list alias
/// (via [`resolve_alias`] — confirmed via `gorun` that a `UNION`'s own
/// internal `ORDER BY` resolves an alias just like the outer/scan path
/// already does, e.g. `SELECT 1 AS n UNION SELECT 2 AS n ORDER BY n`), or one
/// whose expression matches a select-list field directly.
pub(crate) fn output_index(item: &OrderItem, fields: &[SelectField]) -> Result<usize, ExecError> {
    if let Some(pos) = positional(&item.expr)? {
        return Ok(pos);
    }
    let resolved = resolve_alias(&item.expr, fields);
    fields
        .iter()
        .position(|f| matches!(f, SelectField::Expr { expr, .. } if expr == resolved))
        .ok_or(ExecError::Unsupported(
            "ORDER BY expression not an output column",
        ))
}

/// Compares two rows by their precomputed sort keys, honoring per-key `DESC`.
pub(crate) fn cmp_keys(a: &[Datum], b: &[Datum], descs: &[bool]) -> Ordering {
    for ((av, bv), &desc) in a.iter().zip(b).zip(descs) {
        let ord = sort_value_cmp(av, bv);
        if ord != Ordering::Equal {
            return if desc { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

/// A total order for `ORDER BY`: `NULL`s sort first (MySQL ascending default),
/// integers/floats numerically, strings by byte order (`utf8mb4_bin`). Signed
/// and unsigned integers retain MySQL's mixed-domain ordering: every negative
/// signed value precedes UInt, while nonnegative signed values compare by
/// magnitude. This matters for real `INT UNSIGNED`/`BIGINT UNSIGNED` storage,
/// not merely unsigned literal expressions.
fn sort_value_cmp(a: &Datum, b: &Datum) -> Ordering {
    if let Some(ordering) = a.compare_sentinel_order(b) {
        return ordering;
    }
    match (a, b) {
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Less,
        (_, Datum::Null) => Ordering::Greater,
        (Datum::Int(x), Datum::Int(y)) => x.cmp(y),
        (Datum::UInt(x), Datum::UInt(y)) => x.cmp(y),
        (Datum::Int(x), Datum::UInt(_)) if *x < 0 => Ordering::Less,
        (Datum::Int(x), Datum::UInt(y)) => (*x as u64).cmp(y),
        (Datum::UInt(_), Datum::Int(y)) if *y < 0 => Ordering::Greater,
        (Datum::UInt(x), Datum::Int(y)) => x.cmp(&(*y as u64)),
        (Datum::String(x), Datum::String(y)) => x.bytes().cmp(y.bytes()),
        (Datum::Bytes(x), Datum::Bytes(y)) => x.cmp(y),
        (Datum::Decimal(x), Datum::Decimal(y)) => x.cmp(y),
        // `Datum::Real` is always finite, so `partial_cmp` always
        // succeeds here — falls back to `Equal` only in the truly
        // impossible NaN/infinite case, same as the mixed-type fallback.
        (Datum::Real(x), Datum::Real(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    }
}

/// Truncates rows to a `LIMIT [offset,] count` window.
pub(crate) fn apply_limit(rows: Vec<Row>, limit: &Limit) -> Result<Vec<Row>, ExecError> {
    let offset = match &limit.offset {
        Some(e) => const_usize(e)?,
        None => 0,
    };
    let count = const_usize(&limit.count)?;
    Ok(rows.into_iter().skip(offset).take(count).collect())
}

/// Evaluates a constant non-negative integer, for `LIMIT`/`OFFSET`.
pub(crate) fn const_usize(e: &Expr) -> Result<usize, ExecError> {
    match eval(e)? {
        Datum::Int(i) if i >= 0 => Ok(i as usize),
        _ => Err(ExecError::Unsupported("non-constant LIMIT")),
    }
}
