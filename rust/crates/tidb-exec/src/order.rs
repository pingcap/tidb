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

use std::{cmp::Ordering, error::Error, fmt};

use tidb_ast::{CastExpr, Expr, Limit, OrderItem, SelectField};
use tidb_datatype::{Collation, Datum, DatumKind};
use tidb_expr::eval;
use tidb_planner::configured_order_limit_contract::ConfiguredOrderKey;
use tidb_planner::read_only_scan::{ConfiguredScalarType, PreparedOrderColumn};

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
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            origin_position: *origin_position,
            args: args
                .iter()
                .map(|a| resolve_having_aliases(a, fields))
                .collect(),
        },
        Expr::GenericFuncCall {
            schema,
            name,
            args,
            origin_position,
        } => Expr::GenericFuncCall {
            schema: schema.clone(),
            name: name.clone(),
            origin_position: *origin_position,
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
            ilike,
            escape,
        } => Expr::Like {
            expr: Box::new(resolve_having_aliases(expr, fields)),
            pattern: Box::new(resolve_having_aliases(pattern, fields)),
            not: *not,
            ilike: *ilike,
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
    cmp_key_pairs(
        a.iter()
            .zip(b)
            .zip(descs)
            .map(|((left, right), &desc)| (left, right, desc)),
    )
}

/// Compares ordered key pairs through the executor's one total-order
/// authority. Callers that restrict their datum domain must validate it before
/// invoking this function; comparison itself remains shared with every other
/// `ORDER BY` path.
fn cmp_key_pairs<'a>(pairs: impl IntoIterator<Item = (&'a Datum, &'a Datum, bool)>) -> Ordering {
    for (av, bv, desc) in pairs {
        let ord = sort_value_cmp(av, bv);
        if ord != Ordering::Equal {
            return if desc { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

/// A checked configured-order execution failure.
///
/// The planner contract represents only signed-BIGINT order keys. Keeping
/// malformed physical rows distinct from an ordinary tie prevents a widened
/// executor from inventing NULL/zero/coercion semantics before it owns them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredOrderError {
    /// A planner-resolved key does not fit the promised physical FullSchema.
    FullSchemaOffset {
        /// The invalid planner-resolved physical key offset.
        offset: usize,
        /// The promised physical FullSchema width.
        width: usize,
    },
    /// A materialized row is not the width promised by the planner.
    RowWidth {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-promised physical FullSchema width.
        expected: usize,
        /// Actual number of decoded datum slots.
        actual: usize,
    },
    /// A configured signed-BIGINT key decoded as another datum kind.
    KeyDatum {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-resolved physical key offset.
        offset: usize,
        /// Actual datum kind at that physical key offset.
        kind: DatumKind,
    },
}

impl fmt::Display for ConfiguredOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FullSchemaOffset { offset, width } => {
                write!(
                    formatter,
                    "configured ORDER BY offset {offset} exceeds FullSchema width {width}"
                )
            }
            Self::RowWidth {
                row_index,
                expected,
                actual,
            } => write!(
                formatter,
                "configured ORDER BY row {row_index} has width {actual}, expected {expected}"
            ),
            Self::KeyDatum {
                row_index,
                offset,
                kind,
            } => write!(
                formatter,
                "configured ORDER BY row {row_index} key at offset {offset} decoded as {kind:?}"
            ),
        }
    }
}

impl Error for ConfiguredOrderError {}

/// Stably orders materialized configured rows by planner-resolved FullSchema
/// keys.
///
/// This is the first executable consumer of
/// `ConfiguredOrderKey`: every key is a checked physical offset into a row of
/// exactly `full_schema_width` signed-BIGINT datums. The full input is
/// validated before mutation, then Rust's stable slice sort keeps source order
/// for rows whose complete key tuple ties. NULLs, unsigned/mixed types,
/// collations, spilling, and parallel merge execution intentionally belong to
/// later owners rather than being guessed here.
pub fn stable_order_configured_rows(
    rows: &mut [Row],
    full_schema_width: usize,
    keys: &[ConfiguredOrderKey],
) -> Result<(), ConfiguredOrderError> {
    validate_configured_order_rows(rows, full_schema_width, keys)?;

    rows.sort_by(|left, right| compare_configured_rows(left, right, keys));
    Ok(())
}

/// Validates rows before a configured ordering consumer indexes their physical
/// FullSchema offsets.
///
/// A bounded TopN validates each row before it enters its heap;
/// [`compare_configured_rows`] can then stay allocation-free and infallible in
/// the heap's hot comparison path. The contract intentionally accepts only
/// signed-BIGINT key datums until a wider planner/executor contract owns the
/// missing coercion and collation semantics.
pub fn validate_configured_order_rows(
    rows: &[Row],
    full_schema_width: usize,
    keys: &[ConfiguredOrderKey],
) -> Result<(), ConfiguredOrderError> {
    for key in keys {
        if key.full_offset() >= full_schema_width {
            return Err(ConfiguredOrderError::FullSchemaOffset {
                offset: key.full_offset(),
                width: full_schema_width,
            });
        }
    }

    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != full_schema_width {
            return Err(ConfiguredOrderError::RowWidth {
                row_index,
                expected: full_schema_width,
                actual: row.len(),
            });
        }
        for key in keys {
            let value = &row[key.full_offset()];
            if !matches!(value, Datum::Int(_)) {
                return Err(ConfiguredOrderError::KeyDatum {
                    row_index,
                    offset: key.full_offset(),
                    kind: value.kind(),
                });
            }
        }
    }
    Ok(())
}

/// Compares two already validated configured rows by their planner-resolved
/// physical keys.
///
/// Callers must first run [`validate_configured_order_rows`] over every row
/// they may pass here with the same `full_schema_width` and `keys`. This keeps
/// the comparator suitable for a `BinaryHeap` without silently inventing a
/// fallback ordering for malformed physical data.
pub fn compare_configured_rows(left: &Row, right: &Row, keys: &[ConfiguredOrderKey]) -> Ordering {
    cmp_key_pairs(keys.iter().map(|key| {
        (
            &left[key.full_offset()],
            &right[key.full_offset()],
            key.direction().is_descending(),
        )
    }))
}

/// A checked prepared-read ordering failure.
///
/// Unlike [`ConfiguredOrderError`], whose keys are always signed BIGINT, a
/// prepared read's keys carry the projected column's scalar type. Keeping a
/// mistyped physical row distinct from an ordinary tie prevents the executor
/// from inventing a fallback order for data the column's type does not admit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedOrderError {
    /// A resolved key offset does not fit the projected output row width.
    OutputOffset {
        /// The invalid planner-resolved output offset.
        offset: usize,
        /// The projected output row width.
        width: usize,
    },
    /// A materialized row is not the projected output width.
    RowWidth {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-promised projected output width.
        expected: usize,
        /// Actual number of decoded datum slots.
        actual: usize,
    },
    /// A key datum's kind does not match the projected column's scalar type.
    KeyDatum {
        /// Zero-based position of the malformed materialized row.
        row_index: usize,
        /// Planner-resolved output key offset.
        offset: usize,
        /// Actual datum kind decoded at that offset.
        kind: DatumKind,
    },
}

impl fmt::Display for PreparedOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputOffset { offset, width } => write!(
                formatter,
                "prepared ORDER BY offset {offset} exceeds output width {width}"
            ),
            Self::RowWidth {
                row_index,
                expected,
                actual,
            } => write!(
                formatter,
                "prepared ORDER BY row {row_index} has width {actual}, expected {expected}"
            ),
            Self::KeyDatum {
                row_index,
                offset,
                kind,
            } => write!(
                formatter,
                "prepared ORDER BY row {row_index} key at offset {offset} decoded as {kind:?}"
            ),
        }
    }
}

impl Error for PreparedOrderError {}

/// The datum kind a projected column's scalar type stores in an output row.
///
/// A signed integer column decodes to [`Datum::Int`]; an unsigned `BIGINT`
/// decodes to [`Datum::UInt`]; a `DOUBLE` decodes to [`Datum::Real`]; a `CHAR`
/// column decodes to [`Datum::Bytes`] (its `utf8mb4` bytes); `DATE`/`DATETIME`/
/// `TIMESTAMP` decode to [`Datum::Time`]; `TIME` decodes to
/// [`Datum::Duration`]. A nullable column additionally decodes to
/// [`Datum::Null`]; a `NOT NULL` one never may, so a `NULL` there stays a
/// decode contract violation. Any other pairing is a decode contract violation
/// the ordering must not silently reorder.
const fn scalar_type_admits(
    scalar_type: ConfiguredScalarType,
    nullable: bool,
    datum: &Datum,
) -> bool {
    if matches!(datum, Datum::Null) {
        return nullable;
    }
    match scalar_type {
        ConfiguredScalarType::BigInt | ConfiguredScalarType::Int => matches!(datum, Datum::Int(_)),
        ConfiguredScalarType::UnsignedBigInt => matches!(datum, Datum::UInt(_)),
        ConfiguredScalarType::Double => matches!(datum, Datum::Real(_)),
        ConfiguredScalarType::Char { .. } | ConfiguredScalarType::Varchar { .. } => {
            matches!(datum, Datum::Bytes(_))
        }
        ConfiguredScalarType::Decimal { .. } => matches!(datum, Datum::Decimal(_)),
        ConfiguredScalarType::Date
        | ConfiguredScalarType::Datetime { .. }
        | ConfiguredScalarType::Timestamp { .. } => matches!(datum, Datum::Time(_)),
        ConfiguredScalarType::Duration { .. } => matches!(datum, Datum::Duration(_)),
    }
}

/// Validates materialized prepared-read rows before ordering indexes them.
///
/// Mirrors [`validate_configured_order_rows`] but admits each projected
/// column's own scalar type instead of a single signed-BIGINT domain, so the
/// comparator can stay allocation-free and infallible in the sort's hot path.
pub fn validate_prepared_order_rows(
    rows: &[Row],
    output_width: usize,
    keys: &[PreparedOrderColumn],
) -> Result<(), PreparedOrderError> {
    for key in keys {
        if key.output_offset() >= output_width {
            return Err(PreparedOrderError::OutputOffset {
                offset: key.output_offset(),
                width: output_width,
            });
        }
    }

    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != output_width {
            return Err(PreparedOrderError::RowWidth {
                row_index,
                expected: output_width,
                actual: row.len(),
            });
        }
        for key in keys {
            let datum = &row[key.output_offset()];
            if !scalar_type_admits(key.scalar_type(), key.is_nullable(), datum) {
                return Err(PreparedOrderError::KeyDatum {
                    row_index,
                    offset: key.output_offset(),
                    kind: datum.kind(),
                });
            }
        }
    }
    Ok(())
}

/// Compares two already validated prepared-read rows by their resolved keys.
///
/// String columns compare under their `utf8mb4_bin` collation through the
/// crate-shared [`Collation`] authority, which trims trailing spaces exactly as
/// TiDB's Go collator does. Every other pairing — including `NULL`, which sorts
/// before all non-`NULL` values ascending — defers to [`sort_value_cmp`], the
/// same total order the in-process sort executor applies, so the two sorts
/// cannot disagree. Callers must first run [`validate_prepared_order_rows`].
fn compare_prepared_rows(left: &Row, right: &Row, keys: &[PreparedOrderColumn]) -> Ordering {
    for key in keys {
        let offset = key.output_offset();
        let ordering = match (&left[offset], &right[offset]) {
            (Datum::Bytes(a), Datum::Bytes(b)) => Collation::Utf8Mb4Bin.compare(a, b),
            (a, b) => sort_value_cmp(a, b),
        };
        if ordering != Ordering::Equal {
            return if key.direction().is_descending() {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

/// Stably orders materialized prepared-read rows by planner-resolved keys.
///
/// The prepared point/range read has no `LIMIT`, so its `ORDER BY` is a
/// SQL-layer sort over the fully projected output rows rather than a bounded
/// coprocessor TopN. The full input is validated before mutation, then Rust's
/// stable sort keeps source order for rows whose complete key tuple ties.
pub fn stable_order_prepared_rows(
    rows: &mut [Row],
    output_width: usize,
    keys: &[PreparedOrderColumn],
) -> Result<(), PreparedOrderError> {
    validate_prepared_order_rows(rows, output_width, keys)?;
    rows.sort_by(|left, right| compare_prepared_rows(left, right, keys));
    Ok(())
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
        (Datum::Time(x), Datum::Time(y)) => x.compare(*y),
        (Datum::Duration(x), Datum::Duration(y)) => x.compare(*y),
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

#[cfg(test)]
mod tests {
    use tidb_planner::configured_order_limit_contract::{
        ConfiguredOrderDirection, ConfiguredOrderKey,
    };

    use super::{
        stable_order_configured_rows, stable_order_prepared_rows, ConfiguredOrderError,
        ConfiguredScalarType, Datum, DatumKind, PreparedOrderColumn, PreparedOrderError, Row,
    };

    #[test]
    fn configured_order_uses_fullschema_offsets_directions_and_stable_ties() {
        let keys = [
            ConfiguredOrderKey::new(2, ConfiguredOrderDirection::Ascending),
            ConfiguredOrderKey::new(1, ConfiguredOrderDirection::Descending),
        ];
        let mut rows: Vec<Row> = vec![
            vec![Datum::Int(100), Datum::Int(9), Datum::Int(2)],
            vec![Datum::Int(200), Datum::Int(8), Datum::Int(2)],
            vec![Datum::Int(300), Datum::Int(9), Datum::Int(2)],
            vec![Datum::Int(400), Datum::Int(10), Datum::Int(1)],
        ];

        stable_order_configured_rows(&mut rows, 3, &keys).expect("configured signed BIGINT rows");

        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(400), Datum::Int(10), Datum::Int(1)],
                vec![Datum::Int(100), Datum::Int(9), Datum::Int(2)],
                vec![Datum::Int(300), Datum::Int(9), Datum::Int(2)],
                vec![Datum::Int(200), Datum::Int(8), Datum::Int(2)],
            ],
            "equal complete keys retain source order"
        );
    }

    #[test]
    fn configured_order_rejects_invalid_fullschema_rows_before_sorting() {
        let key = ConfiguredOrderKey::new(1, ConfiguredOrderDirection::Ascending);
        let mut wrong_width = vec![vec![Datum::Int(2), Datum::Int(1)], vec![Datum::Int(1)]];
        assert_eq!(
            stable_order_configured_rows(&mut wrong_width, 2, &[key]),
            Err(ConfiguredOrderError::RowWidth {
                row_index: 1,
                expected: 2,
                actual: 1,
            })
        );
        assert_eq!(
            wrong_width[0][0],
            Datum::Int(2),
            "validation precedes mutation"
        );

        let mut wrong_kind = vec![vec![Datum::Int(1), Datum::UInt(2)]];
        assert_eq!(
            stable_order_configured_rows(&mut wrong_kind, 2, &[key]),
            Err(ConfiguredOrderError::KeyDatum {
                row_index: 0,
                offset: 1,
                kind: DatumKind::UInt,
            })
        );

        let mut rows = vec![vec![Datum::Int(1), Datum::Int(2)]];
        let outside = ConfiguredOrderKey::new(2, ConfiguredOrderDirection::Descending);
        assert_eq!(
            stable_order_configured_rows(&mut rows, 2, &[outside]),
            Err(ConfiguredOrderError::FullSchemaOffset {
                offset: 2,
                width: 2,
            })
        );
    }

    fn bytes_row(value: &str) -> Row {
        vec![Datum::new_bytes(value.as_bytes().to_vec())]
    }

    #[test]
    fn prepared_order_sorts_utf8mb4_bin_char_column_by_bytes() {
        // sysbench read 4: `SELECT c ... ORDER BY c`, one projected CHAR column.
        let key = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 120 },
            false,
        );
        let mut rows = vec![bytes_row("banana"), bytes_row("apple"), bytes_row("cherry")];
        stable_order_prepared_rows(&mut rows, 1, &[key]).expect("utf8mb4_bin char rows");
        assert_eq!(
            rows,
            vec![bytes_row("apple"), bytes_row("banana"), bytes_row("cherry")]
        );
    }

    #[test]
    fn prepared_order_char_descending_and_trailing_space_ties_stably() {
        let descending = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Descending,
            ConfiguredScalarType::Char { max_length: 8 },
            false,
        );
        let mut rows = vec![bytes_row("a"), bytes_row("c"), bytes_row("b")];
        stable_order_prepared_rows(&mut rows, 1, &[descending]).expect("descending char rows");
        assert_eq!(rows, vec![bytes_row("c"), bytes_row("b"), bytes_row("a")]);

        // utf8mb4_bin is PAD SPACE: "a " and "a" tie, so the stable sort keeps
        // the source order of the tied rows (the shared Collation authority
        // trims the trailing space exactly as TiDB's Go collator does).
        let ascending = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 8 },
            false,
        );
        let mut padded = vec![bytes_row("a "), bytes_row("a"), bytes_row("a  ")];
        stable_order_prepared_rows(&mut padded, 1, &[ascending]).expect("padded char rows");
        assert_eq!(
            padded,
            vec![bytes_row("a "), bytes_row("a"), bytes_row("a  ")],
            "PAD SPACE ties retain source order"
        );
    }

    #[test]
    fn prepared_order_signed_int_column_compares_numerically() {
        let key = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::BigInt,
            false,
        );
        let mut rows = vec![
            vec![Datum::Int(30)],
            vec![Datum::Int(-5)],
            vec![Datum::Int(2)],
        ];
        stable_order_prepared_rows(&mut rows, 1, &[key]).expect("signed int rows");
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(-5)],
                vec![Datum::Int(2)],
                vec![Datum::Int(30)]
            ]
        );
    }

    #[test]
    fn prepared_order_rejects_mistyped_and_out_of_range_rows_before_sorting() {
        // A CHAR key over an integer datum is a decode contract violation.
        let char_key = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::Char { max_length: 4 },
            false,
        );
        let mut mistyped = vec![bytes_row("b"), vec![Datum::Int(1)]];
        assert_eq!(
            stable_order_prepared_rows(&mut mistyped, 1, &[char_key]),
            Err(PreparedOrderError::KeyDatum {
                row_index: 1,
                offset: 0,
                kind: DatumKind::Int,
            })
        );
        assert_eq!(mistyped[0], bytes_row("b"), "validation precedes mutation");

        let int_key = PreparedOrderColumn::new(
            1,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::BigInt,
            false,
        );
        let mut rows = vec![vec![Datum::Int(1)]];
        assert_eq!(
            stable_order_prepared_rows(&mut rows, 1, &[int_key]),
            Err(PreparedOrderError::OutputOffset {
                offset: 1,
                width: 1
            })
        );

        let mut narrow = vec![vec![Datum::Int(1), Datum::Int(2)], vec![Datum::Int(3)]];
        let offset_zero = PreparedOrderColumn::new(
            0,
            ConfiguredOrderDirection::Ascending,
            ConfiguredScalarType::BigInt,
            false,
        );
        assert_eq!(
            stable_order_prepared_rows(&mut narrow, 2, &[offset_zero]),
            Err(PreparedOrderError::RowWidth {
                row_index: 1,
                expected: 2,
                actual: 1,
            })
        );
    }

    /// MySQL's `ORDER BY` treats `NULL` as smaller than every non-`NULL`
    /// value, so it sorts first ascending and last descending. This is the
    /// same rule the in-process sort executor applies through
    /// `sort_value_cmp`, verified here on the prepared-read comparator so the
    /// two sorts cannot disagree.
    #[test]
    fn prepared_order_sorts_nulls_first_ascending_and_last_descending() {
        let rows = || -> Vec<Row> {
            vec![
                vec![Datum::Int(2)],
                vec![Datum::Null],
                vec![Datum::Int(-1)],
                vec![Datum::Null],
            ]
        };

        let mut ascending = rows();
        stable_order_prepared_rows(
            &mut ascending,
            1,
            &[PreparedOrderColumn::new(
                0,
                ConfiguredOrderDirection::Ascending,
                ConfiguredScalarType::BigInt,
                true,
            )],
        )
        .expect("a nullable key admits NULL");
        assert_eq!(
            ascending,
            vec![
                vec![Datum::Null],
                vec![Datum::Null],
                vec![Datum::Int(-1)],
                vec![Datum::Int(2)],
            ]
        );

        let mut descending = rows();
        stable_order_prepared_rows(
            &mut descending,
            1,
            &[PreparedOrderColumn::new(
                0,
                ConfiguredOrderDirection::Descending,
                ConfiguredScalarType::BigInt,
                true,
            )],
        )
        .expect("a nullable key admits NULL");
        assert_eq!(
            descending,
            vec![
                vec![Datum::Int(2)],
                vec![Datum::Int(-1)],
                vec![Datum::Null],
                vec![Datum::Null],
            ]
        );
    }

    /// A `NOT NULL` column that decoded to `NULL` is a decode contract
    /// violation, not an orderable value; the sort must refuse it rather than
    /// place it anywhere.
    #[test]
    fn a_null_in_a_not_null_order_key_still_fails_closed() {
        let mut rows: Vec<Row> = vec![vec![Datum::Int(1)], vec![Datum::Null]];
        assert_eq!(
            stable_order_prepared_rows(
                &mut rows,
                1,
                &[PreparedOrderColumn::new(
                    0,
                    ConfiguredOrderDirection::Ascending,
                    ConfiguredScalarType::BigInt,
                    false,
                )],
            ),
            Err(PreparedOrderError::KeyDatum {
                row_index: 1,
                offset: 0,
                kind: DatumKind::Null,
            })
        );
    }

    /// Every widened scalar type orders by its own value domain. Before the
    /// comparator delegated to `sort_value_cmp`, an unsigned, floating-point,
    /// or decimal key compared as `Equal` and the sort silently kept source
    /// order.
    #[test]
    fn prepared_order_compares_every_widened_scalar_domain() {
        let cases: [(ConfiguredScalarType, [Datum; 3]); 3] = [
            (
                ConfiguredScalarType::UnsignedBigInt,
                [Datum::UInt(u64::MAX), Datum::UInt(0), Datum::UInt(1 << 63)],
            ),
            (
                ConfiguredScalarType::Double,
                [
                    Datum::new_real(2.5),
                    Datum::new_real(-1.0),
                    Datum::new_real(0.0),
                ],
            ),
            (
                ConfiguredScalarType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                [
                    Datum::new_decimal(tidb_datatype::Decimal::from_int(12)),
                    Datum::new_decimal(tidb_datatype::Decimal::from_int(-3)),
                    Datum::new_decimal(tidb_datatype::Decimal::from_int(0)),
                ],
            ),
        ];
        for (scalar_type, values) in cases {
            let mut rows: Vec<Row> = values.iter().map(|v| vec![v.clone()]).collect();
            stable_order_prepared_rows(
                &mut rows,
                1,
                &[PreparedOrderColumn::new(
                    0,
                    ConfiguredOrderDirection::Ascending,
                    scalar_type,
                    false,
                )],
            )
            .expect("widened scalar rows are orderable");
            assert_eq!(
                rows,
                vec![
                    vec![values[1].clone()],
                    vec![values[2].clone()],
                    vec![values[0].clone()],
                ],
                "{scalar_type:?} must order by value"
            );
        }
    }
}
