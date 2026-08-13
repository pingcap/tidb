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

//! Index range derivation: Go `pkg/util/ranger`.
//!
//! Go splits a `WHERE` into *access* conditions, which become the index
//! ranges an `IndexRangeScan` reads, and *filter* conditions, which stay above
//! the read. The access half is built by a point algebra (`points.go`): every
//! condition on one column becomes a sorted list of interval endpoints, several
//! conditions on the same column intersect, and the endpoints of adjacent index
//! columns are concatenated while the built ranges are still single points.
//!
//! This module ports that algebra --- `point`, `merge`/`intersection`/`union`,
//! `points2Ranges`, `appendPoints2Ranges`, `UnionRanges` --- and the
//! column-walking half of `detacher.go`
//! (`ExtractEqAndInCondition` + `buildRangeOnColsByCNFCond` +
//! `detachDNFCondAndBuildRangeForIndex`).
//!
//! The residual half is handled by construction at the call site: the whole
//! `WHERE` stays in the `Selection` above the scan, so a condition that
//! became a range is re-checked rather than dropped. Ranges only ever
//! *restrict* the rows read, so keeping the predicate is always sound, and it
//! composes with predicate push-down without any condition being applied
//! twice in a way that changes the result.
//!
//! A prefix index is the one shape where an UNCUT range would be a SUBSET
//! rather than a superset, and so the one shape the residual predicate could
//! not rescue: `KEY (s(4))` stores `'alph'` for `'alphabet'`, and seeking
//! `["alphabet","alphabet"]` finds nothing at all -- the rows go missing.
//! That is why [`RangeColumn`] carries the key part's declared length and
//! every endpoint built on it goes through [`cut_prefix_for_points`], Go's
//! `cutPrefixForPoints`, before it becomes a range. Cutting turns the range
//! back into a superset, which the residual predicate then filters.
//!
//! DEFERRED (documented, each a superset --- the residual predicate still
//! filters, so the answer stays correct):
//!   * the handle columns Go appends to a non-clustered index's tail, so
//!     `a = 1 AND b = 2 AND id > 5` on `(a, b)` reads `(1 2 5, 1 2 +inf]`.
//!   * `extractBestCNFItemRanges` / `chooseBetweenRangeAndPoint`: Go's
//!     cost-driven preference for one CNF item's DNF ranges over the
//!     leading-column ranges.
//!   * `handleUnsignedCol`'s signedness clamping.
//!   * `convertToSortKey` for the builders whose endpoints are VALUES: they
//!     stay text and the key codec collates them into exactly the bytes Go's
//!     conversion produced, so the ENCODED range is Go's even though the
//!     printed one shows the text rather than the weights. [`like`] is the
//!     exception and does convert, because its upper bound is a weight string
//!     with no textual preimage --- see that module.

mod like;

use crate::index_prefix_cut::{cut_datum_by_prefix_len, reaches_prefix_len, UNSPECIFIED_LENGTH};
use crate::kv_table::IndexRange;
use like::points_from_like;
use std::cmp::Ordering;
use tidb_ast::{BinaryOp, Expr, IsTarget, UnaryOp};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::rewrite_expr_resolved;

/// One index key part as the ranger sees it: the column it names, its type,
/// and how much of it the index actually stores.
///
/// Go passes the same three separately -- `path.IdxCols[i]`, its `RetType`,
/// and `path.IdxColLens[i]`. Keeping them together makes the length
/// impossible to forget at a call site, which is what a range that silently
/// missed rows would look like.
#[derive(Clone, Debug)]
pub(crate) struct RangeColumn {
    /// The column's name, as the `WHERE` refers to it.
    pub name: String,
    /// The column's type, which every endpoint is converted into.
    pub field_type: FieldType,
    /// Go `IndexColumn.Length`: [`UNSPECIFIED_LENGTH`] for a key part that
    /// stores the whole column.
    pub prefix_len: i64,
}

impl RangeColumn {
    /// A key part that stores the whole column -- an ordinary index, and the
    /// clustered handle, which has no length to declare.
    pub(crate) fn whole(name: String, field_type: FieldType) -> Self {
        Self {
            name,
            field_type,
            prefix_len: UNSPECIFIED_LENGTH,
        }
    }
}

/// Go `ranger.point`: one endpoint of one interval on one column.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Point {
    /// The endpoint value; `MinNotNull`/`MaxValue` are the infinities.
    pub value: Datum,
    /// Go `point.excl`: the endpoint is outside the interval.
    pub excl: bool,
    /// Go `point.start`: this endpoint opens rather than closes an interval.
    pub start: bool,
}

impl Point {
    fn start(value: Datum, excl: bool) -> Self {
        Self {
            value,
            excl,
            start: true,
        }
    }

    fn end(value: Datum, excl: bool) -> Self {
        Self {
            value,
            excl,
            start: false,
        }
    }
}

/// Go `getFullRange`: `[NULL, +inf]`, everything including NULL.
fn full_range() -> Vec<Point> {
    vec![
        Point::start(Datum::Null, false),
        Point::end(Datum::MaxValue, false),
    ]
}

/// Go `getNotNullFullRange`: `[-inf, +inf]`.
fn not_null_full_range() -> Vec<Point> {
    vec![
        Point::start(Datum::MinNotNull, false),
        Point::end(Datum::MaxValue, false),
    ]
}

/// Go `rangePointEqualValueCmp`: with equal values, which endpoint sorts
/// first is decided by which side of an interval each one is on.
fn equal_value_cmp(a: &Point, b: &Point) -> Ordering {
    let less = if a.start && b.start {
        !a.excl && b.excl
    } else if a.start {
        !a.excl && !b.excl
    } else if b.start {
        a.excl || b.excl
    } else {
        a.excl && !b.excl
    };
    if less {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

/// Orders two datums with `MinNotNull` below and `MaxValue` above every
/// ordinary value, and `NULL` below `MinNotNull` (Go's datum kind order).
pub(crate) fn compare_datum_bounds(left: &Datum, right: &Datum) -> Ordering {
    let rank = |value: &Datum| match value {
        Datum::Null => 0,
        Datum::MinNotNull => 1,
        Datum::MaxValue => 3,
        _ => 2,
    };
    match rank(left).cmp(&rank(right)) {
        Ordering::Equal => {}
        other => return other,
    }
    if rank(left) != 2 {
        return Ordering::Equal;
    }
    tidb_expr::compare_datums(left, right).unwrap_or(Ordering::Equal)
}

/// Go `rangePointCmp`.
fn point_cmp(a: &Point, b: &Point) -> Ordering {
    match compare_datum_bounds(&a.value, &b.value) {
        Ordering::Equal => equal_value_cmp(a, b),
        other => other,
    }
}

/// Go `builder.mergeSorted`.
fn merge_sorted(a: &[Point], b: &[Point]) -> Vec<Point> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if point_cmp(&a[i], &b[j]) == Ordering::Less {
            out.push(a[i].clone());
            i += 1;
        } else {
            out.push(b[j].clone());
            j += 1;
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// Go `builder.merge`: a sweep over the merged endpoints keeping the spans
/// covered by `required` inputs at once --- one for a union, two for an
/// intersection.
fn merge(a: &[Point], b: &[Point], union: bool) -> Vec<Point> {
    let merged = merge_sorted(a, b);
    let required = if union { 1 } else { 2 };
    let mut in_range = 0;
    let mut out = Vec::with_capacity(merged.len());
    for point in merged {
        if point.start {
            in_range += 1;
            if in_range == required {
                out.push(point);
            }
        } else {
            if in_range == required {
                out.push(point);
            }
            in_range -= 1;
        }
    }
    out
}

/// Go `builder.intersection`.
pub(crate) fn intersection(a: &[Point], b: &[Point]) -> Vec<Point> {
    merge(a, b, false)
}

/// Go `builder.union`.
pub(crate) fn union_points(a: &[Point], b: &[Point]) -> Vec<Point> {
    merge(a, b, true)
}

/// Go `validInterval`: an interval is non-empty when its low key, advanced
/// past an excluded bound, is below its high key, advanced past an included
/// one. Comparing on the *encoded* key is what makes `(1, 2)` on an integer
/// column empty, since `PrefixNext(encode(1)) == encode(2)`.
fn valid_interval(low: &Point, high: &Point) -> bool {
    let Ok(mut left) = tidb_codec::encode_key(std::slice::from_ref(&low.value)) else {
        return false;
    };
    if low.excl {
        left = prefix_next(left);
    }
    let Ok(mut right) = tidb_codec::encode_key(std::slice::from_ref(&high.value)) else {
        return false;
    };
    if !high.excl {
        right = prefix_next(right);
    }
    left < right
}

/// Go `kv.Key.PrefixNext`: the smallest key strictly above every key with
/// this prefix.
fn prefix_next(mut key: Vec<u8>) -> Vec<u8> {
    for i in (0..key.len()).rev() {
        if key[i] == 0xff {
            key[i] = 0;
        } else {
            key[i] += 1;
            return key;
        }
    }
    // Every byte was 0xff: Go restores the original and appends a zero byte,
    // which is above the original and below every longer key.
    key.fill(0xff);
    key.push(0);
    key
}

/// Go `convertPointInPlace`: casts one endpoint into the indexed column's
/// type, then repairs `excl` when the cast moved the value.
///
/// This is what makes `a >= -2147483648` on an UNSIGNED column collapse to
/// `[0,+inf]` and `col_float > 1e39` collapse to nothing: `convert_to`
/// saturates at the target boundary and reports the overflow, and the
/// exclusivity repair below turns the saturated endpoint back into the
/// smallest/largest interval that means the same thing.
fn convert_point_in_place(p: &mut Point, target: &FieldType) {
    match p.value {
        Datum::MaxValue | Datum::MinNotNull | Datum::Null => return,
        _ => {}
    }
    // Go `convertStringFTToBinaryCollate` (`ranger.go:616`): `points2Ranges`
    // is handed a BINARY-collated clone of the column's type, because by then
    // every string endpoint is a weight string rather than text. Converting a
    // weight string into the column's own collation would stamp that
    // collation onto it, and the key codec would collate it a SECOND time --
    // which puts the upper bound below the lower one and empties the range.
    //
    // This crate defers `convertToSortKey`, so only the endpoints that ARE
    // weight strings ([`increment_sort_key`]'s, the one place a `Datum::Bytes`
    // enters a point over a string column) need Go's binary-collated target;
    // the text endpoints keep the column's collation and are collated once, by
    // the codec, into the same bytes Go's conversion produced.
    let owned;
    let target = match (&p.value, target.eval_type()) {
        (Datum::Bytes(_), tidb_datatype::EvalType::String) => {
            owned = binary_collate_field_type(target);
            &owned
        }
        _ => target,
    };
    // Go tolerates exactly the overflow/truncation events below and keeps the
    // saturated boundary value; anything it propagates as an error leaves the
    // point untouched here, which is the conservative (wider range) choice.
    let Ok(converted) = p.value.convert_to(target, tidb_datatype::STRICT_FLAGS) else {
        return;
    };
    let casted = converted.value;
    let Ok(order) = tidb_expr::compare_datums(&p.value, &casted) else {
        return;
    };
    p.value = casted;
    if order == Ordering::Equal {
        return;
    }
    if p.start {
        if p.excl {
            // e.g. "a > 1.9" converts to "a >= 2".
            if order == Ordering::Less {
                p.excl = false;
            }
        } else if order == Ordering::Greater {
            // e.g. "a >= 1.1" converts to "a > 1".
            p.excl = true;
        }
    } else if p.excl {
        // e.g. "a < 1.1" converts to "a <= 1".
        if order == Ordering::Greater {
            p.excl = false;
        }
    } else if order == Ordering::Less {
        // e.g. "a <= 1.9" converts to "a < 2".
        p.excl = true;
    }
}

/// Go `convertStringFTToBinaryCollate`: the same type with the `binary`
/// charset and collation, which is what makes a conversion into it leave a
/// weight string alone. `ENUM` and `SET` keep their own collation there,
/// because their endpoints are the member's name rather than a weight string.
fn binary_collate_field_type(target: &FieldType) -> FieldType {
    if matches!(
        target.code(),
        tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set
    ) {
        return target.clone();
    }
    let mut binary = target.clone();
    binary.set_charset_name("binary");
    binary.set_collation_name("binary");
    binary
}

/// Go `convertPointsInPlace`.
fn convert_points_in_place(points: &mut [Point], target: &FieldType) {
    for p in points {
        convert_point_in_place(p, target);
    }
}

/// Go `points2Ranges`: consecutive endpoint pairs become single-column
/// ranges, dropping the empty ones.
///
/// `column` is the FIRST index column, because that is the only one Go applies
/// `skipNull` to: `points2Ranges` passes
/// `skipNull = mysql.HasNotNullFlag(newTp.GetFlag())`
/// (`pkg/util/ranger/ranger.go:129`) while `appendPoints2Ranges` passes
/// `false` (`:295`). A `NOT NULL` column cannot hold the value an interval
/// ending at NULL selects, so `convertPointsInPlace` drops that interval
/// entirely (`:102-104`) and an `a IS NULL` over such a column becomes a
/// `TableDual rows:0` rather than a scan of `[NULL,NULL]` that reads a range
/// no row lives in.
fn points_to_ranges(points: &[Point], column: &RangeColumn) -> Vec<IndexRange> {
    let skip_null = column
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let mut ranges = Vec::with_capacity(points.len() / 2);
    for pair in points.chunks_exact(2) {
        let (low, high) = (&pair[0], &pair[1]);
        if skip_null && high.value == Datum::Null {
            continue;
        }
        if !valid_interval(low, high) {
            continue;
        }
        ranges.push(IndexRange {
            low: vec![low.value.clone()],
            high: vec![high.value.clone()],
            low_exclusive: low.excl,
            high_exclusive: high.excl,
        });
    }
    ranges
}

/// Go `Range.IsPoint`, reduced to the shape this crate builds: one closed
/// range whose bounds are equal.
fn is_point_range(range: &IndexRange) -> bool {
    !range.low_exclusive
        && !range.high_exclusive
        && range.low.len() == range.high.len()
        && range
            .low
            .iter()
            .zip(&range.high)
            .all(|(l, h)| compare_datum_bounds(l, h) == Ordering::Equal)
}

/// Go `appendPoints2Ranges`: the next index column's endpoints extend every
/// range that is still a point; a range that already spans is left alone,
/// because nothing after a spanning column is contiguous in the index.
fn append_points_to_ranges(origin: &[IndexRange], points: &[Point]) -> Vec<IndexRange> {
    let mut out = Vec::new();
    for range in origin {
        if !is_point_range(range) {
            out.push(range.clone());
            continue;
        }
        for pair in points.chunks_exact(2) {
            let (low, high) = (&pair[0], &pair[1]);
            if !valid_interval(low, high) {
                continue;
            }
            let mut new_low = range.low.clone();
            new_low.push(low.value.clone());
            let mut new_high = range.high.clone();
            new_high.push(high.value.clone());
            out.push(IndexRange {
                low: new_low,
                high: new_high,
                low_exclusive: low.excl,
                high_exclusive: high.excl,
            });
        }
    }
    out
}

/// Go `UnionRanges`: sorts on the encoded low key and merges overlapping
/// ranges. With `merge_consecutive`, ranges that merely touch on the encoded
/// key are merged too, which is why `a = 1 OR a = 2` prints as `[1,2]`.
pub(crate) fn union_ranges(ranges: Vec<IndexRange>, merge_consecutive: bool) -> Vec<IndexRange> {
    if ranges.is_empty() {
        return ranges;
    }
    let encode = |values: &[Datum]| tidb_codec::encode_key(values).unwrap_or_default();
    let mut objects: Vec<(Vec<u8>, Vec<u8>, IndexRange)> = ranges
        .into_iter()
        .map(|range| {
            let mut left = encode(&range.low);
            if range.low_exclusive {
                left = prefix_next(left);
            }
            let mut right = encode(&range.high);
            if !range.high_exclusive {
                right = prefix_next(right);
            }
            (left, right, range)
        })
        .collect();
    objects.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out: Vec<IndexRange> = Vec::with_capacity(objects.len());
    let mut iter = objects.into_iter();
    let (_, mut last_end, mut last) = iter.next().expect("ranges is non-empty");
    for (start, end, range) in iter {
        let overlaps = if merge_consecutive {
            last_end >= start
        } else {
            last_end > start
        };
        if overlaps {
            if last_end < end {
                last_end = end;
                last.high = range.high;
                last.high_exclusive = range.high_exclusive;
            }
        } else {
            out.push(last);
            last_end = end;
            last = range;
        }
    }
    out.push(last);
    out
}

/// One condition classified against one index column.
struct ColumnPoints {
    points: Vec<Point>,
    /// Whether the condition is an `=`/`IN`, which is what lets the range
    /// builder move on to the next index column (Go's `eqOrInCount`).
    eq_or_in: bool,
}

/// Whether an expression names this column, ignoring any qualifier.
fn is_column(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Column(path) => path
            .last()
            .is_some_and(|last| last.eq_ignore_ascii_case(name)),
        Expr::Paren(inner) => is_column(inner, name),
        _ => false,
    }
}

struct RangeColumnResolver<'a> {
    column: &'a RangeColumn,
    zone: &'a tidb_datatype::SessionTimeZone,
}

impl tidb_expr::rewriter::ColumnResolver for RangeColumnResolver<'_> {
    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        self.zone.clone()
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        path.last()
            .is_some_and(|last| last.eq_ignore_ascii_case(&self.column.name))
            .then(|| (0, self.column.field_type.clone(), 1))
    }
}

/// Whether Go's range builder may use this string comparison to narrow the
/// column. `RangeColumnsPruner.pruneRangeColumns` keeps every partition when
/// the comparison's derived collation differs from the partition column;
/// the sole exception is equality under a binary collation, which identifies
/// one exact byte value and therefore one partition.
fn comparison_collation_allows_range(
    condition: &Expr,
    column: &RangeColumn,
    zone: &tidb_datatype::SessionTimeZone,
    equality: bool,
) -> bool {
    if column.field_type.eval_type() != tidb_datatype::EvalType::String {
        return true;
    }
    let Ok(rewritten) = rewrite_expr_resolved(condition, &RangeColumnResolver { column, zone })
    else {
        return false;
    };
    let expression_collation = tidb_expr::collation_derive::collation_of_node(&rewritten);
    expression_collation == column.field_type.collation()
        || (equality && tidb_expr::expr_collation::is_bin_collation(expression_collation.name()))
}

/// A constant expression's value, when it is one.
fn constant_value(expr: &Expr, zone: &tidb_datatype::SessionTimeZone) -> Option<Datum> {
    match rewrite_expr_resolved(
        expr,
        &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
    ) {
        Ok(Expression::Constant(constant)) => constant.eval().ok(),
        // The rewriter only folds a bare literal into a `Constant`; anything
        // built out of literals -- `-100` is `unaryminus(100)`, and Go folds it
        // before the ranger ever sees it -- stays a `ScalarFunction`. Evaluating
        // it against no columns folds it here and fails for anything that
        // actually reads a column, which is exactly the constant test Go's
        // `FoldConstant` applies.
        Ok(_) => tidb_expr::eval(expr).ok(),
        Err(_) => None,
    }
}

/// Go `flip`: the operator with its operands swapped.
fn flip(op: BinaryOp) -> Option<BinaryOp> {
    Some(match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::NullEq => BinaryOp::NullEq,
        BinaryOp::Ne => BinaryOp::Ne,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Le => BinaryOp::Ge,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Ge => BinaryOp::Le,
        _ => return None,
    })
}

/// The comparison produced by Go `expression.PushDownNot` for `NOT cmp`.
fn negate_comparison(op: BinaryOp) -> Option<BinaryOp> {
    Some(match op {
        BinaryOp::Eq => BinaryOp::Ne,
        BinaryOp::Ne => BinaryOp::Eq,
        BinaryOp::Lt => BinaryOp::Ge,
        BinaryOp::Le => BinaryOp::Gt,
        BinaryOp::Gt => BinaryOp::Le,
        BinaryOp::Ge => BinaryOp::Lt,
        // `NOT (a <=> b)` is not an ordinary comparison inversion.
        _ => return None,
    })
}

/// Go `builder.buildFromBinOp`, for the comparison operators that reach an
/// index range.
/// Go `handleUnsignedCol`: what an UNSIGNED column does with a NEGATIVE
/// constant.
///
/// Every unsigned value is `>= 0`, so the comparison is decided before any
/// row is read. `> -1`, `>= -1` and `!= -1` are true of every non-NULL value
/// and collapse to `>= 0`; `= -1`, `< -1` and `<= -1` are true of none, and
/// `None` here is the empty range Go signals by returning no points -- the
/// scan becomes a `TableDual`, not a range over `[-inf, 0)`.
///
/// The sign test is on the DATUM's own kind, exactly as Go writes it: a
/// `UInt` datum is never negative, and a decimal is asked its own
/// `IsNegative` rather than being converted first.
fn handle_unsigned_col(
    field_type: &FieldType,
    value: Datum,
    op: BinaryOp,
) -> Option<(Datum, BinaryOp)> {
    let is_negative = match &value {
        Datum::Int(v) => *v < 0,
        Datum::Real(v) | Datum::Float32(v) => *v < 0.0,
        Datum::Decimal(d) => d.signum() < 0,
        _ => false,
    };
    if !field_type.is_unsigned() || !is_negative {
        return Some((value, op));
    }
    match op {
        BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Ne => {
            let zero = match value {
                Datum::Int(_) => Datum::UInt(0),
                Datum::Float32(_) => Datum::Float32(0.0),
                Datum::Real(_) => Datum::Real(0.0),
                Datum::Decimal(_) => Datum::Decimal(tidb_datatype::Decimal::from_int(0)),
                other => other,
            };
            Some((zero, BinaryOp::Ge))
        }
        _ => None,
    }
}

/// Go `handleBoundCol`: what a SIGNED column does with a constant past the
/// far end of its own domain.
///
/// A signed integer column can never hold more than `MaxInt64`, so `> that`
/// is empty and `<= that` is every row: Go saturates the bound to `MaxInt64`
/// rather than carrying a value the index codec cannot express.
fn handle_bound_col(
    field_type: &FieldType,
    value: Datum,
    op: BinaryOp,
) -> Option<(Datum, BinaryOp)> {
    use tidb_datatype::FieldTypeCode;
    if field_type.is_unsigned() {
        return Some((value, op));
    }
    match field_type.code() {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong => {
            if matches!(value, Datum::UInt(v) if v > i64::MAX as u64) {
                return match op {
                    BinaryOp::Gt | BinaryOp::Ge => None,
                    BinaryOp::Ne | BinaryOp::Le | BinaryOp::Lt => {
                        Some((Datum::Int(i64::MAX), BinaryOp::Le))
                    }
                    _ => Some((value, op)),
                };
            }
        }
        FieldTypeCode::Float => {
            let as_f64 = match &value {
                Datum::Real(v) | Datum::Float32(v) => Some(*v),
                _ => None,
            };
            if let Some(v) = as_f64 {
                if v > f64::from(f32::MAX) {
                    return match op {
                        BinaryOp::Gt | BinaryOp::Ge => None,
                        BinaryOp::Ne | BinaryOp::Le | BinaryOp::Lt => {
                            Some((Datum::Float32(f64::from(f32::MAX)), BinaryOp::Le))
                        }
                        _ => Some((value, op)),
                    };
                } else if v < -f64::from(f32::MAX) {
                    return match op {
                        BinaryOp::Le | BinaryOp::Lt => None,
                        BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Ne => {
                            Some((Datum::Float32(-f64::from(f32::MAX)), BinaryOp::Ge))
                        }
                        _ => Some((value, op)),
                    };
                }
            }
        }
        _ => {}
    }
    Some((value, op))
}

fn points_from_bin_op(op: BinaryOp, value: Datum) -> Option<Vec<Point>> {
    // A NULL constant makes every ordinary comparison unknown, so no row
    // qualifies: Go returns no points at all, an empty range set.
    if value == Datum::Null && op != BinaryOp::NullEq {
        return Some(Vec::new());
    }
    Some(match op {
        BinaryOp::NullEq if value == Datum::Null => vec![
            Point::start(Datum::Null, false),
            Point::end(Datum::Null, false),
        ],
        BinaryOp::Eq | BinaryOp::NullEq => {
            vec![Point::start(value.clone(), false), Point::end(value, false)]
        }
        BinaryOp::Ne => vec![
            Point::start(Datum::MinNotNull, false),
            Point::end(value.clone(), true),
            Point::start(value, true),
            Point::end(Datum::MaxValue, false),
        ],
        BinaryOp::Lt => vec![
            Point::start(Datum::MinNotNull, false),
            Point::end(value, true),
        ],
        BinaryOp::Le => vec![
            Point::start(Datum::MinNotNull, false),
            Point::end(value, false),
        ],
        BinaryOp::Gt => vec![
            Point::start(value, true),
            Point::end(Datum::MaxValue, false),
        ],
        BinaryOp::Ge => vec![
            Point::start(value, false),
            Point::end(Datum::MaxValue, false),
        ],
        _ => return None,
    })
}

/// Go `builder.buildFromIn`: each list value becomes a point interval, the
/// intervals are sorted, and duplicates are dropped. A NULL in the list is
/// skipped, which is why `a IN (1, NULL)` is exactly `[1,1]`.
fn points_from_in(
    list: &[Expr],
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Vec<Point>, bool)> {
    let mut points = Vec::with_capacity(list.len() * 2);
    let mut has_null = false;
    for item in list {
        let value = constant_value(item, zone)?;
        if value == Datum::Null {
            has_null = true;
            continue;
        }
        points.push(Point::start(value.clone(), false));
        points.push(Point::end(value, false));
    }
    points.sort_by(point_cmp);
    // Go's duplicate removal: keep an endpoint only when it alternates
    // start/end with the one before it.
    let mut cur = 0;
    let mut front = 0;
    while front < points.len() {
        if points[cur].start == points[front].start {
            front += 1;
        } else {
            cur += 1;
            points.swap(cur, front);
            front += 1;
        }
    }
    let kept = if cur > 0 {
        cur + 1
    } else {
        usize::from(!points.is_empty())
    };
    points.truncate(kept);
    Some((points, has_null))
}

/// Go `builder.buildFromNot` for `IN`: the gaps between the list values,
/// starting at an excluded NULL.
fn points_from_not_in(list: &[Expr], zone: &tidb_datatype::SessionTimeZone) -> Option<Vec<Point>> {
    let (points, has_null) = points_from_in(list, zone)?;
    // `a NOT IN (1, NULL)` is never true, so Go builds no points at all.
    if has_null {
        return Some(Vec::new());
    }
    let mut out = Vec::with_capacity(points.len() + 2);
    let mut previous = Datum::Null;
    for pair in points.chunks_exact(2) {
        out.push(Point::start(previous, true));
        out.push(Point::end(pair[0].value.clone(), true));
        previous = pair[0].value.clone();
    }
    out.push(Point::start(previous, true));
    out.push(Point::end(Datum::MaxValue, false));
    Some(out)
}

/// Go `cutPrefixForPoints`: cuts every endpoint to the key part's declared
/// prefix, and drops the exclusiveness of any endpoint the cut made
/// ambiguous.
///
/// Two cases lose exclusiveness, and Go spells out why:
///   * the endpoint was actually CUT, so it no longer names one value but
///     every value behind that prefix;
///   * a START endpoint whose value already REACHES the prefix length --
///     `s > 'abc'` on `KEY (s(3))` must still read the `'abc'` entries,
///     because `'abcdef'` is filed under exactly that key.
///
/// The result is a superset of the qualifying rows, which the residual
/// `WHERE` above the scan then filters back down.
fn cut_prefix_for_points(points: &mut [Point], column: &RangeColumn) {
    if column.prefix_len == UNSPECIFIED_LENGTH {
        return;
    }
    for point in points.iter_mut() {
        let cut = cut_datum_by_prefix_len(&mut point.value, column.prefix_len, &column.field_type);
        if cut
            || (point.start
                && reaches_prefix_len(&point.value, column.prefix_len, &column.field_type))
        {
            point.excl = false;
        }
    }
}

/// Go `builder.build` for one condition against one index column: the
/// condition's endpoints on that column, or `None` when the condition is not
/// an access condition for it.
///
/// Go cuts a prefix key part at the tail of every `build` arm; doing it once
/// here, on the way out, is the same cut with one place to get it right.
fn points_for_condition(
    condition: &Expr,
    column: &RangeColumn,
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> Option<ColumnPoints> {
    let mut column_points = points_on_column(condition, column, zone, like_default_escape)?;
    cut_prefix_for_points(&mut column_points.points, column);
    // Go's `conditionChecker` rejects a condition that bounds nothing --- a
    // LIKE with no literal prefix, an IS NOT NULL --- and `points.go` signals
    // the same by returning the full range. A range spanning the whole index
    // is no better than a full scan, so such a condition stays a filter
    // rather than turning a scan into a range scan over everything.
    if column_points.points == full_range() || column_points.points == not_null_full_range() {
        return None;
    }
    // Go `ExtractEqAndInCondition`'s value-info drop. `allEqOrIn` accepts a
    // bare `IS NULL`, but the very next line asks `extractValueInfo` for the
    // condition's constant and clears the access slot when that constant is
    // NULL -- so `b IS NULL` does NOT advance the walk to the next index
    // column, and `a = 1 AND b IS NULL AND c > 1` reads `[1 NULL,1 NULL]`
    // rather than `(1 NULL 1,1 NULL +inf]`.
    //
    // `extractValueInfo` only ever looks at the TOP-LEVEL function, which is
    // why the same `IS NULL` inside a disjunction keeps its equality
    // standing: `(b IS NULL OR b = 2) AND c > 1` does reach `c`. Applying the
    // rule here, on the conjunct the CNF walk sees, and not inside
    // [`points_on_column`], is what keeps those two cases apart.
    if is_bare_null_test(condition, zone) {
        column_points.eq_or_in = false;
    }
    Some(column_points)
}

/// Whether this conjunct is, at its top level, a test for NULL -- `IS NULL`
/// or `<=> NULL`. Go reaches the same two through `extractValueInfo`'s
/// `ast.IsNull` arm and `getPotentialEqOrInColOffset`'s explicit
/// `NullEQ && val.IsNull()` rejection.
fn is_bare_null_test(condition: &Expr, zone: &tidb_datatype::SessionTimeZone) -> bool {
    match condition {
        Expr::Paren(inner) => is_bare_null_test(inner, zone),
        Expr::Is {
            target: IsTarget::Null,
            not: false,
            ..
        } => true,
        Expr::Binary(BinaryOp::NullEq, lhs, rhs) => {
            constant_value(lhs, zone) == Some(Datum::Null)
                || constant_value(rhs, zone) == Some(Datum::Null)
        }
        _ => false,
    }
}

/// The raw endpoints one condition puts on one column, before the
/// bounds-nothing check above.
fn points_on_column(
    condition: &Expr,
    column: &RangeColumn,
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> Option<ColumnPoints> {
    let name = column.name.as_str();
    match condition {
        Expr::Paren(inner) => points_on_column(inner, column, zone, like_default_escape),
        // Go runs `PushDownNot` before the ranger, so `NOT (a < 5)` arrives
        // here as `a >= 5`. The Rust AST intentionally preserves the source
        // spelling; normalize the same comparison at the range boundary.
        Expr::Unary(UnaryOp::Not | UnaryOp::NotKeyword, inner) => {
            let mut inner = inner.as_ref();
            while let Expr::Paren(next) = inner {
                inner = next;
            }
            let Expr::Binary(op, lhs, rhs) = inner else {
                return None;
            };
            let normalized = Expr::Binary(negate_comparison(*op)?, lhs.clone(), rhs.clone());
            points_on_column(&normalized, column, zone, like_default_escape)
        }
        // Go `buildFromScalarFunc`'s `ast.LogicAnd` / `ast.LogicOr` arms. A
        // boolean connective over ONE index column is still a point set on
        // that column: `b = 1 OR b = 2` is the union of the two, and
        // `b > 1 AND b < 9` the intersection. Without these arms every such
        // conjunct fell through to `None` and the whole disjunction became a
        // filter, so `a = 1 AND (b = 1 OR b = 2) AND c > 1` read `[1,1]`
        // where Go reads `(1 1 1,1 1 +inf], (1 2 1,1 2 +inf]`.
        //
        // Neither side may be dropped: if EITHER side puts no points on this
        // column the connective says nothing about it, and for an `OR` that
        // is not merely a lost opportunity but a WRONG range -- keeping only
        // the side that parsed would exclude rows the other side admits.
        Expr::Binary(op @ (BinaryOp::LogicAnd | BinaryOp::LogicOr), lhs, rhs) => {
            let lhs = points_on_column(lhs, column, zone, like_default_escape)?;
            let rhs = points_on_column(rhs, column, zone, like_default_escape)?;
            let points = if matches!(op, BinaryOp::LogicAnd) {
                intersection(&lhs.points, &rhs.points)
            } else {
                union_points(&lhs.points, &rhs.points)
            };
            // Go `allEqOrIn`, exactly: an `OR` counts as this column's
            // equality slot when EVERY disjunct does, and an `AND` never does.
            //
            // This is what lets the walk keep going. `b = 1 OR b = 2` pins b
            // to a finite set of single points, so Go moves on to `c` and
            // reads `(1 1 1,1 1 +inf], (1 2 1,1 2 +inf]`; treating the `OR` as
            // a spanning interval instead stopped the walk at b and read
            // `[1 1,1 1], [1 2,1 2]` -- a SUPERSET, filtered correctly by the
            // residual predicate but reading every `c` for each `b`.
            //
            // `AND` returning false is Go's rule and not an oversight: the
            // only `AND` this arm ever sees is one nested inside an `OR`
            // branch (a top-level conjunction is flattened before it gets
            // here), and Go's `getPotentialEqOrInColOffset` gives up on the
            // whole disjunction the moment one branch is a conjunction.
            let eq_or_in = matches!(op, BinaryOp::LogicOr) && lhs.eq_or_in && rhs.eq_or_in;
            Some(ColumnPoints { points, eq_or_in })
        }
        Expr::Binary(op, lhs, rhs) => {
            if !comparison_collation_allows_range(
                condition,
                column,
                zone,
                matches!(op, BinaryOp::Eq | BinaryOp::NullEq),
            ) {
                return None;
            }
            let (op, value) = if is_column(lhs, name) {
                (*op, constant_value(rhs, zone)?)
            } else if is_column(rhs, name) {
                (flip(*op)?, constant_value(lhs, zone)?)
            } else {
                return None;
            };
            let eq_or_in = matches!(op, BinaryOp::Eq);
            // Go `buildFromBinOp` runs both domain fixups BEFORE building any
            // point, and a `false` from either is Go's `return nil` -- no
            // points, so no range, so no row. Doing it after the conversion
            // instead is what left `a < -1` on an UNSIGNED column reading
            // `[-inf,0)`: the constant converted to 0 and the interval stayed.
            let Some((value, op)) = handle_unsigned_col(&column.field_type, value, op) else {
                return Some(ColumnPoints {
                    points: Vec::new(),
                    eq_or_in,
                });
            };
            let Some((value, op)) = handle_bound_col(&column.field_type, value, op) else {
                return Some(ColumnPoints {
                    points: Vec::new(),
                    eq_or_in,
                });
            };
            Some(ColumnPoints {
                points: points_from_bin_op(op, value)?,
                eq_or_in,
            })
        }
        Expr::In { expr, list, not } => {
            if !is_column(expr, name) {
                return None;
            }
            if !comparison_collation_allows_range(condition, column, zone, !*not) {
                return None;
            }
            let points = if *not {
                points_from_not_in(list, zone)?
            } else {
                points_from_in(list, zone)?.0
            };
            Some(ColumnPoints {
                points,
                eq_or_in: !*not,
            })
        }
        // Go rewrites BETWEEN into `>= AND <=` before the ranger sees it, and
        // NOT BETWEEN into `< OR >`; the resulting points are these.
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => {
            if !is_column(expr, name) {
                return None;
            }
            let low = constant_value(low, zone)?;
            let high = constant_value(high, zone)?;
            let points = if *not {
                union_points(
                    &points_from_bin_op(BinaryOp::Lt, low)?,
                    &points_from_bin_op(BinaryOp::Gt, high)?,
                )
            } else {
                intersection(
                    &points_from_bin_op(BinaryOp::Ge, low)?,
                    &points_from_bin_op(BinaryOp::Le, high)?,
                )
            };
            Some(ColumnPoints {
                points,
                eq_or_in: false,
            })
        }
        Expr::Like {
            expr,
            pattern,
            not,
            ilike,
            escape,
        } => {
            // Go reports NOT LIKE and ILIKE as unsupported for ranges.
            if *not || *ilike || !is_column(expr, name) {
                return None;
            }
            // The COLUMN's collation, not the literal's, decides both how the
            // bounds sort and whether a `_` excludes the low bound. Go reads
            // it from ARGUMENT 0 (`newBuildFromPatternLike`: `tpOfPattern :=
            // expr.GetArgs()[0].GetType(...)`, and the pad-space test uses
            // `expr.CharsetAndCollation()`, the DERIVED collation) -- which is
            // the column's, because a column's coercibility is IMPLICIT and a
            // literal's is COERCIBLE, so the column always wins the merge.
            //
            // Taking the LITERAL's collation instead read `utf8mb4_bin` off
            // `'abc_%'` even for a `VARBINARY` key, which is a PAD SPACE
            // collation and so silently kept the inclusive low bound.
            let bytes = match constant_value(pattern, zone)? {
                Datum::String(value) => value.bytes().to_vec(),
                Datum::Bytes(value) => value,
                _ => return None,
            };
            let collation = column.field_type.collation();
            let pattern = String::from_utf8(bytes).ok()?;
            Some(ColumnPoints {
                points: points_from_like(
                    &pattern,
                    escape.unwrap_or(like_default_escape),
                    collation,
                    column,
                ),
                eq_or_in: false,
            })
        }
        Expr::Is {
            expr,
            target: IsTarget::Null,
            not,
        } => {
            if !is_column(expr, name) {
                return None;
            }
            let points = if *not {
                not_null_full_range()
            } else {
                vec![
                    Point::start(Datum::Null, false),
                    Point::end(Datum::Null, false),
                ]
            };
            // `ast.IsNull` is in Go's `allEqOrIn` and in
            // `getPotentialEqOrInColOffset`'s switch: `b IS NULL` pins b to
            // the single NULL point, so the walk continues to the next index
            // column. `IS NOT NULL` bounds nothing and does not.
            Some(ColumnPoints {
                points,
                eq_or_in: !*not,
            })
        }
        _ => None,
    }
}

/// The access ranges of one CNF conjunct list over one index, with the
/// conjunct indices that were consumed as access conditions.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexRanges<'a> {
    /// The derived ranges. Empty means no row qualifies.
    pub ranges: Vec<IndexRange>,
    /// How many conjuncts became access conditions (Go's `len(AccessConds)`).
    pub access_count: usize,
    /// How many index columns the ranges span, Go's `EqOrInCount` plus the
    /// spanning column. Used to prefer the index that reaches deepest.
    pub column_count: usize,
    /// The index POSITIONS the access conditions constrain, which is what
    /// Go's `accessCondsColMap` holds (`ExtractCol2Len(path.AccessConds,
    /// path.IdxCols, path.IdxColLens)`); skyline pruning compares candidates
    /// on this set. Sorted and unique.
    ///
    /// For a CNF walk this is the leading `column_count` positions, because
    /// the walk consumes conditions strictly in index-column order. For a DNF
    /// it is the UNION over branches, because Go's access condition there is
    /// the whole disjunction and every column it names is in the map.
    pub access_columns: Vec<usize>,
    /// Go `AccessPath.EqOrInCondCount` as `candidatePath.equalPredicateCount`
    /// reports it: how many leading index columns an `=`/`IN` pinned.
    pub eq_or_in_count: usize,
    /// The conjuncts that did NOT become access conditions, which Go splits
    /// into `IndexFilters` and `TableFilters` (`splitIndexFilterConditions`)
    /// once it knows which columns the index stores.
    pub residual: Vec<&'a Expr>,
}

/// Go `detachCNFCondAndBuildRangeForIndex`, reduced to the column walk:
/// equalities and `IN`s pin the leading index columns one at a time, then
/// every remaining condition on the next column is intersected into one
/// spanning interval.
fn build_cnf_ranges<'a>(
    index_columns: &[RangeColumn],
    conditions: &[&'a Expr],
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> IndexRanges<'a> {
    let mut consumed = vec![false; conditions.len()];
    let mut eq_in_points: Vec<Vec<Point>> = Vec::new();
    let mut access_count = 0;

    // Go `ExtractEqAndInCondition`: walk the index columns in order and stop
    // at the first one with no equality or IN.
    for key_part in index_columns {
        let mut points: Option<Vec<Point>> = None;
        for (i, condition) in conditions.iter().enumerate() {
            if consumed[i] {
                continue;
            }
            let Some(column) = points_for_condition(condition, key_part, zone, like_default_escape)
            else {
                continue;
            };
            if !column.eq_or_in {
                continue;
            }
            // Several equalities on one column intersect, so `a = 1 AND a = 2`
            // is empty rather than the first one alone.
            points = Some(match points {
                Some(existing) => intersection(&existing, &column.points),
                None => column.points,
            });
            consumed[i] = true;
            access_count += 1;
        }
        let Some(points) = points else { break };
        eq_in_points.push(points);
    }
    let eq_in_count = eq_in_points.len();

    // Go `buildRangeOnColsByCNFCond`: the column after the equality prefix
    // takes every remaining access condition, intersected together.
    let mut tail: Option<Vec<Point>> = None;
    if eq_in_count < index_columns.len() {
        let key_part = &index_columns[eq_in_count];
        for (i, condition) in conditions.iter().enumerate() {
            if consumed[i] {
                continue;
            }
            let Some(column) = points_for_condition(condition, key_part, zone, like_default_escape)
            else {
                continue;
            };
            tail = Some(intersection(
                &tail.unwrap_or_else(full_range),
                &column.points,
            ));
            consumed[i] = true;
            access_count += 1;
        }
    }

    // Go `points2Ranges`/`appendPoints2Ranges` convert every endpoint into the
    // column's own type first, which is where unsigned narrowing and overflow
    // clamping happen.
    for (i, points) in eq_in_points.iter_mut().enumerate() {
        convert_points_in_place(points, &index_columns[i].field_type);
    }
    if let Some(tail) = tail.as_mut() {
        convert_points_in_place(tail, &index_columns[eq_in_count].field_type);
    }

    let mut ranges: Vec<IndexRange> = Vec::new();
    let mut column_count = 0;
    for (i, points) in eq_in_points.iter().enumerate() {
        ranges = if i == 0 {
            points_to_ranges(points, &index_columns[i])
        } else {
            append_points_to_ranges(&ranges, points)
        };
        column_count += 1;
    }
    if let Some(tail) = tail {
        ranges = if eq_in_count == 0 {
            points_to_ranges(&tail, &index_columns[eq_in_count])
        } else {
            append_points_to_ranges(&ranges, &tail)
        };
        column_count += 1;
    }
    // Go `detachCNFCondAndBuildRangeForIndex`: `if hasPrefix(d.lengths) {
    // ranges = UnionRanges(...) }`. Cutting can map two distinct points onto
    // one -- `a IN ('abcdef', 'abcxyz')` over `KEY (a(3))` becomes `["abc",
    // "abc"]` twice -- and Go prints ONE range for it. Only the prefix case
    // unions, so an ordinary index's range list is untouched.
    if index_columns
        .iter()
        .any(|column| column.prefix_len != UNSPECIFIED_LENGTH)
    {
        ranges = union_ranges(ranges, true);
    }
    IndexRanges {
        ranges,
        access_count,
        column_count,
        // The walk consumes conditions in index-column order and stops at the
        // first unconstrained column, so the constrained positions are always
        // the leading `column_count` of them.
        access_columns: (0..column_count).collect(),
        eq_or_in_count: eq_in_count,
        residual: conditions
            .iter()
            .enumerate()
            .filter(|(i, _)| !consumed[*i])
            .map(|(_, condition)| *condition)
            .collect(),
    }
}

/// Flattens an `AND` chain into its conjuncts.
fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Paren(inner) => collect_conjuncts(inner, out),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            collect_conjuncts(lhs, out);
            collect_conjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// Flattens an `OR` chain into its disjuncts.
fn collect_disjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Paren(inner) => collect_disjuncts(inner, out),
        Expr::Binary(BinaryOp::LogicOr, lhs, rhs) => {
            collect_disjuncts(lhs, out);
            collect_disjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// Whether this conjunct is a top-level `OR`.
fn is_or(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(inner) => is_or(inner),
        Expr::Binary(BinaryOp::LogicOr, _, _) => true,
        _ => false,
    }
}

/// Go `detachDNFCondAndBuildRangeForIndex`: every branch of the `OR` must
/// yield access conditions of its own, otherwise the whole disjunction is a
/// filter and no range can be built. The branches' ranges are then unioned,
/// merging ranges that only touch --- which is what turns `a = 1 OR a = 2`
/// into `[1,2]`.
fn build_dnf_ranges<'a>(
    index_columns: &[RangeColumn],
    disjunct: &'a Expr,
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> Option<IndexRanges<'a>> {
    let mut branches = Vec::new();
    collect_disjuncts(disjunct, &mut branches);
    let mut ranges = Vec::new();
    let mut column_count = usize::MAX;
    // Go's `minAccessConds` (`detachDNFCondAndBuildRangeForIndex`), and
    // whether every branch consists only of `=`/`IN` predicates, which is
    // what `hasOnlyEqualPredicatesInDNF` decides for the whole disjunction.
    let mut min_access_conds = usize::MAX;
    let mut only_equal = true;
    let mut access_columns: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
    for branch in branches {
        let mut conjuncts = Vec::new();
        collect_conjuncts(branch, &mut conjuncts);
        let built = build_cnf_ranges(index_columns, &conjuncts, zone, like_default_escape);
        // A branch that constrains nothing, or that keeps a residual of its
        // own, makes the disjunction unusable for access.
        if built.access_count == 0 || built.access_count != conjuncts.len() {
            return None;
        }
        min_access_conds = min_access_conds.min(built.access_count);
        only_equal &= built.eq_or_in_count == built.access_count;
        // Go's access condition for a DNF is the WHOLE disjunction, so every
        // index column any branch names lands in `accessCondsColMap`.
        access_columns.extend(built.access_columns.iter().copied());
        if built.ranges.is_empty() {
            continue;
        }
        column_count = column_count.min(built.column_count);
        ranges.extend(built.ranges);
    }
    if min_access_conds == usize::MAX {
        min_access_conds = 0;
    }
    Some(IndexRanges {
        ranges: union_ranges(ranges, true),
        access_count: 1,
        column_count: if column_count == usize::MAX {
            0
        } else {
            column_count
        },
        access_columns: access_columns.into_iter().collect(),
        // Go `candidatePath.equalPredicateCount` for a DNF access condition.
        eq_or_in_count: if only_equal {
            min_access_conds
        } else {
            min_access_conds.saturating_sub(1)
        },
        // The DNF branch is only taken when the WHOLE `WHERE` is one `OR`, so
        // there is no conjunct left over beside it.
        residual: Vec::new(),
    })
}

/// Go `ranger.ExtractAccessConditionsForColumn` + `ranger.BuildColumnRange`
/// over ONE column, which is what `cardinality.getMaskAndRanges` calls down
/// the `ranger.ColumnRangeType` arm.
///
/// The whole conjunct list goes in, so every condition on that column is
/// intersected into one range set -- `a >= 3 AND a <= 7` becomes `[3,7]`, not
/// two independent half-lines. [`IndexRanges::residual`] names the conjuncts
/// the column did NOT take, which is how the caller rebuilds Go's `mask`.
///
/// `access_count == 0` is Go's `len(conds) == 0` fast path in
/// `BuildColumnRange`, which returns the FULL range and an empty mask; the
/// caller drops such a node instead, which the greedy cover cannot tell apart
/// (a zero mask covers nothing and is never selected).
pub(crate) fn detach_conds_for_column<'a>(
    column: &RangeColumn,
    conditions: &[&'a Expr],
    zone: &tidb_datatype::SessionTimeZone,
) -> IndexRanges<'a> {
    // `buildColumnRange` (`ranger.go:491-526`) intersects the point set of
    // EVERY condition it took, with no equality prefix and no per-column walk
    // -- there is only one column, so `a IN (1,2,3) AND a > 1` narrows to
    // `[2,2], [3,3]` rather than leaving the `>` behind as a filter.
    let mut points = full_range();
    let mut access_count = 0;
    let mut residual = Vec::new();
    for condition in conditions {
        // Go `ExtractAccessConditionsForColumn`: a condition belongs to the
        // column exactly when the point builder can turn it into points.
        match points_for_condition(condition, column, zone, b'\\') {
            Some(column_points) => {
                points = intersection(&points, &column_points.points);
                access_count += 1;
            }
            None => residual.push(*condition),
        }
    }
    let mut ranges = Vec::new();
    if access_count > 0 {
        convert_points_in_place(&mut points, &column.field_type);
        ranges = points_to_ranges(&points, column);
        if column.prefix_len != UNSPECIFIED_LENGTH {
            ranges = union_ranges(ranges, true);
        }
    }
    IndexRanges {
        ranges,
        access_count,
        column_count: usize::from(access_count > 0),
        access_columns: if access_count > 0 {
            vec![0]
        } else {
            Vec::new()
        },
        // `BuildColumnRange` has no equality prefix to report; the selectivity
        // caller is the only user of this entry point and never reads it.
        eq_or_in_count: 0,
        residual,
    }
}

/// Go `DetachCondAndBuildRangeForIndex`: the index ranges a `WHERE` implies
/// over one index's columns.
///
/// `None` means the `WHERE` constrains none of the index's columns, so a
/// range scan is no better than a full scan. `Some` with an empty range list
/// means the conditions are contradictory and no row qualifies.
pub(crate) fn detach_cond_and_build_range_for_index<'a>(
    index_columns: &[RangeColumn],
    where_clause: &'a Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<IndexRanges<'a>> {
    detach_cond_and_build_range_for_index_with_like_default_escape(
        index_columns,
        where_clause,
        zone,
        b'\\',
    )
}

/// The statement-aware form of [`detach_cond_and_build_range_for_index`].
/// It keeps `LIKE` range derivation aligned with the residual evaluator when
/// SQL omitted an `ESCAPE` clause.
pub(crate) fn detach_cond_and_build_range_for_index_with_like_default_escape<'a>(
    index_columns: &[RangeColumn],
    where_clause: &'a Expr,
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> Option<IndexRanges<'a>> {
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);

    // A lone top-level OR is the DNF case. Go also detaches an OR that is one
    // conjunct among several (`extractBestCNFItemRanges`); that selection is
    // deferred, so a mixed AND/OR reaches the CNF walk, where the OR simply
    // stays a filter.
    if conjuncts.len() == 1 && is_or(conjuncts[0]) {
        let built = build_dnf_ranges(index_columns, conjuncts[0], zone, like_default_escape)?;
        return (built.column_count > 0).then_some(built);
    }

    let built = build_cnf_ranges(index_columns, &conjuncts, zone, like_default_escape);
    (built.access_count > 0).then_some(built)
}

/// Go's `PredicateSimplification` / `unsatisfiable`
/// (`pkg/planner/core/rule/rule_predicate_simplification.go`): a `WHERE` reads
/// no row on ANY access path when some column carries an equality `col = c`
/// that another binary comparison on the same column contradicts.
///
/// This is the index-INDEPENDENT `TableDual`: `b = 1 AND b = 2` reads nothing
/// no matter which columns are indexed, so Go plans `TableDual rows:0` before
/// any path is costed, and does the same for a partition key
/// (`a = 2 AND a = 3` over a partitioned table). It is distinct from the
/// empty-range short-circuit in [`detach_cond_and_build_range_for_index`],
/// which fires only for the column an access path was chosen on.
///
/// The EQUALITY gate is Go's own. `unsatisfiable` pairs an equality
/// (`equalPredicate`) only with another BINARY COMPARISON
/// (`binaryComparisonPredicate`: `=`, `<>`, `<`, `>`, `<=`, `>=`) -- never with
/// an `IN`, a `BETWEEN` or an `OR` -- and requires one side of the pair to be
/// an equality. So `b > 10 AND b < 1` (no equality) stays an ordinary filter,
/// matching Go which leaves it a `TableFullScan` on a non-indexed column, and
/// `b = 1 AND b IN (2, 3)` is not proven contradictory because the other side
/// is an `IN` rather than a binary comparison.
///
/// Intersecting the equality's single point with each other comparison's point
/// set and asking whether the result is empty is exactly Go's pairwise check:
/// the intersection drops the equality's value iff some `c1 <op> c2` is false.
pub(crate) fn where_is_unsatisfiable(
    columns: &[(String, FieldType)],
    where_clause: &Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    // A lone top-level OR is a disjunction, so a contradictory branch does not
    // make the whole predicate false. A top-level AND is already flattened into
    // several conjuncts by `collect_conjuncts`.
    if conjuncts.len() == 1 && is_or(conjuncts[0]) {
        return false;
    }
    columns
        .iter()
        .any(|(name, field_type)| column_conjuncts_contradict(name, field_type, &conjuncts, zone))
}

/// Whether the binary-comparison conjuncts on one column, taken together with
/// at least one equality among them, admit no value.
fn column_conjuncts_contradict(
    name: &str,
    field_type: &FieldType,
    conjuncts: &[&Expr],
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    let column = RangeColumn::whole(name.to_owned(), field_type.clone());
    let mut points = full_range();
    let mut has_equality = false;
    let mut access = false;
    for condition in conjuncts {
        let Some(column_points) = simple_comparison_points(condition, &column, zone) else {
            continue;
        };
        points = intersection(&points, &column_points.points);
        has_equality |= column_points.eq_or_in;
        access = true;
    }
    if !(access && has_equality) {
        return false;
    }
    convert_points_in_place(&mut points, &column.field_type);
    points_to_ranges(&points, &column).is_empty()
}

/// The points a TOP-LEVEL binary comparison (`=`, `<>`, `<`, `>`, `<=`, `>=`)
/// on `column` puts on it, with `eq_or_in` set only for the `=` Go's
/// `unsatisfiable` requires. `None` for every other shape -- an `IN`, a
/// `BETWEEN`, an `OR`, a `LIKE`, an `IS NULL` -- because Go's pairwise check
/// never uses those as the contradicting side.
fn simple_comparison_points(
    condition: &Expr,
    column: &RangeColumn,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<ColumnPoints> {
    match condition {
        Expr::Paren(inner) => simple_comparison_points(inner, column, zone),
        Expr::Binary(
            BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Gt | BinaryOp::Le | BinaryOp::Ge,
            ..,
        ) => points_on_column(condition, column, zone, b'\\'),
        _ => None,
    }
}

#[cfg(test)]
include!("index_range_tests.rs");
