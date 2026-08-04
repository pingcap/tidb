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
//!   * collation sort keys (`convertToSortKey`) and `handleUnsignedCol`'s
//!     signedness clamping.

use crate::index_prefix_cut::{cut_datum_by_prefix_len, reaches_prefix_len, UNSPECIFIED_LENGTH};
use crate::kv_table::IndexRange;
use std::cmp::Ordering;
use tidb_ast::{BinaryOp, Expr, IsTarget};
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

/// Go `convertPointsInPlace`.
fn convert_points_in_place(points: &mut [Point], target: &FieldType) {
    for p in points {
        convert_point_in_place(p, target);
    }
}

/// Go `points2Ranges`: consecutive endpoint pairs become single-column
/// ranges, dropping the empty ones.
fn points_to_ranges(points: &[Point]) -> Vec<IndexRange> {
    let mut ranges = Vec::with_capacity(points.len() / 2);
    for pair in points.chunks_exact(2) {
        let (low, high) = (&pair[0], &pair[1]);
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

/// A constant expression's value, when it is one.
fn constant_value(expr: &Expr, zone: &tidb_datatype::SessionTimeZone) -> Option<Datum> {
    match rewrite_expr_resolved(expr, &tidb_expr::rewriter::ZonedNoResolver(zone.clone())) {
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

/// Go `builder.newBuildFromPatternLike`: the literal prefix before the first
/// wildcard bounds the scan below, and that prefix's `PrefixNext` bounds it
/// above.
fn points_from_like(
    pattern: &str,
    escape: u8,
    collation: tidb_datatype::Collation,
    column: &RangeColumn,
) -> Vec<Point> {
    let string = |bytes: Vec<u8>| Datum::new_collation_string(bytes, collation);
    if pattern.is_empty() {
        let empty = string(Vec::new());
        return vec![Point::start(empty.clone(), false), Point::end(empty, false)];
    }
    let bytes = pattern.as_bytes();
    let mut low = Vec::with_capacity(bytes.len());
    let mut exact = true;
    let mut exclude = false;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == escape {
            i += 1;
            low.push(if i < bytes.len() { bytes[i] } else { escape });
            i += 1;
            continue;
        }
        if bytes[i] == b'%' {
            exact = false;
            break;
        }
        if bytes[i] == b'_' {
            // Go `points.go:775-788`: `_` matches exactly one character, so
            // the prefix itself is longer than the match and the low bound is
            // EXCLUSIVE -- but only under a NON-PAD-SPACE collation. Under a
            // PAD SPACE one the stored index key has its trailing spaces
            // trimmed, so `'abc'` and `'abc   '` are the same key and
            // excluding the bound would MISS `'abc   '`, a wrong answer rather
            // than a wider scan.
            //
            // `IsPadSpaceCollation` (`collate.go:363`) is a three-name
            // exception list, and `binary` is one of the three -- so a
            // `VARBINARY` key really does take the exclusive bound. Captured:
            //
            // ```text
            // create table b(a varbinary(20), key(a));
            // explain select * from b where a like 'abc_%'
            //   IndexRangeScan  range:("abc","abd")     <- low EXCLUSIVE
            // create table c(a varchar(20), key(a));
            // explain select * from c where a like 'abc_%'
            //   IndexRangeScan  range:["abc","abd")     <- low inclusive
            // ```
            // Spelled as Go's own three-name EXCEPTION list rather than as
            // its complement, so a collation added later defaults to PAD
            // SPACE -- the safe half -- exactly as it does in Go.
            exclude = matches!(
                collation,
                tidb_datatype::Collation::Binary
                    | tidb_datatype::Collation::Utf8Mb40900AiCi
                    | tidb_datatype::Collation::Utf8Mb40900Bin
            );
            exact = false;
            break;
        }
        low.push(bytes[i]);
        i += 1;
    }
    // No literal characters before the wildcard: nothing to bound the scan.
    if low.is_empty() {
        return not_null_full_range();
    }
    if exact {
        let value = string(low);
        return vec![Point::start(value.clone(), false), Point::end(value, false)];
    }
    // Go cuts the START point before deriving the upper bound from it
    // (`newBuildFromPatternLike`'s case 4-2 calls `cutPrefixForPoints` on the
    // start alone, then takes `PrefixNext` of the CUT value). Deriving the
    // bound first and cutting both would collapse `LIKE 'abcd%'` on
    // `KEY (a(3))` onto the empty `["abc","abc")`; Go prints `["abc","abd")`.
    let mut start = Point::start(string(low), exclude);
    cut_prefix_for_points(std::slice::from_mut(&mut start), column);
    let low = match &start.value {
        Datum::String(text) => text.bytes().to_vec(),
        Datum::Bytes(bytes) => bytes.clone(),
        _ => Vec::new(),
    };
    let high = prefix_next(low);
    vec![start, Point::end(string(high), true)]
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
) -> Option<ColumnPoints> {
    let mut column_points = points_on_column(condition, column, zone)?;
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
) -> Option<ColumnPoints> {
    let name = column.name.as_str();
    match condition {
        Expr::Paren(inner) => points_on_column(inner, column, zone),
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
            let lhs = points_on_column(lhs, column, zone)?;
            let rhs = points_on_column(rhs, column, zone)?;
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
                points: points_from_like(&pattern, escape.unwrap_or(b'\\'), collation, column),
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
            let Some(column) = points_for_condition(condition, key_part, zone) else {
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
            let Some(column) = points_for_condition(condition, key_part, zone) else {
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
            points_to_ranges(points)
        } else {
            append_points_to_ranges(&ranges, points)
        };
        column_count += 1;
    }
    if let Some(tail) = tail {
        ranges = if eq_in_count == 0 {
            points_to_ranges(&tail)
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
        let built = build_cnf_ranges(index_columns, &conjuncts, zone);
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
        match points_for_condition(condition, column, zone) {
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
        ranges = points_to_ranges(&points);
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
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);

    // A lone top-level OR is the DNF case. Go also detaches an OR that is one
    // conjunct among several (`extractBestCNFItemRanges`); that selection is
    // deferred, so a mixed AND/OR reaches the CNF walk, where the OR simply
    // stays a filter.
    if conjuncts.len() == 1 && is_or(conjuncts[0]) {
        let built = build_dnf_ranges(index_columns, conjuncts[0], zone)?;
        return (built.column_count > 0).then_some(built);
    }

    let built = build_cnf_ranges(index_columns, &conjuncts, zone);
    (built.access_count > 0).then_some(built)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_trace::range_text;

    /// The `range:` cell EXPLAIN would print for a derived range list, which
    /// is the exact text the Go corpus below was captured from.
    fn render(ranges: &[IndexRange]) -> String {
        ranges.iter().map(range_text).collect::<Vec<_>>().join(", ")
    }

    fn columns(names: &[&str]) -> Vec<RangeColumn> {
        names
            .iter()
            // The corpus table is `t(a int, b int, c int, s varchar(255))`;
            // the field type matters now that `convertPointInPlace` casts
            // every endpoint into the column's own type.
            .map(|name| {
                let code = if *name == "s" {
                    tidb_datatype::FieldTypeCode::VarString
                } else {
                    tidb_datatype::FieldTypeCode::LongLong
                };
                RangeColumn::whole((*name).to_owned(), FieldType::new(code))
            })
            .collect()
    }

    fn derive(index: &[&str], where_sql: &str) -> String {
        derive_with_columns(&columns(index), where_sql)
    }

    /// [`derive`] with the index columns' real field types supplied, which is
    /// what the unsigned/overflow corpus needs: Go's `convertPointInPlace`
    /// converts every range endpoint to the indexed column's type before
    /// building, and that conversion is the whole subject of those rows.
    fn derive_typed(index: &[(&str, FieldType)], where_sql: &str) -> String {
        let typed: Vec<RangeColumn> = index
            .iter()
            .map(|(name, ft)| RangeColumn::whole((*name).to_owned(), ft.clone()))
            .collect();
        derive_with_columns(&typed, where_sql)
    }

    /// [`derive`] over key parts that declare a PREFIX length, which is what
    /// the prefix corpus needs: Go cuts every endpoint to that length before
    /// building the range.
    fn derive_prefixed(index: &[(&str, FieldType, i64)], where_sql: &str) -> String {
        let typed: Vec<RangeColumn> = index
            .iter()
            .map(|(name, ft, prefix_len)| RangeColumn {
                name: (*name).to_owned(),
                field_type: ft.clone(),
                prefix_len: *prefix_len,
            })
            .collect();
        derive_with_columns(&typed, where_sql)
    }

    fn derive_with_columns(index: &[RangeColumn], where_sql: &str) -> String {
        let sql = format!("SELECT * FROM t WHERE {where_sql}");
        let stmt = tidb_parser::parse(&sql).expect("the corpus SQL parses");
        let tidb_ast::Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        let where_clause = select
            .where_clause
            .as_ref()
            .expect("the corpus has a WHERE");
        match detach_cond_and_build_range_for_index(
            index,
            where_clause,
            &tidb_datatype::SessionTimeZone::utc(),
        ) {
            Some(built) => render(&built.ranges),
            None => "<no range>".to_owned(),
        }
    }

    /// Every `range:` cell Go's EXPLAIN prints for these `WHERE` shapes,
    /// captured with `pkg/executor/zz_dump_ranges_test.go` against a mock
    /// store, with the index forced by `USE INDEX` so the text measures range
    /// derivation rather than stats-less plan choice.
    ///
    /// `""` is Go proving the conditions contradictory (it plans a TableDual
    /// and prints no scan at all), which this derivation reports as an empty
    /// range list.
    const GO_CORPUS: &[(&[&str], &str, &str)] = &[
        // single-column comparisons
        (&["a"], "a = 1", "[1,1]"),
        (&["a"], "a > 1", "(1,+inf]"),
        (&["a"], "a >= 1", "[1,+inf]"),
        (&["a"], "a < 1", "[-inf,1)"),
        (&["a"], "a <= 1", "[-inf,1]"),
        (&["a"], "a <> 1", "[-inf,1), (1,+inf]"),
        (&["a"], "1 = a", "[1,1]"),
        (&["a"], "1 < a", "(1,+inf]"),
        (&["a"], "a > 1 and a < 10", "(1,10)"),
        (&["a"], "a >= 1 and a <= 10", "[1,10]"),
        (&["a"], "a > 1 and a > 5", "(5,+inf]"),
        (&["a"], "a > 10 and a < 1", ""),
        (&["a"], "a = 1 and a = 2", ""),
        (&["a"], "a is null", "[NULL,NULL]"),
        // BETWEEN
        (&["a"], "a between 1 and 10", "[1,10]"),
        (&["a"], "a not between 1 and 10", "[-inf,1), (10,+inf]"),
        (&["a"], "a between 10 and 1", ""),
        // IN lists
        (&["a"], "a in (1)", "[1,1]"),
        (&["a"], "a in (1, 2, 3)", "[1,1], [2,2], [3,3]"),
        (&["a"], "a in (3, 1, 2, 1)", "[1,1], [2,2], [3,3]"),
        (&["a"], "a not in (1, 2)", "(NULL,1), (2,+inf]"),
        (&["a"], "a in (1, null)", "[1,1]"),
        // composite index prefixes
        (&["a", "b"], "a = 1 and b = 2", "[1 2,1 2]"),
        (&["a", "b"], "a = 1 and b > 2", "(1 2,1 +inf]"),
        (&["a", "b"], "a = 1 and b >= 2 and b < 8", "[1 2,1 8)"),
        (&["a", "b"], "a = 1 and b between 2 and 8", "[1 2,1 8]"),
        (&["a", "b"], "a = 1 and b in (2, 3)", "[1 2,1 2], [1 3,1 3]"),
        (&["a", "b"], "a in (1, 2) and b = 3", "[1 3,1 3], [2 3,2 3]"),
        (&["a", "b"], "a > 1 and b = 2", "(1,+inf]"),
        (&["a", "b"], "a = 1", "[1,1]"),
        (
            &["a", "b", "c"],
            "a = 1 and b = 2 and c = 3",
            "[1 2 3,1 2 3]",
        ),
        (
            &["a", "b", "c"],
            "a = 1 and b = 2 and c > 3",
            "(1 2 3,1 2 +inf]",
        ),
        (&["a", "b", "c"], "a = 1 and c = 3", "[1,1]"),
        (
            &["a", "b", "c"],
            "a = 1 and b in (2, 3) and c = 4",
            "[1 2 4,1 2 4], [1 3 4,1 3 4]",
        ),
        // DNF / OR
        (&["a"], "a = 1 or a = 2", "[1,2]"),
        (&["a"], "a < 1 or a > 10", "[-inf,1), (10,+inf]"),
        (
            &["a", "b"],
            "(a = 1 and b = 2) or (a = 3 and b = 4)",
            "[1 2,1 2], [3 4,3 4]",
        ),
        (&["a"], "a = 1 or a in (2, 3)", "[1,3]"),
        (&["a"], "a > 5 or a > 1", "(1,+inf]"),
        (&["a"], "a in (1,2) or a = 5", "[1,2], [5,5]"),
        // LIKE
        (&["s"], "s like 'abc%'", "[\"abc\",\"abd\")"),
        (&["s"], "s like 'abc'", "[\"abc\",\"abc\"]"),
        (&["s"], "s like 'ab_c%'", "[\"ab\",\"ac\")"),
        (&["s"], "s = 'abc'", "[\"abc\",\"abc\"]"),
        (&["s"], "s > 'abc'", "(\"abc\",+inf]"),
        // conditions that leave a residual behind
        (&["a", "b", "c"], "a = 1 and c + 1 = 3", "[1,1]"),
        (&["s"], "s like 'x%' and a > 1", "[\"x\",\"y\")"),
    ];

    /// The differential: every derived range must render byte-for-byte as the
    /// text Go's own EXPLAIN prints for the same shape.
    #[test]
    fn derived_ranges_match_gos_explain_range_cell() {
        let mut mismatches = Vec::new();
        for (index, where_sql, expected) in GO_CORPUS {
            let got = derive(index, where_sql);
            if got != *expected {
                mismatches.push(format!(
                    "{:<10} {:<40} go={:<28} rust={}",
                    index.join(","),
                    where_sql,
                    expected,
                    got
                ));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} corpus shapes diverge from Go:\n{}",
            mismatches.len(),
            GO_CORPUS.len(),
            mismatches.join("\n")
        );
    }

    /// The corpus capture shows Go printing no `range:` cell for these, and
    /// this derivation likewise finds no access condition, so the read stays
    /// a full scan.
    #[test]
    fn shapes_go_plans_without_an_index_range_scan() {
        // No condition on the leading column: nothing to bound the scan.
        assert_eq!(derive(&["a", "b"], "b = 2"), "<no range>");
        // One OR branch constrains a column the index does not lead with, so
        // the disjunction as a whole bounds nothing.
        assert_eq!(derive(&["a", "b"], "a = 1 or b = 2"), "<no range>");
        // Conditions that span the whole index bound nothing, so they stay
        // filters rather than turning a full scan into a full range scan.
        assert_eq!(derive(&["a"], "a is not null"), "<no range>");
        assert_eq!(derive(&["s"], "s like '%abc'"), "<no range>");
        assert_eq!(derive(&["s"], "s like '%'"), "<no range>");
    }

    /// A negative bound is `unaryminus(literal)` in the AST, not a literal, so
    /// before constant folding reached [`constant_value`] every one of these
    /// derived no range at all and the read fell back to a full scan. Go folds
    /// the constant long before the ranger runs, so it has always ranged them.
    #[test]
    fn negative_bounds_derive_ranges() {
        assert_eq!(derive(&["a"], "a >= -2147483648"), "[-2147483648,+inf]");
        assert_eq!(derive(&["a"], "a < -1"), "[-inf,-1)");
        assert_eq!(derive(&["a"], "a > -100"), "(-100,+inf]");
        assert_eq!(derive(&["a"], "a < -1 and a < 1"), "[-inf,-1)");
    }

    /// Go `points.go` `buildFromScalarFunc`'s `ast.LogicOr` arm together with
    /// `detacher.go` `allEqOrIn`: an `OR` over ONE index column is that
    /// column's equality slot, so the walk keeps going to the column after it.
    ///
    /// Every expectation here is the `range:` cell
    /// `tests/integrationtest/r/util/ranger.result` records for the same
    /// statement over `t(a int, b int, c int, key(a, b, c))`.
    #[test]
    fn a_disjunction_on_one_index_column_still_pins_it() {
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and (b = 1 or b = 2) and c > 1"),
            "(1 1 1,1 1 +inf], (1 2 1,1 2 +inf]"
        );
        assert_eq!(
            derive(
                &["a", "b", "c"],
                "a = 1 and (b = 1 or b in (2, 3)) and c > 1"
            ),
            "(1 1 1,1 1 +inf], (1 2 1,1 2 +inf], (1 3 1,1 3 +inf]"
        );
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and (b is null or b = 2) and c > 1"),
            "(1 NULL 1,1 NULL +inf], (1 2 1,1 2 +inf]"
        );
        // A second condition on the same column intersects with the union, so
        // a disjunction that cannot hold is an EMPTY range list and not the
        // surviving branch alone.
        assert_eq!(
            derive(
                &["a", "b", "c"],
                "a = 1 and (b = 1 or b = 2) and b = 3 and c > 1"
            ),
            ""
        );
    }

    /// The CONTROL for the test above, and the reason `allEqOrIn` alone is not
    /// the rule: Go's `ExtractEqAndInCondition` asks `extractValueInfo` for
    /// the condition's constant right after `allEqOrIn` accepts it, and clears
    /// the access slot when that constant is NULL.
    ///
    /// So a BARE `IS NULL` does NOT advance the walk -- `c > 1` never reaches
    /// the range -- while the very same `IS NULL` inside a disjunction does.
    /// Reporting `IS NULL` as an equality everywhere passes the test above and
    /// breaks these two, which is exactly how the divergence was found.
    #[test]
    fn a_bare_is_null_does_not_advance_the_walk() {
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and b is null and c > 1"),
            "[1 NULL,1 NULL]"
        );
        assert_eq!(
            derive(
                &["a", "b", "c"],
                "a = 1 and b is null and b is null and c > 1"
            ),
            "[1 NULL,1 NULL]"
        );
        // A bare equality still advances it, so the rule above is about the
        // NULL constant and not about losing the third column generally.
        assert_eq!(
            derive(&["a", "b", "c"], "a = 1 and b = 2 and c > 1"),
            "(1 2 1,1 2 +inf]"
        );
    }

    fn ft(code: tidb_datatype::FieldTypeCode) -> FieldType {
        FieldType::new(code)
    }

    fn unsigned(code: tidb_datatype::FieldTypeCode) -> FieldType {
        FieldType::new(code).with_unsigned(true)
    }

    /// Go `TestIndexRangeForUnsignedAndOverflow`
    /// (`pkg/util/ranger/ranger_test.go:314`), all 19 rows, against the table
    ///
    /// ```sql
    /// create table t(
    ///   a smallint(5) unsigned, decimal_unsigned decimal unsigned,
    ///   float_unsigned float unsigned, double_unsigned double unsigned,
    ///   col_int bigint, col_float float, ...)
    /// ```
    ///
    /// `resultStr` is Go's `fmt.Sprintf("%v", res.Ranges)`; the expectation
    /// below is the same list in this crate's `range:`-cell rendering (outer
    /// brackets dropped, ranges joined by `", "`), and `""` is Go's empty
    /// range list.
    ///
    /// Every row here turns on `convertPointInPlace`: Go converts each range
    /// endpoint to the indexed column's type before building, so `a >= -2147483648`
    /// on an UNSIGNED column collapses to `[0,+inf]` rather than keeping the
    /// negative bound. This crate's derivation does not convert endpoints at
    /// all yet, so the rows that need it are `#[ignore]`d below with Go's
    /// answer asserted -- they are the spec for the conversion step.
    /// One index column of a corpus row: name, type code, and whether the
    /// column is UNSIGNED.
    type IndexColumnSpec = (&'static str, tidb_datatype::FieldTypeCode, bool);

    const GO_UNSIGNED_AND_OVERFLOW: &[(&[IndexColumnSpec], &str, &str)] = &[
        // (index columns as (name, type code, unsigned), expr, Go's ranges)
        (
            &[
                ("a", tidb_datatype::FieldTypeCode::Short, true),
                ("col_int", tidb_datatype::FieldTypeCode::LongLong, false),
            ],
            "a = 1 and a = 2",
            "",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (0, 1, 2)",
            "(NULL,0), (2,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (-1, 1, 2)",
            "(NULL,1), (2,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (-2, -1, 1, 2)",
            "(NULL,1), (2,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (111)",
            "[-inf,111), (111,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a not in (1, 2, 9223372036854775810)",
            "(NULL,1), (2,9223372036854775810), (9223372036854775810,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a >= -2147483648",
            "[0,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a > -2147483648",
            "[0,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a != -2147483648",
            "[0,+inf]",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a < -1 or a < 1",
            "[-inf,1)",
        ),
        (
            &[("a", tidb_datatype::FieldTypeCode::Short, true)],
            "a < -1 and a < 1",
            "",
        ),
        (
            &[(
                "decimal_unsigned",
                tidb_datatype::FieldTypeCode::NewDecimal,
                true,
            )],
            "decimal_unsigned > -100",
            "[0,+inf]",
        ),
        (
            &[("float_unsigned", tidb_datatype::FieldTypeCode::Float, true)],
            "float_unsigned > -100",
            "[0,+inf]",
        ),
        (
            &[(
                "double_unsigned",
                tidb_datatype::FieldTypeCode::Double,
                true,
            )],
            "double_unsigned > -100",
            "[0,+inf]",
        ),
        (
            &[("col_int", tidb_datatype::FieldTypeCode::LongLong, false)],
            "col_int != 9223372036854775808",
            "[-inf,+inf]",
        ),
        (
            &[("col_int", tidb_datatype::FieldTypeCode::LongLong, false)],
            "col_int > 9223372036854775808",
            "",
        ),
        (
            &[("col_int", tidb_datatype::FieldTypeCode::LongLong, false)],
            "col_int < 9223372036854775808",
            "[-inf,+inf]",
        ),
        (
            &[("col_float", tidb_datatype::FieldTypeCode::Float, false)],
            "col_float > 1000000000000000000000000000000000000000",
            "",
        ),
        (
            &[("col_float", tidb_datatype::FieldTypeCode::Float, false)],
            "col_float < -1000000000000000000000000000000000000000",
            "",
        ),
    ];

    fn derive_unsigned_row(index: &[IndexColumnSpec], where_sql: &str) -> String {
        let cols: Vec<(&str, FieldType)> = index
            .iter()
            .map(|(name, code, uns)| (*name, if *uns { unsigned(*code) } else { ft(*code) }))
            .collect();
        derive_typed(&cols, where_sql)
    }

    /// How many of [`GO_UNSIGNED_AND_OVERFLOW`]'s rows this derivation
    /// reproduces today. A ratchet, not a pass: the full table is asserted by
    /// the `#[ignore]`d test below, which names every row that is still wrong.
    #[test]
    fn unsigned_and_overflow_rows_that_already_match_go() {
        let mut matched = 0;
        let mut diverged = Vec::new();
        for (index, where_sql, expected) in GO_UNSIGNED_AND_OVERFLOW {
            let got = derive_unsigned_row(index, where_sql);
            if got == *expected {
                matched += 1;
            } else {
                diverged.push(format!("  {where_sql:<50} go={expected:<40} rust={got}"));
            }
        }
        // This assertion is a ratchet, not a pass: it records how many of Go's
        // 19 rows this derivation reproduces today. It must never fall.
        assert!(
            matched >= 7,
            "only {matched} of {} Go rows match; diverging rows:\n{}",
            GO_UNSIGNED_AND_OVERFLOW.len(),
            diverged.join("\n")
        );
    }

    /// The full Go table asserted verbatim. This fails until endpoint type
    /// conversion (`convertPointInPlace`) lands, and the failure message names
    /// every row that is still wrong -- that list is the work item.
    #[test]
    #[ignore = "12 of 19 rows still need Go's handleUnsignedCol signedness clamping and expression-level RefineCompareArgs for out-of-domain constants"]
    fn unsigned_and_overflow_ranges_match_go() {
        let mut mismatches = Vec::new();
        for (index, where_sql, expected) in GO_UNSIGNED_AND_OVERFLOW {
            let got = derive_unsigned_row(index, where_sql);
            if got != *expected {
                mismatches.push(format!("  {where_sql:<50} go={expected:<40} rust={got}"));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} Go rows diverge:\n{}",
            mismatches.len(),
            GO_UNSIGNED_AND_OVERFLOW.len(),
            mismatches.join("\n")
        );
    }

    /// Go `TestPrefixIndexRange` (`pkg/util/ranger/ranger_test.go:2342`), all
    /// 10 rows, against
    ///
    /// ```sql
    /// create table t(a varchar(50), b varchar(50), c text(50), d varbinary(50),
    ///   index idx_a(a(2)), index idx_ab(a(2), b(2)),
    ///   index idx_c(c(2)), index idx_d(d(2)))
    /// ```
    ///
    /// with `tidb_opt_prefix_index_single_scan = 1`.
    ///
    /// The declared `(2)` reaches the builder now, and 4 of these 10 rows
    /// pass. The other 6 diverge for a reason that has nothing to do with the
    /// prefix: this crate does not turn `IS NOT NULL` (or an `isnull(...)`
    /// call) into an ACCESS condition at all -- `points_for_condition` drops
    /// any condition whose points span the full range -- so Go's
    /// `[-inf,+inf]` and `[NULL,+inf]` come back as "no range". That is the
    /// same gap the module doc lists, and closing it is a different unit, so
    /// the row set stays recorded rather than half-asserted.
    const GO_PREFIX_INDEX_RANGE: &[(&[&str], &str, &str)] = &[
        (&["a"], "a is null", "[NULL,NULL]"),
        // accessConds is empty here: Go detaches nothing and falls back to the
        // full range, which this crate reports as "<no range>".
        (&["a"], "isnull(a) or a in (1,2,3,4)", "[NULL,+inf]"),
        (&["a"], "isnull(a) and a in (1,2,3,4)", "[NULL,NULL]"),
        (&["a"], "a is not null", "[-inf,+inf]"),
        (
            &["a", "b"],
            "a = 'a' and b is null",
            "[\"a\" NULL,\"a\" NULL]",
        ),
        (
            &["a", "b"],
            "a = 'a' and b is not null",
            "[\"a\" -inf,\"a\" +inf]",
        ),
        (&["c"], "c is null", "[NULL,NULL]"),
        (&["c"], "c is not null", "[-inf,+inf]"),
        (&["d"], "d is null", "[NULL,NULL]"),
        (&["d"], "d is not null", "[-inf,+inf]"),
    ];

    #[test]
    #[ignore = "6 of 10 rows need IS NOT NULL as an access condition, which this crate does not build; Go's answers are recorded above as that unit's spec"]
    fn prefix_index_ranges_match_go() {
        let mut mismatches = Vec::new();
        for (index, where_sql, expected) in GO_PREFIX_INDEX_RANGE {
            let cols: Vec<(&str, FieldType, i64)> = index
                .iter()
                .map(|name| (*name, ft(tidb_datatype::FieldTypeCode::VarString), 2))
                .collect();
            let got = derive_prefixed(&cols, where_sql);
            if got != *expected {
                mismatches.push(format!("  {where_sql:<40} go={expected:<28} rust={got}"));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} Go rows diverge:\n{}",
            mismatches.len(),
            GO_PREFIX_INDEX_RANGE.len(),
            mismatches.join("\n")
        );
    }

    /// The endpoint CUT itself, which `GO_PREFIX_INDEX_RANGE` does not
    /// exercise: its rows are all NULL boundaries and one two-character
    /// equality, so none of them is long enough to be cut.
    ///
    /// The `range:` cells are captured from real TiDB's `EXPLAIN` over
    /// `t(a varchar(20), b int, key idx(a(3)))`; the rows that are full
    /// scans there (Go's cost model declines the double read) were captured
    /// with the index forced, since range DERIVATION is what is measured.
    ///
    /// Every one of these is a SUPERSET of the qualifying rows, which is the
    /// property that makes the residual `WHERE` above the scan sufficient. An
    /// uncut `["abcdef","abcdef"]` would be a subset and would lose rows.
    const GO_PREFIX_CUT_RANGE: &[(&str, &str)] = &[
        // Cut to the prefix; a point stays a point.
        ("a = 'abcdef'", "[\"abc\",\"abc\"]"),
        // Shorter than the prefix: nothing to cut, and the point is exact.
        ("a = 'ab'", "[\"ab\",\"ab\"]"),
        // Cut START endpoints lose their exclusiveness, so `> 'abcdef'` still
        // reads the `'abc'` entries that stand for it.
        ("a > 'abcdef'", "[\"abc\",+inf]"),
        ("a >= 'abcdef'", "[\"abc\",+inf]"),
        // A value that REACHES the prefix without being cut loses it too.
        ("a > 'abc'", "[\"abc\",+inf]"),
        // An END endpoint that was cut becomes inclusive.
        ("a < 'abcdef'", "[-inf,\"abc\"]"),
        ("a <= 'abcdef'", "[-inf,\"abc\"]"),
        // A value shorter than the prefix keeps its exclusiveness on both
        // sides: no entry hides behind it.
        ("a > 'ab'", "(\"ab\",+inf]"),
        ("a < 'ab'", "[-inf,\"ab\")"),
        // An IN list cuts each point, and two values sharing the prefix
        // collapse onto one range.
        (
            "a in ('abcdef', 'zzz')",
            "[\"abc\",\"abc\"], [\"zzz\",\"zzz\"]",
        ),
        ("a in ('abcdef', 'abcxyz')", "[\"abc\",\"abc\"]"),
        // A LIKE whose literal prefix outruns the key part is cut the same
        // way, and the derived upper bound goes with it.
        ("a like 'abcd%'", "[\"abc\",\"abd\")"),
        ("a like 'ab%'", "[\"ab\",\"ac\")"),
    ];

    #[test]
    fn a_cut_endpoint_widens_the_range_it_bounds() {
        let mut mismatches = Vec::new();
        for (where_sql, expected) in GO_PREFIX_CUT_RANGE {
            let got = derive_prefixed(
                &[("a", ft(tidb_datatype::FieldTypeCode::VarString), 3)],
                where_sql,
            );
            if got != *expected {
                mismatches.push(format!("  {where_sql:<30} want={expected:<32} got={got}"));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} rows diverge:\n{}",
            mismatches.len(),
            GO_PREFIX_CUT_RANGE.len(),
            mismatches.join("\n")
        );
    }

    /// The control for the row above: the SAME conditions over a key part
    /// with no declared length keep their exclusiveness and their whole
    /// values. Without this, a cut applied unconditionally would pass the
    /// corpus above and quietly widen every ordinary index range.
    #[test]
    fn a_whole_key_part_is_never_cut() {
        for (where_sql, expected) in [
            ("a = 'abcdef'", "[\"abcdef\",\"abcdef\"]"),
            ("a > 'abcdef'", "(\"abcdef\",+inf]"),
            ("a < 'abcdef'", "[-inf,\"abcdef\")"),
            (
                "a in ('abcdef', 'abcxyz')",
                "[\"abcdef\",\"abcdef\"], [\"abcxyz\",\"abcxyz\"]",
            ),
        ] {
            assert_eq!(
                derive_typed(
                    &[("a", ft(tidb_datatype::FieldTypeCode::VarString))],
                    where_sql
                ),
                expected,
                "{where_sql}"
            );
        }
    }
}
