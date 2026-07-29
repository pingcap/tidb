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
//! DEFERRED (documented, each a superset --- the residual predicate still
//! filters, so the answer stays correct):
//!   * prefix indexes (`KEY (s(4))`): Go cuts points to the prefix length and
//!     re-unions; no prefix length reaches this crate's `KvIndex` yet.
//!   * the handle columns Go appends to a non-clustered index's tail, so
//!     `a = 1 AND b = 2 AND id > 5` on `(a, b)` reads `(1 2 5, 1 2 +inf]`.
//!   * `extractBestCNFItemRanges` / `chooseBetweenRangeAndPoint`: Go's
//!     cost-driven preference for one CNF item's DNF ranges over the
//!     leading-column ranges.
//!   * collation sort keys (`convertToSortKey`) and `handleUnsignedCol`'s
//!     signedness clamping.

use crate::kv_table::IndexRange;
use std::cmp::Ordering;
use tidb_ast::{BinaryOp, Expr, IsTarget};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, NoResolver};

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
fn constant_value(expr: &Expr) -> Option<Datum> {
    let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(expr, &NoResolver) else {
        return None;
    };
    constant.eval().ok()
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
fn points_from_in(list: &[Expr]) -> Option<(Vec<Point>, bool)> {
    let mut points = Vec::with_capacity(list.len() * 2);
    let mut has_null = false;
    for item in list {
        let value = constant_value(item)?;
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
fn points_from_not_in(list: &[Expr]) -> Option<Vec<Point>> {
    let (points, has_null) = points_from_in(list)?;
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
fn points_from_like(pattern: &str, escape: u8, collation: tidb_datatype::Collation) -> Vec<Point> {
    let string = |bytes: Vec<u8>| Datum::new_collation_string(bytes, collation);
    if pattern.is_empty() {
        let empty = string(Vec::new());
        return vec![Point::start(empty.clone(), false), Point::end(empty, false)];
    }
    let bytes = pattern.as_bytes();
    let mut low = Vec::with_capacity(bytes.len());
    let mut exact = true;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == escape {
            i += 1;
            low.push(if i < bytes.len() { bytes[i] } else { escape });
            i += 1;
            continue;
        }
        if bytes[i] == b'%' || bytes[i] == b'_' {
            // Go excludes the low bound for `_` only under a non-PAD-SPACE
            // collation; TiDB's own default collations all pad, so the bound
            // stays inclusive.
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
    let high = prefix_next(low.clone());
    vec![
        Point::start(string(low), false),
        Point::end(string(high), true),
    ]
}

/// Go `builder.build` for one condition against one index column: the
/// condition's endpoints on that column, or `None` when the condition is not
/// an access condition for it.
fn points_for_condition(condition: &Expr, column: &str) -> Option<ColumnPoints> {
    let column_points = points_on_column(condition, column)?;
    // Go's `conditionChecker` rejects a condition that bounds nothing --- a
    // LIKE with no literal prefix, an IS NOT NULL --- and `points.go` signals
    // the same by returning the full range. A range spanning the whole index
    // is no better than a full scan, so such a condition stays a filter
    // rather than turning a scan into a range scan over everything.
    if column_points.points == full_range() || column_points.points == not_null_full_range() {
        return None;
    }
    Some(column_points)
}

/// The raw endpoints one condition puts on one column, before the
/// bounds-nothing check above.
fn points_on_column(condition: &Expr, column: &str) -> Option<ColumnPoints> {
    match condition {
        Expr::Paren(inner) => points_for_condition(inner, column),
        Expr::Binary(op, lhs, rhs) => {
            let (op, value) = if is_column(lhs, column) {
                (*op, constant_value(rhs)?)
            } else if is_column(rhs, column) {
                (flip(*op)?, constant_value(lhs)?)
            } else {
                return None;
            };
            let eq_or_in = matches!(op, BinaryOp::Eq);
            Some(ColumnPoints {
                points: points_from_bin_op(op, value)?,
                eq_or_in,
            })
        }
        Expr::In { expr, list, not } => {
            if !is_column(expr, column) {
                return None;
            }
            let points = if *not {
                points_from_not_in(list)?
            } else {
                points_from_in(list)?.0
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
            if !is_column(expr, column) {
                return None;
            }
            let low = constant_value(low)?;
            let high = constant_value(high)?;
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
            if *not || *ilike || !is_column(expr, column) {
                return None;
            }
            // The pattern's own collation carries over to the bounds, so the
            // derived endpoints sort against the column exactly as an `=`
            // constant on the same column would.
            let (bytes, collation) = match constant_value(pattern)? {
                Datum::String(value) => (value.bytes().to_vec(), value.collation()),
                Datum::Bytes(value) => (value, tidb_datatype::Collation::Binary),
                _ => return None,
            };
            let pattern = String::from_utf8(bytes).ok()?;
            Some(ColumnPoints {
                points: points_from_like(&pattern, escape.unwrap_or(b'\\'), collation),
                eq_or_in: false,
            })
        }
        Expr::Is {
            expr,
            target: IsTarget::Null,
            not,
        } => {
            if !is_column(expr, column) {
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
            Some(ColumnPoints {
                points,
                eq_or_in: false,
            })
        }
        _ => None,
    }
}

/// The access ranges of one CNF conjunct list over one index, with the
/// conjunct indices that were consumed as access conditions.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexRanges {
    /// The derived ranges. Empty means no row qualifies.
    pub ranges: Vec<IndexRange>,
    /// How many conjuncts became access conditions (Go's `len(AccessConds)`).
    pub access_count: usize,
    /// How many index columns the ranges span, Go's `EqOrInCount` plus the
    /// spanning column. Used to prefer the index that reaches deepest.
    pub column_count: usize,
}

/// Go `detachCNFCondAndBuildRangeForIndex`, reduced to the column walk:
/// equalities and `IN`s pin the leading index columns one at a time, then
/// every remaining condition on the next column is intersected into one
/// spanning interval.
fn build_cnf_ranges(index_columns: &[(String, FieldType)], conditions: &[&Expr]) -> IndexRanges {
    let mut consumed = vec![false; conditions.len()];
    let mut eq_in_points: Vec<Vec<Point>> = Vec::new();
    let mut access_count = 0;

    // Go `ExtractEqAndInCondition`: walk the index columns in order and stop
    // at the first one with no equality or IN.
    for (name, _) in index_columns {
        let mut points: Option<Vec<Point>> = None;
        for (i, condition) in conditions.iter().enumerate() {
            if consumed[i] {
                continue;
            }
            let Some(column) = points_for_condition(condition, name) else {
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
        let name = &index_columns[eq_in_count].0;
        for (i, condition) in conditions.iter().enumerate() {
            if consumed[i] {
                continue;
            }
            let Some(column) = points_for_condition(condition, name) else {
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
    IndexRanges {
        ranges,
        access_count,
        column_count,
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
fn build_dnf_ranges(index_columns: &[(String, FieldType)], disjunct: &Expr) -> Option<IndexRanges> {
    let mut branches = Vec::new();
    collect_disjuncts(disjunct, &mut branches);
    let mut ranges = Vec::new();
    let mut column_count = usize::MAX;
    for branch in branches {
        let mut conjuncts = Vec::new();
        collect_conjuncts(branch, &mut conjuncts);
        let built = build_cnf_ranges(index_columns, &conjuncts);
        // A branch that constrains nothing, or that keeps a residual of its
        // own, makes the disjunction unusable for access.
        if built.access_count == 0 || built.access_count != conjuncts.len() {
            return None;
        }
        if built.ranges.is_empty() {
            continue;
        }
        column_count = column_count.min(built.column_count);
        ranges.extend(built.ranges);
    }
    Some(IndexRanges {
        ranges: union_ranges(ranges, true),
        access_count: 1,
        column_count: if column_count == usize::MAX {
            0
        } else {
            column_count
        },
    })
}

/// Go `DetachCondAndBuildRangeForIndex`: the index ranges a `WHERE` implies
/// over one index's columns.
///
/// `None` means the `WHERE` constrains none of the index's columns, so a
/// range scan is no better than a full scan. `Some` with an empty range list
/// means the conditions are contradictory and no row qualifies.
pub(crate) fn detach_cond_and_build_range_for_index(
    index_columns: &[(String, FieldType)],
    where_clause: &Expr,
) -> Option<IndexRanges> {
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);

    // A lone top-level OR is the DNF case. Go also detaches an OR that is one
    // conjunct among several (`extractBestCNFItemRanges`); that selection is
    // deferred, so a mixed AND/OR reaches the CNF walk, where the OR simply
    // stays a filter.
    if conjuncts.len() == 1 && is_or(conjuncts[0]) {
        let built = build_dnf_ranges(index_columns, conjuncts[0])?;
        return (built.column_count > 0).then_some(built);
    }

    let built = build_cnf_ranges(index_columns, &conjuncts);
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

    fn columns(names: &[&str]) -> Vec<(String, FieldType)> {
        names
            .iter()
            // Only the column name is read by the derivation; the field type
            // rides along for the eventual type-conversion step Go performs
            // in `convertPointInPlace`.
            .map(|name| {
                (
                    (*name).to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )
            })
            .collect()
    }

    fn derive(index: &[&str], where_sql: &str) -> String {
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
        match detach_cond_and_build_range_for_index(&columns(index), where_clause) {
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
}
