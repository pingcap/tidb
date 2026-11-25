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

//! Go `pkg/util/ranger/types.go`: the `Range` value — a
//! per-column `[low, high]` datum tuple pair with exclusion flags and
//! collators — and the `Ranges` list with its subset/intersection algebra.
//!
//! Go's `collate.Collator` interface slots map to this port's [`Collation`]
//! values: the datum comparison consumes them directly, and Go's
//! `checkCollators` identity test becomes value equality.
//!
//! Static ranges need no rebuilding; IndexJoin's retained definition lives in
//! `find_best_task::index_join::path`. Quota accounting uses Go's 64-bit sizes
//! because the fallback threshold affects access selection and cache admission.

use std::fmt::Write as _;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use tidb_charset::Collation;
use tidb_datatype::Datum;

/// A range endpoint tuple. Intersections share the selected source tuple;
/// changing a shared bound gives the caller its own copy.
#[derive(Clone)]
pub struct RangeBound(BoundValues);

#[derive(Clone)]
enum BoundValues {
    Owned(Vec<Datum>),
    Shared(Arc<Vec<Datum>>),
}

impl Default for RangeBound {
    fn default() -> Self {
        Self(BoundValues::Owned(Vec::new()))
    }
}

impl RangeBound {
    fn share(&mut self) {
        if let BoundValues::Owned(values) = &mut self.0 {
            self.0 = BoundValues::Shared(Arc::new(std::mem::take(values)));
        }
    }

    fn take_or_share(&mut self, last_use: usize, result: usize) -> Self {
        if last_use == result {
            std::mem::take(self)
        } else {
            self.share();
            self.clone()
        }
    }
}

impl From<Vec<Datum>> for RangeBound {
    fn from(values: Vec<Datum>) -> Self {
        // Keep scalar bounds in their existing allocation. Wide byte and
        // string bounds can repay the shared header on their first copy.
        const MIN_SHARED_BYTES: usize = 256;
        let share = values.iter().any(|datum| match datum {
            Datum::Bytes(value) | Datum::Raw(value) => value.len() >= MIN_SHARED_BYTES,
            Datum::String(value) => value.bytes().len() >= MIN_SHARED_BYTES,
            _ => false,
        });
        let mut result = Self(BoundValues::Owned(values));
        if share {
            result.share();
        }
        result
    }
}

impl FromIterator<Datum> for RangeBound {
    fn from_iter<I: IntoIterator<Item = Datum>>(values: I) -> Self {
        values.into_iter().collect::<Vec<_>>().into()
    }
}

impl Deref for RangeBound {
    type Target = Vec<Datum>;

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            BoundValues::Owned(values) => values,
            BoundValues::Shared(values) => values,
        }
    }
}

impl DerefMut for RangeBound {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.0 {
            BoundValues::Owned(values) => values,
            BoundValues::Shared(values) => Arc::make_mut(values),
        }
    }
}

impl std::fmt::Debug for RangeBound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl PartialEq for RangeBound {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialEq<Vec<Datum>> for RangeBound {
    fn eq(&self, other: &Vec<Datum>) -> bool {
        self.deref() == other
    }
}

impl<const N: usize> PartialEq<[Datum; N]> for RangeBound {
    fn eq(&self, other: &[Datum; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl IntoIterator for RangeBound {
    type Item = Datum;
    type IntoIter = std::vec::IntoIter<Datum>;

    fn into_iter(self) -> Self::IntoIter {
        let values = match self.0 {
            BoundValues::Owned(values) => values,
            BoundValues::Shared(values) => Arc::unwrap_or_clone(values),
        };
        values.into_iter()
    }
}

impl<'a> IntoIterator for &'a RangeBound {
    type Item = &'a Datum;
    type IntoIter = std::slice::Iter<'a, Datum>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Go `Range`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Range {
    /// Go `LowVal`.
    pub low_val: RangeBound,
    /// Go `HighVal`.
    pub high_val: RangeBound,
    /// Go `Collators`, one per column.
    pub collators: Vec<Collation>,
    /// Go `LowExclude`.
    pub low_exclude: bool,
    /// Go `HighExclude`.
    pub high_exclude: bool,
}

/// Go `Range` is a `fmt.Stringer`.
impl std::fmt::Display for Range {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_display_string())
    }
}

/// Go `Ranges` (`[]*Range`).
pub type Ranges = Vec<Range>;

impl Range {
    /// Go `Width`.
    #[must_use]
    pub fn width(&self) -> usize {
        self.low_val.len()
    }

    /// Go `Clone` (writes to cloned bounds remain independent).
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        self.clone()
    }

    /// Go `isPoint`: every column pair equal and non-sentinel, both bounds
    /// inclusive; `[NULL, NULL]` is a point only when
    /// `regard_null_as_point`.
    fn is_point_impl(&self, regard_null_as_point: bool) -> bool {
        if self.low_val.len() != self.high_val.len() {
            return false;
        }
        for i in 0..self.low_val.len() {
            let a = &self.low_val[i];
            let b = &self.high_val[i];
            if matches!(a, Datum::MinNotNull) || matches!(b, Datum::MaxValue) {
                return false;
            }
            let Ok(cmp) = a.compare(b, self.collators[i]) else {
                return false;
            };
            if cmp != std::cmp::Ordering::Equal {
                return false;
            }
            if matches!(a, Datum::Null) && matches!(b, Datum::Null) && !regard_null_as_point {
                return false;
            }
        }
        !self.low_exclude && !self.high_exclude
    }

    /// Go `IsPoint`: the session's `RegardNULLAsPoint` (default true)
    /// arrives as the argument until `RangerContext` lands with `ranger.go`.
    #[must_use]
    pub fn is_point(&self, regard_null_as_point: bool) -> bool {
        self.is_point_impl(regard_null_as_point)
    }

    /// Go `IsOnlyNull`: `[NULL, NULL]` (possibly multi-column).
    #[must_use]
    pub fn is_only_null(&self) -> bool {
        for i in 0..self.low_val.len() {
            if !(matches!(self.low_val[i], Datum::Null)
                && matches!(self.high_val.get(i), Some(Datum::Null)))
            {
                return false;
            }
        }
        true
    }

    /// Go `IsPointNonNullable`.
    #[must_use]
    pub fn is_point_non_nullable(&self) -> bool {
        self.is_point_impl(false)
    }

    /// Go `IsPointNullable`.
    #[must_use]
    pub fn is_point_nullable(&self) -> bool {
        self.is_point_impl(true)
    }

    /// Go `IsFullRange`: whether this range is the full scan. An unsigned
    /// int handle reads `[0, +inf]`; otherwise every column must span
    /// boundary-to-boundary, with `[NULL, +inf)` and `(-inf, NULL]`
    /// admitted but `[NULL, NULL]` refused.
    #[must_use]
    pub fn is_full_range(&self, unsigned_int_handle: bool) -> bool {
        if unsigned_int_handle {
            if self.low_val.len() != 1 || self.high_val.len() != 1 {
                return false;
            }
            return is_boundary_value(&self.low_val[0], true, true)
                && is_boundary_value(&self.high_val[0], true, false);
        }
        if self.low_val.len() != self.high_val.len() {
            return false;
        }
        for i in 0..self.low_val.len() {
            let left_is_boundary = is_boundary_value(&self.low_val[i], false, true);
            let left_is_null = matches!(self.low_val[i], Datum::Null);
            let right_is_boundary = is_boundary_value(&self.high_val[i], false, false);
            let right_is_null = matches!(self.high_val[i], Datum::Null);
            // Treat [NULL, +inf), (-inf, NULL] as full range.
            if (!left_is_boundary && !left_is_null)
                || (!right_is_boundary && !right_is_null)
                || (left_is_null && right_is_null)
            {
                return false;
            }
        }
        true
    }

    /// Go `String`: [`Self::redact`] with redaction off.
    #[must_use]
    pub fn to_display_string(&self) -> String {
        self.redact("OFF")
    }

    /// Go `Redact(redact)`: each bound value through `dealWithRedact` --
    /// `OFF` prints it, `ON` prints `?`, anything else wraps it in `‹›`;
    /// infinite bounds always print.
    #[must_use]
    pub fn redact(&self, redact: &str) -> String {
        let mut output = String::with_capacity(3 + 5 * (self.low_val.len() + self.high_val.len()));
        output.push(if self.low_exclude { '(' } else { '[' });
        append_bound(&mut output, &self.low_val, true, redact);
        output.push(',');
        append_bound(&mut output, &self.high_val, false, redact);
        output.push(if self.high_exclude { ')' } else { ']' });
        output
    }

    /// Go `Encode`: the range's `[low, high)` key pair — an exclusive low
    /// bound steps past its prefix, an INCLUSIVE high bound does (the high
    /// key is exclusive on the wire).
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), tidb_codec::CodecError> {
        let mut low = tidb_codec::encode_key(&self.low_val)?;
        if self.low_exclude {
            prefix_next(&mut low);
        }
        let mut high = tidb_codec::encode_key(&self.high_val)?;
        if !self.high_exclude {
            prefix_next(&mut high);
        }
        Ok((low, high))
    }

    /// Go `Equal`.
    #[must_use]
    pub fn equal(&self, other: &Range) -> bool {
        if self.low_exclude != other.low_exclude || self.high_exclude != other.high_exclude {
            return false;
        }
        if self.low_val.len() != other.low_val.len() || self.high_val.len() != other.high_val.len()
        {
            return false;
        }
        for i in 0..self.low_val.len() {
            if !datum_equals(&self.low_val[i], &other.low_val[i]) {
                return false;
            }
        }
        for i in 0..self.high_val.len() {
            if !datum_equals(&self.high_val[i], &other.high_val[i]) {
                return false;
            }
        }
        true
    }

    /// Go `PrefixEqualLen`: how many leading columns are point-equal.
    pub fn prefix_equal_len(&self) -> Result<usize, tidb_datatype::DatumValueError> {
        for i in 0..self.low_val.len() {
            let cmp = self.low_val[i].compare(&self.high_val[i], self.collators[i])?;
            if cmp != std::cmp::Ordering::Equal {
                return Ok(i);
            }
        }
        Ok(self.low_val.len())
    }

    /// Go `Subset` for one range: `other_range` COVERS this range — same
    /// collators over the covering prefix, other's bounds no stricter, and
    /// both bound tuples prefix-equal over other's width.
    #[must_use]
    pub fn subset_of(&self, other_range: &Range) -> bool {
        if self.low_val.len() < other_range.low_val.len() {
            return false;
        }
        if !check_collators(self, other_range, other_range.low_val.len()) {
            return false;
        }
        // Either the covering range is closed or both share the setting.
        let low_exclude_ok =
            !other_range.low_exclude || self.low_exclude == other_range.low_exclude;
        let high_exclude_ok =
            !other_range.high_exclude || self.high_exclude == other_range.high_exclude;
        if !low_exclude_ok || !high_exclude_ok {
            return false;
        }
        prefix(
            &other_range.low_val,
            &self.low_val,
            other_range.low_val.len(),
            &self.collators,
        ) && prefix(
            &other_range.high_val,
            &self.high_val,
            other_range.low_val.len(),
            &self.collators,
        )
    }

    /// Go `IntersectRange`: `None` for a provably empty intersection; the
    /// narrower bound wins each side, the more granular (wider) range's
    /// bound winning ties.
    pub fn intersect_range(
        &self,
        other_range: &Range,
    ) -> Result<Option<Range>, tidb_datatype::DatumValueError> {
        self.intersection_with(
            other_range,
            |low_from_right, high_from_right, collators_from_right| {
                let select = |from_right| if from_right { other_range } else { self };
                let low = select(low_from_right);
                let high = select(high_from_right);
                Range {
                    low_val: low.low_val.clone(),
                    high_val: high.high_val.clone(),
                    collators: select(collators_from_right).collators.clone(),
                    low_exclude: low.low_exclude,
                    high_exclude: high.high_exclude,
                }
            },
        )
    }

    fn intersection_with<T>(
        &self,
        other_range: &Range,
        build: impl FnOnce(bool, bool, bool) -> T,
    ) -> Result<Option<T>, tidb_datatype::DatumValueError> {
        if self.low_val.len() == 1 && !other_range.collators.is_empty() {
            // Single-column integer bounds need no collation or prefix padding.
            let bounds = match (
                self.low_val.as_slice(),
                self.high_val.as_slice(),
                other_range.low_val.as_slice(),
                other_range.high_val.as_slice(),
            ) {
                (
                    [Datum::Int(lo)],
                    [Datum::Int(hi)],
                    [Datum::Int(other_lo)],
                    [Datum::Int(other_hi)],
                ) => Some(self.intersect_integer_bounds(
                    other_range,
                    [*lo, *hi],
                    [*other_lo, *other_hi],
                )),
                (
                    [Datum::UInt(lo)],
                    [Datum::UInt(hi)],
                    [Datum::UInt(other_lo)],
                    [Datum::UInt(other_hi)],
                ) => Some(self.intersect_integer_bounds(
                    other_range,
                    [*lo, *hi],
                    [*other_lo, *other_hi],
                )),
                _ => None,
            };
            if let Some(bounds) = bounds {
                return Ok(bounds.map(|(low, high)| build(low, high, true)));
            }
        }

        let other_range_more_granular = self.low_val.len() <= other_range.low_val.len();
        let collators = if other_range_more_granular {
            &other_range.collators
        } else {
            &self.collators
        };

        let low_vs_high = compare_lexicographically(
            &self.low_val,
            &other_range.high_val,
            collators,
            self.low_exclude,
            other_range.high_exclude,
            true,
            false,
        )?;
        if low_vs_high == std::cmp::Ordering::Greater {
            return Ok(None);
        }
        let low_vs_high = compare_lexicographically(
            &other_range.low_val,
            &self.high_val,
            collators,
            other_range.low_exclude,
            self.high_exclude,
            true,
            false,
        )?;
        if low_vs_high == std::cmp::Ordering::Greater {
            return Ok(None);
        }

        let low_vs_low = compare_lexicographically(
            &self.low_val,
            &other_range.low_val,
            collators,
            self.low_exclude,
            other_range.low_exclude,
            true,
            true,
        )?;
        let low_from_right = low_vs_low == std::cmp::Ordering::Less
            || (low_vs_low == std::cmp::Ordering::Equal && other_range_more_granular);

        let high_vs_high = compare_lexicographically(
            &self.high_val,
            &other_range.high_val,
            collators,
            self.high_exclude,
            other_range.high_exclude,
            false,
            false,
        )?;
        let high_from_right = high_vs_high == std::cmp::Ordering::Greater
            || (high_vs_high == std::cmp::Ordering::Equal && other_range_more_granular);
        Ok(Some(build(
            low_from_right,
            high_from_right,
            other_range_more_granular,
        )))
    }

    fn intersect_integer_bounds<N: Ord>(
        &self,
        other: &Range,
        [lo, hi]: [N; 2],
        [other_lo, other_hi]: [N; 2],
    ) -> Option<(bool, bool)> {
        // At equal values, an open lower endpoint sorts after a closed one,
        // and an open upper endpoint sorts before it.
        let lower = (lo, i8::from(self.low_exclude));
        let upper = (hi, -i8::from(self.high_exclude));
        let other_lower = (other_lo, i8::from(other.low_exclude));
        let other_upper = (other_hi, -i8::from(other.high_exclude));
        if lower > other_upper || other_lower > upper {
            return None;
        }
        Some((lower <= other_lower, upper >= other_upper))
    }

}

#[derive(Clone, Copy, Default)]
struct IntersectionSources {
    low: usize,
    high: usize,
    collators: usize,
}

/// Go `Ranges.Subset`: every range covered by SOME super range, and every
/// super range covering SOMETHING. Empty `ranges` matches only an empty
/// super list; an empty super list covers anything.
#[must_use]
pub fn ranges_subset(ranges: &Ranges, super_ranges: &Ranges) -> bool {
    if ranges.is_empty() {
        return super_ranges.is_empty();
    }
    if super_ranges.is_empty() {
        // Unrestricted super ranges and restricted ranges.
        return true;
    }
    let mut super_ranges_covered = vec![false; super_ranges.len()];
    for sub_range in ranges {
        let mut subset = false;
        for (i, super_range) in super_ranges.iter().enumerate() {
            if sub_range.subset_of(super_range) {
                subset = true;
                super_ranges_covered[i] = true;
                break;
            }
        }
        if !subset {
            return false;
        }
    }
    super_ranges_covered.into_iter().all(|covered| covered)
}

/// Go `Ranges.IntersectRanges`: the pairwise intersections; `None` (Go's
/// nil) on a collator mismatch or comparison failure.
#[must_use]
pub fn intersect_ranges(ranges: &Ranges, other_ranges: &Ranges) -> Option<Ranges> {
    let mut result = Ranges::new();
    for rs_range in ranges {
        for other_range in other_ranges {
            let subset_length = rs_range.low_val.len().min(other_range.low_val.len());
            if !check_collators(rs_range, other_range, subset_length) {
                return None;
            }
            match rs_range.intersect_range(other_range) {
                Err(_) => return None,
                Ok(Some(intersection)) => result.push(intersection),
                Ok(None) => {}
            }
        }
    }
    Some(result)
}

/// Intersects range lists whose bounds can be moved into the result.
///
/// On success, either input may contain moved-out bounds. On comparison
/// failure or mismatched collators, both inputs are left intact.
#[must_use]
pub fn intersect_owned_ranges(ranges: &mut [Range], other_ranges: &mut [Range]) -> Option<Ranges> {
    let compare = |left: &Range, right: &Range, i, j| {
        let width = left.low_val.len().min(right.low_val.len());
        if !check_collators(left, right, width) {
            return None;
        }
        left.intersection_with(right, |low, high, collators| {
            let select = |from_right| if from_right { j } else { i };
            IntersectionSources {
                low: select(low),
                high: select(high),
                collators: select(collators),
            }
        })
        .ok()
    };

    if let ([left], [right]) = (&mut *ranges, &mut *other_ranges) {
        let Some(bounds) = compare(left, right, 0, 1)? else {
            return Some(Ranges::new());
        };
        let mut inputs = [
            (left, IntersectionSources::default()),
            (right, IntersectionSources::default()),
        ];
        return Some(vec![move_intersection(&mut inputs, bounds, 0)]);
    }

    // Collect the pairs before moving anything so an error on a later pair
    // leaves the original lists available for the planner's fallback.
    let mut plans = Vec::new();
    let first_right = ranges.len();
    for (i, left) in ranges.iter().enumerate() {
        for (j, right) in other_ranges.iter().enumerate() {
            if let Some(bounds) = compare(left, right, i, first_right + j)? {
                plans.push(bounds);
            }
        }
    }
    if plans.is_empty() {
        return Some(Ranges::new());
    }

    let mut inputs: Vec<_> = ranges
        .iter_mut()
        .chain(other_ranges)
        .map(|range| (range, IntersectionSources::default()))
        .collect();
    // One input can contribute a bound to several results. Keep the bound
    // until all earlier results have a shared copy or their own buffer.
    for (result, bounds) in plans.iter().enumerate() {
        inputs[bounds.low].1.low = result;
        inputs[bounds.high].1.high = result;
        inputs[bounds.collators].1.collators = result;
    }
    Some(
        plans
            .into_iter()
            .enumerate()
            .map(|(result, bounds)| move_intersection(&mut inputs, bounds, result))
            .collect(),
    )
}

fn move_intersection(
    inputs: &mut [(&mut Range, IntersectionSources)],
    bounds: IntersectionSources,
    result: usize,
) -> Range {
    fn take_or_clone<T: Clone>(value: &mut Vec<T>, last_use: usize, result: usize) -> Vec<T> {
        if last_use == result {
            std::mem::take(value)
        } else {
            value.clone()
        }
    }

    let (low, last_use) = &mut inputs[bounds.low];
    let low_val = low.low_val.take_or_share(last_use.low, result);
    let low_exclude = low.low_exclude;
    let (high, last_use) = &mut inputs[bounds.high];
    let high_val = high.high_val.take_or_share(last_use.high, result);
    let high_exclude = high.high_exclude;
    let (range, last_use) = &mut inputs[bounds.collators];
    let collators = take_or_clone(&mut range.collators, last_use.collators, result);
    Range {
        low_val: low_val.into(),
        high_val: high_val.into(),
        collators,
        low_exclude,
        high_exclude,
    }
}

/// Go `HasFullRange`.
#[must_use]
pub fn has_full_range(ranges: &[Range], unsigned_int_handle: bool) -> bool {
    ranges
        .iter()
        .any(|range| range.is_full_range(unsigned_int_handle))
}

/// Back-compat spelling of [`has_full_range`] matching Go's exported name.
#[allow(non_snake_case)]
#[must_use]
pub fn HasFullRange(ranges: &[Range], unsigned_int_handle: bool) -> bool {
    has_full_range(ranges, unsigned_int_handle)
}

/// Go `isBoundaryValue`.
fn is_boundary_value(d: &Datum, unsigned_int_handle: bool, is_left_side: bool) -> bool {
    let is_right_side = !is_left_side;
    match d {
        Datum::MinNotNull => is_left_side,
        Datum::MaxValue => is_right_side,
        Datum::Int(v) => (*v == i64::MIN && is_left_side) || (*v == i64::MAX && is_right_side),
        Datum::UInt(v) => {
            (*v == 0 && unsigned_int_handle && is_left_side) || (*v == u64::MAX && is_right_side)
        }
        _ => false,
    }
}

/// Go `formatDatum`.
/// Go `EmptyRangeSize`: `unsafe.Sizeof(Range{})` -- three slice headers
/// and two padded bools on 64-bit.
pub const EMPTY_RANGE_SIZE: i64 = 80;
impl Range {
    /// Go `Range.MemUsage`.
    #[must_use]
    pub fn mem_usage(&self) -> i64 {
        EMPTY_RANGE_SIZE
            + self.collators.len() as i64 * 16
            + self
                .low_val
                .iter()
                .chain(self.high_val.iter())
                .map(Datum::mem_usage)
                .sum::<i64>()
    }
}

/// Go `Ranges.MemUsage`.
#[must_use]
pub fn ranges_mem_usage(ranges: &Ranges) -> i64 {
    ranges.iter().map(Range::mem_usage).sum()
}

fn append_bound(output: &mut String, bound: &[Datum], is_left_side: bool, redact: &str) {
    for (i, datum) in bound.iter().enumerate() {
        if i > 0 {
            output.push(' ');
        }
        let infinity = match datum {
            Datum::MinNotNull => Some("-inf"),
            Datum::MaxValue => Some("+inf"),
            Datum::Int(v) if *v == i64::MIN && is_left_side => Some("-inf"),
            Datum::Int(v) if *v == i64::MAX && !is_left_side => Some("+inf"),
            Datum::UInt(v) if *v == u64::MAX && !is_left_side => Some("+inf"),
            _ => None,
        };
        if let Some(infinity) = infinity {
            output.push_str(infinity);
        } else if redact == "ON" {
            output.push('?');
        } else {
            if redact != "OFF" {
                output.push('‹');
            }
            append_datum(output, datum);
            if redact != "OFF" {
                output.push('›');
            }
        }
    }
}

fn append_datum(output: &mut String, datum: &Datum) {
    match datum {
        Datum::Null => output.push_str("NULL"),
        Datum::MinNotNull | Datum::MaxValue => unreachable!("infinities rendered by append_bound"),
        Datum::Int(v) => write!(output, "{v}").unwrap(),
        Datum::UInt(v) => write!(output, "{v}").unwrap(),
        Datum::Bytes(bytes) => output.push_str(&tidb_go_strconv::quote_bytes(bytes)),
        Datum::String(s) => output.push_str(&tidb_go_strconv::quote_bytes(s.bytes())),
        // Go's default arm is fmt.Sprintf("%v", d.GetValue()). A float32
        // datum keeps 32-bit shortest form even though Rust stores it as f64.
        Datum::Real(v) => output.push_str(&go_g_float(*v)),
        Datum::Float32(v) => output.push_str(&tidb_goruntime::go_strconv::format_float(
            f64::from(*v as f32),
            b'g',
            -1,
            32,
        )),
        Datum::Decimal(d) => write!(output, "{d}").unwrap(),
        // Go wraps these values in quotes without escaping their display text.
        Datum::Enum(value, _) => write!(output, "\"{}\"", value.name()).unwrap(),
        Datum::Set(value, _) => write!(output, "\"{value}\"").unwrap(),
        Datum::Json(value) => write!(output, "\"{value}\"").unwrap(),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => write!(output, "\"{value}\"").unwrap(),
        Datum::Time(value) => write!(output, "{value}").unwrap(),
        Datum::Duration(value) => write!(output, "{value}").unwrap(),
        Datum::VectorFloat32(value) => write!(output, "{value}").unwrap(),
        Datum::Raw(bytes) => {
            output.push('[');
            for (i, byte) in bytes.iter().enumerate() {
                if i > 0 {
                    output.push(' ');
                }
                write!(output, "{byte}").unwrap();
            }
            output.push(']');
        }
    }
}

/// The value filling a missing column under Go's prefix-bound extension.
fn bound_padding(low: bool, open: bool) -> &'static Datum {
    if low == open {
        // Open lower or closed upper bound.
        &Datum::MaxValue
    } else {
        &Datum::MinNotNull
    }
}

/// Go `compareLexicographically`: bounds of different widths extend with
/// the side-appropriate infinities, then compare column-wise; the
/// open/low flags break ties exactly per Go's switch.
fn compare_lexicographically(
    bound1: &[Datum],
    bound2: &[Datum],
    collators: &[Collation],
    open1: bool,
    open2: bool,
    low1: bool,
    low2: bool,
) -> Result<std::cmp::Ordering, tidb_datatype::DatumValueError> {
    use std::cmp::Ordering;
    let n1 = bound1.len();
    let n2 = bound2.len();
    for (i, (left, right)) in bound1.iter().zip(bound2).enumerate() {
        let cmp = left.compare(right, collators[i])?;
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    for i in n1.min(n2)..n1.max(n2) {
        let left = bound1.get(i).unwrap_or_else(|| bound_padding(low1, open1));
        let right = bound2.get(i).unwrap_or_else(|| bound_padding(low2, open2));
        let cmp = left.compare(right, collators[i])?;
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }

    Ok(match (open1, open2) {
        (false, false) => Ordering::Equal,
        (true, true) => {
            if low1 == low2 {
                Ordering::Equal
            } else if low1 {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (true, false) => {
            if low1 {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, true) => {
            if low2 {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
    })
}

/// Go `prefix`: whether `sup_value` prefix-equals `super_value` over
/// `length` columns.
fn prefix(
    super_value: &[Datum],
    sup_value: &[Datum],
    length: usize,
    collators: &[Collation],
) -> bool {
    for i in 0..length {
        match super_value[i].compare(&sup_value[i], collators[i]) {
            Ok(std::cmp::Ordering::Equal) => {}
            _ => return false,
        }
    }
    true
}

/// Go `checkCollators`.
fn check_collators(ran1: &Range, ran2: &Range, length: usize) -> bool {
    ran1.collators[..length.min(ran1.collators.len())]
        == ran2.collators[..length.min(ran2.collators.len())]
}

/// Go `Datum.Equals` as `Range.Equal` consumes it: binary equality under
/// the datum's own kind (no collation slot on this call in Go either).
fn datum_equals(a: &Datum, b: &Datum) -> bool {
    a.compare(b, Collation::Binary)
        .map(|cmp| cmp == std::cmp::Ordering::Equal)
        .unwrap_or(false)
        && std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// Go `strconv.FormatFloat(v, 'g', -1, 64)`, `fmt`'s `%v` for a float64.
pub(super) fn go_g_float(value: f64) -> String {
    tidb_goruntime::go_strconv::format_float(value, b'g', -1, 64)
}

/// Go `kv.Key.PrefixNext`: the next prefix key. Each byte from the end is
/// incremented until one does not wrap to zero; the length is kept (`01 ff`
/// is `02 00`), and a key of only `0xff` bytes is itself with a zero byte
/// appended.
pub(crate) fn prefix_next(key: &mut Vec<u8>) {
    if let Some(index) = key.iter().rposition(|&byte| byte != u8::MAX) {
        key[index] += 1;
        key[index + 1..].fill(0);
    } else {
        key.push(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `kv.Key.PrefixNext` (`pkg/kv/key_test.go` `TestPrefixNext`'s
    /// shapes): a carry keeps the key's length.
    #[test]
    fn prefix_next_carries_like_go() {
        for (input, expected) in [
            (&[0x01, 0x02][..], &[0x01, 0x03][..]),
            (&[0x01, 0xff], &[0x02, 0x00]),
            (&[0x01, 0xff, 0xff], &[0x02, 0x00, 0x00]),
            (&[0xff, 0xff], &[0xff, 0xff, 0x00]),
            (&[], &[0x00]),
        ] {
            let mut key = input.to_vec();
            prefix_next(&mut key);
            assert_eq!(key, expected);
        }
    }

    fn int(v: i64) -> Datum {
        Datum::Int(v)
    }

    fn collators(n: usize) -> Vec<Collation> {
        // Go `collate.GetBinaryCollatorSlice(n)`.
        vec![Collation::Binary; n]
    }

    /// Go `buildRange`: MinInt64/MaxInt64 spell the sentinels.
    fn build_range(low: &[i64], high: &[i64], low_exclude: bool, high_exclude: bool) -> Range {
        let datums = |vals: &[i64]| -> Vec<Datum> {
            vals.iter()
                .map(|v| match *v {
                    i64::MIN => Datum::MinNotNull,
                    i64::MAX => Datum::MaxValue,
                    other => Datum::Int(other),
                })
                .collect()
        };
        Range {
            low_val: datums(low).into(),
            high_val: datums(high).into(),
            collators: collators(low.len()),
            low_exclude,
            high_exclude,
        }
    }

    fn range_to_string(range: Option<&Range>) -> String {
        range.map_or_else(|| "<nil>".to_owned(), Range::to_display_string)
    }

    /// Go `TestRange` (`types_test.go:91`): the display strings and the
    /// point test, sample by sample.
    #[test]
    fn range_strings_and_points_match_go() {
        let samples = [
            (build_range(&[1], &[1], false, false), "[1,1]"),
            (build_range(&[1], &[1], false, true), "[1,1)"),
            (build_range(&[1], &[2], true, true), "(1,2)"),
            (
                Range {
                    low_val: vec![Datum::Real(1.1)].into(),
                    high_val: vec![Datum::Real(1.9)].into(),
                    collators: collators(1),
                    low_exclude: false,
                    high_exclude: true,
                },
                "[1.1,1.9)",
            ),
            (
                Range {
                    low_val: vec![Datum::MinNotNull].into(),
                    high_val: vec![int(1)].into(),
                    collators: collators(1),
                    low_exclude: false,
                    high_exclude: true,
                },
                "[-inf,1)",
            ),
        ];
        for (range, expected) in samples {
            assert_eq!(range.to_display_string(), expected);
        }

        let string_datum = |text: &str| {
            Datum::String(tidb_datatype::StringDatum::new(
                text.as_bytes().to_vec(),
                Collation::Utf8Mb4Bin,
            ))
        };
        let is_point_tests = [
            (build_range(&[1], &[1], false, false), true),
            (
                Range {
                    low_val: vec![string_datum("abc")].into(),
                    high_val: vec![string_datum("abc")].into(),
                    collators: collators(1),
                    low_exclude: false,
                    high_exclude: false,
                },
                true,
            ),
            (
                Range {
                    low_val: vec![int(1)].into(),
                    high_val: vec![int(1), int(1)].into(),
                    collators: collators(1),
                    low_exclude: false,
                    high_exclude: false,
                },
                false,
            ),
            (build_range(&[1], &[1], true, false), false),
            (build_range(&[1], &[1], false, true), false),
            (build_range(&[1], &[2], false, false), false),
        ];
        for (range, expected) in is_point_tests {
            // Go's MockContext ranger context has RegardNULLAsPoint = true.
            assert_eq!(
                range.is_point(true),
                expected,
                "{}",
                range.to_display_string()
            );
        }
    }

    /// Go `TestIsFullRange` (`types_test.go:161`), row by row.
    #[test]
    fn full_range_recognition_matches_go() {
        let rows = [
            (
                Range {
                    low_val: vec![Datum::MinNotNull].into(),
                    high_val: vec![Datum::MaxValue].into(),
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                true,
            ),
            (
                Range {
                    low_val: vec![Datum::MaxValue].into(),
                    high_val: vec![Datum::MinNotNull].into(),
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                false,
            ),
            (
                Range {
                    low_val: vec![int(1)].into(),
                    high_val: vec![Datum::UInt(u64::MAX)].into(),
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                false,
            ),
            // Go builds `nullDatum` by SetNull on a sentinel: a plain NULL.
            (
                Range {
                    low_val: vec![Datum::Null].into(),
                    high_val: vec![Datum::UInt(u64::MAX)].into(),
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                true,
            ),
            (
                Range {
                    low_val: vec![Datum::Null].into(),
                    high_val: vec![Datum::Null].into(),
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                false,
            ),
            (
                Range {
                    low_val: vec![Datum::UInt(0)].into(),
                    high_val: vec![Datum::UInt(u64::MAX)].into(),
                    collators: collators(1),
                    ..Range::default()
                },
                true,
                true,
            ),
        ];
        for (range, unsigned_handle, expected) in rows {
            assert_eq!(
                range.is_full_range(unsigned_handle),
                expected,
                "{}",
                range.to_display_string()
            );
        }
    }

    /// Go `TestIntersectionList` (`types_test.go:283`): the worked
    /// two-list example — `(a > 100 OR (a = 100 AND b > 0))` intersected
    /// with `(a < 101 OR (a = 101 AND b < 10))`.
    #[test]
    fn range_list_intersection_matches_gos_example() {
        let r1 = build_range(&[100, 0], &[100, i64::MAX], true, false);
        let r2 = build_range(&[100], &[i64::MAX], true, false);
        let list1: Ranges = vec![r1, r2];
        let r3 = build_range(&[i64::MIN], &[101], false, true);
        let r4 = build_range(&[101, i64::MIN], &[101, 10], false, true);
        let list2: Ranges = vec![r3, r4];

        let intersected = intersect_ranges(&list1, &list2).expect("intersects");
        let actual: Vec<String> = intersected.iter().map(Range::to_display_string).collect();
        assert_eq!(
            actual.join(","),
            "(100 0,100 +inf],(100,101),[101 -inf,101 10)"
        );
    }

    #[test]
    fn owned_intersection_moves_the_last_copy_of_a_shared_bound() {
        let bytes_range = |low: &[u8], high: &[u8]| Range {
            low_val: vec![Datum::Bytes(low.to_vec())].into(),
            high_val: vec![Datum::Bytes(high.to_vec())].into(),
            collators: collators(1),
            ..Range::default()
        };
        let mut left = vec![bytes_range(b"lower", b"upper")];
        let mut right = vec![
            bytes_range(b"first", b"middle"),
            bytes_range(b"first", b"other"),
        ];
        let Datum::Bytes(bytes) = &left[0].low_val[0] else {
            unreachable!();
        };
        let original = bytes.as_ptr();
        let expected = intersect_ranges(&left, &right).unwrap();
        let mut actual = intersect_owned_ranges(&mut left, &mut right).unwrap();
        assert_eq!(actual, expected);
        assert!(left[0].low_val.is_empty());
        let Datum::Bytes(last) = &actual[1].low_val[0] else {
            unreachable!();
        };
        assert_eq!(last.as_ptr(), original);
        let Datum::Bytes(first) = &actual[0].low_val[0] else {
            unreachable!();
        };
        assert_eq!(first.as_ptr(), original);
        let Datum::Bytes(first) = &mut actual[0].low_val[0] else {
            unreachable!();
        };
        first[0] = b'L';
        assert_ne!(first.as_ptr(), original);
        assert_eq!(actual[1], expected[1]);
    }

    #[test]
    fn borrowed_intersection_shares_wide_bounds_until_mutation() {
        let range = |low, high| Range {
            low_val: vec![Datum::Bytes(vec![low; 1024])].into(),
            high_val: vec![Datum::Bytes(vec![high; 1024])].into(),
            collators: collators(1),
            ..Range::default()
        };
        let left = range(b'a', b'd');
        let right = range(b'b', b'e');
        let mut intersection = left.intersect_range(&right).unwrap().unwrap();
        assert_eq!(intersection.low_val.as_ptr(), right.low_val.as_ptr());
        assert_eq!(intersection.high_val.as_ptr(), left.high_val.as_ptr());
        intersection.low_val[0] = Datum::Null;
        assert_ne!(intersection.low_val.as_ptr(), right.low_val.as_ptr());
        assert_eq!(right.low_val[0], Datum::Bytes(vec![b'b'; 1024]));
    }

    #[test]
    fn late_owned_intersection_failure_leaves_both_inputs_intact() {
        let left = vec![build_range(&[1], &[10], false, false)];
        let right = vec![build_range(&[4], &[15], false, false)];
        let mut mismatch = right[0].clone();
        mismatch.collators[0] = Collation::Utf8Mb4GeneralCi;
        let mut invalid = right[0].clone();
        invalid.high_val[0] = Datum::VectorFloat32(tidb_datatype::VectorFloat32::init(1));

        for bad in [mismatch, invalid] {
            let right = vec![right[0].clone(), bad];
            assert!(intersect_ranges(&left, &right).is_none());
            let (mut owned_left, mut owned_right) = (left.clone(), right.clone());
            assert!(intersect_owned_ranges(&mut owned_left, &mut owned_right).is_none());
            assert_eq!(owned_left, left);
            assert_eq!(owned_right, right);
        }
    }

    /// Go `TestIntersectionEmpty` (`types_test.go:318`): the expected MAP is
    /// the oracle (the per-case `expected` field is dead in Go's own test),
    /// and every intersection is SYMMETRIC.
    #[test]
    fn empty_intersections_match_go() {
        let cases: &[(&[i64], &[i64], bool, bool, &[i64], &[i64], bool, bool, &str)] = &[
            (&[1], &[2], false, false, &[3], &[4], false, false, "<nil>"),
            (&[1], &[2], true, false, &[3], &[4], true, false, "<nil>"),
            (&[1], &[2], false, true, &[3], &[4], false, true, "<nil>"),
            (&[1], &[2], true, true, &[3], &[4], true, true, "<nil>"),
            (
                &[1, 2],
                &[1, 3],
                false,
                false,
                &[1, 3],
                &[1, 4],
                true,
                false,
                "<nil>",
            ),
            (
                &[i64::MIN],
                &[1],
                false,
                false,
                &[2],
                &[i64::MAX],
                false,
                false,
                "<nil>",
            ),
            (
                &[1, 2],
                &[1, 3],
                false,
                false,
                &[1, 3],
                &[1, 4],
                false,
                false,
                "[1 3,1 3]",
            ),
            (
                &[1, 1, 2],
                &[1, 1, 5],
                false,
                false,
                &[1, 2],
                &[1, 3],
                true,
                true,
                "<nil>",
            ),
            (
                &[100, 0],
                &[100, i64::MAX],
                true,
                false,
                &[i64::MIN, i64::MIN],
                &[100, i64::MIN],
                false,
                false,
                "<nil>",
            ),
            (
                &[100, 0],
                &[100, i64::MAX],
                true,
                false,
                &[i64::MIN],
                &[100],
                false,
                true,
                "<nil>",
            ),
            (
                &[5],
                &[5],
                false,
                false,
                &[5],
                &[i64::MAX],
                true,
                false,
                "<nil>",
            ),
            (
                &[1],
                &[1],
                false,
                false,
                &[5],
                &[i64::MAX],
                true,
                false,
                "<nil>",
            ),
            (
                &[5],
                &[5],
                false,
                false,
                &[5, 1],
                &[5, i64::MAX],
                true,
                false,
                "(5 1,5 +inf]",
            ),
            (
                &[1],
                &[1],
                false,
                false,
                &[5, 1],
                &[5, i64::MAX],
                true,
                false,
                "<nil>",
            ),
        ];
        for (low1, high1, ex_low1, ex_high1, low2, high2, ex_low2, ex_high2, expected) in cases {
            let range1 = build_range(low1, high1, *ex_low1, *ex_high1);
            let range2 = build_range(low2, high2, *ex_low2, *ex_high2);
            let one = range1.intersect_range(&range2).expect("compares");
            let two = range2.intersect_range(&range1).expect("compares");
            let label = format!(
                "{} {}",
                range1.to_display_string(),
                range2.to_display_string()
            );
            assert_eq!(range_to_string(one.as_ref()), *expected, "{label}");
            assert_eq!(
                range_to_string(one.as_ref()),
                range_to_string(two.as_ref()),
                "asymmetric: {label}"
            );
        }
    }

    /// Go `TestIntersectionSubset` (`types_test.go:388`).
    #[test]
    fn subset_intersections_match_go() {
        let cases: &[(&[i64], &[i64], bool, bool, &[i64], &[i64], bool, bool, &str)] = &[
            (&[1], &[5], false, false, &[2], &[4], false, false, "[2,4]"),
            (&[1], &[5], true, false, &[2], &[4], true, false, "(2,4]"),
            (&[1], &[5], false, true, &[2], &[4], false, true, "[2,4)"),
            (&[1], &[5], true, true, &[2], &[4], true, true, "(2,4)"),
            (
                &[i64::MIN],
                &[5],
                false,
                false,
                &[2],
                &[4],
                false,
                false,
                "[2,4]",
            ),
            (
                &[1, 2],
                &[1, 5],
                false,
                false,
                &[1, 3],
                &[1, 4],
                true,
                false,
                "(1 3,1 4]",
            ),
            (
                &[1, 1, i64::MIN],
                &[1, 1, 15],
                false,
                false,
                &[1, 1],
                &[1, 1],
                false,
                false,
                "[1 1 -inf,1 1 15]",
            ),
        ];
        for (low1, high1, ex_low1, ex_high1, low2, high2, ex_low2, ex_high2, expected) in cases {
            let range1 = build_range(low1, high1, *ex_low1, *ex_high1);
            let range2 = build_range(low2, high2, *ex_low2, *ex_high2);
            let one = range1.intersect_range(&range2).expect("compares");
            let two = range2.intersect_range(&range1).expect("compares");
            let label = format!(
                "{} {}",
                range1.to_display_string(),
                range2.to_display_string()
            );
            assert_eq!(range_to_string(one.as_ref()), *expected, "{label}");
            assert_eq!(
                range_to_string(one.as_ref()),
                range_to_string(two.as_ref()),
                "asymmetric: {label}"
            );
        }
    }

    /// Go `TestIntersectionOverlap` (`types_test.go:443`): overlapping,
    /// non-subset pairs.
    #[test]
    fn overlap_intersections_match_go() {
        let cases: &[(&[i64], &[i64], bool, bool, &[i64], &[i64], bool, bool, &str)] = &[
            (&[1], &[5], false, false, &[2], &[7], false, false, "[2,5]"),
            (&[1], &[5], true, false, &[2], &[7], true, false, "(2,5]"),
            (&[1], &[5], false, true, &[2], &[7], false, true, "[2,5)"),
            (&[1], &[5], true, true, &[2], &[7], true, true, "(2,5)"),
            (
                &[i64::MIN],
                &[5],
                false,
                false,
                &[2],
                &[14],
                false,
                false,
                "[2,5]",
            ),
            (
                &[1, 2],
                &[1, 5],
                false,
                false,
                &[1, 3],
                &[1, 4],
                true,
                false,
                "(1 3,1 4]",
            ),
            (
                &[1, 1, i64::MIN],
                &[1, 1, 15],
                false,
                false,
                &[1, 1, 4],
                &[1, 1, 25],
                false,
                false,
                "[1 1 4,1 1 15]",
            ),
        ];
        for (low1, high1, ex_low1, ex_high1, low2, high2, ex_low2, ex_high2, expected) in cases {
            let range1 = build_range(low1, high1, *ex_low1, *ex_high1);
            let range2 = build_range(low2, high2, *ex_low2, *ex_high2);
            let one = range1.intersect_range(&range2).expect("compares");
            let two = range2.intersect_range(&range1).expect("compares");
            let label = format!(
                "{} {}",
                range1.to_display_string(),
                range2.to_display_string()
            );
            assert_eq!(range_to_string(one.as_ref()), *expected, "{label}");
            assert_eq!(
                range_to_string(one.as_ref()),
                range_to_string(two.as_ref()),
                "asymmetric: {label}"
            );
        }
    }
}
