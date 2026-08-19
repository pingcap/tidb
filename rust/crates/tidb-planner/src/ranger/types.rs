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

//! Go `pkg/util/ranger/types.go`, COMPLETE: the `Range` value — a
//! per-column `[low, high]` datum tuple pair with exclusion flags and
//! collators — and the `Ranges` list with its subset/intersection algebra.
//!
//! Go's `collate.Collator` interface slots map to this port's [`Collation`]
//! values: the datum comparison consumes them directly, and Go's
//! `checkCollators` identity test becomes value equality.
//!
//! `MutableRanges` (the plan-cache rebuild seam) has exactly one
//! non-trivial implementor in Go outside plan-cache internals — `Ranges`
//! itself, whose `Rebuild` is a no-op — so the trait arrives with the
//! plan-cache surface; nothing here consumes it. `MemUsage` pins Go's
//! `unsafe.Sizeof` struct layout — a Go-runtime detail, not observable
//! semantics — and arrives with the memory-tracking surface that will
//! consume it, sized to THIS port's layouts.

use tidb_datatype::{Collation, Datum};

/// Go `Range`.
#[derive(Clone, Debug, Default)]
pub struct Range {
    /// Go `LowVal`.
    pub low_val: Vec<Datum>,
    /// Go `HighVal`.
    pub high_val: Vec<Datum>,
    /// Go `Collators`, one per column.
    pub collators: Vec<Collation>,
    /// Go `LowExclude`.
    pub low_exclude: bool,
    /// Go `HighExclude`.
    pub high_exclude: bool,
}

/// Go `Ranges` (`[]*Range`).
pub type Ranges = Vec<Range>;

impl Range {
    /// Go `Width`.
    #[must_use]
    pub fn width(&self) -> usize {
        self.low_val.len()
    }

    /// Go `Clone` (the derive is the same deep copy).
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

    /// Go `String` (the redaction-free spelling; `Redact` arrives with the
    /// error-redaction surface its callers live in).
    #[must_use]
    pub fn to_display_string(&self) -> String {
        let low: Vec<String> = self
            .low_val
            .iter()
            .map(|d| format_datum(d, true))
            .collect();
        let high: Vec<String> = self
            .high_val
            .iter()
            .map(|d| format_datum(d, false))
            .collect();
        let l = if self.low_exclude { "(" } else { "[" };
        let r = if self.high_exclude { ")" } else { "]" };
        format!("{l}{},{}{r}", low.join(" "), high.join(" "))
    }

    /// Go `Encode`: the range's `[low, high)` key pair — an exclusive low
    /// bound steps past its prefix, an INCLUSIVE high bound does (the high
    /// key is exclusive on the wire).
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), tidb_codec::CodecError> {
        let mut low = tidb_codec::encode_key(&self.low_val)?;
        if self.low_exclude {
            low = prefix_next(&low);
        }
        let mut high = tidb_codec::encode_key(&self.high_val)?;
        if !self.high_exclude {
            high = prefix_next(&high);
        }
        Ok((low, high))
    }

    /// Go `Equal`.
    #[must_use]
    pub fn equal(&self, other: &Range) -> bool {
        if self.low_exclude != other.low_exclude || self.high_exclude != other.high_exclude {
            return false;
        }
        if self.low_val.len() != other.low_val.len()
            || self.high_val.len() != other.high_val.len()
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
        let mut result = Range::default();
        let other_range_more_granular = self.low_val.len() <= other_range.low_val.len();
        result.collators = if other_range_more_granular {
            other_range.collators.clone()
        } else {
            self.collators.clone()
        };

        let low_vs_high = compare_lexicographically(
            &self.low_val,
            &other_range.high_val,
            &result.collators,
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
            &result.collators,
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
            &result.collators,
            self.low_exclude,
            other_range.low_exclude,
            true,
            true,
        )?;
        if low_vs_low == std::cmp::Ordering::Less
            || (low_vs_low == std::cmp::Ordering::Equal && other_range_more_granular)
        {
            result.low_val = other_range.low_val.clone();
            result.low_exclude = other_range.low_exclude;
        } else {
            result.low_val = self.low_val.clone();
            result.low_exclude = self.low_exclude;
        }

        let high_vs_high = compare_lexicographically(
            &self.high_val,
            &other_range.high_val,
            &result.collators,
            self.high_exclude,
            other_range.high_exclude,
            false,
            false,
        )?;
        if high_vs_high == std::cmp::Ordering::Greater
            || (high_vs_high == std::cmp::Ordering::Equal && other_range_more_granular)
        {
            result.high_val = other_range.high_val.clone();
            result.high_exclude = other_range.high_exclude;
        } else {
            result.high_val = self.high_val.clone();
            result.high_exclude = self.high_exclude;
        }
        Ok(Some(result))
    }
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
        Datum::Int(v) => {
            (*v == i64::MIN && is_left_side) || (*v == i64::MAX && is_right_side)
        }
        Datum::UInt(v) => {
            (*v == 0 && unsigned_int_handle && is_left_side)
                || (*v == u64::MAX && is_right_side)
        }
        _ => false,
    }
}

/// Go `formatDatum`.
/// Go `strconv.Quote` over RAW bytes -- what `%q` prints for a bytes
/// datum. Valid printable runes stay verbatim (CJK included), quote and
/// backslash escape, and every other byte -- controls and bytes that are
/// not valid UTF-8, such as a sort key's 0x00 weight or a prefix cut
/// through the middle of a rune -- prints as `\xNN`.
fn go_quote(bytes: &[u8]) -> String {
    let mut out = String::from("\"");
    let mut rest = bytes;
    while !rest.is_empty() {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                push_go_quoted(&mut out, valid);
                break;
            }
            Err(error) => {
                let (valid, after) = rest.split_at(error.valid_up_to());
                push_go_quoted(&mut out, std::str::from_utf8(valid).expect("just validated"));
                let bad = error.error_len().unwrap_or(after.len());
                for byte in &after[..bad] {
                    out.push_str(&format!("\\x{byte:02x}"));
                }
                rest = &after[bad..];
            }
        }
    }
    out.push('"');
    out
}

fn push_go_quoted(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            ch if (ch as u32) < 0x20 || ch as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
}

fn format_datum(d: &Datum, is_left_side: bool) -> String {
    match d {
        Datum::Null => "NULL".to_owned(),
        Datum::MinNotNull => "-inf".to_owned(),
        Datum::MaxValue => "+inf".to_owned(),
        Datum::Int(v) => {
            if *v == i64::MIN && is_left_side {
                return "-inf".to_owned();
            }
            if *v == i64::MAX && !is_left_side {
                return "+inf".to_owned();
            }
            v.to_string()
        }
        Datum::UInt(v) => {
            if *v == u64::MAX && !is_left_side {
                return "+inf".to_owned();
            }
            v.to_string()
        }
        Datum::Bytes(bytes) => go_quote(bytes),
        Datum::String(s) => go_quote(s.bytes()),
        // Go's default arm is `fmt.Sprintf("%v", d.GetValue())`: floats
        // print through `strconv.FormatFloat(v, 'g', -1, 64)` — and a
        // KindFloat32 datum's GetValue is a float32, so its digits are the
        // 32-bit shortest form ("111.111115", not the widened f64 tail).
        Datum::Real(v) => go_g_float(*v),
        // Go `%v` of a MyDecimal prints its decimal text.
        Datum::Decimal(d) => d.to_string(),
        Datum::Float32(v) => {
            let narrowed = *v as f32;
            if (-4..21).contains(&(format!("{narrowed:e}")
                .split_once('e')
                .and_then(|(_, exp)| exp.parse::<i32>().ok())
                .unwrap_or(0)))
            {
                format!("{narrowed}")
            } else {
                go_g_float(f64::from(narrowed))
            }
        }
        // Go's `%v` of a MysqlEnum prints its NAME.
        Datum::Enum(value, _) => go_quote(value.name().as_bytes()),
        // The remaining kinds print their debug shape until a caller
        // formats one (Go's `%v` of those values is type-specific).
        other => format!("{other:?}"),
    }
}

/// Go `extendBound`: pad a partial bound with the correct infinity for its
/// side and openness (the multi-column prefix rule spelled out in Go's
/// comment).
fn extend_bound(bound: &mut Vec<Datum>, low_index: usize, high_index: usize, low: bool, open: bool) {
    for _ in low_index..high_index {
        let sentinel = if low {
            if open {
                // Open lower bound -> +inf (exclude the current value).
                Datum::MaxValue
            } else {
                // Closed lower bound -> -inf (include all lower values).
                Datum::MinNotNull
            }
        } else if open {
            // Open upper bound -> -inf (exclude the current value).
            Datum::MinNotNull
        } else {
            // Closed upper bound -> +inf (include all higher values).
            Datum::MaxValue
        };
        bound.push(sentinel);
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
    let mut local_bound1 = bound1.to_vec();
    let mut local_bound2 = bound2.to_vec();
    if n1 < n2 {
        extend_bound(&mut local_bound1, n1, n2, low1, open1);
    } else if n2 < n1 {
        extend_bound(&mut local_bound2, n2, n1, low2, open2);
    }

    for i in 0..n1.max(n2) {
        let cmp = local_bound1[i].compare(&local_bound2[i], collators[i])?;
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
fn prefix(super_value: &[Datum], sup_value: &[Datum], length: usize, collators: &[Collation]) -> bool {
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


/// Go `strconv.FormatFloat(v, 'g', -1, 64)` — `fmt`'s `%v` for floats:
/// shortest round-trip digits, switching to `e` notation when the decimal
/// exponent is below -4 or at/above 21, with Go's signed two-digit
/// exponent spelling.
pub(super) fn go_g_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_owned();
    }
    // Rust's `{:e}` is the shortest round-trip mantissa with its decimal
    // exponent — the inputs Go's 'g' decision reads.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` always carries an exponent");
    let exponent: i32 = exponent.parse().expect("a decimal exponent");
    if (-4..21).contains(&exponent) {
        // Plain notation; Rust's Display is the same shortest expansion.
        return format!("{value}");
    }
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}

/// Go `kv.Key.PrefixNext`: the next prefix key — increment the last
/// non-0xff byte, truncating after it; all-0xff appends a zero.
fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    for i in (0..next.len()).rev() {
        if next[i] != 0xff {
            next[i] += 1;
            next.truncate(i + 1);
            return next;
        }
    }
    let mut appended = key.to_vec();
    appended.push(0);
    appended
}

#[cfg(test)]
mod tests {
    use super::*;

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
            low_val: datums(low),
            high_val: datums(high),
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
                    low_val: vec![Datum::Real(1.1)],
                    high_val: vec![Datum::Real(1.9)],
                    collators: collators(1),
                    low_exclude: false,
                    high_exclude: true,
                },
                "[1.1,1.9)",
            ),
            (
                Range {
                    low_val: vec![Datum::MinNotNull],
                    high_val: vec![int(1)],
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
                    low_val: vec![string_datum("abc")],
                    high_val: vec![string_datum("abc")],
                    collators: collators(1),
                    low_exclude: false,
                    high_exclude: false,
                },
                true,
            ),
            (
                Range {
                    low_val: vec![int(1)],
                    high_val: vec![int(1), int(1)],
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
            assert_eq!(range.is_point(true), expected, "{}", range.to_display_string());
        }
    }

    /// Go `TestIsFullRange` (`types_test.go:161`), row by row.
    #[test]
    fn full_range_recognition_matches_go() {
        let rows = [
            (
                Range {
                    low_val: vec![Datum::MinNotNull],
                    high_val: vec![Datum::MaxValue],
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                true,
            ),
            (
                Range {
                    low_val: vec![Datum::MaxValue],
                    high_val: vec![Datum::MinNotNull],
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                false,
            ),
            (
                Range {
                    low_val: vec![int(1)],
                    high_val: vec![Datum::UInt(u64::MAX)],
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                false,
            ),
            // Go builds `nullDatum` by SetNull on a sentinel: a plain NULL.
            (
                Range {
                    low_val: vec![Datum::Null],
                    high_val: vec![Datum::UInt(u64::MAX)],
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                true,
            ),
            (
                Range {
                    low_val: vec![Datum::Null],
                    high_val: vec![Datum::Null],
                    collators: collators(1),
                    ..Range::default()
                },
                false,
                false,
            ),
            (
                Range {
                    low_val: vec![Datum::UInt(0)],
                    high_val: vec![Datum::UInt(u64::MAX)],
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
        let actual: Vec<String> = intersected
            .iter()
            .map(Range::to_display_string)
            .collect();
        assert_eq!(
            actual.join(","),
            "(100 0,100 +inf],(100,101),[101 -inf,101 10)"
        );
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
            (&[1, 2], &[1, 3], false, false, &[1, 3], &[1, 4], true, false, "<nil>"),
            (&[i64::MIN], &[1], false, false, &[2], &[i64::MAX], false, false, "<nil>"),
            (&[1, 2], &[1, 3], false, false, &[1, 3], &[1, 4], false, false, "[1 3,1 3]"),
            (&[1, 1, 2], &[1, 1, 5], false, false, &[1, 2], &[1, 3], true, true, "<nil>"),
            (
                &[100, 0], &[100, i64::MAX], true, false,
                &[i64::MIN, i64::MIN], &[100, i64::MIN], false, false, "<nil>",
            ),
            (&[100, 0], &[100, i64::MAX], true, false, &[i64::MIN], &[100], false, true, "<nil>"),
            (&[5], &[5], false, false, &[5], &[i64::MAX], true, false, "<nil>"),
            (&[1], &[1], false, false, &[5], &[i64::MAX], true, false, "<nil>"),
            (&[5], &[5], false, false, &[5, 1], &[5, i64::MAX], true, false, "(5 1,5 +inf]"),
            (&[1], &[1], false, false, &[5, 1], &[5, i64::MAX], true, false, "<nil>"),
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
            (&[i64::MIN], &[5], false, false, &[2], &[4], false, false, "[2,4]"),
            (&[1, 2], &[1, 5], false, false, &[1, 3], &[1, 4], true, false, "(1 3,1 4]"),
            (
                &[1, 1, i64::MIN], &[1, 1, 15], false, false,
                &[1, 1], &[1, 1], false, false, "[1 1 -inf,1 1 15]",
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
            (&[i64::MIN], &[5], false, false, &[2], &[14], false, false, "[2,5]"),
            (&[1, 2], &[1, 5], false, false, &[1, 3], &[1, 4], true, false, "(1 3,1 4]"),
            (
                &[1, 1, i64::MIN], &[1, 1, 15], false, false,
                &[1, 1, 4], &[1, 1, 25], false, false, "[1 1 4,1 1 15]",
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
