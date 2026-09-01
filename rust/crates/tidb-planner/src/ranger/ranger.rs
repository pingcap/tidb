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

//! Go `pkg/util/ranger/ranger.go`, the single-column assembly half: points
//! validate (`validInterval` — an interval survives only if its ENCODED
//! low key sorts before its high key), convert into the range's working
//! type, and assemble into [`Ranges`] for a column
//! (`points2Ranges`) or an int-handle table (`points2TableRanges`, whose
//! bounds must be concrete integers). `UnionRanges` re-sorts and merges by
//! the same encoded-key order the storage layer scans in.
//!
//! The multi-column append family (`appendPoints2Ranges`,
//! `AppendRanges2PointRanges`) and the `rangeDetacher` methods land with
//! `detacher.go`, their only caller.
//!
//! `rangeMaxSize` fallback: Go sizes its estimate with `unsafe.Sizeof`
//! layouts. The MECHANISM (fall back to the full range when the estimate
//! exceeds the quota) is the observable semantic and is ported; the
//! estimate itself uses this port's datum sizes, so the boundary sits at
//! slightly different byte counts. Every current caller passes 0 (no
//! limit).

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::expression::Expression;

use super::checker::UNSPECIFIED_LENGTH;
use super::points::{
    convert_point_in_place, get_full_range, Point, PointBuilder, PointBuilderError,
};
use super::types::{Range, Ranges};

/// Go `validInterval`: encode both ends the way [`Range::encode`] does and
/// keep the interval only when `low < high`.
fn valid_interval(low: &Point, high: &Point) -> Result<bool, PointBuilderError> {
    let mut l = tidb_codec::encode_key(std::slice::from_ref(&low.value))
        .map_err(|error| PointBuilderError::Unsupported(error.to_string()))?;
    if low.excl {
        l = prefix_next(&l);
    }
    let mut r = tidb_codec::encode_key(std::slice::from_ref(&high.value))
        .map_err(|error| PointBuilderError::Unsupported(error.to_string()))?;
    if !high.excl {
        r = prefix_next(&r);
    }
    Ok(l < r)
}

/// Go `kv.Key.PrefixNext` (shared with `types.rs`'s private copy by
/// intent: both are Go's five-line body).
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

/// Go `convertPointsInPlace`: convert each pair into `new_tp`, apply the
/// table-range sentinel substitutions, drop null-ended pairs when asked,
/// and compact the surviving pairs to the front.
fn convert_points_in_place(
    mut range_points: Vec<Point>,
    new_tp: &FieldType,
    skip_null: bool,
    table_range: bool,
    skip_plan_cache_reason: &mut Option<String>,
) -> Result<Vec<Point>, PointBuilderError> {
    let (min_value, max_value) = if new_tp.is_unsigned() {
        (Datum::UInt(0), Datum::UInt(u64::MAX))
    } else {
        (Datum::Int(i64::MIN), Datum::Int(i64::MAX))
    };
    let mut kept: Vec<Point> = Vec::with_capacity(range_points.len());
    let mut j = 0;
    while j + 1 < range_points.len() {
        let mut start_point = range_points[j].clone();
        convert_point_in_place(&mut start_point, new_tp, skip_plan_cache_reason)?;
        if table_range {
            if matches!(start_point.value, Datum::Null) {
                start_point.value = min_value.clone();
                start_point.excl = false;
            } else if matches!(start_point.value, Datum::MinNotNull) {
                start_point.value = min_value.clone();
            }
        }
        let mut end_point = range_points[j + 1].clone();
        convert_point_in_place(&mut end_point, new_tp, skip_plan_cache_reason)?;
        if table_range && matches!(end_point.value, Datum::MaxValue) {
            end_point.value = max_value.clone();
        }
        j += 2;
        if skip_null && matches!(end_point.value, Datum::Null) {
            continue;
        }
        if !valid_interval(&start_point, &end_point)? {
            continue;
        }
        kept.push(start_point);
        kept.push(end_point);
    }
    range_points.clear();
    Ok(kept)
}

/// This port's stand-in for Go's `unsafe.Sizeof`-based point estimate
/// (module header): a coarse per-datum size.
fn points_total_datum_size(points: &[Point]) -> i64 {
    points
        .iter()
        .map(|p| super::types::datum_mem_usage(&p.value))
        .sum()
}

fn ranges_total_datum_size(ranges: &super::types::Ranges) -> i64 {
    ranges
        .iter()
        .map(|range| {
            range
                .low_val
                .iter()
                .chain(range.high_val.iter())
                .map(super::types::datum_mem_usage)
                .sum::<i64>()
        })
        .sum()
}

/// Go `estimateMemUsageForPoints2Ranges` over this port's sizes.
fn estimate_mem_usage_for_points_to_ranges(range_points: &[Point]) -> i64 {
    (super::types::EMPTY_RANGE_SIZE + 16) * range_points.len() as i64 / 2
        + points_total_datum_size(range_points)
}

/// Go `points2Ranges`: one column's points into ranges. The second return
/// says the memory fallback fired and the FULL range came back instead.
pub fn points_to_ranges(
    range_points: Vec<Point>,
    new_tp: &FieldType,
    range_max_size: i64,
    skip_plan_cache_reason: &mut Option<String>,
) -> Result<(Ranges, bool), PointBuilderError> {
    let has_not_null = new_tp.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL != 0;
    let range_points = convert_points_in_place(
        range_points,
        new_tp,
        has_not_null,
        false,
        skip_plan_cache_reason,
    )?;
    if range_max_size > 0 && estimate_mem_usage_for_points_to_ranges(&range_points) > range_max_size
    {
        let full = if has_not_null {
            super::points::full_not_null_range()
        } else {
            super::points::full_range()
        };
        return Ok((full, true));
    }
    let mut ranges = Ranges::with_capacity(range_points.len() / 2);
    let collator = new_tp.collation();
    let mut i = 0;
    while i + 1 < range_points.len() {
        ranges.push(Range {
            low_val: vec![range_points[i].value.clone()],
            low_exclude: range_points[i].excl,
            high_val: vec![range_points[i + 1].value.clone()],
            high_exclude: range_points[i + 1].excl,
            collators: vec![collator],
        });
        i += 2;
    }
    Ok((ranges, false))
}

/// Go `points2TableRanges`: nulls drop, sentinels become the concrete int
/// bounds a table's kv range needs.
pub fn points_to_table_ranges(
    range_points: Vec<Point>,
    new_tp: &FieldType,
    range_max_size: i64,
    skip_plan_cache_reason: &mut Option<String>,
) -> Result<(Ranges, bool), PointBuilderError> {
    let range_points =
        convert_points_in_place(range_points, new_tp, true, true, skip_plan_cache_reason)?;
    if range_max_size > 0 && estimate_mem_usage_for_points_to_ranges(&range_points) > range_max_size
    {
        return Ok((super::points::full_int_range(new_tp.is_unsigned()), true));
    }
    let mut ranges = Ranges::with_capacity(range_points.len() / 2);
    let collator = new_tp.collation();
    let mut i = 0;
    while i + 1 < range_points.len() {
        ranges.push(Range {
            low_val: vec![range_points[i].value.clone()],
            low_exclude: range_points[i].excl,
            high_val: vec![range_points[i + 1].value.clone()],
            high_exclude: range_points[i + 1].excl,
            collators: vec![collator],
        });
        i += 2;
    }
    Ok((ranges, false))
}

/// Go `newFieldType`: the RANGE-BUILDING type — ints widen to LONGLONG (no
/// overflow error), string/blob/float kinds drop their length (no truncate
/// error), everything else passes through.
#[must_use]
pub fn new_field_type(tp: &FieldType) -> FieldType {
    match tp.code() {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong => {
            let mut new_tp = FieldType::new(FieldTypeCode::LongLong);
            new_tp.set_flags(tp.flags());
            new_tp.set_charset_name(tp.charset_name());
            new_tp
        }
        FieldTypeCode::Float
        | FieldTypeCode::Double
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::String
        | FieldTypeCode::Varchar
        | FieldTypeCode::VarString => {
            let mut new_tp = FieldType::new(tp.code());
            new_tp.set_collation_name(tp.collation_name());
            new_tp.set_collation(tp.collation());
            new_tp.set_flen(UNSPECIFIED_LENGTH);
            new_tp.set_charset_name(tp.charset_name());
            new_tp
        }
        _ => tp.clone(),
    }
}

/// Go `convertStringFTToBinaryCollate`: sort-key ranges compare as raw
/// bytes, so the assembled type's collation becomes binary (enums/sets and
/// non-strings keep their own).
#[must_use]
pub fn convert_string_ft_to_binary_collate(ft: &FieldType) -> FieldType {
    if ft.eval_type() != tidb_datatype::EvalType::String
        || matches!(ft.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
    {
        return ft.clone();
    }
    let mut new_tp = ft.clone();
    new_tp.set_charset_name("binary");
    new_tp.set_collation_name("binary");
    new_tp.set_collation(tidb_datatype::Collation::Binary);
    new_tp
}

/// Go `hasPrefix`.
#[must_use]
pub fn has_prefix(lengths: &[i64]) -> bool {
    lengths.iter().any(|l| *l != UNSPECIFIED_LENGTH)
}

/// Go `UnionRanges`: sort by encoded start key and merge overlapping (or,
/// with `merge_consecutive`, touching) ranges.
pub fn union_ranges(ranges: Ranges, merge_consecutive: bool) -> Result<Ranges, PointBuilderError> {
    if ranges.is_empty() {
        return Ok(Ranges::new());
    }
    struct SortRange {
        original: Range,
        encoded_start: Vec<u8>,
        encoded_end: Vec<u8>,
    }
    let mut objects = Vec::with_capacity(ranges.len());
    for ran in ranges {
        let (left, right) = ran
            .encode()
            .map_err(|error| PointBuilderError::Unsupported(error.to_string()))?;
        objects.push(SortRange {
            original: ran,
            encoded_start: left,
            encoded_end: right,
        });
    }
    objects.sort_by(|a, b| a.encoded_start.cmp(&b.encoded_start));
    let mut result = Ranges::new();
    let mut iter = objects.into_iter();
    let mut last_range = iter.next().expect("checked non-empty");
    for object in iter {
        let overlaps = if merge_consecutive {
            last_range.encoded_end >= object.encoded_start
        } else {
            last_range.encoded_end > object.encoded_start
        };
        if overlaps {
            if last_range.encoded_end < object.encoded_end {
                last_range.encoded_end = object.encoded_end;
                last_range.original.high_val = object.original.high_val;
                last_range.original.high_exclude = object.original.high_exclude;
            }
        } else {
            result.push(last_range.original);
            last_range = object;
        }
    }
    result.push(last_range.original);
    Ok(result)
}

/// Go `appendPoints2IndexRange`: widen one POINT range by one more
/// column's points.
fn append_points_to_index_range(origin: &Range, range_points: &[Point], ft: &FieldType) -> Ranges {
    let mut new_ranges = Ranges::with_capacity(range_points.len() / 2);
    let extra_collator = ft.collation();
    let mut i = 0;
    while i + 1 < range_points.len() {
        let start_point = &range_points[i];
        let end_point = &range_points[i + 1];
        let mut low_val = origin.low_val.clone();
        low_val.push(start_point.value.clone());
        let mut high_val = origin.high_val.clone();
        high_val.push(end_point.value.clone());
        let mut collators = origin.collators.clone();
        collators.push(extra_collator);
        new_ranges.push(Range {
            low_val,
            low_exclude: start_point.excl,
            high_val,
            high_exclude: end_point.excl,
            collators,
        });
        i += 2;
    }
    new_ranges
}

/// Go `appendPoints2Ranges`: the additional column's points append only to
/// POINT ranges — `(a > 1, b = 2)` cannot conjoin on an index `(a, b)` —
/// non-point ranges pass through unchanged.
pub fn append_points_to_ranges(
    origin: Ranges,
    range_points: Vec<Point>,
    new_tp: &FieldType,
    range_max_size: i64,
    regard_null_as_point: bool,
    skip_plan_cache_reason: &mut Option<String>,
) -> Result<(Ranges, bool), PointBuilderError> {
    let range_points =
        convert_points_in_place(range_points, new_tp, false, false, skip_plan_cache_reason)?;
    if range_max_size > 0 {
        // Go `estimateMemUsageForAppendPoints2Ranges`.
        let len1 = origin.len() as i64;
        let len2 = range_points.len() as i64 / 2;
        let estimate = (super::types::EMPTY_RANGE_SIZE
            + (origin.first().map_or(0, |r| r.low_val.len() as i64) + 1) * 16)
            * len1
            * len2
            + ranges_total_datum_size(&origin) * len2
            + points_total_datum_size(&range_points) * len1;
        if estimate > range_max_size {
            return Ok((origin, true));
        }
    }
    let mut new_index_ranges = Ranges::new();
    for o_range in origin {
        if !o_range.is_point(regard_null_as_point) {
            new_index_ranges.push(o_range);
        } else {
            new_index_ranges.extend(append_points_to_index_range(
                &o_range,
                &range_points,
                new_tp,
            ));
        }
    }
    Ok((new_index_ranges, false))
}

/// Go `AppendRanges2PointRanges`: the tail ranges appended to every point
/// range, or the point ranges UNCHANGED with `true` when Go's estimate
/// (`estimateMemUsageForAppendRanges2PointRanges`) exceeds the quota.
#[must_use]
pub fn append_ranges_to_point_ranges(
    point_ranges: super::types::Ranges,
    ranges: &super::types::Ranges,
    range_max_size: i64,
) -> (super::types::Ranges, bool) {
    if ranges.is_empty() {
        return (point_ranges, false);
    }
    if range_max_size > 0 {
        let len1 = point_ranges.len() as i64;
        let len2 = ranges.len() as i64;
        let collator_size = (point_ranges.first().map_or(0, |r| r.low_val.len() as i64)
            + ranges.first().map_or(0, |r| r.low_val.len() as i64))
            * 16;
        let estimate = (super::types::EMPTY_RANGE_SIZE + collator_size) * len1 * len2
            + ranges_total_datum_size(&point_ranges) * len2
            + ranges_total_datum_size(ranges) * len1;
        if estimate > range_max_size {
            return (point_ranges, true);
        }
    }
    let mut new_ranges = super::types::Ranges::with_capacity(point_ranges.len() * ranges.len());
    for point_range in &point_ranges {
        for tail in ranges {
            let mut low_val = point_range.low_val.clone();
            low_val.extend(tail.low_val.iter().cloned());
            let mut high_val = point_range.high_val.clone();
            high_val.extend(tail.high_val.iter().cloned());
            let mut collators = point_range.collators.clone();
            collators.extend(tail.collators.iter().copied());
            new_ranges.push(super::types::Range {
                low_val,
                low_exclude: tail.low_exclude,
                high_val,
                high_exclude: tail.high_exclude,
                collators,
            });
        }
    }
    (new_ranges, false)
}

/// The product of one column-range build: the ranges plus which conditions
/// were CONSUMED and which REMAIN as filters (Go's trailing return pair).
#[derive(Debug)]
pub struct ColumnRangeResult {
    /// The built ranges.
    pub ranges: Ranges,
    /// Go's second return: the conditions the ranges absorbed.
    pub access_conds: Vec<Expression>,
    /// Go's third return: the conditions that stay as filters (non-empty
    /// only on the memory fallback).
    pub remained_conds: Vec<Expression>,
}

/// Go `buildColumnRange`.
fn build_column_range_impl(
    access_conditions: &[Expression],
    tp: &FieldType,
    table_range: bool,
    col_len: i64,
    range_max_size: i64,
) -> Result<ColumnRangeResult, PointBuilderError> {
    let mut builder = PointBuilder::default();
    let new_tp = new_field_type(tp);
    let mut range_points = get_full_range();
    for cond in access_conditions {
        let built = builder.build(cond, &new_tp, col_len, true);
        range_points =
            super::points::intersection(&range_points, &built, tidb_datatype::Collation::Binary)?;
        if let Some(error) = builder.err.take() {
            return Err(error);
        }
    }
    let new_tp = convert_string_ft_to_binary_collate(&new_tp);
    let mut skip_reason = builder.skip_plan_cache_reason.take();
    let (ranges, range_fallback) = if table_range {
        points_to_table_ranges(range_points, &new_tp, range_max_size, &mut skip_reason)?
    } else {
        points_to_ranges(range_points, &new_tp, range_max_size, &mut skip_reason)?
    };
    if range_fallback {
        // Go `RecordRangeFallback`: the conditions all REMAIN as filters.
        return Ok(ColumnRangeResult {
            ranges,
            access_conds: Vec::new(),
            remained_conds: access_conditions.to_vec(),
        });
    }
    let ranges = if col_len != UNSPECIFIED_LENGTH {
        union_ranges(ranges, true)?
    } else {
        ranges
    };
    Ok(ColumnRangeResult {
        ranges,
        access_conds: access_conditions.to_vec(),
        remained_conds: Vec::new(),
    })
}

/// Go `BuildTableRange`: the int-handle PK's scan range.
pub fn build_table_range(
    access_conditions: &[Expression],
    tp: &FieldType,
    range_max_size: i64,
) -> Result<ColumnRangeResult, PointBuilderError> {
    build_column_range_impl(
        access_conditions,
        tp,
        true,
        UNSPECIFIED_LENGTH,
        range_max_size,
    )
}

/// Go `BuildColumnRange`: a general column's range (a column path or a
/// prefix-index column via `col_len`).
pub fn build_column_range(
    conds: &[Expression],
    tp: &FieldType,
    col_len: i64,
    range_mem_quota: i64,
) -> Result<ColumnRangeResult, PointBuilderError> {
    if conds.is_empty() {
        return Ok(ColumnRangeResult {
            ranges: super::points::full_range(),
            access_conds: Vec::new(),
            remained_conds: Vec::new(),
        });
    }
    build_column_range_impl(conds, tp, false, col_len, range_mem_quota)
}

/// Go `RangesToString` (`pkg/util/ranger/ranger.go`) rendered as a native
/// Rust helper.  The statement-context argument in Go only supplies datum
/// comparison policy; this port's typed [`Datum`] comparison has no mutable
/// warning sink, so callers provide the range list and column names directly.
///
/// Each range is an OR arm, and each column in a composite range is an AND
/// arm.  As in Go, exclusion flags apply only to the final column and all
/// preceding columns must be point-equal.
pub fn ranges_to_string(ranges: &[Range], col_names: &[&str]) -> Result<String, String> {
    for range in ranges {
        if range.low_val.len() != range.high_val.len() {
            return Err("range length mismatch".to_owned());
        }
        if range.low_val.len() > col_names.len() {
            return Err("column name length mismatch".to_owned());
        }
        if range.collators.len() < range.low_val.len() {
            return Err("collator length mismatch".to_owned());
        }
    }

    let mut arms = Vec::with_capacity(ranges.len());
    for range in ranges {
        let mut columns = Vec::with_capacity(range.low_val.len());
        for (index, (low, high)) in range.low_val.iter().zip(&range.high_val).enumerate() {
            if index + 1 < range.low_val.len()
                && low
                    .compare(high, range.collators[index])
                    .map_err(|error| format!("comparing values error: {error}"))?
                    != std::cmp::Ordering::Equal
            {
                return Err("unexpected form of range".to_owned());
            }
            let low_exclude = range.low_exclude && index + 1 == range.low_val.len();
            let high_exclude = range.high_exclude && index + 1 == range.low_val.len();
            columns.push(format!(
                "({})",
                range_single_col_to_string(
                    low,
                    high,
                    low_exclude,
                    high_exclude,
                    col_names[index],
                    range.collators[index],
                )?
            ));
        }
        arms.push(format!("({})", columns.join(" and ")));
    }

    let result = arms.join(" or ");
    if !result.is_empty() && !result.contains(" or ") && !result.contains(" and ") {
        let trimmed = result.trim_matches('(').trim_matches(')');
        if trimmed == "true" {
            return Ok("true".to_owned());
        }
    }
    Ok(result)
}

/// Go `RangeSingleColToString` rendered without the mutable statement
/// context.  The returned text is valid SQL for the supported value kinds;
/// Go's `ValueExpr.Restore` deliberately rejects ENUM/SET/BIT/JSON/RAW and
/// range sentinels, so those same unsupported kinds return an error here.
pub fn range_single_col_to_string(
    low: &Datum,
    high: &Datum,
    low_exclude: bool,
    high_exclude: bool,
    col_name: &str,
    collator: tidb_datatype::Collation,
) -> Result<String, String> {
    let low_special = matches!(low, Datum::Null | Datum::MinNotNull | Datum::MaxValue);
    let high_special = matches!(high, Datum::Null | Datum::MinNotNull | Datum::MaxValue);
    if low_special && high_special {
        return Ok(match (low, high, low_exclude, high_exclude) {
            (Datum::Null, Datum::Null, false, false) => format!("{col_name} is null"),
            (Datum::Null, Datum::MaxValue, false, _) => "true".to_owned(),
            (Datum::MinNotNull, Datum::MaxValue, _, _) => {
                format!("{col_name} is not null")
            }
            _ => "false".to_owned(),
        });
    }

    let comparison = low
        .compare(high, collator)
        .map_err(|error| error.to_string())?;
    if comparison == std::cmp::Ordering::Equal
        && !low_exclude
        && !high_exclude
        && !matches!(low, Datum::Null)
    {
        return Ok(format!("{col_name} = {}", restore_range_literal(low)?));
    }

    let mut parts = Vec::with_capacity(2);
    match low {
        Datum::Null => parts.push(format!("{col_name} is null")),
        Datum::MinNotNull => {}
        _ => parts.push(format!(
            "{col_name} {} {}",
            if low_exclude { ">" } else { ">=" },
            restore_range_literal(low)?
        )),
    }
    match high {
        Datum::MaxValue => parts.push("true".to_owned()),
        _ => parts.push(format!(
            "{col_name} {} {}",
            if high_exclude { "<" } else { "<=" },
            restore_range_literal(high)?
        )),
    }
    Ok(parts.join(if matches!(low, Datum::Null) {
        " or "
    } else if parts.len() == 1 {
        ""
    } else {
        " and "
    }))
}

fn restore_range_literal(value: &Datum) -> Result<String, String> {
    match value {
        Datum::Null => Ok("NULL".to_owned()),
        Datum::Int(value) => Ok(value.to_string()),
        Datum::UInt(value) => Ok(value.to_string()),
        Datum::Decimal(value) => Ok(value.to_string()),
        Datum::Real(value) => Ok(format_go_scientific(format!("{value:e}"))),
        Datum::Float32(value) => Ok(format_go_scientific(format!("{:e}", *value as f32))),
        Datum::String(value) => quote_range_bytes(value.bytes()),
        Datum::Bytes(value) => quote_range_bytes(value),
        Datum::BinaryLiteral(value) => Ok(value.to_bit_literal_string(true)),
        Datum::Duration(value) => Ok(format!("'{value}'")),
        Datum::Time(value) => Ok(format!("'{value}'")),
        Datum::MinNotNull | Datum::MaxValue => {
            Err("Not implemented: range sentinel cannot be restored".to_owned())
        }
        Datum::Enum(_, _)
        | Datum::Bit(_)
        | Datum::Set(_, _)
        | Datum::Json(_)
        | Datum::Raw(_)
        | Datum::VectorFloat32(_) => Err("Not implemented".to_owned()),
    }
}

fn quote_range_bytes(bytes: &[u8]) -> Result<String, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| format!("invalid UTF-8 string literal: {error}"))?;
    Ok(format!(
        "'{}'",
        text.replace('\\', "\\\\").replace('\'', "''")
    ))
}

fn format_go_scientific(rendered: String) -> String {
    match rendered.as_str() {
        "NaN" => return "NaN".to_owned(),
        "inf" => return "+Inf".to_owned(),
        "-inf" => return "-Inf".to_owned(),
        _ => {}
    }
    let Some((mantissa, exponent)) = rendered.split_once('e') else {
        return rendered;
    };
    let exponent = exponent.parse::<i32>().unwrap_or(0);
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_expr::scalar_function::ScalarFunction;

    fn int_col(unique_id: i64) -> Expression {
        Expression::Column(tidb_expr::column::Column::new(
            unique_id,
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn int_const(v: i64) -> Expression {
        Expression::Constant(tidb_expr::constant::Constant::new(
            Datum::Int(v),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ))
    }

    fn shown(result: &ColumnRangeResult) -> Vec<String> {
        result.ranges.iter().map(Range::to_display_string).collect()
    }

    #[test]
    fn range_single_column_sql_rendering_matches_go() {
        let binary = tidb_datatype::Collation::Binary;
        assert_eq!(
            range_single_col_to_string(&Datum::Int(7), &Datum::Int(7), false, false, "a", binary,)
                .unwrap(),
            "a = 7"
        );
        assert_eq!(
            range_single_col_to_string(&Datum::Null, &Datum::Int(5), false, true, "a", binary,)
                .unwrap(),
            "a is null or a < 5"
        );
        assert_eq!(
            range_single_col_to_string(
                &Datum::MinNotNull,
                &Datum::MaxValue,
                false,
                false,
                "a",
                binary,
            )
            .unwrap(),
            "a is not null"
        );
        assert_eq!(
            range_single_col_to_string(
                &Datum::new_string("a\\b'c"),
                &Datum::new_string("a\\b'c"),
                false,
                false,
                "a",
                binary,
            )
            .unwrap(),
            "a = 'a\\\\b''c'"
        );
    }

    #[test]
    fn ranges_to_string_renders_composite_and_full_ranges() {
        let binary = tidb_datatype::Collation::Binary;
        let composite = Range {
            low_val: vec![Datum::Int(1), Datum::Int(2)],
            high_val: vec![Datum::Int(1), Datum::Int(5)],
            collators: vec![binary, binary],
            low_exclude: false,
            high_exclude: true,
        };
        assert_eq!(
            ranges_to_string(&[composite], &["a", "b"]).unwrap(),
            "((a = 1) and (b >= 2 and b < 5))"
        );

        let full = Range {
            low_val: vec![Datum::Null],
            high_val: vec![Datum::MaxValue],
            collators: vec![binary],
            ..Range::default()
        };
        assert_eq!(ranges_to_string(&[full], &["a"]).unwrap(), "true");
    }

    #[test]
    fn ranges_to_string_rejects_malformed_composite_ranges() {
        let binary = tidb_datatype::Collation::Binary;
        let mismatched = Range {
            low_val: vec![Datum::Int(1)],
            high_val: vec![],
            collators: vec![binary],
            ..Range::default()
        };
        assert_eq!(
            ranges_to_string(&[mismatched], &["a"]).unwrap_err(),
            "range length mismatch"
        );

        let unexpected = Range {
            low_val: vec![Datum::Int(1), Datum::Int(2)],
            high_val: vec![Datum::Int(3), Datum::Int(4)],
            collators: vec![binary, binary],
            ..Range::default()
        };
        assert_eq!(
            ranges_to_string(&[unexpected], &["a", "b"]).unwrap_err(),
            "unexpected form of range"
        );
    }

    /// `BuildTableRange` end to end: `a > 1 AND a < 5` over the int-handle
    /// PK — sentinels become concrete ints, nulls drop.
    #[test]
    fn table_ranges_assemble_with_concrete_bounds() {
        let tp = FieldType::new(FieldTypeCode::LongLong);
        let a = int_col(1);
        let conds = vec![
            func("gt", vec![a.clone(), int_const(1)]),
            func("lt", vec![a.clone(), int_const(5)]),
        ];
        let result = build_table_range(&conds, &tp, 0).expect("builds");
        assert_eq!(shown(&result), ["(1,5)"]);
        assert_eq!(result.access_conds.len(), 2);
        assert!(result.remained_conds.is_empty());

        // No conditions on a COLUMN range answers the full range.
        let empty = build_column_range(&[], &tp, UNSPECIFIED_LENGTH, 0).expect("builds");
        assert_eq!(shown(&empty), ["[NULL,+inf]"]);

        // `a != 3` over the table: two int-bounded ranges.
        let ne = vec![func("ne", vec![a.clone(), int_const(3)])];
        let result = build_table_range(&ne, &tp, 0).expect("builds");
        assert_eq!(shown(&result), ["[-inf,3)", "(3,+inf]"]);
    }

    /// `points2TableRanges` drops NULL point pairs (a table's kv range has
    /// no null row): `a IS NULL` over the PK is EMPTY, over a column it is
    /// the null point.
    #[test]
    fn null_points_drop_from_table_ranges() {
        let tp = FieldType::new(FieldTypeCode::LongLong);
        let is_null = vec![func("isnull", vec![int_col(1)])];
        let table = build_table_range(&is_null, &tp, 0).expect("builds");
        assert!(table.ranges.is_empty(), "{:?}", shown(&table));
        let column = build_column_range(&is_null, &tp, UNSPECIFIED_LENGTH, 0).expect("builds");
        assert_eq!(shown(&column), ["[NULL,NULL]"]);
    }

    /// `UnionRanges` merges by encoded-key order: overlapping merges
    /// always, touching merges only with `merge_consecutive`.
    #[test]
    fn union_ranges_merges_by_encoded_order() {
        let int_range = |low: i64, high: i64, high_exclude: bool| Range {
            low_val: vec![Datum::Int(low)],
            high_val: vec![Datum::Int(high)],
            collators: vec![tidb_datatype::Collation::Binary],
            low_exclude: false,
            high_exclude,
        };
        // [1, 4] and [3, 6] overlap: one range.
        let merged = union_ranges(vec![int_range(3, 6, false), int_range(1, 4, false)], false)
            .expect("unions");
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].to_display_string(), "[1,6]");
        // [1, 3) and [3, 6]: touching. Without consecutive-merge they stay
        // apart; with it they fuse.
        let apart = union_ranges(vec![int_range(1, 3, true), int_range(3, 6, false)], false)
            .expect("unions");
        assert_eq!(apart.len(), 2);
        let fused = union_ranges(vec![int_range(1, 3, true), int_range(3, 6, false)], true)
            .expect("unions");
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].to_display_string(), "[1,6]");
    }

    /// `newFieldType`: ints widen to LONGLONG keeping flags, strings drop
    /// their length, others pass through.
    #[test]
    fn range_building_types_widen_like_go() {
        let mut tiny = FieldType::new(FieldTypeCode::Tiny);
        tiny.set_flags(tiny.flags() | tidb_datatype::FieldTypeFlags::UNSIGNED);
        let widened = new_field_type(&tiny);
        assert_eq!(widened.code(), FieldTypeCode::LongLong);
        assert!(widened.is_unsigned());

        let mut varchar = FieldType::new(FieldTypeCode::Varchar);
        varchar.set_flen(10);
        varchar.set_collation_name("utf8mb4_bin");
        let widened = new_field_type(&varchar);
        assert_eq!(widened.code(), FieldTypeCode::Varchar);
        assert_eq!(widened.flen(), UNSPECIFIED_LENGTH);

        let year = FieldType::new(FieldTypeCode::Year);
        assert_eq!(new_field_type(&year).code(), FieldTypeCode::Year);
    }
}
