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

//! Go `pkg/util/ranger/points.go`, on the whole-file track: the interval
//! END-POINT model ranges are assembled from, its ordering (where the
//! open/closed and start/end flags break value ties), the canonical
//! full/null range constructors, and the constant-fixup pair
//! (`handleUnsignedCol`, `handleBoundCol`) that keeps `col op constant`
//! ranges valid when the constant sits outside the column's domain.
//!
//! The `builder` (Go's `build` dispatch over comparison/IN/LIKE/NOT
//! expressions) continues in this file next; nothing lands as a stub.

use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};

use super::types::Ranges;

/// Go `RangeType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeType {
    /// Go `IntRangeType`.
    Int,
    /// Go `ColumnRangeType`.
    Column,
    /// Go `IndexRangeType`.
    Index,
}

/// Go `point`: one end of an interval.
#[derive(Clone, Debug, Default)]
pub struct Point {
    /// Go `value`.
    pub value: Datum,
    /// Go `excl`.
    pub excl: bool,
    /// Go `start`.
    pub start: bool,
}

impl Point {
    /// Go `point.String`.
    #[must_use]
    pub fn to_display_string(&self) -> String {
        let val = match &self.value {
            Datum::MinNotNull => "-inf".to_owned(),
            Datum::MaxValue => "+inf".to_owned(),
            Datum::Null => "<nil>".to_owned(),
            Datum::Int(v) => v.to_string(),
            Datum::UInt(v) => v.to_string(),
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
            other => format!("{other:?}"),
        };
        if self.start {
            let symbol = if self.excl { "(" } else { "[" };
            format!("{symbol}{val}")
        } else {
            let symbol = if self.excl { ")" } else { "]" };
            format!("{val}{symbol}")
        }
    }
}

/// Go `rangePointCmp`: value order first (enums by their numeric value),
/// then the flag tie-break.
pub fn range_point_cmp(
    a: &Point,
    b: &Point,
    collator: Collation,
) -> Result<std::cmp::Ordering, tidb_datatype::DatumValueError> {
    if let (Datum::Enum(left, _), Datum::Enum(right, _)) = (&a.value, &b.value) {
        return Ok(range_point_enum_cmp(a, b, left.value(), right.value()));
    }
    let cmp = a.value.compare(&b.value, collator)?;
    if cmp != std::cmp::Ordering::Equal {
        return Ok(cmp);
    }
    Ok(range_point_equal_value_cmp(a, b))
}

/// Go `rangePointEnumCmp`: enums order by NUMBER, not collation.
fn range_point_enum_cmp(a: &Point, b: &Point, left: u64, right: u64) -> std::cmp::Ordering {
    let cmp = left.cmp(&right);
    if cmp != std::cmp::Ordering::Equal {
        return cmp;
    }
    range_point_equal_value_cmp(a, b)
}

/// Go `rangePointEqualValueCmp`, the flag tie-table: at equal values, a
/// closed start sorts before an open start, an end before a start when
/// either is open, and a closed end before an open end's shadow.
fn range_point_equal_value_cmp(a: &Point, b: &Point) -> std::cmp::Ordering {
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
        std::cmp::Ordering::Less
    } else {
        std::cmp::Ordering::Equal
    }
}

/// Go `getFullRange`: `[null, +inf]` as points.
#[must_use]
pub fn get_full_range() -> Vec<Point> {
    vec![
        Point {
            start: true,
            ..Point::default()
        },
        Point {
            value: Datum::MaxValue,
            ..Point::default()
        },
    ]
}

/// Go `getNotNullFullRange`: `[-inf, +inf]` as points.
#[must_use]
pub fn get_not_null_full_range() -> Vec<Point> {
    vec![
        Point {
            value: Datum::MinNotNull,
            start: true,
            ..Point::default()
        },
        Point {
            value: Datum::MaxValue,
            ..Point::default()
        },
    ]
}

/// Go `FullIntRange`: the TABLE range, whose max must be a concrete int
/// (a table range cannot carry `MaxValueDatum`).
#[must_use]
pub fn full_int_range(is_unsigned: bool) -> Ranges {
    if is_unsigned {
        return vec![super::types::Range {
            low_val: vec![Datum::UInt(0)],
            high_val: vec![Datum::UInt(u64::MAX)],
            collators: vec![Collation::Binary],
            ..super::types::Range::default()
        }];
    }
    vec![super::types::Range {
        low_val: vec![Datum::Int(i64::MIN)],
        high_val: vec![Datum::Int(i64::MAX)],
        collators: vec![Collation::Binary],
        ..super::types::Range::default()
    }]
}

/// Go `FullRange`: `[null, +inf]`.
#[must_use]
pub fn full_range() -> Ranges {
    vec![super::types::Range {
        low_val: vec![Datum::Null],
        high_val: vec![Datum::MaxValue],
        collators: vec![Collation::Binary],
        ..super::types::Range::default()
    }]
}

/// Go `FullNotNullRange`: `(-inf, +inf)`.
#[must_use]
pub fn full_not_null_range() -> Ranges {
    vec![super::types::Range {
        low_val: vec![Datum::MinNotNull],
        high_val: vec![Datum::MaxValue],
        collators: vec![Collation::Binary],
        ..super::types::Range::default()
    }]
}

/// Go `NullRange`: `[null, null]`.
#[must_use]
pub fn null_range() -> Ranges {
    vec![super::types::Range {
        low_val: vec![Datum::Null],
        high_val: vec![Datum::Null],
        collators: vec![Collation::Binary],
        ..super::types::Range::default()
    }]
}

/// Go's comparison-operator spellings, as `FuncName.L` carries them.
pub const OP_EQ: &str = "eq";
/// `ast.NE`.
pub const OP_NE: &str = "ne";
/// `ast.LT`.
pub const OP_LT: &str = "lt";
/// `ast.LE`.
pub const OP_LE: &str = "le";
/// `ast.GT`.
pub const OP_GT: &str = "gt";
/// `ast.GE`.
pub const OP_GE: &str = "ge";
/// `ast.NullEQ`.
pub const OP_NULL_EQ: &str = "nulleq";

/// Go `handleUnsignedCol`: an unsigned column compared with a NEGATIVE
/// constant. GT/GE/NE clamp to `>= 0`; every other operator has no valid
/// range.
#[must_use]
pub fn handle_unsigned_col(
    ft: &FieldType,
    mut val: Datum,
    op: &str,
) -> (Datum, String, bool) {
    let is_unsigned = ft.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED != 0;
    let is_negative = match &val {
        Datum::Int(v) => *v < 0,
        Datum::Float32(v) | Datum::Real(v) => *v < 0.0,
        Datum::Decimal(d) => d.signum() < 0,
        _ => false,
    };
    if !is_unsigned || !is_negative {
        return (val, op.to_owned(), true);
    }
    if op == OP_GT || op == OP_GE || op == OP_NE {
        val = match &val {
            Datum::Int(_) => Datum::UInt(0),
            Datum::Float32(_) => Datum::Float32(0.0),
            Datum::Real(_) => Datum::Real(0.0),
            Datum::Decimal(_) => Datum::Decimal(tidb_datatype::Decimal::from_int(0)),
            other => other.clone(),
        };
        return (val, OP_GE.to_owned(), true);
    }
    (val, op.to_owned(), false)
}

/// Go `handleBoundCol`: a SIGNED int column against a beyond-`MaxInt64`
/// unsigned constant clamps NE/LE/LT to `<= MaxInt64` and invalidates
/// GT/GE; a `FLOAT` column clamps at the `f32` domain the same way.
#[must_use]
pub fn handle_bound_col(ft: &FieldType, mut val: Datum, op: &str) -> (Datum, String, bool) {
    let is_unsigned = ft.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED != 0;
    if is_unsigned {
        return (val, op.to_owned(), true);
    }
    let mut op = op.to_owned();
    match ft.code() {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong => {
            if let Datum::UInt(v) = &val {
                if *v > i64::MAX as u64 {
                    match op.as_str() {
                        OP_GT | OP_GE => return (val, op, false),
                        OP_NE | OP_LE | OP_LT => {
                            op = OP_LE.to_owned();
                            val = Datum::Int(i64::MAX);
                        }
                        _ => {}
                    }
                }
            }
        }
        FieldTypeCode::Float => {
            if let Datum::Real(v) = &val {
                if *v > f64::from(f32::MAX) {
                    match op.as_str() {
                        OP_GT | OP_GE => return (val, op, false),
                        OP_NE | OP_LE | OP_LT => {
                            op = OP_LE.to_owned();
                            val = Datum::Float32(f64::from(f32::MAX));
                        }
                        _ => {}
                    }
                } else if *v < f64::from(-f32::MAX) {
                    match op.as_str() {
                        OP_LE | OP_LT => return (val, op, false),
                        OP_GT | OP_GE | OP_NE => {
                            op = OP_GE.to_owned();
                            val = Datum::Float32(f64::from(-f32::MAX));
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }
    (val, op, true)
}


/// Go `CutDatumByPrefixLen` (`ranger.go:737`): cut a string/bytes datum to
/// a prefix-index length — by BYTES for binary/ascii charsets, by RUNES
/// otherwise. Answers whether a cut happened.
pub fn cut_datum_by_prefix_len(v: &mut Datum, length: i64, tp: &FieldType) -> bool {
    if length == super::checker::UNSPECIFIED_LENGTH {
        return false;
    }
    let length = usize::try_from(length).unwrap_or(usize::MAX);
    let col_charset = tp.charset_name();
    match v {
        Datum::Bytes(bytes) => {
            // Bytes cut by bytes regardless (Go reaches the byte arm for
            // KindBytes under the binary/ascii charsets, and a KindBytes
            // value under another charset still cuts through SetBytes).
            if col_charset == "binary" || col_charset == "ascii" {
                if bytes.len() > length {
                    bytes.truncate(length);
                    return true;
                }
                return false;
            }
            let text = String::from_utf8_lossy(bytes);
            if text.chars().count() > length {
                let cut: String = text.chars().take(length).collect();
                *bytes = cut.into_bytes();
                return true;
            }
            false
        }
        Datum::String(s) => {
            let collation = s.collation();
            let bytes = s.bytes().to_vec();
            if col_charset == "binary" || col_charset == "ascii" {
                if bytes.len() > length {
                    *v = Datum::String(tidb_datatype::StringDatum::new(
                        bytes[..length].to_vec(),
                        collation,
                    ));
                    return true;
                }
                return false;
            }
            let text = String::from_utf8_lossy(&bytes);
            if text.chars().count() > length {
                let cut: String = text.chars().take(length).collect();
                *v = Datum::String(tidb_datatype::StringDatum::new(
                    cut.into_bytes(),
                    collation,
                ));
                return true;
            }
            false
        }
        _ => false,
    }
}

/// Go `ReachPrefixLen` (`ranger.go:763`): whether the value's length is
/// EXACTLY the prefix length (same byte/rune split as the cut).
#[must_use]
pub fn reach_prefix_len(v: &Datum, length: i64, tp: &FieldType) -> bool {
    if length == super::checker::UNSPECIFIED_LENGTH {
        return false;
    }
    let length = usize::try_from(length).unwrap_or(usize::MAX);
    let bytes = match v {
        Datum::Bytes(bytes) => bytes.as_slice(),
        Datum::String(s) => s.bytes(),
        _ => return false,
    };
    let col_charset = tp.charset_name();
    if col_charset == "binary" || col_charset == "ascii" {
        return bytes.len() == length;
    }
    String::from_utf8_lossy(bytes).chars().count() == length
}

/// Go `cutPrefixForPoints` (`ranger.go:714`): cut every point to the
/// prefix length; a CUT point — or a START point already AT the length —
/// turns inclusive, else `col > 'xx'` over a length-2 prefix would scan
/// `(xx, +inf)` and miss `'xxx'`.
pub fn cut_prefix_for_points(points: &mut [Point], length: i64, tp: &FieldType) {
    if length == super::checker::UNSPECIFIED_LENGTH {
        return;
    }
    for p in points {
        let cut = cut_datum_by_prefix_len(&mut p.value, length, tp);
        if cut || (p.start && reach_prefix_len(&p.value, length, tp)) {
            p.excl = false;
        }
    }
}


/// Go `builder.mergeSorted`: one pass of merge-sort over two sorted point
/// lists.
fn merge_sorted(
    a: &[Point],
    b: &[Point],
    collator: Collation,
) -> Result<Vec<Point>, tidb_datatype::DatumValueError> {
    let mut ret = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let less = range_point_cmp(&a[i], &b[j], collator)?;
        if less == std::cmp::Ordering::Less {
            ret.push(a[i].clone());
            i += 1;
        } else {
            ret.push(b[j].clone());
            j += 1;
        }
    }
    ret.extend_from_slice(&a[i..]);
    ret.extend_from_slice(&b[j..]);
    Ok(ret)
}

/// Go `builder.merge`: sweep the merged points counting open intervals —
/// a union keeps stretches covered by AT LEAST one input, an intersection
/// stretches covered by BOTH.
fn merge_points(
    a: &[Point],
    b: &[Point],
    union: bool,
    collator: Collation,
) -> Result<Vec<Point>, tidb_datatype::DatumValueError> {
    let merged = merge_sorted(a, b, collator)?;
    let required_in_range_count = if union { 1 } else { 2 };
    let mut in_range_count = 0;
    let mut result = Vec::new();
    for val in merged {
        if val.start {
            in_range_count += 1;
            if in_range_count == required_in_range_count {
                result.push(val);
            }
        } else {
            if in_range_count == required_in_range_count {
                result.push(val);
            }
            in_range_count -= 1;
        }
    }
    Ok(result)
}

/// Go `builder.intersection`. The collator must be the SORT-KEY binary
/// collator when the points were converted to sort keys.
pub fn intersection(
    a: &[Point],
    b: &[Point],
    collator: Collation,
) -> Result<Vec<Point>, tidb_datatype::DatumValueError> {
    merge_points(a, b, false, collator)
}

/// Go `builder.union`.
pub fn union(
    a: &[Point],
    b: &[Point],
    collator: Collation,
) -> Result<Vec<Point>, tidb_datatype::DatumValueError> {
    merge_points(a, b, true, collator)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(value: Datum, excl: bool) -> Point {
        Point {
            value,
            excl,
            start: true,
        }
    }

    fn end(value: Datum, excl: bool) -> Point {
        Point {
            value,
            excl,
            start: false,
        }
    }

    /// The point display strings Go's `point.String` writes.
    #[test]
    fn points_display_like_go() {
        assert_eq!(start(Datum::Int(3), false).to_display_string(), "[3");
        assert_eq!(start(Datum::Int(3), true).to_display_string(), "(3");
        assert_eq!(end(Datum::Int(3), false).to_display_string(), "3]");
        assert_eq!(end(Datum::Int(3), true).to_display_string(), "3)");
        assert_eq!(start(Datum::MinNotNull, false).to_display_string(), "[-inf");
        assert_eq!(end(Datum::MaxValue, false).to_display_string(), "+inf]");
    }

    /// Go `rangePointEqualValueCmp`'s tie-table, case by case: the four
    /// start/end combinations with their exclusion outcomes.
    #[test]
    fn equal_value_points_order_by_their_flags() {
        use std::cmp::Ordering::{Equal, Less};
        let cmp = |a: &Point, b: &Point| range_point_cmp(a, b, Collation::Binary).expect("compares");
        let v = || Datum::Int(5);
        // Two starts: closed before open.
        assert_eq!(cmp(&start(v(), false), &start(v(), true)), Less);
        assert_eq!(cmp(&start(v(), true), &start(v(), false)), Equal);
        // Start vs end: a closed start is less than a closed end?? No —
        // Go: start && !end -> less when both closed.
        assert_eq!(cmp(&start(v(), false), &end(v(), false)), Less);
        assert_eq!(cmp(&start(v(), true), &end(v(), false)), Equal);
        // End vs start: less when either is open.
        assert_eq!(cmp(&end(v(), true), &start(v(), false)), Less);
        assert_eq!(cmp(&end(v(), false), &start(v(), true)), Less);
        assert_eq!(cmp(&end(v(), false), &start(v(), false)), Equal);
        // Two ends: open before closed.
        assert_eq!(cmp(&end(v(), true), &end(v(), false)), Less);
        assert_eq!(cmp(&end(v(), false), &end(v(), true)), Equal);
        // Different values order by value regardless of flags.
        assert_eq!(
            cmp(&end(Datum::Int(4), true), &start(Datum::Int(5), true)),
            Less
        );
    }

    /// Go `handleUnsignedCol`'s table: negative constants against an
    /// unsigned column clamp GT/GE/NE to `>= 0` and invalidate the rest.
    #[test]
    fn unsigned_columns_clamp_negative_constants() {
        let mut unsigned_ft = FieldType::new(FieldTypeCode::LongLong);
        unsigned_ft.set_flags(unsigned_ft.flags() | tidb_datatype::FieldTypeFlags::UNSIGNED);

        let (val, op, valid) = handle_unsigned_col(&unsigned_ft, Datum::Int(-1), OP_GT);
        assert!(valid);
        assert_eq!(op, OP_GE);
        assert!(matches!(val, Datum::UInt(0)));

        let (_, _, valid) = handle_unsigned_col(&unsigned_ft, Datum::Int(-1), OP_LT);
        assert!(!valid, "unsigned < negative has no range");
        let (_, _, valid) = handle_unsigned_col(&unsigned_ft, Datum::Int(-1), OP_EQ);
        assert!(!valid);

        // A signed column, or a non-negative value, passes through.
        let signed_ft = FieldType::new(FieldTypeCode::LongLong);
        let (val, op, valid) = handle_unsigned_col(&signed_ft, Datum::Int(-1), OP_LT);
        assert!(valid);
        assert_eq!(op, OP_LT);
        assert!(matches!(val, Datum::Int(-1)));
    }

    /// Go `handleBoundCol`'s table: a beyond-MaxInt64 constant against a
    /// signed int column, and the f32 domain clamp for FLOAT columns.
    #[test]
    fn bound_columns_clamp_out_of_domain_constants() {
        let signed_ft = FieldType::new(FieldTypeCode::LongLong);
        let big = Datum::UInt(u64::MAX);
        let (_, _, valid) = handle_bound_col(&signed_ft, big.clone(), OP_GT);
        assert!(!valid, "signed > MaxUint has no range");
        let (val, op, valid) = handle_bound_col(&signed_ft, big, OP_LT);
        assert!(valid);
        assert_eq!(op, OP_LE);
        assert!(matches!(val, Datum::Int(i64::MAX)));

        let float_ft = FieldType::new(FieldTypeCode::Float);
        let (val, op, valid) =
            handle_bound_col(&float_ft, Datum::Real(f64::from(f32::MAX) * 2.0), OP_NE);
        assert!(valid);
        assert_eq!(op, OP_LE);
        assert!(matches!(val, Datum::Float32(v) if v == f64::from(f32::MAX)));
        let (val, op, valid) =
            handle_bound_col(&float_ft, Datum::Real(f64::from(-f32::MAX) * 2.0), OP_GE);
        assert!(valid);
        assert_eq!(op, OP_GE);
        assert!(matches!(val, Datum::Float32(v) if v == f64::from(-f32::MAX)));
        let (_, _, valid) =
            handle_bound_col(&float_ft, Datum::Real(f64::from(-f32::MAX) * 2.0), OP_LT);
        assert!(!valid);
    }

    /// The full-range constructors' shapes.
    #[test]
    fn full_range_constructors_match_go() {
        assert_eq!(full_int_range(false)[0].to_display_string(), "[-inf,+inf]");
        assert_eq!(
            full_int_range(true)[0].to_display_string(),
            "[0,+inf]"
        );
        assert_eq!(full_range()[0].to_display_string(), "[NULL,+inf]");
        assert_eq!(full_not_null_range()[0].to_display_string(), "[-inf,+inf]");
        assert_eq!(null_range()[0].to_display_string(), "[NULL,NULL]");
        assert!(full_int_range(false)[0].is_full_range(false));
        assert!(full_int_range(true)[0].is_full_range(true));
        assert!(full_range()[0].is_full_range(false));

        let points = get_full_range();
        assert!(points[0].start && matches!(points[0].value, Datum::Null));
        assert!(matches!(points[1].value, Datum::MaxValue));
        let points = get_not_null_full_range();
        assert!(matches!(points[0].value, Datum::MinNotNull));
    }

    /// Go `cutPrefixForPoints`' worked example: `col > 'xx'` over a
    /// length-2 prefix becomes INCLUSIVE `[xx, +inf)`; longer values cut
    /// and turn inclusive; short values keep their exclusion.
    #[test]
    fn prefix_cutting_adjusts_exclusion_like_go() {
        let mut ft = FieldType::new(FieldTypeCode::Varchar);
        ft.set_flen(20);
        ft.set_charset_name("utf8mb4");
        ft.set_collation_name("utf8mb4_bin");
        let string_point = |text: &str, excl: bool, start_flag: bool| Point {
            value: Datum::String(tidb_datatype::StringDatum::new(
                text.as_bytes().to_vec(),
                Collation::Utf8Mb4Bin,
            )),
            excl,
            start: start_flag,
        };

        // `col > 'xx'`: the start point reaches the length and turns
        // inclusive.
        let mut points = vec![string_point("xx", true, true)];
        cut_prefix_for_points(&mut points, 2, &ft);
        assert!(!points[0].excl);

        // `col > 'xxx'`: the value cuts to 'xx' and turns inclusive.
        let mut points = vec![string_point("xxx", true, true)];
        cut_prefix_for_points(&mut points, 2, &ft);
        assert!(!points[0].excl);
        assert!(
            matches!(&points[0].value, Datum::String(s) if s.bytes() == b"xx"),
            "{:?}",
            points[0].value
        );

        // `col > 'x'`: below the length — untouched.
        let mut points = vec![string_point("x", true, true)];
        cut_prefix_for_points(&mut points, 2, &ft);
        assert!(points[0].excl);
        // An END point at the length keeps its exclusion (only starts
        // convert on the reach case).
        let mut points = vec![string_point("xx", true, false)];
        cut_prefix_for_points(&mut points, 2, &ft);
        assert!(points[0].excl);

        // Multi-byte characters cut by RUNES under utf8: '你好世界' at
        // length 2 cuts to '你好'.
        let mut points = vec![string_point("你好世界", false, true)];
        cut_prefix_for_points(&mut points, 2, &ft);
        assert!(
            matches!(&points[0].value, Datum::String(s) if s.bytes() == "你好".as_bytes())
        );

        // A binary charset cuts by BYTES.
        let mut bin_ft = FieldType::new(FieldTypeCode::Varchar);
        bin_ft.set_charset_name("binary");
        let mut points = vec![Point {
            value: Datum::Bytes("你好".as_bytes().to_vec()),
            excl: false,
            start: true,
        }];
        cut_prefix_for_points(&mut points, 2, &bin_ft);
        assert!(
            matches!(&points[0].value, Datum::Bytes(b) if b.len() == 2),
            "{:?}",
            points[0].value
        );
        // Non-string datums never cut.
        let mut points = vec![Point {
            value: Datum::Int(5),
            excl: true,
            start: true,
        }];
        cut_prefix_for_points(&mut points, 2, &ft);
        assert!(points[0].excl);
    }

    /// Go `merge`'s sweep: `(a < 2 OR a > 5)` unioned/intersected with
    /// `(a < 4)`, as point lists.
    #[test]
    fn point_merge_union_and_intersection() {
        // (-inf, 2) OR (5, +inf]
        let disjoint = vec![
            start(Datum::MinNotNull, false),
            end(Datum::Int(2), true),
            start(Datum::Int(5), true),
            end(Datum::MaxValue, false),
        ];
        // [-inf, 4)
        let below_four = vec![start(Datum::MinNotNull, false), end(Datum::Int(4), true)];
        let show = |points: &[Point]| -> Vec<String> {
            points.iter().map(Point::to_display_string).collect()
        };
        let met = intersection(&disjoint, &below_four, Collation::Binary).expect("compares");
        assert_eq!(show(&met), ["[-inf", "2)"]);
        let joined = union(&disjoint, &below_four, Collation::Binary).expect("compares");
        assert_eq!(show(&joined), ["[-inf", "4)", "(5", "+inf]"]);
    }
}
