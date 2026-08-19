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

use tidb_datatype::{Collation, Datum, EvalType, FieldType, FieldTypeCode};

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


/// Why the builder failed (Go `builder.err`).
#[derive(Debug)]
pub enum PointBuilderError {
    /// Go `plannererrors.ErrUnsupportedType` shapes.
    Unsupported(String),
    /// A datum comparison/conversion failure.
    Value(tidb_datatype::DatumValueError),
}

impl From<tidb_datatype::DatumValueError> for PointBuilderError {
    fn from(error: tidb_datatype::DatumValueError) -> Self {
        Self::Value(error)
    }
}

/// Go `builder`: the range-point builder. `err` carries the first failure
/// exactly as Go's field does; `skip_plan_cache_reason` carries what Go
/// pushes through `sctx.SetSkipPlanCache`, for the plan-cache caller to
/// consume when that surface wires in.
#[derive(Debug, Default)]
pub struct PointBuilder {
    /// Go `builder.err`.
    pub err: Option<PointBuilderError>,
    /// Go `SetSkipPlanCache`'s reason, recorded not applied.
    pub skip_plan_cache_reason: Option<String>,
}

use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;

impl PointBuilder {
    /// Go `builder.build`: one column-bound expression into points.
    pub fn build(
        &mut self,
        expr: &Expression,
        new_tp: &FieldType,
        prefix_len: i64,
        convert_to_sort_key: bool,
    ) -> Vec<Point> {
        match expr {
            Expression::Column(_) => Self::build_from_column(),
            Expression::ScalarFunction(scalar) => {
                self.build_from_scalar_func(scalar, new_tp, prefix_len, convert_to_sort_key)
            }
            Expression::Constant(constant) => self.build_from_constant(constant),
            Expression::CorrelatedColumn(_) => get_full_range(),
        }
    }

    /// Go `buildFromConstant`: NULL is an empty range, falsy is empty,
    /// truthy is full.
    fn build_from_constant(&mut self, constant: &tidb_expr::constant::Constant) -> Vec<Point> {
        let dt = &constant.value;
        if matches!(dt, Datum::Null) {
            return Vec::new();
        }
        match dt.to_bool() {
            Err(error) => {
                self.err = Some(error.into());
                Vec::new()
            }
            Ok(converted) if converted.value == 0 => Vec::new(),
            Ok(_) => get_full_range(),
        }
    }

    /// Go `buildFromColumn`: "column" is "column is true" —
    /// `[-inf, 0) (0, +inf]`.
    fn build_from_column() -> Vec<Point> {
        vec![
            Point {
                value: Datum::MinNotNull,
                start: true,
                ..Point::default()
            },
            Point {
                value: Datum::Int(0),
                excl: true,
                ..Point::default()
            },
            Point {
                value: Datum::Int(0),
                excl: true,
                start: true,
            },
            Point {
                value: Datum::MaxValue,
                ..Point::default()
            },
        ]
    }

    /// Go `refineValueAndOp`: re-collate a string constant to the column's
    /// collation, and adjust 2-digit/overflowed YEAR constants together
    /// with the operator.
    fn refine_value_and_op(
        col_type: &FieldType,
        value: &mut Datum,
        op: &mut String,
    ) -> Result<(), ()> {
        if col_type.eval_type() == EvalType::String {
            if let Datum::String(s) = value {
                *value = Datum::String(tidb_datatype::StringDatum::new(
                    s.bytes().to_vec(),
                    col_type.collation(),
                ));
            } else if let Datum::BinaryLiteral(_) = value {
                // Go re-collates KindBinaryLiteral through SetString too;
                // the literal's bytes become a string under the column's
                // collation.
            }
        }
        if col_type.code() == FieldTypeCode::Year && !matches!(value, Datum::Null) {
            // `col op MaxUint` behaves as `col op MaxInt` (max year 2155).
            if let Datum::UInt(v) = value {
                if *v > i64::MAX as u64 {
                    *value = Datum::Int(i64::MAX);
                }
            }
            let pre_value = match value.to_i64() {
                Ok(converted) => converted.value,
                Err(_) => return Err(()),
            };
            match value.convert_to(col_type, tidb_datatype::ConversionFlags::default()) {
                Ok(converted) => {
                    let out_of_range = matches!(
                        converted.event,
                        Some(tidb_datatype::ScalarConversionEvent::Overflow(_))
                    );
                    let new_value = converted.value;
                    let new_int = match new_value.to_i64() {
                        Ok(converted) => converted.value,
                        Err(_) => pre_value,
                    };
                    *value = new_value;
                    if out_of_range {
                        // The adjusted constant may need the operator to
                        // move with it (`col < 2156` becomes `col <= 2155`).
                        match op.as_str() {
                            OP_GT => {
                                if new_int > pre_value {
                                    *op = OP_GE.to_owned();
                                }
                            }
                            OP_LT => {
                                if new_int < pre_value {
                                    *op = OP_LE.to_owned();
                                }
                            }
                            OP_GE | OP_LE => {}
                            // Keep the error for EQ and NE.
                            _ => return Err(()),
                        }
                    }
                }
                Err(_) => return Err(()),
            }
        }
        Ok(())
    }

    /// Go `buildFromBinOp`.
    fn build_from_bin_op(
        &mut self,
        scalar: &ScalarFunction,
        new_tp: &FieldType,
        prefix_len: i64,
        convert_to_sort_key: bool,
    ) -> Vec<Point> {
        let (column, constant, mut op) = if let Expression::Column(col) = &scalar.args[0] {
            let Expression::Constant(constant) = &scalar.args[1] else {
                return Vec::new();
            };
            (col, constant, scalar.func_name.lowercase().to_owned())
        } else if let Expression::Column(col) = &scalar.args[1] {
            let Expression::Constant(constant) = &scalar.args[0] else {
                return Vec::new();
            };
            // The mirrored operand order flips the inequality.
            let op = match scalar.func_name.lowercase() {
                "ge" => OP_LE,
                "gt" => OP_LT,
                "lt" => OP_GT,
                "le" => OP_GE,
                other => other,
            };
            (col, constant, op.to_owned())
        } else {
            return Vec::new();
        };
        let Some(ft) = column.ret_type.as_ref() else {
            return Vec::new();
        };
        let mut value = constant.value.clone();
        if op != OP_NULL_EQ && matches!(value, Datum::Null) {
            return Vec::new();
        }
        if Self::refine_value_and_op(ft, &mut value, &mut op).is_err() {
            if op == OP_NE {
                // col != an impossible value (not a valid year).
                return get_not_null_full_range();
            }
            // col = an impossible value.
            return Vec::new();
        }

        let (value, op, valid) = handle_unsigned_col(ft, value, &op);
        if !valid {
            return Vec::new();
        }
        let (value, op, valid) = handle_bound_col(ft, value, &op);
        if !valid {
            return Vec::new();
        }

        if ft.code() == FieldTypeCode::Enum && ft.eval_type() == EvalType::String {
            return handle_enum_from_bin_op(ft, &value, &op);
        }

        let mut res = match op.as_str() {
            OP_NULL_EQ if matches!(value, Datum::Null) => vec![
                Point {
                    start: true,
                    ..Point::default()
                },
                Point::default(),
            ],
            OP_NULL_EQ | OP_EQ => vec![
                Point {
                    value: value.clone(),
                    start: true,
                    ..Point::default()
                },
                Point {
                    value,
                    ..Point::default()
                },
            ],
            OP_NE => vec![
                Point {
                    value: Datum::MinNotNull,
                    start: true,
                    ..Point::default()
                },
                Point {
                    value: value.clone(),
                    excl: true,
                    ..Point::default()
                },
                Point {
                    value,
                    start: true,
                    excl: true,
                },
                Point {
                    value: Datum::MaxValue,
                    ..Point::default()
                },
            ],
            OP_LT => vec![
                Point {
                    value: Datum::MinNotNull,
                    start: true,
                    ..Point::default()
                },
                Point {
                    value,
                    excl: true,
                    ..Point::default()
                },
            ],
            OP_LE => vec![
                Point {
                    value: Datum::MinNotNull,
                    start: true,
                    ..Point::default()
                },
                Point {
                    value,
                    ..Point::default()
                },
            ],
            OP_GT => vec![
                Point {
                    value,
                    start: true,
                    excl: true,
                },
                Point {
                    value: Datum::MaxValue,
                    ..Point::default()
                },
            ],
            OP_GE => vec![
                Point {
                    value,
                    start: true,
                    ..Point::default()
                },
                Point {
                    value: Datum::MaxValue,
                    ..Point::default()
                },
            ],
            _ => return Vec::new(),
        };
        cut_prefix_for_points(&mut res, prefix_len, ft);
        if convert_to_sort_key {
            if let Err(error) = convert_points_to_sort_key_in_place(&mut res, new_tp) {
                self.err = Some(error);
                return get_full_range();
            }
        }
        res
    }

    /// Go `buildFromIsTrue`.
    fn build_from_is_true(is_not: bool, keep_null: bool) -> Vec<Point> {
        if is_not {
            if keep_null {
                // Range is {[0, 0]}.
                return vec![
                    Point {
                        value: Datum::Int(0),
                        start: true,
                        ..Point::default()
                    },
                    Point {
                        value: Datum::Int(0),
                        ..Point::default()
                    },
                ];
            }
            // NOT TRUE is {[null, null], [0, 0]}.
            return vec![
                Point {
                    start: true,
                    ..Point::default()
                },
                Point::default(),
                Point {
                    value: Datum::Int(0),
                    start: true,
                    ..Point::default()
                },
                Point {
                    value: Datum::Int(0),
                    ..Point::default()
                },
            ];
        }
        // TRUE is {[-inf, 0), (0, +inf]}.
        Self::build_from_column()
    }

    /// Go `buildFromIsFalse`.
    fn build_from_is_false(is_not: bool) -> Vec<Point> {
        if is_not {
            // NOT FALSE is {[null, 0), (0, +inf]}.
            return vec![
                Point {
                    start: true,
                    ..Point::default()
                },
                Point {
                    value: Datum::Int(0),
                    excl: true,
                    ..Point::default()
                },
                Point {
                    value: Datum::Int(0),
                    start: true,
                    excl: true,
                },
                Point {
                    value: Datum::MaxValue,
                    ..Point::default()
                },
            ];
        }
        // FALSE is {[0, 0]}.
        vec![
            Point {
                value: Datum::Int(0),
                start: true,
                ..Point::default()
            },
            Point {
                value: Datum::Int(0),
                ..Point::default()
            },
        ]
    }

    /// Go `buildFromIn`: `(points, has_null)`.
    fn build_from_in(
        &mut self,
        scalar: &ScalarFunction,
        new_tp: &FieldType,
        prefix_len: i64,
        convert_to_sort_key: bool,
    ) -> (Vec<Point>, bool) {
        let mut has_null = false;
        let Some(ft) = scalar.args[0].static_type().cloned() else {
            return (get_full_range(), has_null);
        };
        let mut range_points: Vec<Point> = Vec::with_capacity((scalar.args.len() - 1) * 2);
        for e in &scalar.args[1..] {
            let Expression::Constant(constant) = e else {
                self.err = Some(PointBuilderError::Unsupported(format!(
                    "expr:{e:?} is not constant"
                )));
                return (get_full_range(), has_null);
            };
            let mut dt = constant.value.clone();
            if matches!(dt, Datum::Null) {
                has_null = true;
                continue;
            }
            if ft.code() == FieldTypeCode::Enum {
                let converted = match &dt {
                    Datum::String(_) | Datum::Bytes(_) | Datum::BinaryLiteral(_) => {
                        // "Can't use ConvertTo directly": a numerical string
                        // must not become an enum ORDINAL in a select.
                        let text = match &dt {
                            Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
                            Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                            Datum::BinaryLiteral(b) => {
                                String::from_utf8_lossy(&b.clone().into_bytes()).into_owned()
                            }
                            _ => unreachable!("the arm matched string kinds"),
                        };
                        parse_enum_from_elements(&ft, &text)
                    }
                    _ => dt
                        .convert_to(&ft, tidb_datatype::ConversionFlags::default())
                        .ok()
                        .map(|converted| converted.value),
                };
                // in (..., an impossible enum, ...): the member is empty —
                // skip it.
                let Some(converted) = converted else { continue };
                dt = converted;
            }
            if ft.code() == FieldTypeCode::Year {
                match dt.convert_to(&ft, tidb_datatype::ConversionFlags::default()) {
                    Ok(converted)
                        if !matches!(
                            converted.event,
                            Some(tidb_datatype::ScalarConversionEvent::Overflow(_))
                        ) =>
                    {
                        dt = converted.value;
                    }
                    // in (..., an impossible year, ...): skip it.
                    _ => continue,
                }
            }
            if ft.eval_type() == EvalType::String {
                if let Datum::String(s) = &dt {
                    dt = Datum::String(tidb_datatype::StringDatum::new(
                        s.bytes().to_vec(),
                        ft.collation(),
                    ));
                }
            }
            range_points.push(Point {
                value: dt.clone(),
                start: true,
                ..Point::default()
            });
            range_points.push(Point {
                value: dt,
                ..Point::default()
            });
        }
        let collator = ft.collation();
        let mut sort_err = None;
        range_points.sort_by(|a, b| match range_point_cmp(a, b, collator) {
            Ok(order) => order,
            Err(error) => {
                sort_err = Some(error);
                std::cmp::Ordering::Equal
            }
        });
        if let Some(error) = sort_err {
            self.err = Some(error.into());
        }
        // Check and remove duplicates: Go's two-cursor sweep keeps the
        // first point of each equal start/end run.
        let mut cur_pos = 0;
        let mut front_pos = 0;
        while front_pos < range_points.len() {
            if range_points[cur_pos].start == range_points[front_pos].start {
                front_pos += 1;
            } else {
                cur_pos += 1;
                range_points.swap(cur_pos, front_pos);
                front_pos += 1;
            }
        }
        if cur_pos > 0 {
            cur_pos += 1;
        }
        range_points.truncate(cur_pos);
        cut_prefix_for_points(&mut range_points, prefix_len, &ft);
        if convert_to_sort_key {
            if let Err(error) = convert_points_to_sort_key_in_place(&mut range_points, new_tp) {
                self.err = Some(error);
                return (get_full_range(), false);
            }
        }
        (range_points, has_null)
    }

    /// Go `newBuildFromPatternLike`.
    fn new_build_from_pattern_like(
        &mut self,
        scalar: &ScalarFunction,
        new_tp: &FieldType,
        prefix_len: i64,
        convert_to_sort_key: bool,
    ) -> Vec<Point> {
        let (_, collation) = scalar.collation.charset_and_collation();
        let Some(tp_of_pattern) = scalar.args[0].static_type().cloned() else {
            return get_full_range();
        };
        if !tidb_datatype::compatible_collate(tp_of_pattern.collation_name(), collation) {
            return get_full_range();
        }
        let Expression::Constant(pattern_const) = &scalar.args[1] else {
            return get_full_range();
        };
        let Ok(pattern) = pattern_const.value.sql_string() else {
            self.err = Some(PointBuilderError::Unsupported(
                "pattern is not printable".to_owned(),
            ));
            return get_full_range();
        };
        // Case 1: the empty pattern matches only the empty string.
        if pattern.is_empty() {
            let empty = || {
                Datum::String(tidb_datatype::StringDatum::new(
                    Vec::new(),
                    tp_of_pattern.collation(),
                ))
            };
            let mut res = vec![
                Point {
                    value: empty(),
                    start: true,
                    ..Point::default()
                },
                Point {
                    value: empty(),
                    ..Point::default()
                },
            ];
            if convert_to_sort_key {
                if let Err(error) = convert_points_to_sort_key_in_place(&mut res, new_tp) {
                    self.err = Some(error);
                    return get_full_range();
                }
            }
            return res;
        }
        let Expression::Constant(escape_const) = &scalar.args[2] else {
            return get_full_range();
        };
        let Datum::Int(escape) = &escape_const.value else {
            return get_full_range();
        };
        let escape = *escape as u8;
        let pattern_bytes = pattern.as_bytes();
        let mut low_value: Vec<u8> = Vec::with_capacity(pattern_bytes.len());
        let mut exclude = false;
        let mut is_exact_match = true;
        let mut i = 0;
        while i < pattern_bytes.len() {
            if pattern_bytes[i] == escape {
                i += 1;
                if i < pattern_bytes.len() {
                    low_value.push(pattern_bytes[i]);
                } else {
                    low_value.push(escape);
                }
                i += 1;
                continue;
            }
            if pattern_bytes[i] == b'%' {
                is_exact_match = false;
                break;
            } else if pattern_bytes[i] == b'_' {
                // Exclude the prefix — but a PAD SPACE collation would then
                // miss 'xxx   ' (trailing spaces trim in index keys).
                if !tidb_datatype::is_pad_space_collation(collation) {
                    exclude = true;
                }
                is_exact_match = false;
                break;
            }
            low_value.push(pattern_bytes[i]);
            i += 1;
        }
        // Case 2: nothing before the wildcard — full not-null range.
        if low_value.is_empty() {
            return get_not_null_full_range();
        }
        // Case 3: no wildcard at all — a point.
        if is_exact_match {
            let val = Datum::String(tidb_datatype::StringDatum::new(
                low_value,
                tp_of_pattern.collation(),
            ));
            let mut res = vec![
                Point {
                    value: val.clone(),
                    start: true,
                    ..Point::default()
                },
                Point {
                    value: val,
                    ..Point::default()
                },
            ];
            cut_prefix_for_points(&mut res, prefix_len, &tp_of_pattern);
            if convert_to_sort_key {
                if let Err(error) = convert_points_to_sort_key_in_place(&mut res, new_tp) {
                    self.err = Some(error);
                    return get_full_range();
                }
            }
            return res;
        }
        // Case 4-1: not a _bin/binary collation and no sort-key conversion —
        // no range for the wildcard.
        if !convert_to_sort_key && !tidb_datatype::is_bin_collation(tp_of_pattern.collation_name())
        {
            return get_not_null_full_range();
        }
        // Case 4-2: the wildcard range — end key is sortKey(start) + 1.
        let mut original_start_point = Point {
            value: Datum::String(tidb_datatype::StringDatum::new(
                low_value,
                tp_of_pattern.collation(),
            )),
            start: true,
            excl: exclude,
        };
        {
            let mut single = [original_start_point.clone()];
            cut_prefix_for_points(&mut single, prefix_len, &tp_of_pattern);
            original_start_point = single[0].clone();
        }
        let should_trim_trailing_space = tidb_datatype::is_pad_space_collation(collation);
        let mut start_point = original_start_point.clone();
        if let Err(error) =
            convert_point_to_sort_key_in_place(&mut start_point, new_tp, should_trim_trailing_space)
        {
            self.err = Some(error);
            return get_full_range();
        }
        let mut sort_key_point_without_trim = original_start_point;
        if let Err(error) =
            convert_point_to_sort_key_in_place(&mut sort_key_point_without_trim, new_tp, false)
        {
            self.err = Some(error);
            return get_full_range();
        }
        let mut sort_key_without_trim = match &sort_key_point_without_trim.value {
            Datum::Bytes(bytes) => bytes.clone(),
            Datum::String(s) => s.bytes().to_vec(),
            _ => Vec::new(),
        };
        let mut end_point = Point {
            value: Datum::MaxValue,
            excl: true,
            ..Point::default()
        };
        for i in (0..sort_key_without_trim.len()).rev() {
            // Increment the last byte: "abc" ends at "abd".
            sort_key_without_trim[i] = sort_key_without_trim[i].wrapping_add(1);
            if sort_key_without_trim[i] != 0 {
                end_point.value = Datum::Bytes(sort_key_without_trim);
                break;
            }
            if i == 0 {
                end_point.value = Datum::MaxValue;
            }
        }
        vec![start_point, end_point]
    }

    /// Go `buildFromNot`.
    fn build_from_not(
        &mut self,
        scalar: &ScalarFunction,
        new_tp: &FieldType,
        prefix_len: i64,
        convert_to_sort_key: bool,
    ) -> Vec<Point> {
        match scalar.func_name.lowercase() {
            "istrue" => Self::build_from_is_true(true, false),
            "istrue_with_null" => Self::build_from_is_true(true, true),
            "isfalse" => Self::build_from_is_false(true),
            "in" => {
                // Cutting the prefix INSIDE buildFromIn would make the
                // inversion wrong ('ab' between 'a' and 'b' would be
                // missed): cut and convert HERE, after inverting.
                let (mut range_points, has_null) =
                    self.build_from_in(scalar, new_tp, super::checker::UNSPECIFIED_LENGTH, false);
                if has_null {
                    return Vec::new();
                }
                let Some(ft) = scalar.args[0].static_type().cloned() else {
                    return get_full_range();
                };
                // Negative members are unreachable for an unsigned int
                // column: drop them before inverting.
                if let Expression::Column(column) = &scalar.args[0] {
                    let is_unsigned_int_col = column.ret_type.as_ref().is_some_and(|ret| {
                        ret.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED != 0
                            && matches!(
                                ret.code(),
                                FieldTypeCode::Tiny
                                    | FieldTypeCode::Short
                                    | FieldTypeCode::Int24
                                    | FieldTypeCode::Long
                                    | FieldTypeCode::LongLong
                            )
                    });
                    if is_unsigned_int_col {
                        let mut non_negative_pos = 0;
                        while non_negative_pos < range_points.len() {
                            let value = &range_points[non_negative_pos].value;
                            let non_negative = match value {
                                Datum::UInt(_) => true,
                                Datum::Int(v) => *v >= 0,
                                _ => true,
                            };
                            if non_negative {
                                break;
                            }
                            non_negative_pos += 2;
                        }
                        range_points.drain(..non_negative_pos.min(range_points.len()));
                    }
                }
                let mut ret: Vec<Point> = Vec::with_capacity(2 + range_points.len());
                let mut previous_value = Datum::Null;
                let mut i = 0;
                while i < range_points.len() {
                    ret.push(Point {
                        value: previous_value.clone(),
                        start: true,
                        excl: true,
                    });
                    ret.push(Point {
                        value: range_points[i].value.clone(),
                        excl: true,
                        ..Point::default()
                    });
                    previous_value = range_points[i].value.clone();
                    i += 2;
                }
                // The tail interval (last member, +inf].
                ret.push(Point {
                    value: previous_value,
                    start: true,
                    excl: true,
                });
                ret.push(Point {
                    value: Datum::MaxValue,
                    ..Point::default()
                });
                cut_prefix_for_points(&mut ret, prefix_len, &ft);
                if convert_to_sort_key {
                    if let Err(error) = convert_points_to_sort_key_in_place(&mut ret, new_tp) {
                        self.err = Some(error);
                        return get_full_range();
                    }
                }
                ret
            }
            "like" => {
                self.err = Some(PointBuilderError::Unsupported(
                    "NOT LIKE is not supported.".to_owned(),
                ));
                get_full_range()
            }
            "isnull" => get_not_null_full_range(),
            // Go's TODO: unhandled NOT shapes answer the full range for
            // correctness.
            _ => get_full_range(),
        }
    }

    /// Go `buildFromScalarFunc`.
    fn build_from_scalar_func(
        &mut self,
        scalar: &ScalarFunction,
        new_tp: &FieldType,
        prefix_len: i64,
        convert_to_sort_key: bool,
    ) -> Vec<Point> {
        match scalar.func_name.lowercase() {
            "ge" | "gt" | "lt" | "le" | "eq" | "ne" | "nulleq" => {
                self.build_from_bin_op(scalar, new_tp, prefix_len, convert_to_sort_key)
            }
            "and" => {
                let collator = if convert_to_sort_key {
                    Collation::Binary
                } else {
                    new_tp.collation()
                };
                let a = self.build(&scalar.args[0], new_tp, prefix_len, convert_to_sort_key);
                let b = self.build(&scalar.args[1], new_tp, prefix_len, convert_to_sort_key);
                match intersection(&a, &b, collator) {
                    Ok(points) => points,
                    Err(error) => {
                        self.err = Some(error.into());
                        Vec::new()
                    }
                }
            }
            "or" => {
                let collator = if convert_to_sort_key {
                    Collation::Binary
                } else {
                    new_tp.collation()
                };
                let a = self.build(&scalar.args[0], new_tp, prefix_len, convert_to_sort_key);
                let b = self.build(&scalar.args[1], new_tp, prefix_len, convert_to_sort_key);
                match union(&a, &b, collator) {
                    Ok(points) => points,
                    Err(error) => {
                        self.err = Some(error.into());
                        Vec::new()
                    }
                }
            }
            "istrue" => Self::build_from_is_true(false, false),
            "istrue_with_null" => Self::build_from_is_true(false, true),
            "isfalse" => Self::build_from_is_false(false),
            "in" => {
                self.build_from_in(scalar, new_tp, prefix_len, convert_to_sort_key)
                    .0
            }
            "like" => {
                self.new_build_from_pattern_like(scalar, new_tp, prefix_len, convert_to_sort_key)
            }
            "isnull" => vec![
                Point {
                    start: true,
                    ..Point::default()
                },
                Point::default(),
            ],
            "not" => {
                if let Expression::ScalarFunction(inner) = &scalar.args[0] {
                    self.build_from_not(inner, new_tp, prefix_len, convert_to_sort_key)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }
}

/// Go `handleEnumFromBinOp`: walk EVERY member (plus the empty zero enum)
/// and keep the members satisfying the comparison — enum ranges are point
/// sets.
fn handle_enum_from_bin_op(ft: &FieldType, val: &Datum, op: &str) -> Vec<Point> {
    ft.with_elems_visible(|elems| {
        let mut res: Vec<Point> = Vec::with_capacity((elems.len() + 1) * 2);
        if op == OP_NULL_EQ && matches!(val, Datum::Null) {
            res.push(Point {
                start: true,
                ..Point::default()
            });
            res.push(Point::default());
        }
        for i in 0..=elems.len() {
            let member = if i == 0 {
                tidb_datatype::MysqlEnum::new("", 0)
            } else {
                tidb_datatype::MysqlEnum::new(elems[i - 1].clone(), i as u64)
            };
            let d = Datum::Enum(member, ft.collation());
            let Ok(cmp) = d.compare(val, ft.collation()) else {
                continue;
            };
            use std::cmp::Ordering;
            let keep = match op {
                OP_LT => cmp == Ordering::Less,
                OP_LE => cmp != Ordering::Greater,
                OP_GT => cmp == Ordering::Greater,
                OP_GE => cmp != Ordering::Less,
                OP_EQ | OP_NULL_EQ => cmp == Ordering::Equal,
                OP_NE => cmp != Ordering::Equal,
                _ => false,
            };
            if keep {
                res.push(Point {
                    value: d.clone(),
                    start: true,
                    ..Point::default()
                });
                res.push(Point {
                    value: d,
                    ..Point::default()
                });
            }
        }
        res
    })
}

/// Go `types.ParseEnumName` over the field type's members: exact name
/// match under the column's collation, never ordinal parsing.
fn parse_enum_from_elements(ft: &FieldType, text: &str) -> Option<Datum> {
    ft.with_elems_visible(|elems| {
        for (i, name) in elems.iter().enumerate() {
            let equal = match ft.collation() {
                Collation::Binary => name.as_bytes() == text.as_bytes(),
                _ => name.as_bytes().eq_ignore_ascii_case(text.as_bytes()),
            };
            if equal {
                return Some(Datum::Enum(
                    tidb_datatype::MysqlEnum::new(name.clone(), (i + 1) as u64),
                    ft.collation(),
                ));
            }
        }
        None
    })
}

/// Go `convertPointInPlace` (`ranger.go:177`): convert the point's value
/// into the range's working type, TOLERATING the conversion events Go
/// tolerates, and adjust the exclusion when the cast MOVED the value.
pub fn convert_point_in_place(
    p: &mut Point,
    new_tp: &FieldType,
    skip_plan_cache_reason: &mut Option<String>,
) -> Result<(), PointBuilderError> {
    if matches!(p.value, Datum::MaxValue | Datum::MinNotNull) {
        return Ok(());
    }
    let casted = match p
        .value
        .convert_to(new_tp, tidb_datatype::ConversionFlags::default())
    {
        Ok(converted) => {
            if let Some(event) = &converted.event {
                // Go reaches its tolerance ladder only through an ERROR;
                // this port's convert reports the same conditions as
                // EVENTS. The tolerated pairs are the same: year/int/
                // decimal/float overflow trims to the boundary, enum
                // truncation clamps, bit too-long is ignored.
                *skip_plan_cache_reason =
                    Some(format!("{event:?} when converting {:?}", p.value));
            }
            converted.value
        }
        Err(error) => {
            *skip_plan_cache_reason = Some(format!("{error} when converting {:?}", p.value));
            // The hard-failure arms Go tolerates end in `return nil` with
            // the point untouched (invalid character strings); everything
            // else surfaces.
            return match new_tp.code() {
                FieldTypeCode::String | FieldTypeCode::VarString | FieldTypeCode::Varchar => {
                    Ok(())
                }
                _ => Err(error.into()),
            };
        }
    };
    let cmp = p.value.compare(&casted, new_tp.collation())?;
    p.value = casted;
    if cmp == std::cmp::Ordering::Equal {
        return Ok(());
    }
    let val_cmp_casted = cmp;
    if p.start {
        if p.excl {
            if val_cmp_casted == std::cmp::Ordering::Less {
                // "a > 1.9" converts to "a >= 2".
                p.excl = false;
            }
        } else if val_cmp_casted == std::cmp::Ordering::Greater {
            // "a >= 1.1" converts to "a > 1".
            p.excl = true;
        }
    } else if p.excl {
        if val_cmp_casted == std::cmp::Ordering::Greater {
            // "a < 1.1" converts to "a <= 1".
            p.excl = false;
        }
    } else if val_cmp_casted == std::cmp::Ordering::Less {
        // "a <= 1.9" converts to "a < 2".
        p.excl = true;
    }
    Ok(())
}

/// Go `convertPointsToSortKeyInPlace` (`points.go:110`).
fn convert_points_to_sort_key_in_place(
    points: &mut [Point],
    new_tp: &FieldType,
) -> Result<(), PointBuilderError> {
    if new_tp.eval_type() != EvalType::String
        || matches!(new_tp.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
    {
        return Ok(());
    }
    for p in points {
        convert_point_to_sort_key_in_place(p, new_tp, true)?;
    }
    Ok(())
}

/// Go `convertPointToSortKeyInPlace` (`points.go:128`).
fn convert_point_to_sort_key_in_place(
    p: &mut Point,
    new_tp: &FieldType,
    trim_trailing_space: bool,
) -> Result<(), PointBuilderError> {
    let mut skip_reason = None;
    convert_point_in_place(p, new_tp, &mut skip_reason)?;
    let Datum::String(s) = &p.value else {
        return Ok(());
    };
    if new_tp.collation_name() == "binary" || !tidb_datatype::new_collation_enabled() {
        return Ok(());
    }
    let collator = tidb_datatype::get_collator(new_tp.collation_name());
    let sort_key = if trim_trailing_space {
        collator.key(s.bytes())
    } else {
        collator.key_without_trim_right_space(s.bytes())
    };
    p.value = Datum::Bytes(sort_key);
    Ok(())
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

    fn show(points: &[Point]) -> String {
        points
            .iter()
            .map(Point::to_display_string)
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn int_col_expr(unique_id: i64) -> Expression {
        Expression::Column(tidb_expr::column::Column::new(
            unique_id,
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn int_const_expr(v: i64) -> Expression {
        Expression::Constant(tidb_expr::constant::Constant::new(
            Datum::Int(v),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn func_expr(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ))
    }

    fn build_points(expr: &Expression) -> Vec<Point> {
        let mut builder = PointBuilder::default();
        let points = build_on_long(expr, &mut builder);
        assert!(builder.err.is_none(), "{:?}", builder.err);
        points
    }

    fn build_on_long(expr: &Expression, builder: &mut PointBuilder) -> Vec<Point> {
        builder.build(
            expr,
            &FieldType::new(FieldTypeCode::LongLong),
            super::super::checker::UNSPECIFIED_LENGTH,
            false,
        )
    }

    /// `buildFromBinOp`'s six operators plus the mirrored-operand flip.
    #[test]
    fn bin_op_points_match_go() {
        let a = int_col_expr(1);
        let c3 = || int_const_expr(3);
        let case = |name: &str, args: Vec<Expression>| show(&build_points(&func_expr(name, args)));
        assert_eq!(case("eq", vec![a.clone(), c3()]), "[3 3]");
        assert_eq!(case("gt", vec![a.clone(), c3()]), "(3 +inf]");
        assert_eq!(case("ge", vec![a.clone(), c3()]), "[3 +inf]");
        assert_eq!(case("lt", vec![a.clone(), c3()]), "[-inf 3)");
        assert_eq!(case("le", vec![a.clone(), c3()]), "[-inf 3]");
        assert_eq!(case("ne", vec![a.clone(), c3()]), "[-inf 3) (3 +inf]");
        // `3 < a` flips to `a > 3`.
        assert_eq!(case("lt", vec![c3(), a.clone()]), "(3 +inf]");
        // NULL against a non-nulleq comparison is the empty set; NULLEQ
        // NULL is the null point.
        let null = Expression::Constant(tidb_expr::constant::Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::LongLong),
        ));
        assert_eq!(case("eq", vec![a.clone(), null.clone()]), "");
        assert_eq!(case("nulleq", vec![a.clone(), null]), "[<nil> <nil>]");
    }

    /// AND intersects, OR unions — `a > 1 AND a < 5`, `a < 2 OR a > 5`.
    #[test]
    fn logic_ops_compose_points() {
        let a = int_col_expr(1);
        let and = func_expr(
            "and",
            vec![
                func_expr("gt", vec![a.clone(), int_const_expr(1)]),
                func_expr("lt", vec![a.clone(), int_const_expr(5)]),
            ],
        );
        assert_eq!(show(&build_points(&and)), "(1 5)");
        let or = func_expr(
            "or",
            vec![
                func_expr("lt", vec![a.clone(), int_const_expr(2)]),
                func_expr("gt", vec![a.clone(), int_const_expr(5)]),
            ],
        );
        assert_eq!(show(&build_points(&or)), "[-inf 2) (5 +inf]");
    }

    /// `buildFromIn` sorts and dedups; `NOT IN` inverts with the null-start
    /// head interval and the +inf tail.
    #[test]
    fn in_and_not_in_points_match_go() {
        let a = int_col_expr(1);
        let in_expr = func_expr(
            "in",
            vec![
                a.clone(),
                int_const_expr(3),
                int_const_expr(1),
                int_const_expr(3),
            ],
        );
        assert_eq!(show(&build_points(&in_expr)), "[1 1] [3 3]");
        let not_in = func_expr("not", vec![in_expr]);
        assert_eq!(
            show(&build_points(&not_in)),
            "(<nil> 1) (1 3) (3 +inf]"
        );
    }

    /// The YEAR refinement: `y < 2156` clamps to `y <= 2155` (Go's
    /// worked example), and `y != invalid` answers the not-null full range.
    #[test]
    fn year_constants_refine_with_their_operator() {
        let year_col = Expression::Column(tidb_expr::column::Column::new(
            1,
            FieldType::new(FieldTypeCode::Year),
        ));
        let lt = func_expr("lt", vec![year_col.clone(), int_const_expr(2156)]);
        assert_eq!(show(&build_points(&lt)), "[-inf 2155]");
        // `y > 2156` clamps the CONSTANT to 2155 but keeps GT (the value
        // moved down, not up): Go answers `(2155, +inf]`, empty only at
        // execution over the YEAR domain.
        let gt = func_expr("gt", vec![year_col.clone(), int_const_expr(2156)]);
        assert_eq!(show(&build_points(&gt)), "(2155 +inf]");
    }

    /// `LIKE 'abc%'` on a _bin column: the sort-key range `[abc, abd)`
    /// (Go's increment-last-byte end key).
    #[test]
    fn like_prefix_builds_the_increment_range() {
        let mut str_ft = FieldType::new(FieldTypeCode::Varchar);
        str_ft.set_flen(20);
        str_ft.set_charset_name("utf8mb4");
        str_ft.set_collation_name("utf8mb4_bin");
        let col = Expression::Column(tidb_expr::column::Column::new(1, str_ft.clone()));
        let pattern = Expression::Constant(tidb_expr::constant::Constant::new(
            Datum::String(tidb_datatype::StringDatum::new(
                b"abc%".to_vec(),
                Collation::Utf8Mb4Bin,
            )),
            str_ft.clone(),
        ));
        let escape = int_const_expr(i64::from(b'\\'));
        let mut like = ScalarFunction::new(
            tidb_ast::CiString::new("like"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![col, pattern, escape],
        );
        like.collation
            .set_charset_and_collation("utf8mb4", "utf8mb4_bin");
        let mut builder = PointBuilder::default();
        let points = builder.build(
            &Expression::ScalarFunction(like),
            &str_ft,
            super::super::checker::UNSPECIFIED_LENGTH,
            true,
        );
        assert!(builder.err.is_none(), "{:?}", builder.err);
        assert_eq!(points.len(), 2, "{}", show(&points));
        assert!(points[0].start && !points[0].excl);
        assert!(
            matches!(&points[0].value, Datum::Bytes(b) if b.as_slice() == b"abc"),
            "{:?}",
            points[0].value
        );
        assert!(!points[1].start && points[1].excl);
        assert!(
            matches!(&points[1].value, Datum::Bytes(b) if b.as_slice() == b"abd"),
            "{:?}",
            points[1].value
        );
    }

    /// Constants and bare columns: truthy/falsy/NULL constants, and the
    /// is-true shape of a bare column.
    #[test]
    fn constants_and_columns_build_like_go() {
        assert_eq!(show(&build_points(&int_const_expr(1))), "[<nil> +inf]");
        assert_eq!(show(&build_points(&int_const_expr(0))), "");
        assert_eq!(
            show(&build_points(&int_col_expr(1))),
            "[-inf 0) (0 +inf]"
        );
        let is_null = func_expr("isnull", vec![int_col_expr(1)]);
        assert_eq!(show(&build_points(&is_null)), "[<nil> <nil>]");
    }
}
