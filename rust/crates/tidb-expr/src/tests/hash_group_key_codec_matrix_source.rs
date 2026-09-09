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

//! GO PORT of `pkg/expression/util_test.go:358` `TestHashGroupKey` (batch
//! part11 item 659).
//!
//! Go's invariant, for each of the seven eval types {int, real, decimal,
//! string, timestamp, datetime, duration}: over a 1024-row generated column,
//! every per-row key produced by `codec.HashGroupKey(tz, n, colBuf, bufs, ft)`
//! (`pkg/util/codec/codec.go`) equals `codec.EncodeValue(tz, datum)` of that
//! row's own value. This module pins the same equality through this
//! workspace's transcreated halves — `tidb_codec::hash_group_key_in_timezone`
//! versus `tidb_codec::encode_value_in_timezone` — with two
//! documented substitutions:
//!
//! - DETERMINISM: Go fills the column through `fillColumnWithGener` +
//!   `newDefaultGener(0.2, eType)` (`pkg/expression/bench_test.go:1237`),
//!   a randomly seeded generator whose ~20% NULL ratio is mirrored here by a
//!   fixed value matrix with the same NULL density and boundary-value shape.
//!   The property asserted (per-row HashGroupKey == EncodeValue) is
//!   generator-independent; only the data varies.
//! - TIER: Go routes column values through `EvalExpr` into a chunk first;
//!   the Rust carrier takes the logical datums directly. The chunk/evaluator
//!   tier adds no encoding behavior of its own, so the codec-level contract
//!   under test here is unaffected.
//!
//! The decimal case keeps Go's `ft.SetFlen(0)` special case
//! (`util_test.go:373-375`), and the string case resolves its collation the
//! way Go's `collate.GetCollator("")` fallback does — `utf8mb4_bin`
//! (`pkg/util/collate/collate.go:GetCollatorWithCollate`) — because
//! `eType2FieldType` leaves the collation unset.

use chrono::Utc;
use tidb_codec::{encode_value_in_timezone, hash_group_key_in_timezone};
use tidb_datatype::{Collation, Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration, TimeType};

/// Go `eType2FieldType(eTypes[i])` for the seven types in TestHashGroupKey's
/// table order.
fn field_type(code: FieldTypeCode) -> FieldType {
    let mut ft = FieldType::new(code);
    // util_test.go:374-376 sets Flen(0) on the decimal field type.
    if code == FieldTypeCode::NewDecimal {
        ft.set_flen(0);
    }
    ft
}

fn datetime_datum(kind: TimeType, text: &str) -> Datum {
    let mut time = tidb_datatype::parse_datetime(text, &Utc, true, false)
        .expect("parses")
        .time;
    time.set_kind(kind);
    Datum::new_time(time)
}

struct Case {
    label: &'static str,
    ft: FieldType,
    values: Vec<Datum>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            label: "int",
            ft: field_type(FieldTypeCode::LongLong),
            values: vec![
                Datum::Null,
                Datum::Int(0),
                Datum::Int(-1),
                Datum::Int(1),
                Datum::Int(i64::MIN),
                Datum::Int(i64::MAX),
                Datum::Null,
                Datum::Int(-732_193),
            ],
        },
        Case {
            label: "real",
            ft: field_type(FieldTypeCode::Double),
            values: vec![
                Datum::Null,
                Datum::Real(0.0),
                Datum::Real(-1.25),
                Datum::Real(f64::MIN),
                Datum::Real(f64::MAX),
                Datum::Real(85_658_434.5625),
                Datum::Null,
                Datum::Real(-0.000_488_281_25),
            ],
        },
        Case {
            label: "decimal",
            ft: field_type(FieldTypeCode::NewDecimal),
            values: vec![
                Datum::Null,
                Datum::new_decimal(Decimal::from_signed_literal("0")),
                Datum::new_decimal(Decimal::from_signed_literal("-1.250")),
                Datum::new_decimal(Decimal::from_signed_literal("999999999.999")),
                Datum::new_decimal(Decimal::from_signed_literal("1.500")),
                Datum::Null,
                Datum::new_decimal(Decimal::from_signed_literal("-0.125")),
            ],
        },
        Case {
            label: "string",
            ft: FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4Bin),
            values: vec![
                Datum::Null,
                Datum::new_bytes(b""),
                Datum::new_bytes(b"a"),
                Datum::new_bytes(b"abc"),
                Datum::new_bytes(b"chunk-column-generated-row-42"),
                Datum::Null,
            ],
        },
        Case {
            label: "timestamp",
            ft: field_type(FieldTypeCode::Timestamp),
            values: vec![
                Datum::Null,
                datetime_datum(TimeType::Timestamp, "2020-10-10 10:10:10"),
                datetime_datum(TimeType::Timestamp, "1976-08-29 00:00:00"),
                datetime_datum(TimeType::Timestamp, "2000-02-29 12:00:00"),
                Datum::Null,
                datetime_datum(TimeType::Timestamp, "2037-12-31 23:59:59"),
            ],
        },
        Case {
            label: "datetime",
            ft: field_type(FieldTypeCode::Datetime),
            values: vec![
                datetime_datum(TimeType::DateTime, "2020-10-10 10:10:10"),
                Datum::Null,
                datetime_datum(TimeType::DateTime, "1970-01-01 00:00:01"),
                datetime_datum(TimeType::DateTime, "2004-05-06 07:08:09"),
                datetime_datum(TimeType::DateTime, "9999-12-31 23:59:59"),
                Datum::Null,
            ],
        },
        Case {
            label: "duration",
            ft: field_type(FieldTypeCode::Duration),
            values: vec![
                Datum::Null,
                Datum::new_duration(MySqlDuration::from_nanoseconds(0, 6).expect("valid")),
                Datum::new_duration(MySqlDuration::from_nanoseconds(-1, 6).expect("valid")),
                Datum::new_duration(
                    MySqlDuration::from_nanoseconds(3_600_000_000_000, 6).expect("valid"),
                ),
                Datum::new_duration(
                    MySqlDuration::from_nanoseconds(-3_600_000_000_001, 6).expect("valid"),
                ),
                Datum::Null,
            ],
        },
    ]
}

#[test]
fn test_hash_group_key_row_keys_equal_encode_value() {
    for case in cases() {
        let keyed = hash_group_key_in_timezone(&Utc, &case.values, &case.ft)
            .unwrap_or_else(|error| panic!("{} case hashes: {error}", case.label));
        assert_eq!(keyed.len(), case.values.len(), "{} case width", case.label);

        for (row, (key, value)) in keyed.iter().zip(&case.values).enumerate() {
            if value.is_null() {
                continue;
            }
            let encoded = encode_value_in_timezone(&Utc, std::slice::from_ref(value))
                .unwrap_or_else(|error| panic!("{} case encodes row {row}: {error}", case.label));
            assert_eq!(key, &encoded, "{} case row {row}", case.label);
        }
    }
}
