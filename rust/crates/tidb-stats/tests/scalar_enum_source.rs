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

use tidb_datatype::{CoreTime, Datum, MySqlDuration, Time, TimeType};
use tidb_stats::enum_range_values;

fn ints(values: Option<Vec<Datum>>) -> Vec<i64> {
    values
        .unwrap()
        .into_iter()
        .map(|value| match value {
            Datum::Int(value) => value,
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

fn time(kind: TimeType, day: u8, second: u8) -> Time {
    Time::new(CoreTime::from_date(2017, 1, day, 0, 0, second, 0), kind, 0).unwrap()
}

#[test]
fn source_signed_and_unsigned_boundaries_match() {
    assert_eq!(
        ints(enum_range_values(
            &Datum::Int(0),
            &Datum::Int(5),
            false,
            true
        )),
        [0, 1, 2, 3, 4]
    );
    assert!(
        enum_range_values(&Datum::Int(i64::MIN), &Datum::Int(i64::MAX), false, false).is_none()
    );
    assert!(enum_range_values(&Datum::Int(i64::MIN), &Datum::Int(0), false, false).is_none());
    assert_eq!(
        enum_range_values(&Datum::UInt(0), &Datum::UInt(5), false, true).unwrap(),
        (0..5).map(Datum::UInt).collect::<Vec<_>>()
    );
    assert!(enum_range_values(&Datum::UInt(5), &Datum::UInt(0), false, false).is_none());
    assert_eq!(
        enum_range_values(&Datum::Int(1), &Datum::Int(1), true, false),
        Some(Vec::new())
    );
    assert!(enum_range_values(&Datum::Int(1), &Datum::UInt(1), false, false).is_none());
}

#[test]
fn source_duration_uses_max_fsp_rounding_and_exclusions() {
    let low = Datum::Duration(MySqlDuration::from_nanoseconds(400_000_000, 0).unwrap());
    let high = Datum::Duration(MySqlDuration::from_nanoseconds(5_000_000_000, 0).unwrap());
    let values = enum_range_values(&low, &high, false, true).unwrap();
    let nanos: Vec<i64> = values
        .into_iter()
        .map(|value| match value {
            Datum::Duration(value) => value.nanoseconds(),
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(
        nanos,
        [
            0,
            1_000_000_000,
            2_000_000_000,
            3_000_000_000,
            4_000_000_000,
        ]
    );

    let precise_high = Datum::Duration(MySqlDuration::from_nanoseconds(4_000, 6).unwrap());
    let precise = enum_range_values(
        &Datum::Duration(MySqlDuration::from_nanoseconds(0, 0).unwrap()),
        &precise_high,
        false,
        false,
    )
    .unwrap();
    assert_eq!(precise.len(), 5);
}

#[test]
fn source_date_datetime_and_timestamp_ranges_match() {
    let dates = enum_range_values(
        &Datum::Time(time(TimeType::Date, 1, 0)),
        &Datum::Time(time(TimeType::Date, 5, 0)),
        false,
        true,
    )
    .unwrap();
    assert_eq!(dates.len(), 4);
    assert_eq!(dates[3], Datum::Time(time(TimeType::Date, 4, 0)));

    for kind in [TimeType::DateTime, TimeType::Timestamp] {
        let values = enum_range_values(
            &Datum::Time(time(kind, 1, 0)),
            &Datum::Time(time(kind, 1, 5)),
            false,
            true,
        )
        .unwrap();
        assert_eq!(values.len(), 5);
        assert_eq!(values[4], Datum::Time(time(kind, 1, 4)));
    }

    assert!(enum_range_values(
        &Datum::Time(time(TimeType::Date, 1, 0)),
        &Datum::Time(time(TimeType::Date, 1, 0)),
        true,
        true,
    )
    .is_none());
    assert!(enum_range_values(
        &Datum::Time(time(TimeType::DateTime, 1, 0)),
        &Datum::Time(time(TimeType::Timestamp, 1, 0)),
        false,
        false,
    )
    .is_none());
}
