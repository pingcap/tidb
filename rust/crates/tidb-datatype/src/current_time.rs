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

//! The internal session's clock as TiDB values: Go `types.CurrentTime` over
//! `time.Now()`, read in UTC for `TIMESTAMP` columns and in the SYSTEM
//! location for `DATETIME` ones.

use chrono::{Datelike, Timelike};

use crate::{Time, TimeType};

/// The UTC wall clock as one `TIMESTAMP` value, for
/// the bootstrap environment's current timestamp.
///
/// # Panics
///
/// Never in practice: the fields come from a calendar date that is by
/// construction in range.
#[must_use]
pub fn utc_now_timestamp() -> Time {
    calendar_time(chrono::Utc::now(), TimeType::Timestamp, 0)
}

/// The local wall clock as the `DATETIME(6)` used by historical statistics.
///
/// Go's payload writer formats `time.Now()` and its metadata writer evaluates
/// `NOW(6)` in the internal system session's `SYSTEM` location.
#[must_use]
pub fn local_now_datetime6() -> Time {
    calendar_time(chrono::Local::now(), TimeType::DateTime, 6)
}

/// The internal SYSTEM session's `NOW()`, truncated to whole seconds.
#[must_use]
pub fn local_now_datetime() -> Time {
    calendar_time(chrono::Local::now(), TimeType::DateTime, 0)
}

fn calendar_time<Tz: chrono::TimeZone>(
    now: chrono::DateTime<Tz>,
    time_type: TimeType,
    fsp: u8,
) -> Time {
    Time::from_date_checked(
        now.year(),
        i32::try_from(now.month()).expect("a month fits in i32"),
        i32::try_from(now.day()).expect("a day fits in i32"),
        i32::try_from(now.hour()).expect("an hour fits in i32"),
        i32::try_from(now.minute()).expect("a minute fits in i32"),
        i32::try_from(now.second()).expect("a second fits in i32"),
        if fsp == 0 {
            0
        } else {
            i32::try_from(now.nanosecond() / 1_000).expect("microseconds fit in i32")
        },
        time_type,
        i64::from(fsp),
    )
    .expect("the current wall-clock calendar date is a valid timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_timestamp_retains_six_fractional_digits() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-30T12:34:56.654321Z")
            .expect("fixed RFC3339 timestamp")
            .with_timezone(&chrono::Utc);
        let timestamp = calendar_time(now, TimeType::DateTime, 6);
        assert_eq!(timestamp.core_time().microsecond(), 654_321);
        assert_eq!(timestamp.fsp(), 6);

        let second_precision = calendar_time(
            chrono::DateTime::parse_from_rfc3339("2026-08-30T12:34:56.654321Z")
                .expect("fixed RFC3339 timestamp")
                .with_timezone(&chrono::Utc),
            TimeType::Timestamp,
            0,
        );
        assert_eq!(second_precision.core_time().microsecond(), 0);
        assert_eq!(second_precision.fsp(), 0);
    }
}
