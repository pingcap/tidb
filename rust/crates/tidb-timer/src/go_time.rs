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

//! `boundary:` Go's standard-library `time.Time`, which the whole of
//! `pkg/timer/api` stores in its records and compares for equality.
//!
//! A Go `time.Time` is an instant *plus* a `*time.Location`, and this package
//! depends on both halves: `normalizeTimeFields` re-homes a record's instants
//! into the timer's zone, `TimerRecord.NextEventTime` evaluates the schedule
//! against the watermark's zone, and the upstream tests compare the zone as
//! part of the value. Rust's `chrono::DateTime<Tz>` fixes the zone in the type,
//! so this module pairs a UTC instant with `tidb_util::timeutil::TimeZone` and
//! reimplements exactly the `time` operations this package uses:
//! `Now`, `Unix`, `UnixMilli`, `Date`, `IsZero`, `Add`, `Sub`, `AddDate`,
//! `Truncate`, `In`, and the local calendar accessors the cron evaluator needs.
//!
//! Not implemented (the source never calls them): formatting/parsing, monotonic
//! clock readings, `Round`, and marshalling.
//!
//! Ambiguous and non-existent local wall clocks (DST transitions) resolve to
//! the earlier offset; Go's `time.Date` documents the same "one of the two"
//! latitude and picks the pre-transition offset for the ambiguous case.

use chrono::{
    DateTime, Datelike, Duration, Local, NaiveDate, NaiveDateTime, TimeZone as _, Timelike, Utc,
};
use tidb_util::timeutil::TimeZone;

/// Nanoseconds in a second, as Go's `time.Second`.
pub const SECOND: i64 = 1_000_000_000;
/// Go `time.Minute`.
pub const MINUTE: i64 = 60 * SECOND;
/// Go `time.Hour`.
pub const HOUR: i64 = 60 * MINUTE;

/// The Rust shape of Go's `time.Time`: an instant plus a location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoTime {
    instant: DateTime<Utc>,
    location: TimeZone,
}

fn zero_instant() -> DateTime<Utc> {
    // Go's zero `time.Time` is January 1, year 1, 00:00:00 UTC.
    Utc.from_utc_datetime(
        &NaiveDate::from_ymd_opt(1, 1, 1)
            .expect("year 1 is representable")
            .and_hms_opt(0, 0, 0)
            .expect("midnight is representable"),
    )
}

impl Default for GoTime {
    fn default() -> Self {
        Self::zero()
    }
}

impl GoTime {
    /// Go's zero `time.Time` (`time.Time{}`), whose location reads as UTC.
    pub fn zero() -> Self {
        Self {
            instant: zero_instant(),
            location: TimeZone::Named(chrono_tz::Tz::UTC),
        }
    }

    /// Go `time.Now()` — the current instant in the process-local zone.
    pub fn now() -> Self {
        Self {
            instant: Utc::now(),
            location: TimeZone::Local,
        }
    }

    /// Go `time.Unix(sec, nsec)`, which returns a local-zone time.
    pub fn from_unix(seconds: i64, nanoseconds: i64) -> Self {
        let instant = Utc
            .timestamp_opt(seconds, 0)
            .single()
            .expect("in-range unix second")
            + Duration::nanoseconds(nanoseconds);
        Self {
            instant,
            location: TimeZone::Local,
        }
    }

    /// Go `time.UnixMilli(ms)`, likewise local-zone.
    pub fn from_unix_milli(milliseconds: i64) -> Self {
        Self::from_unix(
            milliseconds.div_euclid(1000),
            milliseconds.rem_euclid(1000) * 1_000_000,
        )
    }

    /// Go `time.Date(...)`, including its normalization of out-of-range fields
    /// (month 13 rolls into the next year, day 0 into the previous month, ...).
    #[allow(clippy::too_many_arguments)]
    pub fn date(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: i32,
        nanosecond: i64,
        location: &TimeZone,
    ) -> Self {
        let zero_based = month - 1;
        let year = year + zero_based.div_euclid(12);
        let month = zero_based.rem_euclid(12) + 1;
        let first = NaiveDate::from_ymd_opt(year, month as u32, 1)
            .expect("normalized year/month is representable")
            .and_hms_opt(0, 0, 0)
            .expect("midnight is representable");
        let naive = first
            + Duration::days(i64::from(day) - 1)
            + Duration::hours(i64::from(hour))
            + Duration::minutes(i64::from(minute))
            + Duration::seconds(i64::from(second))
            + Duration::nanoseconds(nanosecond);
        Self::from_local(naive, location)
    }

    /// Go `t.IsZero()`.
    pub fn is_zero(&self) -> bool {
        self.instant == zero_instant()
    }

    /// Go `t.Unix()`.
    pub fn unix(&self) -> i64 {
        self.instant.timestamp()
    }

    /// Go `t.Add(d)` — absolute, so the location is carried through unchanged.
    pub fn add(&self, nanoseconds: i64) -> Self {
        Self {
            instant: self.instant + Duration::nanoseconds(nanoseconds),
            location: self.location.clone(),
        }
    }

    /// Go `t.Sub(u)`, in nanoseconds. Go clamps an overflowing difference to
    /// the maximum (or minimum) `time.Duration`, and so does this.
    pub fn sub(&self, other: &Self) -> i64 {
        match (self.instant - other.instant).num_nanoseconds() {
            Some(nanoseconds) => nanoseconds,
            None if self.instant > other.instant => i64::MAX,
            None => i64::MIN,
        }
    }

    /// Go `t.After(u)`.
    pub fn after(&self, other: &Self) -> bool {
        self.instant > other.instant
    }

    /// Go `t.Before(u)`.
    pub fn before(&self, other: &Self) -> bool {
        self.instant < other.instant
    }

    /// Go `t.Compare(u)`.
    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.instant.cmp(&other.instant)
    }

    /// Go `t.In(loc)`.
    pub fn in_location(&self, location: &TimeZone) -> Self {
        Self {
            instant: self.instant,
            location: location.clone(),
        }
    }

    /// Go `t.Location()`.
    pub fn location(&self) -> &TimeZone {
        &self.location
    }

    /// Go `t.AddDate(years, months, days)`, which is `Date` applied to the
    /// local calendar fields and therefore renormalizes overflow.
    pub fn add_date(&self, years: i32, months: i32, days: i32) -> Self {
        let naive = self.naive_local();
        Self::date(
            naive.year() + years,
            naive.month() as i32 + months,
            naive.day() as i32 + days,
            naive.hour() as i32,
            naive.minute() as i32,
            naive.second() as i32,
            i64::from(naive.nanosecond()),
            &self.location,
        )
    }

    /// Go `t.Truncate(d)`, which rounds the *absolute* time down toward the
    /// zero instant. Only whole-second and whole-minute `d` values are used.
    ///
    /// The remainder is taken against the Unix epoch rather than Go's year-1
    /// epoch, because nanoseconds since year 1 overflow `i64`. The two epochs
    /// are 62135596800 seconds apart — a whole number of minutes — so
    /// second- and minute-granularity truncation lands on the same instant.
    pub fn truncate(&self, nanoseconds: i64) -> Self {
        let elapsed =
            self.instant.timestamp() * SECOND + i64::from(self.instant.timestamp_subsec_nanos());
        Self {
            instant: self.instant - Duration::nanoseconds(elapsed.rem_euclid(nanoseconds)),
            location: self.location.clone(),
        }
    }

    /// The local wall clock in this value's location.
    pub fn naive_local(&self) -> NaiveDateTime {
        match &self.location {
            TimeZone::Local => Local
                .from_utc_datetime(&self.instant.naive_utc())
                .naive_local(),
            TimeZone::Named(zone) => zone
                .from_utc_datetime(&self.instant.naive_utc())
                .naive_local(),
            TimeZone::Fixed { offset_secs, .. } => {
                self.instant.naive_utc() + Duration::seconds(i64::from(*offset_secs))
            }
        }
    }

    /// Go `t.Year()` in the value's location.
    pub fn year(&self) -> i32 {
        self.naive_local().year()
    }

    /// Go `t.Month()` as 1..=12.
    pub fn month(&self) -> u32 {
        self.naive_local().month()
    }

    /// Go `t.Day()`.
    pub fn day(&self) -> u32 {
        self.naive_local().day()
    }

    /// Go `t.Weekday()` as 0 (Sunday) ..= 6 (Saturday).
    pub fn weekday(&self) -> u32 {
        self.naive_local().weekday().num_days_from_sunday()
    }

    /// Go `t.Hour()`.
    pub fn hour(&self) -> u32 {
        self.naive_local().hour()
    }

    /// Go `t.Minute()`.
    pub fn minute(&self) -> u32 {
        self.naive_local().minute()
    }

    /// Go `t.Second()`.
    pub fn second(&self) -> u32 {
        self.naive_local().second()
    }

    /// Go `t.Nanosecond()`.
    pub fn nanosecond(&self) -> u32 {
        self.naive_local().nanosecond()
    }

    fn from_local(naive: NaiveDateTime, location: &TimeZone) -> Self {
        let instant = match location {
            TimeZone::Local => resolve_local(&Local, naive),
            TimeZone::Named(zone) => resolve_local(zone, naive),
            TimeZone::Fixed { offset_secs, .. } => {
                Utc.from_utc_datetime(&(naive - Duration::seconds(i64::from(*offset_secs))))
            }
        };
        Self {
            instant,
            location: location.clone(),
        }
    }
}

/// Maps a local wall clock onto an instant, preferring the earlier offset for
/// ambiguous times and stepping forward across a spring-forward gap.
fn resolve_local<Tz: chrono::TimeZone>(zone: &Tz, naive: NaiveDateTime) -> DateTime<Utc> {
    match zone.from_local_datetime(&naive).earliest() {
        Some(resolved) => resolved.with_timezone(&Utc),
        None => {
            let shifted = naive + Duration::hours(1);
            zone.from_local_datetime(&shifted)
                .earliest()
                .map(|resolved| resolved.with_timezone(&Utc))
                .unwrap_or_else(|| Utc.from_utc_datetime(&naive))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_and_arithmetic() {
        assert!(GoTime::zero().is_zero());
        assert!(!GoTime::now().is_zero());

        let base = GoTime::from_unix(0, 0).in_location(&TimeZone::Named(chrono_tz::Tz::UTC));
        assert_eq!(base.unix(), 0);
        assert_eq!(base.add(HOUR).unix(), 3600);
        assert_eq!(base.add(HOUR).sub(&base), HOUR);
    }

    #[test]
    fn date_normalizes_like_go() {
        let utc = TimeZone::Named(chrono_tz::Tz::UTC);
        // Go: time.Date(2021, 13, 1, ...) == 2022-01-01.
        let rolled = GoTime::date(2021, 13, 1, 0, 0, 0, 0, &utc);
        assert_eq!((rolled.year(), rolled.month(), rolled.day()), (2022, 1, 1));
        // Go: February 30 rolls into March 2.
        let rolled = GoTime::date(2021, 2, 30, 0, 0, 0, 0, &utc);
        assert_eq!((rolled.year(), rolled.month(), rolled.day()), (2021, 3, 2));
    }

    #[test]
    fn weekday_is_sunday_based() {
        let utc = TimeZone::Named(chrono_tz::Tz::UTC);
        // 2021-11-21 was a Sunday.
        assert_eq!(GoTime::date(2021, 11, 21, 0, 0, 0, 0, &utc).weekday(), 0);
        assert_eq!(GoTime::date(2021, 11, 19, 0, 0, 0, 0, &utc).weekday(), 5);
    }
}
