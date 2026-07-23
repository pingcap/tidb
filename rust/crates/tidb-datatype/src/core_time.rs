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

use std::cmp::Ordering;
use std::fmt;

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, LocalResult, NaiveDate, NaiveDateTime,
    TimeZone, Timelike,
};

const YEAR_OFFSET: u64 = 50;
const MONTH_OFFSET: u64 = 46;
const DAY_OFFSET: u64 = 41;
const HOUR_OFFSET: u64 = 36;
const MINUTE_OFFSET: u64 = 30;
const SECOND_OFFSET: u64 = 24;
const MICROSECOND_OFFSET: u64 = 4;
const SECONDS_IN_24_HOURS: i64 = 86_400;
const DAYS_BY_MONTH: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// TiDB's compact internal calendar representation.
#[derive(Clone, Copy, Default, Eq, Hash, PartialEq)]
pub struct CoreTime(u64);

impl CoreTime {
    /// Constructs from TiDB's exact internal bit representation.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns TiDB's exact internal bit representation.
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Constructs the exact bit layout used by Go `FromDate`.
    pub const fn from_date(
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    ) -> Self {
        Self(
            ((year as u64 & 0x3fff) << YEAR_OFFSET)
                | ((month as u64 & 0x0f) << MONTH_OFFSET)
                | ((day as u64 & 0x1f) << DAY_OFFSET)
                | ((hour as u64 & 0x1f) << HOUR_OFFSET)
                | ((minute as u64 & 0x3f) << MINUTE_OFFSET)
                | ((second as u64 & 0x3f) << SECOND_OFFSET)
                | ((microsecond as u64 & 0x0f_ffff) << MICROSECOND_OFFSET),
        )
    }

    /// Returns the year.
    pub const fn year(self) -> i32 {
        ((self.0 >> YEAR_OFFSET) & 0x3fff) as i32
    }

    /// Returns the month.
    pub const fn month(self) -> u8 {
        ((self.0 >> MONTH_OFFSET) & 0x0f) as u8
    }

    /// Returns the day of month.
    pub const fn day(self) -> u8 {
        ((self.0 >> DAY_OFFSET) & 0x1f) as u8
    }

    /// Returns the hour.
    pub const fn hour(self) -> u8 {
        ((self.0 >> HOUR_OFFSET) & 0x1f) as u8
    }

    /// Returns the minute.
    pub const fn minute(self) -> u8 {
        ((self.0 >> MINUTE_OFFSET) & 0x3f) as u8
    }

    /// Returns the second.
    pub const fn second(self) -> u8 {
        ((self.0 >> SECOND_OFFSET) & 0x3f) as u8
    }

    /// Returns the microsecond.
    pub const fn microsecond(self) -> u32 {
        ((self.0 >> MICROSECOND_OFFSET) & 0x0f_ffff) as u32
    }

    /// Returns whether the represented year is a leap year.
    pub const fn is_leap_year(self) -> bool {
        is_leap_year(self.year())
    }

    /// Returns the day within the year, or zero for an incomplete date.
    pub const fn year_day(self) -> i32 {
        if self.month() == 0 || self.day() == 0 {
            0
        } else {
            calc_daynr(self.year(), self.month() as i32, self.day() as i32)
                - calc_daynr(self.year(), 1, 1)
                + 1
        }
    }

    /// Returns the normalized Gregorian weekday.
    ///
    /// Like Go's `time.Date`, invalid month-day combinations are normalized;
    /// for example 2019-02-31 is the Sunday 2019-03-03.
    pub fn weekday(self) -> Weekday {
        let month_offset = i32::from(self.month()) - 1;
        let year = self.year() + month_offset.div_euclid(12);
        let month = month_offset.rem_euclid(12) as u32 + 1;
        let first = NaiveDate::from_ymd_opt(year, month, 1)
            .expect("CoreTime's encoded year and month fit chrono");
        let normalized = first + ChronoDuration::days(i64::from(self.day()) - 1);
        Weekday::from_sunday_index(normalized.weekday().num_days_from_sunday() as i32)
    }

    /// Converts this value through an IANA timezone, rejecting invalid or
    /// nonexistent local wall-clock values.
    pub fn to_datetime<TZ: TimeZone>(
        self,
        timezone: &TZ,
    ) -> Result<DateTime<TZ>, TimeConversionError> {
        resolve_local_datetime(timezone, self.naive_datetime()?, false)
    }

    /// Converts through an IANA timezone and moves a spring-forward gap to its
    /// closest valid upper boundary.
    pub fn adjusted_datetime<TZ: TimeZone>(
        self,
        timezone: &TZ,
    ) -> Result<DateTime<TZ>, TimeConversionError> {
        resolve_local_datetime(timezone, self.naive_datetime()?, true)
    }

    /// Returns the week under MySQL's mode rules.
    pub const fn week(self, mode: u8) -> i32 {
        if self.month() == 0 || self.day() == 0 {
            return 0;
        }
        calc_week(self, week_mode(mode)).1
    }

    /// Returns the week-numbering year and week with MySQL `YEARWEEK` rules.
    pub const fn year_week(self, mode: u8) -> (i32, i32) {
        calc_week(self, week_mode(mode) | WEEK_BEHAVIOUR_YEAR)
    }

    /// Returns the signed comparison used by Go `compareTime`.
    pub fn compare(self, other: Self) -> Ordering {
        datetime_to_u64(self)
            .cmp(&datetime_to_u64(other))
            .then_with(|| self.microsecond().cmp(&other.microsecond()))
    }

    /// Returns the calendar day difference between two dates.
    pub const fn date_diff(self, other: Self) -> i32 {
        calc_daynr(self.year(), self.month() as i32, self.day() as i32)
            - calc_daynr(other.year(), other.month() as i32, other.day() as i32)
    }

    /// Calculates the absolute temporal difference with Go's signed operand rule.
    pub fn time_diff(self, other: Self, sign: i32) -> TimeDifference {
        time_diff_internal(
            self,
            other.year(),
            other.month() as i32,
            other.day() as i32,
            other.hour() as i32,
            other.minute() as i32,
            other.second() as i32,
            other.microsecond() as i32,
            sign,
        )
    }

    /// Computes MySQL `TIMESTAMPDIFF`.
    pub fn timestamp_diff(self, other: Self, interval: TimestampInterval) -> i64 {
        timestamp_diff(interval, self, other)
    }

    /// Adds calendar years, months, and days with TiDB's month-end rule.
    pub fn add_date(self, years: i64, months: i64, days: i64) -> Result<Self, DateAddError> {
        const MAX_ADD: i64 = 10_000 * 365;
        if !(-MAX_ADD..=MAX_ADD).contains(&years)
            || !(-MAX_ADD..=MAX_ADD).contains(&months)
            || !(-MAX_ADD..=MAX_ADD).contains(&days)
        {
            return Err(DateAddError);
        }

        let total_months = i64::from(self.year())
            .checked_mul(12)
            .and_then(|value| value.checked_add(i64::from(self.month()) - 1))
            .and_then(|value| value.checked_add(years.checked_mul(12)?))
            .and_then(|value| value.checked_add(months))
            .ok_or(DateAddError)?;
        let mut year = total_months.div_euclid(12);
        let mut month = total_months.rem_euclid(12) + 1;
        let mut day = i64::from(self.day());

        if days == 0 && (years != 0 || months != 0) {
            day += fix_days(years, months, days, self);
        } else {
            day = day.checked_add(days).ok_or(DateAddError)?;
            normalize_day(&mut year, &mut month, &mut day);
        }
        if !(0..=9999).contains(&year) {
            return Err(DateAddError);
        }
        Ok(Self::from_date(
            year as u16,
            month as u8,
            day as u8,
            self.hour(),
            self.minute(),
            self.second(),
            self.microsecond(),
        ))
    }

    /// Returns the `YYYYMMDDHHMMSS` integer used by temporal comparison.
    pub const fn datetime_number(self) -> u64 {
        datetime_to_u64(self)
    }

    /// Adds a signed duration while preserving the existing clock fields.
    pub fn add_duration(self, nanoseconds: i64) -> Self {
        let own_micros = i64::from(calc_daynr(
            self.year(),
            self.month() as i32,
            self.day() as i32,
        )) * SECONDS_IN_24_HOURS
            * 1_000_000
            + i64::from(self.hour()) * 3_600_000_000
            + i64::from(self.minute()) * 60_000_000
            + i64::from(self.second()) * 1_000_000
            + i64::from(self.microsecond());
        let result = own_micros + nanoseconds / 1_000;
        let daynr = result.div_euclid(SECONDS_IN_24_HOURS * 1_000_000);
        let time = result.rem_euclid(SECONDS_IN_24_HOURS * 1_000_000);
        let (year, month, day) = get_date_from_daynr(daynr as u32);
        let seconds = time / 1_000_000;
        Self::from_date(
            year as u16,
            month as u8,
            day as u8,
            (seconds / 3_600) as u8,
            (seconds % 3_600 / 60) as u8,
            (seconds % 60) as u8,
            (time % 1_000_000) as u32,
        )
    }

    /// Adds a signed duration in nanoseconds using TiDB's date/time mixing.
    pub fn mix_duration(&mut self, nanoseconds: i64) {
        if nanoseconds >= 0 && nanoseconds / 3_600_000_000_000 < 24 {
            let micros = nanoseconds / 1_000;
            self.set_time_from_micros(micros);
            return;
        }
        *self = self.add_duration(nanoseconds);
    }

    fn set_time_from_micros(&mut self, micros: i64) {
        let seconds = micros / 1_000_000;
        let hour = (seconds / 3_600) as u8;
        let minute = (seconds % 3_600 / 60) as u8;
        let second = (seconds % 60) as u8;
        let microsecond = (micros % 1_000_000) as u32;
        *self = Self::from_date(
            self.year() as u16,
            self.month(),
            self.day(),
            hour,
            minute,
            second,
            microsecond,
        );
    }

    fn naive_datetime(self) -> Result<NaiveDateTime, TimeConversionError> {
        let date =
            NaiveDate::from_ymd_opt(self.year(), u32::from(self.month()), u32::from(self.day()))
                .ok_or(TimeConversionError::InvalidCalendar)?;
        date.and_hms_micro_opt(
            u32::from(self.hour()),
            u32::from(self.minute()),
            u32::from(self.second()),
            self.microsecond(),
        )
        .ok_or(TimeConversionError::InvalidCalendar)
    }
}

/// Absolute seconds/microseconds plus the sign of a temporal difference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeDifference {
    /// Absolute whole seconds.
    pub seconds: i64,
    /// Absolute remaining microseconds.
    pub microseconds: i32,
    /// Whether the source difference was negative.
    pub negative: bool,
}

/// Calendar overflow from [`CoreTime::add_date`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DateAddError;

impl fmt::Display for DateAddError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("datetime function overflow: datetime")
    }
}

impl std::error::Error for DateAddError {}

/// Failure converting a MySQL wall-clock value through an IANA timezone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeConversionError {
    /// The calendar or clock fields are invalid.
    InvalidCalendar,
    /// The local time lies in a timezone transition gap.
    NonexistentLocalTime,
    /// No valid transition boundary exists within TiDB's four-hour limit.
    TransitionOutOfRange,
}

impl fmt::Display for TimeConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCalendar => "invalid calendar time",
            Self::NonexistentLocalTime => "nonexistent local time",
            Self::TransitionOutOfRange => "timezone transition exceeds four hours",
        })
    }
}

impl std::error::Error for TimeConversionError {}

/// Gregorian weekday using Go's Sunday-zero order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Weekday {
    /// Sunday.
    Sunday,
    /// Monday.
    Monday,
    /// Tuesday.
    Tuesday,
    /// Wednesday.
    Wednesday,
    /// Thursday.
    Thursday,
    /// Friday.
    Friday,
    /// Saturday.
    Saturday,
}

impl Weekday {
    const fn from_sunday_index(index: i32) -> Self {
        match index {
            0 => Self::Sunday,
            1 => Self::Monday,
            2 => Self::Tuesday,
            3 => Self::Wednesday,
            4 => Self::Thursday,
            5 => Self::Friday,
            _ => Self::Saturday,
        }
    }

    /// Returns the Sunday-zero index used by Go and MySQL `%w`.
    pub const fn sunday_index(self) -> u8 {
        match self {
            Self::Sunday => 0,
            Self::Monday => 1,
            Self::Tuesday => 2,
            Self::Wednesday => 3,
            Self::Thursday => 4,
            Self::Friday => 5,
            Self::Saturday => 6,
        }
    }

    /// Returns MySQL's abbreviated English weekday name.
    pub const fn abbreviated_name(self) -> &'static str {
        match self {
            Self::Sunday => "Sun",
            Self::Monday => "Mon",
            Self::Tuesday => "Tue",
            Self::Wednesday => "Wed",
            Self::Thursday => "Thu",
            Self::Friday => "Fri",
            Self::Saturday => "Sat",
        }
    }
}

impl fmt::Display for Weekday {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Sunday => "Sunday",
            Self::Monday => "Monday",
            Self::Tuesday => "Tuesday",
            Self::Wednesday => "Wednesday",
            Self::Thursday => "Thursday",
            Self::Friday => "Friday",
            Self::Saturday => "Saturday",
        })
    }
}

fn resolve_local_datetime<TZ: TimeZone>(
    timezone: &TZ,
    naive: NaiveDateTime,
    adjust_gap: bool,
) -> Result<DateTime<TZ>, TimeConversionError> {
    match timezone.from_local_datetime(&naive) {
        LocalResult::Single(value) => Ok(value),
        // Go's time package resolves a repeated wall-clock time to the later
        // occurrence used by TiDB's source tests.
        LocalResult::Ambiguous(_, later) => Ok(later),
        LocalResult::None if !adjust_gap => Err(TimeConversionError::NonexistentLocalTime),
        LocalResult::None => {
            let transition_search = naive.with_nanosecond(0).expect("zero nanosecond is valid");
            for seconds in 1..=4 * 60 * 60 {
                let candidate = transition_search + ChronoDuration::seconds(seconds);
                match timezone.from_local_datetime(&candidate) {
                    LocalResult::Single(value) => return Ok(value),
                    LocalResult::Ambiguous(_, later) => return Ok(later),
                    LocalResult::None => {}
                }
            }
            Err(TimeConversionError::TransitionOutOfRange)
        }
    }
}

/// Units accepted by MySQL `TIMESTAMPDIFF`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampInterval {
    /// Calendar years.
    Year,
    /// Calendar quarters.
    Quarter,
    /// Calendar months.
    Month,
    /// Seven-day weeks.
    Week,
    /// Days.
    Day,
    /// Hours.
    Hour,
    /// Minutes.
    Minute,
    /// Seconds.
    Second,
    /// Microseconds.
    Microsecond,
}

impl fmt::Debug for CoreTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for CoreTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{{} {} {} {} {} {} {}}}",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.microsecond()
        )
    }
}

/// Returns whether `year` is a Gregorian leap year.
pub const fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Returns the last valid day of `month`, or zero for an invalid month.
pub const fn get_last_day(year: i32, month: u8) -> u8 {
    if month == 0 || month > 12 {
        return 0;
    }
    if month == 2 && is_leap_year(year) {
        29
    } else {
        DAYS_BY_MONTH[month as usize - 1]
    }
}

/// Calculates days since MySQL's `0000-00-00` epoch.
pub const fn calc_daynr(mut year: i32, month: i32, day: i32) -> i32 {
    if year == 0 && month == 0 {
        return 0;
    }
    let mut sum = 365 * year + 31 * (month - 1) + day;
    if month <= 2 {
        year -= 1;
    } else {
        sum -= (month * 4 + 23) / 10;
    }
    let temp = ((year / 100 + 1) * 3) / 4;
    sum + year / 4 - temp
}

/// Converts a MySQL day number back to a calendar date.
pub const fn get_date_from_daynr(daynr: u32) -> (u32, u32, u32) {
    if daynr <= 365 || daynr >= 3_652_500 {
        return (0, 0, 0);
    }
    let mut year = daynr * 100 / 36_525;
    let temp = (((year - 1) / 100 + 1) * 3) / 4;
    let mut day_of_year = daynr - year * 365 - (year - 1) / 4 + temp;
    let mut days_in_year = calc_days_in_year(year as i32) as u32;
    while day_of_year > days_in_year {
        day_of_year -= days_in_year;
        year += 1;
        days_in_year = calc_days_in_year(year as i32) as u32;
    }
    let mut leap_day = 0;
    if days_in_year == 366 && day_of_year > 59 {
        day_of_year -= 1;
        if day_of_year == 59 {
            leap_day = 1;
        }
    }
    let mut month = 1;
    let mut index = 0;
    while index < DAYS_BY_MONTH.len() {
        let days = DAYS_BY_MONTH[index] as u32;
        if day_of_year <= days {
            break;
        }
        day_of_year -= days;
        month += 1;
        index += 1;
    }
    (year, month, day_of_year + leap_day)
}

fn normalize_day(year: &mut i64, month: &mut i64, day: &mut i64) {
    while *day <= 0 {
        *month -= 1;
        if *month == 0 {
            *month = 12;
            *year -= 1;
        }
        *day += i64::from(get_last_day(*year as i32, *month as u8));
    }
    loop {
        let days_in_month = i64::from(get_last_day(*year as i32, *month as u8));
        if *day <= days_in_month {
            break;
        }
        *day -= days_in_month;
        *month += 1;
        if *month == 13 {
            *month = 1;
            *year += 1;
        }
    }
}

fn fix_days(years: i64, months: i64, days: i64, original: CoreTime) -> i64 {
    if (years == 0 && months == 0) || days != 0 {
        return 0;
    }
    let total_months =
        i64::from(original.year()) * 12 + i64::from(original.month()) - 1 + years * 12 + months;
    let year = total_months.div_euclid(12);
    let month = total_months.rem_euclid(12) + 1;
    let last = i64::from(get_last_day(year as i32, month as u8));
    (last - i64::from(original.day())).min(0)
}

const WEEK_BEHAVIOUR_MONDAY_FIRST: u8 = 1;
const WEEK_BEHAVIOUR_YEAR: u8 = 2;
const WEEK_BEHAVIOUR_FIRST_WEEKDAY: u8 = 4;

const fn week_mode(mode: u8) -> u8 {
    let mut format = mode & 7;
    if format & WEEK_BEHAVIOUR_MONDAY_FIRST == 0 {
        format ^= WEEK_BEHAVIOUR_FIRST_WEEKDAY;
    }
    format
}

/// Calculates weekday from a MySQL day number.
pub const fn calc_weekday(mut daynr: i32, sunday_first: bool) -> i32 {
    daynr += 5;
    if sunday_first {
        daynr += 1;
    }
    daynr % 7
}

/// Returns 365 or 366 using TiDB's year-zero rule.
pub const fn calc_days_in_year(year: i32) -> i32 {
    if year & 3 == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0)) {
        366
    } else {
        365
    }
}

const fn calc_week(time: CoreTime, behaviour: u8) -> (i32, i32) {
    let mut year = time.year();
    let month = time.month() as i32;
    let day = time.day() as i32;
    let daynr = calc_daynr(year, month, day);
    let mut first_daynr = calc_daynr(year, 1, 1);
    let monday_first = behaviour & WEEK_BEHAVIOUR_MONDAY_FIRST != 0;
    let mut week_year = behaviour & WEEK_BEHAVIOUR_YEAR != 0;
    let first_weekday = behaviour & WEEK_BEHAVIOUR_FIRST_WEEKDAY != 0;
    let mut weekday = calc_weekday(first_daynr, !monday_first);

    if month == 1 && day <= 7 - weekday {
        if !week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)) {
            return (year, 0);
        }
        week_year = true;
        year -= 1;
        let days = calc_days_in_year(year);
        first_daynr -= days;
        weekday = (weekday + 53 * 7 - days) % 7;
    }
    let days = if (first_weekday && weekday != 0) || (!first_weekday && weekday >= 4) {
        daynr - (first_daynr + 7 - weekday)
    } else {
        daynr - (first_daynr - weekday)
    };
    if week_year && days >= 52 * 7 {
        weekday = (weekday + calc_days_in_year(year)) % 7;
        if (!first_weekday && weekday < 4) || (first_weekday && weekday == 0) {
            return (year + 1, 1);
        }
    }
    (year, days / 7 + 1)
}

const fn datetime_to_u64(time: CoreTime) -> u64 {
    time.year() as u64 * 10_000_000_000
        + time.month() as u64 * 100_000_000
        + time.day() as u64 * 1_000_000
        + time.hour() as u64 * 10_000
        + time.minute() as u64 * 100
        + time.second() as u64
}

#[allow(clippy::too_many_arguments)]
fn time_diff_internal(
    left: CoreTime,
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    microsecond: i32,
    sign: i32,
) -> TimeDifference {
    let days = calc_daynr(left.year(), left.month() as i32, left.day() as i32)
        - sign * calc_daynr(year, month, day);
    let mut micros = (i64::from(days) * SECONDS_IN_24_HOURS
        + i64::from(left.hour()) * 3_600
        + i64::from(left.minute()) * 60
        + i64::from(left.second())
        - i64::from(sign) * (i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second)))
        * 1_000_000
        + i64::from(left.microsecond())
        - i64::from(sign) * i64::from(microsecond);
    let negative = micros < 0;
    if negative {
        micros = -micros;
    }
    TimeDifference {
        seconds: micros / 1_000_000,
        microseconds: (micros % 1_000_000) as i32,
        negative,
    }
}

fn timestamp_diff(interval: TimestampInterval, start: CoreTime, end: CoreTime) -> i64 {
    let difference = end.time_diff(start, 1);
    let mut months = 0_u32;
    if matches!(
        interval,
        TimestampInterval::Year | TimestampInterval::Quarter | TimestampInterval::Month
    ) {
        let (begin, finish) = if difference.negative {
            (end, start)
        } else {
            (start, end)
        };
        let mut years = (finish.year() - begin.year()) as u32;
        if finish.month() < begin.month()
            || (finish.month() == begin.month() && finish.day() < begin.day())
        {
            years -= 1;
        }
        months = 12 * years;
        if finish.month() < begin.month()
            || (finish.month() == begin.month() && finish.day() < begin.day())
        {
            months += 12 - u32::from(begin.month() - finish.month());
        } else {
            months += u32::from(finish.month() - begin.month());
        }
        let begin_seconds = u32::from(begin.hour()) * 3_600
            + u32::from(begin.minute()) * 60
            + u32::from(begin.second());
        let finish_seconds = u32::from(finish.hour()) * 3_600
            + u32::from(finish.minute()) * 60
            + u32::from(finish.second());
        if finish.day() < begin.day()
            || (finish.day() == begin.day()
                && (finish_seconds < begin_seconds
                    || (finish_seconds == begin_seconds
                        && finish.microsecond() < begin.microsecond())))
        {
            months -= 1;
        }
    }
    let sign = if difference.negative { -1 } else { 1 };
    let value = match interval {
        TimestampInterval::Year => i64::from(months / 12),
        TimestampInterval::Quarter => i64::from(months / 3),
        TimestampInterval::Month => i64::from(months),
        TimestampInterval::Week => difference.seconds / SECONDS_IN_24_HOURS / 7,
        TimestampInterval::Day => difference.seconds / SECONDS_IN_24_HOURS,
        TimestampInterval::Hour => difference.seconds / 3_600,
        TimestampInterval::Minute => difference.seconds / 60,
        TimestampInterval::Second => difference.seconds,
        TimestampInterval::Microsecond => {
            difference.seconds * 1_000_000 + i64::from(difference.microseconds)
        }
    };
    value * sign
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Offset};

    #[test]
    fn test_week_behaviour_and_week() {
        let time = CoreTime::from_date(2008, 2, 20, 0, 0, 0, 0);
        assert_eq!(time.week(0), 7);
        assert_eq!(time.week(1), 8);
        assert_eq!(CoreTime::from_date(2008, 12, 31, 0, 0, 0, 0).week(1), 53);
    }

    #[test]
    fn test_calc_daynr() {
        assert_eq!(calc_daynr(0, 0, 0), 0);
        assert_eq!(calc_daynr(9999, 12, 31), 3_652_424);
        assert_eq!(calc_daynr(1970, 1, 1), 719_528);
        assert_eq!(calc_daynr(2006, 12, 16), 733_026);
        assert_eq!(calc_daynr(10, 1, 2), 3_654);
        assert_eq!(calc_daynr(2008, 2, 20), 733_457);
    }

    #[test]
    fn test_compare_time() {
        let rows = [
            (
                CoreTime::from_date(0, 0, 0, 0, 0, 0, 0),
                CoreTime::from_date(0, 0, 0, 0, 0, 0, 0),
                Ordering::Equal,
            ),
            (
                CoreTime::from_date(0, 0, 0, 0, 1, 0, 0),
                CoreTime::default(),
                Ordering::Greater,
            ),
            (
                CoreTime::from_date(2006, 1, 2, 3, 4, 5, 6),
                CoreTime::from_date(2016, 1, 2, 3, 4, 5, 0),
                Ordering::Less,
            ),
            (
                CoreTime::from_date(0, 0, 0, 11, 22, 33, 0),
                CoreTime::from_date(0, 0, 0, 12, 21, 33, 0),
                Ordering::Less,
            ),
            (
                CoreTime::from_date(9999, 12, 30, 23, 59, 59, 999_999),
                CoreTime::from_date(0, 1, 2, 3, 4, 5, 6),
                Ordering::Greater,
            ),
        ];
        for (left, right, expected) in rows {
            assert_eq!(left.compare(right), expected);
            assert_eq!(right.compare(left), expected.reverse());
        }
    }

    #[test]
    fn test_calc_time_time_diff() {
        let rows = [
            (
                CoreTime::from_date(2006, 0, 1, 12, 23, 21, 0),
                CoreTime::from_date(2006, 0, 3, 21, 23, 22, 0),
                1,
                57 * 3_600 + 1,
            ),
            (
                CoreTime::from_date(0, 0, 0, 21, 23, 24, 0),
                CoreTime::from_date(0, 0, 0, 11, 23, 22, 0),
                1,
                10 * 3_600 + 2,
            ),
            (
                CoreTime::from_date(0, 0, 0, 1, 2, 3, 0),
                CoreTime::from_date(0, 0, 0, 5, 2, 0, 0),
                -1,
                6 * 3_600 + 4 * 60 + 3,
            ),
        ];
        for (left, right, sign, expected_seconds) in rows {
            let difference = left.time_diff(right, sign);
            assert_eq!(difference.seconds, expected_seconds);
            assert_eq!(difference.microseconds, 0);
        }
    }

    #[test]
    fn test_timestamp_diff() {
        let start = CoreTime::from_date(2002, 5, 1, 0, 0, 0, 0);
        let end = CoreTime::from_date(2001, 1, 1, 0, 0, 0, 0);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Year), -1);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Quarter), -5);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Month), -16);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Day), -485);
        assert_eq!(
            start.timestamp_diff(end, TimestampInterval::Microsecond),
            -41_904_000_000_000
        );
    }

    #[test]
    fn test_get_date_from_daynr() {
        for (daynr, expected) in [
            (730_669, (2000, 7, 3)),
            (720_195, (1971, 10, 30)),
            (719_528, (1970, 1, 1)),
            (719_892, (1970, 12, 31)),
            (730_850, (2000, 12, 31)),
            (730_544, (2000, 2, 29)),
            (204_960, (561, 2, 28)),
            (0, (0, 0, 0)),
            (32, (0, 0, 0)),
            (366, (1, 1, 1)),
            (744_729, (2038, 12, 31)),
            (3_652_424, (9999, 12, 31)),
        ] {
            assert_eq!(get_date_from_daynr(daynr), expected);
        }
    }

    #[test]
    fn test_mix_date_and_time() {
        let rows = [
            (
                CoreTime::from_date(1896, 3, 4, 0, 0, 0, 0),
                44_604_000_005_000,
                CoreTime::from_date(1896, 3, 4, 12, 23, 24, 5),
            ),
            (
                CoreTime::from_date(1896, 3, 4, 0, 0, 0, 0),
                87_804_000_005_000,
                CoreTime::from_date(1896, 3, 5, 0, 23, 24, 5),
            ),
            (
                CoreTime::from_date(2016, 12, 31, 0, 0, 0, 0),
                86_400_000_000_000,
                CoreTime::from_date(2017, 1, 1, 0, 0, 0, 0),
            ),
            (
                CoreTime::from_date(2016, 12, 0, 0, 0, 0, 0),
                86_400_000_000_000,
                CoreTime::from_date(2016, 12, 1, 0, 0, 0, 0),
            ),
            (
                CoreTime::from_date(2017, 1, 12, 3, 23, 15, 0),
                -8_470_000_000_000,
                CoreTime::from_date(2017, 1, 12, 1, 2, 5, 0),
            ),
        ];
        for (mut date, duration, expected) in rows {
            date.mix_duration(duration);
            assert_eq!(date, expected);
        }
    }

    #[test]
    fn test_is_leap_year_and_get_last_day() {
        for (year, expected) in [
            (1960, true),
            (1963, false),
            (2008, true),
            (2017, false),
            (1988, true),
            (2000, true),
            (1992, true),
            (2024, true),
            (2016, true),
            (2015, false),
            (2014, false),
            (2001, false),
            (1989, false),
        ] {
            assert_eq!(is_leap_year(year), expected);
        }
        assert_eq!(get_last_day(2000, 1), 31);
        assert_eq!(get_last_day(2000, 2), 29);
        assert_eq!(get_last_day(2000, 4), 30);
        assert_eq!(get_last_day(1900, 2), 28);
        assert_eq!(get_last_day(1996, 2), 29);
    }

    #[test]
    fn test_weekday_normalizes_invalid_calendar_dates() {
        for (time, expected) in [
            (
                CoreTime::from_date(2019, 1, 1, 0, 0, 0, 0),
                Weekday::Tuesday,
            ),
            (
                CoreTime::from_date(2019, 2, 31, 0, 0, 0, 0),
                Weekday::Sunday,
            ),
            (
                CoreTime::from_date(2019, 4, 31, 0, 0, 0, 0),
                Weekday::Wednesday,
            ),
        ] {
            assert_eq!(time.weekday(), expected);
            assert_eq!(time.weekday().to_string(), expected.to_string());
        }
    }

    #[test]
    fn test_add_date_source_boundaries_and_month_end() {
        let january_end = CoreTime::from_date(2018, 1, 31, 1, 2, 3, 4);
        assert_eq!(
            january_end.add_date(0, 1, 0).unwrap(),
            CoreTime::from_date(2018, 2, 28, 1, 2, 3, 4)
        );
        assert_eq!(
            january_end.add_date(0, 1, 12).unwrap(),
            CoreTime::from_date(2018, 3, 15, 1, 2, 3, 4)
        );

        let source = CoreTime::from_date(2000, 1, 1, 0, 0, 0, 0);
        for (years, months, days, expected_year) in [
            (1, 1, 0, 2001),
            (2, 1, 12, 2002),
            (3, 1, 12, 2003),
            (4, 2, 24, 2004),
            (7_999, 1, 1, 9999),
            (-2_000, 1, 1, 0),
        ] {
            assert_eq!(
                source.add_date(years, months, days).unwrap().year(),
                expected_year
            );
        }
        for (years, months, days) in [
            (8_000, 1, 1),
            (10_001 * 365, 1, 1),
            (1, 10_001 * 36, 1),
            (1, 1, 10_001 * 365),
            (-2_001, 1, 1),
            (-10_001 * 365, 1, 1),
            (1, -10_001 * 36, 1),
            (1, 1, -10_001 * 365),
        ] {
            assert_eq!(source.add_date(years, months, days), Err(DateAddError));
        }
    }

    #[test]
    fn test_fix_days_source_rows() {
        for (years, months, days, original, expected) in [
            (
                2_000,
                1,
                0,
                CoreTime::from_date(2000, 1, 31, 0, 0, 0, 0),
                -2,
            ),
            (
                2_000,
                1,
                12,
                CoreTime::from_date(2000, 1, 31, 0, 0, 0, 0),
                0,
            ),
            (
                2_000,
                1,
                12,
                CoreTime::from_date(1999, 12, 31, 0, 0, 0, 0),
                0,
            ),
            (
                2_000,
                2,
                24,
                CoreTime::from_date(2000, 2, 10, 0, 0, 0, 0),
                0,
            ),
            (2_019, 4, 5, CoreTime::from_date(2019, 4, 1, 1, 2, 3, 0), 0),
        ] {
            assert_eq!(fix_days(years, months, days, original), expected);
        }
    }

    #[test]
    fn test_adjusted_datetime_source_dst_rows() {
        for (zone, input, expected_date, expected_clock, expected_microsecond, expected_offset) in [
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 1, 59, 59, 997),
                (2020, 10, 4),
                (1, 59, 59),
                997,
                10 * 3_600 + 30 * 60,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 0, 0, 0),
                (2020, 10, 4),
                (2, 30, 0),
                0,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 15, 0, 0),
                (2020, 10, 4),
                (2, 30, 0),
                0,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 29, 59, 999_999),
                (2020, 10, 4),
                (2, 30, 0),
                0,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 30, 0, 1),
                (2020, 10, 4),
                (2, 30, 0),
                1,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 6, 29, 3, 45, 0, 0),
                (2020, 6, 29),
                (3, 45, 0),
                0,
                10 * 3_600 + 30 * 60,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 4, 4, 1, 45, 0, 0),
                (2020, 4, 4),
                (1, 45, 0),
                0,
                11 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 3, 29, 3, 45, 0, 0),
                (2020, 3, 29),
                (4, 0, 0),
                0,
                3 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 3, 29, 3, 59, 59, 456_789),
                (2020, 3, 29),
                (4, 0, 0),
                0,
                3 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 3, 29, 4, 0, 1, 130_000),
                (2020, 3, 29),
                (4, 0, 1),
                130_000,
                3 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 10, 25, 3, 45, 0, 0),
                (2020, 10, 25),
                (3, 45, 0),
                0,
                2 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 6, 29, 3, 45, 0, 0),
                (2020, 6, 29),
                (3, 45, 0),
                0,
                3 * 3_600,
            ),
            (
                "Europe/Amsterdam",
                CoreTime::from_date(2020, 3, 29, 2, 45, 0, 0),
                (2020, 3, 29),
                (3, 0, 0),
                0,
                2 * 3_600,
            ),
            (
                "Europe/Amsterdam",
                CoreTime::from_date(2020, 10, 25, 2, 35, 0, 0),
                (2020, 10, 25),
                (2, 35, 0),
                0,
                3_600,
            ),
        ] {
            let timezone: chrono_tz::Tz = zone.parse().unwrap();
            let value = input.adjusted_datetime(&timezone).unwrap();
            assert_eq!(
                (value.year(), value.month(), value.day()),
                expected_date,
                "{zone} {input}"
            );
            assert_eq!(
                (value.hour(), value.minute(), value.second()),
                expected_clock,
                "{zone} {input}"
            );
            assert_eq!(value.nanosecond() / 1_000, expected_microsecond);
            assert_eq!(value.offset().fix().local_minus_utc(), expected_offset);
        }

        let timezone: chrono_tz::Tz = "Europe/Amsterdam".parse().unwrap();
        let gap = CoreTime::from_date(2020, 3, 29, 2, 45, 0, 0);
        assert_eq!(
            gap.to_datetime(&timezone),
            Err(TimeConversionError::NonexistentLocalTime)
        );
        assert_eq!(
            CoreTime::from_date(2020, 2, 31, 2, 35, 0, 0).adjusted_datetime(&chrono_tz::UTC),
            Err(TimeConversionError::InvalidCalendar)
        );
    }
}
