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

//! `boundary:` `github.com/robfig/cron/v3`, which `pkg/timer/api/timer.go`
//! uses for exactly two calls: `cron.ParseStandard(expr)` and
//! `cron.Schedule.Next(t)`.
//!
//! No cron crate is present in this workspace's lockfile or in the local
//! registry cache, so the offline build cannot add one. This module
//! reimplements the subset of robfig/cron/v3 that `ParseStandard` reaches,
//! transcreated from that library's `parser.go` and `spec.go`:
//!
//! Supported — the five standard fields (minute, hour, day-of-month, month,
//! day-of-week) with `*`, `?`, single values, `a-b` ranges, `a-b/n` and `*/n`
//! steps, comma lists, three-letter month names (`JAN`..`DEC`) and day names
//! (`SUN`..`SAT`); the descriptors `@yearly`, `@annually`, `@monthly`,
//! `@weekly`, `@daily`, `@midnight`, `@hourly` and `@every <duration>`; the
//! "day-of-month OR day-of-week when both are restricted, AND otherwise" rule;
//! and `Next`'s five-year search horizon that returns the zero time when no
//! occurrence exists (which is what `"* * 30 2 *"` pins).
//!
//! Not supported — robfig's optional-second and optional-year field sets
//! (unreachable through `ParseStandard`), and the `TZ=`/`CRON_TZ=` prefix.
//! `ParseStandard` accepts that prefix upstream; here it is rejected as a
//! malformed spec. TiDB never writes one: the timer's zone comes from
//! `TimerSpec.TimeZone`, applied by the caller before `Next`.
//!
//! Error strings follow robfig's wording so the `invalid cron expr '%s': ...`
//! wrapping in `NewCronPolicy` reads the same.

use crate::go_time::{GoTime, HOUR, MINUTE, SECOND};

/// Marks a field that was written as `*` or `?`, which the day-of-month /
/// day-of-week rule inspects (robfig's `starBit`).
const STAR_BIT: u64 = 1 << 63;

struct Bounds {
    min: u32,
    max: u32,
    names: &'static [&'static str],
}

const SECONDS: Bounds = Bounds {
    min: 0,
    max: 59,
    names: &[],
};
const MINUTES: Bounds = Bounds {
    min: 0,
    max: 59,
    names: &[],
};
const HOURS: Bounds = Bounds {
    min: 0,
    max: 23,
    names: &[],
};
const DOM: Bounds = Bounds {
    min: 1,
    max: 31,
    names: &[],
};
const MONTHS: Bounds = Bounds {
    min: 1,
    max: 12,
    names: &[
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ],
};
const DOW: Bounds = Bounds {
    min: 0,
    max: 6,
    names: &["sun", "mon", "tue", "wed", "thu", "fri", "sat"],
};

/// A parsed cron specification (robfig's `SpecSchedule`), or the constant delay
/// that `@every` produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Bit sets over each field.
    Spec {
        /// Seconds; always exactly `{0}` for `ParseStandard`.
        second: u64,
        /// Minutes 0..=59.
        minute: u64,
        /// Hours 0..=23.
        hour: u64,
        /// Days of month 1..=31.
        dom: u64,
        /// Months 1..=12.
        month: u64,
        /// Days of week 0..=6, Sunday first.
        dow: u64,
    },
    /// `@every <duration>`: a fixed delay in nanoseconds, at least one second.
    ConstantDelay(i64),
}

/// Transcreation of `cron.ParseStandard`.
pub fn parse_standard(spec: &str) -> Result<Schedule, String> {
    if spec.is_empty() {
        return Err("empty spec string".to_string());
    }

    if let Some(descriptor) = spec.strip_prefix('@') {
        return parse_descriptor(descriptor, spec);
    }

    let fields: Vec<&str> = spec.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(format!(
            "expected exactly 5 fields, found {}: {spec}",
            fields.len()
        ));
    }

    Ok(Schedule::Spec {
        // `ParseStandard` omits the second field, so robfig defaults it to "0".
        second: get_field("0", &SECONDS)?,
        minute: get_field(fields[0], &MINUTES)?,
        hour: get_field(fields[1], &HOURS)?,
        dom: get_field(fields[2], &DOM)?,
        month: get_field(fields[3], &MONTHS)?,
        dow: get_field(fields[4], &DOW)?,
    })
}

fn parse_descriptor(descriptor: &str, spec: &str) -> Result<Schedule, String> {
    let lowered = descriptor.to_ascii_lowercase();
    let expand = |text: &str| parse_standard(text);
    match lowered.as_str() {
        "yearly" | "annually" => expand("0 0 1 1 *"),
        "monthly" => expand("0 0 1 * *"),
        "weekly" => expand("0 0 * * 0"),
        "daily" | "midnight" => expand("0 0 * * *"),
        "hourly" => expand("0 * * * *"),
        _ => {
            if let Some(rest) = lowered.strip_prefix("every ") {
                let delay = parse_go_duration(rest.trim())
                    .map_err(|err| format!("failed to parse duration {rest}: {err}"))?;
                return Ok(Schedule::ConstantDelay(delay.max(SECOND)));
            }
            Err(format!("unrecognized descriptor: {spec}"))
        }
    }
}

/// robfig's `getField`: a comma-separated list of ranges, or-ed together.
fn get_field(field: &str, bounds: &Bounds) -> Result<u64, String> {
    let mut bits = 0_u64;
    for expression in field.split(',') {
        bits |= get_range(expression, bounds)?;
    }
    Ok(bits)
}

/// robfig's `getRange`: `*`, `?`, `n`, `a-b`, and any of those with `/step`.
fn get_range(expression: &str, bounds: &Bounds) -> Result<u64, String> {
    let (range_and_step, extra) = {
        let mut parts = expression.splitn(3, '/');
        let first = parts.next().unwrap_or_default();
        (first, parts.collect::<Vec<_>>())
    };
    if extra.len() > 1 {
        return Err(format!("too many slashes: {expression}"));
    }

    let single_digit_star = range_and_step == "*" || range_and_step == "?";
    let (start, end) = if single_digit_star {
        (bounds.min, bounds.max)
    } else {
        let mut halves = range_and_step.splitn(2, '-');
        let low = halves.next().unwrap_or_default();
        let start = parse_int_or_name(low, bounds)?;
        match halves.next() {
            None => {
                if extra.is_empty() {
                    (start, start)
                } else {
                    // "N/step" means "N through the maximum", stepping.
                    (start, bounds.max)
                }
            }
            Some(high) => (start, parse_int_or_name(high, bounds)?),
        }
    };

    if start > end {
        return Err(format!(
            "beginning of range ({start}) beyond end of range ({end}): {expression}"
        ));
    }

    let step = match extra.first() {
        None => 1,
        Some(text) => {
            let step = must_parse_int(text)?;
            if step == 0 {
                return Err(format!(
                    "step of range should be a positive number: {expression}"
                ));
            }
            step
        }
    };

    let mut bits = 0_u64;
    let mut value = start;
    while value <= end {
        bits |= 1 << value;
        value += step;
    }
    if single_digit_star {
        bits |= STAR_BIT;
    }
    Ok(bits)
}

fn parse_int_or_name(text: &str, bounds: &Bounds) -> Result<u32, String> {
    if !bounds.names.is_empty() {
        let lowered = text.to_ascii_lowercase();
        if let Some(index) = bounds.names.iter().position(|name| *name == lowered) {
            return Ok(index as u32 + bounds.min);
        }
    }
    let value = must_parse_int(text)?;
    if value < bounds.min {
        return Err(format!(
            "beginning of range ({value}) below minimum ({}): {text}",
            bounds.min
        ));
    }
    if value > bounds.max {
        return Err(format!(
            "end of range ({value}) above maximum ({}): {text}",
            bounds.max
        ));
    }
    Ok(value)
}

fn must_parse_int(text: &str) -> Result<u32, String> {
    let value: u32 = text
        .parse()
        .map_err(|_| format!("failed to parse int from {text}"))?;
    Ok(value)
}

/// Go's `time.ParseDuration`, reached only through `@every`. Supports the same
/// unit set (`ns`, `us`/`µs`, `ms`, `s`, `m`, `h`) with an optional sign and
/// fractional magnitudes.
fn parse_go_duration(text: &str) -> Result<i64, String> {
    let (negative, mut rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(format!("time: invalid duration {text:?}"));
    }

    let mut total = 0_f64;
    while !rest.is_empty() {
        let split = rest
            .find(|character: char| !(character.is_ascii_digit() || character == '.'))
            .ok_or_else(|| format!("time: missing unit in duration {text:?}"))?;
        if split == 0 {
            return Err(format!("time: invalid duration {text:?}"));
        }
        let magnitude: f64 = rest[..split]
            .parse()
            .map_err(|_| format!("time: invalid duration {text:?}"))?;
        rest = &rest[split..];
        let (scale, width) = if rest.starts_with("ns") {
            (1.0, 2)
        } else if rest.starts_with("us")
            || rest.starts_with("\u{b5}s")
            || rest.starts_with("\u{3bc}s")
        {
            (
                1e3,
                rest.char_indices().nth(1).map_or(2, |(index, _)| index) + 1,
            )
        } else if rest.starts_with("ms") {
            (1e6, 2)
        } else if rest.starts_with('s') {
            (1e9, 1)
        } else if rest.starts_with('m') {
            (6e10, 1)
        } else if rest.starts_with('h') {
            (3.6e12, 1)
        } else {
            return Err(format!("time: unknown unit in duration {text:?}"));
        };
        total += magnitude * scale;
        rest = &rest[width..];
    }

    let total = total as i64;
    Ok(if negative { -total } else { total })
}

impl Schedule {
    /// Transcreation of robfig's `SpecSchedule.Next` / `ConstantDelaySchedule.Next`.
    ///
    /// `ParseStandard` leaves the schedule's own location as `time.Local`, so
    /// the upstream `if s.Location != time.Local` re-homing never fires and all
    /// arithmetic happens in the argument's own zone — which is what this
    /// package relies on to evaluate a timer in its configured time zone.
    pub fn next(&self, after: &GoTime) -> GoTime {
        match self {
            Self::ConstantDelay(delay) => {
                after.add(delay - after.add(0).nanosecond() as i64 % SECOND)
            }
            Self::Spec {
                second,
                minute,
                hour,
                dom,
                month,
                dow,
            } => spec_next(*second, *minute, *hour, *dom, *month, *dow, after),
        }
    }
}

fn spec_next(
    second: u64,
    minute: u64,
    hour: u64,
    dom: u64,
    month: u64,
    dow: u64,
    after: &GoTime,
) -> GoTime {
    // Round up to the next whole second, as robfig does before searching.
    let mut time = after.add(SECOND - i64::from(after.nanosecond()));
    let mut added = false;
    let year_limit = time.year() + 5;

    'wrap: loop {
        if time.year() > year_limit {
            return GoTime::zero();
        }

        while (1 << time.month()) & month == 0 {
            if !added {
                added = true;
                time = GoTime::date(
                    time.year(),
                    time.month() as i32,
                    1,
                    0,
                    0,
                    0,
                    0,
                    time.location(),
                );
            }
            time = time.add_date(0, 1, 0);
            if time.month() == 1 {
                continue 'wrap;
            }
        }

        while !day_matches(dom, dow, &time) {
            if !added {
                added = true;
                time = GoTime::date(
                    time.year(),
                    time.month() as i32,
                    time.day() as i32,
                    0,
                    0,
                    0,
                    0,
                    time.location(),
                );
            }
            time = time.add_date(0, 0, 1);
            // A daylight-saving jump can leave the wall clock off midnight;
            // robfig nudges it back onto the day boundary.
            if time.hour() != 0 {
                time = if time.hour() > 12 {
                    time.add((24 - i64::from(time.hour())) * HOUR)
                } else {
                    time.add(-(i64::from(time.hour())) * HOUR)
                };
            }
            if time.day() == 1 {
                continue 'wrap;
            }
        }

        while (1 << time.hour()) & hour == 0 {
            if !added {
                added = true;
                time = GoTime::date(
                    time.year(),
                    time.month() as i32,
                    time.day() as i32,
                    time.hour() as i32,
                    0,
                    0,
                    0,
                    time.location(),
                );
            }
            time = time.add(HOUR);
            if time.hour() == 0 {
                continue 'wrap;
            }
        }

        while (1 << time.minute()) & minute == 0 {
            if !added {
                added = true;
                time = time.truncate(MINUTE);
            }
            time = time.add(MINUTE);
            if time.minute() == 0 {
                continue 'wrap;
            }
        }

        while (1 << time.second()) & second == 0 {
            if !added {
                added = true;
                time = time.truncate(SECOND);
            }
            time = time.add(SECOND);
            if time.second() == 0 {
                continue 'wrap;
            }
        }

        return time;
    }
}

/// robfig's `dayMatches`: when either day field is `*`/`?` the two are AND-ed,
/// otherwise a match on either one is enough.
fn day_matches(dom: u64, dow: u64, time: &GoTime) -> bool {
    let dom_match = (1 << time.day()) & dom > 0;
    let dow_match = (1 << time.weekday()) & dow > 0;
    if dom & STAR_BIT > 0 || dow & STAR_BIT > 0 {
        return dom_match && dow_match;
    }
    dom_match || dow_match
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptors_expand_to_specs() {
        assert_eq!(parse_standard("@hourly"), parse_standard("0 * * * *"));
        assert_eq!(parse_standard("@daily"), parse_standard("0 0 * * *"));
        assert_eq!(parse_standard("@midnight"), parse_standard("0 0 * * *"));
        assert_eq!(parse_standard("@weekly"), parse_standard("0 0 * * 0"));
        assert_eq!(parse_standard("@monthly"), parse_standard("0 0 1 * *"));
        assert_eq!(parse_standard("@yearly"), parse_standard("0 0 1 1 *"));
        assert_eq!(parse_standard("@annually"), parse_standard("0 0 1 1 *"));
        assert_eq!(
            parse_standard("@every 1h30m"),
            Ok(Schedule::ConstantDelay(90 * MINUTE))
        );
    }

    #[test]
    fn names_ranges_and_steps() {
        assert_eq!(parse_standard("0 0 * JAN *"), parse_standard("0 0 * 1 *"));
        assert_eq!(parse_standard("0 0 * * SUN"), parse_standard("0 0 * * 0"));
        assert_eq!(
            parse_standard("0 0-2 * * *"),
            parse_standard("0 0,1,2 * * *")
        );
        assert_eq!(
            parse_standard("0 0-6/2 * * *"),
            parse_standard("0 0,2,4,6 * * *")
        );
    }

    #[test]
    fn rejects_bad_specs() {
        assert_eq!(parse_standard(""), Err("empty spec string".to_string()));
        assert!(parse_standard("aaa")
            .unwrap_err()
            .starts_with("expected exactly 5 fields"));
        assert!(parse_standard("61 1 * * *")
            .unwrap_err()
            .starts_with("end of range (61)"));
        assert!(parse_standard("@nope").is_err());
    }
}
