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

//! Small discrete-range enumeration from `pkg/statistics/scalar.go`.

use chrono_tz::UTC;
use tidb_datatype::{CoreTime, Datum, MySqlDuration, TimeType};

/// Go `maxNumStep`: ten or more returned values are not enumerated.
pub const MAX_NUM_STEP: i64 = 10;

fn rounded_to_step(value: i64, step: i64) -> i64 {
    let quotient = value / step;
    let remainder = value % step;
    if remainder.unsigned_abs().saturating_mul(2) >= step as u64 {
        quotient.wrapping_add(remainder.signum()).wrapping_mul(step)
    } else {
        quotient.wrapping_mul(step)
    }
}

/// Go `EnumRangeValues`. `None` is Go `nil`; `Some(vec![])` preserves the
/// distinct empty-but-non-nil integer result reached after exclusions.
#[must_use]
pub fn enum_range_values(
    low: &Datum,
    high: &Datum,
    low_exclude: bool,
    high_exclude: bool,
) -> Option<Vec<Datum>> {
    let exclude = i64::from(low_exclude) + i64::from(high_exclude);
    match (low, high) {
        (Datum::Int(low), Datum::Int(high)) => {
            if *low <= 0 && *high >= 0 && (*low < -MAX_NUM_STEP || *high > MAX_NUM_STEP) {
                return None;
            }
            let difference = high.wrapping_sub(*low);
            if difference > MAX_NUM_STEP {
                return None;
            }
            let remaining = difference.wrapping_add(1).wrapping_sub(exclude);
            if !(0..MAX_NUM_STEP).contains(&remaining) {
                return None;
            }
            let start = low.wrapping_add(i64::from(low_exclude));
            Some(
                (0..remaining)
                    .map(|offset| Datum::Int(start.wrapping_add(offset)))
                    .collect(),
            )
        }
        (Datum::UInt(low), Datum::UInt(high)) => {
            let difference = high.wrapping_sub(*low);
            if difference >= (MAX_NUM_STEP + 1) as u64 {
                return None;
            }
            let remaining = difference.wrapping_add(1).wrapping_sub(exclude as u64);
            if remaining >= MAX_NUM_STEP as u64 {
                return None;
            }
            let start = low.wrapping_add(u64::from(low_exclude));
            Some(
                (0..remaining)
                    .map(|offset| Datum::UInt(start.wrapping_add(offset)))
                    .collect(),
            )
        }
        (Datum::Duration(low), Datum::Duration(high)) => {
            let fsp = low.fsp().max(high.fsp());
            let step = 10_i64
                .pow(u32::try_from(6 - fsp).expect("duration FSP does not exceed MaxFsp"))
                * 1_000;
            let low_nanos = rounded_to_step(low.nanoseconds(), step);
            let remaining = high
                .nanoseconds()
                .wrapping_sub(low_nanos)
                .wrapping_div(step)
                .wrapping_add(1)
                .wrapping_sub(exclude);
            if remaining <= 0 || remaining >= MAX_NUM_STEP {
                return None;
            }
            let start = low_nanos.wrapping_add(i64::from(low_exclude).wrapping_mul(step));
            let mut values = Vec::with_capacity(remaining as usize);
            for offset in 0..remaining {
                let duration = MySqlDuration::from_raw_parts(
                    start.wrapping_add(offset.wrapping_mul(step)),
                    fsp,
                );
                values.push(Datum::Duration(duration));
            }
            Some(values)
        }
        (Datum::Time(low), Datum::Time(high)) => {
            if low.kind() != high.kind() {
                return None;
            }
            let fsp = low.fsp().max(high.fsp());
            let (low, step) = if low.kind() == TimeType::Date {
                let core = low.core_time();
                let mut date = *low;
                date.set_core_time(CoreTime::from_date(
                    core.year() as u16,
                    core.month(),
                    core.day(),
                    0,
                    0,
                    0,
                    0,
                ));
                (date, 86_400_000_000_000_i64)
            } else {
                (
                    low.round_frac(i64::from(fsp), &UTC).ok()?,
                    10_i64.pow(u32::from(6 - fsp)) * 1_000,
                )
            };
            let difference = high.sub(low, &UTC).ok()?.nanoseconds();
            let remaining = difference
                .wrapping_div(step)
                .wrapping_add(1)
                .wrapping_sub(exclude);
            if remaining <= 0 || remaining >= MAX_NUM_STEP {
                return None;
            }
            let initial_step = i64::from(low_exclude).wrapping_mul(step);
            let mut values = Vec::with_capacity(remaining as usize);
            for offset in 0..remaining {
                let duration = MySqlDuration::from_nanoseconds(
                    initial_step.wrapping_add(offset.wrapping_mul(step)),
                    i64::from(fsp),
                )
                .ok()?;
                values.push(Datum::Time(low.add_duration(duration).ok()?));
            }
            Some(values)
        }
        _ => None,
    }
}

#[cfg(test)]
mod enum_range_values_tests {
    use super::{enum_range_values, MAX_NUM_STEP};
    use tidb_datatype::{Datum, MySqlDuration, Time, TimeType};

    fn ints(range: std::ops::Range<i64>) -> Vec<Datum> {
        range.map(Datum::Int).collect()
    }

    /// Go `EnumRangeValues`'s plain integer arm: every value from low to
    /// high, both ends inclusive by default.
    #[test]
    fn integers_enumerate_both_ends_inclusive() {
        assert_eq!(
            enum_range_values(&Datum::Int(1), &Datum::Int(5), false, false),
            Some(ints(1..6))
        );
    }

    /// Each `EXCLUDED` bound drops exactly its own endpoint; excluding both
    /// ends of a two-value range leaves the EMPTY-but-present result Go's
    /// `make([]types.Datum, 0, 0)` produces (never `nil`).
    #[test]
    fn exclusions_drop_endpoints_and_can_leave_the_empty_result() {
        assert_eq!(
            enum_range_values(&Datum::Int(1), &Datum::Int(5), true, false),
            Some(ints(2..6))
        );
        assert_eq!(
            enum_range_values(&Datum::Int(1), &Datum::Int(5), false, true),
            Some(ints(1..5))
        );
        assert_eq!(
            enum_range_values(&Datum::Int(2), &Datum::Int(3), true, true),
            Some(Vec::new()),
            "remaining == 0 keeps the empty non-nil distinction"
        );
        assert_eq!(
            enum_range_values(&Datum::Int(2), &Datum::Int(2), true, true),
            None,
            "remaining < 0 is Go nil"
        );
    }

    /// A span whose post-exclusion remainder reaches `maxNumStep` refuses to
    /// enumerate: Go's two gates are `difference >= maxNumStep+1` (raw span)
    /// and `remaining >= maxNumStep` after `+1 - exclusions`, so at most
    /// NINE values ever come back.
    #[test]
    fn spans_past_max_num_step_return_nil() {
        assert_eq!(
            enum_range_values(&Datum::Int(0), &Datum::Int(11), false, false),
            None,
            "raw difference 11 exceeds maxNumStep+1"
        );
        assert_eq!(
            enum_range_values(&Datum::Int(0), &Datum::Int(10), false, false),
            None,
            "difference 10 leaves remaining 11 >= maxNumStep"
        );
        assert_eq!(
            enum_range_values(&Datum::Int(0), &Datum::Int(9), false, false),
            None,
            "difference 9 leaves remaining 10 >= maxNumStep"
        );
        assert_eq!(
            enum_range_values(&Datum::Int(0), &Datum::Int(8), false, false),
            Some(ints(0..9)),
            "difference 8 leaves remaining 9 < maxNumStep"
        );
    }

    /// Go's sign-crossing pre-check: a range spanning zero whose either side
    /// escapes `[-maxNumStep, maxNumStep]` refuses; the same width that stays
    /// inside both bounds enumerates.
    #[test]
    fn sign_crossing_ranges_enforce_the_zero_anchored_bounds() {
        assert_eq!(
            enum_range_values(&Datum::Int(-11), &Datum::Int(11), false, false),
            None,
            "crosses zero AND escapes the anchor bounds"
        );
        assert_eq!(
            enum_range_values(&Datum::Int(-5), &Datum::Int(5), false, false),
            None,
            "crosses zero inside the anchor bounds but remaining 11 >= maxNumStep"
        );
        assert_eq!(
            enum_range_values(&Datum::Int(-4), &Datum::Int(4), false, false),
            Some(ints(-4..5)),
            "crosses zero AND enumerates: remaining 9 < maxNumStep"
        );
    }

    /// The unsigned arm has no sign-crossing pre-check (Go's is
    /// `lowVal <= 0`, unreachable for uint64) and uses unsigned arithmetic.
    #[test]
    fn unsigned_ranges_enumerate_like_signed_ones() {
        assert_eq!(
            enum_range_values(&Datum::UInt(1), &Datum::UInt(3), false, false),
            Some(vec![Datum::UInt(1), Datum::UInt(2), Datum::UInt(3)])
        );
        assert_eq!(
            enum_range_values(&Datum::UInt(0), &Datum::UInt(11), false, false),
            None
        );
    }

    /// Kind disagreement refuses, as Go's opening `low.Kind() != high.Kind()`.
    #[test]
    fn mismatched_kinds_return_nil() {
        assert_eq!(
            enum_range_values(&Datum::Int(1), &Datum::UInt(3), false, false),
            None
        );
    }

    /// The duration arm rounds the low bound to the FSP step and walks the
    /// step size `10^(MaxFsp-fsp)` microseconds.
    #[test]
    fn durations_walk_their_fsp_step() {
        let low = MySqlDuration::from_raw_parts(0, 0);
        let high = MySqlDuration::from_raw_parts(5 * 1_000_000_000, 0);
        assert_eq!(
            enum_range_values(&Datum::Duration(low), &Datum::Duration(high), false, false),
            Some(
                (0..=5_i64)
                    .map(|seconds| Datum::Duration(MySqlDuration::from_raw_parts(
                        seconds * 1_000_000_000,
                        0
                    )))
                    .collect::<Vec<_>>()
            ),
            "ten-second... one-second steps over [0s, 5s] at fsp 0"
        );
        assert_eq!(MAX_NUM_STEP, 10, "Go `maxNumStep`");
    }

    /// The time arm normalizes a DATE low bound to midnight and refuses
    /// mismatched time kinds.
    #[test]
    fn date_ranges_normalize_low_to_midnight_and_refuse_kind_mixes() {
        let day = |y: i32, m: i32, d: i32| {
            Time::from_date_checked(y, m, d, 0, 0, 0, 0, TimeType::Date, 0).expect("a valid date")
        };
        let low = day(2020, 1, 1);
        let high = day(2020, 1, 3);
        assert_eq!(
            enum_range_values(&Datum::Time(low), &Datum::Time(high), false, false)
                .expect("dates enumerate")
                .len(),
            3,
            "midnight-normalized [Jan 1, Jan 3] yields the three dates"
        );
        let datetime = Time::from_date_checked(2020, 1, 3, 0, 0, 0, 0, TimeType::DateTime, 0)
            .expect("a valid datetime");
        assert_eq!(
            enum_range_values(&Datum::Time(low), &Datum::Time(datetime), false, false),
            None,
            "DATE against DATETIME is a kind mismatch"
        );
    }
}
