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
            if difference >= MAX_NUM_STEP + 1 {
                return None;
            }
            let remaining = difference.wrapping_add(1).wrapping_sub(exclude);
            if remaining >= MAX_NUM_STEP || remaining < 0 {
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
            let step = 10_i64.pow(u32::from(6 - fsp)) * 1_000;
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
                let duration = MySqlDuration::from_nanoseconds(
                    start.wrapping_add(offset.wrapping_mul(step)),
                    i64::from(fsp),
                )
                .ok()?;
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
