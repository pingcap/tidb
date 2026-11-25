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

//! Go's `time.Duration.String()`, reproduced for the `SHOW`-style rendering
//! of durations in model metadata (e.g. resource-group runaway settings).
//!
//! `d` is a duration in nanoseconds.

/// Go `time.Duration.String()`: renders `d` nanoseconds the way Go does
/// (`"0s"`, `"1.5s"`, `"1m30s"`, `"1h1m1s"`, sub-second `ns`/`µs`/`ms`).
#[must_use]
pub fn format_go_duration(d: i64) -> String {
    format_duration_into(d, &mut [0; 32]).to_owned()
}

fn format_duration_into(d: i64, buffer: &mut [u8; 32]) -> &str {
    if d == 0 {
        return "0s";
    }
    // Go writes the fractional digits from the end of the buffer so it can
    // skip trailing zeroes. Thirty-two bytes hold even i64::MIN.
    let mut start = buffer.len();
    let magnitude = d.unsigned_abs();
    if magnitude < 1_000_000_000 {
        let (unit, precision) = if magnitude < 1_000 {
            ("ns", 0)
        } else if magnitude < 1_000_000 {
            ("µs", 3)
        } else {
            ("ms", 6)
        };
        start -= unit.len();
        buffer[start..].copy_from_slice(unit.as_bytes());
        let integer = prepend_fraction(buffer, &mut start, magnitude, precision);
        prepend_integer(buffer, &mut start, integer);
    } else {
        prepend_byte(buffer, &mut start, b's');
        let seconds = prepend_fraction(buffer, &mut start, magnitude, 9);
        prepend_integer(buffer, &mut start, seconds % 60);
        let minutes = seconds / 60;
        if minutes != 0 {
            prepend_byte(buffer, &mut start, b'm');
            prepend_integer(buffer, &mut start, minutes % 60);
            let hours = minutes / 60;
            if hours != 0 {
                prepend_byte(buffer, &mut start, b'h');
                prepend_integer(buffer, &mut start, hours);
            }
        }
    }
    if d < 0 {
        prepend_byte(buffer, &mut start, b'-');
    }
    std::str::from_utf8(&buffer[start..]).expect("duration digits and units are UTF-8")
}

fn prepend_byte(buffer: &mut [u8; 32], start: &mut usize, byte: u8) {
    *start -= 1;
    buffer[*start] = byte;
}

fn prepend_integer(buffer: &mut [u8; 32], start: &mut usize, mut value: u64) {
    loop {
        prepend_byte(buffer, start, b'0' + (value % 10) as u8);
        value /= 10;
        if value == 0 {
            break;
        }
    }
}

fn prepend_fraction(
    buffer: &mut [u8; 32],
    start: &mut usize,
    mut value: u64,
    precision: usize,
) -> u64 {
    let end = *start;
    for _ in 0..precision {
        let digit = (value % 10) as u8;
        if digit != 0 || *start != end {
            prepend_byte(buffer, start, b'0' + digit);
        }
        value /= 10;
    }
    if *start != end {
        prepend_byte(buffer, start, b'.');
    }
    value
}

/// Formats `ms` milliseconds as Go does for `time.Duration(ms)*time.Millisecond`.
#[must_use]
pub fn format_go_duration_ms(ms: i64) -> String {
    format_go_duration(ms * 1_000_000)
}

/// Go `execdetails.FormatDuration`: prune elapsed-time precision for EXPLAIN.
/// Shared by the physical executor and the detailed runtime-stat collector.
#[must_use]
pub fn format_explain_duration(ns: i64) -> String {
    format_go_duration(prune_explain_duration(ns))
}

/// Appends the same pruned duration as [`format_explain_duration`].
pub fn append_explain_duration(output: &mut String, ns: i64) {
    output.push_str(format_duration_into(prune_explain_duration(ns), &mut [0; 32]));
}

#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    reason = "Go FormatDuration converts time.Duration through float64"
)]
fn prune_explain_duration(ns: i64) -> i64 {
    if ns <= 1_000 {
        return ns;
    }
    let unit = if ns >= 1_000_000_000 {
        1_000_000_000
    } else if ns >= 1_000_000 {
        1_000_000
    } else {
        1_000
    };
    let integer = (ns / unit) * unit;
    let mut decimal = (ns % unit) as f64 / unit as f64;
    if ns < 10 * unit {
        decimal = (decimal * 100.0).round() / 100.0;
    } else {
        decimal = (decimal * 10.0).round() / 10.0;
    }
    integer.wrapping_add((decimal * unit as f64) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known outputs from Go's time.Duration.String().
    #[test]
    fn go_known_outputs() {
        assert_eq!(format_go_duration(0), "0s");
        assert_eq!(format_go_duration(1_000_000_000), "1s");
        assert_eq!(format_go_duration(1_500_000_000), "1.5s");
        assert_eq!(format_go_duration(90_000_000_000), "1m30s");
        assert_eq!(format_go_duration(3_661_000_000_000), "1h1m1s");
        assert_eq!(format_go_duration(500), "500ns");
        assert_eq!(format_go_duration(1_500), "1.5µs");
        assert_eq!(format_go_duration(1_500_000), "1.5ms");
        assert_eq!(format_go_duration(-2_000_000_000), "-2s");
        // A minute with no hour omits the hour segment.
        assert_eq!(format_go_duration(60_000_000_000), "1m0s");
    }

    #[test]
    fn ms_helper() {
        assert_eq!(format_go_duration_ms(5000), "5s");
        assert_eq!(format_go_duration_ms(1500), "1.5s");
        assert_eq!(format_go_duration_ms(0), "0s");
    }

    #[test]
    fn pruned_duration_wraps_like_go_at_the_signed_limit() {
        assert_eq!(
            format_go_duration(i64::MIN),
            "-2562047h47m16.854775808s"
        );
        let mut output = String::from("time:");
        append_explain_duration(&mut output, i64::MAX);
        assert_eq!(output, "time:-2562047h47m16.809551616s");
    }
}
