// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

/// Render a non-negative duration with client-go's `util.FormatDuration`
/// precision policy. Rust's standard duration formatters use different
/// rounding rules, so source-compatible diagnostics share this helper.
pub fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos <= 1_000 {
        return go_duration_string(nanos);
    }

    let unit = if nanos >= 1_000_000_000 {
        1_000_000_000_u128
    } else if nanos >= 1_000_000 {
        1_000_000_u128
    } else {
        1_000_u128
    };
    let integer = nanos / unit;
    let precision = if integer < 10 { 100 } else { 10 };
    let scaled = ((nanos % unit) * precision + unit / 2) / unit;
    let rounded = integer * precision + scaled;
    go_duration_string(rounded * unit / precision)
}

fn go_duration_string(nanos: u128) -> String {
    if nanos == 0 {
        return "0s".to_owned();
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format_decimal(nanos, 1_000, 3, "µs");
    }
    if nanos < 1_000_000_000 {
        return format_decimal(nanos, 1_000_000, 6, "ms");
    }

    let total_seconds = nanos / 1_000_000_000;
    let fractional_nanos = nanos % 1_000_000_000;
    let seconds = total_seconds % 60;
    let total_minutes = total_seconds / 60;
    let minutes = total_minutes % 60;
    let hours = total_minutes / 60;
    let seconds = format_decimal(
        seconds * 1_000_000_000 + fractional_nanos,
        1_000_000_000,
        9,
        "s",
    );
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}")
    } else if total_minutes > 0 {
        format!("{total_minutes}m{seconds}")
    } else {
        seconds
    }
}

fn format_decimal(value: u128, unit: u128, width: usize, suffix: &str) -> String {
    let whole = value / unit;
    let remainder = value % unit;
    if remainder == 0 {
        return format!("{whole}{suffix}");
    }
    let fraction = format!("{remainder:0width$}");
    format!("{whole}.{}{suffix}", fraction.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_client_go_precision_boundaries() {
        for (duration, expected) in [
            (Duration::ZERO, "0s"),
            (Duration::from_nanos(999), "999ns"),
            (Duration::from_nanos(1_000), "1µs"),
            (Duration::from_nanos(100_450), "100.5µs"),
            (Duration::from_nanos(9_412_345), "9.41ms"),
            (Duration::from_nanos(10_412_345), "10.4ms"),
            (Duration::from_millis(1_001), "1s"),
            (Duration::from_millis(2_200), "2.2s"),
        ] {
            assert_eq!(format_duration(duration), expected);
        }
    }

    #[test]
    fn source_uncovered_preserves_go_composite_minute_and_hour_units() {
        for (duration, expected) in [
            (Duration::from_secs(60), "1m0s"),
            (Duration::from_millis(61_234), "1m1.2s"),
            (Duration::from_secs(3_600), "1h0m0s"),
            (Duration::from_millis(3_661_234), "1h1m1.2s"),
        ] {
            assert_eq!(format_duration(duration), expected);
        }
    }
}
