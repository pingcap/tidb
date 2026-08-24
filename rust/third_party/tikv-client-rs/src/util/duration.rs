// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

/// Render a non-negative duration with client-go's `util.FormatDuration`
/// precision policy. Rust's standard duration formatters use different
/// rounding rules, so source-compatible diagnostics share this helper.
pub(crate) fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos <= 1_000 {
        return match nanos {
            0 => "0s".to_owned(),
            1_000 => "1µs".to_owned(),
            nanos => format!("{nanos}ns"),
        };
    }

    let (unit, suffix) = if nanos >= 1_000_000_000 {
        (1_000_000_000_u128, "s")
    } else if nanos >= 1_000_000 {
        (1_000_000_u128, "ms")
    } else {
        (1_000_u128, "µs")
    };
    let integer = nanos / unit;
    let precision = if integer < 10 { 100 } else { 10 };
    let scaled = ((nanos % unit) * precision + unit / 2) / unit;
    let rounded = integer * precision + scaled;
    let whole = rounded / precision;
    let fraction = rounded % precision;
    if fraction == 0 {
        format!("{whole}{suffix}")
    } else if precision == 100 && fraction % 10 == 0 {
        format!("{whole}.{}{suffix}", fraction / 10)
    } else if precision == 100 {
        format!("{whole}.{fraction:02}{suffix}")
    } else {
        format!("{whole}.{fraction}{suffix}")
    }
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
}
