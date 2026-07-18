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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Stateless information-function builtins.  This independent family keeps
//! scalar presentation functions out of session-bound information functions
//! such as `DATABASE()` and `CURRENT_USER()`.

use crate::ops::to_f64_with_mysql_string;
use crate::{Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals) {
        ("FORMAT_BYTES", [value]) => Some(format_bytes(value)),
        ("FORMAT_NANO_TIME", [value]) => Some(format_nano_time(value)),
        _ => None,
    }
}

/// `FORMAT_BYTES(value)`, ported from `builtinFormatBytesSig.evalString` and
/// `GetFormatBytes` in `pkg/expression/builtin_info.go` / `util.go`.
fn format_bytes(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = real_arg(value) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format_scaled(
        value,
        &[
            (1_u64 << 60, "EiB"),
            (1_u64 << 50, "PiB"),
            (1_u64 << 40, "TiB"),
            (1_u64 << 30, "GiB"),
            (1_u64 << 20, "MiB"),
            (1_u64 << 10, "KiB"),
        ],
        "bytes",
    )))
}

/// `FORMAT_NANO_TIME(value)`, ported from `builtinFormatNanoTimeSig` and
/// `GetFormatNanoTime` in `pkg/expression/builtin_info.go` / `util.go`.
/// Despite the similarly named MySQL documentation function, TiDB's SQL name
/// is `FORMAT_NANO_TIME` and its input unit is nanoseconds.
fn format_nano_time(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = real_arg(value) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format_scaled(
        value,
        &[
            (86_400_000_000_000, "d"),
            (3_600_000_000_000, "h"),
            (60_000_000_000, "min"),
            (1_000_000_000, "s"),
            (1_000_000, "ms"),
            (1_000, "us"),
        ],
        "ns",
    )))
}

/// The function classes build their sole argument as ETReal.  Reuse the
/// shared MySQL numeric-prefix coercion so strings, decimals, and integers
/// reach the formatter exactly as they do in TiDB; `NULL` alone propagates.
fn real_arg(value: &Datum) -> Option<f64> {
    match value {
        Datum::Null => None,
        _ => Some(to_f64_with_mysql_string(value)),
    }
}

/// Shared structural port of `GetFormatBytes` and `GetFormatNanoTime`.
fn format_scaled(value: f64, scales: &[(u64, &str)], base_unit: &str) -> String {
    let magnitude = value.abs();
    let Some(&(divisor, unit)) = scales
        .iter()
        .find(|(divisor, _)| magnitude >= *divisor as f64)
    else {
        return format!("{} {base_unit}", fixed(value, 0));
    };
    let scaled = value / divisor as f64;
    let number = if scaled.abs() >= 100_000.0 {
        scientific(scaled)
    } else {
        fixed(scaled, 2)
    };
    format!("{number} {unit}")
}

/// Go's `strconv.FormatFloat(value, 'f', precision, 64)` uses positive zero
/// for `-0`, as confirmed with `FORMAT_BYTES(-0.0)` and
/// `FORMAT_NANO_TIME(-0.0)` through `goeval`.
fn fixed(value: f64, precision: usize) -> String {
    let value = if value == 0.0 { 0.0 } else { value };
    format!("{value:.precision$}")
}

/// Go's `strconv.FormatFloat(value, 'e', 2, 64)` always emits an exponent
/// sign and pads its absolute exponent to at least two digits (`e+08`). Rust
/// supplies the correctly rounded mantissa, then this normalizes only that
/// spelling difference.
fn scientific(value: f64) -> String {
    let rendered = format!("{value:.2e}");
    let (mantissa, exponent) = rendered
        .split_once('e')
        .expect("Rust scientific format always contains an exponent");
    let exponent = exponent
        .parse::<i32>()
        .expect("Rust scientific exponent is a signed integer");
    format!("{mantissa}e{exponent:+03}")
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::Datum;

    fn call(name: &str, value: Datum) -> Datum {
        dispatch(name, &[value])
            .expect("name/arity should dispatch")
            .expect("formatting must be total over finite ETReal values")
    }

    #[test]
    fn format_bytes_matches_go_test_vectors_and_thresholds() {
        // Exact `TestFormatBytes` vectors in
        // pkg/expression/builtin_info_test.go.
        let cases = [
            (0.0, "0 bytes"),
            (2048.0, "2.00 KiB"),
            (75_295_729.0, "71.81 MiB"),
            (5_287_242_702.0, "4.92 GiB"),
            (5_039_757_204_245.0, "4.58 TiB"),
            (890_250_274_520_475_525.0, "790.70 PiB"),
            (18_446_644_073_709_551_615.0, "16.00 EiB"),
            (287_952_852_482_075_252_752_429_875.0, "2.50e+08 EiB"),
            (-18_446_644_073_709_551_615.0, "-16.00 EiB"),
        ];
        for (input, want) in cases {
            assert_eq!(
                call("FORMAT_BYTES", Datum::Real(input))
                    .sql_string()
                    .unwrap(),
                want
            );
        }
        assert_eq!(call("FORMAT_BYTES", Datum::Null), Datum::Null);
        assert_eq!(
            call("FORMAT_BYTES", Datum::new_string("1e9999".to_string()))
                .sql_string()
                .unwrap(),
            "1.56e+290 EiB",
            "ETReal overflow clamps before formatting"
        );
        assert_eq!(
            call("FORMAT_BYTES", Datum::Real(-0.0))
                .sql_string()
                .unwrap(),
            "0 bytes"
        );
    }

    #[test]
    fn format_nano_time_matches_go_test_vectors_and_thresholds() {
        // Exact `TestFormatNanoTime` vectors in
        // pkg/expression/builtin_info_test.go.
        let cases = [
            (0.0, "0 ns"),
            (2000.0, "2.00 us"),
            (898_787_877.0, "898.79 ms"),
            (9_999_999_991.0, "10.00 s"),
            (898_787_877_424.0, "14.98 min"),
            (5_827_527_520_021.0, "1.62 h"),
            (42_566_623_663_736_353.0, "492.67 d"),
            (4_827_524_825_702_572_425_242_552.0, "5.59e+10 d"),
            (-9_999_999_991.0, "-10.00 s"),
        ];
        for (input, want) in cases {
            assert_eq!(
                call("FORMAT_NANO_TIME", Datum::Real(input))
                    .sql_string()
                    .unwrap(),
                want
            );
        }
        assert_eq!(call("FORMAT_NANO_TIME", Datum::Null), Datum::Null);
        assert_eq!(
            call("FORMAT_NANO_TIME", Datum::new_string("1e9999".to_string()))
                .sql_string()
                .unwrap(),
            "2.08e+294 d"
        );
        assert_eq!(
            call("FORMAT_NANO_TIME", Datum::Real(-0.0))
                .sql_string()
                .unwrap(),
            "0 ns"
        );
    }
}
