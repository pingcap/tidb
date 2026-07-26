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
//! This duplicates the algorithm already ported in `tidb-config`; both should
//! later share one low-level Go-time crate. `d` is a duration in nanoseconds.

/// Go `time.Duration.String()`: renders `d` nanoseconds the way Go does
/// (`"0s"`, `"1.5s"`, `"1m30s"`, `"1h1m1s"`, sub-second `ns`/`µs`/`ms`).
#[must_use]
pub fn format_go_duration(d: i64) -> String {
    if d == 0 {
        return "0s".to_string();
    }
    let neg = d < 0;
    let u = d.unsigned_abs();
    let mut out = String::new();

    if u < 1_000_000_000 {
        // Sub-second: ns, µs, or ms with a trimmed fractional part.
        let (unit, prec): (&str, u32) = if u < 1_000 {
            ("ns", 0)
        } else if u < 1_000_000 {
            ("µs", 3)
        } else {
            ("ms", 6)
        };
        let scale = 10u64.pow(prec);
        let int = u / scale;
        let frac = u % scale;
        out.push_str(&int.to_string());
        if frac != 0 {
            let f = format!("{frac:0width$}", width = prec as usize);
            let f = f.trim_end_matches('0');
            out.push('.');
            out.push_str(f);
        }
        out.push_str(unit);
    } else {
        let frac = u % 1_000_000_000;
        let mut secs = u / 1_000_000_000;
        let mut sec_part = (secs % 60).to_string();
        if frac != 0 {
            let f = format!("{frac:09}");
            sec_part.push('.');
            sec_part.push_str(f.trim_end_matches('0'));
        }
        secs /= 60; // now minutes
        let mins = secs % 60;
        let hours = secs / 60;
        if hours != 0 {
            out.push_str(&hours.to_string());
            out.push('h');
        }
        if hours != 0 || mins != 0 {
            out.push_str(&mins.to_string());
            out.push('m');
        }
        out.push_str(&sec_part);
        out.push('s');
    }
    if neg {
        format!("-{out}")
    } else {
        out
    }
}

/// Formats `ms` milliseconds as Go does for `time.Duration(ms)*time.Millisecond`.
#[must_use]
pub fn format_go_duration_ms(ms: i64) -> String {
    format_go_duration(ms * 1_000_000)
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
}
