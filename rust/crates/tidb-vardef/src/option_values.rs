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

//! Session option text conversions from TiDB's `varsutil.go`.
//!
//! This leaf keeps only the source ON/OFF and true/false compatibility
//! conversions plus TiDB's narrow `ON`/`1` option predicate. It deliberately
//! does not parse SQL expressions, validate a system-variable type, mutate
//! `SessionVars`, or publish warnings.

use std::borrow::Cow;

use crate::tidb_vars::{OFF, ON};

/// Returns the source canonical ON/OFF spelling for a boolean.
#[must_use]
pub const fn bool_to_on_off(value: bool) -> &'static str {
    if value {
        ON
    } else {
        OFF
    }
}

/// Converts a `true`/`false` table value to a canonical ON/OFF value.
///
/// Values other than case-insensitive `true` and `false` are returned without
/// modification, matching the Go helper's pass-through behavior.
#[must_use]
pub fn true_false_to_on_off(value: &str) -> Cow<'_, str> {
    if value.eq_ignore_ascii_case("true") {
        Cow::Borrowed(ON)
    } else if value.eq_ignore_ascii_case("false") {
        Cow::Borrowed(OFF)
    } else {
        Cow::Borrowed(value)
    }
}

/// Converts a canonical ON/OFF value to a `true`/`false` table value.
///
/// Values other than case-insensitive `ON` and `OFF` are returned unchanged.
#[must_use]
pub fn on_off_to_true_false(value: &str) -> Cow<'_, str> {
    if value.eq_ignore_ascii_case(ON) {
        Cow::Borrowed("true")
    } else if value.eq_ignore_ascii_case(OFF) {
        Cow::Borrowed("false")
    } else {
        Cow::Borrowed(value)
    }
}

/// Returns whether a TiDB option is enabled by exactly `ON` or `1`.
#[must_use]
pub fn tidb_opt_on(value: &str) -> bool {
    value.eq_ignore_ascii_case(ON) || value == "1"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tidb_option_on_accepts_only_on_or_one() {
        // Source: pkg/sessionctx/variable/varsutil_test.go:33-54 and
        // pkg/sessionctx/variable/varsutil.go:183-186.
        for value in ["ON", "on", "On", "1"] {
            assert!(tidb_opt_on(value), "{value}");
        }
        for value in ["off", "OFF", "No", "0", "1.1", "", "true", " 1", "01"] {
            assert!(!tidb_opt_on(value), "{value}");
        }
    }

    #[test]
    fn boolean_and_table_text_conversions_preserve_source_spellings() {
        // Source: pkg/sessionctx/variable/varsutil.go:42-48, 148-168 and
        // pkg/sessionctx/variable/varsutil_test.go:704-718.
        assert_eq!(bool_to_on_off(true), ON);
        assert_eq!(bool_to_on_off(false), OFF);
        assert_eq!(true_false_to_on_off("TRUE"), ON);
        assert_eq!(true_false_to_on_off("TRue"), ON);
        assert_eq!(true_false_to_on_off("true"), ON);
        assert_eq!(true_false_to_on_off("FALSE"), OFF);
        assert_eq!(true_false_to_on_off("False"), OFF);
        assert_eq!(true_false_to_on_off("false"), OFF);
        assert_eq!(true_false_to_on_off("other"), "other");
        assert_eq!(on_off_to_true_false("ON"), "true");
        assert_eq!(on_off_to_true_false("on"), "true");
        assert_eq!(on_off_to_true_false("On"), "true");
        assert_eq!(on_off_to_true_false("OFF"), "false");
        assert_eq!(on_off_to_true_false("Off"), "false");
        assert_eq!(on_off_to_true_false("off"), "false");
        assert_eq!(on_off_to_true_false("other"), "other");
    }
}
