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

/// Canonical system-variable ON text.
pub const ON: &str = "ON";
/// Canonical system-variable OFF text.
pub const OFF: &str = "OFF";

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
