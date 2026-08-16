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

//! SEED of Go `pkg/sessionctx/variable`, covering `varsutil.go`'s pure
//! conversion and validation helpers that had no Rust owner yet.
//!
//! This is a seed of the `variable` package, not its completion. Already
//! owned elsewhere and deliberately not duplicated here: `TiDBOptOn` and the
//! ON/OFF conversions (`tidb-exec/src/option_values.rs`),
//! `checkIsolationLevel` (`isolation_state.rs`), `checkReadOnly`
//! (`noop_read_only.rs`), and `GlobalSystemVariableInitialValue`
//! (`tidb-vardef/src/global_sysvar_initial.rs`). The hook-calling helpers
//! (`switchDDL`/`switchStats`) ride Go's function-pointer registration in
//! `tidb_vars.go` and stay with that integration seam.
//!
//! `parseMemoryLimit` reads the host's total memory through
//! `memory.GetMemTotalIgnoreErr`; that probe is the platform's, so the total
//! arrives as a parameter here and `0` reproduces Go's failed-probe branch
//! (percentages are then never accepted).

use std::collections::BTreeSet;

use tidb_vardef::tidb_vars::{
    ASSERTION_FAST_STR, ASSERTION_STRICT_STR, ON, TIDB_ANALYZE_SKIP_COLUMN_TYPES, WARN,
};

use crate::vars::VarError;

/// Go `OffInt`.
pub const OFF_INT: i64 = 0;
/// Go `OnInt`.
pub const ON_INT: i64 = 1;
/// Go `WarnInt`.
pub const WARN_INT: i64 = 2;

/// Go `TiDBOptOnOffWarn`: the three-state mode used by `MultiStmtMode` and
/// `NoopFunctionsMode`. Anything that is not exactly `ON` or `WARN` is off.
#[must_use]
pub fn tidb_opt_on_off_warn(opt: &str) -> i64 {
    match opt {
        _ if opt == WARN => WARN_INT,
        _ if opt == ON => ON_INT,
        _ => OFF_INT,
    }
}

/// Go `AssertionLevel`: how much assertion runs during transactions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum AssertionLevel {
    /// Go `AssertionLevelOff`.
    #[default]
    Off,
    /// Go `AssertionLevelFast`.
    Fast,
    /// Go `AssertionLevelStrict`.
    Strict,
}

/// Go's private `tidbOptAssertionLevel`; an unknown spelling is off.
#[must_use]
pub fn tidb_opt_assertion_level(opt: &str) -> AssertionLevel {
    match opt {
        _ if opt == ASSERTION_STRICT_STR => AssertionLevel::Strict,
        _ if opt == ASSERTION_FAST_STR => AssertionLevel::Fast,
        _ => AssertionLevel::Off,
    }
}

/// Go's private `tidbOptPositiveInt32`: unparsable or non-positive input
/// falls back to the default.
#[must_use]
pub fn tidb_opt_positive_int32(opt: &str, default_val: i64) -> i64 {
    match opt.parse::<i64>() {
        Ok(val) if val > 0 => val,
        _ => default_val,
    }
}

/// Go `TidbOptInt`.
#[must_use]
pub fn tidb_opt_int(opt: &str, default_val: i64) -> i64 {
    opt.parse::<i64>().unwrap_or(default_val)
}

/// Go `TidbOptInt64`.
#[must_use]
pub fn tidb_opt_int64(opt: &str, default_val: i64) -> i64 {
    opt.parse::<i64>().unwrap_or(default_val)
}

/// Go `TidbOptUint64`.
#[must_use]
pub fn tidb_opt_uint64(opt: &str, default_val: u64) -> u64 {
    opt.parse::<u64>().unwrap_or(default_val)
}

/// Go's private `tidbOptFloat64`.
#[must_use]
pub fn tidb_opt_float64(opt: &str, default_val: f64) -> f64 {
    opt.parse::<f64>().unwrap_or(default_val)
}

/// Go's private `parsePercentage`: accepts exactly `N%`, and answers nothing
/// for 0% or anything at or above 100%.
#[must_use]
pub fn parse_percentage(s: &str) -> Option<(u64, String)> {
    let digits = s.strip_suffix('%')?;
    let percentage: u64 = digits.parse().ok()?;
    if percentage == 0 || percentage >= 100 {
        return None;
    }
    Some((percentage, format!("{percentage}%")))
}

/// Go's private `parseByteSize`: a bare byte count or a count with one of the
/// exact suffixes `KB`/`KiB`/`MB`/`MiB`/`GB`/`GiB`/`TB`/`TiB` — Go treats the
/// decimal and binary spellings identically, both binary-scaled.
#[must_use]
pub fn parse_byte_size(s: &str) -> Option<(u64, String)> {
    const SUFFIXES: [(&str, u32); 9] = [
        ("", 0),
        ("KB", 10),
        ("KiB", 10),
        ("MB", 20),
        ("MiB", 20),
        ("GB", 30),
        ("GiB", 30),
        ("TB", 40),
        ("TiB", 40),
    ];
    for (suffix, shift) in SUFFIXES {
        if let Some(digits) = s.strip_suffix(suffix) {
            if let Ok(size) = digits.parse::<u64>() {
                return size
                    .checked_shl(shift)
                    .map(|scaled| (scaled, format!("{size}{suffix}")));
            }
        }
    }
    None
}

/// What Go's private `parseMemoryLimit` produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedMemoryLimit {
    /// The limit in bytes.
    pub byte_size: u64,
    /// The normalized spelling stored for the variable.
    pub normalized: String,
    /// Whether the value was clamped up to 512 MiB; Go appends an
    /// `ErrTruncatedWrongValue` warning to the statement when so.
    pub clamped: bool,
}

/// Go `ErrTruncatedWrongValue` (1292) as `parseMemoryLimit` raises it for
/// `tidb_server_memory_limit`. `VarError` has no 1292 variant yet — the SET
/// wiring for this variable lands with the sysvar closures — so the error
/// keeps its own faithful type rather than borrowing 1231.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncatedWrongValue {
    /// The variable name.
    pub name: String,
    /// The offending original value.
    pub value: String,
}

impl std::fmt::Display for TruncatedWrongValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Go's template truncates the name to 64 and the value to 128 chars.
        let name: String = self.name.chars().take(64).collect();
        let value: String = self.value.chars().take(128).collect();
        write!(
            formatter,
            "[variable:1292]Truncated incorrect {name} value: '{value}'"
        )
    }
}

impl std::error::Error for TruncatedWrongValue {}

/// Go's private `parseMemoryLimit`, with the host's total memory passed in.
///
/// A percentage of a known total wins, then a byte size; a positive result
/// under 512 MiB clamps to `512MB` with a warning.
pub fn parse_memory_limit(
    total_mem: u64,
    normalized_value: &str,
    original_value: &str,
) -> Result<ParsedMemoryLimit, TruncatedWrongValue> {
    let parsed = if total_mem != 0 {
        parse_percentage(normalized_value)
            .map(|(percentage, normalized)| (total_mem * percentage / 100, normalized))
    } else {
        None
    };
    let (byte_size, normalized) = match parsed.or_else(|| parse_byte_size(normalized_value)) {
        Some(parsed) => parsed,
        None => {
            return Err(TruncatedWrongValue {
                name: "tidb_server_memory_limit".to_owned(),
                value: original_value.to_owned(),
            });
        }
    };
    if byte_size > 0 && byte_size < (512 << 20) {
        return Ok(ParsedMemoryLimit {
            byte_size: 512 << 20,
            normalized: "512MB".to_owned(),
            clamped: true,
        });
    }
    Ok(ParsedMemoryLimit {
        byte_size,
        normalized,
        clamped: false,
    })
}

/// Go `GAFunction4ExpressionIndex`: the functions GA for expression indexes,
/// spelled as `pkg/parser/ast` spells their names.
pub const GA_FUNCTION_4_EXPRESSION_INDEX: [&str; 30] = [
    "lower",
    "upper",
    "md5",
    "reverse",
    "vitess_hash",
    "tidb_shard",
    // JSON functions.
    "json_type",
    "json_extract",
    "json_unquote",
    "json_array",
    "json_object",
    "json_set",
    "json_insert",
    "json_replace",
    "json_remove",
    "json_contains",
    "json_contains_path",
    "json_valid",
    "json_array_append",
    "json_array_insert",
    "json_merge_patch",
    "json_merge_preserve",
    "json_pretty",
    "json_quote",
    "json_schema_valid",
    "json_search",
    "json_storage_size",
    "json_depth",
    "json_keys",
    "json_length",
];

/// Whether a function is GA for expression indexes.
#[must_use]
pub fn is_ga_function_4_expression_index(func_name: &str) -> bool {
    GA_FUNCTION_4_EXPRESSION_INDEX.contains(&func_name)
}

/// Go's private `collectAllowFuncName4ExpressionIndex`: the sorted,
/// comma-joined allowlist used in error messages.
#[must_use]
pub fn collect_allow_func_name_4_expression_index() -> String {
    let sorted: BTreeSet<&str> = GA_FUNCTION_4_EXPRESSION_INDEX.iter().copied().collect();
    sorted.into_iter().collect::<Vec<_>>().join(", ")
}

/// Go's private `analyzeSkipAllowedTypes`.
const ANALYZE_SKIP_ALLOWED_TYPES: [&str; 7] = [
    "json",
    "text",
    "mediumtext",
    "longtext",
    "blob",
    "mediumblob",
    "longblob",
];

/// Go `ValidAnalyzeSkipColumnTypes`: normalizes the comma list to lowercase
/// trimmed entries; any entry outside the allowed set rejects the whole value
/// with `ErrWrongValueForVar`.
pub fn valid_analyze_skip_column_types(val: &str) -> Result<String, VarError> {
    if val.is_empty() {
        return Ok(String::new());
    }
    let mut column_types = Vec::new();
    for item in val.to_lowercase().split(',') {
        let column_type = item.trim();
        if !ANALYZE_SKIP_ALLOWED_TYPES.contains(&column_type) {
            return Err(VarError::WrongValueForVar(
                TIDB_ANALYZE_SKIP_COLUMN_TYPES.to_owned(),
                val.to_owned(),
            ));
        }
        column_types.push(column_type.to_owned());
    }
    Ok(column_types.join(","))
}

/// Go `ParseAnalyzeSkipColumnTypes`: the stored value as a set, silently
/// dropping anything outside the allowed types.
#[must_use]
pub fn parse_analyze_skip_column_types(val: &str) -> BTreeSet<String> {
    val.to_lowercase()
        .split(',')
        .filter(|column_type| ANALYZE_SKIP_ALLOWED_TYPES.contains(column_type))
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestHelperFuncs`' numeric rows.
    #[test]
    fn numeric_options_fall_back_to_their_defaults() {
        assert_eq!(tidb_opt_positive_int32("1234", 5), 1234);
        assert_eq!(tidb_opt_positive_int32("-1234", 5), 5);
        assert_eq!(tidb_opt_positive_int32("bogus", 5), 5);

        assert_eq!(tidb_opt_int("1234", 5), 1234);
        assert_eq!(tidb_opt_int("-1234", 5), -1234);
        assert_eq!(tidb_opt_int("bogus", 5), 5);

        assert_eq!(tidb_opt_int64("-9223372036854775808", 5), i64::MIN);
        assert_eq!(tidb_opt_int64("bogus", 5), 5);
        assert_eq!(tidb_opt_uint64("18446744073709551615", 5), u64::MAX);
        assert_eq!(tidb_opt_uint64("-1", 5), 5);
        assert!((tidb_opt_float64("1.5", 0.0) - 1.5).abs() < f64::EPSILON);
        assert!((tidb_opt_float64("bogus", 2.5) - 2.5).abs() < f64::EPSILON);
    }

    // The ON/OFF/WARN tri-state is exact-match, not case-folded.
    #[test]
    fn on_off_warn_is_exact_match() {
        assert_eq!(tidb_opt_on_off_warn("WARN"), WARN_INT);
        assert_eq!(tidb_opt_on_off_warn("ON"), ON_INT);
        assert_eq!(tidb_opt_on_off_warn("OFF"), OFF_INT);
        assert_eq!(tidb_opt_on_off_warn("on"), OFF_INT);
        assert_eq!(tidb_opt_on_off_warn(""), OFF_INT);
    }

    // Assertion levels parse their vardef spellings; anything else is off.
    #[test]
    fn assertion_levels_parse_their_spellings() {
        assert_eq!(tidb_opt_assertion_level("STRICT"), AssertionLevel::Strict);
        assert_eq!(tidb_opt_assertion_level("FAST"), AssertionLevel::Fast);
        assert_eq!(tidb_opt_assertion_level("OFF"), AssertionLevel::Off);
        assert_eq!(tidb_opt_assertion_level("strict"), AssertionLevel::Off);
        assert_eq!(tidb_opt_assertion_level(""), AssertionLevel::Off);
    }

    // Go's parsePercentage: exactly N%, open interval (0, 100).
    #[test]
    fn percentages_accept_the_open_interval_only() {
        assert_eq!(parse_percentage("50%"), Some((50, "50%".to_owned())));
        assert_eq!(parse_percentage("1%"), Some((1, "1%".to_owned())));
        assert_eq!(parse_percentage("99%"), Some((99, "99%".to_owned())));
        assert_eq!(parse_percentage("0%"), None);
        assert_eq!(parse_percentage("100%"), None);
        assert_eq!(parse_percentage("150%"), None);
        assert_eq!(parse_percentage("50"), None);
        assert_eq!(parse_percentage("x%"), None);
        assert_eq!(parse_percentage("50%x"), None);
    }

    // Go's parseByteSize: KB and KiB are both binary-scaled, suffixes are
    // case-sensitive, and trailing junk fails.
    #[test]
    fn byte_sizes_scale_binary_for_both_spellings() {
        assert_eq!(parse_byte_size("1024"), Some((1024, "1024".to_owned())));
        assert_eq!(parse_byte_size("10KB"), Some((10 << 10, "10KB".to_owned())));
        assert_eq!(
            parse_byte_size("10KiB"),
            Some((10 << 10, "10KiB".to_owned()))
        );
        assert_eq!(parse_byte_size("3MB"), Some((3 << 20, "3MB".to_owned())));
        assert_eq!(parse_byte_size("2GiB"), Some((2 << 30, "2GiB".to_owned())));
        assert_eq!(parse_byte_size("1TB"), Some((1 << 40, "1TB".to_owned())));
        assert_eq!(parse_byte_size("10kb"), None);
        assert_eq!(parse_byte_size("10KBx"), None);
        assert_eq!(parse_byte_size("x"), None);
    }

    // Go's parseMemoryLimit: percentage-of-total wins, small positives clamp
    // to 512MB with a warning, and garbage is the truncated-value error.
    #[test]
    fn memory_limits_prefer_percentages_and_clamp_small_values() {
        let gib = 1_u64 << 30;

        let parsed = parse_memory_limit(64 * gib, "50%", "50%").unwrap();
        assert_eq!(parsed.byte_size, 32 * gib);
        assert_eq!(parsed.normalized, "50%");
        assert!(!parsed.clamped);

        // With no known total, a percentage cannot be accepted.
        assert!(parse_memory_limit(0, "50%", "50%").is_err());

        let parsed = parse_memory_limit(64 * gib, "1GB", "1GB").unwrap();
        assert_eq!(parsed.byte_size, 1 << 30);
        assert!(!parsed.clamped);

        // 100MB is positive but under the 512MiB floor.
        let parsed = parse_memory_limit(64 * gib, "100MB", "100MB").unwrap();
        assert_eq!(parsed.byte_size, 512 << 20);
        assert_eq!(parsed.normalized, "512MB");
        assert!(parsed.clamped);

        // Zero disables the limit and is not clamped.
        let parsed = parse_memory_limit(64 * gib, "0", "0").unwrap();
        assert_eq!(parsed.byte_size, 0);
        assert!(!parsed.clamped);

        let error = parse_memory_limit(64 * gib, "bogus", "bogus").unwrap_err();
        assert_eq!(
            error.to_string(),
            "[variable:1292]Truncated incorrect tidb_server_memory_limit value: 'bogus'"
        );
    }

    // The GA allowlist and its error-message rendering.
    #[test]
    fn the_expression_index_allowlist_is_sorted_when_collected() {
        assert!(is_ga_function_4_expression_index("lower"));
        assert!(is_ga_function_4_expression_index("json_length"));
        assert!(!is_ga_function_4_expression_index("rand"));

        let collected = collect_allow_func_name_4_expression_index();
        assert!(collected.starts_with("json_array, "));
        assert!(collected.ends_with("upper, vitess_hash"));
        assert_eq!(collected.matches(", ").count(), 29);
    }

    // Go `ValidAnalyzeSkipColumnTypes` / `ParseAnalyzeSkipColumnTypes`.
    #[test]
    fn analyze_skip_types_validate_and_parse() {
        assert_eq!(valid_analyze_skip_column_types("").unwrap(), "");
        assert_eq!(
            valid_analyze_skip_column_types("JSON, text").unwrap(),
            "json,text"
        );
        assert_eq!(
            valid_analyze_skip_column_types("json,int"),
            Err(VarError::WrongValueForVar(
                TIDB_ANALYZE_SKIP_COLUMN_TYPES.to_owned(),
                "json,int".to_owned()
            ))
        );

        let parsed = parse_analyze_skip_column_types("json,blob,int");
        assert!(parsed.contains("json"));
        assert!(parsed.contains("blob"));
        assert_eq!(parsed.len(), 2);
    }
}
