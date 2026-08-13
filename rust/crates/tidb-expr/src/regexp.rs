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

//! `[NOT] REGEXP`/`RLIKE` pattern matching — this workspace's first
//! external dependency, the `regex` crate (see the workspace
//! `Cargo.toml`'s own doc comment for why it's a high-fidelity match
//! for real TiDB's own Go-`regexp`-package-based implementation, not
//! an arbitrary choice). Shared by the operator and named-function evaluators.

use regex::{Regex, RegexBuilder};

use crate::EvalError;

fn build_regexp(pattern: &str, match_type: &str) -> Result<Regex, EvalError> {
    if pattern.is_empty() {
        return Err(EvalError::Unsupported("empty regular expression pattern"));
    }

    // Reduce Go's rightmost-flag-wins rule deterministically before building
    // the expression. The three remaining RE2 options are independent.
    let mut case_insensitive = false;
    let mut multi_line = false;
    let mut dot_matches_new_line = false;
    for flag in match_type.bytes() {
        match flag {
            b'i' => case_insensitive = true,
            b'c' => case_insensitive = false,
            b'm' => multi_line = true,
            b's' => dot_matches_new_line = true,
            _ => return Err(EvalError::Unsupported("Invalid match type")),
        }
    }

    RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .multi_line(multi_line)
        .dot_matches_new_line(dot_matches_new_line)
        .build()
        .map_err(|_| EvalError::Unsupported("invalid regular expression pattern"))
}

/// Compiles one of TiDB's RE2-compatible regular expressions for the scalar
/// functions in `builtin_ext::regexp`.  Keeping compilation here makes the
/// `[NOT] REGEXP` predicate and the positional regexp family share exactly
/// the same empty-pattern, flag, and syntax validation rules.
pub(crate) fn compile_regexp(pattern: &str, match_type: &str) -> Result<Regex, EvalError> {
    build_regexp(pattern, match_type)
}

/// `REGEXP_LIKE(expr, pat[, match_type])` over the seed evaluator's UTF-8
/// scalar value domain. Go's session-selected collation, warning channel,
/// vectorized chunk path, and context-aware regexp cache remain outside this
/// function; callers provide only the source `match_type`.
pub(crate) fn regexp_like(text: &str, pattern: &str, match_type: &str) -> Result<bool, EvalError> {
    Ok(build_regexp(pattern, match_type)?.is_match(text))
}

/// Whether `text` matches `pattern` anywhere within it — a genuine
/// substring/partial match, NOT full-string anchoring — case-SENSITIVE,
/// matching the seed evaluator's `utf8mb4_bin` convention. Empty and
/// malformed patterns are surfaced as `Unsupported`, matching TiDB's source
/// runtime errors rather than allowing the regex crate's empty-pattern default.
pub(crate) fn regexp_match(text: &str, pattern: &str) -> Result<bool, EvalError> {
    regexp_match_with_collation(text, pattern, crate::ops::DERIVATION_FREE_COLLATION)
}

/// [`regexp_match`] under the collation the expression derivation aggregated
/// over both operands (Go `deriveCollation`'s `ast.Regexp` arm).
///
/// Go does NOT compare through a collator here -- `getRegexpMatchType`
/// (`pkg/expression/builtin_regexp.go`) seeds the match-type flag set with
/// `flagI` when `collate.IsCICollation(collation)`, so a case-insensitive
/// collation is expressed to RE2 as the `i` flag. Captured from TiDB:
/// `'ABC' COLLATE utf8mb4_general_ci REGEXP 'abc'` is 1 where the
/// `utf8mb4_bin` form is 0.
///
/// A user-supplied `match_type` can still override this: an explicit `c` flag
/// deletes `i` again, which [`build_regexp`]'s left-to-right flag scan already
/// reproduces because the seeded `i` is passed as the leading flag.
pub(crate) fn regexp_match_with_collation(
    text: &str,
    pattern: &str,
    collation: tidb_datatype::Collation,
) -> Result<bool, EvalError> {
    let match_type = if tidb_datatype::is_ci_collation(collation.name()) {
        "i"
    } else {
        ""
    };
    regexp_like(text, pattern, match_type)
}

/// `REGEXP_LIKE` applies the expression's derived collation before its
/// user-supplied match type. A later `c`/`i` therefore wins, as Go's
/// `getRegexpMatchType` requires.
pub(crate) fn regexp_like_with_collation(
    text: &str,
    pattern: &str,
    match_type: &str,
    collation: tidb_datatype::Collation,
) -> Result<bool, EvalError> {
    let initial = if tidb_datatype::is_ci_collation(collation.name()) {
        "i"
    } else {
        ""
    };
    regexp_like(text, pattern, &format!("{initial}{match_type}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regexp_like_source_scalar_rows() {
        // These first nine rows are copied from Go's
        // `pkg/expression/builtin_like_test.go:64 TestRegexp`.  Keep the
        // table in source order: this is the `[NOT] REGEXP` (not the newer
        // three-argument `REGEXP_LIKE`) contract that the parser dispatcher
        // below exposes.
        let rows = [
            ("a", "^$", "", false),
            ("a", "a", "", true),
            ("b", "a", "", false),
            ("aA", "aA", "", true),
            ("a", ".", "", true),
            ("ab", "^.$", "", false),
            ("b", "..", "", false),
            ("aab", ".ab", "", true),
            ("abcd", ".*", "", true),
            ("abc", "AbC", "", false),
            ("abc", "AbC", "i", true),
            ("123\n321", "23$", "", false),
            ("123\n321", "23$", "m", true),
            ("good\nday", "^day", "m", true),
            ("\n", ".", "", false),
            ("\n", ".", "s", true),
            ("abc", "aBc", "ic", false),
            ("abc", "aBc", "ci", true),
        ];
        for (text, pattern, match_type, expected) in rows {
            assert_eq!(
                regexp_like(text, pattern, match_type).unwrap(),
                expected,
                "REGEXP_LIKE({text:?}, {pattern:?}, {match_type:?})"
            );
        }
    }

    /// The malformed-pattern rows from Go's `TestRegexp` must fail while
    /// compiling the pattern, rather than being treated as a non-match.  The
    /// production `regexp_match` path is deliberately exercised here instead
    /// of testing the `regex` dependency directly.
    #[test]
    fn regexp_source_malformed_patterns() {
        for pattern in ["(", "(*", "[a", "\\"] {
            assert!(
                matches!(
                    regexp_match("", pattern),
                    Err(EvalError::Unsupported("invalid regular expression pattern"))
                ),
                "pattern {pattern:?}"
            );
        }
    }

    #[test]
    fn regexp_like_rejects_empty_invalid_pattern_and_match_type() {
        assert!(matches!(
            regexp_like("a", "", ""),
            Err(EvalError::Unsupported("empty regular expression pattern"))
        ));
        assert!(matches!(
            regexp_like("a", "[a", ""),
            Err(EvalError::Unsupported("invalid regular expression pattern"))
        ));
        for pattern in ["(", "(*", "\\"] {
            assert!(matches!(
                regexp_like("", pattern, ""),
                Err(EvalError::Unsupported("invalid regular expression pattern"))
            ));
        }
        assert!(matches!(
            regexp_like("abc", "abc", "p"),
            Err(EvalError::Unsupported("Invalid match type"))
        ));
        assert!(matches!(
            regexp_like("abc", "abc", "cpi"),
            Err(EvalError::Unsupported("Invalid match type"))
        ));
    }
}
