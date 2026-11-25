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

//! `[NOT] REGEXP`/`RLIKE` and SQL regexp compilation. The shared Go
//! compatibility adapter owns syntax and character classes; this layer owns
//! SQL match flags and statement-context caching.

use std::{ops::Range, sync::Arc};

use memchr::memmem::Finder;
use regex::Regex;
use tidb_go_regexp::RegexpPredicate;

use crate::EvalError;

/// Go `regexpMemorizedSig`: a cached pattern retains either its compiled
/// expression or the compile error produced for this statement context.
#[derive(Clone, Debug)]
pub(crate) struct RegexpMemorizedSig<T = PositionalRegexp> {
    pub(crate) result: Result<T, EvalError>,
}

#[derive(Clone, Debug)]
pub(crate) enum PositionalRegexp {
    Literal(Arc<Finder<'static>>),
    AsciiFolded(Box<[u8]>),
    Regex(Regex),
}

impl PositionalRegexp {
    pub(crate) fn find_nth(&self, text: &str, index: usize) -> Option<Range<usize>> {
        match self {
            Self::Literal(finder) => finder
                .find_iter(text.as_bytes())
                .nth(index)
                .map(|start| start..start + finder.needle().len()),
            Self::AsciiFolded(literal) => find_ascii_folded(text, literal).nth(index),
            Self::Regex(regex) => regex.find_iter(text).nth(index).map(|found| found.range()),
        }
    }
}

pub(crate) fn find_ascii_folded<'a>(
    text: &'a str,
    literal: &'a [u8],
) -> impl Iterator<Item = Range<usize>> + 'a {
    let text = text.as_bytes();
    let candidates = text
        .len()
        .checked_sub(literal.len())
        .map_or(0, |last| last + 1);
    let first = literal[0];
    let mut next_start = 0;
    memchr::memchr2_iter(
        first.to_ascii_lowercase(),
        first.to_ascii_uppercase(),
        &text[..candidates],
    )
    .filter_map(move |start| {
        let end = start + literal.len();
        if start < next_start || !text[start..end].eq_ignore_ascii_case(literal) {
            return None;
        }
        next_start = end;
        Some(start..end)
    })
}

fn with_regexp_flags<T>(
    pattern: &str,
    match_type: &str,
    evaluate: impl FnOnce(&str, bool, bool, bool) -> Result<T, String>,
) -> Result<T, EvalError> {
    if pattern.is_empty() {
        return Err(err_regexp("Empty pattern is invalid"));
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
            _ => return Err(err_regexp("Invalid match type")),
        }
    }

    evaluate(pattern, case_insensitive, multi_line, dot_matches_new_line)
        .map_err(|error| err_regexp(&go_compile_error_text(pattern, &error)))
}

/// Go's `regexp.Compile` error text (`error parsing regexp: <code>: `<arg>``)
/// for the failures this compatibility layer can name; anything else keeps the
/// underlying reason after Go's prefix.
fn go_compile_error_text(pattern: &str, reason: &str) -> String {
    let code = if reason.contains("unclosed group") {
        format!("missing closing ): `{pattern}`")
    } else if reason.contains("unopened group") {
        format!("unexpected ): `{pattern}`")
    } else if reason.contains("incomplete escape sequence") || reason == "trailing backslash" {
        "trailing backslash at end of expression: ``".to_owned()
    } else if reason.contains("unclosed character class") {
        let tail = pattern.rfind('[').map_or(pattern, |at| &pattern[at..]);
        format!("missing closing ]: `{tail}`")
    } else if reason.contains("repetition operator missing expression") {
        "missing argument to repetition operator".to_owned()
    } else if reason.contains("invalid repeat count") || reason.contains("repetition quantifier exceeds") {
        "invalid repeat count".to_owned()
    } else {
        reason.lines().last().unwrap_or(reason).trim().to_owned()
    };
    format!("error parsing regexp: {code}")
}

/// Compiles one of TiDB's RE2-compatible regular expressions for the scalar
/// functions in `builtin_ext::regexp`. Keeping compilation here makes the
/// `[NOT] REGEXP` predicate and the positional regexp family share exactly
/// the same empty-pattern, flag, and syntax validation rules.
pub(crate) fn compile_regexp(pattern: &str, match_type: &str) -> Result<PositionalRegexp, EvalError> {
    with_regexp_flags(pattern, match_type, |pattern, insensitive, multiline, dot_all| {
        let literal = pattern.as_bytes();
        if is_plain_regexp(literal) {
            if !insensitive {
                return Ok(PositionalRegexp::Literal(Arc::new(Finder::new(pattern).into_owned())));
            }
            // Go's Unicode simple fold can match K with K and S with ſ.
            // Only use byte matching for short ASCII literals without those folds.
            if literal.len() <= 16
                && literal
                    .iter()
                    .all(|byte| byte.is_ascii() && !matches!(byte, b'k' | b'K' | b's' | b'S'))
            {
                return Ok(PositionalRegexp::AsciiFolded(literal.into()));
            }
        }
        tidb_go_regexp::compile_with_flags(pattern, insensitive, multiline, dot_all)
            .map(PositionalRegexp::Regex)
    })
}

pub(crate) fn is_plain_regexp(pattern: &[u8]) -> bool {
    !pattern.is_empty() && pattern.len() <= 256 && !pattern.iter().any(|byte| {
        matches!(
            byte,
            b'\\' | b'.' | b'+' | b'*' | b'?' | b'(' | b')' | b'|' | b'[' | b']' | b'{' | b'}' | b'^' | b'$'
        )
    })
}

/// Go `ErrRegexp` (`ClassExpression.NewStd(mysql.ErrRegexp)`, 1139):
/// `Got error '%-.64s' from regexp`.
pub(crate) fn err_regexp(message: &str) -> EvalError {
    use tidb_mysql::errors::FormatArg;
    use tidb_terror::{TerrorClass, TerrorError};
    static ERR_REGEXP: std::sync::LazyLock<TerrorError> = std::sync::LazyLock::new(|| {
        tidb_dbterror::ErrClass(TerrorClass::Expression).new_std(1139)
    });
    EvalError::Conversion(ERR_REGEXP.gen_with_stack_by_args(&[FormatArg::from(message)]))
}

/// [`err_regexp`] for an argument that is itself a TiDB `*errors.Error`
/// (the `verifyArgs` failure `regexp_instr`/`regexp_replace` wrap): that type
/// implements `fmt.Formatter` and ignores the template's `%-.64s` precision,
/// so the wrapped text is never cut.
pub(crate) fn err_regexp_wrapping_error(message: &str) -> EvalError {
    use tidb_mysql::errors::FormatArg;
    use tidb_terror::{TerrorClass, TerrorError};
    static ERR_REGEXP: std::sync::LazyLock<TerrorError> = std::sync::LazyLock::new(|| {
        tidb_dbterror::ErrClass(TerrorClass::Expression).new_std(1139)
    });
    EvalError::Conversion(ERR_REGEXP.gen_with_stack_by_args(&[FormatArg::new(
        message,
        message,
        "*errors.Error",
    )]))
}

/// Go `binaryCollateErr`: the positional regexp builtins refuse a
/// `binary` charset and collation at build time, in `getFunction`.
/// Go `ErrRegexp.GenWithStackByArgs(err)` for an error value: the argument
/// is the wrapped error's `Error()` text.
pub(crate) fn err_regexp_wrapping(err: EvalError) -> EvalError {
    use crate::eval_error_sql::EvalErrorSql;
    let sql = err.to_sql_error();
    let text = match sql.identity {
        Some(identity) => format!("[{}]{}", identity.rfc_code(), sql.message),
        None => sql.message,
    };
    err_regexp_wrapping_error(&text)
}

pub(crate) const BINARY_COLLATE_ERR: &str = "Not support binary collation so far";

/// Go `getRegexpMatchType`'s collation-derived initial flag. Explicit match
/// type flags are appended so the rightmost user flag keeps precedence.
pub(crate) fn regexp_match_type_with_collation(
    match_type: &str,
    collation: tidb_charset::Collation,
) -> String {
    let initial = if tidb_datatype::is_ci_collation(collation.name()) {
        "i"
    } else {
        ""
    };
    format!("{initial}{match_type}")
}

/// Matches a predicate, memoizing the compiled result when both
/// pattern-bearing arguments are constant within the statement context.
/// The `Result` itself is cached so an invalid pattern is reported repeatedly
/// without recompiling, matching `regexpMemorizedSig.memorizedErr`.
pub(crate) fn regexp_like_with_cache(
    cache: &crate::builtin_ext::BuiltinFuncCache<RegexpMemorizedSig<RegexpPredicate>>,
    context_id: u64,
    cache_enabled: bool,
    text: &str,
    pattern: &str,
    match_type: &str,
) -> Result<bool, EvalError> {
    if !cache_enabled {
        return regexp_like(text, pattern, match_type);
    }
    let cached = cache.get_or_init_cache(context_id, || {
        Ok::<_, EvalError>(RegexpMemorizedSig {
            result: with_regexp_flags(pattern, match_type, tidb_go_regexp::compile_predicate),
        })
    })?;
    cached
        .result
        .as_ref()
        .map(|regexp| regexp.is_match(text))
        .map_err(Clone::clone)
}

/// `REGEXP_LIKE(expr, pat[, match_type])` over the seed evaluator's UTF-8
/// scalar value domain. Callers provide only the source `match_type`; the
/// scalar-function layer supplies statement-context caching where the Go
/// signature permits it.
pub(crate) fn regexp_like(text: &str, pattern: &str, match_type: &str) -> Result<bool, EvalError> {
    with_regexp_flags(pattern, match_type, |pattern, insensitive, multiline, dot_all| {
        tidb_go_regexp::is_match_with_flags(text, pattern, insensitive, multiline, dot_all)
    })
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
    collation: tidb_charset::Collation,
) -> Result<bool, EvalError> {
    let match_type = regexp_match_type_with_collation("", collation);
    regexp_like(text, pattern, &match_type)
}

/// `[NOT] REGEXP` matching for the statistics TopN-assisted estimation path
/// (`GetSelectivityByFilter`): stored values and patterns arrive as raw
/// bytes, the estimator's own gate only reaches binary-collation string
/// columns (Go's new-collation refusal), so the case-sensitive default
/// matcher is exact, and any invalid UTF-8 operand or malformed pattern
/// answers `None` — the by-value form of Go's error fallback, which declines
/// the whole estimation.
pub fn regexp_match_bin_collation(text: &[u8], pattern: &[u8]) -> Option<bool> {
    let text = std::str::from_utf8(text).ok()?;
    let pattern = std::str::from_utf8(pattern).ok()?;
    regexp_match(text, pattern).ok()
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
                    Err(EvalError::Conversion(_))
                ),
                "pattern {pattern:?}"
            );
        }
    }

    #[test]
    fn regexp_like_rejects_empty_invalid_pattern_and_match_type() {
        assert!(matches!(
            regexp_like("a", "", ""),
            Err(EvalError::Conversion(_))
        ));
        assert!(matches!(
            regexp_like("a", "[a", ""),
            Err(EvalError::Conversion(_))
        ));
        for pattern in ["(", "(*", "\\"] {
            assert!(matches!(
                regexp_like("", pattern, ""),
                Err(EvalError::Conversion(_))
            ));
        }
        assert!(matches!(
            regexp_like("abc", "abc", "p"),
            Err(EvalError::Conversion(_))
        ));
        assert!(matches!(
            regexp_like("abc", "abc", "cpi"),
            Err(EvalError::Conversion(_))
        ));
    }
}
