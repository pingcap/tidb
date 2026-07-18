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

//! Shared low-level restore primitives (identifier quoting, name-path/list
//! formatting, literal normalization) used across every domain module
//! ([`crate::ddl`], [`crate::dml`], [`crate::select`], [`crate::expr`]) —
//! kept here rather than duplicated or hung off any one domain, since none
//! of these carry domain-specific meaning of their own.

/// Back-quotes an identifier, doubling any embedded back-quote
/// (`RestoreNameBackQuotes`).
pub(crate) fn back_quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Restores a string literal value as `_UTF8MB4'…'` under the default utf8mb4
/// charset (`RestoreStringSingleQuotes`): backslashes are escaped and single
/// quotes are doubled, so the text re-parses to the same value.
pub fn restore_string_literal(value: &str) -> String {
    format!("_UTF8MB4'{}'", escape_string_literal(value))
}

/// Escapes a string literal's body (backslash and quote doubling) without
/// the `_UTF8MB4` charset-introducer prefix `restore_string_literal` adds —
/// used where the Go AST restores a plain quoted string, such as `COMMENT`.
pub(crate) fn escape_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

/// Drops leading zeros from an integer literal's digits, matching how the Go
/// AST restores an integer by its numeric value.
pub(crate) fn normalize_int(digits: &str) -> String {
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Normalizes a decimal literal's text: a leading `.` gains a `0` prefix
/// (`.5` -> `0.5`), and leading integer-part zeros are removed
/// (`00.0001000` -> `0.0001000`), matching the Go AST. Trailing zeros are
/// preserved. Then clamped to real MySQL/TiDB's own internal storage limit (see
/// [`clamp_decimal_magnitude`]).
pub(crate) fn normalize_decimal(text: &str) -> String {
    let text = if let Some(rest) = text.strip_prefix('.') {
        format!("0.{rest}")
    } else {
        text.to_string()
    };
    let text = if let Some((integer, fraction)) = text.split_once('.') {
        format!("{}.{}", normalize_int(integer), fraction)
    } else {
        normalize_int(&text)
    };
    clamp_decimal_magnitude(&text)
}

/// Clamps a decimal literal's DIGIT COUNT to real MySQL/TiDB's internal
/// storage limit — 9 "words" of 9 digits each (81 digits total), split
/// between the integer and fraction parts, with the integer part alone
/// capped at 9 words (81 digits) — read from real TiDB's own
/// literal-parsing pipeline, not guessed from restore text alone:
/// `pkg/types/mydecimal.go`'s `MyDecimal.FromString`/`fixWordCntError`
/// (the internal `[9]int32` word buffer and its overflow/truncate rule) and
/// `pkg/parser/lexer_helpers.go`'s `toDecimal` (which catches the resulting
/// error and decides what replaces the literal). Two distinct outcomes,
/// confirmed via `godump restore` across boundary cases (81/82-digit
/// integers, 72/73-digit fractions):
/// - integer part alone needs MORE than 9 words (>81 digits): `ErrOverflow`
///   — the token's `toDecimal` handler discards the parsed value entirely
///   and substitutes `mysql.DefaultDecimal`, a fixed constant (65 nines,
///   NOT a computed bound — confirmed via `pkg/parser/mysql/const.go`).
/// - otherwise, if integer-words + fraction-words together exceed 9:
///   `ErrTruncated` (silently swallowed by `ast.NewDecimal`, so parsing
///   still succeeds) — the integer part is kept EXACTLY as written, and
///   the fraction is truncated (no rounding) to whatever whole-word budget
///   remains: `(9 - wordsInt) * 9` digits, which may be 0 (dropping the
///   fraction, and its `.`, entirely — confirmed via `godump restore`: an
///   81-digit integer plus any fraction restores with the fraction and dot
///   both gone).
///
/// Deliberately scoped to RESTORE fidelity only, matching this crate's
/// established "parse/restore correctness, evaluation stays exact"
/// boundary (`tidb_datatype::Decimal`'s own doc: "exact for any precision
/// DECIMAL supports" — extending real MySQL/TiDB's magnitude clamping to
/// arithmetic RESULTS, not just literal restore, is a separate, much
/// larger design decision left alone here).
fn clamp_decimal_magnitude(text: &str) -> String {
    const WORD_DIGITS: usize = 9;
    const MAX_WORDS: usize = 9;
    const OVERFLOW_VALUE: &str =
        "99999999999999999999999999999999999999999999999999999999999999999";

    let words = |digits: usize| digits.div_ceil(WORD_DIGITS);
    let (int_part, frac_part) = text.split_once('.').unwrap_or((text, ""));
    let words_int = words(int_part.len());
    if words_int > MAX_WORDS {
        return OVERFLOW_VALUE.to_string();
    }
    let words_frac = words(frac_part.len());
    if words_int + words_frac <= MAX_WORDS {
        return text.to_string();
    }
    let max_frac_digits = (MAX_WORDS - words_int) * WORD_DIGITS;
    if max_frac_digits == 0 {
        int_part.to_string()
    } else {
        format!("{int_part}.{}", &frac_part[..max_frac_digits])
    }
}

/// Formats a float like Go's `strconv.FormatFloat(f, 'e', -1, 64)`: shortest
/// scientific notation, lowercase `e`, a signed exponent zero-padded to at least
/// two digits (`1000.0` -> `1e+03`, `0.0025` -> `2.5e-03`).
pub(crate) fn format_go_float(f: f64) -> String {
    // Rust's `{:e}` gives the same shortest mantissa but a bare exponent
    // (`1e3`); reformat the exponent to Go's signed, zero-padded form.
    let s = format!("{f:e}");
    let (mantissa, exp) = s.split_once('e').unwrap_or((s.as_str(), "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let sign = if exp < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exp.abs())
}

/// Restores a dotted, back-quoted name path (`db`.`t`).
pub(crate) fn push_name_path(out: &mut String, path: &[String]) {
    for (i, part) in path.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(&back_quote(part));
    }
}
