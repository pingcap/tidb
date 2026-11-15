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

use crate::{AdminStmt, QueryStmt, SelectStmt, SetOprStmt, SetOprTermBody, Stmt};
use tidb_mysql::to_lowercase as identifier_to_lower;

impl Stmt {
    /// Returns whether executing this statement is read-only.
    ///
    /// This is the immutable Rust equivalent of Go's `ast.IsReadOnly` visitor.
    /// The global-variable switch is retained in the API; Rust's parser
    /// normalizes `@@scope.name := value` to the same user-variable assignment
    /// node as Go, so there is no distinct global assignment left to reject.
    pub fn is_read_only(&self, _check_global_vars: bool) -> bool {
        match self {
            Self::Query(query) => query.is_read_only(),
            Self::Admin(admin) => admin.is_read_only(_check_global_vars),
            Self::Dml(_) | Self::Ddl(_) | Self::Session(_) => false,
        }
    }
}

impl QueryStmt {
    fn is_read_only(&self) -> bool {
        match self {
            Self::Select(select) => select.is_read_only(),
            Self::SetOpr(set_operation) => set_operation.is_read_only(),
        }
    }
}

impl SelectStmt {
    fn is_read_only(&self) -> bool {
        self.lock.is_none()
    }
}

impl SetOprStmt {
    fn is_read_only(&self) -> bool {
        self.terms.iter().all(|term| match &term.body {
            SetOprTermBody::Select(select) => select.is_read_only(),
            SetOprTermBody::Nested(set_operation) => set_operation.is_read_only(),
        })
    }
}

impl AdminStmt {
    fn is_read_only(&self, check_global_vars: bool) -> bool {
        match self {
            Self::Explain(explain) => {
                !explain.analyze
                    || explain
                        .statement()
                        .is_none_or(|statement| statement.is_read_only(check_global_vars))
            }
            Self::Trace(trace) => trace.statement.is_read_only(check_global_vars),
            Self::Do(_)
            | Self::ShowBdrRole
            | Self::ShowSlow(_)
            | Self::ShowDdl
            | Self::ShowDdlJobs(_)
            | Self::ShowDdlJobQueries(_)
            | Self::ShowNextRowId(_)
            | Self::Show(_) => true,
            _ => false,
        }
    }
}

/// Back-quotes an identifier, doubling any embedded back-quote
/// (`RestoreNameBackQuotes`).
pub fn back_quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go `ast.RedactedValue`: replaces a secret in redacted output.
pub const REDACTED_VALUE: &str = "xxxxxx";

/// Go `ast.InvalidURLPlaceholder`: replaces a value that cannot be parsed as
/// a URL, because such a value cannot be reliably masked.
pub const INVALID_URL_PLACEHOLDER: &str = "(invalid URL)";

/// Go `ast.RedactURL`: removes userinfo and fragments; for S3-like, GCS and
/// Azure URLs keeps only known query options and masks the sensitive ones;
/// drops the query of every other scheme. A plain local path is unchanged
/// and an invalid URL becomes [`INVALID_URL_PLACEHOLDER`].
pub fn redact_url(value: &str) -> String {
    if !value.contains("://") && !value.contains('?') {
        return value.to_string();
    }
    let Ok(mut u) = tidb_go_url::parse(value) else {
        return INVALID_URL_PLACEHOLDER.to_string();
    };
    u.user = None;
    u.fragment.clear();
    u.raw_fragment.clear();
    // The allowlist maps known options to whether their values are masked.
    // Unknown keys may be new secrets, misspellings or parts of split values.
    let query_keys: &[(&str, bool)] = match identifier_to_lower(&String::from_utf8_lossy(&u.scheme)).as_str() {
        "s3" | "ks3" | "oss" => &[
            // Endpoints can embed userinfo or signed query credentials.
            ("endpoint", true),
            ("region", false),
            ("storage-class", false),
            ("sse", false),
            ("sse-kms-key-id", false),
            ("acl", false),
            ("provider", false),
            ("force-path-style", false),
            ("use-accelerate-endpoint", false),
            ("role-arn", false),
            ("external-id", false),
            ("profile", false),
            ("object-lock-enabled", false),
            ("access-key", true),
            ("secret-access-key", true),
            ("session-token", true),
            ("sse-kms-context", true),
        ],
        "gs" | "gcs" => {
            u.force_query = false;
            &[
                ("endpoint", true),
                ("storage-class", false),
                ("predefined-acl", false),
                ("credentials-file", true),
            ]
        }
        "azure" | "azblob" => &[
            ("account-name", false),
            ("access-tier", false),
            ("encryption-scope", false),
            ("account-key", true),
            ("encryption-key", true),
            ("sas-token", true),
            // Azure endpoints can contain SAS tokens the client uses directly.
            ("endpoint", true),
        ],
        _ => &[],
    };
    if query_keys.is_empty() {
        u.raw_query.clear();
        u.force_query = false;
        return u.string();
    }
    // URL.Query skips undecodable pairs, which may hide a split secret.
    let (mut values, err) = tidb_go_url::parse_query(&u.raw_query);
    if err.is_some() {
        u.raw_query.clear();
        return u.string();
    }
    let keys: Vec<Vec<u8>> = values.0.keys().cloned().collect();
    for key in keys {
        let normalized = tidb_mysql::to_lowercase(&String::from_utf8_lossy(&key).replace('_', "-"));
        match query_keys.iter().find(|(name, _)| *name == normalized) {
            None => values.del(&key),
            Some((_, true)) => {
                // An unescaped '&' can split a JSON context into an allowed
                // key; drop the whole query if any context is invalid JSON.
                if normalized == "sse-kms-context"
                    && values.0[&key]
                        .iter()
                        .any(|v| serde_json::from_slice::<serde_json::Value>(v).is_err())
                {
                    u.raw_query.clear();
                    return u.string();
                }
                values.set(key, REDACTED_VALUE);
            }
            Some((_, false)) => {}
        }
    }
    u.raw_query = values.encode();
    u.string()
}

/// Escapes a string literal's body (backslash and quote doubling) without
/// the `_utf8mb4` charset-introducer prefix the value-expression restore adds —
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

/// Go `strconv.FormatFloat(f, 'e', -1, 64)`: shortest scientific notation, a
/// signed exponent zero-padded to two digits (`1000.0` -> `1e+03`).
pub(crate) fn format_go_float(f: f64) -> String {
    tidb_goruntime::go_strconv::format_float(f, b'e', -1, 64)
}

/// Restores a dotted, back-quoted name path (`db`.`t`).
pub fn push_name_path(out: &mut String, path: &[String]) {
    for (i, part) in path.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(&back_quote(part));
    }
}
