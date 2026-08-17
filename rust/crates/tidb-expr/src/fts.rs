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

//! Full-text-search to `LIKE` fallback helpers, transcreated from Go
//! `pkg/expression/fts_to_like.go`.
//!
//! # Partial port — scope and what is missing
//!
//! This file is a PARTIAL port of `pkg/expression/fts_to_like.go`. It covers
//! the dependency-free tokenizing/validation half of that Go file:
//!
//! - `parseFTSBooleanSearchString` (Go lines 39-49) -> [`parse_fts_boolean_search_string`]
//! - `parseFTSSearchTerm` (Go lines 54-65) -> [`parse_fts_search_term`]
//! - `isFTSWordByte` (Go lines 71-73) -> [`is_fts_word_byte`]
//! - `escapeFTSLikePattern` (Go lines 77-98) -> [`escape_fts_like_pattern`]
//! - `ValidateFTSSearchStringForLikeFallback` (Go lines 119-142) ->
//!   [`validate_fts_search_string_for_like_fallback`]
//!
//! NOT ported here (they need the expression-building surface that this crate
//! does not yet expose — see the boundary notes below):
//!
//! - `BuildFTSToILikeExpression` (Go lines 165-202) and its two mode helpers
//!   `buildFTSBooleanModeILikeExpression` (Go lines 218-314) /
//!   `buildFTSNaturalLanguageModeILikeExpression` (Go lines 319-345),
//!   plus `ftsZeroIntConst` (Go lines 207-212).
//! - `BuildFTSToILikeExpressionFromBuiltin` (Go lines 362-397).
//! - `buildFTSILikePredicate` (Go lines 401-438).
//!
//! Correspondingly, the Go test file `pkg/expression/fts_to_like_test.go`
//! contributes six test functions; the four that exercise the ported half are
//! ported below. The two that are NOT ported are
//! `TestBuildFTSToILikeExpressionFromBuiltin` (Go test lines 240-311) and
//! `TestScalarExprSupportedByFlashRejectsNonDefaultFTSModifier` (Go test lines
//! 313-340).
//!
//! # Boundaries and narrowings
//!
//! - **`BuildContext` / `NewFunction` boundary.** The unported builders call
//!   `NewFunction(ctx, ast.Ilike | ast.Ifnull | ast.UnaryNot, ...)`. Those
//!   function builders, and the `FTSMysqlMatchAgainst` builtin signature
//!   (`builtinFtsMysqlMatchAgainstSig`, plus
//!   `SetFTSMysqlMatchAgainstModifier`) that
//!   `BuildFTSToILikeExpressionFromBuiltin` downcasts to, are not present in
//!   this crate yet, so those functions have no faithful target here.
//! - **Error narrowing.** Go returns
//!   `ErrNotSupportedYet.GenWithStackByArgs(msg)` where `ErrNotSupportedYet`
//!   is `dbterror.ClassExpression.NewStd(mysql.ErrNotSupportedYet)`. This
//!   module returns a local [`FtsLikeFallbackError`] carrying the identical
//!   message argument rather than wiring up terror class registration, which
//!   is unrelated to the tokenizer being ported. The Go tests only assert
//!   error-vs-no-error, so no observable behavior is narrowed away.
//! - **Modifier narrowing.** Go takes `ast.FulltextSearchModifier`, a bitmask.
//!   This crate's ported AST models the same thing as the already-transcreated
//!   [`MatchModifier`] enum; only `IsBooleanMode()` is consulted by the ported
//!   half, and [`MatchModifier::is_boolean_mode`] is its exact counterpart.
//! - **Whitespace splitting.** Go's `strings.Fields` splits on
//!   `unicode.IsSpace`; [`str::split_whitespace`] splits on the Unicode
//!   `White_Space` property, which is the same set. Neither yields empty
//!   fields, so the Go comment about `body[0]` being safe applies unchanged.

use tidb_ast::MatchModifier;

/// Go `ftsSearchTerm` (`pkg/expression/fts_to_like.go:29`): a single token in
/// a boolean-mode FTS search string surviving the strict-subset validator —
/// a plain alphanumeric word optionally prefixed with `+` (required) or `-`
/// (excluded).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FtsSearchTerm {
    /// The token body with any leading operator stripped.
    pub word: String,
    /// Set when the token was written `+word`.
    pub is_required: bool,
    /// Set when the token was written `-word`.
    pub is_excluded: bool,
}

/// Rejection returned by [`validate_fts_search_string_for_like_fallback`],
/// standing in for Go's
/// `ErrNotSupportedYet.GenWithStackByArgs(...)` — see the module-level error
/// narrowing note. The payload is the exact Go message argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsLikeFallbackError {
    /// The message argument Go passes to `GenWithStackByArgs`.
    pub message: String,
}

impl std::fmt::Display for FtsLikeFallbackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FtsLikeFallbackError {}

/// Go `parseFTSBooleanSearchString` (`pkg/expression/fts_to_like.go:39`).
///
/// Splits a boolean-mode search string into terms. Inputs reach this function
/// only after [`validate_fts_search_string_for_like_fallback`] has accepted
/// them, so every whitespace-separated field is either a bare alphanumeric
/// word or `+word`/`-word`. Go returns a nil slice for an empty field list;
/// the Rust counterpart returns an empty `Vec`, which the Go test only
/// observes through its length.
pub fn parse_fts_boolean_search_string(text: &str) -> Vec<FtsSearchTerm> {
    text.split_whitespace().map(parse_fts_search_term).collect()
}

/// Go `parseFTSSearchTerm` (`pkg/expression/fts_to_like.go:54`).
///
/// Parses a single boolean-mode token. The strict-subset validator guarantees
/// `word`, `+word`, or `-word` with an alphanumeric body, so only the leading
/// operator needs interpretation.
pub fn parse_fts_search_term(word: &str) -> FtsSearchTerm {
    match word.as_bytes().first() {
        None => FtsSearchTerm::default(),
        Some(b'+') => FtsSearchTerm {
            word: word[1..].to_owned(),
            is_required: true,
            is_excluded: false,
        },
        Some(b'-') => FtsSearchTerm {
            word: word[1..].to_owned(),
            is_required: false,
            is_excluded: true,
        },
        Some(_) => FtsSearchTerm {
            word: word.to_owned(),
            is_required: false,
            is_excluded: false,
        },
    }
}

/// Go `isFTSWordByte` (`pkg/expression/fts_to_like.go:71`).
///
/// Returns true for alphanumeric ASCII and non-ASCII bytes. Punctuation
/// including underscore is NOT a word character, consistent with MySQL's
/// built-in FTS tokenizer which treats `_` as a word separator. Used by
/// [`validate_fts_search_string_for_like_fallback`] to gate the LIKE rewrite.
pub const fn is_fts_word_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c > 127
}

/// Go `escapeFTSLikePattern` (`pkg/expression/fts_to_like.go:77`).
///
/// Escapes special LIKE characters (`%`, `_`, `\`) in the search term so they
/// are treated as literal characters rather than wildcards. The Go body
/// pre-sizes its builder by counting specials first; the byte-for-byte output
/// is what the test pins, and the same exact reservation is kept here.
pub fn escape_fts_like_pattern(term: &str) -> String {
    let bytes = term.as_bytes();
    let escape_count = bytes
        .iter()
        .filter(|&&ch| ch == b'\\' || ch == b'%' || ch == b'_')
        .count();

    // Go appends raw bytes, so the copy stays byte-wise here too: mapping
    // each byte through `char` would re-encode multi-byte UTF-8 sequences.
    let mut result = Vec::with_capacity(term.len() + escape_count);
    for &ch in bytes {
        if ch == b'\\' || ch == b'%' || ch == b'_' {
            result.push(b'\\');
        }
        result.push(ch);
    }
    // Only ASCII backslashes were inserted, and they can never land inside a
    // multi-byte sequence, so the byte string is still valid UTF-8.
    String::from_utf8(result).expect("escaping only inserts ASCII backslashes between whole bytes")
}

/// Go `ValidateFTSSearchStringForLikeFallback`
/// (`pkg/expression/fts_to_like.go:119`).
///
/// Reports whether `search_text` falls inside the strict subset that the LIKE
/// fallback is allowed to translate. The supported subset is, by mode:
///
/// - Boolean mode: each whitespace-separated token must be `word`, `+word`,
///   or `-word`, where `word` consists of ASCII alphanumeric characters or
///   non-ASCII UTF-8 bytes (the same definition used by [`is_fts_word_byte`]).
/// - Natural-language mode: each whitespace-separated token must be a `word`
///   of the same alphanumeric form (no leading `+`/`-` operators).
///
/// An empty or whitespace-only search string is valid; `BuildFTSToILikeExpression`
/// (not ported here) short-circuits to a constant-0 result for it.
///
/// Anything outside this subset (phrases, `*` prefix, `>` `<` `~` relevance
/// modifiers, `()` grouping, mid-word punctuation like `xx-yy`, etc.) is
/// rejected because MySQL FTS tokenizes those constructs in ways that differ
/// from a substring LIKE match. The planner uses this signal to skip the LIKE
/// fallback for rejected strings; the native `FTSMysqlMatchAgainst` builtin
/// can still serve the query when an FTS index is available.
pub fn validate_fts_search_string_for_like_fallback(
    search_text: &str,
    modifier: MatchModifier,
) -> Result<(), FtsLikeFallbackError> {
    let is_boolean = modifier.is_boolean_mode();
    for token in search_text.split_whitespace() {
        let mut body = token;
        // `split_whitespace` never yields an empty token (consecutive
        // whitespace is collapsed), so the leading byte is safe today. Keep
        // the emptiness guard explicit so the indexing is obviously bounded
        // and the check stays correct if the tokenization ever changes.
        if is_boolean && matches!(body.as_bytes().first(), Some(b'+') | Some(b'-')) {
            body = &body[1..];
        }
        if body.is_empty() || body.as_bytes().iter().any(|&b| !is_fts_word_byte(b)) {
            return Err(FtsLikeFallbackError {
                message: format!(
                    "MATCH...AGAINST search term '{token}' is not supported in the LIKE fallback"
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Local fixture standing in for the Go test's
    /// `ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode)`.
    const NATURAL_MODE: MatchModifier = MatchModifier::None;
    /// Local fixture for
    /// `ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode)`.
    const BOOLEAN_MODE: MatchModifier = MatchModifier::BooleanMode;

    /// Go `TestValidateFTSSearchStringForLikeFallback`
    /// (`pkg/expression/fts_to_like_test.go:27`).
    #[test]
    fn validate_fts_search_string_for_like_fallback_cases() {
        let cases: &[(&str, &str, MatchModifier, bool)] = &[
            // Natural-language mode: plain alphanumeric words only.
            ("natural empty", "", NATURAL_MODE, false),
            ("natural whitespace only", " \t\n ", NATURAL_MODE, false),
            ("natural single word", "MySQL", NATURAL_MODE, false),
            (
                "natural multi word",
                "MySQL tutorial PostgreSQL",
                NATURAL_MODE,
                false,
            ),
            (
                "natural alphanumeric mix",
                "abc123 mysql8",
                NATURAL_MODE,
                false,
            ),
            ("natural rejects mid-word dash", "x-x", NATURAL_MODE, true),
            (
                "natural rejects punctuation suffix",
                "MySQL,",
                NATURAL_MODE,
                true,
            ),
            ("natural rejects + operator", "+word", NATURAL_MODE, true),
            ("natural rejects - operator", "-word", NATURAL_MODE, true),
            ("natural rejects quote", "\"phrase\"", NATURAL_MODE, true),
            ("natural rejects wildcard", "word*", NATURAL_MODE, true),
            ("natural rejects percent", "100%", NATURAL_MODE, true),
            (
                "natural rejects underscore",
                "test_file",
                NATURAL_MODE,
                true,
            ),
            // Boolean mode: plain word, +word, -word with alphanumeric body only.
            ("boolean empty", "", BOOLEAN_MODE, false),
            ("boolean plain word", "MySQL", BOOLEAN_MODE, false),
            ("boolean required word", "+MySQL", BOOLEAN_MODE, false),
            ("boolean excluded word", "-MySQL", BOOLEAN_MODE, false),
            ("boolean mix", "+apple -cherry pie", BOOLEAN_MODE, false),
            ("boolean rejects mid-word dash", "xx-yy", BOOLEAN_MODE, true),
            ("boolean rejects bare operator", "+", BOOLEAN_MODE, true),
            ("boolean rejects bare minus", "-", BOOLEAN_MODE, true),
            ("boolean rejects + after body", "x+y", BOOLEAN_MODE, true),
            ("boolean rejects wildcard", "word*", BOOLEAN_MODE, true),
            (
                "boolean rejects required wildcard",
                "+word*",
                BOOLEAN_MODE,
                true,
            ),
            ("boolean rejects relevance gt", ">word", BOOLEAN_MODE, true),
            ("boolean rejects relevance lt", "<word", BOOLEAN_MODE, true),
            (
                "boolean rejects relevance tilde",
                "~word",
                BOOLEAN_MODE,
                true,
            ),
            (
                "boolean rejects phrase",
                "\"exact phrase\"",
                BOOLEAN_MODE,
                true,
            ),
            (
                "boolean rejects required phrase",
                "+\"required phrase\"",
                BOOLEAN_MODE,
                true,
            ),
            ("boolean rejects grouping", "(word)", BOOLEAN_MODE, true),
            ("boolean rejects percent", "+100%", BOOLEAN_MODE, true),
            // Multi-byte UTF-8 word characters pass (matches is_fts_word_byte > 127 case).
            ("natural utf8 word", "你好", NATURAL_MODE, false),
            ("boolean utf8 word", "+你好", BOOLEAN_MODE, false),
        ];

        for &(name, text, modifier, want_err) in cases {
            let got = validate_fts_search_string_for_like_fallback(text, modifier);
            assert_eq!(got.is_err(), want_err, "case {name}: got {got:?}");
        }
    }

    /// Go `TestParseFTSBooleanSearchString`
    /// (`pkg/expression/fts_to_like_test.go:93`). Covers the strict-subset
    /// inputs the boolean parser is expected to handle in production; inputs
    /// outside the subset are rejected upstream by the validator and never
    /// reach this parser.
    #[test]
    fn parse_fts_boolean_search_string_cases() {
        /// One expected term: `(word, is_required, is_excluded)`.
        type ExpectedTerm = (&'static str, bool, bool);

        let cases: &[(&str, &[ExpectedTerm])] = &[
            (
                "+apple +pie",
                &[("apple", true, false), ("pie", true, false)],
            ),
            (
                "+apple -cherry",
                &[("apple", true, false), ("cherry", false, true)],
            ),
            (
                "word1 word2 word3",
                &[
                    ("word1", false, false),
                    ("word2", false, false),
                    ("word3", false, false),
                ],
            ),
            (
                "word1\t\nword2",
                &[("word1", false, false), ("word2", false, false)],
            ),
            ("", &[]),
            ("   \t\n  ", &[]),
        ];

        for &(input, expected) in cases {
            let result = parse_fts_boolean_search_string(input);
            assert_eq!(
                result.len(),
                expected.len(),
                "number of terms should match for {input:?}"
            );
            for (index, &(word, is_required, is_excluded)) in expected.iter().enumerate() {
                assert_eq!(result[index].word, word, "word should match for {input:?}");
                assert_eq!(
                    result[index].is_required, is_required,
                    "is_required should match for {input:?}"
                );
                assert_eq!(
                    result[index].is_excluded, is_excluded,
                    "is_excluded should match for {input:?}"
                );
            }
        }
    }

    /// Go `TestParseFTSSearchTerm` (`pkg/expression/fts_to_like_test.go:150`).
    #[test]
    fn parse_fts_search_term_cases() {
        let cases: &[(&str, &str, bool, bool)] = &[
            ("+word", "word", true, false),
            ("-word", "word", false, true),
            ("word", "word", false, false),
            ("", "", false, false),
            // Bare operator with no body (caller passes the result through;
            // the upstream validator rejects this case before the parser sees
            // it).
            ("+", "", true, false),
            ("-", "", false, true),
        ];

        for &(input, word, is_required, is_excluded) in cases {
            let result = parse_fts_search_term(input);
            assert_eq!(
                result,
                FtsSearchTerm {
                    word: word.to_owned(),
                    is_required,
                    is_excluded,
                },
                "term should match for {input:?}"
            );
        }
    }

    /// Go `TestEscapeFTSLikePattern`
    /// (`pkg/expression/fts_to_like_test.go:175`).
    #[test]
    fn escape_fts_like_pattern_cases() {
        let cases: &[(&str, &str)] = &[
            ("normal text", "normal text"),
            ("100%", r"100\%"),
            ("test_file", r"test\_file"),
            (r"path\to\file", r"path\\to\\file"),
            ("mix_%_all", r"mix\_\%\_all"),
            (r"\%_", r"\\\%\_"),
            ("", ""),
        ];

        for &(input, expected) in cases {
            assert_eq!(
                escape_fts_like_pattern(input),
                expected,
                "escaped pattern should match for {input:?}"
            );
        }
    }

    /// Multi-byte terms must survive escaping byte-for-byte, mirroring Go's
    /// byte-wise `strings.Builder` loop. No Go test row covers this, but the
    /// validator explicitly admits non-ASCII words, so the escaper must not
    /// mangle them.
    #[test]
    fn escape_fts_like_pattern_preserves_multibyte() {
        assert_eq!(escape_fts_like_pattern("你好_%"), r"你好\_\%");
    }
}
