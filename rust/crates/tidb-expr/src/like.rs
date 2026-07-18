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

//! `[NOT] LIKE` pattern matching (`%`/`_` wildcards, `\` escape by
//! default), with both binary and source-registered collation matching.
//! Called directly from `crate::eval_in`'s `Expr::Like` arm.

use std::cmp::Ordering;

use tidb_datatype::Collation;

/// One token of a compiled `LIKE` pattern.
enum Pat {
    /// `%` — any run of zero or more characters.
    Any,
    /// `_` — exactly one character.
    One,
    /// A literal character (including an escaped `%`, `_`, or the
    /// escape character itself).
    Lit(char),
}

/// Compiles a `LIKE` pattern under the given escape character (`escape`
/// mirrors `tidb_ast::Expr::Like::escape`'s own shape exactly: `None`
/// means the real MySQL/TiDB default, `\`; `Some(0)` means `ESCAPE ''`,
/// i.e. NO character triggers escape processing at all — every `\` in
/// the pattern is then just an ordinary literal character, not special;
/// `Some(b)` is an explicit single-byte escape character). `\c` (or
/// `<escape>c`) for any `c` is the literal character `c`; a TRAILING
/// escape character with nothing following it is a literal instance of
/// itself (confirmed via `gorun` for the default `\` case; generalizes
/// naturally to any other escape byte, not separately re-verified).
fn compile_like(pattern: &str, escape: Option<u8>) -> Vec<Pat> {
    let escape_char = match escape {
        None => Some('\\'),
        Some(0) => None,
        Some(b) => Some(b as char),
    };
    let mut out = Vec::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape_char {
            out.push(Pat::Lit(chars.next().unwrap_or(c)));
            continue;
        }
        out.push(match c {
            '%' => Pat::Any,
            '_' => Pat::One,
            other => Pat::Lit(other),
        });
    }
    out
}

/// Matches `text` against a `LIKE` pattern (`%` any run, `_` one character),
/// case-sensitively. Greedy scan with backtracking on the most recent `%`.
/// `escape` is `tidb_ast::Expr::Like::escape` passed straight through —
/// see `compile_like`'s own doc for its exact meaning.
pub(crate) fn like_match(text: &str, pattern: &str, escape: Option<u8>) -> bool {
    like_match_by(text, pattern, escape, |left, right| left == right)
}

/// Matches a pattern using one of the collations translated into
/// `tidb-datatype`.
///
/// Go's `Collator.Pattern().DoMatch` compares one decoded rune at a time;
/// `%` and `_` retain their binary wildcard meaning while literal runes use
/// the collation's weight table. This is deliberately a separate entry point
/// from [`like_match`]: the expression evaluator's current default remains
/// `utf8mb4_bin`, while source-shaped tests can exercise the explicit
/// `utf8mb4_general_ci` and `utf8mb4_unicode_ci` collator contracts without
/// inventing session metadata in the value-only evaluator.
pub fn like_match_with_collation(
    text: &str,
    pattern: &str,
    escape: Option<u8>,
    collation: Collation,
) -> bool {
    like_match_by(text, pattern, escape, |left, right| {
        collation_char_equal(left, right, collation)
    })
}

fn like_match_by<F>(text: &str, pattern: &str, escape: Option<u8>, equal: F) -> bool
where
    F: Fn(char, char) -> bool,
{
    let pat = compile_like(pattern, escape);
    let text: Vec<char> = text.chars().collect();
    let (mut i, mut j) = (0usize, 0usize);
    // Last `%` seen and the text position where it started matching, so a
    // failed match can resume with the `%` consuming one more character.
    let (mut star_j, mut star_i): (Option<usize>, usize) = (None, 0);
    while i < text.len() {
        match pat.get(j) {
            Some(Pat::One) => {
                i += 1;
                j += 1;
            }
            Some(Pat::Lit(c)) if equal(*c, text[i]) => {
                i += 1;
                j += 1;
            }
            Some(Pat::Any) => {
                star_j = Some(j);
                star_i = i;
                j += 1;
            }
            _ => match star_j {
                Some(sj) => {
                    j = sj + 1;
                    star_i += 1;
                    i = star_i;
                }
                None => return false,
            },
        }
    }
    // Any trailing `%` tokens match the empty string.
    while matches!(pat.get(j), Some(Pat::Any)) {
        j += 1;
    }
    j == pat.len()
}

/// Compares one source rune under a TiDB collation.
///
/// The Go general-CI table maps every rune above U+FFFF to one replacement
/// weight, but the Unicode-CI wildcard callback intentionally keeps all
/// supplementary-plane runes distinct; handling that boundary before the
/// byte-level collation helper preserves both source contracts. For the
/// remaining BMP runes, the generated `Collation::compare` tables are the
/// same weights consumed by Go's wildcard callback.
fn collation_char_equal(left: char, right: char, collation: Collation) -> bool {
    if left == right {
        return true;
    }
    if matches!(
        collation,
        Collation::Binary
            | Collation::AsciiBin
            | Collation::Latin1Bin
            | Collation::Utf8Bin
            | Collation::Utf8Mb4Bin
    ) {
        return false;
    }
    if matches!(
        collation,
        Collation::Utf8UnicodeCi | Collation::Utf8Mb4UnicodeCi
    ) && ((left as u32) > 0xFFFF || (right as u32) > 0xFFFF)
    {
        return false;
    }
    let left = left.to_string();
    let right = right.to_string();
    collation.compare(left.as_bytes(), right.as_bytes()) == Ordering::Equal
}

/// Matches TiDB's `ILIKE` seed semantics: ASCII letters compare without case,
/// while every non-ASCII character remains byte/rune-sensitive.  The Go
/// builtin lowercases the value and pattern before handing them to the binary
/// wildcard matcher.  When the escape byte itself is an ASCII letter, the
/// pattern lowercasing must preserve escape markers and return the transformed
/// marker (`'a'` becomes `'A'`); otherwise an escaped wildcard would change
/// meaning.  This helper deliberately owns only that scalar value path.  The
/// Go signature/cache/vectorized chunk state and session collation registry
/// remain outside this seed evaluator.
pub fn ilike_match(text: &str, pattern: &str, escape: u8) -> bool {
    let text = lower_ascii(text);
    let (pattern, escape) = if escape.is_ascii_alphabetic() {
        lower_ascii_excluding_escape(pattern, escape)
    } else {
        (lower_ascii(pattern), escape)
    };
    like_match(&text, &pattern, Some(escape))
}

fn lower_ascii(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_uppercase() {
                ch.to_ascii_lowercase()
            } else {
                ch
            }
        })
        .collect()
}

/// Port `stringutil.LowerOneStringExcludeEscapeChar` without applying Unicode
/// case folding.  The `escaped` state is intentionally carried across an
/// escape marker for exactly one following character, matching Go's byte
/// implementation (including the `AA` -> `Aa` example for escape `A`).
fn lower_ascii_excluding_escape(value: &str, escape: u8) -> (String, u8) {
    let actual_escape = if escape.is_ascii_lowercase() {
        escape.to_ascii_uppercase()
    } else {
        escape
    };
    let mut out = String::with_capacity(value.len());
    let mut escaped = false;
    for ch in value.chars() {
        if ch.is_ascii_uppercase() {
            if ch as u8 == escape && !escaped {
                out.push(ch);
                escaped = true;
                continue;
            }
            out.push(ch.to_ascii_lowercase());
        } else if ch.is_ascii() && ch as u8 == escape && !escaped {
            out.push(actual_escape as char);
            escaped = true;
            continue;
        } else {
            out.push(ch);
        }
        escaped = false;
    }
    (out, actual_escape)
}

#[cfg(test)]
mod tests {
    use tidb_datatype::Collation;

    use super::{ilike_match, like_match_with_collation};

    /// Source rows from `pkg/expression/builtin_like_test.go:100
    /// TestCILike`. Each row is run against the two collations represented by
    /// the Rust datatype registry; the original 0900 column remains an
    /// explicit unsupported registry boundary (its one distinct U+2C25/U+2C55
    /// result is not silently collapsed into Unicode 4.0 semantics).
    #[test]
    fn ci_like_source_vectors_cover_general_and_unicode_collations() {
        let rows = [
            ("a", "", false, false),
            ("a", "a", true, true),
            ("a", "á", true, true),
            ("a", "b", false, false),
            ("aA", "Aa", true, true),
            ("áAb", "Aa%", true, true),
            ("áAb", "%ab%", true, true),
            ("áAb", "%ab", true, true),
            ("ÀAb", "aA_", true, true),
            ("áééá", "a_%a", true, true),
            ("áééá", "a%_a", true, true),
            ("áéá", "a_%a", true, true),
            ("áéá", "a%_a", true, true),
            ("áá", "a_%a", false, false),
            ("áá", "a%_a", false, false),
            ("áééáííí", "a_%a%", true, true),
            ("数汉据字库", "数%据_库", true, true),
            ("ß", "s%", true, false),
            ("ß", "%s", true, false),
            ("ß", "ss", false, false),
            ("ß", "s", true, false),
            ("ss", "%ß%", true, false),
            ("ß", "_", true, true),
            ("ß", "__", false, false),
            ("Ⱕ", "ⱕ", false, false),
        ];
        for (input, pattern, general, unicode) in rows {
            assert_eq!(
                like_match_with_collation(input, pattern, Some(0), Collation::Utf8Mb4GeneralCi,),
                general,
                "general-ci input={input:?}, pattern={pattern:?}"
            );
            assert_eq!(
                like_match_with_collation(input, pattern, Some(0), Collation::Utf8Mb4UnicodeCi,),
                unicode,
                "unicode-ci input={input:?}, pattern={pattern:?}"
            );
        }
    }

    #[test]
    fn ci_like_keeps_supplementary_unicode_runes_distinct() {
        // General-CI intentionally folds supplementary runes to U+FFFD, but
        // Unicode-CI's wildcard callback compares them by identity. This is
        // the boundary that a whole-string `Collation::compare` shortcut gets
        // wrong.
        assert!(like_match_with_collation(
            "𐀀",
            "𐀀",
            Some(0),
            Collation::Utf8Mb4GeneralCi
        ));
        assert!(like_match_with_collation(
            "𐀀",
            "𐀁",
            Some(0),
            Collation::Utf8Mb4GeneralCi
        ));
        assert!(!like_match_with_collation(
            "𐀀",
            "𐀁",
            Some(0),
            Collation::Utf8Mb4UnicodeCi
        ));
    }

    /// Source rows from `pkg/expression/builtin_ilike_test.go:31 TestIlike`.
    /// The Go table's 33 rows run the same value set through general and Unicode
    /// collations; the scalar builtin lowercases ASCII before using its binary
    /// wildcard matcher, so both representable result columns agree here.
    #[test]
    fn ilike_source_vectors() {
        let rows = [
            ("a", "", 0, 0),
            ("a", "a", 0, 1),
            ("ü", "Ü", 0, 0),
            ("a", "á", 0, 0),
            ("a", "b", 0, 0),
            ("aA", "Aa", 0, 1),
            ("áAb", "Aa%", 0, 0),
            ("áAb", "%ab%", 0, 1),
            ("", "", 0, 1),
            ("ß", "s%", 0, 0),
            ("ß", "%s", 0, 0),
            ("ß", "ss", 0, 0),
            ("ß", "s", 0, 0),
            ("ss", "%ß%", 0, 0),
            ("ß", "_", 0, 1),
            ("ß", "__", 0, 0),
            ("啊aaa啊啊啊aa", "啊aaa啊啊啊aa", 0, 1),
            // Custom escape rows exercise both lower- and upper-case escape
            // markers and the escaped-marker state machine.
            ("abc", "ABC", b'a', 1),
            ("abc", "ABC", b'A', 0),
            ("aaz", "Aaaz", b'a', 1),
            ("AAz", "AAAAz", b'a', 0),
            ("a", "Aa", b'A', 1),
            ("a", "AA", b'A', 1),
            ("Aa", "AAAA", b'A', 1),
            ("gTp", "AGTAp", b'A', 1),
            ("gTAp", "AGTAap", b'A', 1),
            ("A", "aA", b'a', 1),
            ("a", "aA", b'a', 1),
            ("aaa", "AAaA", b'a', 1),
            ("a啊啊a", "a啊啊A", b'A', 0),
            ("啊aaa啊啊啊aa", "啊aaa啊啊啊aa", b'A', 1),
            ("啊aAa啊啊啊aA", "啊AAA啊啊啊AA", b'a', 1),
            ("啊aaa啊啊啊aa", "啊aaa啊啊啊aa", b'a', 0),
        ];
        for (input, pattern, escape, expected) in rows {
            assert_eq!(
                ilike_match(input, pattern, escape),
                expected == 1,
                "input={input:?}, pattern={pattern:?}, escape={escape:?}"
            );
        }
    }

    #[test]
    fn ilike_zero_escape_lowers_ascii_without_disabling_wildcards() {
        assert!(ilike_match("a%", "A%", 0));
        assert!(ilike_match("a\\b", "A\\B", 0));
        assert!(ilike_match("a_b", "A_B", 0));
    }
}
