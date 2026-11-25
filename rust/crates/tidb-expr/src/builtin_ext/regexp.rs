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

//! Positional regular-expression builtins, ported from
//! `pkg/expression/builtin_regexp.go`.
//!
//! This module owns the scalar value-domain portions of
//! `REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_REPLACE`.  TiDB's Go
//! signatures additionally select collations, issue statement warnings, and
//! expose vectorized/DAG paths. This evaluator accepts evaluated UTF-8 scalar
//! values; its scalar-function caller supplies the source's context-keyed
//! compiled-pattern and replacement-instruction caches.

use std::{
    borrow::{Borrow, Cow},
    ops::Range,
    sync::Arc,
};

use regex::Regex;

use super::BuiltinFuncCache;
use crate::coerce::coerce_str;
use crate::regexp::{
    compile_regexp, find_ascii_folded, is_plain_regexp, PositionalRegexp, RegexpMemorizedSig,
};
use crate::{Datum, EvalError};

const INVALID_INDEX: &str = "Index out of bounds in regular expression search";
const INVALID_RETURN_OPTION: &str =
    "Incorrect arguments to regexp_instr: return_option must be 1 or 0";
const INVALID_SUBSTITUTION: &str = "Substitution number is out of range";

enum MemoizedValue<T> {
    Local(T),
    Cached(Arc<T>),
}

impl<T> AsRef<T> for MemoizedValue<T> {
    fn as_ref(&self) -> &T {
        match self {
            Self::Local(value) => value,
            Self::Cached(value) => value,
        }
    }
}

impl MemoizedValue<RegexpMemorizedSig> {
    fn regexp(&self) -> Result<&PositionalRegexp, EvalError> {
        self.as_ref().result.as_ref().map_err(Clone::clone)
    }
}

/// Values-only regexp dispatch with Go's statement-context memoization. The
/// caller supplies the build-time constness decisions because this tier has
/// already evaluated the argument expressions.
pub(crate) fn dispatch_with_cache(
    name: &str,
    vals: &[impl Borrow<Datum>],
    context_id: u64,
    cache_pattern: bool,
    cache_replacement: bool,
    collation: tidb_charset::Collation,
    regexp_cache: &BuiltinFuncCache<RegexpMemorizedSig>,
    replacement_cache: &BuiltinFuncCache<Vec<ReplacementPart>>,
) -> Option<Result<Datum, EvalError>> {
    let compile = |pattern: &str, match_type: &str| {
        let build = || {
            // Go `getRegexpMatchType`: the derived collation seeds the flags.
            let match_type = crate::regexp::regexp_match_type_with_collation(match_type, collation);
            compile_regexp(pattern, &match_type)
        };
        if !cache_pattern {
            return build().map(|regexp| {
                MemoizedValue::Local(RegexpMemorizedSig { result: Ok(regexp) })
            });
        }
        regexp_cache
            .get_or_init_cache(context_id, || {
                Ok::<_, EvalError>(RegexpMemorizedSig { result: build() })
            })
            .map(MemoizedValue::Cached)
    };
    let resolve_replacement = |value: &str| {
        if !cache_replacement {
            return Ok(MemoizedValue::Local(replacement_parts(value)));
        }
        replacement_cache
            .get_or_init_cache(context_id, || Ok::<_, EvalError>(replacement_parts(value)))
            .map(MemoizedValue::Cached)
    };

    match (name, vals.len()) {
        ("REGEXP_SUBSTR", 2..=5) => Some(regexp_substr_with_compiler(vals, compile)),
        ("REGEXP_INSTR", 2..=6) => Some(regexp_instr_with_compiler(vals, compile)),
        ("REGEXP_REPLACE", 3..=6) => Some(regexp_replace_with_compiler(
            vals,
            compile,
            resolve_replacement,
        )),
        _ => None,
    }
}

fn integer(value: &Datum) -> Result<Option<i64>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(*value)),
        Datum::UInt(value) => i64::try_from(*value)
            .map(Some)
            .map_err(|_| EvalError::IntOverflow),
        _ => Err(EvalError::Unsupported(
            "regular-expression integer argument coercion",
        )),
    }
}

/// Returns the byte offset at which one-indexed character position `pos`
/// starts.  TiDB's non-binary regexp signatures count UTF-8 characters, while
/// Go's regexp engine reports byte offsets internally and converts them back
/// to character positions for `REGEXP_INSTR`.
fn trim_at(text: &str, pos: i64) -> Result<(usize, &str), EvalError> {
    if pos < 1 {
        return Err(crate::regexp::err_regexp(INVALID_INDEX));
    }
    // Position one is valid even for the empty string.
    if pos == 1 {
        return Ok((0, text));
    }
    let Some((byte, _)) = usize::try_from(pos - 1)
        .ok()
        .and_then(|skip| text.char_indices().nth(skip))
    else {
        return Err(crate::regexp::err_regexp(INVALID_INDEX));
    };
    Ok((byte, &text[byte..]))
}

fn optional_string(value: &Datum) -> Result<Option<Cow<'_, str>>, EvalError> {
    let bytes = match value {
        Datum::String(value) => value.bytes(),
        Datum::Bytes(value) => value,
        _ => return coerce_str(value).map(|value| value.map(Cow::Owned)),
    };
    Ok(Some(lossy_text(bytes)))
}

fn lossy_text(bytes: &[u8]) -> Cow<'_, str> {
    if bytes.len() >= 64 {
        if let Ok(text) = simdutf8::basic::from_utf8(bytes) {
            return Cow::Borrowed(text);
        }
    }
    String::from_utf8_lossy(bytes)
}

/// An ASCII literal cannot match an invalid UTF-8 byte or a replacement
/// character introduced by lossy decoding. Count characters only in the
/// prefix needed to return the position.
pub(crate) fn find_ascii_literal(text: &[u8], pattern: &[u8], position: bool) -> Option<Datum> {
    if !pattern.is_ascii() || !is_plain_regexp(pattern) {
        return None;
    }
    let last = pattern.len() - 1;
    let found = text.get(last..).and_then(|ends| {
        // No candidate can start before the first possible final byte.
        let first = memchr::memchr(pattern[last], ends)?;
        if last == 0 {
            return Some(first);
        }
        memchr::memmem::find(&text[first..], pattern).map(|offset| first + offset)
    });
    Some(match (found, position) {
        (Some(byte), true) => Datum::Int(1 + lossy_text(&text[..byte]).chars().count() as i64),
        (Some(_), false) => Datum::new_string_datum(pattern),
        (None, true) => Datum::Int(0),
        (None, false) => Datum::Null,
    })
}

pub(crate) fn replace_plain_literal(text: &[u8], pattern: &[u8], replacement: &[u8]) -> Option<Datum> {
    if !is_plain_regexp(pattern) || memchr::memchr(b'\\', replacement).is_some() {
        return None;
    }
    let pattern = std::str::from_utf8(pattern).ok()?;
    let text = lossy_text(text);
    let replacement = lossy_text(replacement);
    Some(Datum::new_string_datum(text.replace(pattern, replacement.as_ref())))
}

fn required_string(value: &Datum) -> Result<Option<Cow<'_, str>>, EvalError> {
    optional_string(value)
}

fn regexp_substr_with_compiler(
    vals: &[impl Borrow<Datum>],
    compile: impl FnOnce(&str, &str) -> Result<MemoizedValue<RegexpMemorizedSig>, EvalError>,
) -> Result<Datum, EvalError> {
    let (Some(text), Some(pattern)) = (
        required_string(vals[0].borrow())?,
        required_string(vals[1].borrow())?,
    ) else {
        return Ok(Datum::Null);
    };
    let pos = if vals.len() >= 3 {
        let Some(pos) = integer(vals[2].borrow())? else {
            return Ok(Datum::Null);
        };
        pos
    } else {
        1
    };
    let occurrence = if vals.len() >= 4 {
        let Some(occurrence) = integer(vals[3].borrow())? else {
            return Ok(Datum::Null);
        };
        occurrence.max(1)
    } else {
        1
    };
    let match_type = if vals.len() == 5 {
        let Some(match_type) = optional_string(vals[4].borrow())? else {
            return Ok(Datum::Null);
        };
        match_type
    } else {
        Cow::Borrowed("")
    };

    let (_, trimmed) = trim_at(&text, pos)?;
    let regexp = compile(&pattern, &match_type)?;
    let matched = regexp
        .regexp()?
        .find_nth(trimmed, (occurrence - 1) as usize)
        .map(|matched| &trimmed[matched]);
    Ok(matched.map_or(Datum::Null, Datum::new_string_datum))
}

fn regexp_instr_with_compiler(
    vals: &[impl Borrow<Datum>],
    compile: impl FnOnce(&str, &str) -> Result<MemoizedValue<RegexpMemorizedSig>, EvalError>,
) -> Result<Datum, EvalError> {
    let (Some(text), Some(pattern)) = (
        required_string(vals[0].borrow())?,
        required_string(vals[1].borrow())?,
    ) else {
        return Ok(Datum::Null);
    };
    let pos = if vals.len() >= 3 {
        let Some(pos) = integer(vals[2].borrow())? else {
            return Ok(Datum::Null);
        };
        pos
    } else {
        1
    };
    let occurrence = if vals.len() >= 4 {
        let Some(occurrence) = integer(vals[3].borrow())? else {
            return Ok(Datum::Null);
        };
        occurrence.max(1)
    } else {
        1
    };
    let return_option = if vals.len() >= 5 {
        let Some(return_option) = integer(vals[4].borrow())? else {
            return Ok(Datum::Null);
        };
        if return_option != 0 && return_option != 1 {
            return Err(EvalError::Unsupported(INVALID_RETURN_OPTION));
        }
        return_option
    } else {
        0
    };
    let match_type = if vals.len() == 6 {
        let Some(match_type) = optional_string(vals[5].borrow())? else {
            return Ok(Datum::Null);
        };
        match_type
    } else {
        Cow::Borrowed("")
    };

    let (_, trimmed) = trim_at(&text, pos)?;
    let regexp = compile(&pattern, &match_type)?;
    let Some(matched) = regexp.regexp()?.find_nth(trimmed, (occurrence - 1) as usize) else {
        return Ok(Datum::Int(0));
    };

    let byte = if return_option == 0 { matched.start } else { matched.end };
    Ok(Datum::Int(pos + trimmed[..byte].chars().count() as i64))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReplacementPart {
    Group(usize),
    Literal(Vec<u8>),
}

/// TiDB's source does not use `regex.ReplaceAllString`'s `$name` syntax.  It
/// tokenizes a backslash followed by one ASCII digit as a capture reference;
/// every other escaped byte is inserted literally and a trailing backslash is
/// ignored.  This is copied from `getInstructions` in the Go source.
pub(crate) fn replacement_parts(replacement: &str) -> Vec<ReplacementPart> {
    let bytes = replacement.as_bytes();
    let mut parts = Vec::new();
    let mut literal = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            literal.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 1 >= bytes.len() {
            break;
        }
        if bytes[index + 1].is_ascii_digit() {
            if !literal.is_empty() {
                parts.push(ReplacementPart::Literal(std::mem::take(&mut literal)));
            }
            parts.push(ReplacementPart::Group((bytes[index + 1] - b'0') as usize));
        } else {
            literal.push(bytes[index + 1]);
        }
        index += 2;
    }
    if !literal.is_empty() {
        parts.push(ReplacementPart::Literal(literal));
    }
    parts
}

fn render_replacement<'a>(
    output: &mut String,
    matched: &str,
    capture: impl Fn(usize) -> Option<&'a str>,
    parts: &[ReplacementPart],
) -> Result<(), EvalError> {
    for part in parts {
        let text = match part {
            ReplacementPart::Literal(literal) => std::str::from_utf8(literal)
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 regexp replacement"))?,
            ReplacementPart::Group(0) => matched,
            ReplacementPart::Group(group) => {
                capture(*group).ok_or(EvalError::Unsupported(INVALID_SUBSTITUTION))?
            }
        };
        output.push_str(text);
    }
    Ok(())
}

fn replace_matches(
    text: &str,
    start: usize,
    regexp: &PositionalRegexp,
    parts: &[ReplacementPart],
    occurrence: i64,
) -> Result<String, EvalError> {
    let suffix = &text[start..];
    match regexp {
        PositionalRegexp::Literal(finder) => replace_iter(
            text, start, parts, occurrence,
            finder.find_iter(suffix.as_bytes()).map(|at| at..at + finder.needle().len()),
        ),
        PositionalRegexp::AsciiFolded(literal) => replace_iter(
            text, start, parts, occurrence, find_ascii_folded(suffix, literal),
        ),
        PositionalRegexp::Regex(regexp)
            if !parts.iter().any(|part| matches!(part, ReplacementPart::Group(1..))) =>
        {
            replace_iter(
                text, start, parts, occurrence,
                regexp.find_iter(suffix).map(|matched| matched.range()),
            )
        }
        PositionalRegexp::Regex(regexp) => replace_captures(text, start, regexp, parts, occurrence),
    }
}

fn replace_iter(
    text: &str,
    start: usize,
    parts: &[ReplacementPart],
    occurrence: i64,
    matches: impl Iterator<Item = Range<usize>>,
) -> Result<String, EvalError> {
    let mut output = String::with_capacity(text.len());
    let mut copied_until = 0;
    let count = if occurrence == 0 { usize::MAX } else { 1 };
    let skip = usize::try_from(occurrence.max(1) - 1).unwrap_or(usize::MAX);
    for matched in matches.skip(skip).take(count) {
        let absolute = start + matched.start..start + matched.end;
        output.push_str(&text[copied_until..absolute.start]);
        copied_until = absolute.end;
        render_replacement(&mut output, &text[absolute], |_| None, parts)?;
    }
    output.push_str(&text[copied_until..]);
    Ok(output)
}

fn replace_captures(
    text: &str,
    start: usize,
    regexp: &Regex,
    parts: &[ReplacementPart],
    occurrence: i64,
) -> Result<String, EvalError> {
    let suffix = &text[start..];
    let mut locations = regexp.capture_locations();
    let mut previous_end = None;
    let mut output = String::with_capacity(text.len());
    let mut copied_until = 0;
    let mut skip = occurrence.max(1) - 1;
    while let Some(mut matched) =
        regexp.captures_read_at(&mut locations, suffix, previous_end.unwrap_or(0))
    {
        // Both the Go and Rust match iterators skip empty matches at the end
        // of a preceding match. Resume at the next UTF-8 boundary.
        if matched.is_empty() && previous_end == Some(matched.end()) {
            let at = matched.end();
            if at == suffix.len() {
                break;
            }
            let next = suffix.ceil_char_boundary(at + 1);
            let Some(found) = regexp.captures_read_at(&mut locations, suffix, next) else {
                break;
            };
            matched = found;
        }
        previous_end = Some(matched.end());
        if skip > 0 {
            skip -= 1;
            continue;
        }
        output.push_str(&text[copied_until..start + matched.start()]);
        copied_until = start + matched.end();
        render_replacement(
            &mut output,
            matched.as_str(),
            |group| locations.get(group).map(|(lo, hi)| &suffix[lo..hi]),
            parts,
        )?;
        if occurrence != 0 {
            break;
        }
    }
    output.push_str(&text[copied_until..]);
    Ok(output)
}

fn regexp_replace_with_compiler(
    vals: &[impl Borrow<Datum>],
    compile: impl FnOnce(&str, &str) -> Result<MemoizedValue<RegexpMemorizedSig>, EvalError>,
    resolve_replacement: impl FnOnce(&str) -> Result<MemoizedValue<Vec<ReplacementPart>>, EvalError>,
) -> Result<Datum, EvalError> {
    let (Some(text), Some(pattern), Some(replacement)) = (
        required_string(vals[0].borrow())?,
        required_string(vals[1].borrow())?,
        required_string(vals[2].borrow())?,
    ) else {
        return Ok(Datum::Null);
    };
    let pos = if vals.len() >= 4 {
        let Some(pos) = integer(vals[3].borrow())? else {
            return Ok(Datum::Null);
        };
        pos
    } else {
        1
    };
    let occurrence = if vals.len() >= 5 {
        let Some(occurrence) = integer(vals[4].borrow())? else {
            return Ok(Datum::Null);
        };
        if occurrence < 0 {
            1
        } else {
            occurrence
        }
    } else {
        0
    };
    let match_type = if vals.len() == 6 {
        let Some(match_type) = optional_string(vals[5].borrow())? else {
            return Ok(Datum::Null);
        };
        match_type
    } else {
        Cow::Borrowed("")
    };

    let (byte, _) = trim_at(&text, pos)?;
    let regexp = compile(&pattern, &match_type)?;
    let regexp = regexp.regexp()?;
    let parts = resolve_replacement(&replacement)?;
    let output = replace_matches(&text, byte, regexp, parts.as_ref(), occurrence)?;
    Ok(Datum::new_string_datum(output))
}

#[cfg(test)]
mod tests {
    use crate::{Datum, EvalError};

    #[test]
    fn literal_search_preserves_byte_matches_and_character_positions() {
        let text = "界end banana";
        assert_eq!(super::find_ascii_literal(text.as_bytes(), b"ana", true), Some(Datum::Int(7)));
        assert_eq!(
            super::find_ascii_literal(b"\xffneedle", b"needle", false),
            Some(Datum::new_string_datum("needle")),
        );
        assert_eq!(super::find_ascii_literal(b"n", b"needle", true), Some(Datum::Int(0)));
        assert!(super::find_ascii_literal(b"abc", b"a.c", false).is_none());
        assert!(super::find_ascii_literal(b"abc", b"", true).is_none());
        assert!(super::replace_plain_literal(b"abc", b"a", b"\\0").is_none());
    }

    /// Go's builtin test door over a context-free session.
    fn eval_builtin_func(name: &str, vals: &[Datum]) -> Option<Result<Datum, crate::EvalError>> {
        crate::tests::eval_builtin_func(name, vals, &crate::NoColumns)
    }

    fn string(value: &str) -> Datum {
        Datum::new_string_datum(value.to_owned())
    }

    fn call(name: &str, values: &[Datum]) -> Result<Datum, EvalError> {
        eval_builtin_func(name, values).expect("regexp name/arity should dispatch")
    }

    fn assert_string(name: &str, values: &[Datum], expected: Option<&str>) {
        let actual = call(name, values).expect("source scalar row should evaluate");
        let expected = expected.map(string).unwrap_or(Datum::Null);
        assert_eq!(actual, expected, "{name}({values:?})");
    }

    fn assert_int(name: &str, values: &[Datum], expected: Option<i64>) {
        let actual = call(name, values).expect("source scalar row should evaluate");
        let expected = expected.map_or(Datum::Null, Datum::Int);
        assert_eq!(actual, expected, "{name}({values:?})");
    }

    #[test]
    fn regexp_case_insensitive_literals_keep_go_positions_and_folds() {
        let text = "界aAAa☃aa";
        assert_int(
            "REGEXP_INSTR",
            &[string(text), string("Aa"), Datum::Int(1), Datum::Int(2), Datum::Int(1), string("i")],
            Some(6),
        );
        assert_string(
            "REGEXP_REPLACE",
            &[string(text), string("Aa"), string(r"<\0>"), Datum::Int(1), Datum::Int(0), string("i")],
            Some("界<aA><Aa>☃<aa>"),
        );
        for (text, pattern, expected) in [("μKk", "K", "K"), ("ſs界S", "S", "ſ")] {
            assert_string(
                "REGEXP_SUBSTR",
                &[string(text), string(pattern), Datum::Int(1), Datum::Int(1), string("i")],
                Some(expected),
            );
        }
    }

    #[test]
    fn regexp_replace_capture_locations_keep_match_boundaries() {
        for (text, pattern, pos, occurrence, expected) in [
            ("文aa界a💡", "(a*)", 1, 0, "<:>文<aa:aa>界<a:a>💡<:>"),
            ("文aa界a💡", "(a*)", 1, 2, "文<aa:aa>界a💡"),
            ("前catdog catdog", "(^cat|dog)", 2, 1, "前<cat:cat>dog catdog"),
            (
                "aa word! 世界 word? aaword",
                r"\b(word)\b",
                1,
                2,
                "aa word! 世界 <word:word>? aaword",
            ),
        ] {
            assert_string(
                "REGEXP_REPLACE",
                &[
                    string(text),
                    string(pattern),
                    string(r"<\1:\0>"),
                    Datum::Int(pos),
                    Datum::Int(occurrence),
                ],
                Some(expected),
            );
        }
    }

    #[test]
    fn regexp_substr_matches_go_source_vectors() {
        // `pkg/expression/builtin_regexp_test.go:356 TestRegexpSubstr`.
        for (input, pattern, expected) in [
            ("abc", "bc", Some("bc")),
            ("你好", "好", Some("好")),
            ("a", "", None),
        ] {
            if pattern.is_empty() {
                assert!(call("REGEXP_SUBSTR", &[string(input), string(pattern)]).is_err());
            } else {
                assert_string("REGEXP_SUBSTR", &[string(input), string(pattern)], expected);
            }
        }
        assert_string("REGEXP_SUBSTR", &[string("abc"), Datum::Null], None);
        assert_string("REGEXP_SUBSTR", &[Datum::Null, string("bc")], None);
        assert_string("REGEXP_SUBSTR", &[Datum::Null, Datum::Null], None);

        for (input, pattern, pos, expected) in [
            ("abc", "bc", 2, Some("bc")),
            ("你好", "好", 2, Some("好")),
            ("abc", "bc", 3, None),
            ("你好啊", "好", 3, None),
            ("", "^$", 1, Some("")),
        ] {
            assert_string(
                "REGEXP_SUBSTR",
                &[string(input), string(pattern), Datum::Int(pos)],
                expected,
            );
        }
        for (input, pattern, pos) in [
            ("abc", "bc", -1),
            ("abc", "bc", 4),
            ("", "bc", 0),
            ("", "^$", 2),
        ] {
            assert!(call(
                "REGEXP_SUBSTR",
                &[string(input), string(pattern), Datum::Int(pos)]
            )
            .is_err());
        }
        assert_string(
            "REGEXP_SUBSTR",
            &[string(""), string("^$"), Datum::Null],
            None,
        );
        assert_string(
            "REGEXP_SUBSTR",
            &[Datum::Null, string("^$"), Datum::Null],
            None,
        );
        assert_string(
            "REGEXP_SUBSTR",
            &[string(""), Datum::Null, Datum::Null],
            None,
        );

        for (input, pattern, pos, occurrence, expected) in [
            ("abc abd abe", "ab.", 1, 1, Some("abc")),
            ("abc abd abe", "ab.", 1, 0, Some("abc")),
            ("abc abd abe", "ab.", 1, -1, Some("abc")),
            ("abc abd abe", "ab.", 1, 2, Some("abd")),
            ("abc abd abe", "ab.", 3, 1, Some("abd")),
            ("abc abd abe", "ab.", 3, 2, Some("abe")),
            ("abc abd abe", "ab.", 6, 1, Some("abe")),
            ("abc abd abe", "ab.", 6, 100, None),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, Some("嗯嗯")),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 2, Some("嗯好")),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 1, Some("嗯呐")),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 2, None),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 100, None),
        ] {
            assert_string(
                "REGEXP_SUBSTR",
                &[
                    string(input),
                    string(pattern),
                    Datum::Int(pos),
                    Datum::Int(occurrence),
                ],
                expected,
            );
        }
        assert_string(
            "REGEXP_SUBSTR",
            &[string(""), string("^$"), Datum::Int(1), Datum::Null],
            None,
        );
        assert_string(
            "REGEXP_SUBSTR",
            &[Datum::Null, string("^$"), Datum::Int(1), Datum::Null],
            None,
        );

        for (input, pattern, pos, occurrence, match_type, expected) in [
            ("abc", "ab.", 1, 1, "", Some("abc")),
            ("abc", "aB.", 1, 1, "i", Some("abc")),
            ("good\nday", "od", 1, 1, "m", Some("od")),
            ("\n", ".", 1, 1, "s", Some("\n")),
            ("abc", "ab.", 1, 1, "p", None),
        ] {
            let result = call(
                "REGEXP_SUBSTR",
                &[
                    string(input),
                    string(pattern),
                    Datum::Int(pos),
                    Datum::Int(occurrence),
                    string(match_type),
                ],
            );
            if match_type == "p" {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), expected.map_or(Datum::Null, string));
            }
        }
        assert_string(
            "REGEXP_SUBSTR",
            &[
                string("abc"),
                string("ab."),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Null,
            ],
            None,
        );
    }

    #[test]
    fn regexp_instr_matches_go_source_vectors() {
        // `pkg/expression/builtin_regexp_test.go:611 TestRegexpInStr`.
        for (input, pattern, expected) in [
            ("abc", "bc", Some(2)),
            ("你好", "好", Some(2)),
            ("", "^$", Some(1)),
        ] {
            assert_int("REGEXP_INSTR", &[string(input), string(pattern)], expected);
        }
        assert!(call("REGEXP_INSTR", &[string("a"), string("")]).is_err());
        assert_int("REGEXP_INSTR", &[Datum::Null, string("bc")], None);
        assert_int("REGEXP_INSTR", &[string("abc"), Datum::Null], None);

        for (input, pattern, pos, expected) in [
            ("abc", "bc", 2, Some(2)),
            ("你好", "好", 2, Some(2)),
            ("abc", "bc", 3, Some(0)),
            ("你好啊", "好", 3, Some(0)),
            ("", "^$", 1, Some(1)),
        ] {
            assert_int(
                "REGEXP_INSTR",
                &[string(input), string(pattern), Datum::Int(pos)],
                expected,
            );
        }
        for (input, pattern, pos) in [("abc", "bc", -1), ("abc", "bc", 4), ("", "bc", 0)] {
            assert!(call(
                "REGEXP_INSTR",
                &[string(input), string(pattern), Datum::Int(pos)]
            )
            .is_err());
        }

        for (input, pattern, pos, occurrence, expected) in [
            ("abc abd abe", "ab.", 1, 1, 1),
            ("abc abd abe", "ab.", 1, 0, 1),
            ("abc abd abe", "ab.", 1, -1, 1),
            ("abc abd abe", "ab.", 1, 2, 5),
            ("abc abd abe", "ab.", 3, 1, 5),
            ("abc abd abe", "ab.", 3, 2, 9),
            ("abc abd abe", "ab.", 6, 1, 9),
            ("abc abd abe", "ab.", 6, 100, 0),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, 1),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 2, 4),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 1, 7),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 5, 2, 0),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 100, 0),
        ] {
            assert_int(
                "REGEXP_INSTR",
                &[
                    string(input),
                    string(pattern),
                    Datum::Int(pos),
                    Datum::Int(occurrence),
                ],
                Some(expected),
            );
        }
        for (input, pattern, pos, occurrence, return_option, expected) in [
            ("abc abd abe", "ab.", 1, 1, 0, 1),
            ("abc abd abe", "ab.", 1, 1, 1, 4),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, 0, 1),
            ("嗯嗯 嗯好 嗯呐", "嗯.", 1, 1, 1, 3),
            ("", "^$", 1, 1, 0, 1),
            ("", "^$", 1, 1, 1, 1),
        ] {
            assert_int(
                "REGEXP_INSTR",
                &[
                    string(input),
                    string(pattern),
                    Datum::Int(pos),
                    Datum::Int(occurrence),
                    Datum::Int(return_option),
                ],
                Some(expected),
            );
        }
        assert!(call(
            "REGEXP_INSTR",
            &[
                string("abc"),
                string("ab."),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(2),
            ]
        )
        .is_err());

        for (input, pattern, ret, match_type, expected) in [
            ("abc", "ab.", 0, "", Some(1)),
            ("abc", "aB.", 0, "i", Some(1)),
            ("good\nday", "od$", 0, "m", Some(3)),
            ("good\nday", "oD$", 0, "mi", Some(3)),
            ("\n", ".", 0, "s", Some(1)),
            ("abc", "ab.", 0, "p", None),
        ] {
            let result = call(
                "REGEXP_INSTR",
                &[
                    string(input),
                    string(pattern),
                    Datum::Int(1),
                    Datum::Int(1),
                    Datum::Int(ret),
                    string(match_type),
                ],
            );
            if match_type == "p" {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), expected.map_or(Datum::Null, Datum::Int));
            }
        }
        assert_int(
            "REGEXP_INSTR",
            &[
                string("abc"),
                string("ab."),
                Datum::Null,
                Datum::Int(1),
                Datum::Int(0),
                Datum::Null,
            ],
            None,
        );
    }

    #[test]
    fn regexp_replace_matches_go_source_vectors() {
        // `pkg/expression/builtin_regexp_test.go:923 TestRegexpReplace`.
        let url1 = "https://go.mail/folder-1/online/ru-en/#lingvo/#1О 50000&price_ashka/rav4/page=/check.xml";
        let url2 = "http://saint-peters-total=меньше 1000-rublyayusche/catalogue/kolasuryat-v-2-kadyirovka-personal/serial_id=0&input_state/apartments/mokrotochki.net/upravda.ru/yandex.ru/GameMain.aspx?mult]/on/orders/50195&text=мыс и орелка в Балаш смотреть онлайн бесплатно в хорошем камбалакс&lr=20030393833539353862643188&op_promo=C-Teaser_id=06d162.html";
        let url_pat = r"^https?://(?:www\.)?([^/]+)/.*$";
        for (input, pattern, replacement, expected) in [
            ("abc abd abe", "ab.", "cz", "cz cz cz"),
            ("你好 好的", "好", "逸", "你逸 逸的"),
            ("", "^$", "123", "123"),
            (
                "stackoverflow",
                "(.{5})(.*)",
                r"\\+\2+\1+\2+\1\",
                r"\+overflow+stack+overflow+stack",
            ),
            (
                "fooabcdefghij fooABCDEFGHIJ",
                "foo(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)",
                r"\\\9\\\8-\7\\\6-\5\\\4-\3\\\2-\1\\",
                r"\i\h-g\f-e\d-c\b-a\ \I\H-G\F-E\D-C\B-A\",
            ),
            ("fool food foo", "foo(.?)", r"\0+\1", "fool+l food+d foo+"),
            (url1, url_pat, r"a\12\13", "ago.mail2go.mail3"),
            (
                url2,
                url_pat,
                r"aaa\1233",
                "aaasaint-peters-total=меньше 1000-rublyayusche233",
            ),
            ("abc", r"\d*", "d", "dadbdcd"),
            ("abc", r"\d*$", "d", "abcd"),
            ("我们", r"\d*", "d", "d我d们d"),
        ] {
            assert_string(
                "REGEXP_REPLACE",
                &[string(input), string(pattern), string(replacement)],
                Some(expected),
            );
        }
        assert_string(
            "REGEXP_REPLACE",
            &[Datum::Null, string("bc"), string("x")],
            None,
        );
        assert_string(
            "REGEXP_REPLACE",
            &[string("abc"), Datum::Null, Datum::Null],
            None,
        );
        assert!(call("REGEXP_REPLACE", &[string("a"), string(""), string("a")]).is_err());

        for (input, pattern, replacement, pos, expected) in [
            ("abc", "ab.", "cc", 1, "cc"),
            ("abc", "bc", "cc", 3, "abc"),
            ("你好", "好", "的", 2, "你的"),
            ("你好啊", "好", "的", 3, "你好啊"),
            ("", "^$", "cc", 1, "cc"),
            ("seafood fool", "foo(.?)", "123", 3, "sea123 123"),
            ("seafood fool", "foo(.?)", "123", 5, "seafood 123"),
            ("seafood fool", "foo(.?)", "123", 10, "seafood fool"),
            ("seafood fool", "foo(.?)", r"z\12", 3, "seazd2 zl2"),
            ("seafood fool", "foo(.?)", r"z\12", 5, "seafood zl2"),
        ] {
            assert_string(
                "REGEXP_REPLACE",
                &[
                    string(input),
                    string(pattern),
                    string(replacement),
                    Datum::Int(pos),
                ],
                Some(expected),
            );
        }
        for (input, pattern, pos) in [
            ("", "^$", 2),
            ("", "^&", 0),
            ("abc", "bc", -1),
            ("abc", "bc", 4),
        ] {
            assert!(call(
                "REGEXP_REPLACE",
                &[string(input), string(pattern), string("a"), Datum::Int(pos)]
            )
            .is_err());
        }

        for (input, pattern, replacement, pos, occurrence, expected) in [
            ("abc abd", "ab.", "cc", 1, 1, "cc abd"),
            ("abc abd", "ab.", "cc", 1, 2, "abc cc"),
            ("abc abd", "ab.", "cc", 1, 0, "cc cc"),
            ("abc abd abe", "ab.", "cc", 3, 2, "abc abd cc"),
            ("abc abd abe", "ab.", "cc", 3, 10, "abc abd abe"),
            ("你好 好啊", "好", "的", 1, 1, "你的 好啊"),
            ("你好 好啊", "好", "的", 3, 1, "你好 的啊"),
            ("seafood fool", "foo(.?)", "123", 1, 1, "sea123 fool"),
            ("seafood fool", "foo(.?)", "123", 1, 2, "seafood 123"),
            ("seafood fool", "foo(.?)", "123", 1, 10, "seafood fool"),
            ("seafood fool", "foo(.?)", r"z\12", 1, 1, "seazd2 fool"),
            ("seafood fool", "foo(.?)", r"z\12", 1, 2, "seafood zl2"),
            ("", "^$", "cc", 1, 1, "cc"),
            ("", "^$", "cc", 1, 2, ""),
            ("", "^$", "cc", 1, -1, "cc"),
            ("abc", r"\d*", "p", 1, 2, "apbc"),
            ("abc", r"\d*$", "p", 1, 1, "abcp"),
            ("我们", r"\d*", "p", 1, 2, "我p们"),
        ] {
            assert_string(
                "REGEXP_REPLACE",
                &[
                    string(input),
                    string(pattern),
                    string(replacement),
                    Datum::Int(pos),
                    Datum::Int(occurrence),
                ],
                Some(expected),
            );
        }

        for (input, pattern, replacement, occurrence, match_type, expected) in [
            ("abc", "ab.", "cc", 0, "", "cc"),
            ("abc", "aB.", "cc", 0, "i", "cc"),
            ("good\nday", "od$", "cc", 0, "m", "gocc\nday"),
            ("good\nday", "oD$", "cc", 0, "mi", "gocc\nday"),
            ("Good\nday", "a(B)", r"a\12", 0, "msi", "Good\nday"),
            ("Good\nday", ".", "cc", 3, "ci", "Goccd\nday"),
            ("seafood fool", "foo(.?)", "的", 2, "m", "seafood 的"),
            ("abc abd abe", "(.)", "cc", 4, "cii", "abcccabd abe"),
            ("\n", ".", "cc", 0, "s", "cc"),
            ("好的 好滴 好~", ".", "的", 0, "msi", "的的的的的的的的"),
        ] {
            assert_string(
                "REGEXP_REPLACE",
                &[
                    string(input),
                    string(pattern),
                    string(replacement),
                    Datum::Int(1),
                    Datum::Int(occurrence),
                    string(match_type),
                ],
                Some(expected),
            );
        }
        assert!(call(
            "REGEXP_REPLACE",
            &[
                string("abc"),
                string("ab."),
                string("cc"),
                Datum::Int(1),
                Datum::Int(0),
                string("p"),
            ]
        )
        .is_err());
        assert_string(
            "REGEXP_REPLACE",
            &[
                string("abc"),
                string("ab."),
                Datum::Null,
                Datum::Int(1),
                Datum::Int(0),
                Datum::Null,
            ],
            None,
        );
    }

    #[test]
    fn regexp_source_helpers_keep_capture_replacement_contract() {
        assert!(matches!(
            crate::tests::builtin("regexp_substr", &[string("a"), string("(")]),
            Err(EvalError::Conversion(_))
        ));
        assert!(matches!(
            crate::tests::builtin("regexp_instr", &[
                string("a"),
                string("a"),
                Datum::Int(1),
                Datum::Int(1),
                Datum::Int(2),
            ]),
            Err(EvalError::Unsupported(
                "Incorrect arguments to regexp_instr: return_option must be 1 or 0"
            ))
        ));
        assert!(matches!(
            crate::tests::builtin("regexp_replace", &[string("abc"), string("(a)"), string(r"\2"),]),
            Err(EvalError::Unsupported(
                "Substitution number is out of range"
            ))
        ));
    }
}
