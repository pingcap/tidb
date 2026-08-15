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

//! String, wildcard-pattern, identifier, and ASCII helpers from
//! `pkg/util/stringutil`.
//!
//! Go strings may contain arbitrary bytes, so byte-oriented operations accept
//! byte slices. Unicode pattern operations use Rust `char`, while binary
//! pattern operations retain the source bytes exactly.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

use tidb_mysql::SqlMode;

/// An invalid quoted string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnquoteError;

impl fmt::Display for UnquoteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid syntax")
    }
}

impl std::error::Error for UnquoteError {}

/// Decodes the first byte or UTF-8 character in a quoted-string body.
///
/// The decoded bytes and the unconsumed suffix are returned separately.
pub fn unquote_char(input: &[u8], quote: u8) -> Result<(Vec<u8>, &[u8]), UnquoteError> {
    let Some(&first) = input.first() else {
        return Err(UnquoteError);
    };
    if first == quote {
        return Err(UnquoteError);
    }
    if first >= 0x80 {
        let width = decode_utf8_prefix(input)
            .filter(|(character, _)| *character != char::REPLACEMENT_CHARACTER)
            .map_or(1, |(_, width)| width);
        return Ok((input[..width].to_vec(), &input[width..]));
    }
    if first != b'\\' {
        return Ok((vec![first], &input[1..]));
    }
    let Some((&escaped, tail)) = input[1..].split_first() else {
        return Err(UnquoteError);
    };
    let value = match escaped {
        b'b' => vec![b'\x08'],
        b'n' => vec![b'\n'],
        b'r' => vec![b'\r'],
        b't' => vec![b'\t'],
        b'Z' => vec![0x1a],
        b'0' => vec![0],
        b'_' | b'%' => vec![b'\\', escaped],
        b'\\' => vec![b'\\'],
        b'\'' | b'"' => vec![escaped],
        other => vec![other],
    };
    Ok((value, tail))
}

fn decode_utf8_prefix(input: &[u8]) -> Option<(char, usize)> {
    let expected = match input[0] {
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return None,
    };
    if input.len() < expected {
        return None;
    }
    std::str::from_utf8(&input[..expected])
        .ok()?
        .chars()
        .next()
        .map(|character| (character, expected))
}

fn decode_go_runes(input: &[u8]) -> Vec<char> {
    let mut runes = Vec::with_capacity(input.len());
    let mut input = input;
    while let Some((&first, _)) = input.split_first() {
        if first < 0x80 {
            runes.push(char::from(first));
            input = &input[1..];
        } else if let Some((character, width)) = decode_utf8_prefix(input) {
            runes.push(character);
            input = &input[width..];
        } else {
            runes.push(char::REPLACEMENT_CHARACTER);
            input = &input[1..];
        }
    }
    runes
}

/// Removes matching single or double quotes and decodes TiDB backslash
/// escapes. The returned bytes are not required to be UTF-8.
pub fn unquote(input: &[u8]) -> Result<Vec<u8>, UnquoteError> {
    if input.len() < 2 {
        return Err(UnquoteError);
    }
    let quote = input[0];
    if input[input.len() - 1] != quote || !matches!(quote, b'\'' | b'"') {
        return Err(UnquoteError);
    }
    let mut body = &input[1..input.len() - 1];
    if !body.contains(&b'\\') && !body.contains(&quote) {
        return Ok(body.to_vec());
    }
    let mut result = Vec::with_capacity(body.len().saturating_mul(3) / 2);
    while !body.is_empty() {
        let (value, tail) = unquote_char(body, quote)?;
        result.extend_from_slice(&value);
        body = tail;
    }
    Ok(result)
}

/// One compiled wildcard-pattern token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PatternType {
    /// A literal character.
    Match,
    /// `_`, matching exactly one character.
    One,
    /// `%`, matching zero or more characters.
    Any,
}

/// Compiles a Unicode wildcard pattern using `escape` as the escape byte.
pub fn compile_pattern(pattern: impl AsRef<[u8]>, escape: u8) -> (Vec<char>, Vec<PatternType>) {
    compile_pattern_with_escape(pattern, Some(char::from(escape)))
}

/// Compiles a Unicode wildcard pattern. `None` disables escape processing.
pub fn compile_pattern_with_escape(
    pattern: impl AsRef<[u8]>,
    escape: Option<char>,
) -> (Vec<char>, Vec<PatternType>) {
    compile_pattern_units(decode_go_runes(pattern.as_ref()), escape)
}

fn compile_pattern_units<T: Copy + PartialEq + From<u8>>(
    units: impl IntoIterator<Item = T>,
    escape: Option<T>,
) -> (Vec<T>, Vec<PatternType>) {
    let units = units.into_iter().collect::<Vec<_>>();
    let mut weights = Vec::with_capacity(units.len());
    let mut types = Vec::with_capacity(units.len());
    let mut index = 0;
    while index < units.len() {
        let mut unit = units[index];
        let kind = if Some(unit) == escape {
            if index + 1 < units.len() {
                index += 1;
                unit = units[index];
            }
            PatternType::Match
        } else if unit == T::from(b'_') {
            if types.last() == Some(&PatternType::Any) {
                *weights.last_mut().expect("Any has a weight") = T::from(b'_');
                *types.last_mut().expect("Any has a type") = PatternType::One;
                unit = T::from(b'%');
                PatternType::Any
            } else {
                PatternType::One
            }
        } else if unit == T::from(b'%') {
            if types.last() == Some(&PatternType::Any) {
                index += 1;
                continue;
            }
            PatternType::Any
        } else {
            PatternType::Match
        };
        weights.push(unit);
        types.push(kind);
        index += 1;
    }
    (weights, types)
}

/// Compiles a binary wildcard pattern.
pub fn compile_pattern_binary(pattern: &[u8], escape: u8) -> (Vec<u8>, Vec<PatternType>) {
    compile_pattern_units(pattern.iter().copied(), Some(escape))
}

/// Converts a TiDB `LIKE` pattern to an anchored regular expression.
#[must_use]
pub fn compile_like_to_regexp(pattern: impl AsRef<[u8]>) -> String {
    let (weights, types) = compile_pattern(pattern, b'\\');
    let mut result = String::with_capacity(weights.len().saturating_mul(2) + 2);
    result.push('^');
    for (weight, kind) in weights.into_iter().zip(types) {
        match kind {
            PatternType::Match => {
                if matches!(
                    weight,
                    '\\' | '.'
                        | '+'
                        | '*'
                        | '?'
                        | '('
                        | ')'
                        | '|'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '^'
                        | '$'
                ) {
                    result.push('\\');
                }
                result.push(weight);
            }
            PatternType::One => result.push('.'),
            PatternType::Any => result.push_str(".*"),
        }
    }
    result.push('$');
    result
}

/// Matches a binary string against a compiled binary wildcard pattern.
#[must_use]
pub fn do_match_binary(input: &[u8], weights: &[u8], types: &[PatternType]) -> bool {
    do_match_units(input, weights, types, |left, right| left == right)
}

/// Matches a Unicode string against a compiled Unicode wildcard pattern.
#[must_use]
pub fn do_match(input: impl AsRef<[u8]>, weights: &[char], types: &[PatternType]) -> bool {
    do_match_customized(input, weights, types, |left, right| left == right)
}

/// Matches a Unicode string with caller-provided literal-character equality.
#[must_use]
pub fn do_match_customized(
    input: impl AsRef<[u8]>,
    weights: &[char],
    types: &[PatternType],
    matcher: impl Fn(char, char) -> bool,
) -> bool {
    let input = decode_go_runes(input.as_ref());
    do_match_units(&input, weights, types, matcher)
}

fn do_match_units<T: Copy>(
    input: &[T],
    weights: &[T],
    types: &[PatternType],
    matcher: impl Fn(T, T) -> bool,
) -> bool {
    debug_assert_eq!(weights.len(), types.len());
    let (mut char_index, mut pattern_index) = (0, 0);
    let (mut next_char_index, mut next_pattern_index) = (0, 0);
    while pattern_index < weights.len() || char_index < input.len() {
        if pattern_index < weights.len() {
            match types[pattern_index] {
                PatternType::Match
                    if char_index < input.len()
                        && matcher(input[char_index], weights[pattern_index]) =>
                {
                    pattern_index += 1;
                    char_index += 1;
                    continue;
                }
                PatternType::One if char_index < input.len() => {
                    pattern_index += 1;
                    char_index += 1;
                    continue;
                }
                PatternType::Any => {
                    next_pattern_index = pattern_index;
                    next_char_index = char_index + 1;
                    pattern_index += 1;
                    continue;
                }
                PatternType::Match | PatternType::One => {}
            }
        }
        if next_char_index > 0 && next_char_index <= input.len() {
            pattern_index = next_pattern_index;
            char_index = next_char_index;
            continue;
        }
        return false;
    }
    true
}

/// Whether a compiled pattern contains no wildcard token.
#[must_use]
pub fn is_exact_match(types: &[PatternType]) -> bool {
    types.iter().all(|kind| *kind == PatternType::Match)
}

/// Creates an independent owned copy of a string.
#[must_use]
pub fn copy(value: &str) -> String {
    value.to_owned()
}

/// Creates an independent owned copy of arbitrary Go-string bytes.
#[must_use]
pub fn copy_bytes(value: &[u8]) -> Vec<u8> {
    value.to_vec()
}

/// A displayable closure.
pub struct StringerFn<F>(RefCell<F>);

impl<F> StringerFn<F> {
    /// Wraps a string-producing closure.
    pub fn new(function: F) -> Self {
        Self(RefCell::new(function))
    }
}

impl<F: FnMut() -> String> fmt::Display for StringerFn<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&(self.0.borrow_mut())())
    }
}

/// A displayable string value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StringerStr(pub String);

impl fmt::Display for StringerStr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Non-thread-safe display wrapper that caches the first nonempty result.
pub struct MemoizedString<F> {
    function: RefCell<F>,
    result: RefCell<String>,
}

impl<F: FnMut() -> String> fmt::Display for MemoizedString<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.result.borrow().is_empty() {
            *self.result.borrow_mut() = (self.function.borrow_mut())();
        }
        formatter.write_str(&self.result.borrow())
    }
}

/// Returns a non-thread-safe memoized string function.
pub fn memoize_str<F: FnMut() -> String>(function: F) -> MemoizedString<F> {
    MemoizedString {
        function: RefCell::new(function),
        result: RefCell::new(String::new()),
    }
}

/// Quotes an identifier according to `ANSI_QUOTES` mode.
#[must_use]
pub fn escape_identifier(identifier: &str, sql_mode: SqlMode) -> String {
    String::from_utf8(escape_identifier_bytes(identifier.as_bytes(), sql_mode))
        .expect("a UTF-8 identifier remains UTF-8 after ASCII quoting")
}

/// Quotes arbitrary identifier bytes according to ANSI_QUOTES mode.
#[must_use]
pub fn escape_identifier_bytes(identifier: &[u8], sql_mode: SqlMode) -> Vec<u8> {
    let quote = if sql_mode.has_ansi_quotes_mode() {
        b'"'
    } else {
        b'`'
    };
    let mut escaped = Vec::with_capacity(identifier.len() + 2);
    escaped.push(quote);
    for &byte in identifier {
        escaped.push(byte);
        if byte == quote {
            escaped.push(byte);
        }
    }
    escaped.push(quote);
    escaped
}

/// Formats labels in stable key order as `key=value,key=value`.
#[must_use]
pub fn build_string_from_labels(labels: &HashMap<String, String>) -> String {
    let mut labels = labels.iter().collect::<Vec<_>>();
    labels.sort_unstable_by_key(|(key, _)| *key);
    labels
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Counts trailing ASCII spaces.
#[must_use]
pub fn get_tail_space_count(value: &[u8]) -> i64 {
    value.iter().rev().take_while(|byte| **byte == b' ').count() as i64
}

/// Returns the width encoded by the first byte of a UTF-8 sequence.
#[must_use]
pub const fn utf8_len(first: u8) -> usize {
    if first & 0x80 == 0 {
        1
    } else {
        first.leading_ones() as usize
    }
}

/// Removes `trimmed_chars` valid UTF-8 characters from the front and returns
/// their byte length.
pub fn trim_utf8_string(value: &mut &str, trimmed_chars: i64) -> i64 {
    let mut removed = 0;
    let mut remaining = *value;
    for _ in 0..trimmed_chars {
        let width = utf8_len(remaining.as_bytes()[0]);
        remaining = &remaining[width..];
        removed += width as i64;
    }
    *value = remaining;
    removed
}

/// Converts a zero-based UTF-8 byte index to a one-based character position.
#[must_use]
pub fn convert_pos_in_utf8(value: &str, byte_position: i64) -> i64 {
    value[..byte_position as usize].chars().count() as i64 + 1
}

/// Whether a byte is an uppercase ASCII letter.
#[must_use]
pub const fn is_upper_ascii(value: u8) -> bool {
    value.is_ascii_uppercase()
}

/// Whether a byte is a lowercase ASCII letter.
#[must_use]
pub const fn is_lower_ascii(value: u8) -> bool {
    value.is_ascii_lowercase()
}

/// Whether a byte is an ASCII digit.
#[must_use]
pub const fn is_numeric_ascii(value: u8) -> bool {
    value.is_ascii_digit()
}

/// Lowercases ASCII letters in place and leaves every other byte unchanged.
pub fn lower_one_string(value: &mut [u8]) {
    for byte in value {
        if is_upper_ascii(*byte) {
            *byte = byte.to_ascii_lowercase();
        }
    }
}

/// Lowercases ASCII letters without changing the meaning of an ASCII-letter
/// escape marker. Returns the possibly uppercased effective escape byte.
pub fn lower_one_string_excluding_escape_char(value: &mut [u8], escape: u8) -> u8 {
    let actual_escape = if is_lower_ascii(escape) {
        escape.to_ascii_uppercase()
    } else {
        escape
    };
    let mut escaped = false;
    let mut index = 0;
    while index < value.len() {
        if is_upper_ascii(value[index]) {
            if value[index] == escape && !escaped {
                escaped = true;
                index += 1;
                continue;
            }
            value[index] = value[index].to_ascii_lowercase();
        } else {
            if value[index] == escape && !escaped {
                escaped = true;
                value[index] = actual_escape;
                index += 1;
                continue;
            }
            index += utf8_len(value[index]).saturating_sub(1);
        }
        escaped = false;
        index += 1;
    }
    actual_escape
}

/// Escapes `?` for a glob path pattern.
#[must_use]
pub fn escape_glob_question_mark(value: impl AsRef<[u8]>) -> String {
    let mut escaped = String::new();
    for character in decode_go_runes(value.as_ref()) {
        if character == '?' {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use tidb_mysql::{ModeANSIQuotes, SqlMode};

    #[test]
    fn unquote_source_vectors() {
        let rows: &[(&[u8], &[u8], bool)] = &[
            (b"", b"", false),
            (b"'", b"", false),
            (b"'abc\"", b"", false),
            (b"abcdea", b"", false),
            (b"'abc'def'", b"", false),
            (b"\"abc\\\"", b"", false),
            (b"\"abcdef\"", b"abcdef", true),
            (b"\"abc'def\"", b"abc'def", true),
            ("\"\\a汉字测试\"".as_bytes(), "a汉字测试".as_bytes(), true),
            ("\"☺\"".as_bytes(), "☺".as_bytes(), true),
            (b"\"\\xFF\"", b"xFF", true),
            (b"\"\\U00010111\"", b"U00010111", true),
            (b"\"\\U0001011111\"", b"U0001011111", true),
            (
                b"\"\\a\\b\\f\\n\\r\\t\\v\\\\\\\"\"",
                b"a\x08f\n\r\tv\\\"",
                true,
            ),
            (b"\"\\Z\\%\\_\"", b"\x1a\\%\\_", true),
            (b"\"abc\\0\"", b"abc\0", true),
            (b"\"abc\\\"abc\"", b"abc\"abc", true),
            (b"'abcdef'", b"abcdef", true),
            (b"'\"'", b"\"", true),
            (b"'\\a\\b\\f\\n\\r\\t\\v\\\\\\''", b"a\x08f\n\r\tv\\'", true),
            (b"' '", b" ", true),
            ("'\\a汉字'".as_bytes(), "a汉字".as_bytes(), true),
            (b"'\\a\x90'", b"a\x90", true),
            (
                "\"\\a\x18èàø»\x05\"".as_bytes(),
                "a\x18èàø»\x05".as_bytes(),
                true,
            ),
        ];
        for (input, expected, valid) in rows {
            let result = unquote(input);
            assert_eq!(result.is_ok(), *valid, "input={input:?}");
            assert_eq!(result.unwrap_or_default(), *expected, "input={input:?}");
        }

        let replacement = char::REPLACEMENT_CHARACTER.to_string();
        let replacement = replacement.as_bytes();
        let (head, tail) = unquote_char(replacement, b'"').unwrap();
        assert_eq!(head, replacement[..1]);
        assert_eq!(tail, &replacement[1..]);
    }

    #[test]
    fn pattern_source_vectors() {
        let rows = [
            ("", "a", b'\\', false),
            ("a", "a", b'\\', true),
            ("a", "b", b'\\', false),
            ("aA", "aA", b'\\', true),
            ("_", "a", b'\\', true),
            ("_", "ab", b'\\', false),
            ("__", "b", b'\\', false),
            ("%", "abcd", b'\\', true),
            ("%", "", b'\\', true),
            ("%b", "AAA", b'\\', false),
            ("%a%", "BBB", b'\\', false),
            ("a%", "BBB", b'\\', false),
            (r"\%a", "%a", b'\\', true),
            (r"\%a", "aa", b'\\', false),
            (r"\_a", "_a", b'\\', true),
            (r"\_a", "aa", b'\\', false),
            (r"\\_a", r"\xa", b'\\', true),
            (r"\a\b", r"\a\b", b'\\', false),
            (r"\a\b", "ab", b'\\', true),
            ("%%_", "abc", b'\\', true),
            ("%_%_aA", "aaaA", b'\\', true),
            ("+_a", "_a", b'+', true),
            ("+%a", "%a", b'+', true),
            (r"\%a", "%a", b'+', false),
            ("++a", "+a", b'+', true),
            ("+a", "a", b'+', true),
            ("++_a", "+xa", b'+', true),
            ("___Հ", "䇇Հ", b'\\', false),
        ];
        for (pattern, input, escape, expected) in rows {
            let (weights, types) = compile_pattern(pattern, escape);
            assert_eq!(
                do_match(input, &weights, &types),
                expected,
                "pattern={pattern:?}"
            );
        }
    }

    #[test]
    fn binary_and_customized_pattern_boundaries_are_preserved() {
        let (weights, types) = compile_pattern_binary(&[0xff, b'%', b'_', 0x80], b'\\');
        assert!(do_match_binary(&[0xff, b'x', b'y', 0x80], &weights, &types));
        assert!(!do_match_binary(&[0xff, 0x80], &weights, &types));

        let (weights, types) = compile_pattern("A_%", b'\\');
        assert!(do_match_customized(
            "aZtail",
            &weights,
            &types,
            |left, right| left.eq_ignore_ascii_case(&right)
        ));

        let (weights, types) = compile_pattern_with_escape(r"\%", None);
        assert!(do_match(r"\abc", &weights, &types));

        let (weights, types) = compile_pattern([0xff, b'_', 0xfe], b'\\');
        assert!(do_match([0x80, b'x', 0xfd], &weights, &types));
    }

    #[test]
    fn regexp_and_exact_match_source_vectors() {
        let regex_rows = [
            ("", "^$"),
            ("a", "^a$"),
            ("aA", "^aA$"),
            ("$a$%", r"^\$a\$.*$"),
            ("a.b%", r"^a\.b.*$"),
            ("a+b", r"^a\+b$"),
            ("_", "^.$"),
            ("__", "^..$"),
            ("%", "^.*$"),
            ("%b", "^.*b$"),
            ("%a%", "^.*a.*$"),
            ("a%", "^a.*$"),
            (r"\%a", "^%a$"),
            (r"\_a", "^_a$"),
            (r"\\_a", r"^\\.a$"),
            (r"\a\b", "^ab$"),
            ("%%_", "^..*$"),
            ("%_%_aA", "^...*aA$"),
        ];
        for (pattern, expected) in regex_rows {
            assert_eq!(
                compile_like_to_regexp(pattern),
                expected,
                "pattern={pattern:?}"
            );
        }
        assert_eq!(compile_like_to_regexp("-&#~"), "^-&#~$");
        let exact_rows = [
            ("", b'\\', true),
            ("_", b'\\', false),
            ("%", b'\\', false),
            ("a", b'\\', true),
            ("a_", b'\\', false),
            ("a%", b'\\', false),
            (r"a\_", b'\\', true),
            (r"a\%", b'\\', true),
            (r"a\\", b'\\', true),
            (r"a\\_", b'\\', false),
            ("a+%", b'+', true),
            (r"a\%", b'+', false),
            ("a++", b'+', true),
            ("a++_", b'+', false),
        ];
        for (pattern, escape, expected) in exact_rows {
            let (_, types) = compile_pattern(pattern, escape);
            assert_eq!(is_exact_match(&types), expected, "pattern={pattern:?}");
        }
    }

    #[test]
    fn labels_glob_and_memoization_source_vectors() {
        assert_eq!(build_string_from_labels(&HashMap::new()), "");
        let one_label = HashMap::from([("aaa".to_owned(), "bbb".to_owned())]);
        assert_eq!(build_string_from_labels(&one_label), "aaa=bbb");
        let labels = HashMap::from([
            ("ccc".to_owned(), "ddd".to_owned()),
            ("aaa".to_owned(), "bbb".to_owned()),
        ]);
        assert_eq!(build_string_from_labels(&labels), "aaa=bbb,ccc=ddd");
        assert_eq!(escape_glob_question_mark("123"), "123");
        assert_eq!(escape_glob_question_mark("12*3"), "12*3");
        assert_eq!(escape_glob_question_mark("12?"), r"12\?");
        assert_eq!(escape_glob_question_mark("[1-2]"), "[1-2]");

        let calls = Cell::new(0);
        let memoized = memoize_str(|| {
            calls.set(calls.get() + 1);
            "slow".to_owned()
        });
        assert_eq!(memoized.to_string(), "slow");
        assert_eq!(memoized.to_string(), "slow");
        assert_eq!(calls.get(), 1);

        let empty_calls = Cell::new(0);
        let empty = memoize_str(|| {
            empty_calls.set(empty_calls.get() + 1);
            String::new()
        });
        assert_eq!(empty.to_string(), "");
        assert_eq!(empty.to_string(), "");
        assert_eq!(empty_calls.get(), 2);
    }

    #[test]
    fn remaining_public_helpers_preserve_source_behavior() {
        assert_eq!(
            escape_identifier("foo `bar`", SqlMode::default()),
            "`foo ``bar```"
        );
        assert_eq!(
            escape_identifier("foo \"bar\"", ModeANSIQuotes),
            "\"foo \"\"bar\"\"\""
        );
        assert_eq!(
            escape_identifier_bytes(&[0xff, b'`'], SqlMode::default()),
            [b'`', 0xff, b'`', b'`', b'`']
        );
        assert_eq!(get_tail_space_count(b"x   "), 3);
        assert_eq!(utf8_len(b'a'), 1);
        assert_eq!(utf8_len("你".as_bytes()[0]), 3);
        let mut value = "你好x";
        assert_eq!(trim_utf8_string(&mut value, 2), 6);
        assert_eq!(value, "x");
        assert_eq!(convert_pos_in_utf8("你好", 3), 2);
        assert!(is_upper_ascii(b'A'));
        assert!(is_lower_ascii(b'z'));
        assert!(is_numeric_ascii(b'7'));

        let mut lower = "AéB".as_bytes().to_vec();
        lower_one_string(&mut lower);
        assert_eq!(lower, "aéb".as_bytes());
        let mut escaped = b"AAAA".to_vec();
        assert_eq!(
            lower_one_string_excluding_escape_char(&mut escaped, b'A'),
            b'A'
        );
        assert_eq!(escaped, b"AaAa");
        let mut escaped = b"ABC".to_vec();
        assert_eq!(
            lower_one_string_excluding_escape_char(&mut escaped, b'a'),
            b'A'
        );
        assert_eq!(escaped, b"abc");
        assert_eq!(copy("copy"), "copy");
        assert_eq!(copy_bytes(&[0xff, 0]), [0xff, 0]);
        assert_eq!(StringerFn::new(|| "fn".to_owned()).to_string(), "fn");
        assert_eq!(StringerStr("str".to_owned()).to_string(), "str");
    }
}
