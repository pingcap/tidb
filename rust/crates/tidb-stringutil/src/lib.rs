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

use memchr::{memchr, memchr2, memchr3};
use tidb_goruntime::go_chars;
use tidb_mysql::SqlMode;

/// An invalid quoted string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnquoteError;

/// The package's canonical invalid-syntax error value.
pub const ERR_SYNTAX: UnquoteError = UnquoteError;

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
    let mut value = Vec::with_capacity(4);
    let tail = append_unquoted_char(input, quote, &mut value)?;
    Ok((value, tail))
}

fn append_unquoted_char<'a>(
    input: &'a [u8],
    quote: u8,
    out: &mut Vec<u8>,
) -> Result<&'a [u8], UnquoteError> {
    let first = input[0];
    if first == quote {
        return Err(UnquoteError);
    }
    if first >= 0x80 {
        let width = decode_utf8_prefix(input)
            .filter(|(character, _)| *character != char::REPLACEMENT_CHARACTER)
            .map_or(1, |(_, width)| width);
        out.extend_from_slice(&input[..width]);
        return Ok(&input[width..]);
    }
    if first != b'\\' {
        out.push(first);
        return Ok(&input[1..]);
    }
    let Some((&escaped, tail)) = input[1..].split_first() else {
        return Err(UnquoteError);
    };
    if matches!(escaped, b'_' | b'%') {
        out.push(b'\\');
    }
    out.push(match escaped {
        b'b' => b'\x08',
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'Z' => 0x1a,
        b'0' => 0,
        other => other,
    });
    Ok(tail)
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
    let Some(mut special) = memchr2(b'\\', quote, body) else {
        return Ok(body.to_vec());
    };
    let mut result = Vec::with_capacity(body.len());
    // A second escape near the first favors the scalar decoder over
    // repeatedly searching short literal spans.
    if special < 8
        && body.get(special + 2..).is_some_and(|tail| {
            memchr2(b'\\', quote, &tail[..tail.len().min(6)]).is_some()
        })
    {
        while !body.is_empty() {
            body = append_unquoted_char(body, quote, &mut result)?;
        }
        return Ok(result);
    }
    loop {
        result.extend_from_slice(&body[..special]);
        body = &body[special..];
        body = append_unquoted_char(body, quote, &mut result)?;
        let Some(next) = memchr2(b'\\', quote, body) else {
            result.extend_from_slice(body);
            return Ok(result);
        };
        special = next;
    }
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
    compile_pattern_inner(pattern, escape)
}

/// Handles escapes and wildcards in a Unicode pattern.
pub fn compile_pattern_inner(
    pattern: impl AsRef<[u8]>,
    escape: u8,
) -> (Vec<char>, Vec<PatternType>) {
    let pattern = pattern.as_ref();
    if pattern.len() <= SHORT_PATTERN_BYTES {
        if pattern.is_ascii() {
            if memchr3(escape, b'_', b'%', pattern).is_none() {
                let weights = pattern.iter().copied().map(char::from).collect();
                return (weights, vec![PatternType::Match; pattern.len()]);
            }
            return compile_short_pattern(
                pattern.iter().copied().map(char::from),
                char::from(escape),
            );
        }
        return compile_short_pattern(go_chars(pattern), char::from(escape));
    }
    let mut weights = Vec::new();
    let mut types = Vec::new();
    compile_pattern_into(pattern, escape, &mut weights, &mut types);
    (weights, types)
}

const SHORT_PATTERN_BYTES: usize = 32;

fn compile_short_pattern<T: Copy + PartialEq + From<u8>>(
    units: impl Iterator<Item = T>,
    escape: T,
) -> (Vec<T>, Vec<PatternType>) {
    let mut weights = [T::from(0); SHORT_PATTERN_BYTES];
    let mut types = [PatternType::Match; SHORT_PATTERN_BYTES];
    let mut length = 0;
    walk_pattern(units, escape, |weight, kind| {
        weights[length] = weight;
        types[length] = kind;
        length += 1;
    });
    (weights[..length].to_vec(), types[..length].to_vec())
}

/// Compiles a Unicode wildcard pattern into reusable buffers.
pub fn compile_pattern_into(
    pattern: &[u8],
    escape: u8,
    weights: &mut Vec<char>,
    types: &mut Vec<PatternType>,
) {
    if pattern.is_ascii() {
        if memchr3(escape, b'_', b'%', pattern).is_none() {
            weights.clear();
            weights.reserve(pattern.len());
            weights.extend(pattern.iter().copied().map(char::from));
            types.clear();
            types.resize(pattern.len(), PatternType::Match);
            return;
        }
        compile_pattern_units(
            pattern.iter().copied().map(char::from),
            pattern.len(),
            char::from(escape),
            weights,
            types,
        );
        return;
    }
    let length = if pattern.len() <= 64 {
        pattern.len()
    } else {
        pattern
            .utf8_chunks()
            .map(|chunk| chunk.valid().chars().count() + chunk.invalid().len())
            .sum()
    };
    compile_pattern_units(
        go_chars(pattern),
        length,
        char::from(escape),
        weights,
        types,
    );
}

fn compile_pattern_units<T: Copy + PartialEq + From<u8>>(
    units: impl Iterator<Item = T>,
    capacity: usize,
    escape: T,
    weights: &mut Vec<T>,
    types: &mut Vec<PatternType>,
) {
    weights.resize(capacity, T::from(0));
    types.resize(capacity, PatternType::Match);
    let mut entries = weights.iter_mut().zip(types.iter_mut());
    walk_pattern(units, escape, |weight, kind| {
        let (stored_weight, stored_type) = entries.next().expect("one token per input unit");
        *stored_weight = weight;
        *stored_type = kind;
    });
    let length = capacity - entries.len();
    weights.truncate(length);
    types.truncate(length);
}

/// A run of wildcards can emit its underscores immediately. Consecutive
/// percent tokens collapse into one trailing percent for the run.
fn walk_pattern<T: Copy + PartialEq + From<u8>>(
    mut units: impl Iterator<Item = T>,
    escape: T,
    mut emit: impl FnMut(T, PatternType),
) {
    let percent = T::from(b'%');
    let underscore = T::from(b'_');
    let mut any = false;
    while let Some(mut unit) = units.next() {
        if unit == escape {
            unit = units.next().unwrap_or(unit);
        } else if unit == underscore {
            emit(unit, PatternType::One);
            continue;
        } else if unit == percent {
            any = true;
            continue;
        }
        if any {
            emit(percent, PatternType::Any);
            any = false;
        }
        emit(unit, PatternType::Match);
    }
    if any {
        emit(percent, PatternType::Any);
    }
}

/// Compiles a binary wildcard pattern.
#[inline]
pub fn compile_pattern_binary(pattern: &[u8], escape: u8) -> (Vec<u8>, Vec<PatternType>) {
    compile_pattern_inner_binary(pattern, escape)
}

/// Handles escapes and wildcards in a binary pattern.
#[inline]
pub fn compile_pattern_inner_binary(pattern: &[u8], escape: u8) -> (Vec<u8>, Vec<PatternType>) {
    if pattern.len() <= SHORT_PATTERN_BYTES {
        if memchr3(escape, b'_', b'%', pattern).is_none() {
            return (pattern.to_vec(), vec![PatternType::Match; pattern.len()]);
        }
        return compile_short_pattern(pattern.iter().copied(), escape);
    }
    let mut weights = Vec::new();
    let mut types = Vec::new();
    compile_pattern_binary_into(pattern, escape, &mut weights, &mut types);
    (weights, types)
}

/// Compiles a binary wildcard pattern into reusable buffers.
pub fn compile_pattern_binary_into(
    pattern: &[u8],
    escape: u8,
    weights: &mut Vec<u8>,
    types: &mut Vec<PatternType>,
) {
    if memchr3(escape, b'_', b'%', pattern).is_none() {
        weights.clear();
        weights.extend_from_slice(pattern);
        types.clear();
        types.resize(pattern.len(), PatternType::Match);
        return;
    }
    compile_pattern_units(
        pattern.iter().copied(),
        pattern.len(),
        escape,
        weights,
        types,
    );
}

/// Converts a TiDB `LIKE` pattern to an anchored regular expression using the
/// caller-provided escape byte.
pub fn compile_like_to_regexp(pattern: impl AsRef<[u8]>, escape: u8) -> String {
    let pattern = pattern.as_ref();
    match pattern {
        [] => return "^$".to_owned(),
        [b'%'] if escape != b'%' => return "^.*$".to_owned(),
        [b'_'] if escape != b'_' => return "^.$".to_owned(),
        [byte] if !byte.is_ascii() => return "^\u{fffd}$".to_owned(),
        _ => {}
    }
    let mut result = String::with_capacity(pattern.len() + 2);
    result.push('^');
    walk_pattern(go_chars(pattern), char::from(escape), |weight, kind| {
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
    });
    result.push('$');
    result
}

/// Matches a binary string against a compiled binary wildcard pattern.
pub fn do_match_binary(input: &[u8], weights: &[u8], types: &[PatternType]) -> bool {
    do_match_units(input.iter().copied(), weights, types, |left, right| left == right)
}

/// Matches a Unicode string against a compiled Unicode wildcard pattern.
pub fn do_match(input: impl AsRef<[u8]>, weights: &[char], types: &[PatternType]) -> bool {
    do_match_customized(input, weights, types, |left, right| left == right)
}

/// Matches a Unicode string with caller-provided literal-character equality.
pub fn do_match_customized(
    input: impl AsRef<[u8]>,
    weights: &[char],
    types: &[PatternType],
    matcher: impl Fn(char, char) -> bool,
) -> bool {
    do_match_units(go_chars(input.as_ref()), weights, types, matcher)
}

fn do_match_units<T: Copy, I: Iterator<Item = T> + Clone>(
    mut input: I,
    weights: &[T],
    types: &[PatternType],
    matcher: impl Fn(T, T) -> bool,
) -> bool {
    debug_assert_eq!(weights.len(), types.len());
    let mut index = 0;
    let mut retry = None;
    loop {
        match types.get(index) {
            Some(PatternType::Any) => {
                index += 1;
                if index == types.len() {
                    return true;
                }
                retry = Some((index, input.clone()));
                continue;
            }
            Some(PatternType::One) if input.next().is_some() => {
                index += 1;
                continue;
            }
            Some(PatternType::Match) if input.next().is_some_and(|c| matcher(c, weights[index])) => {
                index += 1;
                continue;
            }
            None if input.next().is_none() => return true,
            _ => {}
        }
        // Let the most recent % consume one more input unit and retry its suffix.
        let Some((suffix, cursor)) = &mut retry else { return false };
        if cursor.next().is_none() {
            return false;
        }
        index = *suffix;
        input = cursor.clone();
    }
}

/// Whether a compiled pattern contains no wildcard token.
pub fn is_exact_match(types: &[PatternType]) -> bool {
    types.iter().all(|kind| *kind == PatternType::Match)
}

/// Creates an independent owned copy of arbitrary Go-string bytes.
pub fn copy(value: &[u8]) -> Vec<u8> {
    value.to_vec()
}

/// A displayable closure.
pub struct StringerFunc<F>(pub F);

impl<F: Fn() -> String> fmt::Display for StringerFunc<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&(self.0)())
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
struct MemoizedString<F> {
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
pub fn memoize_str<F: FnMut() -> String>(function: F) -> impl fmt::Display {
    MemoizedString {
        function: RefCell::new(function),
        result: RefCell::new(String::new()),
    }
}

/// Quotes arbitrary identifier bytes according to `ANSI_QUOTES` mode.
pub fn escape(identifier: &[u8], sql_mode: SqlMode) -> Vec<u8> {
    let quote = if sql_mode.has_ansi_quotes_mode() {
        b'"'
    } else {
        b'`'
    };
    let quotes = memchr::memchr_iter(quote, identifier);
    let count = quotes.clone().count();
    let mut escaped = Vec::with_capacity(identifier.len() + count + 2);
    escaped.push(quote);
    if count == 0 {
        escaped.extend_from_slice(identifier);
    } else if count > identifier.len() / 8 {
        for &byte in identifier {
            escaped.push(byte);
            if byte == quote {
                escaped.push(quote);
            }
        }
    } else {
        let mut start = 0;
        for end in quotes {
            escaped.extend_from_slice(&identifier[start..=end]);
            escaped.push(quote);
            start = end + 1;
        }
        escaped.extend_from_slice(&identifier[start..]);
    }
    escaped.push(quote);
    escaped
}

/// Formats labels in stable key order as `key=value,key=value`.
pub fn build_string_from_labels(labels: &HashMap<String, String>) -> String {
    let mut labels = labels.iter().collect::<Vec<_>>();
    labels.sort_unstable_by_key(|(key, _)| *key);
    let capacity = labels
        .iter()
        .map(|(k, v)| k.len() + v.len() + 2)
        .sum::<usize>()
        .saturating_sub(1);
    let mut out = String::with_capacity(capacity);
    for (i, (key, value)) in labels.into_iter().enumerate() {
        if i != 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push('=');
        out.push_str(value);
    }
    out
}

/// Counts trailing ASCII spaces.
#[inline]
pub fn get_tail_space_count(value: &[u8]) -> i64 {
    let mut remaining = value;
    while let Some((head, tail)) = remaining.split_last_chunk::<8>() {
        let difference = u64::from_le_bytes(*tail) ^ u64::from_le_bytes([b' '; 8]);
        if difference != 0 {
            return (value.len() - remaining.len()) as i64
                + i64::from(difference.leading_zeros() / 8);
        }
        remaining = head;
    }
    let end = remaining
        .iter()
        .rposition(|&byte| byte != b' ')
        .map_or(0, |index| index + 1);
    (value.len() - end) as i64
}

/// Returns the width encoded by the first byte of a UTF-8 sequence.
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
pub fn convert_pos_in_utf8(value: &str, byte_position: i64) -> i64 {
    value[..byte_position as usize].chars().count() as i64 + 1
}

/// Whether a byte is an uppercase ASCII letter.
pub const fn is_upper_ascii(value: u8) -> bool {
    value.is_ascii_uppercase()
}

/// Whether a byte is a lowercase ASCII letter.
pub const fn is_lower_ascii(value: u8) -> bool {
    value.is_ascii_lowercase()
}

/// Whether a byte is an ASCII digit.
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
    let actual_escape = escape.to_ascii_uppercase();
    // An incomplete UTF-8 sequence at the end is only non-ASCII bytes.
    // Go skips them, so lowercasing the ASCII bytes is still equivalent.
    if value.is_ascii()
        || (escape.is_ascii()
            && value.len() >= 64
            && !simdutf8::compat::from_utf8(value).is_err_and(|error| error.error_len().is_some()))
    {
        if escape.is_ascii_alphabetic() && memchr(escape, value).is_some() {
            let mut escaped = false;
            for byte in value {
                escaped = *byte == escape && !escaped;
                *byte = if escaped {
                    actual_escape
                } else {
                    byte.to_ascii_lowercase()
                };
            }
        } else {
            value.make_ascii_lowercase();
        }
        return actual_escape;
    }

    // An ASCII non-letter escape retains its value and has width one.
    // For other bytes, Go advances by the lead-byte width even on malformed UTF-8.
    if escape.is_ascii() && !escape.is_ascii_alphabetic() {
        let mut index = 0;
        while let Some(byte) = value.get_mut(index) {
            let current = *byte;
            if current.is_ascii_uppercase() {
                *byte = current | 0x20;
                index += 1;
            } else {
                index += utf8_len(current);
            }
        }
        return escape;
    }

    let mut escaped = false;
    let mut index = 0;
    while let Some(byte) = value.get_mut(index) {
        let current = *byte;
        if current == escape && !escaped {
            *byte = actual_escape;
            escaped = true;
            index += 1;
        } else {
            *byte = current.to_ascii_lowercase();
            escaped = false;
            index += utf8_len(current);
        }
    }
    actual_escape
}

/// Escapes `?` for a glob path pattern.
pub fn escape_glob_question_mark(value: impl AsRef<[u8]>) -> String {
    let bytes = value.as_ref();
    let mut escaped =
        String::with_capacity(bytes.len() + memchr::memchr_iter(b'?', bytes).count());
    for chunk in bytes.utf8_chunks() {
        let text = chunk.valid();
        if text.contains('?') {
            for ch in text.chars() {
                if ch == '?' {
                    escaped.push('\\');
                }
                escaped.push(ch);
            }
        } else {
            escaped.push_str(text);
        }
        for _ in chunk.invalid() {
            escaped.push(char::REPLACEMENT_CHARACTER);
        }
    }
    escaped
}

/// Go `encoding/json`'s `float64`: the shortest decimal that round-trips,
/// in exponent form (with `e-7`, not `e-07`) below 1e-6 and from 1e21.
pub fn go_json_float(value: f64) -> String {
    let abs = value.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let formatted = tidb_goruntime::go_strconv::format_float(value, b'e', -1, 64);
        // Go cleans `e-07` to `e-7`.
        if let Some(index) = formatted.find("e-0") {
            let mut cleaned = formatted.clone();
            cleaned.replace_range(index..index + 3, "e-");
            return cleaned;
        }
        return formatted;
    }
    tidb_goruntime::go_strconv::format_float(value, b'f', -1, 64)
}

/// Go `json.Marshal` of a value: `encoding/json` escapes `<`, `>`, `&` and the
/// U+2028/U+2029 separators inside strings.
pub fn go_json_marshal<T: serde::Serialize + ?Sized>(value: &T) -> String {
    let compact = serde_json::to_string(value).unwrap_or_default();
    escape_json_html(&compact).unwrap_or(compact)
}

/// The escapes Go `json.Marshal` applies inside strings over serde's compact
/// text: `<`, `>`, `&` and U+2028/U+2029 (none of them can occur outside a
/// string in JSON text).
pub fn go_json_html_escape(compact: &str) -> String {
    escape_json_html(compact).unwrap_or_else(|| compact.to_owned())
}

fn escape_json_html(compact: &str) -> Option<String> {
    let bytes = compact.as_bytes();
    let first_html = memchr3(b'<', b'>', b'&', bytes);
    let prefix = &bytes[..first_html.unwrap_or(bytes.len())];
    let first_separator = memchr(0xe2, prefix).and_then(|start| {
        let suffix = &prefix[start..];
        memchr::memchr2_iter(0xa8, 0xa9, suffix)
            .find(|&end| end >= 2 && suffix[end - 2..end] == [0xe2, 0x80])
            .map(|end| start + end - 2)
    });
    let first = first_separator.or(first_html)?;
    let mut result = String::with_capacity(bytes.len() + 16);
    result.push_str(&compact[..first]);
    let mut copied = first;
    let mut escape_block = |offset, block: &[u8]| {
        for (index, &byte) in block.iter().enumerate() {
            let index = offset + index;
            let (width, escaped) = match byte {
                b'<' => (1, "\\u003c"),
                b'>' => (1, "\\u003e"),
                b'&' => (1, "\\u0026"),
                0xe2 => match bytes.get(index + 1..index + 3) {
                    Some([0x80, 0xa8]) => (3, "\\u2028"),
                    Some([0x80, 0xa9]) => (3, "\\u2029"),
                    _ => continue,
                },
                _ => continue,
            };
            result.push_str(&compact[copied..index]);
            result.push_str(escaped);
            copied = index + width;
        }
    };
    const BLOCK_SIZE: usize = 256;
    for (index, block) in bytes[first..].chunks(BLOCK_SIZE).enumerate() {
        if index == 0 || memchr3(b'<', b'>', b'&', block).is_some() || memchr(0xe2, block).is_some() {
            escape_block(first + index * BLOCK_SIZE, block);
        }
    }
    result.push_str(&compact[copied..]);
    Some(result)
}

/// Go `json.Indent(dst, src, prefix, indent)` over valid JSON text: a newline
/// and the indentation after `{`, `[` and `,`, a space after `:`, and empty
/// objects and arrays kept as `{}` and `[]`. Strings and the source's trailing
/// JSON whitespace are copied unchanged.
#[must_use]
pub fn go_json_indent(src: &str, prefix: &str, indent: &str) -> String {
    let is_space = |c| matches!(c, ' ' | '\t' | '\n' | '\r');
    let src = src.trim_start_matches(is_space);
    if !src.starts_with(['{', '[']) {
        return src.to_owned();
    }
    let body = src.trim_end_matches(is_space);
    let newline = |dst: &mut String, depth: usize| {
        dst.push('\n');
        dst.push_str(prefix);
        for _ in 0..depth {
            dst.push_str(indent);
        }
    };
    let mut dst = String::with_capacity(src.len() * 2);
    let mut need_indent = false;
    let mut depth = 0usize;
    let mut rest = body;
    while let [byte, ..] = rest.as_bytes() {
        if matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
            rest = &rest[1..];
            continue;
        }
        if need_indent && !matches!(byte, b'}' | b']') {
            need_indent = false;
            depth += 1;
            newline(&mut dst, depth);
        }
        match byte {
            // delay indent so that empty object and array are formatted as {} and [].
            b'{' | b'[' => {
                need_indent = true;
            }
            b',' => {
                dst.push(',');
                newline(&mut dst, depth);
                rest = &rest[1..];
                continue;
            }
            b':' => {
                dst.push_str(": ");
                rest = &rest[1..];
                continue;
            }
            b'}' | b']' => {
                if need_indent {
                    // suppress indent in empty object/array
                    need_indent = false;
                } else {
                    depth = depth.saturating_sub(1);
                    newline(&mut dst, depth);
                }
            }
            b'"' => {
                let end = json_string_span(rest.as_bytes());
                dst.push_str(&rest[..end]);
                rest = &rest[end..];
                continue;
            }
            _ => {
                let len = rest.as_bytes()[1..]
                    .iter()
                    .position(|byte| {
                        matches!(
                            byte,
                            b' ' | b'\t' | b'\n' | b'\r' | b'{' | b'}' | b'[' | b']' | b',' | b':' | b'"'
                        )
                    })
                    .map_or(rest.len(), |index| index + 1);
                dst.push_str(&rest[..len]);
                rest = &rest[len..];
                continue;
            }
        }
        dst.push(char::from(*byte));
        rest = &rest[1..];
    }
    dst.push_str(&src[body.len()..]);
    dst
}

fn json_string_span(bytes: &[u8]) -> usize {
    let mut offset = 1;
    while let Some(next) = memchr::memchr2(b'"', b'\\', &bytes[offset..]) {
        let index = offset + next;
        if bytes[index] == b'"' {
            return index + 1;
        }
        offset = (index + 2).min(bytes.len());
        // Scan a bounded window after an escape. Adjacent escapes can then
        // be skipped without repeatedly restarting the byte search.
        let end = (offset + 64).min(bytes.len());
        while offset < end {
            match bytes[offset] {
                b'"' => return offset + 1,
                b'\\' => offset += 2,
                _ => offset += 1,
            }
        }
        offset = offset.min(bytes.len());
    }
    bytes.len()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    /// Go `json.MarshalIndent(v, "", " ")` (verified against Go 1.26): nested
    /// values, empty containers kept inline, strings untouched.
    #[test]
    fn go_json_indent_follows_go() {
        let src = r#"{"a":[1,{"b":"x, y: {z}"}],"e":{},"f":[],"g":"q\"\\"}"#;
        assert_eq!(
            go_json_indent(src, "", " "),
            "{\n \"a\": [\n  1,\n  {\n   \"b\": \"x, y: {z}\"\n  }\n ],\n \"e\": {},\n \"f\": [],\n \"g\": \"q\\\"\\\\\"\n}"
        );
        assert_eq!(go_json_indent("\"success!\"", "", " "), "\"success!\"");
        assert_eq!(go_json_indent(" \t42\r\n", "p", "i"), "42\r\n");
        assert_eq!(go_json_indent(" \n[ 1 ]\t\r\n", "p", "i"), "[\npi1\np]\t\r\n");
        assert_eq!(go_json_html_escape(r#"{"a":"<&>"}"#), r#"{"a":"\u003c\u0026\u003e"}"#);
    }

    #[test]
    fn json_formatting_preserves_escapes_across_scan_boundaries() {
        for gap in 61..=66 {
            let token = format!(r#""\\{}\"µ""#, "x".repeat(gap));
            let input = format!(" [{token},[]] \r\n");
            let expected = format!("[\n. {token},\n. []\n.] \r\n");
            assert_eq!(go_json_indent(&input, ".", " "), expected);
        }

        let text = "x".repeat(254);
        let input = format!("\"<{text}\u{2028}€\u{2029}\"");
        let escaped = format!("\"\\u003c{text}\\u2028€\\u2029\"");
        assert_eq!(go_json_html_escape(&input), escaped);
        assert_eq!(go_json_marshal(&format!("<{text}\u{2028}€\u{2029}")), escaped);
    }

    #[test]
    fn test_unquote() {
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
    }

    #[test]
    fn test_pattern_match() {
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
    fn punctuation_escape_keeps_the_go_utf8_byte_steps() {
        let mut input = b"A\xe7\x81AB\\C".to_vec();
        assert_eq!(
            lower_one_string_excluding_escape_char(&mut input, b'\\'),
            b'\\'
        );
        assert_eq!(input, b"a\xe7\x81Ab\\c");
    }

    #[test]
    fn test_compile_like_to_regexp() {
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
                compile_like_to_regexp(pattern, b'\\'),
                expected,
                "pattern={pattern:?}"
            );
        }
    }

    #[test]
    fn test_compile_like_to_regexp_honors_custom_escape() {
        assert_eq!(compile_like_to_regexp("+%a", b'+'), "^%a$");
        assert_eq!(compile_like_to_regexp("+_a", b'+'), "^_a$");
        assert_eq!(compile_like_to_regexp(r"\%a", b'+'), r"^\\.*a$");
    }

    #[test]
    fn binary_wildcard_runs_keep_escaped_percent_between_runs() {
        use PatternType::{Any, Match, One};

        let tail = br"%_%_\%%_";
        let mut weights = Vec::new();
        let mut kinds = Vec::new();
        for prefix_len in [64, 0] {
            let mut pattern = vec![0xff; prefix_len];
            pattern.extend_from_slice(tail);
            let mut expected_weights = vec![0xff; prefix_len];
            expected_weights.extend_from_slice(b"__%%_%");
            let mut expected_kinds = vec![Match; prefix_len];
            expected_kinds.extend([One, One, Any, Match, One, Any]);
            compile_pattern_binary_into(&pattern, b'\\', &mut weights, &mut kinds);
            assert_eq!(weights, expected_weights);
            assert_eq!(kinds, expected_kinds);
            assert_eq!(
                compile_pattern_binary(&pattern, b'\\'),
                (expected_weights, expected_kinds),
            );
        }
    }

    #[test]
    fn lowercase_preserves_ascii_inside_a_malformed_sequence() {
        let mut text = vec![b'B'; 67];
        text.extend_from_slice(b"\xe7\x81AZ\xf0\x9f");
        let mut expected = vec![b'b'; 67];
        expected.extend_from_slice(b"\xe7\x81Az\xf0\x9f");
        assert_eq!(lower_one_string_excluding_escape_char(&mut text, b'\\'), b'\\');
        assert_eq!(text, expected);
    }

    #[test]
    fn test_is_exact_match() {
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
    fn test_build_string_from_labels() {
        assert_eq!(build_string_from_labels(&HashMap::new()), "");
        let one_label = HashMap::from([("aaa".to_owned(), "bbb".to_owned())]);
        assert_eq!(build_string_from_labels(&one_label), "aaa=bbb");
        let labels = HashMap::from([
            ("ccc".to_owned(), "ddd".to_owned()),
            ("aaa".to_owned(), "bbb".to_owned()),
        ]);
        assert_eq!(build_string_from_labels(&labels), "aaa=bbb,ccc=ddd");
    }

    #[test]
    fn test_escape_glob_question_mark() {
        assert_eq!(escape_glob_question_mark("123"), "123");
        assert_eq!(escape_glob_question_mark("12*3"), "12*3");
        assert_eq!(escape_glob_question_mark("12?"), r"12\?");
        assert_eq!(escape_glob_question_mark("[1-2]"), "[1-2]");
        assert_eq!(escape_glob_question_mark(b"\xf0\x9f?"), r"��\?");
    }

    #[test]
    fn test_memoize_str() {
        let calls = Cell::new(0);
        let memoized = memoize_str(|| {
            calls.set(calls.get() + 1);
            "slow".to_owned()
        });
        assert_eq!(memoized.to_string(), "slow");
        assert_eq!(memoized.to_string(), "slow");
        assert_eq!(calls.get(), 1);
    }
}
