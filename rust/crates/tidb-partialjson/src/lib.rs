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

//! Complete transcreation of Go `pkg/util/partialjson` (`extract.go`).
//!
//! Requested top-level members are captured as ordered JSON decoder tokens.
//! Parsing stops after the last requested member, duplicate names keep their
//! first value, and an empty name set never reads the input. Malformed input
//! before the last requested member still fails, with native `serde_json`
//! error text.
//! Like Go `encoding/json`, invalid UTF-8 and unpaired UTF-16 surrogates in
//! JSON strings become U+FFFD. The parser finds token boundaries on the
//! original byte slice and repairs affected strings within those boundaries.
//! A compatibility retry preserves the decoder's scalar cutoff and errors.
//! `BUILD.bazel`'s `fastjson` and `partialjson` aliases both map to this module.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Read};

use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;

/// Go `encoding/json.Token` values returned by the package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonToken {
    /// `{`, `}`, `[`, or `]`.
    Delim(char),
    /// A JSON string, including object member names.
    String(String),
    /// A number in its original lexical representation (`json.Number`).
    Number(String),
    /// A JSON boolean.
    Bool(bool),
    /// JSON `null`.
    Null,
}

/// Error returned by [`extract_top_level_members`].
#[derive(Debug, Clone)]
pub struct PartialJsonError(String);

impl fmt::Display for PartialJsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PartialJsonError {}

struct ExtractSeed<'out, 'names> {
    remain: HashSet<&'names str>,
    out: &'out mut HashMap<String, Vec<JsonToken>>,
    done: &'out Cell<bool>,
}

impl<'de> Visitor<'de> for ExtractSeed<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The counterpart of Go's "expected '{' for topLevelJSONTokenIter".
        f.write_str("a top-level JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        while !self.remain.is_empty() {
            let Some(name) = map.next_key::<String>()? else {
                // The object ended before every requested name was found; Go
                // surfaces the iterator's io.EOF here.
                return Err(serde::de::Error::custom(
                    "EOF before all requested top-level members were found",
                ));
            };
            // `remove` is true only for the FIRST occurrence of a requested
            // name, so a duplicate is discarded exactly as in Go.
            if self.remain.remove(name.as_str()) {
                let value = map.next_value::<Box<RawValue>>()?;
                let tokens = tokenize(value.get()).map_err(serde::de::Error::custom)?;
                self.out.insert(name, tokens);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        // All members captured; anything the deserializer trips over past this
        // point is in the tail Go never reads.
        self.done.set(true);
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for ExtractSeed<'_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

/// Extracts the given top-level members from a JSON object. It stops parsing
/// as soon as all names are found. Port of Go's `ExtractTopLevelMembers`; each
/// member is returned as the equivalent of Go's `[]json.Token`.
///
/// # Errors
///
/// Fails when the top level is not an object, the JSON before the last
/// requested member is malformed, or the object ends before every requested
/// name is found.
pub fn extract_top_level_members(
    content: &[u8],
    names: &[&str],
) -> Result<HashMap<String, Vec<JsonToken>>, PartialJsonError> {
    // Go's loop condition is `len(remainNames) > 0`; with no names it never
    // touches the iterator, so even invalid content succeeds.
    if names.is_empty() {
        return Ok(HashMap::new());
    }

    let mut repaired = false;
    match extract_borrowed_members(content, names, &mut repaired) {
        Ok(values) => Ok(values),
        Err(error) if !repaired && !error.is_syntax() => Err(PartialJsonError(error.to_string())),
        Err(error) => {
            let done = Cell::new(false);
            let mut reader = JsonTokenReader::new(content, Some(&done));
            let mut de = serde_json::Deserializer::from_reader(&mut reader);
            extract_repaired_members(&mut de, names, &done).map_err(|retry_error| {
                let error = if reader.repaired { retry_error } else { error };
                PartialJsonError(error.to_string())
            })
        }
    }
}

fn extract_borrowed_members(
    content: &[u8],
    names: &[&str],
    repaired: &mut bool,
) -> Result<HashMap<String, Vec<JsonToken>>, serde_json::Error> {
    let mut rest = consume_separator(content, b'{', "expected a top-level JSON object")?;
    let mut remain: HashSet<_> = names.iter().copied().collect();
    let mut out = HashMap::with_capacity(names.len());

    loop {
        rest = skip_json_space(rest);
        if rest.first() != Some(&b'"') {
            return Err(serde::de::Error::custom("expected a JSON member name"));
        }
        let (name, key_len) = read_member_name(rest, repaired)?;
        rest = consume_separator(&rest[key_len..], b':', "expected ':'")?;
        rest = skip_json_space(rest);

        let consumed = if remain.remove(name.as_str()) {
            let (tokens, len) = read_member_value(rest, repaired)?;
            out.insert(name, tokens);
            // Check the next separator only when a caller needs another member.
            if remain.is_empty() {
                return Ok(out);
            }
            len
        } else {
            skip_member_value(rest)?
        };
        rest = consume_separator(&rest[consumed..], b',', "expected another JSON member")?;
    }
}

fn read_member_name(input: &[u8], repaired: &mut bool) -> Result<(String, usize), serde_json::Error> {
    let mut keys = serde_json::Deserializer::from_slice(input).into_iter::<String>();
    match keys.next().transpose() {
        Ok(Some(name)) => Ok((name, keys.byte_offset())),
        Ok(None) => Err(serde::de::Error::custom(
            "EOF before all requested top-level members were found",
        )),
        Err(error) if error.is_syntax() => {
            let Ok(len) = skip_member_value(input) else {
                return Err(error);
            };
            let name = repair_quoted_string(&input[..len]).map_err(|_| error)?;
            *repaired = true;
            Ok((name, len))
        }
        Err(error) => Err(error),
    }
}

fn read_member_value(
    input: &[u8],
    repaired: &mut bool,
) -> Result<(Vec<JsonToken>, usize), serde_json::Error> {
    if matches!(input.first(), Some(b'"' | b'{' | b'[')) {
        // These values have their own closing delimiter. The syntax-only
        // pass can find it even if a string contains invalid UTF-8.
        let len = skip_member_value(input)?;
        let value = &input[..len];
        // Check small values together. For large ones, validate only the
        // strings and numbers that actually need to become owned text.
        let utf8 = if len <= 64 * 1024 {
            std::str::from_utf8(value).ok()
        } else {
            None
        };
        let tokens = tokenize_bytes(value, utf8, repaired).map_err(serde::de::Error::custom)?;
        return Ok((tokens, len));
    }

    // A scalar may end before the outer separator, which serde_json's
    // streaming iterator would try to validate before returning it.
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let value = <&RawValue>::deserialize(&mut deserializer)?;
    let tokens = tokenize(value.get()).map_err(serde::de::Error::custom)?;
    Ok((tokens, value.get().len()))
}

fn skip_member_value(input: &[u8]) -> Result<usize, serde_json::Error> {
    let mut values = serde_json::Deserializer::from_slice(input).into_iter::<IgnoredAny>();
    values
        .next()
        .transpose()?
        .ok_or_else(|| serde::de::Error::custom("expected a JSON member value"))?;
    Ok(values.byte_offset())
}

#[inline]
fn skip_json_space(input: &[u8]) -> &[u8] {
    match input {
        [b' ' | b'\t' | b'\r' | b'\n', rest @ ..] => skip_json_space_run(rest),
        _ => input,
    }
}

fn skip_json_space_run(mut input: &[u8]) -> &[u8] {
    const LOW_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    const HIGH_BITS: u64 = !LOW_BITS;

    // In each byte, adding 0x7f to its low seven bits sets the high bit
    // unless those low bits were zero. Mask off input bytes with a high bit
    // set as well. None of these additions can carry between bytes.
    #[inline]
    fn zero_byte_mask(word: u64) -> u64 {
        !(((word & LOW_BITS) + LOW_BITS) | word | LOW_BITS)
    }

    #[inline]
    fn non_space_mask(word: u64) -> u64 {
        let space = zero_byte_mask(word ^ u64::from_le_bytes([b' '; 8]));
        let newline = zero_byte_mask(word ^ u64::from_le_bytes([b'\n'; 8]));
        // Tab and carriage return differ by bit 2.
        let tab_or_return = zero_byte_mask(
            (word & !u64::from_le_bytes([4; 8])) ^ u64::from_le_bytes([b'\t'; 8]),
        );
        !(space | newline | tab_or_return) & HIGH_BITS
    }

    while let Some((block, tail)) = input.split_first_chunk::<32>() {
        let non_space = non_space_mask(u64::from_le_bytes(*block.first_chunk::<8>().unwrap()))
            | non_space_mask(u64::from_le_bytes(block[8..16].try_into().unwrap()))
            | non_space_mask(u64::from_le_bytes(block[16..24].try_into().unwrap()))
            | non_space_mask(u64::from_le_bytes(*block.last_chunk::<8>().unwrap()));
        if non_space != 0 {
            break;
        }
        input = tail;
    }
    while let Some((block, tail)) = input.split_first_chunk::<8>() {
        let non_space = non_space_mask(u64::from_le_bytes(*block));
        if non_space != 0 {
            return &input[non_space.trailing_zeros() as usize / 8..];
        }
        input = tail;
    }
    while let [b' ' | b'\t' | b'\r' | b'\n', rest @ ..] = input {
        input = rest;
    }
    input
}

fn consume_separator<'a>(
    input: &'a [u8],
    separator: u8,
    message: &str,
) -> Result<&'a [u8], serde_json::Error> {
    skip_json_space(input)
        .strip_prefix(&[separator])
        .ok_or_else(|| serde::de::Error::custom(message))
}

fn extract_repaired_members<'de, D>(
    deserializer: D,
    names: &[&str],
    done: &Cell<bool>,
) -> Result<HashMap<String, Vec<JsonToken>>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut out = HashMap::with_capacity(names.len());
    let seed = ExtractSeed {
        remain: names.iter().copied().collect(),
        out: &mut out,
        done,
    };
    // No `de.end()`: content after the top-level object is never inspected.
    match seed.deserialize(deserializer) {
        Ok(()) => Ok(out),
        // An error after `done` was raised while closing over the unread tail
        // (serde_json's `end_map`), which the early stop deliberately skips.
        Err(_) if done.get() => Ok(out),
        Err(error) => Err(error),
    }
}

fn tokenize(value: &str) -> Result<Vec<JsonToken>, PartialJsonError> {
    let mut repaired = false;
    tokenize_bytes(value.as_bytes(), Some(value), &mut repaired)
}

// The deserializer has already validated the syntax. The byte-only path
// additionally repairs strings without copying the rest of the value.
fn tokenize_bytes(
    value: &[u8],
    utf8: Option<&str>,
    repaired: &mut bool,
) -> Result<Vec<JsonToken>, PartialJsonError> {
    struct Parser<'a, 'repair> {
        value: &'a [u8],
        utf8: Option<&'a str>,
        repaired: &'repair mut bool,
        pos: usize,
        tokens: Vec<JsonToken>,
    }

    impl Parser<'_, '_> {
        fn skip_space(&mut self) {
            let input = &self.value[self.pos..];
            self.pos += input.len() - skip_json_space(input).len();
        }

        fn text_slice(&self, range: std::ops::Range<usize>) -> Result<&str, std::str::Utf8Error> {
            match self.utf8 {
                Some(value) => Ok(&value[range]),
                None => std::str::from_utf8(&self.value[range]),
            }
        }

        fn parse_string(&mut self) -> Result<String, PartialJsonError> {
            let start = self.pos + 1;
            let mut next = start;
            while let Some(input) = self.value.get(next..) {
                let Some(offset) = next_string_delimiter(input) else {
                    break;
                };
                let end = next + offset;
                if input[offset] == b'\\' {
                    next = end + 2;
                    continue;
                }

                self.pos = end + 1;
                if next == start {
                    return Ok(match self.text_slice(start..end) {
                        Ok(value) => value.to_owned(),
                        Err(error) => {
                            *self.repaired = true;
                            let bytes = &self.value[start..end];
                            let mut value = String::with_capacity(bytes.len() + 2);
                            append_repaired_utf8(&mut value, bytes, error);
                            value
                        }
                    });
                }
                let quoted = &self.value[start - 1..=end];
                return serde_json::from_slice(quoted)
                    .or_else(|_| {
                        *self.repaired |= self.utf8.is_none() && std::str::from_utf8(quoted).is_err();
                        repair_quoted_string(quoted)
                    })
                    .map_err(|error| PartialJsonError(error.to_string()));
            }
            Err(PartialJsonError("unexpected EOF".to_owned()))
        }

        fn parse_value(&mut self) -> Result<(), PartialJsonError> {
            self.skip_space();
            let bytes = self.value;
            match bytes.get(self.pos).copied() {
                Some(b'{') => self.parse_object(),
                Some(b'[') => self.parse_array(),
                Some(b'"') => {
                    let value = self.parse_string()?;
                    self.tokens.push(JsonToken::String(value));
                    Ok(())
                }
                Some(b't') if bytes.get(self.pos..self.pos + 4) == Some(b"true") => {
                    self.pos += 4;
                    self.tokens.push(JsonToken::Bool(true));
                    Ok(())
                }
                Some(b'f') if bytes.get(self.pos..self.pos + 5) == Some(b"false") => {
                    self.pos += 5;
                    self.tokens.push(JsonToken::Bool(false));
                    Ok(())
                }
                Some(b'n') if bytes.get(self.pos..self.pos + 4) == Some(b"null") => {
                    self.pos += 4;
                    self.tokens.push(JsonToken::Null);
                    Ok(())
                }
                Some(_) => {
                    let start = self.pos;
                    while bytes
                        .get(self.pos)
                        .is_some_and(|byte| !byte.is_ascii_whitespace() && !b",]}".contains(byte))
                    {
                        self.pos += 1;
                    }
                    let number = self
                        .text_slice(start..self.pos)
                        .map_err(|error| PartialJsonError(error.to_string()))?
                        .to_owned();
                    self.tokens.push(JsonToken::Number(number));
                    Ok(())
                }
                None => Err(PartialJsonError("unexpected EOF".to_owned())),
            }
        }

        fn parse_object(&mut self) -> Result<(), PartialJsonError> {
            self.pos += 1;
            self.tokens.push(JsonToken::Delim('{'));
            self.skip_space();
            if self.value.get(self.pos) == Some(&b'}') {
                self.pos += 1;
                self.tokens.push(JsonToken::Delim('}'));
                return Ok(());
            }
            loop {
                self.skip_space();
                if self.value.get(self.pos) != Some(&b'"') {
                    return Err(PartialJsonError("unexpected JSON name".to_owned()));
                }
                let name = self.parse_string()?;
                self.tokens.push(JsonToken::String(name));
                self.skip_space();
                if self.value.get(self.pos) != Some(&b':') {
                    return Err(PartialJsonError("expected ':'".to_owned()));
                }
                self.pos += 1;
                self.parse_value()?;
                self.skip_space();
                match self.value.get(self.pos) {
                    Some(b',') => self.pos += 1,
                    Some(b'}') => {
                        self.pos += 1;
                        self.tokens.push(JsonToken::Delim('}'));
                        return Ok(());
                    }
                    _ => return Err(PartialJsonError("unexpected EOF".to_owned())),
                }
            }
        }

        fn parse_array(&mut self) -> Result<(), PartialJsonError> {
            self.pos += 1;
            self.tokens.push(JsonToken::Delim('['));
            self.skip_space();
            if self.value.get(self.pos) == Some(&b']') {
                self.pos += 1;
                self.tokens.push(JsonToken::Delim(']'));
                return Ok(());
            }
            loop {
                self.parse_value()?;
                self.skip_space();
                match self.value.get(self.pos) {
                    Some(b',') => self.pos += 1,
                    Some(b']') => {
                        self.pos += 1;
                        self.tokens.push(JsonToken::Delim(']'));
                        return Ok(());
                    }
                    _ => return Err(PartialJsonError("unexpected EOF".to_owned())),
                }
            }
        }
    }

    let mut parser = Parser {
        value,
        utf8,
        repaired,
        pos: 0,
        tokens: Vec::new(),
    };
    parser.parse_value()?;
    parser.skip_space();
    if parser.pos != parser.value.len() {
        return Err(PartialJsonError("unexpected trailing JSON".to_owned()));
    }
    Ok(parser.tokens)
}

#[inline]
fn next_string_delimiter(input: &[u8]) -> Option<usize> {
    let head_len = input.len().min(8);
    let (head, tail) = input.split_at(head_len);
    head.iter()
        .position(|&byte| matches!(byte, b'"' | b'\\'))
        .or_else(|| memchr::memchr2(b'"', b'\\', tail).map(|offset| head_len + offset))
}

// Go's decoder emits a replacement character for each invalid byte, even
// when the input ends with an incomplete multi-byte sequence.
fn append_repaired_utf8(result: &mut String, mut bytes: &[u8], mut error: std::str::Utf8Error) {
    loop {
        let valid = error.valid_up_to();
        result.push_str(std::str::from_utf8(&bytes[..valid]).expect("valid UTF-8 prefix"));
        result.push('�');
        bytes = &bytes[valid + 1..];
        match std::str::from_utf8(bytes) {
            Ok(tail) => {
                result.push_str(tail);
                return;
            }
            Err(next) => error = next,
        }
    }
}

fn repair_quoted_string(quoted: &[u8]) -> Result<String, serde_json::Error> {
    let mut reader = JsonTokenReader::new(quoted, None);
    if quoted.len() <= 64 {
        return serde_json::from_reader(reader);
    }

    // On long strings, copy runs between escapes in bulk. Reuse the
    // streaming reader's escape repair and serde_json's string decoding.
    let mut normalized = String::with_capacity(quoted.len() + 2);
    while !reader.remaining.is_empty() {
        let width = memchr::memchr(b'\\', reader.remaining).unwrap_or(reader.remaining.len());
        let run = JsonTokenReader::take(&mut reader, width);
        match std::str::from_utf8(run) {
            Ok(text) => normalized.push_str(text),
            Err(error) => append_repaired_utf8(&mut normalized, run, error),
        }
        if !reader.remaining.is_empty() {
            let escape = std::str::from_utf8(reader.read_escape())
                .map_err(serde::de::Error::custom)?;
            normalized.push_str(escape);
        }
    }
    serde_json::from_str(&normalized)
}

// The retry yields Go-compatible tokens without scanning past the cutoff.
struct JsonTokenReader<'a> {
    remaining: &'a [u8],
    chunk: &'a [u8],
    done: Option<&'a Cell<bool>>,
    in_string: bool,
    at_value_start: bool,
    repaired: bool,
}

impl<'a> JsonTokenReader<'a> {
    fn new(content: &'a [u8], done: Option<&'a Cell<bool>>) -> Self {
        Self {
            remaining: content,
            chunk: &[],
            done,
            in_string: false,
            at_value_start: false,
            repaired: false,
        }
    }

    fn take(&mut self, width: usize) -> &'a [u8] {
        let (chunk, rest) = self.remaining.split_at(width);
        self.remaining = rest;
        chunk
    }

    fn next_chunk(&mut self) -> &'a [u8] {
        match self.remaining {
            [] => &[],
            [b'"', ..] => {
                self.in_string = !self.in_string;
                self.at_value_start = false;
                self.take(1)
            }
            [b'\\', ..] if self.in_string => self.read_escape(),
            [first @ 0x80..=0xff, ..] if self.in_string => {
                let width = first.leading_ones() as usize;
                if (2..=4).contains(&width)
                    && self
                        .remaining
                        .get(..width)
                        .is_some_and(|bytes| std::str::from_utf8(bytes).is_ok())
                {
                    return self.take(width);
                }
                // Go's utf8.DecodeRune advances one byte for each error.
                self.take(1);
                self.repaired = true;
                "�".as_bytes()
            }
            [b'0', b'0'..=b'9', ..] if !self.in_string && self.at_value_start => {
                // Go's token reader ends a number at the leading zero. The
                // next digit is an error only if extraction reads past it.
                self.at_value_start = false;
                self.take(1);
                self.repaired = true;
                b"0 "
            }
            [byte, ..] => {
                if !self.in_string && !byte.is_ascii_whitespace() {
                    self.at_value_start = matches!(byte, b'[' | b':' | b',')
                        || (*byte == b'-' && self.at_value_start);
                }
                self.take(1)
            }
        }
    }

    fn read_escape(&mut self) -> &'a [u8] {
        let bytes = self.remaining;
        if !bytes.starts_with(br"\u") {
            return self.take(bytes.len().min(2));
        }
        let Some(first) = parse_hex_quad(bytes.get(2..6)) else {
            // Leave malformed escape syntax for the deserializer to reject.
            return self.take(2);
        };
        let valid_pair = (0xd800..=0xdbff).contains(&first)
            && bytes.get(6..8) == Some(br"\u")
            && parse_hex_quad(bytes.get(8..12)).is_some_and(|low| (0xdc00..=0xdfff).contains(&low));
        if valid_pair {
            return self.take(12);
        }
        let escape = self.take(6);
        if (0xd800..=0xdfff).contains(&first) {
            self.repaired = true;
            br"\uFFFD"
        } else {
            escape
        }
    }
}

impl Read for JsonTokenReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.done.is_some_and(Cell::get) {
            return Ok(0);
        }
        if self.chunk.is_empty() && !buffer.is_empty() {
            self.chunk = self.next_chunk();
        }
        self.chunk.read(buffer)
    }
}

fn parse_hex_quad(bytes: Option<&[u8]>) -> Option<u16> {
    let bytes: &[u8; 4] = bytes?.try_into().ok()?;
    bytes.iter().try_fold(0_u16, |value, byte| {
        let digit = match byte {
            b'0'..=b'9' => u16::from(byte - b'0'),
            b'a'..=b'f' => u16::from(byte - b'a' + 10),
            b'A'..=b'F' => u16::from(byte - b'A' + 10),
            _ => return None,
        };
        Some(value * 16 + digit)
    })
}

#[cfg(test)]
mod tests {
    use super::{extract_top_level_members, skip_json_space, JsonToken};

    /// Go `TestIter`.
    #[test]
    fn test_iter() {
        for content in ["{", "[]", "{a}", "{]"] {
            assert!(extract_top_level_members(content.as_bytes(), &["a"]).is_err());
        }

        let got = extract_top_level_members(
            br#"{"a":1,"long1":{"skip":"skip"},"b":"val","long2":[0,0,{"skip":2}]}"#,
            &["a", "long1", "b", "long2"],
        )
        .unwrap();
        assert_eq!(got["a"], [JsonToken::Number("1".to_owned())]);
        assert_eq!(got["b"], [JsonToken::String("val".to_owned())]);
        assert_eq!(
            got["long1"],
            [
                JsonToken::Delim('{'),
                JsonToken::String("skip".to_owned()),
                JsonToken::String("skip".to_owned()),
                JsonToken::Delim('}'),
            ]
        );
        assert_eq!(
            got["long2"],
            [
                JsonToken::Delim('['),
                JsonToken::Number("0".to_owned()),
                JsonToken::Number("0".to_owned()),
                JsonToken::Delim('{'),
                JsonToken::String("skip".to_owned()),
                JsonToken::Number("2".to_owned()),
                JsonToken::Delim('}'),
                JsonToken::Delim(']'),
            ]
        );
    }

    #[test]
    fn replaces_each_invalid_utf8_byte_in_requested_names_and_values() {
        let got = extract_top_level_members(b"{\"\xe2\x82\":\"\xe2\x82\"}", &["��"]).unwrap();
        assert_eq!(got["��"], [JsonToken::String("��".to_owned())]);
    }

    #[test]
    fn stops_at_last_value_and_decodes_requested_surrogates() {
        let got =
            extract_top_level_members(b"{\"name\":\"\\ud800\",\"tail\":\xff", &["name"]).unwrap();
        assert_eq!(got["name"], [JsonToken::String("�".to_owned())]);
        assert!(
            extract_top_level_members(b"{\"name\":\"\\ud800\",\"tail\":\xff", &["tail"]).is_err()
        );
    }

    #[test]
    fn stops_after_a_leading_zero_only_for_the_last_requested_scalar() {
        let got = extract_top_level_members(br#"{"x":-01,"y":2}"#, &["x"]).unwrap();
        assert_eq!(got["x"], [JsonToken::Number("-0".to_owned())]);
        assert!(extract_top_level_members(br#"{"x":-01,"y":2}"#, &["x", "y"]).is_err());
        assert!(extract_top_level_members(br#"{"x":[01]}"#, &["x"]).is_err());
    }

    #[test]
    fn whitespace_blocks_stop_at_every_non_json_byte() {
        let whitespace = b" \t\r\n".repeat(12);
        let mut input = whitespace.clone();
        for offset in 0..whitespace.len() {
            for byte in 0..=u8::MAX {
                if matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
                    continue;
                }
                input[offset] = byte;
                assert_eq!(skip_json_space(&input), &input[offset..]);
            }
            input[offset] = whitespace[offset];
        }
        assert!(skip_json_space(&input).is_empty());
    }

    #[test]
    fn repairs_nested_utf8_and_escapes_across_a_long_prefix() {
        let plain = "a".repeat(64 * 1024 + 1);
        let mut content = b"{\"x\":[\"\xe2\x82\",".to_vec();
        content.extend_from_slice(b" \t\r\n".repeat(16 * 1024).as_slice());
        content.extend_from_slice(br#""\n"#);
        content.extend_from_slice(plain.as_bytes());
        content.extend_from_slice(br#"\ud800"],"x":false,"y":-0.00e+01,"tail": broken"#);
        let got = extract_top_level_members(&content, &["x", "y", "x"]).unwrap();
        assert_eq!(
            got["x"],
            [
                JsonToken::Delim('['),
                JsonToken::String("��".to_owned()),
                JsonToken::String(format!("\n{plain}�")),
                JsonToken::Delim(']'),
            ]
        );
        assert_eq!(got["y"], [JsonToken::Number("-0.00e+01".to_owned())]);
    }
}
