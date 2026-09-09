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
//! before the last requested member still fails. Error text is native
//! `serde_json` wording; the failure boundary matches the package contract.
//! Like Go `encoding/json`, invalid UTF-8 and unpaired UTF-16 surrogates in
//! JSON strings become U+FFFD; ordinary valid input stays on the borrowed path.
//! `BUILD.bazel`'s `fastjson` and `partialjson` aliases both map to this module.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
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

struct ExtractSeed<'c> {
    remain: HashSet<String>,
    out: &'c RefCell<HashMap<String, Box<RawValue>>>,
    done: &'c Cell<bool>,
}

impl<'de> Visitor<'de> for ExtractSeed<'_> {
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
            if self.remain.remove(&name) {
                let value = map.next_value::<Box<RawValue>>()?;
                self.out.borrow_mut().insert(name, value);
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

impl<'de> DeserializeSeed<'de> for ExtractSeed<'_> {
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

    let content = normalize_json_strings(content);
    let out = RefCell::new(HashMap::with_capacity(names.len()));
    let done = Cell::new(false);
    let seed = ExtractSeed {
        remain: names.iter().map(|s| (*s).to_string()).collect(),
        out: &out,
        done: &done,
    };
    let mut de = serde_json::Deserializer::from_str(&content);
    // No `de.end()`: content after the top-level object is never inspected.
    let raw = match seed.deserialize(&mut de) {
        Ok(()) => out.into_inner(),
        // An error after `done` was raised while closing over the unread tail
        // (serde_json's `end_map`), which the early stop deliberately skips.
        Err(_) if done.get() => out.into_inner(),
        Err(e) => return Err(PartialJsonError(e.to_string())),
    };
    raw.into_iter()
        .map(|(name, value)| tokenize(value.get()).map(|tokens| (name, tokens)))
        .collect()
}

fn tokenize(value: &str) -> Result<Vec<JsonToken>, PartialJsonError> {
    struct Parser<'a> {
        bytes: &'a [u8],
        pos: usize,
        tokens: Vec<JsonToken>,
    }

    impl Parser<'_> {
        fn skip_space(&mut self) {
            while self
                .bytes
                .get(self.pos)
                .is_some_and(u8::is_ascii_whitespace)
            {
                self.pos += 1;
            }
        }

        fn parse_string(&mut self) -> Result<String, PartialJsonError> {
            let start = self.pos;
            self.pos += 1;
            while let Some(&byte) = self.bytes.get(self.pos) {
                match byte {
                    b'"' => {
                        self.pos += 1;
                        return serde_json::from_slice(&self.bytes[start..self.pos])
                            .map_err(|error| PartialJsonError(error.to_string()));
                    }
                    b'\\' => self.pos += 2,
                    _ => self.pos += 1,
                }
            }
            Err(PartialJsonError("unexpected EOF".to_owned()))
        }

        fn parse_value(&mut self) -> Result<(), PartialJsonError> {
            self.skip_space();
            match self.bytes.get(self.pos).copied() {
                Some(b'{') => self.parse_object(),
                Some(b'[') => self.parse_array(),
                Some(b'"') => {
                    let value = self.parse_string()?;
                    self.tokens.push(JsonToken::String(value));
                    Ok(())
                }
                Some(b't') if self.bytes.get(self.pos..self.pos + 4) == Some(b"true") => {
                    self.pos += 4;
                    self.tokens.push(JsonToken::Bool(true));
                    Ok(())
                }
                Some(b'f') if self.bytes.get(self.pos..self.pos + 5) == Some(b"false") => {
                    self.pos += 5;
                    self.tokens.push(JsonToken::Bool(false));
                    Ok(())
                }
                Some(b'n') if self.bytes.get(self.pos..self.pos + 4) == Some(b"null") => {
                    self.pos += 4;
                    self.tokens.push(JsonToken::Null);
                    Ok(())
                }
                Some(_) => {
                    let start = self.pos;
                    while self
                        .bytes
                        .get(self.pos)
                        .is_some_and(|byte| !byte.is_ascii_whitespace() && !b",]}".contains(byte))
                    {
                        self.pos += 1;
                    }
                    let number = std::str::from_utf8(&self.bytes[start..self.pos])
                        .map_err(|error| PartialJsonError(error.to_string()))?;
                    self.tokens.push(JsonToken::Number(number.to_owned()));
                    Ok(())
                }
                None => Err(PartialJsonError("unexpected EOF".to_owned())),
            }
        }

        fn parse_object(&mut self) -> Result<(), PartialJsonError> {
            self.pos += 1;
            self.tokens.push(JsonToken::Delim('{'));
            self.skip_space();
            if self.bytes.get(self.pos) == Some(&b'}') {
                self.pos += 1;
                self.tokens.push(JsonToken::Delim('}'));
                return Ok(());
            }
            loop {
                self.skip_space();
                if self.bytes.get(self.pos) != Some(&b'"') {
                    return Err(PartialJsonError("unexpected JSON name".to_owned()));
                }
                let name = self.parse_string()?;
                self.tokens.push(JsonToken::String(name));
                self.skip_space();
                if self.bytes.get(self.pos) != Some(&b':') {
                    return Err(PartialJsonError("expected ':'".to_owned()));
                }
                self.pos += 1;
                self.parse_value()?;
                self.skip_space();
                match self.bytes.get(self.pos) {
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
            if self.bytes.get(self.pos) == Some(&b']') {
                self.pos += 1;
                self.tokens.push(JsonToken::Delim(']'));
                return Ok(());
            }
            loop {
                self.parse_value()?;
                self.skip_space();
                match self.bytes.get(self.pos) {
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
        bytes: value.as_bytes(),
        pos: 0,
        tokens: Vec::new(),
    };
    parser.parse_value()?;
    parser.skip_space();
    if parser.pos != parser.bytes.len() {
        return Err(PartialJsonError("unexpected trailing JSON".to_owned()));
    }
    Ok(parser.tokens)
}

fn normalize_json_strings(content: &[u8]) -> Cow<'_, str> {
    let utf8 = String::from_utf8_lossy(content);
    let bytes = utf8.as_bytes();
    let mut output: Option<Vec<u8>> = None;
    let mut in_string = false;
    let mut index = 0;

    while index < bytes.len() {
        let byte = bytes[index];
        if !in_string {
            if byte == b'"' {
                in_string = true;
            }
            if let Some(output) = &mut output {
                output.push(byte);
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = false;
            if let Some(output) = &mut output {
                output.push(byte);
            }
            index += 1;
            continue;
        }
        if byte != b'\\' || bytes.get(index + 1) != Some(&b'u') {
            let width = if byte == b'\\' && index + 1 < bytes.len() {
                2
            } else {
                1
            };
            if let Some(output) = &mut output {
                output.extend_from_slice(&bytes[index..index + width]);
            }
            index += width;
            continue;
        }

        let Some(code) = parse_hex_quad(bytes.get(index + 2..index + 6)) else {
            if let Some(output) = &mut output {
                output.push(byte);
            }
            index += 1;
            continue;
        };
        let valid_pair = (0xd800..=0xdbff).contains(&code)
            && bytes.get(index + 6..index + 8) == Some(br#"\u"#)
            && parse_hex_quad(bytes.get(index + 8..index + 12))
                .is_some_and(|low| (0xdc00..=0xdfff).contains(&low));
        let invalid =
            (0xdc00..=0xdfff).contains(&code) || ((0xd800..=0xdbff).contains(&code) && !valid_pair);

        if invalid {
            let output = output.get_or_insert_with(|| bytes[..index].to_vec());
            output.extend_from_slice(br#"\uFFFD"#);
            index += 6;
        } else {
            let width = if valid_pair { 12 } else { 6 };
            if let Some(output) = &mut output {
                output.extend_from_slice(&bytes[index..index + width]);
            }
            index += width;
        }
    }

    match output {
        Some(output) => Cow::Owned(String::from_utf8(output).expect("normalized JSON stays UTF-8")),
        None => utf8,
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
    use super::{extract_top_level_members, JsonToken};

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
}
