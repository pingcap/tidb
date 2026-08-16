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

//! `boundary:` Go's standard-library `encoding/json`, which `sql.go` uses to
//! marshal the `TIMER_EXT` column and `store.go` uses to unmarshal it back.
//!
//! This workspace has no serialization crate available to `tidb-timer`, so the
//! two directions are written out by hand against exactly the shapes the
//! package needs:
//!
//! - Marshalling is done by the `timer_ext` writers in [`super::sql`], not
//!   here; this module only supplies the string escaper they share. The
//!   escaper reproduces `encoding/json`'s default HTML escaping (`<`, `>` and
//!   `&` become `<`, `>`, `&`) plus the usual short escapes,
//!   because the upstream tests compare the marshalled bytes byte-for-byte.
//! - Unmarshalling is a complete but minimal recursive-descent parser over the
//!   JSON grammar, producing [`JsonValue`]. Numbers are parsed as `f64` and
//!   read back as `i64`, which is exact for the unix-second values the
//!   `TIMER_EXT` document carries.
//!
//! Not covered (nothing in the package needs it): struct tag reflection,
//! streaming decoders, and `json.Number`.

use std::fmt::Write as _;

/// Appends `text` as a JSON string literal, escaped the way Go's
/// `encoding/json` escapes by default.
pub fn write_json_string(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            ch if (ch as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", ch as u32);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

/// A parsed JSON document.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    /// JSON `null`.
    Null,
    /// JSON `true`/`false`.
    Bool(bool),
    /// Any JSON number.
    Number(f64),
    /// A JSON string.
    Str(String),
    /// A JSON array.
    Array(Vec<JsonValue>),
    /// A JSON object, in document order.
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    /// The member named `key`, or `None` when absent or when `self` is not an
    /// object. A member explicitly set to `null` reads back as `None`, which is
    /// what Go's pointer-typed fields do.
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        match self {
            Self::Object(members) => members
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value)
                .filter(|value| !matches!(value, Self::Null)),
            _ => None,
        }
    }

    /// This value as a string, when it is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => Some(text),
            _ => None,
        }
    }

    /// This value as an `i64`, when it is a number.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(number) => Some(*number as i64),
            _ => None,
        }
    }

    /// This value as a bool, when it is one.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// This value's elements, when it is an array.
    pub fn as_array(&self) -> Option<&[JsonValue]> {
        match self {
            Self::Array(items) => Some(items),
            _ => None,
        }
    }
}

/// Go `json.Unmarshal` into a generic document.
pub fn parse(text: &str) -> Result<JsonValue, String> {
    let bytes: Vec<char> = text.chars().collect();
    let mut parser = Parser { bytes, pos: 0 };
    parser.skip_ws();
    let value = parser.value()?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return Err("invalid character after top-level value".to_string());
    }
    Ok(value)
}

struct Parser {
    bytes: Vec<char>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, ch: char) -> Result<(), String> {
        if self.peek() == Some(ch) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected '{ch}' at offset {}", self.pos))
        }
    }

    fn literal(&mut self, text: &str) -> Result<(), String> {
        for ch in text.chars() {
            self.expect(ch)?;
        }
        Ok(())
    }

    fn value(&mut self) -> Result<JsonValue, String> {
        match self.peek() {
            None => Err("unexpected end of JSON input".to_string()),
            Some('n') => {
                self.literal("null")?;
                Ok(JsonValue::Null)
            }
            Some('t') => {
                self.literal("true")?;
                Ok(JsonValue::Bool(true))
            }
            Some('f') => {
                self.literal("false")?;
                Ok(JsonValue::Bool(false))
            }
            Some('"') => Ok(JsonValue::Str(self.string()?)),
            Some('[') => self.array(),
            Some('{') => self.object(),
            Some(_) => self.number(),
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut out = String::new();
        loop {
            let ch = self.peek().ok_or("unexpected end of JSON string")?;
            self.pos += 1;
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let escape = self.peek().ok_or("unexpected end of JSON escape")?;
                    self.pos += 1;
                    match escape {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let mut code = 0u32;
                            for _ in 0..4 {
                                let digit = self.peek().ok_or("truncated \\u escape")?;
                                self.pos += 1;
                                code = code * 16
                                    + digit.to_digit(16).ok_or("invalid \\u escape digit")?;
                            }
                            out.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                        }
                        other => return Err(format!("invalid escape '\\{other}'")),
                    }
                }
                ch => out.push(ch),
            }
        }
    }

    fn array(&mut self) -> Result<JsonValue, String> {
        self.expect('[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(']') {
            self.pos += 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value()?);
            self.skip_ws();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some(']') => {
                    self.pos += 1;
                    return Ok(JsonValue::Array(items));
                }
                _ => return Err("expected ',' or ']' in JSON array".to_string()),
            }
        }
    }

    fn object(&mut self) -> Result<JsonValue, String> {
        self.expect('{')?;
        let mut members = Vec::new();
        self.skip_ws();
        if self.peek() == Some('}') {
            self.pos += 1;
            return Ok(JsonValue::Object(members));
        }
        loop {
            self.skip_ws();
            let key = self.string()?;
            self.skip_ws();
            self.expect(':')?;
            self.skip_ws();
            members.push((key, self.value()?));
            self.skip_ws();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some('}') => {
                    self.pos += 1;
                    return Ok(JsonValue::Object(members));
                }
                _ => return Err("expected ',' or '}' in JSON object".to_string()),
            }
        }
    }

    fn number(&mut self) -> Result<JsonValue, String> {
        let start = self.pos;
        while matches!(self.peek(), Some('-' | '+' | '.' | 'e' | 'E' | '0'..='9')) {
            self.pos += 1;
        }
        let text: String = self.bytes[start..self.pos].iter().collect();
        text.parse::<f64>()
            .map(JsonValue::Number)
            .map_err(|_| format!("invalid number literal '{text}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_the_timer_ext_shapes() {
        let parsed = parse(
            r#"{"tags":["l1","l2"],"manual":{"request_id":"req1","request_time_unix":123,"processed":true},"event":null}"#,
        )
        .unwrap();
        assert_eq!(
            parsed
                .get("tags")
                .and_then(JsonValue::as_array)
                .map(|items| items.len()),
            Some(2)
        );
        assert_eq!(
            parsed
                .get("manual")
                .and_then(|manual| manual.get("request_time_unix"))
                .and_then(JsonValue::as_i64),
            Some(123)
        );
        assert_eq!(
            parsed
                .get("manual")
                .and_then(|manual| manual.get("processed"))
                .and_then(JsonValue::as_bool),
            Some(true)
        );
        assert!(parsed.get("event").is_none());
    }

    #[test]
    fn escapes_like_go() {
        let mut out = String::new();
        write_json_string(&mut out, "a<b>&\"c\"\n");
        assert_eq!(out, r#""a\u003cb\u003e\u0026\"c\"\n""#);
    }
}
